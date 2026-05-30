//! SSE subscriber for `GET /api/streams/runners`.
//!
//! Opens a long-lived bearer-authed connection, parses the SSE frames
//! the server emits, and yields each `data:` payload as a JSON string.
//! Higher-level dispatch (decode → [`crate::Event`] → run handlers)
//! lives in [`crate::runner::Runner`].

use crate::config::Config;
use crate::error::{Error, Result};

use bytes::Bytes;
use futures::stream::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Path of the runner SSE endpoint on a patchwave server.
pub const RUNNER_STREAM_PATH: &str = "/api/streams/runners";

/// Open a long-lived SSE connection. Caller drives the returned
/// [`SseStream`] to receive parsed event payloads.
///
/// Sends the runner's identity (`name`, `instance`, `version`, `role`,
/// `hostname`, optional `repos` subscription) as query params at
/// connect time so the patchwave dashboard can show this runner as a
/// distinct row. All identity params are optional — the server fills
/// in reasonable defaults (e.g. `name` falls back to the token sub).
pub async fn subscribe(cfg: &Config, client: &reqwest::Client) -> Result<SseStream> {
    let url = build_subscribe_url(&cfg.server, cfg);
    let resp = client
        .get(&url)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", cfg.token),
        )
        .send()
        .await
        .map_err(Error::Http)?
        .error_for_status()
        .map_err(Error::Http)?;

    Ok(SseStream::new(resp.bytes_stream()))
}

/// Build the full `GET /api/streams/runners?…` URL, appending whichever
/// identity params are set on `cfg`. Extracted for testability — the
/// reqwest path above is hard to exercise without spinning a server.
fn build_subscribe_url(server: &str, cfg: &Config) -> String {
    let base = format!("{}{}", server, RUNNER_STREAM_PATH);
    let mut url = url::Url::parse(&base)
        .expect("RUNNER_STREAM_PATH joins cleanly onto a valid base URL");
    {
        let mut q = url.query_pairs_mut();
        if let Some(ref v) = cfg.runner_name     { q.append_pair("name", v); }
        if let Some(ref v) = cfg.runner_instance { q.append_pair("instance", v); }
        if let Some(ref v) = cfg.runner_version  { q.append_pair("version", v); }
        if let Some(ref v) = cfg.runner_role     { q.append_pair("role", v); }
        if let Some(ref v) = cfg.runner_hostname { q.append_pair("hostname", v); }
        if let Some(ref repos) = cfg.runner_repos {
            if !repos.is_empty() {
                q.append_pair("repos", &repos.join(","));
            }
        }
    }
    // `query_pairs_mut` always sets the query to `Some(_)`, even when
    // nothing was appended, leaving a trailing `?`. Drop it so the
    // wire shape matches the pre-presence-dashboard SDK exactly.
    let mut s = url.to_string();
    if s.ends_with('?') {
        s.pop();
    }
    s
}

/// Stream of decoded SSE `data:` payloads (one item per complete event
/// frame). Comment lines (`:` prefix, used for keep-alive pings) are
/// silently dropped.
pub struct SseStream {
    bytes: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    buf: Vec<u8>,
    pending: VecDeque<String>,
}

impl SseStream {
    fn new<S>(bytes: S) -> Self
    where
        S: Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
    {
        Self {
            bytes: Box::pin(bytes),
            buf: Vec::new(),
            pending: VecDeque::new(),
        }
    }

    /// Drain any complete `data:` frames already sitting in `self.buf`
    /// into `self.pending` so `poll_next` can return them one at a time.
    fn drain_frames(&mut self) {
        while let Some(end) = find_double_newline(&self.buf) {
            // `end` is the index of the first byte of the separator;
            // it's either 2 bytes (`\n\n`) or 4 (`\r\n\r\n`).
            let sep_len = sep_len_at(&self.buf, end);
            let frame: Vec<u8> = self.buf.drain(..end + sep_len).collect();
            let frame = &frame[..end]; // strip the separator
            if let Some(data) = parse_data(frame) {
                self.pending.push_back(data);
            }
        }
    }
}

impl Stream for SseStream {
    type Item = Result<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(next) = self.pending.pop_front() {
                return Poll::Ready(Some(Ok(next)));
            }
            match self.bytes.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    // Stream closed; flush whatever's left in the buffer.
                    self.drain_frames();
                    if let Some(next) = self.pending.pop_front() {
                        return Poll::Ready(Some(Ok(next)));
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(Error::Http(e)))),
                Poll::Ready(Some(Ok(chunk))) => {
                    self.buf.extend_from_slice(&chunk);
                    self.drain_frames();
                }
            }
        }
    }
}

/// Find the byte index of the start of the next SSE event separator
/// (`\n\n` or `\r\n\r\n`) in `buf`, if any.
fn find_double_newline(buf: &[u8]) -> Option<usize> {
    let lf_lf = buf.windows(2).position(|w| w == b"\n\n");
    let crlf_crlf = buf.windows(4).position(|w| w == b"\r\n\r\n");
    match (lf_lf, crlf_crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn sep_len_at(buf: &[u8], pos: usize) -> usize {
    if buf.get(pos..pos + 4) == Some(b"\r\n\r\n") {
        4
    } else {
        2
    }
}

/// Parse a complete SSE frame (minus its terminating separator) into
/// the joined `data:` payload. Returns `None` for frames that contain
/// only comments / keep-alive pings.
fn parse_data(frame: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(frame).ok()?;
    let mut data_lines: Vec<&str> = Vec::new();
    for line in text.split('\n') {
        // Strip a trailing \r so \r\n line endings work too.
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(rest) = line.strip_prefix("data:") {
            // Per spec, exactly one optional leading space after `:`.
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
        // `:comment`, `event:`, `id:`, `retry:` — ignored.
    }
    if data_lines.is_empty() {
        None
    } else {
        Some(data_lines.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt as _};

    fn collect_payloads(frames: &[&[u8]]) -> Vec<String> {
        let owned: Vec<Bytes> = frames.iter().map(|f| Bytes::copy_from_slice(f)).collect();
        let s = stream::iter(owned.into_iter().map(Ok::<_, reqwest::Error>));
        let stream = SseStream::new(s);
        futures::executor::block_on(async move {
            stream.map(|r| r.unwrap()).collect::<Vec<_>>().await
        })
    }

    #[test]
    fn parses_two_events_split_across_chunks() {
        let got = collect_payloads(&[b"data: a\n\nd", b"ata: b\n\n"]);
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn ignores_keepalive_comments() {
        let got = collect_payloads(&[b":ping\n\ndata: hello\n\n"]);
        assert_eq!(got, vec!["hello".to_string()]);
    }

    #[test]
    fn handles_crlf_endings() {
        let got = collect_payloads(&[b"data: hi\r\n\r\n"]);
        assert_eq!(got, vec!["hi".to_string()]);
    }

    #[test]
    fn multi_line_data_joined_with_newline() {
        let got = collect_payloads(&[b"data: a\ndata: b\n\n"]);
        assert_eq!(got, vec!["a\nb".to_string()]);
    }

    // ── build_subscribe_url ──────────────────────────────────────────────

    /// Construct a minimal `Config` for URL tests. Caller mutates the
    /// fields it cares about. `server` and `token` are required by the
    /// struct but irrelevant to the query string.
    fn url_test_cfg() -> Config {
        Config {
            server: "https://patchwave.example".to_string(),
            token: "irrelevant".to_string(),
            runner_name: None,
            runner_instance: None,
            runner_version: None,
            runner_role: None,
            runner_hostname: None,
            runner_repos: None,
            workspace: std::path::PathBuf::from("/tmp"),
        }
    }

    /// Strip the query string (after `?`) so the test asserts only on
    /// path + query — not the scheme/host, which the caller controls.
    fn split_query(url: &str) -> (&str, &str) {
        url.split_once('?').unwrap_or((url, ""))
    }

    /// Parse `q` into a sorted `Vec<(k, v)>` so assertions are
    /// independent of `url::Url::query_pairs_mut`'s emission order.
    fn parse_query(q: &str) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = q
            .split('&')
            .filter(|s| !s.is_empty())
            .filter_map(|p| p.split_once('='))
            .map(|(k, v)| {
                (
                    url::form_urlencoded::parse(k.as_bytes()).map(|(k, _)| k.into_owned()).next().unwrap_or_else(|| k.to_string()),
                    url::form_urlencoded::parse(format!("k={v}").as_bytes())
                        .next()
                        .map(|(_, v)| v.into_owned())
                        .unwrap_or_else(|| v.to_string()),
                )
            })
            .collect();
        pairs.sort();
        pairs
    }

    #[test]
    fn build_url_no_params_matches_legacy_wire_shape() {
        let cfg = url_test_cfg();
        let url = build_subscribe_url(&cfg.server, &cfg);
        // No identity params set ⇒ URL identical to the pre-presence-dashboard
        // SDK's request. Critical for backwards compatibility with older
        // patchwave servers that don't understand the new query string.
        assert_eq!(
            url,
            "https://patchwave.example/api/streams/runners",
        );
    }

    #[test]
    fn build_url_appends_only_set_fields() {
        let mut cfg = url_test_cfg();
        cfg.runner_name = Some("alpha".to_string());
        cfg.runner_role = Some("cargo-test".to_string());
        let url = build_subscribe_url(&cfg.server, &cfg);
        let (path, query) = split_query(&url);
        assert_eq!(path, "https://patchwave.example/api/streams/runners");
        assert_eq!(
            parse_query(query),
            vec![
                ("name".to_string(), "alpha".to_string()),
                ("role".to_string(), "cargo-test".to_string()),
            ],
        );
    }

    #[test]
    fn build_url_emits_all_identity_fields_when_set() {
        let mut cfg = url_test_cfg();
        cfg.runner_name     = Some("alpha".to_string());
        cfg.runner_instance = Some("inst-1".to_string());
        cfg.runner_version  = Some("0.1.0".to_string());
        cfg.runner_role     = Some("ripple".to_string());
        cfg.runner_hostname = Some("box-a".to_string());
        cfg.runner_repos    = Some(vec!["alice/proj".to_string(), "bob/lib".to_string()]);

        let url = build_subscribe_url(&cfg.server, &cfg);
        let (_, query) = split_query(&url);
        let q = parse_query(query);
        assert!(q.contains(&("name".into(), "alpha".into())));
        assert!(q.contains(&("instance".into(), "inst-1".into())));
        assert!(q.contains(&("version".into(), "0.1.0".into())));
        assert!(q.contains(&("role".into(), "ripple".into())));
        assert!(q.contains(&("hostname".into(), "box-a".into())));
        // Repo list is sent as a single comma-joined value.
        assert!(q.contains(&("repos".into(), "alice/proj,bob/lib".into())));
        assert_eq!(q.len(), 6);
    }

    #[test]
    fn build_url_skips_empty_repo_list() {
        // `runner_repos = Some(vec![])` must NOT emit `repos=` — the
        // server would reject that as a malformed empty subscription.
        let mut cfg = url_test_cfg();
        cfg.runner_repos = Some(vec![]);
        let url = build_subscribe_url(&cfg.server, &cfg);
        assert_eq!(url, "https://patchwave.example/api/streams/runners");
    }

    #[test]
    fn build_url_percent_encodes_values_with_special_chars() {
        // Roles/hostnames can contain characters that need encoding
        // (spaces, slashes, ampersands). The `url` crate handles this;
        // the test pins the contract.
        let mut cfg = url_test_cfg();
        cfg.runner_role = Some("ci & deploy".to_string());
        cfg.runner_hostname = Some("box/with/slashes".to_string());
        let url = build_subscribe_url(&cfg.server, &cfg);
        let (_, query) = split_query(&url);
        // Raw query keeps the percent-encoded form…
        assert!(query.contains("role=ci+%26+deploy") || query.contains("role=ci%20%26%20deploy"));
        // …and decoding round-trips back to the original strings.
        let q = parse_query(query);
        assert!(q.contains(&("role".into(), "ci & deploy".into())));
        assert!(q.contains(&("hostname".into(), "box/with/slashes".into())));
    }
}
