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
pub async fn subscribe(cfg: &Config, client: &reqwest::Client) -> Result<SseStream> {
    let url = format!("{}{}", cfg.server, RUNNER_STREAM_PATH);
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
}
