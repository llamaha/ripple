//! POST `/api/ci/{change_hash}/result` — flips the CI badge and
//! advances any linked intent.
//!
//! This piece is the most-tested half of the SDK: the endpoint
//! contract is verified by `patchwave/deploy/smoke-ci-reporting.sh`
//! against the production droplet, so the wire shape is solid.

use crate::config::Config;
use crate::error::{Error, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::io::Write;

/// Logs strictly smaller than this go inline in `details.output`.
/// Anything at or above this threshold uploads as a gzipped blob and
/// the report carries `details.log_blob = "<blake3-hex>"` instead.
/// 4 KiB matches the LFS threshold pattern; revisit after live data.
pub const INLINE_LOG_MAX_BYTES: usize = 4 * 1024;

/// Builder for a CI result POST. Construct via [`Reporter::new`] or
/// (more typically) via [`crate::RunnerContext::report`].
#[must_use = "Reporter does nothing until .send() is awaited"]
pub struct Reporter<'a> {
    cfg: &'a Config,
    client: &'a reqwest::Client,
    change_hash: String,
    status: ReportStatus,
    details: Map<String, Value>,
    owner: Option<String>,
    repo: Option<String>,
}

/// Permitted CI report statuses. Patchwave widens this set over
/// time; missing variants serialise via [`ReportStatus::custom`].
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportStatus {
    /// Build / tests succeeded.
    Pass,
    /// Build / tests failed.
    Fail,
    /// Long-running job still in flight.
    Pending,
    /// Job intentionally skipped (e.g. docs-only change).
    Skipped,
    /// Escape hatch for new server-side statuses the SDK doesn't yet model.
    #[serde(untagged)]
    Other(String),
}

impl ReportStatus {
    /// Free-form status for forward compatibility. Avoid for the
    /// known four — use the typed variants.
    pub fn custom(s: impl Into<String>) -> Self {
        Self::Other(s.into())
    }
}

impl<'a> Reporter<'a> {
    /// Manually construct a Reporter. Most users go through
    /// [`crate::RunnerContext::report`] instead.
    pub fn new(
        cfg: &'a Config,
        client: &'a reqwest::Client,
        change_hash: impl Into<String>,
        status: ReportStatus,
    ) -> Self {
        let mut details = Map::new();
        if let Some(name) = &cfg.runner_name {
            details.insert("provider".into(), json!(name));
        }
        Self {
            cfg,
            client,
            change_hash: change_hash.into(),
            status,
            details,
            owner: None,
            repo: None,
        }
    }

    /// Tag the reporter with the repo it should upload log blobs into.
    /// [`RunnerContext::report`] sets this automatically from the
    /// in-flight event; callers that build a [`Reporter`] directly must
    /// supply it themselves or [`attach_log`](Self::attach_log) will
    /// fall back to truncated-inline for oversized logs.
    pub fn with_repo(mut self, owner: impl Into<String>, repo: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self.repo = Some(repo.into());
        self
    }

    /// Set `details.summary`.
    pub fn summary(mut self, s: impl Into<String>) -> Self {
        self.details.insert("summary".into(), json!(s.into()));
        self
    }

    /// Set `details.run_url` (link to the CI run / logs).
    pub fn run_url(mut self, s: impl Into<String>) -> Self {
        self.details.insert("run_url".into(), json!(s.into()));
        self
    }

    /// Set `details.logs_url` (direct link to job logs).
    pub fn logs_url(mut self, s: impl Into<String>) -> Self {
        self.details.insert("logs_url".into(), json!(s.into()));
        self
    }

    /// Set `details.duration_ms`.
    pub fn duration_ms(mut self, ms: u64) -> Self {
        self.details.insert("duration_ms".into(), json!(ms));
        self
    }

    /// Override the auto-detected provider chip.
    pub fn provider(mut self, s: impl Into<String>) -> Self {
        self.details.insert("provider".into(), json!(s.into()));
        self
    }

    /// Attach an arbitrary key. Anything not in the recognised-keys
    /// table is stored and echoed back but otherwise ignored.
    pub fn detail(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    /// Attach captured job output to the report.
    ///
    /// Logs smaller than [`INLINE_LOG_MAX_BYTES`] go straight into
    /// `details.output` (one round-trip, no blob bookkeeping). Larger
    /// logs are gzipped, content-addressed by Blake3, and uploaded via
    /// `POST /api/blobs/{owner}/{repo}/{hash}`; the report then carries
    /// `details.log_blob = "<hash>"` and
    /// `details.log_compressed = "gzip"`. The UI knows to fetch and
    /// inflate from there.
    ///
    /// If the reporter has no `(owner, repo)` context (i.e. it was
    /// constructed manually without [`Reporter::with_repo`]), oversized
    /// logs are truncated to the tail and inlined instead — the report
    /// still goes through, just with less context than ideal.
    pub async fn attach_log(mut self, output: &str) -> Result<Self> {
        if output.len() < INLINE_LOG_MAX_BYTES {
            self.details.insert("output".into(), json!(output));
            return Ok(self);
        }

        // Need (owner, repo) to know where to PUT the blob. Without
        // them, degrade to a tail-truncated inline log so the report
        // still goes out — the operator still gets *something*.
        let (owner, repo) = match (self.owner.clone(), self.repo.clone()) {
            (Some(o), Some(r)) => (o, r),
            _ => {
                let mut start = output.len().saturating_sub(INLINE_LOG_MAX_BYTES);
                while start < output.len() && !output.is_char_boundary(start) {
                    start += 1;
                }
                let tail = &output[start..];
                self.details.insert(
                    "output".into(),
                    json!(format!(
                        "…[log too large to upload; reporter has no (owner, repo) — keeping last {} bytes]…\n{}",
                        tail.len(),
                        tail,
                    )),
                );
                return Ok(self);
            }
        };

        let gz_bytes = {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(output.as_bytes())?;
            encoder.finish()?
        };

        let hash_hex = blake3::hash(&gz_bytes).to_hex().to_string();

        let url = format!(
            "{}/api/blobs/{}/{}/{}",
            self.cfg.server, owner, repo, hash_hex
        );
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.cfg.token)
            .body(gz_bytes)
            .send()
            .await
            .map_err(Error::Http)?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let truncated = body.chars().take(400).collect::<String>();
            return Err(Error::Report {
                status: status.as_u16(),
                body: truncated,
            });
        }

        self.details.insert("log_blob".into(), json!(hash_hex));
        self.details
            .insert("log_compressed".into(), json!("gzip"));
        Ok(self)
    }

    /// POST the report. Returns `Ok(())` on `204 No Content`.
    pub async fn send(self) -> Result<()> {
        let url = format!("{}/api/ci/{}/result", self.cfg.server, self.change_hash);
        let body = json!({
            "status":  self.status,
            "details": Value::Object(self.details),
        });

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.cfg.token)
            .json(&body)
            .send()
            .await
            .map_err(Error::Http)?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        let truncated = body.chars().take(400).collect::<String>();
        Err(Error::Report {
            status: status.as_u16(),
            body: truncated,
        })
    }
}
