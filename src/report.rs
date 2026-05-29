//! POST `/api/ci/{change_hash}/result` — flips the CI badge and
//! advances any linked intent.
//!
//! This piece is the most-tested half of the SDK: the endpoint
//! contract is verified by `patchwave/deploy/smoke-ci-reporting.sh`
//! against the production droplet, so the wire shape is solid.

use crate::config::Config;
use crate::error::{Error, Result};
use serde::Serialize;
use serde_json::{json, Map, Value};

/// Builder for a CI result POST. Construct via [`Reporter::new`] or
/// (more typically) via [`crate::RunnerContext::report`].
#[must_use = "Reporter does nothing until .send() is awaited"]
pub struct Reporter<'a> {
    cfg: &'a Config,
    client: &'a reqwest::Client,
    change_hash: String,
    status: ReportStatus,
    details: Map<String, Value>,
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
        }
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
