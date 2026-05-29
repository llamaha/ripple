//! Long-lived SSE subscriber for `GET /api/streams/discuss`.
//!
//! Phase 0 of `plans/patchwave-runner.md` pins the wire schema and
//! Phase 2 wires up the actual subscriber. For now this module is a
//! shape placeholder so the rest of the SDK can compile against it.

use crate::config::Config;
use crate::error::{Error, Result};

/// Open a long-lived SSE connection to the patchwave server and
/// return the underlying response. Caller is responsible for parsing
/// the byte stream until [Phase 2] lands a framer.
///
/// [Phase 2]: https://github.com/llamaha/patchwave/blob/main/plans/patchwave-runner.md
pub async fn subscribe(cfg: &Config, client: &reqwest::Client) -> Result<reqwest::Response> {
    let url = format!("{}/api/streams/discuss", cfg.server);
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
    Ok(resp)
}
