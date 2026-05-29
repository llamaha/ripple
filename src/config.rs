//! Runtime configuration. A runner reads these from the environment:
//!
//! - `PATCHWAVE_URL` — base URL of the patchwave server (no trailing slash).
//! - `PATCHWAVE_TOKEN` — API token with push access to the target repos.
//! - `PATCHWAVE_RUNNER_NAME` — optional, surfaced as `details.provider`.
//! - `PATCHWAVE_RUNNER_WORKSPACE` — optional scratch dir for checkouts.

use crate::error::{Error, Result};
use std::env;
use std::path::PathBuf;

/// Runtime configuration loaded from the environment.
#[derive(Debug, Clone)]
pub struct Config {
    /// Patchwave server base URL (e.g. `https://patchwave.example.com`).
    pub server: String,
    /// Bearer token used for SSE subscribe + report POST.
    pub token: String,
    /// Optional provider chip name (`details.provider`).
    pub runner_name: Option<String>,
    /// Optional scratch dir; defaults to `std::env::temp_dir()`.
    pub workspace: PathBuf,
}

impl Config {
    /// Read configuration from the process environment.
    pub fn from_env() -> Result<Self> {
        let server = env::var("PATCHWAVE_URL")
            .map_err(|_| Error::Env("PATCHWAVE_URL".into()))?
            .trim_end_matches('/')
            .to_string();
        let token = env::var("PATCHWAVE_TOKEN")
            .map_err(|_| Error::Env("PATCHWAVE_TOKEN".into()))?;
        let runner_name = env::var("PATCHWAVE_RUNNER_NAME").ok();
        let workspace = env::var("PATCHWAVE_RUNNER_WORKSPACE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| env::temp_dir());

        Ok(Self { server, token, runner_name, workspace })
    }
}
