//! Runtime configuration. A runner reads these from the environment:
//!
//! Required:
//! - `PATCHWAVE_URL` — base URL of the patchwave server (no trailing slash).
//! - `PATCHWAVE_TOKEN` — API token with push access to the target repos.
//!
//! Optional:
//! - `PATCHWAVE_RUNNER_NAME` — human label shown in the dashboard;
//!   also surfaced as `details.provider` on the CI badge. Defaults to
//!   the token subject server-side.
//! - `PATCHWAVE_RUNNER_INSTANCE` — stable identifier for a single
//!   runner process. Two runners with the same `name` from the same
//!   host should set distinct instances. Defaults absent.
//! - `PATCHWAVE_RUNNER_VERSION` — version string surfaced in the
//!   dashboard. Defaults to the ripple SDK's compile-time version.
//! - `PATCHWAVE_RUNNER_ROLE` — free-form role label
//!   (e.g. `ripple-cargo-test`, `custom-ci`).
//! - `PATCHWAVE_RUNNER_HOSTNAME` — runner-supplied hostname. The
//!   server also captures the source IP independently.
//! - `PATCHWAVE_RUNNER_REPOS` — comma-separated `owner/repo` list to
//!   subscribe to. Absent means "every repo the token can push to".
//!   Each entry must be in the token's access set or the server
//!   rejects the connection with 400.
//! - `PATCHWAVE_RUNNER_WORKSPACE` — scratch dir for checkouts.

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
    /// Optional provider chip name (`details.provider`) and dashboard label.
    pub runner_name: Option<String>,
    /// Optional stable identifier for this runner process.
    pub runner_instance: Option<String>,
    /// Runner version. Defaults to the ripple SDK's `CARGO_PKG_VERSION`
    /// so dashboards always have something to show.
    pub runner_version: Option<String>,
    /// Optional free-form role (`ripple`, `custom-ci`, …).
    pub runner_role: Option<String>,
    /// Optional runner-supplied hostname.
    pub runner_hostname: Option<String>,
    /// Optional explicit repo subscription. Each entry is `owner/repo`.
    pub runner_repos: Option<Vec<String>>,
    /// Optional scratch dir; defaults to `std::env::temp_dir()`.
    pub workspace: PathBuf,
}

/// SDK version baked in at compile time. Sent as `?version=` when the
/// runner doesn't override via `PATCHWAVE_RUNNER_VERSION`.
const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

impl Config {
    /// Read configuration from the process environment.
    pub fn from_env() -> Result<Self> {
        let server = env::var("PATCHWAVE_URL")
            .map_err(|_| Error::Env("PATCHWAVE_URL".into()))?
            .trim_end_matches('/')
            .to_string();
        let token = env::var("PATCHWAVE_TOKEN")
            .map_err(|_| Error::Env("PATCHWAVE_TOKEN".into()))?;

        let opt = |k: &str| env::var(k).ok().filter(|s| !s.trim().is_empty());

        let runner_name     = opt("PATCHWAVE_RUNNER_NAME");
        let runner_instance = opt("PATCHWAVE_RUNNER_INSTANCE");
        let runner_version  = opt("PATCHWAVE_RUNNER_VERSION")
            .or_else(|| Some(SDK_VERSION.to_string()));
        let runner_role     = opt("PATCHWAVE_RUNNER_ROLE");
        let runner_hostname = opt("PATCHWAVE_RUNNER_HOSTNAME");
        let runner_repos = opt("PATCHWAVE_RUNNER_REPOS").map(|csv| {
            csv.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        });

        let workspace = env::var("PATCHWAVE_RUNNER_WORKSPACE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| env::temp_dir());

        Ok(Self {
            server,
            token,
            runner_name,
            runner_instance,
            runner_version,
            runner_role,
            runner_hostname,
            runner_repos,
            workspace,
        })
    }
}
