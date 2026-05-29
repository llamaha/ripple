//! Repo checkout helper. Wraps `atomic clone` / `atomic pull` into a
//! handful of typed methods.
//!
//! The bot already keeps long-lived clones for its KG features; a
//! runner is typically ephemeral, so the default behaviour is
//! "clone into a tmp dir, drop it on exit". A `pull` path exists for
//! warm-caching across many events.

use crate::config::Config;
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};
use tokio::process::Command;

/// A working copy of a patchwave repo on disk.
#[derive(Debug, Clone)]
pub struct RepoCheckout {
    /// Owner slug.
    pub owner: String,
    /// Repo slug.
    pub repo: String,
    /// On-disk path of the clone.
    pub path: PathBuf,
}

impl RepoCheckout {
    /// Clone `<server>/api/repos/{owner}/{repo}/sync` into a fresh
    /// directory under `cfg.workspace`. The directory name embeds
    /// the change hash (if provided) so multiple events don't race.
    pub async fn clone(
        cfg: &Config,
        owner: &str,
        repo: &str,
        change_hash: Option<&str>,
    ) -> Result<Self> {
        let dir_name = match change_hash {
            Some(h) => format!("{}-{}-{}", owner, repo, &h[..h.len().min(12)]),
            None => format!("{}-{}", owner, repo),
        };
        let path = cfg.workspace.join(&dir_name);

        if path.exists() {
            tokio::fs::remove_dir_all(&path).await.ok();
        }

        let url = format!("{}/api/repos/{}/{}/sync", cfg.server, owner, repo);
        let url_with_auth = url_with_basic_auth(&url, &cfg.token);

        let status = Command::new("atomic")
            .arg("clone")
            .arg(&url_with_auth)
            .arg(&path)
            .status()
            .await
            .map_err(Error::Io)?;

        if !status.success() {
            return Err(Error::Atomic(format!(
                "atomic clone exited with {status}"
            )));
        }

        Ok(Self {
            owner: owner.into(),
            repo: repo.into(),
            path,
        })
    }

    /// Run `atomic pull` inside this checkout.
    pub async fn pull(&self) -> Result<()> {
        let status = Command::new("atomic")
            .arg("pull")
            .current_dir(&self.path)
            .status()
            .await
            .map_err(Error::Io)?;
        if !status.success() {
            return Err(Error::Atomic(format!("atomic pull exited with {status}")));
        }
        Ok(())
    }

    /// Shell out a command inside the checkout. Returns `true` on
    /// exit code 0.
    pub async fn run(&self, cmd: &str) -> Result<bool> {
        self.run_in(&self.path, cmd).await
    }

    /// Shell out a command in an explicit directory.
    pub async fn run_in(&self, dir: &Path, cmd: &str) -> Result<bool> {
        let status = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .current_dir(dir)
            .status()
            .await
            .map_err(Error::Io)?;
        Ok(status.success())
    }
}

/// Embed `username:token` userinfo into the URL so the patchwave
/// CLI sends a Basic auth header. Patchwave's auth-ext accepts a
/// pubkey-registered user with an empty password.
fn url_with_basic_auth(url: &str, token: &str) -> String {
    // Trivial; for production we'd want to parse + reassemble with
    // url::Url. Phase 2 hardens this.
    match url.split_once("://") {
        Some((scheme, rest)) => format!("{scheme}://runner:{token}@{rest}"),
        None => url.to_string(),
    }
}
