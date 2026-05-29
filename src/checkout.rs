//! Repo checkout. Drives the patchwave sync protocol directly via
//! `atomic-remote::HttpRemote` and materialises the working copy via
//! `atomic-repository::Repository`. No `atomic` CLI shellout.
//!
//! The runner workflow is typically:
//!
//! 1. An event arrives over SSE with `(owner, repo, view, change_hash)`.
//! 2. The runner calls [`RepoCheckout::clone`] into a tmp dir.
//! 3. The runner shells out to whatever build tool it cares about via
//!    [`RepoCheckout::run`], inspects the result, then POSTs back.
//!
//! `clone` produces a fresh directory; [`RepoCheckout::pull`] refreshes
//! an existing one for runners that keep a warm cache.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Duration;

use atomic_core::change::Change;
use atomic_core::types::{Base32, Hash};
use atomic_remote::{HttpRemote, HttpRemoteConfig};
use atomic_repository::{InsertOptions, Repository};
use bytes::Bytes;
use tokio::process::Command;
use tokio::task;

use crate::config::Config;
use crate::error::{Error, Result};

/// HTTP timeout for sync operations. 60s is enough for the head-of-graph
/// state and changelist queries; large change downloads stream so they
/// don't really care about this value.
const SYNC_TIMEOUT_SECS: u64 = 60;

/// A working copy of a patchwave repo on disk.
#[derive(Debug, Clone)]
pub struct RepoCheckout {
    /// Owner slug.
    pub owner: String,
    /// Repo slug.
    pub repo: String,
    /// View this checkout was synced from. `pull` resyncs the same view.
    pub view: String,
    /// On-disk path of the working copy.
    pub path: PathBuf,
}

impl RepoCheckout {
    /// Clone `<server>/api/repos/{owner}/{repo}/sync` at `view` into a
    /// fresh directory under `cfg.workspace`. The directory name embeds
    /// the change hash (if provided) so concurrent events don't race
    /// on the same path.
    pub async fn clone(
        cfg: &Config,
        owner: &str,
        repo: &str,
        view: &str,
        change_hash: Option<&str>,
    ) -> Result<Self> {
        let dir_name = match change_hash {
            Some(h) => format!("{}-{}-{}", owner, repo, &h[..h.len().min(12)]),
            None => format!("{}-{}", owner, repo),
        };
        let path = cfg.workspace.join(&dir_name);

        if path.exists() {
            tokio::fs::remove_dir_all(&path).await.map_err(Error::Io)?;
        }
        tokio::fs::create_dir_all(&path).await.map_err(Error::Io)?;

        let url = sync_url(&cfg.server, owner, repo);
        let remote = build_remote(&url, &cfg.token)?;

        let entries = remote
            .get_changelist(view, 0)
            .await
            .map_err(|e| Error::Vcs(format!("get_changelist {view}: {e}")))?;

        let mut downloads: Vec<(Hash, Bytes)> = Vec::with_capacity(entries.len());
        for entry in &entries {
            let hash = Hash::from_base32(entry.hash.as_bytes())
                .ok_or_else(|| Error::Vcs(format!("bad hash from server: {}", entry.hash)))?;
            let bytes = remote
                .download_change(&entry.hash)
                .await
                .map_err(|e| Error::Vcs(format!("download_change {}: {e}", entry.hash)))?;
            downloads.push((hash, bytes));
        }

        let target = path.clone();
        let url_for_remote = url.clone();
        task::spawn_blocking(move || -> Result<()> {
            let repo = Repository::init(&target)
                .map_err(|e| Error::Vcs(format!("Repository::init: {e}")))?;
            apply_downloads(&repo, &downloads)?;
            // Best-effort: record the remote so a subsequent `atomic pull`
            // from the working copy still works. Failure here doesn't
            // invalidate the checkout.
            let _ = repo.add_remote_default("origin", &url_for_remote);
            Ok(())
        })
        .await
        .map_err(|e| Error::Vcs(format!("spawn_blocking: {e}")))??;

        Ok(Self {
            owner: owner.into(),
            repo: repo.into(),
            view: view.into(),
            path,
        })
    }

    /// Refresh this checkout from the server. Useful for runners that
    /// keep a long-lived working copy across many events instead of
    /// cloning per event.
    ///
    /// Already-applied changes are skipped. The local working copy is
    /// re-materialised at the end so any new files land on disk.
    pub async fn pull(&self, cfg: &Config) -> Result<()> {
        let url = sync_url(&cfg.server, &self.owner, &self.repo);
        let remote = build_remote(&url, &cfg.token)?;

        let entries = remote
            .get_changelist(&self.view, 0)
            .await
            .map_err(|e| Error::Vcs(format!("get_changelist {}: {e}", self.view)))?;

        // Dedup downloads against what's already in the local store.
        // We open the repo once on the blocking pool to read `has_change`,
        // then again to apply. Two opens is cheap; threading the same
        // `Repository` across the await would force it to be `Send`
        // and is not worth the constraint.
        let path = self.path.clone();
        let hashes: Vec<Hash> = entries
            .iter()
            .filter_map(|e| Hash::from_base32(e.hash.as_bytes()))
            .collect();

        let known_path = path.clone();
        let known_hashes = hashes.clone();
        let missing: Vec<Hash> = task::spawn_blocking(move || -> Result<Vec<Hash>> {
            let repo = Repository::open(&known_path)
                .map_err(|e| Error::Vcs(format!("Repository::open: {e}")))?;
            Ok(known_hashes
                .into_iter()
                .filter(|h| !repo.has_change(h))
                .collect())
        })
        .await
        .map_err(|e| Error::Vcs(format!("spawn_blocking: {e}")))??;

        let mut downloads: Vec<(Hash, Bytes)> = Vec::with_capacity(missing.len());
        for hash in &missing {
            let hash_b32 = hash.to_base32();
            let bytes = remote
                .download_change(&hash_b32)
                .await
                .map_err(|e| Error::Vcs(format!("download_change {hash_b32}: {e}")))?;
            downloads.push((*hash, bytes));
        }

        task::spawn_blocking(move || -> Result<()> {
            let repo = Repository::open(&path)
                .map_err(|e| Error::Vcs(format!("Repository::open: {e}")))?;
            apply_downloads(&repo, &downloads)?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Vcs(format!("spawn_blocking: {e}")))??;

        Ok(())
    }

    /// Run a shell command inside the checkout. Returns `true` on exit
    /// code 0.
    pub async fn run(&self, cmd: &str) -> Result<bool> {
        self.run_in(&self.path, cmd).await
    }

    /// Run a shell command in an explicit directory.
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

    /// Run a shell command inside the checkout, capturing stdout + stderr
    /// as one interleaved string. Returns `(success, combined_output)`.
    /// Useful for surfacing build/test output in the CI report `details`.
    pub async fn run_capture(&self, cmd: &str) -> Result<(bool, String)> {
        self.run_capture_in(&self.path, cmd).await
    }

    /// `run_capture` against an explicit directory.
    pub async fn run_capture_in(&self, dir: &Path, cmd: &str) -> Result<(bool, String)> {
        let output = Command::new("sh")
            .arg("-c")
            // 2>&1 merges stderr into stdout in shell-execution order so the
            // log reads the way a human running the command interactively
            // would expect.
            .arg(format!("({cmd}) 2>&1"))
            .current_dir(dir)
            .output()
            .await
            .map_err(Error::Io)?;
        let combined = String::from_utf8_lossy(&output.stdout).into_owned();
        Ok((output.status.success(), combined))
    }
}

// ── helpers ──────────────────────────────────────────────────────────

fn sync_url(server: &str, owner: &str, repo: &str) -> String {
    format!("{server}/api/repos/{owner}/{repo}/sync")
}

fn build_remote(url: &str, token: &str) -> Result<HttpRemote> {
    let config = HttpRemoteConfig::new()
        .with_timeout(Duration::from_secs(SYNC_TIMEOUT_SECS))
        .with_header("Authorization", format!("Bearer {token}"));
    HttpRemote::with_config(url, config)
        .map_err(|e| Error::Vcs(format!("HttpRemote::with_config: {e}")))
}

/// Decode each downloaded change, verify its hash, save it to the
/// change store, then insert all of them into the view in changelist
/// order and materialise the working copy.
///
/// `downloads` is in the same order the server returned the changelist,
/// which is also the topological order needed for `insert_change`.
fn apply_downloads(repo: &Repository, downloads: &[(Hash, Bytes)]) -> Result<()> {
    for (hash, bytes) in downloads {
        let mut cursor = Cursor::new(&bytes[..]);
        let (change, computed) = Change::deserialize(&mut cursor)
            .map_err(|e| Error::Vcs(format!("Change::deserialize: {e}")))?;
        if computed != *hash {
            return Err(Error::Vcs(format!(
                "hash mismatch: expected {}, got {}",
                hash.to_base32(),
                computed.to_base32()
            )));
        }
        repo.save_change(&change)
            .map_err(|e| Error::Vcs(format!("save_change: {e}")))?;
    }

    let opts = InsertOptions::default();
    for (hash, _) in downloads {
        if let Err(e) = repo.insert_change(hash, opts.clone()) {
            let msg = e.to_string();
            if !(msg.contains("already applied") || msg.contains("AlreadyApplied")) {
                return Err(Error::Vcs(format!(
                    "insert_change {}: {e}",
                    hash.to_base32()
                )));
            }
        }
    }

    repo.materialize()
        .map_err(|e| Error::Vcs(format!("materialize: {e}")))?;
    Ok(())
}
