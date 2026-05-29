//! Error type for the SDK. Most public APIs return [`Result`].

use thiserror::Error;

/// SDK result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by the SDK.
#[derive(Debug, Error)]
pub enum Error {
    /// A required environment variable was missing or malformed.
    #[error("missing or invalid env var: {0}")]
    Env(String),

    /// HTTP transport failure (network, TLS, body).
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON serialisation / deserialisation failure.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    /// A VCS-layer operation failed (sync protocol, change store, or
    /// working-copy materialise).
    #[error("vcs: {0}")]
    Vcs(String),

    /// User-provided runner handler returned an error.
    #[error("handler: {0}")]
    Handler(#[from] anyhow::Error),

    /// I/O failure (filesystem, process spawn, etc.).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Server returned a non-success status to a report POST.
    #[error("patchwave returned {status}: {body}")]
    Report {
        /// HTTP status code.
        status: u16,
        /// Truncated response body for diagnostics.
        body: String,
    },
}
