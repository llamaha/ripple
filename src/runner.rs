//! Runner builder + event dispatch loop.
//!
//! The user constructs a [`Runner`] via [`Runner::from_env`], registers
//! handlers per [`EventKind`] with [`Runner::on`], and then calls
//! [`Runner::run`]. `run` opens an SSE connection to
//! `/api/streams/runners`, decodes each event, and spawns the matching
//! handler with a per-event [`RunnerContext`]. The loop reconnects with
//! exponential backoff on transport errors or clean stream closes.

use crate::checkout::RepoCheckout;
use crate::config::Config;
use crate::error::Result;
use crate::event::{Event, EventKind};
use crate::report::{ReportStatus, Reporter};
use crate::sse;

use futures::StreamExt as _;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type HandlerFn = Arc<dyn Fn(RunnerContext) -> HandlerFuture + Send + Sync + 'static>;

const RECONNECT_INITIAL: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// Top-level runner builder. Construct via [`Runner::from_env`], chain
/// `.on(...)` handlers, then `.run()` the event loop.
pub struct Runner {
    cfg: Arc<Config>,
    client: reqwest::Client,
    handlers: HashMap<EventKind, HandlerFn>,
    repo_filter: Option<(String, String)>,
}

impl Runner {
    /// Construct from `PATCHWAVE_URL` + `PATCHWAVE_TOKEN` env.
    pub fn from_env() -> Result<Self> {
        let cfg = Arc::new(Config::from_env()?);
        let client = reqwest::Client::builder()
            .user_agent(concat!("ripple/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            cfg,
            client,
            handlers: HashMap::new(),
            repo_filter: None,
        })
    }

    /// Only dispatch events for one specific repo. Omit for "listen
    /// to everywhere your token can see".
    pub fn filter_repo(mut self, owner: impl Into<String>, repo: impl Into<String>) -> Self {
        self.repo_filter = Some((owner.into(), repo.into()));
        self
    }

    /// Register a handler for the given event kind. Last write wins
    /// per kind — one handler per kind per runner binary.
    pub fn on<F, Fut>(mut self, kind: EventKind, handler: F) -> Self
    where
        F: Fn(RunnerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers
            .insert(kind, Arc::new(move |ctx| Box::pin(handler(ctx))));
        self
    }

    /// Drive the event loop. Runs until cancelled (Ctrl-C / drop).
    /// Reconnects with exponential backoff on transport errors.
    pub async fn run(self) -> Result<()> {
        let mut backoff = RECONNECT_INITIAL;
        loop {
            match self.run_one_connection().await {
                Ok(()) => {
                    tracing::info!("ripple SSE: stream closed cleanly, reconnecting");
                    backoff = RECONNECT_INITIAL;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ripple SSE: error, reconnecting in {:?}", backoff);
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }

    /// Open one SSE connection, dispatch events until it closes, then
    /// return. The outer `run` decides whether to reconnect.
    async fn run_one_connection(&self) -> Result<()> {
        let mut stream = sse::subscribe(&self.cfg, &self.client).await?;
        tracing::info!("ripple SSE: connected to {}", sse::RUNNER_STREAM_PATH);

        while let Some(payload) = stream.next().await {
            let payload = match payload {
                Ok(p) => p,
                Err(e) => return Err(e),
            };
            self.dispatch_payload(&payload);
        }
        Ok(())
    }

    /// Decode one SSE `data:` payload and dispatch it to the matching
    /// handler if any. Decode failures and unmatched events are logged
    /// at debug level so they don't drown out real activity.
    fn dispatch_payload(&self, payload: &str) {
        let event: Event = match serde_json::from_str(payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::debug!(error = %e, payload = %payload, "ripple: undecodable SSE payload");
                return;
            }
        };

        let kind = match event.kind() {
            Some(k) => k,
            None => {
                tracing::debug!("ripple: unhandled event kind (Event::Other)");
                return;
            }
        };

        if let (Some((owner, repo)), Some((f_owner, f_repo))) =
            (event.coords(), self.repo_filter.as_ref().map(|(o, r)| (o.as_str(), r.as_str())))
        {
            if owner != f_owner || repo != f_repo {
                return;
            }
        }

        let Some(handler) = self.handlers.get(&kind).cloned() else {
            tracing::debug!(?kind, "ripple: no handler registered");
            return;
        };

        let ctx = RunnerContext {
            cfg: self.cfg.clone(),
            client: self.client.clone(),
            event,
        };

        tokio::spawn(async move {
            if let Err(e) = handler(ctx).await {
                tracing::error!(error = %e, "ripple: handler returned error");
            }
        });
    }
}

/// Per-event context handed to a user handler. Provides ergonomic
/// access to checkout + report helpers.
#[derive(Clone)]
pub struct RunnerContext {
    pub(crate) cfg: Arc<Config>,
    pub(crate) client: reqwest::Client,
    /// The event that triggered this handler invocation.
    pub event: Event,
}

impl RunnerContext {
    /// Clone the repo named in the event into the configured workspace.
    /// Falls back to view `"dev"` when the event payload doesn't carry
    /// one, matching patchwave's default.
    pub async fn checkout(&self) -> Result<RepoCheckout> {
        let (owner, repo) = self
            .event
            .coords()
            .ok_or_else(|| crate::Error::Env("event carries no repo coords".into()))?;
        let view = self.event.view().unwrap_or("dev");
        RepoCheckout::clone(&self.cfg, owner, repo, view, self.event.change_hash()).await
    }

    /// Build a Reporter for the change-hash this event implies.
    pub fn report(&self, status: &str) -> Reporter<'_> {
        Reporter::new(
            &self.cfg,
            &self.client,
            self.event.change_hash().unwrap_or_default(),
            parse_status(status),
        )
    }
}

fn parse_status(s: &str) -> ReportStatus {
    match s {
        "pass" => ReportStatus::Pass,
        "fail" => ReportStatus::Fail,
        "pending" => ReportStatus::Pending,
        "skipped" => ReportStatus::Skipped,
        other => ReportStatus::custom(other),
    }
}
