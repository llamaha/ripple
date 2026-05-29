//! Runner builder + event dispatch loop.
//!
//! Phase 1 scaffolds the public surface. The actual SSE consumption
//! + handler dispatch lands in Phase 2 (see
//! `plans/patchwave-runner.md`).

use crate::checkout::RepoCheckout;
use crate::config::Config;
use crate::error::Result;
use crate::event::{Event, EventKind};
use crate::report::{Reporter, ReportStatus};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type HandlerFn =
    Arc<dyn Fn(RunnerContext) -> HandlerFuture + Send + Sync + 'static>;

/// Top-level runner builder. Construct via [`Runner::from_env`], chain
/// `.on(...)` handlers, then `.run()` the event loop.
pub struct Runner {
    cfg: Config,
    client: reqwest::Client,
    handlers: HashMap<EventKind, HandlerFn>,
    repo_filter: Option<(String, String)>,
}

impl Runner {
    /// Construct from `PATCHWAVE_URL` + `PATCHWAVE_TOKEN` env.
    pub fn from_env() -> Result<Self> {
        let cfg = Config::from_env()?;
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
    /// per kind — register one handler per kind per runner binary.
    pub fn on<F, Fut>(mut self, kind: EventKind, handler: F) -> Self
    where
        F: Fn(RunnerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers.insert(
            kind,
            Arc::new(move |ctx| Box::pin(handler(ctx))),
        );
        self
    }

    /// Drive the event loop. Returns when the SSE stream ends and
    /// reconnect retries are exhausted (Phase 2 detail).
    pub async fn run(self) -> Result<()> {
        // Phase 2: open SSE via `crate::sse::subscribe`, parse
        // payloads into Event, dispatch to handlers via
        // tokio::spawn. For Phase 1 this is a NOOP that keeps the
        // surface stable.
        let _ = (self.cfg, self.client, self.handlers, self.repo_filter);
        Ok(())
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
    pub async fn checkout(&self) -> Result<RepoCheckout> {
        let (owner, repo) = repo_from_event(&self.event)
            .ok_or_else(|| crate::Error::Env("event carries no repo coords".into()))?;
        RepoCheckout::clone(&self.cfg, owner, repo, self.event.change_hash()).await
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

fn repo_from_event(ev: &Event) -> Option<(&str, &str)> {
    match ev {
        Event::ChangePushed(p) => Some((&p.owner, &p.repo)),
        Event::TagCreated(p) => Some((&p.owner, &p.repo)),
        Event::IntentApproved(p) => Some((&p.owner, &p.repo)),
        Event::IntentCiPending(p) => Some((&p.owner, &p.repo)),
        Event::Other => None,
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
