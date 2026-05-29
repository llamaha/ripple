//! # ripple
//!
//! A small SDK for writing CI runners against a [patchwave] server.
//! A runner is a regular program that:
//!
//! 1. Subscribes to a patchwave SSE event stream.
//! 2. Does one job — `cargo test`, `terraform plan`, ship a deploy.
//! 3. POSTs the result back to `/api/ci/{change_hash}/result`.
//!
//! The SDK wraps the SSE plumbing, the `atomic clone` shellout, and
//! the result-reporting POST so the user code is just "on this event,
//! do this work, return pass/fail".
//!
//! ```ignore
//! use ripple::{Runner, Event};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     Runner::from_env()?
//!         .on(Event::TagCreated, |ctx| async move {
//!             let work = ctx.checkout().await?;
//!             let ok = ctx.run("cargo test --quiet", &work).await?;
//!             ctx.report(if ok { "pass" } else { "fail" }).send().await
//!         })
//!         .run()
//!         .await
//! }
//! ```
//!
//! See `plans/patchwave-runner.md` in the workspace for the live
//! roadmap.
//!
//! [patchwave]: https://github.com/llamaha/patchwave

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod checkout;
pub mod config;
pub mod error;
pub mod event;
pub mod report;
pub mod runner;
pub mod sse;

pub use checkout::RepoCheckout;
pub use config::Config;
pub use error::{Error, Result};
pub use event::Event;
pub use report::Reporter;
pub use runner::{Runner, RunnerContext};
