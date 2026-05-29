//! Reference ripple runner.
//!
//! Listens for `change.pushed` events, clones the repo, runs
//! `cargo test --quiet` inside it, and reports pass/fail back to
//! patchwave.
//!
//! Configure via env:
//!
//! ```bash
//! export PATCHWAVE_URL=https://patchwave.example.com
//! export PATCHWAVE_TOKEN=<api-token>
//! export PATCHWAVE_RUNNER_NAME=cargo-test-runner   # optional
//! ```

use ripple::event::EventKind;
use ripple::Runner;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,ripple=debug".into()),
        )
        .init();

    info!("ripple-cargo-test starting");

    Runner::from_env()?
        .on(EventKind::ChangePushed, |ctx| async move {
            let checkout = ctx.checkout().await?;
            info!(
                owner = %checkout.owner,
                repo  = %checkout.repo,
                path  = %checkout.path.display(),
                "cloned, running cargo test"
            );

            let ok = checkout.run("cargo test --quiet").await?;

            ctx.report(if ok { "pass" } else { "fail" })
                .summary(if ok { "cargo test passed" } else { "cargo test failed" })
                .send()
                .await
        })
        .run()
        .await?;

    Ok(())
}
