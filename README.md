# ripple

Purpose-built CI runners for [patchwave]. No YAML DSL — each runner
is a small program in a real language that subscribes to patchwave
events, does one job, and reports back.

```
patchwave event  ──→  ripple runner  ──→  POST /api/ci/{hash}/result
(tag, push, intent)    (your binary)        (badge flips, intent advances)
```

## Status

**Phase 1 scaffold.** Public API surface and module structure are in
place; the SSE loop is still a stub. See
[`plans/patchwave-runner.md`](https://github.com/llamaha/patchwave/blob/main/plans/patchwave-runner.md)
in the patchwave workspace for the live roadmap. Not yet usable as
shipped; the reporting half (POST `/api/ci/{hash}/result`) is the
solid piece and works today.

## Why

Standard CI configuration languages accumulate the shape of
programming languages — variable binding, conditional execution,
inheritance, anchor merges — without becoming good ones. A
purpose-built runner is just a program. Real types, real debugger,
real test loop, real refactoring.

- **Any language, any tool.** Rust, Go, Python, Bun, plain bash.
  Terraform, Ansible, Pulumi, Nix, Bazel — if it has a CLI, you
  can drive it.
- **AI-friendly.** A 50-line runner is a one-turn task for an
  AI assistant. The configuration *is* regular code.
- **Composable.** Several runners against the same event, each doing
  one thing. They report independently.
- **No inbound port.** SSE subscribe + outbound POST. Runs on a
  laptop, VPS, homelab Pi, container job — anywhere with outbound
  HTTPS to your patchwave server.

## A 50-line runner

```rust
use ripple::{event::EventKind, Runner};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    Runner::from_env()?
        .on(EventKind::ChangePushed, |ctx| async move {
            let checkout = ctx.checkout().await?;
            let ok = checkout.run("cargo test --quiet").await?;
            ctx.report(if ok { "pass" } else { "fail" })
                .summary("cargo test")
                .send()
                .await
        })
        .run()
        .await?;
    Ok(())
}
```

Wired into a binary, it's `crates/example-cargo-test/src/main.rs` in
this repo. Configure via env:

```bash
export PATCHWAVE_URL=https://patchwave.example.com
export PATCHWAVE_TOKEN=<api-token>
export PATCHWAVE_RUNNER_NAME=cargo-test-runner
cargo run -p example-cargo-test
```

## Layout

```
ripple/
├── Cargo.toml                       workspace
├── crates/
│   ├── ripple/                      the SDK
│   └── example-cargo-test/          the reference binary
└── plans → see patchwave workspace
```

The SDK currently exposes:

| Module | Purpose |
|--------|---------|
| `config`   | Env-driven runtime config (`PATCHWAVE_URL`, `PATCHWAVE_TOKEN`, …) |
| `sse`      | Long-lived `GET /api/streams/discuss` subscriber (stub — Phase 2) |
| `event`    | Typed event enum (`ChangePushed`, `TagCreated`, `IntentApproved`, …) |
| `checkout` | `RepoCheckout` — wraps `atomic clone` / `atomic pull` |
| `report`   | `Reporter` builder — POSTs to `/api/ci/{hash}/result` |
| `runner`   | `Runner::from_env().on(kind, handler).run()` |

## Trade-offs

Be honest about what this design gives up:

- **No reusable-action marketplace.** Replacing that with 8 lines of
  Rust is the deliberate choice.
- **No multi-host fan-out.** Spawn handlers concurrently in one
  process; spread across machines with whatever scheduler you like.
- **No artifact storage.** Ship artifacts to S3, a registry,
  wherever, from inside your runner.
- **No caching layer.** Re-clone or `atomic pull` an existing
  checkout — that's it.

The thing this design does well is make the *config layer* into
actual code. The infrastructure side stays as simple or as complex
as you choose to make it.

## Building

```bash
cargo build --workspace
```

Rust 1.75+. No nightly. No build scripts. No proc-macro tricks
beyond `serde_derive`.

## Licence

Intended to ship dual-licensed under MIT or Apache-2.0 (matches the
broader patchwave ecosystem). Licence files to be added before the
first tagged release.

[patchwave]: https://github.com/llamaha/patchwave
