//! Patchwave event types a runner can subscribe to.
//!
//! Today the SSE stream at `GET /api/streams/discuss` is shaped for
//! the discuss UI and tags non-chat events via `system_kind`. The
//! exact event-payload schema for runners is being pinned in
//! [Phase 0 of `plans/patchwave-runner.md`]. Until then this module
//! treats unrecognised payloads as `Event::Other` and keeps the
//! parser additive.

use serde::{Deserialize, Serialize};

/// One discrete patchwave event. Unrecognised payloads land in
/// [`Event::Other`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// `atomic push` was accepted; a new change has been recorded.
    ChangePushed(ChangePushed),
    /// A tag was created (typically a release marker).
    TagCreated(TagCreated),
    /// An intent transitioned to `executing` after review.
    IntentApproved(IntentApproved),
    /// An intent has entered `ci_pending` waiting for a runner.
    IntentCiPending(IntentCiPending),
    /// Anything the SDK does not recognise. Payload preserved as
    /// JSON so the runner can poke at it if it wants.
    #[serde(other)]
    Other,
}

/// Discriminator-only enum for `Runner::on` filters. Matches the
/// outer variant of [`Event`] without binding the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// Matches [`Event::ChangePushed`].
    ChangePushed,
    /// Matches [`Event::TagCreated`].
    TagCreated,
    /// Matches [`Event::IntentApproved`].
    IntentApproved,
    /// Matches [`Event::IntentCiPending`].
    IntentCiPending,
}

/// `change.pushed` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePushed {
    /// Repo owner slug.
    pub owner: String,
    /// Repo name slug.
    pub repo: String,
    /// Content-addressed change hash.
    pub change_hash: String,
    /// View the change landed on.
    pub view: Option<String>,
    /// Username of the pusher.
    pub pusher: Option<String>,
}

/// `tag.created` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagCreated {
    /// Repo owner slug.
    pub owner: String,
    /// Repo name slug.
    pub repo: String,
    /// Tag name (e.g. `v1.2.3`).
    pub tag: String,
    /// State hash the tag points at.
    pub state_hash: String,
}

/// `intent.approved` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentApproved {
    /// Repo owner slug.
    pub owner: String,
    /// Repo name slug.
    pub repo: String,
    /// Intent UUID.
    pub intent_id: String,
    /// Change hash the intent is linked to, if any.
    pub change_hash: Option<String>,
}

/// `intent.ci_pending` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentCiPending {
    /// Repo owner slug.
    pub owner: String,
    /// Repo name slug.
    pub repo: String,
    /// Intent UUID.
    pub intent_id: String,
    /// Change hash to report against.
    pub change_hash: String,
}

impl Event {
    /// Best-effort kind discriminator. Returns `None` for [`Event::Other`].
    pub fn kind(&self) -> Option<EventKind> {
        Some(match self {
            Event::ChangePushed(_) => EventKind::ChangePushed,
            Event::TagCreated(_) => EventKind::TagCreated,
            Event::IntentApproved(_) => EventKind::IntentApproved,
            Event::IntentCiPending(_) => EventKind::IntentCiPending,
            Event::Other => return None,
        })
    }

    /// The change hash this event implies a CI run should target, if any.
    pub fn change_hash(&self) -> Option<&str> {
        match self {
            Event::ChangePushed(p) => Some(&p.change_hash),
            Event::TagCreated(p) => Some(&p.state_hash),
            Event::IntentApproved(p) => p.change_hash.as_deref(),
            Event::IntentCiPending(p) => Some(&p.change_hash),
            Event::Other => None,
        }
    }
}
