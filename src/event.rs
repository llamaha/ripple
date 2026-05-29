//! Patchwave runner events.
//!
//! The server emits events on `GET /api/streams/runners` (see
//! `patchwave/crates/patchwave/src/api/runners.rs`). The wire envelope
//! is:
//!
//! ```json
//! {
//!   "kind":    "change.pushed | tag.created | view.merged",
//!   "owner":   "...",
//!   "repo":    "...",
//!   "payload": { /* kind-specific */ }
//! }
//! ```
//!
//! Each [`Event`] variant maps to one envelope; the kind-specific
//! body lives under `payload`. Unrecognised kinds land in
//! [`Event::Other`] so future server kinds don't break existing
//! runners until the SDK catches up.

use serde::{Deserialize, Serialize};

/// One discrete patchwave runner event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Event {
    /// `atomic push` was accepted; a new change has been recorded.
    #[serde(rename = "change.pushed")]
    ChangePushed(ChangePushed),
    /// A tag was created (typically a release marker).
    #[serde(rename = "tag.created")]
    TagCreated(TagCreated),
    /// A view was merged into another via `POST .../views/{from}/apply/{to}`.
    #[serde(rename = "view.merged")]
    ViewMerged(ViewMerged),
    /// Anything the SDK does not recognise — newer server kinds, etc.
    /// Payload is dropped; if you need it, upgrade the SDK.
    #[serde(other)]
    Other,
}

/// Common envelope coordinates. Every typed variant embeds these.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePushed {
    /// Repo owner slug.
    pub owner: String,
    /// Repo name slug.
    pub repo: String,
    /// Payload-specific fields.
    pub payload: ChangePushedPayload,
}

/// `change.pushed` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePushedPayload {
    /// Base32 change hash.
    pub change_hash: String,
    /// View the change landed on.
    pub view: String,
    /// Unix-seconds server-receive timestamp.
    pub pushed_at: i64,
}

/// `tag.created` envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagCreated {
    /// Repo owner slug.
    pub owner: String,
    /// Repo name slug.
    pub repo: String,
    /// Payload-specific fields.
    pub payload: TagCreatedPayload,
}

/// `tag.created` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagCreatedPayload {
    /// Tag name (e.g. `v1.2.3`).
    pub name: String,
    /// State hash the tag points at.
    pub state_hash: String,
    /// View the tag was created on.
    pub view: String,
}

/// `view.merged` envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewMerged {
    /// Repo owner slug.
    pub owner: String,
    /// Repo name slug.
    pub repo: String,
    /// Payload-specific fields.
    pub payload: ViewMergedPayload,
}

/// `view.merged` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewMergedPayload {
    /// View the merge pulled changes from.
    pub from_view: String,
    /// View the merge applied changes to.
    pub to_view: String,
    /// Number of changes applied.
    pub applied: usize,
    /// Hash of the resulting head change.
    pub head: Option<String>,
}

/// Discriminator-only enum used by `Runner::on` to register handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// Matches [`Event::ChangePushed`].
    ChangePushed,
    /// Matches [`Event::TagCreated`].
    TagCreated,
    /// Matches [`Event::ViewMerged`].
    ViewMerged,
}

impl Event {
    /// Discriminator. Returns `None` for [`Event::Other`].
    pub fn kind(&self) -> Option<EventKind> {
        Some(match self {
            Event::ChangePushed(_) => EventKind::ChangePushed,
            Event::TagCreated(_) => EventKind::TagCreated,
            Event::ViewMerged(_) => EventKind::ViewMerged,
            Event::Other => return None,
        })
    }

    /// `(owner, repo)` if the event carries them.
    pub fn coords(&self) -> Option<(&str, &str)> {
        match self {
            Event::ChangePushed(e) => Some((&e.owner, &e.repo)),
            Event::TagCreated(e) => Some((&e.owner, &e.repo)),
            Event::ViewMerged(e) => Some((&e.owner, &e.repo)),
            Event::Other => None,
        }
    }

    /// The change/state hash this event implies a CI run should target.
    /// `change.pushed` → the change hash; `tag.created` → the tagged
    /// state hash; `view.merged` → the resulting head change.
    pub fn change_hash(&self) -> Option<&str> {
        match self {
            Event::ChangePushed(e) => Some(&e.payload.change_hash),
            Event::TagCreated(e) => Some(&e.payload.state_hash),
            Event::ViewMerged(e) => e.payload.head.as_deref(),
            Event::Other => None,
        }
    }

    /// View this event happened on. `view.merged` returns the
    /// destination view (`to_view`).
    pub fn view(&self) -> Option<&str> {
        match self {
            Event::ChangePushed(e) => Some(&e.payload.view),
            Event::TagCreated(e) => Some(&e.payload.view),
            Event::ViewMerged(e) => Some(&e.payload.to_view),
            Event::Other => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_change_pushed_envelope() {
        let raw = r#"{
            "kind": "change.pushed",
            "owner": "alice",
            "repo": "demo",
            "payload": {
                "change_hash": "ABC",
                "view": "dev",
                "pushed_at": 1700000000
            }
        }"#;
        let ev: Event = serde_json::from_str(raw).unwrap();
        assert_eq!(ev.kind(), Some(EventKind::ChangePushed));
        assert_eq!(ev.coords(), Some(("alice", "demo")));
        assert_eq!(ev.change_hash(), Some("ABC"));
        assert_eq!(ev.view(), Some("dev"));
    }

    #[test]
    fn parses_tag_created_envelope() {
        let raw = r#"{
            "kind": "tag.created",
            "owner": "alice",
            "repo": "demo",
            "payload": { "name": "v1", "state_hash": "ST", "view": "main" }
        }"#;
        let ev: Event = serde_json::from_str(raw).unwrap();
        assert_eq!(ev.kind(), Some(EventKind::TagCreated));
        assert_eq!(ev.change_hash(), Some("ST"));
    }

    #[test]
    fn parses_view_merged_envelope() {
        let raw = r#"{
            "kind": "view.merged",
            "owner": "alice",
            "repo": "demo",
            "payload": {
                "from_view": "feat", "to_view": "dev",
                "applied": 3, "head": "H"
            }
        }"#;
        let ev: Event = serde_json::from_str(raw).unwrap();
        assert_eq!(ev.kind(), Some(EventKind::ViewMerged));
        assert_eq!(ev.view(), Some("dev"));
    }

    #[test]
    fn unknown_kind_becomes_other() {
        let raw = r#"{"kind":"not.a.real.kind","owner":"a","repo":"b","payload":{}}"#;
        let ev: Event = serde_json::from_str(raw).unwrap();
        assert!(matches!(ev, Event::Other));
        assert!(ev.kind().is_none());
    }
}
