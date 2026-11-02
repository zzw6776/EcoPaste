//! Tracks attachment readiness independently while retaining each item's causal event order.

use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::Result;

use super::{
    crypto,
    model::{DecryptedSyncEvent, StoredBlob, StoredSyncEvent},
};

struct PendingEvent {
    stored: StoredSyncEvent,
    sync_key: String,
    required: HashSet<String>,
    complete: bool,
    failed: bool,
}

pub(super) struct TransferQueue {
    events: Vec<PendingEvent>,
    waiting: VecDeque<StoredBlob>,
    started: HashSet<String>,
    stored: HashSet<String>,
    failures: Vec<(String, String)>,
}

impl TransferQueue {
    /// Resolves content dependencies from authenticated envelopes, including state-only events.
    pub fn new(
        events: Vec<StoredSyncEvent>,
        blobs: Vec<StoredBlob>,
        key: &[u8; 32],
        require_source_icons: bool,
    ) -> Result<Self> {
        let available: HashMap<_, _> = blobs
            .into_iter()
            .map(|blob| (blob.blob_id.clone(), blob))
            .collect();
        let mut waiting = VecDeque::new();
        let mut queued = HashSet::new();
        let mut pending = Vec::new();
        let mut failures = Vec::new();
        for stored in events {
            let decrypted = crypto::decrypt_sync_event(key, &stored.event)?;
            let sync_key = decrypted.sync_key().to_owned();
            let mut required = HashSet::new();
            if let DecryptedSyncEvent::Clipboard(envelope) = decrypted {
                for blob in &envelope.blobs {
                    required.insert(blob.blob_id.clone());
                }
                if require_source_icons {
                    if let Some(icon) = envelope.source_app.and_then(|app| app.icon) {
                        required.insert(icon.blob_id);
                    }
                }
            }
            let mut failed = false;
            for blob_id in &required {
                match available.get(blob_id) {
                    Some(blob) => {
                        if queued.insert(blob_id.clone()) {
                            waiting.push_back(blob.clone());
                        }
                    }
                    None => failed = true,
                }
            }
            if failed {
                failures.push((
                    stored.event.event_id.clone(),
                    "missing local attachment for sync event".to_owned(),
                ));
            }
            pending.push(PendingEvent {
                stored,
                sync_key,
                required,
                complete: false,
                failed,
            });
        }
        Ok(Self {
            events: pending,
            waiting,
            started: HashSet::new(),
            stored: HashSet::new(),
            failures,
        })
    }

    pub fn take_failures(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.failures)
    }

    /// Replenishes a freed transfer slot immediately, without waiting for another attachment.
    pub fn next_blob(&mut self) -> Option<StoredBlob> {
        while let Some(blob) = self.waiting.pop_front() {
            if !self.events.iter().any(|event| {
                !event.failed && !event.complete && event.required.contains(&blob.blob_id)
            }) {
                continue;
            }
            self.started.insert(blob.blob_id.clone());
            return Some(blob);
        }
        None
    }

    pub fn blob_stored(&mut self, blob_id: String) {
        self.stored.insert(blob_id);
    }

    pub fn events_for_blob(&self, blob_id: &str) -> Vec<String> {
        self.events
            .iter()
            .filter(|event| !event.failed && !event.complete && event.required.contains(blob_id))
            .map(|event| event.stored.event.event_id.clone())
            .collect()
    }

    /// Fails only records requiring this attachment; unrelated uploads remain runnable.
    pub fn fail_blob(&mut self, blob_id: &str) -> Vec<String> {
        let ids = self.events_for_blob(blob_id);
        self.fail_events(&ids);
        ids
    }

    pub fn fail_events(&mut self, ids: &[String]) {
        for event in &mut self.events {
            if ids.contains(&event.stored.event.event_id) {
                event.failed = true;
            }
        }
    }

    /// Selects ready records across independent keys, holding updates/deletes behind earlier content.
    pub fn ready_events(&self) -> Vec<StoredSyncEvent> {
        let mut blocked_keys = HashSet::new();
        self.events
            .iter()
            .filter_map(|event| {
                if event.complete || !blocked_keys.insert(&event.sync_key) {
                    return None;
                }
                (!event.failed && event.required.is_subset(&self.stored))
                    .then(|| event.stored.clone())
            })
            .collect()
    }

    pub fn confirm_events(&mut self, ids: &[String]) {
        for event in &mut self.events {
            if ids.contains(&event.stored.event.event_id) {
                event.complete = true;
            }
        }
    }

    pub fn has_pending(&self) -> bool {
        self.events.iter().any(|event| !event.complete)
    }

    pub fn active_events(&self) -> Vec<String> {
        self.events
            .iter()
            .filter(|event| {
                !event.failed && !event.complete && !event.required.is_disjoint(&self.started)
            })
            .map(|event| event.stored.event.event_id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::model::{BlobManifest, BlobRole, ClipboardEnvelope, SyncedClipboardItem};

    const KEY: [u8; 32] = [7; 32];

    fn event(id: &str, key: &str, blobs: &[&str]) -> StoredSyncEvent {
        let envelope = ClipboardEnvelope {
            version: 1,
            item: SyncedClipboardItem {
                kind: "files".into(),
                sub_kind: None,
                content: "file".into(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(4),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "android".into(),
                created_at_ms: 1,
                sync_key: key.into(),
                updated_at_ms: Some(1),
                source_revision: None,
            },
            blobs: blobs
                .iter()
                .map(|id| BlobManifest {
                    blob_id: (*id).into(),
                    name: "file".into(),
                    original_size: 4,
                    encrypted_size: 20,
                    role: BlobRole::File,
                    file_index: Some(0),
                    is_directory_archive: false,
                })
                .collect(),
            source_app: None,
        };
        StoredSyncEvent {
            cursor: 1,
            event: crypto::encrypt_event(&KEY, id.into(), "origin".into(), 1, 1, &envelope)
                .unwrap(),
        }
    }

    fn blobs(ids: &[&str]) -> Vec<StoredBlob> {
        ids.iter()
            .map(|id| StoredBlob {
                blob_id: (*id).into(),
                encrypted_path: (*id).into(),
                size: 20,
            })
            .collect()
    }

    #[test]
    fn ready_record_does_not_wait_for_unrelated_stalled_attachment() {
        let mut queue = TransferQueue::new(
            vec![
                event("large", "a", &["slow"]),
                event("small", "b", &["fast"]),
                event("next", "c", &["next"]),
            ],
            blobs(&["slow", "fast", "next"]),
            &KEY,
            false,
        )
        .unwrap();
        assert_eq!(queue.next_blob().unwrap().blob_id, "slow");
        assert_eq!(queue.next_blob().unwrap().blob_id, "fast");
        queue.blob_stored("fast".into());
        assert_eq!(
            queue
                .ready_events()
                .iter()
                .map(|e| e.event.event_id.as_str())
                .collect::<Vec<_>>(),
            ["small"]
        );
        // A completed stream frees its slot while the other stream is still stalled.
        assert_eq!(queue.next_blob().unwrap().blob_id, "next");
        queue.confirm_events(&["small".into()]);
        assert!(queue.has_pending());
        assert!(queue.ready_events().is_empty());
    }

    #[test]
    fn failure_does_not_cancel_independent_events_or_release_a_later_delete() {
        let content = event("content", "same", &["broken"]);
        let patch = crate::sync::model::SyncedItemStatePatch {
            sync_key: "same".into(),
            is_present: Some(false),
            is_favorite: None,
            is_pinned: None,
            note: None,
            note_changed: false,
        };
        let deletion = StoredSyncEvent {
            cursor: 2,
            event: crypto::encrypt_item_state(&KEY, "delete".into(), "origin".into(), 2, 2, &patch)
                .unwrap(),
        };
        let mut queue = TransferQueue::new(
            vec![content, deletion, event("healthy", "other", &["good"])],
            blobs(&["broken", "good"]),
            &KEY,
            false,
        )
        .unwrap();
        assert_eq!(queue.fail_blob("broken"), ["content"]);
        queue.blob_stored("good".into());
        let ready = queue.ready_events();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].event.event_id, "healthy");
        queue.confirm_events(&["healthy".into()]);
        assert!(queue.ready_events().is_empty());
    }

    #[test]
    fn one_record_requires_all_its_attachments_and_updates_wait_for_confirmation() {
        let mut queue = TransferQueue::new(
            vec![
                event("content", "same", &["one", "two"]),
                event("reuse", "same", &[]),
            ],
            blobs(&["one", "two"]),
            &KEY,
            false,
        )
        .unwrap();
        queue.blob_stored("one".into());
        assert!(queue.ready_events().is_empty());
        queue.blob_stored("two".into());
        assert_eq!(
            queue
                .ready_events()
                .iter()
                .map(|e| e.event.event_id.as_str())
                .collect::<Vec<_>>(),
            ["content"]
        );
        queue.confirm_events(&["content".into()]);
        assert_eq!(queue.ready_events()[0].event.event_id, "reuse");
    }

    #[test]
    fn shared_blobs_are_transferred_once_and_missing_blobs_fail_only_dependents() {
        let mut queue = TransferQueue::new(
            vec![
                event("first", "a", &["shared"]),
                event("second", "b", &["shared"]),
                event("missing", "c", &["absent"]),
            ],
            blobs(&["shared"]),
            &KEY,
            false,
        )
        .unwrap();
        assert_eq!(queue.take_failures().len(), 1);
        assert_eq!(queue.next_blob().unwrap().blob_id, "shared");
        assert!(queue.next_blob().is_none());
        queue.blob_stored("shared".into());
        assert_eq!(queue.ready_events().len(), 2);
        queue.confirm_events(&["first".into(), "second".into()]);
        assert!(queue.ready_events().is_empty());
    }
}
