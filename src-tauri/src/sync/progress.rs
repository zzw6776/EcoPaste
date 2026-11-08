use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter};

pub const TRANSFER_UPDATED_EVENT: &str = "sync-transfer://updated";
const EMIT_INTERVAL: Duration = Duration::from_millis(500);
const MAX_RETAINED_TASKS: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TransferDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TransferRoute {
    Lan,
    Cloud,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TransferPhase {
    Queued,
    Preparing,
    Connecting,
    Transferring,
    Confirming,
    Completed,
    Failed,
    Canceled,
}

impl TransferPhase {
    fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Canceled)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferTask {
    pub id: String,
    pub operation_id: String,
    pub item_id: String,
    pub direction: TransferDirection,
    pub route: TransferRoute,
    pub target: String,
    pub file_name: Option<String>,
    pub phase: TransferPhase,
    pub transferred_bytes: u64,
    pub total_bytes: u64,
    pub operation_total_bytes: u64,
    pub speed_bytes_per_second: u64,
    pub elapsed_ms: u64,
    pub eta_seconds: Option<u64>,
    pub error: Option<String>,
    pub cancellable: bool,
}

struct TransferEntry {
    task: TransferTask,
    preparation: Option<PreparationContext>,
    cancel: Arc<AtomicBool>,
    started_at: Instant,
    last_sample_at: Instant,
    last_sample_bytes: u64,
    last_emit_at: Instant,
}

#[derive(Clone)]
struct PreparationContext {
    operation_order: i64,
    source_revision: String,
}

pub(super) struct CanceledPreparation {
    pub item_id: String,
    pub operation_order: i64,
    pub source_revision: String,
}

pub(super) enum PreparationClaim {
    Claimed(TransferHandle),
    Canceled,
    Missing,
}

pub(super) enum MetadataOnlyDeliveryStart {
    Tracked,
    Canceled,
    Untracked,
}

#[derive(Clone)]
pub(super) struct TransferRegistry {
    app: AppHandle,
    entries: Arc<Mutex<HashMap<String, TransferEntry>>>,
}

impl TransferRegistry {
    pub fn new(app: AppHandle) -> Self {
        Self {
            app,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn start(&self, task: TransferTask) -> TransferHandle {
        self.start_entry(task, None)
    }

    pub fn start_preparation(
        &self,
        task: TransferTask,
        operation_order: i64,
        source_revision: String,
    ) -> TransferHandle {
        self.start_entry(
            task,
            Some(PreparationContext {
                operation_order,
                source_revision,
            }),
        )
    }

    fn start_entry(
        &self,
        task: TransferTask,
        preparation: Option<PreparationContext>,
    ) -> TransferHandle {
        let cancel = Arc::new(AtomicBool::new(false));
        let id = task.id.clone();
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        if task.direction == TransferDirection::Download {
            entries.retain(|_, entry| {
                entry.task.item_id != task.item_id
                    || entry.task.direction != TransferDirection::Download
                    || !entry.task.phase.terminal()
            });
        }
        entries.insert(
            id.clone(),
            TransferEntry {
                task,
                preparation,
                cancel: cancel.clone(),
                started_at: now,
                last_sample_at: now,
                last_sample_bytes: 0,
                last_emit_at: now.checked_sub(EMIT_INTERVAL).unwrap_or(now),
            },
        );
        drop(entries);
        self.emit(&id, true);
        TransferHandle {
            id,
            cancel,
            registry: self.clone(),
        }
    }

    /// Converts the global preparation task into the first route-specific attachment task.
    pub fn claim_preparation(
        &self,
        operation_id: &str,
        route: TransferRoute,
        target: &str,
        total_bytes: u64,
        operation_total_bytes: u64,
    ) -> PreparationClaim {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some((id, entry)) = entries.iter_mut().find(|(_, entry)| {
            entry.preparation.is_some()
                && entry.task.operation_id == operation_id
                && entry.task.route == route
                && entry.task.target == target
        }) else {
            return PreparationClaim::Missing;
        };
        if entry.cancel.load(Ordering::Acquire) || entry.task.phase == TransferPhase::Canceled {
            return PreparationClaim::Canceled;
        }
        if entry.task.phase != TransferPhase::Queued || !entry.task.cancellable {
            return PreparationClaim::Missing;
        }
        let now = Instant::now();
        entry.preparation = None;
        entry.task.phase = TransferPhase::Queued;
        entry.task.transferred_bytes = 0;
        entry.task.total_bytes = total_bytes;
        entry.task.operation_total_bytes = operation_total_bytes;
        entry.task.speed_bytes_per_second = 0;
        entry.task.elapsed_ms = 0;
        entry.task.eta_seconds = None;
        entry.task.error = None;
        entry.task.cancellable = true;
        entry.started_at = now;
        entry.last_sample_at = now;
        entry.last_sample_bytes = 0;
        entry.last_emit_at = now.checked_sub(EMIT_INTERVAL).unwrap_or(now);
        let id = id.clone();
        let cancel = entry.cancel.clone();
        drop(entries);
        self.emit(&id, false);
        PreparationClaim::Claimed(TransferHandle {
            id,
            cancel,
            registry: self.clone(),
        })
    }

    /// Closes cancellation before a metadata-only event is handed to the transport.
    pub fn begin_metadata_only_delivery(
        &self,
        operation_id: &str,
        route: TransferRoute,
        target: &str,
    ) -> MetadataOnlyDeliveryStart {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some((id, entry)) = entries.iter_mut().find(|(_, entry)| {
            entry.preparation.is_some()
                && entry.task.operation_id == operation_id
                && entry.task.route == route
                && entry.task.target == target
        }) else {
            return MetadataOnlyDeliveryStart::Untracked;
        };
        if entry.cancel.load(Ordering::Acquire) || entry.task.phase == TransferPhase::Canceled {
            return MetadataOnlyDeliveryStart::Canceled;
        }
        if entry.task.phase != TransferPhase::Queued || !entry.task.cancellable {
            return MetadataOnlyDeliveryStart::Untracked;
        }
        entry.task.phase = TransferPhase::Confirming;
        entry.task.cancellable = false;
        let id = id.clone();
        drop(entries);
        self.emit(&id, false);
        MetadataOnlyDeliveryStart::Tracked
    }

    /// Restores cancellation after a metadata-only exchange fails or defers this event.
    pub fn retry_metadata_only_delivery(
        &self,
        operation_id: &str,
        route: TransferRoute,
        target: &str,
    ) {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some((id, entry)) = entries.iter_mut().find(|(_, entry)| {
            entry.preparation.is_some()
                && entry.task.operation_id == operation_id
                && entry.task.route == route
                && entry.task.target == target
                && entry.task.phase == TransferPhase::Confirming
                && !entry.cancel.load(Ordering::Acquire)
        }) else {
            return;
        };
        entry.task.phase = TransferPhase::Queued;
        entry.task.cancellable = true;
        let id = id.clone();
        drop(entries);
        self.emit(&id, false);
    }

    /// Completes a prepared upload whose receiver accepted only the authenticated metadata.
    pub fn complete_metadata_only_preparation(
        &self,
        operation_id: &str,
        route: TransferRoute,
        target: &str,
    ) -> bool {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some((id, entry)) = entries.iter_mut().find(|(_, entry)| {
            entry.preparation.is_some()
                && entry.task.operation_id == operation_id
                && entry.task.route == route
                && entry.task.target == target
                && matches!(
                    entry.task.phase,
                    TransferPhase::Queued | TransferPhase::Confirming
                )
                && !entry.cancel.load(Ordering::Acquire)
        }) else {
            return false;
        };
        complete_metadata_only_entry(entry);
        let id = id.clone();
        drop(entries);
        self.emit(&id, false);
        true
    }

    fn prepared(&self, id: &str, route: TransferRoute, target: String) {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some(entry) = entries.get_mut(id) else {
            return;
        };
        if entry.task.phase.terminal() || entry.cancel.load(Ordering::Acquire) {
            return;
        }
        entry.task.route = route;
        entry.task.target = target;
        entry.task.phase = TransferPhase::Queued;
        entry.task.transferred_bytes = 0;
        entry.task.speed_bytes_per_second = 0;
        entry.task.eta_seconds = None;
        entry.task.cancellable = true;
        entry.last_sample_bytes = 0;
        entry.last_sample_at = Instant::now();
        drop(entries);
        self.emit(id, false);
    }

    pub fn list(&self) -> Vec<TransferTask> {
        let mut tasks = self
            .entries
            .lock()
            .expect("transfer registry poisoned")
            .values()
            .map(|entry| entry.task.clone())
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| std::cmp::Reverse(task.elapsed_ms));
        tasks
    }

    pub fn cancellable_task(&self, id: &str) -> Option<TransferTask> {
        self.entries
            .lock()
            .expect("transfer registry poisoned")
            .get(id)
            .filter(|entry| entry.task.cancellable && !entry.task.phase.terminal())
            .map(|entry| entry.task.clone())
    }

    /// Reports the short interval after preparation cancellation closes and before commit ends.
    pub fn preparation_committing(&self, id: &str) -> bool {
        self.entries
            .lock()
            .expect("transfer registry poisoned")
            .get(id)
            .is_some_and(|entry| {
                entry.preparation.is_some()
                    && entry.task.phase == TransferPhase::Preparing
                    && !entry.task.cancellable
                    && !entry.cancel.load(Ordering::Acquire)
            })
    }

    /// Wins the preparation cancel/commit race without publishing a terminal state yet.
    pub fn request_preparation_cancel(&self, id: &str) -> Option<CanceledPreparation> {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let entry = entries.get_mut(id)?;
        let preparation = entry.preparation.clone()?;
        if entry.task.phase != TransferPhase::Preparing || !entry.task.cancellable {
            return None;
        }
        let task = entry.task.clone();
        entry.cancel.store(true, Ordering::Release);
        entry.task.cancellable = false;
        drop(entries);
        self.emit(id, false);
        Some(CanceledPreparation {
            item_id: task.item_id,
            operation_order: preparation.operation_order,
            source_revision: preparation.source_revision,
        })
    }

    /// Publishes the preparation terminal state only after its durable outbox row is handled.
    pub fn finish_preparation_cancel(&self, id: &str, error: Option<String>) {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some(entry) = entries.get_mut(id) else {
            return;
        };
        if entry.task.phase.terminal() {
            return;
        }
        entry.task.phase = if error.is_some() {
            TransferPhase::Failed
        } else {
            TransferPhase::Canceled
        };
        entry.task.elapsed_ms = entry
            .started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        entry.task.error = error;
        entry.task.cancellable = false;
        drop(entries);
        self.emit(id, false);
    }

    pub fn cancel(&self, id: &str) -> Option<TransferTask> {
        let entries = self.entries.lock().expect("transfer registry poisoned");
        let entry = entries.get(id)?;
        if !entry.task.cancellable || entry.task.phase.terminal() {
            return None;
        }
        let task = entry.task.clone();
        let mut canceled = Vec::new();
        for related in entries.values() {
            if related.task.operation_id == task.operation_id
                && related.task.direction == task.direction
                && related.task.target == task.target
                && !related.task.phase.terminal()
            {
                related.cancel.store(true, Ordering::Release);
                canceled.push((related.task.id.clone(), related.task.transferred_bytes));
            }
        }
        drop(entries);
        for (id, transferred_bytes) in canceled {
            self.update(&id, TransferPhase::Canceled, transferred_bytes, None, true);
        }
        Some(task)
    }

    fn reroute(&self, id: &str, route: TransferRoute, target: String, transferred_bytes: u64) {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some(entry) = entries.get_mut(id) else {
            return;
        };
        if entry.task.phase.terminal() || entry.cancel.load(Ordering::Acquire) {
            return;
        }
        entry.task.route = route;
        entry.task.target = target;
        drop(entries);
        self.update(id, TransferPhase::Connecting, transferred_bytes, None, true);
    }

    fn begin_finalizing(&self, id: &str, transferred_bytes: u64) -> bool {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some(entry) = entries.get_mut(id) else {
            return false;
        };
        if entry.task.phase.terminal() || entry.cancel.load(Ordering::Acquire) {
            return false;
        }
        entry.task.cancellable = false;
        drop(entries);
        self.update(id, TransferPhase::Confirming, transferred_bytes, None, true);
        true
    }

    /// Atomically closes preparation cancellation before its event is committed.
    fn begin_preparation_commit(&self, id: &str) -> bool {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some(entry) = entries.get_mut(id) else {
            return false;
        };
        if entry.preparation.is_none()
            || entry.task.phase != TransferPhase::Preparing
            || !entry.task.cancellable
            || entry.cancel.load(Ordering::Acquire)
        {
            return false;
        }
        entry.task.cancellable = false;
        drop(entries);
        self.emit(id, false);
        true
    }

    fn update(
        &self,
        id: &str,
        phase: TransferPhase,
        transferred_bytes: u64,
        error: Option<String>,
        force_emit: bool,
    ) {
        let mut entries = self.entries.lock().expect("transfer registry poisoned");
        let Some(entry) = entries.get_mut(id) else {
            return;
        };
        if entry.task.phase.terminal()
            || (entry.cancel.load(Ordering::Acquire) && phase != TransferPhase::Canceled)
        {
            return;
        }
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(entry.started_at);
        let sample_elapsed = now.saturating_duration_since(entry.last_sample_at);
        if sample_elapsed >= EMIT_INTERVAL || phase.terminal() {
            let sample_bytes = transferred_bytes.saturating_sub(entry.last_sample_bytes);
            let sample_speed = if sample_elapsed.is_zero() {
                0
            } else {
                (sample_bytes as f64 / sample_elapsed.as_secs_f64()) as u64
            };
            entry.task.speed_bytes_per_second = if entry.task.speed_bytes_per_second == 0 {
                sample_speed
            } else {
                ((entry.task.speed_bytes_per_second as f64 * 0.7) + (sample_speed as f64 * 0.3))
                    as u64
            };
            entry.last_sample_at = now;
            entry.last_sample_bytes = transferred_bytes;
        }
        entry.task.phase = phase;
        entry.task.transferred_bytes = transferred_bytes.min(entry.task.total_bytes);
        entry.task.elapsed_ms = elapsed.as_millis().try_into().unwrap_or(u64::MAX);
        entry.task.error = error;
        entry.task.cancellable = entry.task.cancellable && !phase.terminal();
        entry.task.eta_seconds = (entry.task.speed_bytes_per_second > 0
            && entry.task.transferred_bytes < entry.task.total_bytes)
            .then(|| {
                entry
                    .task
                    .total_bytes
                    .saturating_sub(entry.task.transferred_bytes)
                    / entry.task.speed_bytes_per_second
            });
        let should_emit = force_emit
            || phase.terminal()
            || now.saturating_duration_since(entry.last_emit_at) >= EMIT_INTERVAL;
        if should_emit {
            entry.last_emit_at = now;
        }
        drop(entries);
        if should_emit {
            self.emit(id, false);
        }
    }

    fn emit(&self, id: &str, prune: bool) {
        let task = {
            let mut entries = self.entries.lock().expect("transfer registry poisoned");
            if prune && entries.len() > MAX_RETAINED_TASKS {
                let mut terminal = entries
                    .iter()
                    .filter(|(_, entry)| entry.task.phase.terminal())
                    .map(|(id, entry)| (id.clone(), entry.started_at))
                    .collect::<Vec<_>>();
                terminal.sort_by_key(|(_, started_at)| *started_at);
                let remove_count = entries.len().saturating_sub(MAX_RETAINED_TASKS);
                for (old_id, _) in terminal.into_iter().take(remove_count) {
                    entries.remove(&old_id);
                }
            }
            entries.get(id).map(|entry| entry.task.clone())
        };
        if let Some(task) = task {
            if let Err(error) = self.app.emit(TRANSFER_UPDATED_EVENT, task) {
                log::warn!("emit transfer progress failed: {error}");
            }
        }
    }
}

/// Closes the preparation lifecycle without pretending that attachment bytes were uploaded.
fn complete_metadata_only_entry(entry: &mut TransferEntry) {
    entry.preparation = None;
    entry.task.phase = TransferPhase::Completed;
    entry.task.transferred_bytes = 0;
    entry.task.total_bytes = 0;
    entry.task.operation_total_bytes = 0;
    entry.task.speed_bytes_per_second = 0;
    entry.task.elapsed_ms = entry
        .started_at
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    entry.task.eta_seconds = None;
    entry.task.error = None;
    entry.task.cancellable = false;
    entry.last_sample_bytes = 0;
    entry.last_sample_at = Instant::now();
    entry.last_emit_at = entry.last_sample_at;
}

#[derive(Clone)]
pub(super) struct TransferHandle {
    id: String,
    cancel: Arc<AtomicBool>,
    registry: TransferRegistry,
}

impl TransferHandle {
    pub fn canceled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    pub fn connecting(&self) {
        self.registry
            .update(&self.id, TransferPhase::Connecting, 0, None, true);
    }

    pub fn preparing(&self, bytes: u64) {
        self.registry
            .update(&self.id, TransferPhase::Preparing, bytes, None, false);
    }

    pub fn prepared(&self, route: TransferRoute, target: String) {
        self.registry.prepared(&self.id, route, target);
    }

    pub fn begin_preparation_commit(&self) -> bool {
        self.registry.begin_preparation_commit(&self.id)
    }

    /// Switches an active transfer to its alternate transport without creating a second task.
    pub fn reroute(&self, route: TransferRoute, target: String, transferred_bytes: u64) {
        self.registry
            .reroute(&self.id, route, target, transferred_bytes);
    }

    pub fn transferred(&self, bytes: u64) {
        self.registry
            .update(&self.id, TransferPhase::Transferring, bytes, None, false);
    }

    pub fn confirming(&self, bytes: u64) {
        self.registry
            .update(&self.id, TransferPhase::Confirming, bytes, None, true);
    }

    /// Atomically closes the cancellation window before irreversible finalization begins.
    pub fn begin_finalizing(&self, bytes: u64) -> bool {
        self.registry.begin_finalizing(&self.id, bytes)
    }

    pub fn completed(&self, bytes: u64) {
        self.registry
            .update(&self.id, TransferPhase::Completed, bytes, None, true);
    }

    pub fn failed(&self, bytes: u64, error: impl Into<String>) {
        self.registry.update(
            &self.id,
            TransferPhase::Failed,
            bytes,
            Some(error.into()),
            true,
        );
    }

    pub fn mark_canceled(&self, bytes: u64) {
        self.registry
            .update(&self.id, TransferPhase::Canceled, bytes, None, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_only_confirmation_finishes_queued_preparation_without_payload_bytes() {
        let now = Instant::now();
        let mut entry = TransferEntry {
            task: TransferTask {
                id: "task".into(),
                operation_id: "event".into(),
                item_id: "item".into(),
                direction: TransferDirection::Upload,
                route: TransferRoute::Lan,
                target: "peer".into(),
                file_name: None,
                phase: TransferPhase::Queued,
                transferred_bytes: 0,
                total_bytes: 5 * 1024 * 1024 * 1024,
                operation_total_bytes: 5 * 1024 * 1024 * 1024,
                speed_bytes_per_second: 0,
                elapsed_ms: 0,
                eta_seconds: None,
                error: None,
                cancellable: true,
            },
            preparation: Some(PreparationContext {
                operation_order: 1,
                source_revision: "revision".into(),
            }),
            cancel: Arc::new(AtomicBool::new(false)),
            started_at: now,
            last_sample_at: now,
            last_sample_bytes: 0,
            last_emit_at: now,
        };

        complete_metadata_only_entry(&mut entry);

        assert_eq!(entry.task.phase, TransferPhase::Completed);
        assert_eq!(entry.task.transferred_bytes, 0);
        assert_eq!(entry.task.total_bytes, 0);
        assert_eq!(entry.task.operation_total_bytes, 0);
        assert!(!entry.task.cancellable);
        assert!(entry.preparation.is_none());
    }
}
