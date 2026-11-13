use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter};

pub const TRANSFER_UPDATED_EVENT: &str = "sync-transfer://updated";
pub const TRANSFER_REMOVED_EVENT: &str = "sync-transfer://removed";
const EMIT_INTERVAL: Duration = Duration::from_millis(500);
const FINISHED_TASK_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_FINISHED_TASKS: usize = 50;
const LOCAL_IMPORT_TARGET: &str = "import";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TransferDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
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
    Available,
    Completed,
    Failed,
    Canceled,
}

impl TransferPhase {
    fn terminal(self) -> bool {
        matches!(
            self,
            Self::Available | Self::Completed | Self::Failed | Self::Canceled
        )
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferTask {
    pub id: String,
    pub revision: u64,
    pub started_at_ms: i64,
    pub operation_id: String,
    pub item_id: String,
    pub direction: TransferDirection,
    pub route: TransferRoute,
    pub target: String,
    pub file_name: Option<String>,
    pub phase: TransferPhase,
    pub transferred_bytes: u64,
    pub total_bytes: u64,
    pub speed_bytes_per_second: u64,
    pub elapsed_ms: u64,
    pub eta_seconds: Option<u64>,
    pub error: Option<String>,
    pub cancellable: bool,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct TransferOperationKey {
    operation_id: String,
    direction: TransferDirection,
    route: TransferRoute,
    target: String,
}

impl TransferOperationKey {
    fn from_task(task: &TransferTask) -> Self {
        Self {
            operation_id: task.operation_id.clone(),
            direction: task.direction,
            route: task.route,
            target: task.target.clone(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransferPersistence {
    Durable,
    Transient,
}

struct TransferEntry {
    task: TransferTask,
    preparation: Option<PreparationContext>,
    persistence: TransferPersistence,
    cancel: Arc<AtomicBool>,
    active_components: u32,
    completed_bytes: u64,
    started_at: Instant,
    finished_at: Option<Instant>,
    last_sample_at: Instant,
    last_sample_bytes: u64,
    last_emit_at: Instant,
}

#[derive(Clone)]
struct PreparationContext {
    operation_order: i64,
    source_revision: String,
}

#[derive(Default)]
struct ComponentProgress {
    reported_bytes: AtomicU64,
    terminal: AtomicBool,
}

pub(super) struct CanceledPreparation {
    pub item_id: String,
    pub operation_order: i64,
    pub source_revision: String,
}

pub(super) struct LocalImportPromotion {
    pub item_id: String,
    pub operation_id: String,
    pub route: TransferRoute,
    pub target: String,
    pub total_bytes: u64,
    pub operation_order: i64,
    pub source_revision: String,
}

pub(super) enum PreparationClaim {
    Claimed(TransferHandle),
    Canceled,
    Missing,
}

#[derive(Default)]
struct RegistryState {
    entries: HashMap<String, TransferEntry>,
    active_operations: HashMap<TransferOperationKey, String>,
    local_imports: HashMap<String, String>,
    next_revision: u64,
}

#[derive(Clone)]
pub(super) struct TransferRegistry {
    app: AppHandle,
    state: Arc<Mutex<RegistryState>>,
}

impl TransferRegistry {
    pub fn new(app: AppHandle) -> Self {
        Self {
            app,
            state: Arc::new(Mutex::new(RegistryState::default())),
        }
    }

    /// Joins parallel attachment streams into one Rust-owned user-visible operation.
    pub fn start(&self, task: TransferTask) -> TransferHandle {
        self.start_entry(task, None, TransferPersistence::Durable, true)
    }

    pub fn start_transient(&self, task: TransferTask) -> TransferHandle {
        self.start_entry(task, None, TransferPersistence::Transient, true)
    }

    #[cfg(target_os = "android")]
    pub fn start_local_import(&self, task: TransferTask) -> TransferHandle {
        self.start_entry(task, None, TransferPersistence::Transient, false)
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
            TransferPersistence::Durable,
            false,
        )
    }

    fn start_entry(
        &self,
        mut task: TransferTask,
        preparation: Option<PreparationContext>,
        persistence: TransferPersistence,
        join_operation: bool,
    ) -> TransferHandle {
        let now = Instant::now();
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let operation_key = TransferOperationKey::from_task(&task);

        if join_operation {
            if let Some(entry_id) = state.active_operations.get(&operation_key).cloned() {
                if let Some(entry) = state.entries.get_mut(&entry_id) {
                    entry.active_components = entry.active_components.saturating_add(1);
                    entry.task.total_bytes = entry.task.total_bytes.max(task.total_bytes);
                    entry.task.file_name = None;
                    entry.task.cancellable = true;
                    let cancel = entry.cancel.clone();
                    drop(state);
                    self.emit(&entry_id, false);
                    return self.handle(entry_id, cancel);
                }
            }
        }

        let entry_id = task.id.clone();
        task.revision = 0;
        let cancel = Arc::new(AtomicBool::new(false));
        let item_id = task.item_id.clone();
        let local_import = task.target == LOCAL_IMPORT_TARGET;
        state.entries.insert(
            entry_id.clone(),
            TransferEntry {
                task,
                preparation,
                persistence,
                cancel: cancel.clone(),
                active_components: 1,
                completed_bytes: 0,
                started_at: now,
                finished_at: None,
                last_sample_at: now,
                last_sample_bytes: 0,
                last_emit_at: now.checked_sub(EMIT_INTERVAL).unwrap_or(now),
            },
        );
        state
            .active_operations
            .insert(operation_key, entry_id.clone());
        if local_import {
            state.local_imports.insert(item_id, entry_id.clone());
        }
        drop(state);
        self.emit(&entry_id, true);
        self.handle(entry_id, cancel)
    }

    fn handle(&self, entry_id: String, cancel: Arc<AtomicBool>) -> TransferHandle {
        TransferHandle {
            entry_id,
            component: Arc::new(ComponentProgress::default()),
            cancel,
            registry: self.clone(),
        }
    }

    pub fn item_has_active_local_import(&self, item_id: &str) -> bool {
        let state = self.state.lock().expect("transfer registry poisoned");
        state
            .local_imports
            .get(item_id)
            .and_then(|id| state.entries.get(id))
            .is_some_and(|entry| entry.task.phase == TransferPhase::Preparing)
    }

    /// Ends every component of a failed card operation, including queued preparation.
    pub fn fail_upload_operation(&self, operation_id: &str, target: &str, error: &str) {
        let route = if target == "cloud" {
            TransferRoute::Cloud
        } else {
            TransferRoute::Lan
        };
        let key = operation_key(operation_id, TransferDirection::Upload, route, target);
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry_id) = state.active_operations.remove(&key) else {
            return;
        };
        let Some(entry) = state.entries.get_mut(&entry_id) else {
            return;
        };
        entry.cancel.store(true, Ordering::Release);
        entry.preparation = None;
        let item_id = entry.task.item_id.clone();
        let bytes = entry.task.transferred_bytes;
        finish_entry(entry, TransferPhase::Failed, bytes, Some(error.to_owned()));
        state.local_imports.remove(&item_id);
        drop(state);
        self.emit(&entry_id, false);
    }

    /// Ends an imported file task when another route materialized its outgoing event.
    pub fn complete_local_import_for_item(&self, item_id: &str) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry_id) = state.local_imports.get(item_id).cloned() else {
            return;
        };
        let Some(entry) = state.entries.get_mut(&entry_id) else {
            return;
        };
        if entry.task.target != LOCAL_IMPORT_TARGET || entry.task.phase != TransferPhase::Queued {
            return;
        }
        let key = TransferOperationKey::from_task(&entry.task);
        let bytes = entry.task.transferred_bytes;
        entry.preparation = None;
        finish_entry(entry, TransferPhase::Completed, bytes, None);
        state.active_operations.remove(&key);
        state.local_imports.remove(item_id);
        drop(state);
        self.emit(&entry_id, false);
    }

    #[cfg(target_os = "android")]
    pub fn local_import_canceled(&self, id: &str) -> bool {
        self.state
            .lock()
            .expect("transfer registry poisoned")
            .entries
            .get(id)
            .is_none_or(|entry| entry.cancel.load(Ordering::Acquire))
    }

    #[cfg(target_os = "android")]
    pub fn update_local_import(&self, id: &str, bytes: u64) {
        self.update_absolute(id, TransferPhase::Preparing, bytes, None, false);
    }

    #[cfg(target_os = "android")]
    pub fn finish_local_import(&self, id: &str, total_bytes: u64) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry) = state.entries.get_mut(id) else {
            return;
        };
        if entry.task.target != LOCAL_IMPORT_TARGET || entry.task.phase.terminal() {
            return;
        }
        entry.task.transferred_bytes = total_bytes;
        entry.task.total_bytes = total_bytes;
        entry.task.phase = TransferPhase::Queued;
        entry.task.speed_bytes_per_second = 0;
        entry.task.eta_seconds = None;
        entry.active_components = 0;
        entry.last_sample_bytes = total_bytes;
        drop(state);
        self.emit(id, false);
    }

    #[cfg(target_os = "android")]
    pub fn bind_local_import(&self, id: &str, operation_order: i64, source_revision: String) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry) = state.entries.get_mut(id) else {
            return;
        };
        if entry.task.target != LOCAL_IMPORT_TARGET || entry.task.phase.terminal() {
            return;
        }
        entry.preparation = Some(PreparationContext {
            operation_order,
            source_revision,
        });
    }

    #[cfg(target_os = "android")]
    pub fn fail_local_import(&self, id: &str, bytes: u64, error: String) {
        self.update_absolute(id, TransferPhase::Failed, bytes, Some(error), true);
    }

    /// Continues the Android import operation as upload preparation without a second task.
    pub fn promote_local_import(&self, promotion: LocalImportPromotion) -> Option<TransferHandle> {
        let LocalImportPromotion {
            item_id,
            operation_id,
            route,
            target,
            total_bytes,
            operation_order,
            source_revision,
        } = promotion;
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let entry_id = state.local_imports.get(&item_id)?.clone();
        let old_key = TransferOperationKey::from_task(&state.entries.get(&entry_id)?.task);
        state.active_operations.remove(&old_key);
        state.local_imports.remove(&item_id);
        let entry = state.entries.get_mut(&entry_id)?;
        if entry.task.phase != TransferPhase::Queued || entry.cancel.load(Ordering::Acquire) {
            return None;
        }
        entry.preparation = Some(PreparationContext {
            operation_order,
            source_revision,
        });
        entry.task.operation_id = operation_id;
        entry.task.route = route;
        entry.task.target = target;
        entry.persistence = TransferPersistence::Durable;
        reset_entry(entry, TransferPhase::Preparing, total_bytes);
        let key = TransferOperationKey::from_task(&entry.task);
        let cancel = entry.cancel.clone();
        state.active_operations.insert(key, entry_id.clone());
        drop(state);
        self.emit(&entry_id, false);
        Some(self.handle(entry_id, cancel))
    }

    /// Converts upload preparation into the first attachment stream of the same operation.
    pub fn claim_preparation(
        &self,
        operation_id: &str,
        route: TransferRoute,
        target: &str,
        total_bytes: u64,
    ) -> PreparationClaim {
        let key = operation_key(operation_id, TransferDirection::Upload, route, target);
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry_id) = state.active_operations.get(&key).cloned() else {
            return PreparationClaim::Missing;
        };
        let Some(entry) = state.entries.get_mut(&entry_id) else {
            return PreparationClaim::Missing;
        };
        if entry.preparation.is_none() {
            return PreparationClaim::Missing;
        }
        if entry.cancel.load(Ordering::Acquire) || entry.task.phase == TransferPhase::Canceled {
            return PreparationClaim::Canceled;
        }
        if entry.task.phase != TransferPhase::Queued || !entry.task.cancellable {
            return PreparationClaim::Missing;
        }
        entry.preparation = None;
        reset_entry(entry, TransferPhase::Queued, total_bytes);
        let cancel = entry.cancel.clone();
        drop(state);
        self.emit(&entry_id, false);
        PreparationClaim::Claimed(self.handle(entry_id, cancel))
    }

    pub fn complete_metadata_only_preparation(
        &self,
        operation_id: &str,
        route: TransferRoute,
        target: &str,
    ) -> bool {
        let key = operation_key(operation_id, TransferDirection::Upload, route, target);
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry_id) = state.active_operations.get(&key).cloned() else {
            return false;
        };
        let Some(entry) = state.entries.get(&entry_id) else {
            return false;
        };
        if entry.preparation.is_none()
            || !matches!(
                entry.task.phase,
                TransferPhase::Queued | TransferPhase::Confirming
            )
            || entry.cancel.load(Ordering::Acquire)
        {
            return false;
        }
        state.active_operations.remove(&key);
        state.entries.remove(&entry_id);
        drop(state);
        self.emit_removed(vec![entry_id]);
        true
    }

    fn prepared(&self, id: &str, route: TransferRoute, target: String) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(old_key) = state
            .entries
            .get(id)
            .map(|entry| TransferOperationKey::from_task(&entry.task))
        else {
            return;
        };
        state.active_operations.remove(&old_key);
        let entry = state.entries.get_mut(id).expect("entry checked above");
        if entry.task.phase.terminal() || entry.cancel.load(Ordering::Acquire) {
            return;
        }
        entry.task.route = route;
        entry.task.target = target;
        entry.task.phase = TransferPhase::Queued;
        entry.task.transferred_bytes = 0;
        entry.task.speed_bytes_per_second = 0;
        entry.task.eta_seconds = None;
        entry.task.error = None;
        entry.task.cancellable = true;
        entry.active_components = 0;
        entry.last_sample_bytes = 0;
        entry.last_sample_at = Instant::now();
        let new_key = TransferOperationKey::from_task(&entry.task);
        state.active_operations.insert(new_key, id.to_owned());
        drop(state);
        self.emit(id, false);
    }

    pub fn list(&self) -> Vec<TransferTask> {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let removed = prune_finished_tasks(&mut state, Instant::now());
        let mut entries = state.entries.values().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.started_at);
        let tasks = entries
            .into_iter()
            .map(|entry| entry.task.clone())
            .collect();
        drop(state);
        self.emit_removed(removed);
        tasks
    }

    /// Clears display history without canceling work or changing durable delivery state.
    pub fn clear_finished(&self) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let mut removed = Vec::new();
        state.entries.retain(|id, entry| {
            if !entry.task.phase.terminal() {
                return true;
            }
            removed.push(id.clone());
            false
        });
        drop(state);
        self.emit_removed(removed);
    }

    pub fn cancellable_task(&self, id: &str) -> Option<(TransferTask, bool)> {
        self.state
            .lock()
            .expect("transfer registry poisoned")
            .entries
            .get(id)
            .filter(|entry| entry.task.cancellable && !entry.task.phase.terminal())
            .map(|entry| {
                (
                    entry.task.clone(),
                    entry.persistence == TransferPersistence::Transient,
                )
            })
    }

    pub fn preparation_committing(&self, id: &str) -> bool {
        self.state
            .lock()
            .expect("transfer registry poisoned")
            .entries
            .get(id)
            .is_some_and(|entry| {
                entry.preparation.is_some()
                    && entry.task.phase == TransferPhase::Preparing
                    && !entry.task.cancellable
                    && !entry.cancel.load(Ordering::Acquire)
            })
    }

    pub fn request_preparation_cancel(&self, id: &str) -> Option<CanceledPreparation> {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let entry = state.entries.get_mut(id)?;
        let preparation = entry.preparation.clone()?;
        let cancelable_phase = if entry.task.target == LOCAL_IMPORT_TARGET {
            matches!(
                entry.task.phase,
                TransferPhase::Preparing | TransferPhase::Queued
            )
        } else {
            entry.task.phase == TransferPhase::Preparing
        };
        if !cancelable_phase || !entry.task.cancellable {
            return None;
        }
        entry.cancel.store(true, Ordering::Release);
        entry.task.cancellable = false;
        let item_id = entry.task.item_id.clone();
        drop(state);
        self.emit(id, false);
        Some(CanceledPreparation {
            item_id,
            operation_order: preparation.operation_order,
            source_revision: preparation.source_revision,
        })
    }

    pub fn finish_preparation_cancel(&self, id: &str, error: Option<String>) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry) = state.entries.get_mut(id) else {
            return;
        };
        if entry.task.phase.terminal() {
            return;
        }
        let phase = if error.is_some() {
            TransferPhase::Failed
        } else {
            TransferPhase::Canceled
        };
        let bytes = entry.task.transferred_bytes;
        finish_entry(entry, phase, bytes, error);
        let key = TransferOperationKey::from_task(&entry.task);
        let item_id = entry.task.item_id.clone();
        state.active_operations.remove(&key);
        state.local_imports.remove(&item_id);
        drop(state);
        self.emit(id, false);
    }

    pub fn cancel(&self, id: &str) -> Option<TransferTask> {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let entry = state.entries.get_mut(id)?;
        if !entry.task.cancellable || entry.task.phase.terminal() {
            return None;
        }
        entry.cancel.store(true, Ordering::Release);
        let bytes = entry.task.transferred_bytes;
        finish_entry(entry, TransferPhase::Canceled, bytes, None);
        let task = entry.task.clone();
        let key = TransferOperationKey::from_task(&entry.task);
        let item_id = entry.task.item_id.clone();
        state.active_operations.remove(&key);
        state.local_imports.remove(&item_id);
        drop(state);
        self.emit(id, false);
        Some(task)
    }

    fn reroute(&self, id: &str, route: TransferRoute, target: String, transferred_bytes: u64) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(old_key) = state
            .entries
            .get(id)
            .map(|entry| TransferOperationKey::from_task(&entry.task))
        else {
            return;
        };
        state.active_operations.remove(&old_key);
        let entry = state.entries.get_mut(id).expect("entry checked above");
        if entry.task.phase.terminal() || entry.cancel.load(Ordering::Acquire) {
            return;
        }
        entry.task.route = route;
        entry.task.target = target;
        entry.task.phase = TransferPhase::Connecting;
        entry.task.transferred_bytes =
            bounded_transfer_bytes(entry.task.total_bytes, transferred_bytes);
        entry.task.speed_bytes_per_second = 0;
        entry.task.eta_seconds = None;
        entry.last_sample_at = Instant::now();
        entry.last_sample_bytes = entry.task.transferred_bytes;
        let key = TransferOperationKey::from_task(&entry.task);
        state.active_operations.insert(key, id.to_owned());
        drop(state);
        self.emit(id, false);
    }

    fn begin_finalizing(&self, id: &str, transferred_bytes: u64) -> bool {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry) = state.entries.get_mut(id) else {
            return false;
        };
        if entry.task.phase.terminal() || entry.cancel.load(Ordering::Acquire) {
            return false;
        }
        entry.task.cancellable = false;
        entry.task.phase = TransferPhase::Confirming;
        entry.task.transferred_bytes =
            bounded_transfer_bytes(entry.task.total_bytes, transferred_bytes);
        drop(state);
        self.emit(id, false);
        true
    }

    fn begin_preparation_commit(&self, id: &str) -> bool {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry) = state.entries.get_mut(id) else {
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
        drop(state);
        self.emit(id, false);
        true
    }

    #[cfg(target_os = "android")]
    fn update_absolute(
        &self,
        id: &str,
        phase: TransferPhase,
        bytes: u64,
        error: Option<String>,
        force_emit: bool,
    ) {
        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry) = state.entries.get_mut(id) else {
            return;
        };
        if entry.task.phase.terminal()
            || (entry.cancel.load(Ordering::Acquire) && phase != TransferPhase::Canceled)
        {
            return;
        }
        let now = Instant::now();
        update_speed(entry, now, bytes);
        entry.task.phase = phase;
        entry.task.transferred_bytes = bounded_transfer_bytes(entry.task.total_bytes, bytes);
        entry.task.error = error;
        entry.task.elapsed_ms = elapsed_ms(entry.started_at, now);
        entry.task.cancellable &= !phase.terminal();
        if phase.terminal() {
            entry.finished_at = Some(now);
        }
        update_eta(entry);
        let should_emit = force_emit
            || phase.terminal()
            || now.saturating_duration_since(entry.last_emit_at) >= EMIT_INTERVAL;
        if should_emit {
            entry.last_emit_at = now;
        }
        let terminal = phase.terminal();
        let key = terminal.then(|| TransferOperationKey::from_task(&entry.task));
        let item_id = terminal.then(|| entry.task.item_id.clone());
        if let Some(key) = key {
            state.active_operations.remove(&key);
        }
        if let Some(item_id) = item_id {
            state.local_imports.remove(&item_id);
        }
        drop(state);
        if should_emit {
            self.emit(id, false);
        }
    }

    fn update_handle(
        &self,
        id: &str,
        component: &ComponentProgress,
        phase: TransferPhase,
        bytes: u64,
        completion_bytes: Option<u64>,
        error: Option<String>,
        force_emit: bool,
    ) {
        if component.terminal.load(Ordering::Acquire) {
            return;
        }
        let previous = component.reported_bytes.swap(bytes, Ordering::AcqRel);
        let delta = bytes.saturating_sub(previous);
        if phase.terminal() {
            component.terminal.store(true, Ordering::Release);
        }

        let mut state = self.state.lock().expect("transfer registry poisoned");
        let Some(entry) = state.entries.get_mut(id) else {
            return;
        };
        if entry.task.phase.terminal()
            || (entry.cancel.load(Ordering::Acquire) && phase != TransferPhase::Canceled)
        {
            return;
        }
        let now = Instant::now();
        let aggregate_bytes = bounded_transfer_bytes(
            entry.task.total_bytes,
            entry.task.transferred_bytes.saturating_add(delta),
        );
        update_speed(entry, now, aggregate_bytes);
        entry.task.transferred_bytes = aggregate_bytes;
        entry.task.elapsed_ms = elapsed_ms(entry.started_at, now);
        if let Some(error) = error {
            entry.cancel.store(true, Ordering::Release);
            finish_entry(entry, TransferPhase::Failed, aggregate_bytes, Some(error));
        } else {
            match phase {
                TransferPhase::Available | TransferPhase::Completed => {
                    entry.active_components = entry.active_components.saturating_sub(1);
                    entry.completed_bytes = entry
                        .completed_bytes
                        .saturating_add(completion_bytes.unwrap_or(bytes));
                    if (entry.task.total_bytes == 0 && entry.active_components == 0)
                        || (entry.task.total_bytes > 0
                            && entry.completed_bytes >= entry.task.total_bytes)
                    {
                        let completed_phase = if aggregate_bytes == 0 {
                            TransferPhase::Available
                        } else {
                            TransferPhase::Completed
                        };
                        finish_entry(entry, completed_phase, aggregate_bytes, None);
                    } else if entry.active_components == 0 {
                        entry.task.phase = TransferPhase::Queued;
                    }
                }
                TransferPhase::Canceled => {
                    finish_entry(entry, TransferPhase::Canceled, aggregate_bytes, None);
                }
                next => {
                    entry.task.phase = advance_phase(entry.task.phase, next);
                }
            }
        }
        entry.task.cancellable &= !entry.task.phase.terminal();
        update_eta(entry);
        let should_emit = force_emit
            || phase.terminal()
            || now.saturating_duration_since(entry.last_emit_at) >= EMIT_INTERVAL;
        if should_emit {
            entry.last_emit_at = now;
        }
        let terminal = entry.task.phase.terminal();
        let key = terminal.then(|| TransferOperationKey::from_task(&entry.task));
        let item_id = terminal.then(|| entry.task.item_id.clone());
        if let Some(key) = key {
            state.active_operations.remove(&key);
        }
        if let Some(item_id) = item_id {
            state.local_imports.remove(&item_id);
        }
        drop(state);
        if should_emit {
            self.emit(id, false);
        }
    }

    fn emit(&self, id: &str, prune: bool) {
        let (task, removed) = {
            let mut state = self.state.lock().expect("transfer registry poisoned");
            let prune = prune
                || state
                    .entries
                    .get(id)
                    .is_some_and(|entry| entry.task.phase.terminal());
            let removed = if prune {
                prune_finished_tasks(&mut state, Instant::now())
            } else {
                Vec::new()
            };
            state.next_revision = state.next_revision.saturating_add(1);
            let revision = state.next_revision;
            let task = state.entries.get_mut(id).map(|entry| {
                entry.task.revision = revision;
                entry.task.clone()
            });
            (task, removed)
        };
        if let Some(task) = task {
            if let Err(error) = self.app.emit(TRANSFER_UPDATED_EVENT, task) {
                log::warn!("emit transfer progress failed: {error}");
            }
        }
        self.emit_removed(removed);
    }

    fn emit_removed(&self, task_ids: Vec<String>) {
        if task_ids.is_empty() {
            return;
        }
        if let Err(error) = self.app.emit(TRANSFER_REMOVED_EVENT, task_ids) {
            log::warn!("emit removed transfer records failed: {error}");
        }
    }
}

/// Bounds terminal history by completion time while preserving every unfinished operation.
fn prune_finished_tasks(state: &mut RegistryState, now: Instant) -> Vec<String> {
    let mut finished = state
        .entries
        .iter()
        .filter_map(|(id, entry)| {
            entry
                .finished_at
                .map(|finished_at| (id.clone(), finished_at))
        })
        .collect::<Vec<_>>();
    finished.sort_by_key(|(_, finished_at)| std::cmp::Reverse(*finished_at));

    let removed = finished
        .into_iter()
        .enumerate()
        .filter_map(|(index, (id, finished_at))| {
            (index >= MAX_FINISHED_TASKS
                || now.saturating_duration_since(finished_at) >= FINISHED_TASK_RETENTION)
                .then_some(id)
        })
        .collect::<Vec<_>>();
    for id in &removed {
        state.entries.remove(id);
    }
    removed
}

fn operation_key(
    operation_id: &str,
    direction: TransferDirection,
    route: TransferRoute,
    target: &str,
) -> TransferOperationKey {
    TransferOperationKey {
        operation_id: operation_id.to_owned(),
        direction,
        route,
        target: target.to_owned(),
    }
}

fn reset_entry(entry: &mut TransferEntry, phase: TransferPhase, total_bytes: u64) {
    let now = Instant::now();
    entry.task.started_at_ms = chrono::Utc::now().timestamp_millis();
    entry.task.phase = phase;
    entry.task.transferred_bytes = 0;
    entry.task.total_bytes = total_bytes;
    entry.task.speed_bytes_per_second = 0;
    entry.task.elapsed_ms = 0;
    entry.task.eta_seconds = None;
    entry.task.error = None;
    entry.task.cancellable = true;
    entry.active_components = 1;
    entry.completed_bytes = 0;
    entry.started_at = now;
    entry.finished_at = None;
    entry.last_sample_at = now;
    entry.last_sample_bytes = 0;
    entry.last_emit_at = now.checked_sub(EMIT_INTERVAL).unwrap_or(now);
}

fn finish_entry(
    entry: &mut TransferEntry,
    phase: TransferPhase,
    bytes: u64,
    error: Option<String>,
) {
    let now = Instant::now();
    entry.task.phase = phase;
    entry.task.transferred_bytes = bounded_transfer_bytes(entry.task.total_bytes, bytes);
    entry.task.elapsed_ms = elapsed_ms(entry.started_at, now);
    entry.task.eta_seconds = None;
    entry.task.error = error;
    entry.task.cancellable = false;
    entry.active_components = 0;
    entry.finished_at = Some(now);
}

fn bounded_transfer_bytes(total_bytes: u64, bytes: u64) -> u64 {
    if total_bytes == 0 {
        bytes
    } else {
        bytes.min(total_bytes)
    }
}

fn update_speed(entry: &mut TransferEntry, now: Instant, bytes: u64) {
    let sample_elapsed = now.saturating_duration_since(entry.last_sample_at);
    if sample_elapsed < EMIT_INTERVAL {
        return;
    }
    let sample_bytes = bytes.saturating_sub(entry.last_sample_bytes);
    let sample_speed = if sample_elapsed.is_zero() {
        0
    } else {
        (sample_bytes as f64 / sample_elapsed.as_secs_f64()) as u64
    };
    entry.task.speed_bytes_per_second = if entry.task.speed_bytes_per_second == 0 {
        sample_speed
    } else {
        ((entry.task.speed_bytes_per_second as f64 * 0.7) + (sample_speed as f64 * 0.3)) as u64
    };
    entry.last_sample_at = now;
    entry.last_sample_bytes = bytes;
}

fn update_eta(entry: &mut TransferEntry) {
    entry.task.eta_seconds = (entry.task.speed_bytes_per_second > 0
        && entry.task.transferred_bytes < entry.task.total_bytes)
        .then(|| {
            entry
                .task
                .total_bytes
                .saturating_sub(entry.task.transferred_bytes)
                / entry.task.speed_bytes_per_second
        });
}

fn elapsed_ms(started_at: Instant, now: Instant) -> u64 {
    now.saturating_duration_since(started_at)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn advance_phase(current: TransferPhase, next: TransferPhase) -> TransferPhase {
    let rank = |phase| match phase {
        TransferPhase::Queued => 0,
        TransferPhase::Connecting => 1,
        TransferPhase::Preparing => 2,
        TransferPhase::Transferring => 3,
        TransferPhase::Confirming => 4,
        TransferPhase::Available
        | TransferPhase::Completed
        | TransferPhase::Failed
        | TransferPhase::Canceled => 5,
    };
    if rank(next) >= rank(current) {
        next
    } else {
        current
    }
}

#[derive(Clone)]
pub(super) struct TransferHandle {
    entry_id: String,
    component: Arc<ComponentProgress>,
    cancel: Arc<AtomicBool>,
    registry: TransferRegistry,
}

impl TransferHandle {
    pub fn canceled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    pub fn connecting(&self) {
        self.update(TransferPhase::Connecting, 0, None, None, true);
    }

    pub fn preparing(&self, bytes: u64) {
        self.update(TransferPhase::Preparing, bytes, None, None, false);
    }

    pub fn prepared(&self, route: TransferRoute, target: String) {
        self.registry.prepared(&self.entry_id, route, target);
    }

    pub fn begin_preparation_commit(&self) -> bool {
        self.registry.begin_preparation_commit(&self.entry_id)
    }

    pub fn reroute(&self, route: TransferRoute, target: String, transferred_bytes: u64) {
        self.component
            .reported_bytes
            .store(transferred_bytes, Ordering::Release);
        self.registry
            .reroute(&self.entry_id, route, target, transferred_bytes);
    }

    pub fn transferred(&self, bytes: u64) {
        self.update(TransferPhase::Transferring, bytes, None, None, false);
    }

    pub fn begin_finalizing(&self, bytes: u64) -> bool {
        self.registry.begin_finalizing(&self.entry_id, bytes)
    }

    pub fn completed(&self, bytes: u64) {
        self.update(TransferPhase::Completed, bytes, Some(bytes), None, true);
    }

    /// Completes one logical attachment without reporting bytes that never crossed the network.
    pub fn already_available(&self, bytes: u64) {
        self.update(TransferPhase::Available, 0, Some(bytes), None, true);
    }

    pub fn failed(&self, bytes: u64, error: impl Into<String>) {
        self.update(TransferPhase::Failed, bytes, None, Some(error.into()), true);
    }

    pub fn mark_canceled(&self, bytes: u64) {
        self.update(TransferPhase::Canceled, bytes, None, None, true);
    }

    fn update(
        &self,
        phase: TransferPhase,
        bytes: u64,
        completion_bytes: Option<u64>,
        error: Option<String>,
        force_emit: bool,
    ) {
        self.registry.update_handle(
            &self.entry_id,
            &self.component,
            phase,
            bytes,
            completion_bytes,
            error,
            force_emit,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn later_parallel_stream_does_not_regress_visible_phase() {
        assert_eq!(
            advance_phase(TransferPhase::Transferring, TransferPhase::Connecting),
            TransferPhase::Transferring
        );
    }

    #[test]
    fn metadata_only_completion_has_no_fake_payload_bytes() {
        let now = Instant::now();
        let mut entry = TransferEntry {
            task: TransferTask {
                id: "task".into(),
                revision: 0,
                started_at_ms: chrono::Utc::now().timestamp_millis(),
                operation_id: "event".into(),
                item_id: "item".into(),
                direction: TransferDirection::Upload,
                route: TransferRoute::Cloud,
                target: "cloud".into(),
                file_name: None,
                phase: TransferPhase::Queued,
                transferred_bytes: 0,
                total_bytes: 100,
                speed_bytes_per_second: 0,
                elapsed_ms: 0,
                eta_seconds: None,
                error: None,
                cancellable: true,
            },
            preparation: None,
            persistence: TransferPersistence::Durable,
            cancel: Arc::new(AtomicBool::new(false)),
            active_components: 0,
            completed_bytes: 0,
            started_at: now,
            finished_at: None,
            last_sample_at: now,
            last_sample_bytes: 0,
            last_emit_at: now,
        };

        finish_entry(&mut entry, TransferPhase::Completed, 0, None);

        assert_eq!(entry.task.phase, TransferPhase::Completed);
        assert_eq!(entry.task.transferred_bytes, 0);
        assert!(!entry.task.cancellable);
    }

    #[test]
    fn unknown_total_preserves_reported_bytes() {
        assert_eq!(
            bounded_transfer_bytes(0, 147 * 1024 * 1024),
            147 * 1024 * 1024
        );
        assert_eq!(bounded_transfer_bytes(100, 120), 100);
    }
}
