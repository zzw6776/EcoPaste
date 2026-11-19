//! Owns the durable and in-memory lifecycle of outgoing synchronization deliveries.

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
};

use anyhow::Result;
use sqlx::SqlitePool;
use tauri::AppHandle;
use tokio::sync::{Mutex, MutexGuard};

use crate::db;

use super::{
    model::StoredBlob,
    progress::{
        LocalImportPromotion, PreparationClaim, TransferDirection, TransferHandle, TransferPhase,
        TransferRegistry, TransferRoute, TransferTask,
    },
    repository,
};

pub(super) enum DeliveryCancel {
    NotFound,
    Canceled,
    Preparation {
        item_id: String,
        source_revision: String,
    },
}

pub(super) struct UploadStartRequest<'a> {
    pub target: &'a str,
    pub route: TransferRoute,
    pub sync_key: &'a str,
    pub item_id: Option<&'a str>,
    pub operation_total_bytes: u64,
    pub blob: &'a StoredBlob,
}

pub(super) enum UploadTransferStart {
    Ready(Option<TransferHandle>),
    Canceled,
}

#[derive(Clone)]
pub(super) struct PreparationTarget {
    pub route: TransferRoute,
    pub target: String,
}

impl PreparationTarget {
    pub fn matches_delivery_route(&self, delivery_route: Option<&str>) -> bool {
        match delivery_route {
            Some("cloud") => self.route == TransferRoute::Cloud,
            Some("lan") => self.route == TransferRoute::Lan,
            Some(_) => false,
            None => true,
        }
    }
}

#[derive(Clone)]
pub(super) struct PreparationProgress {
    pub handle: TransferHandle,
    processed: Arc<AtomicU64>,
    target: PreparationTarget,
}

impl PreparationProgress {
    pub fn new(handle: TransferHandle, target: PreparationTarget) -> Self {
        Self {
            handle,
            processed: Arc::new(AtomicU64::new(0)),
            target,
        }
    }

    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Acquire)
    }

    pub fn report(&self, bytes: u64) {
        self.processed.store(bytes, Ordering::Release);
        self.handle.preparing(bytes);
    }

    pub fn prepared(&self) {
        self.handle
            .prepared(self.target.route, self.target.target.clone());
    }
}

pub(super) struct PreparationCommitGuard {
    pub progress: Option<PreparationProgress>,
    failure_message: &'static str,
    committed: bool,
}

#[derive(Clone, Default)]
pub(super) struct OutgoingStagingLeases {
    entries: Arc<StdMutex<HashMap<String, OutgoingStagingLeaseState>>>,
}

#[derive(Default)]
struct OutgoingStagingLeaseState {
    active: usize,
    release_path: Option<PathBuf>,
    rebuild_lock: Arc<StdMutex<()>>,
}

pub(super) struct OutgoingStagingLease {
    blob_id: String,
    release_path: Option<PathBuf>,
    entries: Arc<StdMutex<HashMap<String, OutgoingStagingLeaseState>>>,
}

pub(super) struct TaskStagingCleanup {
    entries: Vec<(String, PathBuf)>,
    leases: OutgoingStagingLeases,
}

pub(super) struct RemoteDownloadClaim {
    item_id: String,
    claims: Arc<StdMutex<HashSet<String>>>,
}

impl Drop for RemoteDownloadClaim {
    fn drop(&mut self) {
        self.claims
            .lock()
            .expect("remote download claims poisoned")
            .remove(&self.item_id);
    }
}

pub(super) fn claim_remote_download(
    claims: &Arc<StdMutex<HashSet<String>>>,
    item_id: &str,
) -> Option<RemoteDownloadClaim> {
    let mut active = claims.lock().expect("remote download claims poisoned");
    if !active.insert(item_id.to_owned()) {
        return None;
    }
    Some(RemoteDownloadClaim {
        item_id: item_id.to_owned(),
        claims: claims.clone(),
    })
}

impl TaskStagingCleanup {
    pub fn new(leases: OutgoingStagingLeases) -> Self {
        Self {
            entries: Vec::new(),
            leases,
        }
    }

    pub fn track(&mut self, blob: &StoredBlob) {
        if blob.encrypted_path.is_empty() {
            return;
        }
        self.entries
            .push((blob.blob_id.clone(), PathBuf::from(&blob.encrypted_path)));
    }

    /// After committing an item snapshot, its delivery/retention cleanup owns staged content.
    pub fn retain_for_delivery(mut self) {
        self.entries.clear();
    }

    pub fn extend(&mut self, mut other: Self) {
        self.entries.append(&mut other.entries);
    }
}

impl Drop for TaskStagingCleanup {
    fn drop(&mut self) {
        for (blob_id, path) in self.entries.drain(..) {
            self.leases.request_release(&blob_id, path);
        }
    }
}

impl OutgoingStagingLeases {
    pub fn acquire(&self, blob_id: &str) -> OutgoingStagingLease {
        let mut entries = self
            .entries
            .lock()
            .expect("outgoing staging leases poisoned");
        entries.entry(blob_id.to_owned()).or_default().active += 1;
        OutgoingStagingLease {
            blob_id: blob_id.to_owned(),
            release_path: None,
            entries: self.entries.clone(),
        }
    }

    pub fn request_release(&self, blob_id: &str, path: PathBuf) {
        let mut entries = self
            .entries
            .lock()
            .expect("outgoing staging leases poisoned");
        let idle = {
            let state = entries.entry(blob_id.to_owned()).or_default();
            state.release_path = Some(path);
            state.active == 0
        };
        if idle {
            release_outgoing_staging_locked(&mut entries, blob_id);
        }
    }

    pub fn rebuild_lock(&self, blob_id: &str) -> Arc<StdMutex<()>> {
        self.entries
            .lock()
            .expect("outgoing staging leases poisoned")
            .entry(blob_id.to_owned())
            .or_default()
            .rebuild_lock
            .clone()
    }
}

impl OutgoingStagingLease {
    pub fn release_on_drop(&mut self, path: PathBuf) {
        self.release_path = Some(path);
    }
}

impl Drop for OutgoingStagingLease {
    fn drop(&mut self) {
        let mut entries = self
            .entries
            .lock()
            .expect("outgoing staging leases poisoned");
        let idle = {
            let Some(state) = entries.get_mut(&self.blob_id) else {
                return;
            };
            state.active = state.active.saturating_sub(1);
            if let Some(path) = self.release_path.take() {
                state.release_path = Some(path);
            }
            state.active == 0
        };
        if idle {
            release_outgoing_staging_locked(&mut entries, &self.blob_id);
        }
    }
}

fn release_outgoing_staging_locked(
    entries: &mut HashMap<String, OutgoingStagingLeaseState>,
    blob_id: &str,
) {
    let Some(state) = entries.remove(blob_id) else {
        return;
    };
    let Some(path) = state.release_path else {
        return;
    };
    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => log::debug!("defer sync staging cleanup {}: {error}", path.display()),
    }
}

impl PreparationCommitGuard {
    pub fn new(progress: Option<PreparationProgress>, failure_message: &'static str) -> Self {
        Self {
            progress,
            failure_message,
            committed: false,
        }
    }

    pub fn commit(&mut self) {
        if let Some(progress) = &self.progress {
            progress.prepared();
        }
        self.committed = true;
    }

    pub fn begin_commit(&self) -> bool {
        self.progress
            .as_ref()
            .is_none_or(|progress| progress.handle.begin_preparation_commit())
    }
}

impl Drop for PreparationCommitGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(progress) = &self.progress {
            if !progress.handle.canceled() {
                progress
                    .handle
                    .failed(progress.processed(), self.failure_message.to_owned());
            }
        }
    }
}

/// Holds every local materialization lock during quiescence or sync-group transitions.
pub(super) struct EnqueueLocks<'a> {
    _backfill_files: MutexGuard<'a, ()>,
    _realtime_files: MutexGuard<'a, ()>,
    _backfill_images: MutexGuard<'a, ()>,
    _realtime_images: MutexGuard<'a, ()>,
    _inline: MutexGuard<'a, ()>,
}

/// Keeps durable delivery state authoritative while projecting progress into short-lived UI tasks.
pub(super) struct DeliveryCoordinator {
    app: AppHandle,
    enqueue_lock: Mutex<()>,
    image_enqueue_lock: Mutex<()>,
    realtime_image_enqueue_lock: Mutex<()>,
    file_enqueue_lock: Mutex<()>,
    realtime_file_enqueue_lock: Mutex<()>,
    transition_lock: Mutex<()>,
    registry: TransferRegistry,
    staging: OutgoingStagingLeases,
    remote_download_claims: Arc<StdMutex<HashSet<String>>>,
}

impl DeliveryCoordinator {
    pub fn new(app: AppHandle) -> Self {
        Self {
            registry: TransferRegistry::new(app.clone()),
            app,
            enqueue_lock: Mutex::new(()),
            image_enqueue_lock: Mutex::new(()),
            realtime_image_enqueue_lock: Mutex::new(()),
            file_enqueue_lock: Mutex::new(()),
            realtime_file_enqueue_lock: Mutex::new(()),
            transition_lock: Mutex::new(()),
            staging: OutgoingStagingLeases::default(),
            remote_download_claims: Arc::new(StdMutex::new(HashSet::new())),
        }
    }

    pub async fn lock_enqueue(&self) -> EnqueueLocks<'_> {
        let backfill_files = self.file_enqueue_lock.lock().await;
        let realtime_files = self.realtime_file_enqueue_lock.lock().await;
        let backfill_images = self.image_enqueue_lock.lock().await;
        let realtime_images = self.realtime_image_enqueue_lock.lock().await;
        let inline = self.enqueue_lock.lock().await;
        EnqueueLocks {
            _backfill_files: backfill_files,
            _realtime_files: realtime_files,
            _backfill_images: backfill_images,
            _realtime_images: realtime_images,
            _inline: inline,
        }
    }

    /// Serializes only the matching preparation lane and priority tier so historical backfill
    /// never blocks freshly copied or manually shared images and files.
    pub async fn lock_materialization(
        &self,
        lane: Option<crate::db::sync_items::OutboxContentLane>,
        realtime_only: bool,
    ) -> MutexGuard<'_, ()> {
        match (lane, realtime_only) {
            (Some(crate::db::sync_items::OutboxContentLane::Files), true) => {
                self.realtime_file_enqueue_lock.lock().await
            }
            (Some(crate::db::sync_items::OutboxContentLane::Files), false) => {
                self.file_enqueue_lock.lock().await
            }
            (Some(crate::db::sync_items::OutboxContentLane::Image), true) => {
                self.realtime_image_enqueue_lock.lock().await
            }
            (Some(crate::db::sync_items::OutboxContentLane::Image), false) => {
                self.image_enqueue_lock.lock().await
            }
            (Some(crate::db::sync_items::OutboxContentLane::Text) | None, _) => {
                self.enqueue_lock.lock().await
            }
        }
    }

    pub fn tasks(&self) -> Vec<TransferTask> {
        self.registry.list()
    }

    pub fn clear_finished_transfers(&self) {
        self.registry.clear_finished();
    }

    pub fn start(&self, task: TransferTask) -> TransferHandle {
        self.registry.start(task)
    }

    #[cfg(target_os = "android")]
    pub fn start_local_import(&self, task: TransferTask) -> TransferHandle {
        self.registry.start_local_import(task)
    }

    #[cfg(target_os = "android")]
    pub fn local_import_canceled(&self, task_id: &str) -> bool {
        self.registry.local_import_canceled(task_id)
    }

    #[cfg(target_os = "android")]
    pub fn update_local_import(&self, task_id: &str, bytes: u64) {
        self.registry.update_local_import(task_id, bytes);
    }

    #[cfg(target_os = "android")]
    pub fn finish_local_import(&self, task_id: &str, total_bytes: u64) {
        self.registry.finish_local_import(task_id, total_bytes);
    }

    #[cfg(target_os = "android")]
    pub fn bind_local_import(&self, task_id: &str, operation_order: i64, source_revision: String) {
        self.registry
            .bind_local_import(task_id, operation_order, source_revision);
    }

    #[cfg(target_os = "android")]
    pub fn fail_local_import(&self, task_id: &str, bytes: u64, error: String) {
        self.registry.fail_local_import(task_id, bytes, error);
    }

    pub fn promote_local_import(&self, promotion: LocalImportPromotion) -> Option<TransferHandle> {
        self.registry.promote_local_import(promotion)
    }

    pub fn start_preparation(
        &self,
        task: TransferTask,
        operation_order: i64,
        source_revision: String,
    ) -> TransferHandle {
        self.registry
            .start_preparation(task, operation_order, source_revision)
    }

    pub fn new_staging_cleanup(&self) -> TaskStagingCleanup {
        TaskStagingCleanup::new(self.staging.clone())
    }

    pub fn acquire_staging(&self, blob_id: &str) -> OutgoingStagingLease {
        self.staging.acquire(blob_id)
    }

    pub fn request_staging_release(&self, blob_id: &str, path: PathBuf) {
        self.staging.request_release(blob_id, path);
    }

    pub fn staging_rebuild_lock(&self, blob_id: &str) -> Arc<StdMutex<()>> {
        self.staging.rebuild_lock(blob_id)
    }

    pub fn claim_upload(&self, target: &str, sync_key: &str) -> Option<RemoteDownloadClaim> {
        claim_remote_download(
            &self.remote_download_claims,
            &format!("upload:{target}:{sync_key}"),
        )
    }

    pub fn claim_remote_download(&self, item_id: &str) -> Option<RemoteDownloadClaim> {
        claim_remote_download(&self.remote_download_claims, item_id)
    }

    pub fn item_has_active_local_import(&self, item_id: &str) -> bool {
        self.registry.item_has_active_local_import(item_id)
    }

    pub fn complete_local_import_for_item(&self, item_id: &str) {
        self.registry.complete_local_import_for_item(item_id);
    }

    /// Cancels preparation or delivery while serializing the transition to confirmation.
    pub async fn cancel(&self, pool: &SqlitePool, task_id: &str) -> Result<DeliveryCancel> {
        if let Some(preparation) = self.registry.request_preparation_cancel(task_id) {
            if let Err(error) =
                db::sync_items::cancel_content_prep(pool, preparation.operation_order).await
            {
                let message = crate::i18n::commands::label(
                    crate::i18n::current_language(&self.app),
                    crate::i18n::commands::Key::SyncTransferCancelFailed,
                );
                self.registry
                    .finish_preparation_cancel(task_id, Some(message.to_owned()));
                return Err(error.into());
            }
            self.registry.finish_preparation_cancel(task_id, None);
            return Ok(DeliveryCancel::Preparation {
                item_id: preparation.item_id,
                source_revision: preparation.source_revision,
            });
        }

        let _transition_guard = self.transition_lock.lock().await;
        let _commit_guard = if self.registry.preparation_committing(task_id) {
            Some(self.realtime_file_enqueue_lock.lock().await)
        } else {
            None
        };
        let Some((task, transient)) = self.registry.cancellable_task(task_id) else {
            return Ok(DeliveryCancel::NotFound);
        };
        if task.direction == TransferDirection::Upload && !transient {
            repository::cancel_delivery(pool, &task.target, &task.operation_id).await?;
        }
        Ok(if self.registry.cancel(task_id).is_some() {
            DeliveryCancel::Canceled
        } else {
            DeliveryCancel::NotFound
        })
    }

    pub async fn start_upload(
        &self,
        pool: &SqlitePool,
        request: UploadStartRequest<'_>,
    ) -> Result<UploadTransferStart> {
        let UploadStartRequest {
            target,
            route,
            sync_key,
            item_id,
            operation_total_bytes,
            blob,
        } = request;
        if repository::is_delivery_canceled(pool, target, sync_key).await? {
            return Ok(UploadTransferStart::Canceled);
        }
        let Some(item_id) = item_id.map(str::to_owned) else {
            return Ok(UploadTransferStart::Ready(None));
        };
        let task_key = format!("{sync_key}\0{target}\0{}", blob.blob_id);
        let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
        match self
            .registry
            .claim_preparation(sync_key, route, target, operation_total_bytes)
        {
            PreparationClaim::Claimed(transfer) => {
                return Ok(UploadTransferStart::Ready(Some(transfer)));
            }
            PreparationClaim::Canceled => return Ok(UploadTransferStart::Canceled),
            PreparationClaim::Missing => {}
        }

        Ok(UploadTransferStart::Ready(Some(self.registry.start(
            TransferTask {
                id: task_id,
                revision: 0,
                started_at_ms: chrono::Utc::now().timestamp_millis(),
                operation_id: sync_key.to_owned(),
                item_id,
                direction: TransferDirection::Upload,
                route,
                target: target.to_owned(),
                file_name: None,
                phase: TransferPhase::Queued,
                transferred_bytes: 0,
                total_bytes: operation_total_bytes,
                speed_bytes_per_second: 0,
                elapsed_ms: 0,
                eta_seconds: None,
                error: None,
                cancellable: true,
            },
        ))))
    }

    pub async fn blocked(
        &self,
        pool: &SqlitePool,
        target: &str,
        sync_key: &str,
        error: &str,
    ) -> Result<()> {
        let _transition_guard = self.transition_lock.lock().await;
        repository::block_delivery(pool, target, sync_key, error).await?;
        self.registry.fail_upload_operation(sync_key, target, error);
        Ok(())
    }

    pub async fn failed(
        &self,
        pool: &SqlitePool,
        target: &str,
        sync_keys: &[String],
        error: &str,
    ) -> Result<()> {
        let _transition_guard = self.transition_lock.lock().await;
        repository::mark_delivery_error(pool, target, sync_keys, error).await
    }
}
