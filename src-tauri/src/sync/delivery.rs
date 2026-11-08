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
    model::{StoredBlob, StoredSyncEvent},
    progress::{
        MetadataOnlyDeliveryStart, PreparationClaim, TransferDirection, TransferHandle,
        TransferPhase, TransferRegistry, TransferRoute, TransferTask,
    },
    repository,
    transfer::TransferQueue,
};

pub(super) enum DeliveryCancel {
    NotFound,
    Canceled,
    Preparation {
        item_id: String,
        source_revision: String,
    },
}

pub(super) enum UploadTransferStart {
    Ready(Option<TransferHandle>),
    Canceled,
}

enum DurableDeliveryTransition<'a> {
    Queued,
    Syncing,
    Confirming,
    Failed(&'a str),
    Completed,
}

pub(super) struct PreparedReadyEvents {
    pub events: Vec<StoredSyncEvent>,
    pub metadata_only_operations: Vec<String>,
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

/// Keeps durable delivery state authoritative while projecting progress into short-lived UI tasks.
pub(super) struct DeliveryCoordinator {
    app: AppHandle,
    /// Serializes local event materialization with group changes and retention cleanup.
    enqueue_lock: Mutex<()>,
    /// Closes the cancel/confirm boundary without holding a lock during network I/O.
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
            transition_lock: Mutex::new(()),
            staging: OutgoingStagingLeases::default(),
            remote_download_claims: Arc::new(StdMutex::new(HashSet::new())),
        }
    }

    pub async fn lock_enqueue(&self) -> MutexGuard<'_, ()> {
        self.enqueue_lock.lock().await
    }

    pub fn tasks(&self) -> Vec<TransferTask> {
        self.registry.list()
    }

    pub fn start(&self, task: TransferTask) -> TransferHandle {
        self.registry.start(task)
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

    pub fn claim_remote_download(&self, item_id: &str) -> Option<RemoteDownloadClaim> {
        claim_remote_download(&self.remote_download_claims, item_id)
    }

    /// Cancels preparation or delivery while serializing the transition to confirmation.
    pub async fn cancel(&self, pool: &SqlitePool, task_id: &str) -> Result<DeliveryCancel> {
        if let Some(preparation) = self.registry.request_preparation_cancel(task_id) {
            if let Err(error) =
                db::sync_queue::complete_content(pool, preparation.operation_order).await
            {
                let message = crate::i18n::commands::label(
                    crate::i18n::current_language(&self.app),
                    crate::i18n::commands::Key::SyncTransferCancelFailed,
                );
                self.registry
                    .finish_preparation_cancel(task_id, Some(message.to_owned()));
                return Err(error);
            }
            self.registry.finish_preparation_cancel(task_id, None);
            return Ok(DeliveryCancel::Preparation {
                item_id: preparation.item_id,
                source_revision: preparation.source_revision,
            });
        }

        let _transition_guard = self.transition_lock.lock().await;
        let _commit_guard = if self.registry.preparation_committing(task_id) {
            Some(self.enqueue_lock.lock().await)
        } else {
            None
        };
        let Some(task) = self.registry.cancellable_task(task_id) else {
            return Ok(DeliveryCancel::NotFound);
        };
        if task.direction == TransferDirection::Upload {
            repository::cancel_delivery(pool, &task.target, &task.operation_id).await?;
        }
        Ok(if self.registry.cancel(task_id).is_some() {
            DeliveryCancel::Canceled
        } else {
            DeliveryCancel::NotFound
        })
    }

    /// Merges durable cancellation markers with tasks canceled after batch selection.
    async fn canceled_event_ids(
        &self,
        pool: &SqlitePool,
        target: &str,
        event_ids: &[String],
    ) -> Result<HashSet<String>> {
        repository::canceled_delivery_ids(pool, target, event_ids).await
    }

    pub async fn filter_pending(
        &self,
        pool: &SqlitePool,
        target: &str,
        pending: &mut Vec<StoredSyncEvent>,
        deferred_event_ids: &mut HashSet<String>,
    ) -> Result<()> {
        let ids = pending
            .iter()
            .map(|stored| stored.event.event_id.clone())
            .collect::<Vec<_>>();
        let canceled = self.canceled_event_ids(pool, target, &ids).await?;
        pending.retain(|stored| !canceled.contains(&stored.event.event_id));
        deferred_event_ids.retain(|event_id| !canceled.contains(event_id));
        Ok(())
    }

    pub async fn reconcile_queue(
        &self,
        pool: &SqlitePool,
        target: &str,
        queue: &mut TransferQueue,
    ) -> Result<()> {
        let canceled = self
            .canceled_event_ids(pool, target, &queue.pending_event_ids())
            .await?;
        queue.cancel_events(&canceled);
        Ok(())
    }

    pub async fn start_upload(
        &self,
        pool: &SqlitePool,
        target: &str,
        route: TransferRoute,
        event_id: &str,
        blob: &StoredBlob,
    ) -> Result<UploadTransferStart> {
        if !repository::canceled_delivery_ids(pool, target, &[event_id.to_owned()])
            .await?
            .is_empty()
        {
            return Ok(UploadTransferStart::Canceled);
        }
        let Some((item_id, operation_total_bytes)) =
            repository::transfer_item_for_event(pool, event_id).await?
        else {
            return Ok(UploadTransferStart::Ready(None));
        };
        match self.registry.claim_preparation(
            event_id,
            route,
            target,
            blob.size,
            operation_total_bytes,
        ) {
            PreparationClaim::Claimed(transfer) => {
                return Ok(UploadTransferStart::Ready(Some(transfer)));
            }
            PreparationClaim::Canceled => return Ok(UploadTransferStart::Canceled),
            PreparationClaim::Missing => {}
        }

        let task_key = format!("{event_id}\0{target}\0{}", blob.blob_id);
        let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
        Ok(UploadTransferStart::Ready(Some(self.registry.start(
            TransferTask {
                id: task_id,
                operation_id: event_id.to_owned(),
                item_id,
                direction: TransferDirection::Upload,
                route,
                target: target.to_owned(),
                file_name: None,
                phase: TransferPhase::Queued,
                transferred_bytes: 0,
                total_bytes: blob.size,
                operation_total_bytes,
                speed_bytes_per_second: 0,
                elapsed_ms: 0,
                eta_seconds: None,
                error: None,
                cancellable: true,
            },
        ))))
    }

    pub async fn queued(
        &self,
        pool: &SqlitePool,
        target: &str,
        event_ids: &[String],
    ) -> Result<()> {
        self.transition_active(pool, target, event_ids, DurableDeliveryTransition::Queued)
            .await
    }

    pub async fn syncing(
        &self,
        pool: &SqlitePool,
        target: &str,
        event_ids: &[String],
    ) -> Result<()> {
        self.transition_active(pool, target, event_ids, DurableDeliveryTransition::Syncing)
            .await
    }

    pub async fn confirming(
        &self,
        pool: &SqlitePool,
        target: &str,
        event_ids: &[String],
    ) -> Result<()> {
        self.transition_active(
            pool,
            target,
            event_ids,
            DurableDeliveryTransition::Confirming,
        )
        .await
    }

    pub async fn failed(
        &self,
        pool: &SqlitePool,
        target: &str,
        event_ids: &[String],
        error: &str,
    ) -> Result<()> {
        self.transition_active(
            pool,
            target,
            event_ids,
            DurableDeliveryTransition::Failed(error),
        )
        .await
    }

    pub async fn completed(
        &self,
        pool: &SqlitePool,
        target: &str,
        event_ids: &[String],
    ) -> Result<()> {
        self.transition_active(
            pool,
            target,
            event_ids,
            DurableDeliveryTransition::Completed,
        )
        .await
    }

    /// Serializes every durable phase change with cancellation and ignores canceled operations.
    async fn transition_active(
        &self,
        pool: &SqlitePool,
        target: &str,
        event_ids: &[String],
        transition: DurableDeliveryTransition<'_>,
    ) -> Result<()> {
        let _transition_guard = self.transition_lock.lock().await;
        let canceled = self.canceled_event_ids(pool, target, event_ids).await?;
        let active = event_ids
            .iter()
            .filter(|event_id| !canceled.contains(*event_id))
            .cloned()
            .collect::<Vec<_>>();
        if active.is_empty() {
            return Ok(());
        }

        match transition {
            DurableDeliveryTransition::Queued => {
                repository::mark_delivery_queued(pool, target, &active).await
            }
            DurableDeliveryTransition::Syncing => {
                repository::mark_delivery_syncing(pool, target, &active).await
            }
            DurableDeliveryTransition::Confirming => {
                repository::mark_delivery_confirming(pool, target, &active).await
            }
            DurableDeliveryTransition::Failed(error) => {
                repository::mark_delivery_error(pool, target, &active, error).await
            }
            DurableDeliveryTransition::Completed => {
                repository::mark_delivered(pool, target, &active).await
            }
        }
    }

    /// Selects ready events and atomically closes cancellation for metadata-only deliveries.
    pub async fn prepare_ready_events(
        &self,
        pool: &SqlitePool,
        target: &str,
        route: TransferRoute,
        deferred_event_ids: &HashSet<String>,
        queue: &mut TransferQueue,
    ) -> Result<PreparedReadyEvents> {
        let _transition_guard = self.transition_lock.lock().await;
        self.reconcile_queue(pool, target, queue).await?;
        let mut events = queue.ready_events();
        let mut metadata_only_operations = Vec::new();
        let mut canceled_metadata = HashSet::new();
        events.retain(|stored| {
            if !deferred_event_ids.contains(&stored.event.event_id) {
                return true;
            }
            match self
                .registry
                .begin_metadata_only_delivery(&stored.event.event_id, route, target)
            {
                MetadataOnlyDeliveryStart::Tracked => {
                    metadata_only_operations.push(stored.event.event_id.clone());
                    true
                }
                MetadataOnlyDeliveryStart::Canceled => {
                    canceled_metadata.insert(stored.event.event_id.clone());
                    false
                }
                MetadataOnlyDeliveryStart::Untracked => true,
            }
        });
        queue.cancel_events(&canceled_metadata);
        Ok(PreparedReadyEvents {
            events,
            metadata_only_operations,
        })
    }

    pub fn complete_metadata_only(
        &self,
        operations: &[String],
        delivered: &[String],
        route: TransferRoute,
        target: &str,
    ) {
        let delivered = delivered.iter().collect::<HashSet<_>>();
        for event_id in operations {
            if delivered.contains(event_id) {
                self.registry
                    .complete_metadata_only_preparation(event_id, route, target);
            } else {
                self.registry
                    .retry_metadata_only_delivery(event_id, route, target);
            }
        }
    }

    pub fn retry_metadata_only(&self, operations: &[String], route: TransferRoute, target: &str) {
        for event_id in operations {
            self.registry
                .retry_metadata_only_delivery(event_id, route, target);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outgoing_staging_release_waits_for_every_active_lease() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("encrypted");
        fs::write(&path, b"ciphertext").unwrap();
        let leases = OutgoingStagingLeases::default();
        let first = leases.acquire("blob");
        let second = leases.acquire("blob");

        leases.request_release("blob", path.clone());
        drop(first);
        assert!(path.exists());

        drop(second);
        assert!(!path.exists());
    }

    #[test]
    fn materialized_staging_is_deleted_when_the_task_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("encrypted");
        fs::write(&path, b"ciphertext").unwrap();
        let leases = OutgoingStagingLeases::default();
        let active_transfer = leases.acquire("blob");
        {
            let mut cleanup = TaskStagingCleanup::new(leases);
            cleanup.track(&StoredBlob {
                blob_id: "blob".into(),
                encrypted_path: path.to_string_lossy().into_owned(),
                size: 10,
                source_path: Some("source".into()),
                source_is_directory: false,
                stream_nonce: Some(vec![1; 24]),
            });
        }

        assert!(path.exists());
        drop(active_transfer);
        assert!(!path.exists());
    }

    #[test]
    fn outgoing_staging_rebuilds_share_one_lock_per_blob() {
        let leases = OutgoingStagingLeases::default();
        let _first_lease = leases.acquire("blob");
        let _second_lease = leases.acquire("blob");
        let first = leases.rebuild_lock("blob");
        let second = leases.rebuild_lock("blob");

        assert!(Arc::ptr_eq(&first, &second));
        let _guard = first.lock().unwrap();
        assert!(second.try_lock().is_err());
    }

    #[test]
    fn reproducible_staging_is_released_when_its_task_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("encrypted");
        fs::write(&path, b"ciphertext").unwrap();
        let leases = OutgoingStagingLeases::default();
        let mut lease = leases.acquire("blob");
        lease.release_on_drop(path.clone());

        drop(lease);

        assert!(!path.exists());
    }

    #[test]
    fn remote_download_claim_blocks_duplicates_until_released() {
        let claims = Arc::new(StdMutex::new(HashSet::new()));
        let claim = claim_remote_download(&claims, "item").unwrap();
        assert!(claim_remote_download(&claims, "item").is_none());

        drop(claim);

        assert!(claim_remote_download(&claims, "item").is_some());
    }
}
