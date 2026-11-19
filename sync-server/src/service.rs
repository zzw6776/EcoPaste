use ecopaste_sync_protocol::transfer::{self as multipart, TransferLimits, Transfers};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use ecopaste_sync_protocol::{
    ErrorCode, FrameError, HubStatus, MAX_ITEMS_PER_BATCH, QuotaPolicy, Request, Response,
    SyncItemWrite, limit_items_page, limit_sync_response, read_frame, write_frame,
};
use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tracing::{debug, info, warn};

use crate::repository::{
    CleanupPolicy, QuotaExceeded, Repository, UpsertItemsResult, now_ms, validate_identifier,
};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static WATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const BLOB_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const GLOBAL_STREAM_ADMISSION: usize = 256;
const GLOBAL_REQUEST_CONCURRENCY: usize = 128;
const GLOBAL_WATCH_CONCURRENCY: usize = 512;
const HUB_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const HUB_CLEANUP_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const HUB_ORPHAN_GRACE: Duration = Duration::from_secs(5 * 60);
const HUB_CLEANUP_BATCH_SIZE: u32 = 500;
const HUB_DISK_SAFETY_MIN_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
struct ActiveWatch {
    generation: u64,
    cancel: oneshot::Sender<()>,
}

#[derive(Debug)]
struct InsufficientStorage {
    required_bytes: u64,
    available_bytes: u64,
}

impl std::fmt::Display for InsufficientStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Hub disk space is insufficient: required {} bytes, available {} bytes",
            self.required_bytes, self.available_bytes
        )
    }
}

impl std::error::Error for InsufficientStorage {}

type ActiveWatchRegistry = Arc<Mutex<HashMap<(String, String), ActiveWatch>>>;

#[derive(Clone)]
struct ConnectionLimits {
    requests: Arc<Semaphore>,
    blobs: Arc<TransferLimits>,
    transfers: Arc<Transfers>,
    watches: Arc<Semaphore>,
}

struct TemporaryFileCleanup {
    path: PathBuf,
    armed: bool,
}

struct BlobQuotaReservation {
    group_id: String,
    bytes: u64,
    states: Arc<StdMutex<HashMap<String, BlobQuotaState>>>,
    cleanup_targets: Arc<StdMutex<HashMap<String, u64>>>,
    cleanup_wake: Arc<Notify>,
}

impl BlobQuotaReservation {
    /// Moves written bytes from the in-memory reservation into actual disk usage.
    fn consume(&mut self, written: u64) {
        let consumed = written.min(self.bytes);
        if consumed == 0 {
            return;
        }
        let mut states = self
            .states
            .lock()
            .expect("Hub blob quota reservations poisoned");
        if let Some(state) = states.get_mut(&self.group_id) {
            state.reserved_bytes = state.reserved_bytes.saturating_sub(consumed);
        }
        self.bytes = self.bytes.saturating_sub(consumed);
    }
}

#[derive(Debug, Default)]
struct BlobQuotaState {
    reserved_bytes: u64,
    active_uploads: u32,
}

impl Drop for BlobQuotaReservation {
    fn drop(&mut self) {
        {
            let mut states = self
                .states
                .lock()
                .expect("Hub blob quota reservations poisoned");
            if let Some(state) = states.get_mut(&self.group_id) {
                state.reserved_bytes = state.reserved_bytes.saturating_sub(self.bytes);
                state.active_uploads = state.active_uploads.saturating_sub(1);
                if state.active_uploads == 0 {
                    states.remove(&self.group_id);
                }
            }
        }
        if self
            .cleanup_targets
            .lock()
            .expect("Hub cleanup targets poisoned")
            .contains_key(&self.group_id)
        {
            self.cleanup_wake.notify_one();
        }
    }
}

impl Drop for TemporaryFileCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug, Clone)]
pub struct HubService {
    repository: Repository,
    blob_root: Arc<PathBuf>,
    max_group_bytes: u64,
    quota_policy: QuotaPolicy,
    item_retention_days: u32,
    max_items_per_group: u64,
    blob_quota_states: Arc<StdMutex<HashMap<String, BlobQuotaState>>>,
    quota_cleanup_targets: Arc<StdMutex<HashMap<String, u64>>>,
    cleanup_wake: Arc<Notify>,
    group_notifications: Arc<Mutex<HashMap<String, watch::Sender<u64>>>>,
    group_sync_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    stream_admission: Arc<Semaphore>,
    request_concurrency: Arc<Semaphore>,
    watch_concurrency: Arc<Semaphore>,
    active_watches: ActiveWatchRegistry,
}

impl HubService {
    pub fn new(
        repository: Repository,
        blob_root: PathBuf,
        max_group_bytes: u64,
        quota_policy: QuotaPolicy,
        item_retention_days: u32,
        max_items_per_group: u64,
    ) -> Self {
        Self {
            repository,
            blob_root: Arc::new(blob_root),
            max_group_bytes,
            quota_policy,
            item_retention_days,
            max_items_per_group,
            blob_quota_states: Arc::new(StdMutex::new(HashMap::new())),
            quota_cleanup_targets: Arc::new(StdMutex::new(HashMap::new())),
            cleanup_wake: Arc::new(Notify::new()),
            group_notifications: Arc::new(Mutex::new(HashMap::new())),
            group_sync_locks: Arc::new(Mutex::new(HashMap::new())),
            stream_admission: Arc::new(Semaphore::new(GLOBAL_STREAM_ADMISSION)),
            request_concurrency: Arc::new(Semaphore::new(GLOBAL_REQUEST_CONCURRENCY)),
            watch_concurrency: Arc::new(Semaphore::new(GLOBAL_WATCH_CONCURRENCY)),
            active_watches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Runs retention and reference reclamation independently from latency-sensitive sync calls.
    pub fn spawn_cleanup_worker(&self) {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                let retry_pending = match service.cleanup_all_groups().await {
                    Ok(retry_pending) => retry_pending,
                    Err(error) => {
                        warn!(?error, "Hub cleanup cycle failed");
                        service.has_pending_quota_cleanup()
                    }
                };
                let delay = if retry_pending {
                    HUB_CLEANUP_RETRY_INTERVAL
                } else {
                    HUB_CLEANUP_INTERVAL
                };
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = service.cleanup_wake.notified() => {}
                }
            }
        });
    }

    async fn group_changes(&self, group_id: &str) -> watch::Receiver<u64> {
        let mut notifications = self.group_notifications.lock().await;
        notifications
            .entry(group_id.to_owned())
            .or_insert_with(|| watch::channel(0).0)
            .subscribe()
    }

    async fn notify_group_change(&self, group_id: &str) {
        let mut notifications = self.group_notifications.lock().await;
        let mut notification_version = 0_u64;
        notifications
            .entry(group_id.to_owned())
            .or_insert_with(|| watch::channel(0).0)
            .send_modify(|version| {
                *version = version.wrapping_add(1);
                notification_version = *version;
            });
        info!(
            group_ref = %group_log_ref(group_id),
            notification_version,
            published_at_ms = now_ms(),
            "cloud group change published"
        );
    }

    /// Serializes membership changes and item writes within one sync group.
    async fn group_sync_lock(&self, group_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.group_sync_locks.lock().await;
        locks
            .entry(group_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn has_active_blob_upload(&self, group_id: &str) -> bool {
        self.blob_quota_states
            .lock()
            .expect("Hub blob quota reservations poisoned")
            .get(group_id)
            .is_some_and(|state| state.active_uploads > 0)
    }

    /// Coalesces quota pressure into the strictest requested target without blocking the request.
    fn request_quota_cleanup(&self, group_id: &str, target_bytes: u64) {
        let mut targets = self
            .quota_cleanup_targets
            .lock()
            .expect("Hub cleanup targets poisoned");
        targets
            .entry(group_id.to_owned())
            .and_modify(|target| *target = (*target).min(target_bytes))
            .or_insert(target_bytes);
        drop(targets);
        self.cleanup_wake.notify_one();
    }

    fn pending_quota_cleanup_target(&self, group_id: &str) -> Option<u64> {
        self.quota_cleanup_targets
            .lock()
            .expect("Hub cleanup targets poisoned")
            .get(group_id)
            .copied()
    }

    fn complete_quota_cleanup(&self, group_id: &str, handled_target: u64) {
        let mut targets = self
            .quota_cleanup_targets
            .lock()
            .expect("Hub cleanup targets poisoned");
        if targets.get(group_id) == Some(&handled_target) {
            targets.remove(group_id);
        }
    }

    fn has_pending_quota_cleanup(&self) -> bool {
        !self
            .quota_cleanup_targets
            .lock()
            .expect("Hub cleanup targets poisoned")
            .is_empty()
    }

    async fn request_policy_cleanup_if_needed(&self, group_id: &str) {
        if self.max_group_bytes == 0 {
            return;
        }
        match self.repository.group_used_bytes(group_id).await {
            Ok(used_bytes) => {
                if let Some(target_bytes) = self
                    .quota_policy
                    .cleanup_target_bytes(self.max_group_bytes, used_bytes)
                {
                    self.request_quota_cleanup(group_id, target_bytes);
                }
            }
            Err(error) => warn!(?error, "measure Hub group after write failed"),
        }
    }

    /// Reserves blob capacity while the caller holds the group's sync lock.
    async fn reserve_blob_quota_locked(
        &self,
        group_id: &str,
        disk_bytes: u64,
        logical_bytes: u64,
    ) -> Result<BlobQuotaReservation> {
        let used_bytes = self.repository.group_used_bytes(group_id).await?;
        let mut states = self
            .blob_quota_states
            .lock()
            .expect("Hub blob quota reservations poisoned");
        let all_reserved_bytes = states.values().fold(0_u64, |total, state| {
            total.saturating_add(state.reserved_bytes)
        });
        let safety_bytes = HUB_DISK_SAFETY_MIN_BYTES.max(disk_bytes / 20);
        let required_disk_bytes = all_reserved_bytes
            .saturating_add(disk_bytes)
            .saturating_add(safety_bytes);
        let disk_probe_path = if self.blob_root.exists() {
            self.blob_root.as_path()
        } else {
            self.blob_root
                .parent()
                .context("Hub blob root has no parent")?
        };
        let available_disk_bytes =
            fs2::available_space(disk_probe_path).context("measure Hub available disk space")?;
        if required_disk_bytes > available_disk_bytes {
            drop(states);
            self.request_quota_cleanup(
                group_id,
                disk_pressure_cleanup_target(used_bytes, required_disk_bytes, available_disk_bytes),
            );
            return Err(InsufficientStorage {
                required_bytes: required_disk_bytes,
                available_bytes: available_disk_bytes,
            }
            .into());
        }
        let state = states.entry(group_id.to_owned()).or_default();
        state.reserved_bytes = state.reserved_bytes.saturating_add(disk_bytes);
        state.active_uploads = state.active_uploads.saturating_add(1);
        drop(states);
        if self.max_group_bytes > 0
            && self
                .quota_policy
                .cleanup_target_bytes(
                    self.max_group_bytes,
                    used_bytes.saturating_add(logical_bytes),
                )
                .is_some()
        {
            self.request_quota_cleanup(
                group_id,
                self.quota_policy.target_bytes(self.max_group_bytes),
            );
        }
        Ok(BlobQuotaReservation {
            group_id: group_id.to_owned(),
            bytes: disk_bytes,
            states: self.blob_quota_states.clone(),
            cleanup_targets: self.quota_cleanup_targets.clone(),
            cleanup_wake: self.cleanup_wake.clone(),
        })
    }

    async fn upsert_items_with_quota_recovery(
        &self,
        group_id: &str,
        writes: &[SyncItemWrite],
    ) -> Result<UpsertItemsResult> {
        let result = self.repository.upsert_items(group_id, writes, None).await;
        if matches!(&result, Ok(upserted) if !upserted.accepted.is_empty()) {
            self.request_policy_cleanup_if_needed(group_id).await;
        }
        result
    }

    /// Reconciles blob files with durable metadata while no upload can publish a new path.
    async fn reconcile_blob_storage(&self) -> Result<bool> {
        let known_groups = self
            .repository
            .list_group_ids()
            .await?
            .into_iter()
            .map(|group_id| {
                let directory = blake3::hash(group_id.as_bytes()).to_hex().to_string();
                (directory, group_id)
            })
            .collect::<HashMap<_, _>>();
        let mut groups = match tokio::fs::read_dir(self.blob_root.as_path()).await {
            Ok(groups) => groups,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error).context("read Hub blob root"),
        };
        let mut retry_pending = false;
        while let Some(group_entry) = groups.next_entry().await? {
            let group_path = group_entry.path();
            let group_type = group_entry.file_type().await?;
            if !group_type.is_dir() {
                if let Err(error) = tokio::fs::remove_file(&group_path).await
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    retry_pending = true;
                    warn!(path = %group_path.display(), ?error, "remove unexpected Hub blob entry failed");
                }
                continue;
            }
            let group_directory = group_entry.file_name();
            let Some(group_id) = group_directory
                .to_str()
                .and_then(|directory| known_groups.get(directory))
                .cloned()
            else {
                continue;
            };
            let group_lock = self.group_sync_lock(&group_id).await;
            let _group_guard = group_lock.lock().await;
            if self.has_active_blob_upload(&group_id) {
                continue;
            }
            let live_blob_ids = self.repository.blob_ids(&group_id).await?;
            let mut prefixes = match tokio::fs::read_dir(&group_path).await {
                Ok(prefixes) => prefixes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    retry_pending = true;
                    warn!(group_ref = %group_log_ref(&group_id), ?error, "read Hub group blob directory failed");
                    continue;
                }
            };
            while let Some(prefix_entry) = prefixes.next_entry().await? {
                let prefix_path = prefix_entry.path();
                if !prefix_entry.file_type().await?.is_dir() {
                    if let Err(error) = tokio::fs::remove_file(&prefix_path).await
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        retry_pending = true;
                        warn!(path = %prefix_path.display(), ?error, "remove unexpected Hub blob prefix entry failed");
                    }
                    continue;
                }
                let mut blobs = match tokio::fs::read_dir(&prefix_path).await {
                    Ok(blobs) => blobs,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        retry_pending = true;
                        warn!(path = %prefix_path.display(), ?error, "read Hub blob prefix directory failed");
                        continue;
                    }
                };
                while let Some(blob_entry) = blobs.next_entry().await? {
                    let blob_name = blob_entry.file_name();
                    let path = blob_entry.path();
                    if blob_name.to_str().is_some_and(|blob_id| {
                        live_blob_ids.contains(blob_id)
                            && path == blob_path(&self.blob_root, &group_id, blob_id)
                    }) {
                        continue;
                    }
                    let file_type = blob_entry.file_type().await?;
                    let removed = if file_type.is_dir() {
                        tokio::fs::remove_dir_all(&path).await
                    } else {
                        tokio::fs::remove_file(&path).await
                    };
                    if let Err(error) = removed
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        retry_pending = true;
                        warn!(path = %path.display(), ?error, "remove orphaned Hub blob failed");
                    }
                }
                if let Err(error) = tokio::fs::remove_dir(&prefix_path).await
                    && !matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                    )
                {
                    retry_pending = true;
                    warn!(path = %prefix_path.display(), ?error, "remove empty Hub blob prefix failed");
                }
            }
        }
        Ok(retry_pending)
    }

    async fn cleanup_all_groups(&self) -> Result<bool> {
        let mut physical_cleanup_pending = self.reconcile_blob_storage().await?;
        for group_id in self.repository.list_group_ids().await? {
            let requested_quota_target = self.pending_quota_cleanup_target(&group_id);
            let policy_target = if self.max_group_bytes > 0 {
                self.quota_policy.cleanup_target_bytes(
                    self.max_group_bytes,
                    self.repository.group_used_bytes(&group_id).await?,
                )
            } else {
                None
            };
            let cleanup_target = strictest_cleanup_target(requested_quota_target, policy_target);
            let mut blocked_by_upload = false;
            let mut history_pruned = false;
            loop {
                let now = now_ms();
                let retention_cutoff_ms = (self.item_retention_days > 0)
                    .then(|| now.saturating_sub(i64::from(self.item_retention_days) * 86_400_000));
                let result = {
                    let group_lock = self.group_sync_lock(&group_id).await;
                    let _group_guard = group_lock.lock().await;
                    if self.has_active_blob_upload(&group_id) {
                        blocked_by_upload = true;
                        break;
                    }
                    let result = self
                        .repository
                        .cleanup_group(
                            &group_id,
                            CleanupPolicy {
                                retention_cutoff_ms,
                                max_items: (self.max_items_per_group > 0)
                                    .then_some(self.max_items_per_group),
                                target_bytes: cleanup_target,
                                orphan_cutoff_ms: now.saturating_sub(
                                    i64::try_from(HUB_ORPHAN_GRACE.as_millis()).unwrap_or(i64::MAX),
                                ),
                                batch_size: HUB_CLEANUP_BATCH_SIZE,
                            },
                        )
                        .await?;
                    for blob_id in &result.deleted_blob_ids {
                        let path = blob_path(&self.blob_root, &group_id, blob_id);
                        if let Err(error) = tokio::fs::remove_file(&path).await
                            && error.kind() != std::io::ErrorKind::NotFound
                        {
                            physical_cleanup_pending = true;
                            warn!(
                                group_ref = %group_log_ref(&group_id),
                                blob_ref = %blob_log_ref(blob_id),
                                ?error,
                                "remove reclaimed Hub blob failed"
                            );
                        }
                    }
                    result
                };
                if result.deleted_items > 0 || !result.deleted_blob_ids.is_empty() {
                    info!(
                        group_ref = %group_log_ref(&group_id),
                        deleted_items = result.deleted_items,
                        deleted_blobs = result.deleted_blob_ids.len(),
                        "Hub retention batch completed"
                    );
                }
                history_pruned |= result.deleted_items > 0;
                if !result.has_more {
                    break;
                }
                tokio::task::yield_now().await;
            }
            if history_pruned {
                self.notify_group_change(&group_id).await;
            }
            if let Some(target) = requested_quota_target
                && !blocked_by_upload
                && self.repository.group_used_bytes(&group_id).await? <= target
            {
                self.complete_quota_cleanup(&group_id, target);
            }
        }
        Ok(physical_cleanup_pending)
    }

    async fn register_watch(
        &self,
        group_id: &str,
        endpoint_id: &str,
    ) -> (u64, oneshot::Receiver<()>) {
        let key = (group_id.to_owned(), endpoint_id.to_owned());
        let generation = WATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let (cancel, receiver) = oneshot::channel();
        let mut watches = self.active_watches.lock().await;
        if let Some(previous) = watches.insert(key, ActiveWatch { generation, cancel }) {
            let _ = previous.cancel.send(());
        }
        (generation, receiver)
    }

    async fn unregister_watch(&self, group_id: &str, endpoint_id: &str, generation: u64) {
        let key = (group_id.to_owned(), endpoint_id.to_owned());
        let mut watches = self.active_watches.lock().await;
        if watches
            .get(&key)
            .is_some_and(|watch| watch.generation == generation)
        {
            watches.remove(&key);
        }
    }

    async fn online_endpoint_ids(&self, group_id: &str) -> HashSet<String> {
        self.active_watches
            .lock()
            .await
            .keys()
            .filter(|(watch_group_id, _)| watch_group_id == group_id)
            .map(|(_, endpoint_id)| endpoint_id.clone())
            .collect()
    }

    async fn handle_stream(
        &self,
        remote_endpoint_id: &str,
        mut send: SendStream,
        mut recv: RecvStream,
        admission_permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
        connection_limits: ConnectionLimits,
    ) -> Result<()> {
        let request: Request = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
            .await
            .context("read request frame timeout")?
            .context("read request frame")?;
        drop(admission_permits);
        if let Request::BlobPart { transfer_id, index } = request {
            return connection_limits
                .transfers
                .route(transfer_id, index, send, recv)
                .await;
        }
        let _attachment_permit = if let Some((files, sending)) = request.attachment_lane() {
            Some(connection_limits.blobs.acquire(files, sending).await?)
        } else {
            None
        };
        let _request_permits = if matches!(&request, Request::WatchCloud { .. }) {
            Some((
                self.watch_concurrency.clone().acquire_owned().await?,
                connection_limits.watches.acquire_owned().await?,
            ))
        } else if _attachment_permit.is_none() {
            Some((
                self.request_concurrency.clone().acquire_owned().await?,
                connection_limits.requests.acquire_owned().await?,
            ))
        } else {
            None
        };
        if let Err(error) = self
            .dispatch(
                remote_endpoint_id,
                &mut send,
                &mut recv,
                request,
                &connection_limits.transfers,
            )
            .await
        {
            if is_frame_io_error(&error) {
                debug!(?error, "sync stream disconnected");
                return Ok(());
            }
            warn!(?error, "sync request failed");
            tokio::time::timeout(
                FRAME_WRITE_TIMEOUT,
                write_frame(&mut send, &response_for_error(&error)),
            )
            .await
            .context("write error response timeout")?
            .context("write error response")?;
        }
        send.finish().context("finish response stream")?;
        Ok(())
    }

    async fn dispatch(
        &self,
        remote_endpoint_id: &str,
        send: &mut SendStream,
        recv: &mut RecvStream,
        request: Request,
        transfers: &Arc<Transfers>,
    ) -> Result<()> {
        match request {
            Request::CloudHealth => {
                write_frame(
                    send,
                    &Response::CloudHealth {
                        protocol_version: ecopaste_sync_protocol::PROTOCOL_VERSION,
                        server_time_ms: now_ms(),
                        server_version: env!("CARGO_PKG_VERSION").to_owned(),
                    },
                )
                .await?;
            }
            Request::CreateGroup {
                group_id,
                access_token,
            } => {
                self.repository
                    .create_group(&group_id, &access_token)
                    .await?;
                write_frame(send, &Response::GroupCreated).await?;
            }
            Request::CloudSyncItems {
                group_id,
                access_token,
                device,
                after_seq,
                before_seq,
                items,
                removed_devices,
                limit,
            } => {
                let started_at = Instant::now();
                let submitted_item_count = items.len();
                let submitted_removal_count = removed_devices.len();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                if device.endpoint_id != remote_endpoint_id {
                    bail!("device endpoint identity does not match the connection");
                }
                if items.len() > usize::from(MAX_ITEMS_PER_BATCH) {
                    bail!("too many items");
                }
                if removed_devices.len() > 256 {
                    bail!("too many removed devices");
                }
                let group_lock = self.group_sync_lock(&group_id).await;
                let group_guard = group_lock.lock().await;

                let remote_was_removed = self
                    .repository
                    .is_removed_device(&group_id, &device.device_id, remote_endpoint_id)
                    .await?;
                let (merged_removals, removals_changed) = if remote_was_removed {
                    (
                        self.repository.list_removed_devices(&group_id).await?,
                        false,
                    )
                } else {
                    self.repository
                        .merge_removed_devices(&group_id, &removed_devices)
                        .await?
                };
                let remote_is_removed = self
                    .repository
                    .is_removed_device(&group_id, &device.device_id, remote_endpoint_id)
                    .await?;
                if remote_is_removed {
                    drop(group_guard);
                    if removals_changed {
                        self.notify_group_change(&group_id).await;
                    }
                    info!(
                        protocol = "cloud",
                        submitted_items = submitted_item_count,
                        submitted_removals = submitted_removal_count,
                        returned_removals = merged_removals.len(),
                        elapsed_ms = started_at.elapsed().as_millis(),
                        "rejected cloud sync from removed endpoint"
                    );
                    write_frame(
                        send,
                        &Response::CloudSynced {
                            accepted: Vec::new(),
                            conflicts: Vec::new(),
                            items: Vec::new(),
                            peers: Vec::new(),
                            removed_devices: merged_removals,
                            latest_seq: after_seq,
                            pruned_through_seq: 0,
                            next_before_seq: None,
                        },
                    )
                    .await?;
                    return Ok(());
                }

                self.repository.upsert_device(&group_id, &device).await?;
                let upserted = self
                    .upsert_items_with_quota_recovery(&group_id, &items)
                    .await?;
                let items_changed = !upserted.accepted.is_empty();
                let window = self
                    .repository
                    .list_items_window(
                        &group_id,
                        after_seq,
                        before_seq,
                        items_changed.then_some(upserted.pre_write_latest_seq),
                        limit.clamp(1, MAX_ITEMS_PER_BATCH),
                    )
                    .await?;
                let peers = self
                    .repository
                    .list_peers(&group_id, &device.device_id)
                    .await?;
                drop(group_guard);
                if removals_changed || items_changed {
                    self.notify_group_change(&group_id).await;
                }

                let returned_removal_count = merged_removals.len();
                let mut response = Response::CloudSynced {
                    accepted: upserted.accepted,
                    conflicts: upserted.conflicts,
                    items: window.items,
                    peers,
                    removed_devices: merged_removals,
                    latest_seq: window.latest_seq,
                    pruned_through_seq: window.pruned_through_seq,
                    next_before_seq: None,
                };
                limit_sync_response(&mut response, window.had_more)?;
                let Response::CloudSynced {
                    accepted,
                    conflicts,
                    items,
                    latest_seq,
                    pruned_through_seq,
                    next_before_seq,
                    ..
                } = &response
                else {
                    unreachable!();
                };
                info!(
                    protocol = "cloud",
                    group_ref = %group_log_ref(&group_id),
                    source_endpoint_id = remote_endpoint_id,
                    submitted_items = submitted_item_count,
                    accepted_items = accepted.len(),
                    conflict_items = conflicts.len(),
                    returned_items = items.len(),
                    submitted_removals = submitted_removal_count,
                    returned_removals = returned_removal_count,
                    latest_seq,
                    pruned_through_seq,
                    next_before_seq = ?next_before_seq,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "cloud sync request completed"
                );
                write_frame(send, &response).await?;
            }
            request @ (Request::PutManagedBlob { .. }
            | Request::PutManagedSourceIcon { .. }
            | Request::PutManagedStreamBlob { .. }) => {
                let is_source_icon = matches!(request, Request::PutManagedSourceIcon { .. });
                let streaming = matches!(request, Request::PutManagedStreamBlob { .. });
                let (group_id, access_token, blob_id, size, transfer_id) = match request {
                    Request::PutManagedBlob {
                        group_id,
                        access_token,
                        blob_id,
                        size,
                        transfer_id,
                        file_content: _,
                    }
                    | Request::PutManagedSourceIcon {
                        group_id,
                        access_token,
                        blob_id,
                        size,
                        transfer_id,
                        file_content: _,
                    }
                    | Request::PutManagedStreamBlob {
                        group_id,
                        access_token,
                        blob_id,
                        size,
                        transfer_id,
                        file_content: _,
                    } => (group_id, access_token, blob_id, size, transfer_id),
                    _ => unreachable!(),
                };
                let started_at = Instant::now();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                validate_blob(&blob_id, size)?;
                let destination = blob_path(&self.blob_root, &group_id, &blob_id);
                let lookup_started_at = Instant::now();
                let (already_stored, reservation) = {
                    let group_lock = self.group_sync_lock(&group_id).await;
                    let _group_guard = group_lock.lock().await;
                    let existing_size = self.repository.blob_size(&group_id, &blob_id).await?;
                    let already_stored = existing_size == Some(size) && destination.is_file();
                    if already_stored {
                        self.repository.touch_blob(&group_id, &blob_id).await?;
                        (true, None)
                    } else {
                        let reservation = self
                            .reserve_blob_quota_locked(
                                &group_id,
                                size,
                                size.saturating_sub(existing_size.unwrap_or(0)),
                            )
                            .await?;
                        (false, Some(reservation))
                    }
                };
                let lookup_elapsed = lookup_started_at.elapsed();
                if already_stored {
                    let response_started_at = Instant::now();
                    write_frame(send, &Response::BlobStored).await?;
                    info!(
                        group_ref = %group_log_ref(&group_id),
                        source_endpoint_id = remote_endpoint_id,
                        blob_ref = %blob_log_ref(&blob_id),
                        kind = if is_source_icon { "source-icon" } else { "content" },
                        outcome = "already-stored",
                        blob_bytes = size,
                        uploaded_bytes = 0_u64,
                        lookup_ms = lookup_elapsed.as_millis(),
                        response_write_ms = response_started_at.elapsed().as_millis(),
                        total_ms = started_at.elapsed().as_millis(),
                        "cloud blob request completed"
                    );
                } else {
                    let mut reservation = reservation.expect("missing blob quota reservation");
                    let session = transfers.register(transfer_id, size)?;
                    let readiness_started_at = Instant::now();
                    write_frame(send, &Response::BlobReady { size: 0 }).await?;
                    let readiness_elapsed = readiness_started_at.elapsed();
                    let receive_started_at = Instant::now();
                    let (mut reader, mut writer) = tokio::io::duplex(64 * 1024);
                    let receive = multipart::receive(&mut writer, session.peer(), size, |_| {});
                    let save = receive_blob(
                        &mut reader,
                        if streaming { Some(recv) } else { None },
                        &destination,
                        &blob_id,
                        size,
                        &mut reservation,
                        streaming,
                    );
                    multipart::until_stopped(send, async {
                        tokio::try_join!(receive, save)?;
                        Ok(())
                    })
                    .await?;
                    let receive_elapsed = receive_started_at.elapsed();
                    let items_bumped = {
                        let group_lock = self.group_sync_lock(&group_id).await;
                        let _group_guard = group_lock.lock().await;
                        let bumped = self
                            .repository
                            .record_blob(&group_id, &blob_id, size)
                            .await?;
                        drop(reservation);
                        bumped
                    };
                    self.request_policy_cleanup_if_needed(&group_id).await;
                    if items_bumped {
                        self.notify_group_change(&group_id).await;
                    }
                    let confirmation_started_at = Instant::now();
                    write_frame(send, &Response::BlobStored).await?;
                    info!(
                        group_ref = %group_log_ref(&group_id),
                        source_endpoint_id = remote_endpoint_id,
                        blob_ref = %blob_log_ref(&blob_id),
                        kind = if is_source_icon { "source-icon" } else { "content" },
                        outcome = "uploaded",
                        blob_bytes = size,
                        uploaded_bytes = size,
                        lookup_ms = lookup_elapsed.as_millis(),
                        response_write_ms = readiness_elapsed.as_millis(),
                        receive_ms = receive_elapsed.as_millis(),
                        confirmation_write_ms = confirmation_started_at.elapsed().as_millis(),
                        watchers_notified = items_bumped,
                        total_ms = started_at.elapsed().as_millis(),
                        "cloud blob request completed"
                    );
                }
            }
            Request::GetBlob {
                group_id,
                access_token,
                blob_id,
                transfer_id,
                file_content: _,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                validate_identifier("blob_id", &blob_id)?;
                let (size, mut file) = {
                    let group_lock = self.group_sync_lock(&group_id).await;
                    let _group_guard = group_lock.lock().await;
                    let size = self
                        .repository
                        .blob_size(&group_id, &blob_id)
                        .await?
                        .context("blob not found")?;
                    let source = blob_path(&self.blob_root, &group_id, &blob_id);
                    let file = tokio::fs::File::open(source)
                        .await
                        .context("open encrypted blob")?;
                    (size, file)
                };
                let session = transfers.register(transfer_id, size)?;
                write_frame(send, &Response::BlobReady { size }).await?;
                multipart::until_stopped(
                    send,
                    multipart::send(&mut file, session.peer(), size, |_| {}),
                )
                .await
                .context("send encrypted blob")?;
                write_frame(send, &Response::BlobStored).await?;
            }
            Request::WatchCloud {
                group_id,
                access_token,
                after_seq,
                after_removed_at_ms,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                let mut changes = self.group_changes(&group_id).await;
                let (mut latest_seq, mut latest_removed_at_ms) =
                    self.repository.group_watermarks(&group_id).await?;
                self.repository
                    .touch_device(&group_id, remote_endpoint_id)
                    .await?;
                let (watch_generation, mut cancelled) =
                    self.register_watch(&group_id, remote_endpoint_id).await;
                let group_ref = group_log_ref(&group_id);
                let watch_result: Result<()> = async {
                    tokio::time::timeout(
                        FRAME_WRITE_TIMEOUT,
                        write_frame(
                            send,
                            &cloud_changed_response(latest_seq, latest_removed_at_ms),
                        ),
                    )
                    .await
                    .context("write initial watch response timeout")??;
                    info!(
                        group_ref = %group_ref,
                        target_endpoint_id = remote_endpoint_id,
                        watch_generation,
                        latest_seq,
                        latest_removed_at_ms,
                        ready_at_ms = now_ms(),
                        "cloud watch stream ready"
                    );

                    loop {
                        let changed = tokio::select! {
                            _ = &mut cancelled => return Ok(()),
                            result = tokio::time::timeout(
                                Duration::from_secs(60),
                                changes.changed(),
                            ) => matches!(result, Ok(Ok(()))),
                        };
                        if changed {
                            (latest_seq, latest_removed_at_ms) =
                                self.repository.group_watermarks(&group_id).await?;
                        }
                        self.repository
                            .touch_device(&group_id, remote_endpoint_id)
                            .await?;
                        let write_started_at = Instant::now();
                        tokio::time::timeout(
                            FRAME_WRITE_TIMEOUT,
                            write_frame(
                                send,
                                &cloud_changed_response(
                                    latest_seq.max(after_seq),
                                    latest_removed_at_ms.max(after_removed_at_ms),
                                ),
                            ),
                        )
                        .await
                        .context("write watch response timeout")??;
                        if changed {
                            info!(
                                group_ref = %group_ref,
                                target_endpoint_id = remote_endpoint_id,
                                watch_generation,
                                notification_version = *changes.borrow(),
                                latest_seq,
                                latest_removed_at_ms,
                                sent_at_ms = now_ms(),
                                write_ms = write_started_at.elapsed().as_millis(),
                                "cloud watch notification sent"
                            );
                        }
                    }
                }
                .await;
                self.unregister_watch(&group_id, remote_endpoint_id, watch_generation)
                    .await;
                watch_result?;
            }
            Request::ListItems {
                group_id,
                access_token,
                before_seq,
                limit,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                let limit = limit.clamp(1, 100);
                let (items, next_before_seq, total) = self
                    .repository
                    .list_active_items_page(&group_id, before_seq, limit)
                    .await?;
                let mut response = Response::ItemsPage {
                    items,
                    next_before_seq,
                    total,
                };
                limit_items_page(&mut response)?;
                write_frame(send, &response).await?;
            }
            Request::ListDevices {
                group_id,
                access_token,
                device,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                if device.endpoint_id != remote_endpoint_id {
                    bail!("device endpoint identity does not match the connection");
                }
                self.repository.upsert_device(&group_id, &device).await?;
                let online_endpoint_ids = self.online_endpoint_ids(&group_id).await;
                let mut devices = self.repository.list_devices(&group_id).await?;
                for device in &mut devices {
                    device.online = device.endpoint_id == remote_endpoint_id
                        || online_endpoint_ids.contains(&device.endpoint_id);
                }
                let usage = self.repository.group_usage(&group_id).await?;
                write_frame(
                    send,
                    &Response::Devices {
                        devices,
                        hub_status: Some(HubStatus {
                            used_bytes: usage.used_bytes,
                            max_bytes: self.max_group_bytes,
                            event_count: usage.item_count,
                            blob_count: usage.blob_count,
                            oldest_event_received_at_ms: usage.oldest_item_received_at_ms,
                            retention_days: self.item_retention_days,
                            max_events: self.max_items_per_group,
                        }),
                    },
                )
                .await?;
            }
            Request::Health
            | Request::SyncItems { .. }
            | Request::BlobPart { .. }
            | Request::SyncRemovedDevices { .. }
            | Request::AckLanItems { .. }
            | Request::CommitStreamBlob { .. } => {
                bail!("request is not part of the current cloud protocol");
            }
        }
        Ok(())
    }

    async fn ensure_endpoint_active(&self, group_id: &str, endpoint_id: &str) -> Result<()> {
        if self
            .repository
            .is_removed_endpoint(group_id, endpoint_id)
            .await?
        {
            bail!("device was removed from this sync group");
        }
        Ok(())
    }
}

impl ProtocolHandler for HubService {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_endpoint_id = connection.remote_id().to_string();
        debug!(remote = %remote_endpoint_id, "accepted sync connection");
        let connection_admission = Arc::new(Semaphore::new(8));
        let connection_limits = ConnectionLimits {
            requests: Arc::new(Semaphore::new(16)),
            blobs: Arc::new(TransferLimits::default()),
            transfers: Arc::new(Transfers::default()),
            watches: Arc::new(Semaphore::new(4)),
        };
        loop {
            let (send, recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };
            let global_admission = match self.stream_admission.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let connection_admission = match connection_admission.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let service = self.clone();
            let remote_endpoint_id = remote_endpoint_id.clone();
            let connection_limits = connection_limits.clone();
            tokio::spawn(async move {
                if let Err(error) = service
                    .handle_stream(
                        &remote_endpoint_id,
                        send,
                        recv,
                        (global_admission, connection_admission),
                        connection_limits,
                    )
                    .await
                {
                    warn!(?error, "sync stream failed");
                }
            });
        }
        Ok(())
    }
}

fn validate_blob(blob_id: &str, size: u64) -> Result<()> {
    validate_identifier("blob_id", blob_id)?;
    if blob_id.len() != 64 || !blob_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("invalid blob_id");
    }
    if size == 0 {
        bail!("blob is empty");
    }
    Ok(())
}

/// Retains all bytes not needed to cover the current disk-space shortfall.
fn disk_pressure_cleanup_target(
    used_bytes: u64,
    required_disk_bytes: u64,
    available_disk_bytes: u64,
) -> u64 {
    used_bytes.saturating_sub(required_disk_bytes.saturating_sub(available_disk_bytes))
}

fn strictest_cleanup_target(requested: Option<u64>, policy: Option<u64>) -> Option<u64> {
    requested.into_iter().chain(policy).min()
}

fn blob_path(root: &Path, group_id: &str, blob_id: &str) -> PathBuf {
    let group_directory = blake3::hash(group_id.as_bytes()).to_hex().to_string();
    root.join(group_directory).join(&blob_id[..2]).join(blob_id)
}

async fn receive_blob<R: AsyncRead + Unpin>(
    recv: &mut R,
    commit: Option<&mut RecvStream>,
    destination: &Path,
    expected_hash: &str,
    size: u64,
    reservation: &mut BlobQuotaReservation,
    streaming: bool,
) -> Result<()> {
    let parent = destination.parent().context("blob path has no parent")?;
    tokio::fs::create_dir_all(parent)
        .await
        .context("create blob directory")?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = destination.with_extension(format!("part-{}-{sequence}", now_ms()));
    let mut cleanup = TemporaryFileCleanup {
        path: temporary.clone(),
        armed: true,
    };
    async {
        let mut file = tokio::fs::File::create(&temporary)
            .await
            .context("create blob temporary file")?;
        let mut remaining = size;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        while remaining > 0 {
            let requested =
                usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let read = recv.read(&mut buffer[..requested]).await?;
            if read == 0 {
                bail!("blob ended early");
            }
            file.write_all(&buffer[..read]).await?;
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
            reservation.consume(read as u64);
        }
        file.flush().await?;
        drop(file);
        let actual_hash = hasher.finalize().to_hex().to_string();
        if !streaming && actual_hash != expected_hash {
            bail!("blob hash mismatch");
        }
        if let Some(control) = commit {
            let commit: Request = tokio::time::timeout(BLOB_IDLE_TIMEOUT, read_frame(control))
                .await
                .context("streaming blob commit timeout")??;
            if commit
                != (Request::CommitStreamBlob {
                    blob_id: expected_hash.to_owned(),
                })
            {
                bail!("invalid streaming blob commit");
            }
        }
        tokio::fs::rename(&temporary, destination)
            .await
            .context("commit encrypted blob")?;
        cleanup.armed = false;
        Ok(())
    }
    .await
}

fn response_for_error(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    let code = if error
        .chain()
        .any(|cause| cause.is::<QuotaExceeded>() || cause.is::<InsufficientStorage>())
    {
        ErrorCode::QuotaExceeded
    } else if message == "unauthorized" {
        ErrorCode::Unauthorized
    } else if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    }) || message.contains("not found")
    {
        ErrorCode::NotFound
    } else if message.contains("too large") || message.contains("too many") {
        ErrorCode::TooLarge
    } else {
        ErrorCode::InvalidRequest
    };
    Response::Error { code, message }
}

fn group_log_ref(group_id: &str) -> String {
    blake3::hash(group_id.as_bytes()).to_hex()[..12].to_owned()
}

fn blob_log_ref(blob_id: &str) -> &str {
    blob_id.get(..12).unwrap_or(blob_id)
}

fn cloud_changed_response(latest_seq: u64, latest_removed_at_ms: i64) -> Response {
    Response::CloudChanged {
        latest_seq,
        latest_removed_at_ms,
        server_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

fn is_frame_io_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| matches!(cause.downcast_ref::<FrameError>(), Some(FrameError::Io(_))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_write(item_key_char: char, ciphertext_len: usize) -> SyncItemWrite {
        SyncItemWrite {
            item_key: item_key_char.to_string().repeat(64),
            expected_hub_rev: 0,
            local_seq: 1,
            origin_device_id: "device_123456".into(),
            updated_at_ms: 1,
            deleted: false,
            protected: false,
            nonce: vec![1; 24],
            ciphertext: vec![2; ciphertext_len],
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
        }
    }

    #[test]
    fn disk_pressure_requests_only_the_missing_capacity() {
        assert_eq!(disk_pressure_cleanup_target(1_000, 600, 400), 800);
        assert_eq!(disk_pressure_cleanup_target(100, 600, 400), 0);
        assert_eq!(disk_pressure_cleanup_target(1_000, 400, 600), 1_000);
    }

    #[test]
    fn quota_cleanup_uses_the_strictest_active_target() {
        assert_eq!(strictest_cleanup_target(Some(90), Some(70)), Some(70));
        assert_eq!(strictest_cleanup_target(Some(60), Some(70)), Some(60));
        assert_eq!(strictest_cleanup_target(None, Some(70)), Some(70));
    }

    #[test]
    fn missing_blob_file_maps_to_not_found() {
        let error = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::NotFound))
            .context("open encrypted blob");

        assert!(matches!(
            response_for_error(&error),
            Response::Error {
                code: ErrorCode::NotFound,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn successful_item_write_requests_the_shared_quota_target() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let service = HubService::new(
            repository.clone(),
            directory.path().join("blobs"),
            100,
            QuotaPolicy::default(),
            0,
            0,
        );

        service
            .upsert_items_with_quota_recovery("group_123", &[sample_write('a', 68)])
            .await
            .unwrap();

        assert_eq!(repository.group_used_bytes("group_123").await.unwrap(), 92);
        assert_eq!(service.pending_quota_cleanup_target("group_123"), Some(70));
    }

    #[tokio::test]
    async fn soft_quota_allows_upload_and_schedules_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        repository
            .upsert_items("group_123", &[sample_write('a', 16)], None)
            .await
            .unwrap();
        let service = HubService::new(
            repository.clone(),
            directory.path().join("blobs"),
            64,
            QuotaPolicy::default(),
            0,
            0,
        );

        let reservation = service
            .reserve_blob_quota_locked("group_123", 40, 40)
            .await
            .unwrap();
        assert_eq!(service.pending_quota_cleanup_target("group_123"), Some(44));
        repository
            .record_blob("group_123", &"b".repeat(64), 40)
            .await
            .unwrap();
        drop(reservation);

        service.cleanup_all_groups().await.unwrap();

        assert_eq!(repository.group_used_bytes("group_123").await.unwrap(), 40);
        assert_eq!(service.pending_quota_cleanup_target("group_123"), None);
    }

    #[tokio::test]
    async fn orphaned_blob_files_are_reconciled_after_active_uploads_finish() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let live_blob = "a".repeat(64);
        let orphaned_blob = "b".repeat(64);
        repository
            .record_blob("group_123", &live_blob, 4)
            .await
            .unwrap();
        let blob_root = directory.path().join("blobs");
        let live_path = blob_path(&blob_root, "group_123", &live_blob);
        let orphaned_path = blob_path(&blob_root, "group_123", &orphaned_blob);
        tokio::fs::create_dir_all(live_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::create_dir_all(orphaned_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&live_path, b"live").await.unwrap();
        tokio::fs::write(&orphaned_path, b"orphan").await.unwrap();
        let service = HubService::new(repository, blob_root, 0, QuotaPolicy::default(), 0, 0);

        let reservation = service
            .reserve_blob_quota_locked("group_123", 1, 1)
            .await
            .unwrap();
        assert!(!service.reconcile_blob_storage().await.unwrap());
        assert!(orphaned_path.exists());

        drop(reservation);
        assert!(!service.reconcile_blob_storage().await.unwrap());
        assert!(live_path.exists());
        assert!(!orphaned_path.exists());
    }

    #[tokio::test]
    async fn quota_pressure_reclaims_recent_referenced_blob() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let blob_id = "a".repeat(64);
        repository
            .record_blob("group_123", &blob_id, 48)
            .await
            .unwrap();
        let mut write = sample_write('a', 16);
        write.blob_ids = vec![blob_id];
        repository
            .upsert_items("group_123", &[write], None)
            .await
            .unwrap();
        let service = HubService::new(
            repository.clone(),
            directory.path().join("blobs"),
            96,
            QuotaPolicy::default(),
            0,
            0,
        );

        let reservation = service
            .reserve_blob_quota_locked("group_123", 32, 32)
            .await
            .unwrap();
        drop(reservation);
        service.cleanup_all_groups().await.unwrap();

        assert_eq!(repository.group_used_bytes("group_123").await.unwrap(), 0);
        assert_eq!(service.pending_quota_cleanup_target("group_123"), None);
    }
}
