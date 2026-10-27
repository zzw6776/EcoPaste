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
    ErrorCode, FrameError, HubStatus, MAX_EVENTS_PER_BATCH, MAX_FRAME_BYTES, Request, Response,
    limit_sync_response, read_frame, write_frame,
};
use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tracing::{debug, info, warn};

use crate::repository::{
    CleanupPolicy, HistoryGap, QuotaExceeded, Repository, now_ms, validate_identifier,
};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static WATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const BLOB_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const BACKUP_WATCH_NOTIFICATION_DELAY: Duration = Duration::from_millis(100);
const PRIMARY_WATCH_SLOT: u8 = 0;
const BACKUP_WATCH_SLOT: u8 = 1;
const GLOBAL_STREAM_ADMISSION: usize = 256;
const GLOBAL_REQUEST_CONCURRENCY: usize = 128;
const GLOBAL_BLOB_CONCURRENCY: usize = 8;
const GLOBAL_WATCH_CONCURRENCY: usize = 512;
const HUB_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const HUB_CLEANUP_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const HUB_ORPHAN_GRACE: Duration = Duration::from_secs(5 * 60);
const HUB_CLEANUP_BATCH_SIZE: u32 = 500;

#[derive(Debug)]
struct ActiveWatch {
    generation: u64,
    cancel: oneshot::Sender<()>,
}

type ActiveWatchRegistry = Arc<Mutex<HashMap<(String, String, u8), ActiveWatch>>>;

#[derive(Clone)]
struct ConnectionLimits {
    requests: Arc<Semaphore>,
    blobs: Arc<Semaphore>,
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
    max_blob_bytes: u64,
    max_group_bytes: u64,
    event_retention_days: u32,
    max_events_per_group: u64,
    blob_quota_states: Arc<StdMutex<HashMap<String, BlobQuotaState>>>,
    quota_cleanup_targets: Arc<StdMutex<HashMap<String, u64>>>,
    cleanup_wake: Arc<Notify>,
    group_notifications: Arc<Mutex<HashMap<String, watch::Sender<u64>>>>,
    group_sync_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    stream_admission: Arc<Semaphore>,
    request_concurrency: Arc<Semaphore>,
    blob_concurrency: Arc<Semaphore>,
    watch_concurrency: Arc<Semaphore>,
    active_watches: ActiveWatchRegistry,
}

impl HubService {
    pub fn new(
        repository: Repository,
        blob_root: PathBuf,
        max_blob_bytes: u64,
        max_group_bytes: u64,
        event_retention_days: u32,
        max_events_per_group: u64,
    ) -> Self {
        Self {
            repository,
            blob_root: Arc::new(blob_root),
            max_blob_bytes,
            max_group_bytes,
            event_retention_days,
            max_events_per_group,
            blob_quota_states: Arc::new(StdMutex::new(HashMap::new())),
            quota_cleanup_targets: Arc::new(StdMutex::new(HashMap::new())),
            cleanup_wake: Arc::new(Notify::new()),
            group_notifications: Arc::new(Mutex::new(HashMap::new())),
            group_sync_locks: Arc::new(Mutex::new(HashMap::new())),
            stream_admission: Arc::new(Semaphore::new(GLOBAL_STREAM_ADMISSION)),
            request_concurrency: Arc::new(Semaphore::new(GLOBAL_REQUEST_CONCURRENCY)),
            blob_concurrency: Arc::new(Semaphore::new(GLOBAL_BLOB_CONCURRENCY)),
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
                    Ok(()) => false,
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

    /// Serializes membership changes and event insertion within one sync group.
    async fn group_sync_lock(&self, group_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.group_sync_locks.lock().await;
        locks
            .entry(group_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn reserved_blob_bytes(&self, group_id: &str) -> u64 {
        self.blob_quota_states
            .lock()
            .expect("Hub blob quota reservations poisoned")
            .get(group_id)
            .map(|state| state.reserved_bytes)
            .unwrap_or(0)
    }

    fn has_active_blob_upload(&self, group_id: &str) -> bool {
        self.blob_quota_states
            .lock()
            .expect("Hub blob quota reservations poisoned")
            .get(group_id)
            .is_some_and(|state| state.active_uploads > 0)
    }

    fn quota_low_watermark(&self) -> u64 {
        let reduction =
            self.max_group_bytes / 5 + u64::from(!self.max_group_bytes.is_multiple_of(5));
        self.max_group_bytes.saturating_sub(reduction)
    }

    /// Coalesces quota pressure into the strictest requested target without blocking the request.
    fn request_quota_cleanup(&self, group_id: &str, target_bytes: u64) {
        if self.max_group_bytes == 0 {
            return;
        }
        let target_bytes = target_bytes.min(self.quota_low_watermark());
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

    /// Reserves blob capacity while the caller holds the group's sync lock.
    async fn reserve_blob_quota_locked(
        &self,
        group_id: &str,
        bytes: u64,
    ) -> Result<BlobQuotaReservation> {
        let used_bytes = self.repository.group_used_bytes(group_id).await?;
        let mut states = self
            .blob_quota_states
            .lock()
            .expect("Hub blob quota reservations poisoned");
        let reserved_bytes = states
            .get(group_id)
            .map(|state| state.reserved_bytes)
            .unwrap_or(0);
        if self.max_group_bytes > 0 {
            let required_capacity = reserved_bytes.saturating_add(bytes);
            if required_capacity > self.max_group_bytes {
                return Err(QuotaExceeded.into());
            }
            if used_bytes.saturating_add(required_capacity) > self.max_group_bytes {
                drop(states);
                self.request_quota_cleanup(
                    group_id,
                    self.max_group_bytes.saturating_sub(required_capacity),
                );
                return Err(QuotaExceeded.into());
            }
        }
        let state = states.entry(group_id.to_owned()).or_default();
        state.reserved_bytes = state.reserved_bytes.saturating_add(bytes);
        state.active_uploads = state.active_uploads.saturating_add(1);
        drop(states);
        Ok(BlobQuotaReservation {
            group_id: group_id.to_owned(),
            bytes,
            states: self.blob_quota_states.clone(),
            cleanup_targets: self.quota_cleanup_targets.clone(),
            cleanup_wake: self.cleanup_wake.clone(),
        })
    }

    /// Leaves one bounded request frame of slack so a completed blob upload can still be referenced.
    fn event_quota_limit(&self, group_id: &str) -> Option<u64> {
        (self.max_group_bytes > 0).then(|| {
            self.max_group_bytes
                .saturating_add(MAX_FRAME_BYTES as u64)
                .saturating_sub(self.reserved_blob_bytes(group_id))
        })
    }

    async fn insert_events_with_quota_recovery(
        &self,
        group_id: &str,
        events: &[ecopaste_sync_protocol::EncryptedEvent],
    ) -> Result<Vec<String>> {
        let result = self
            .repository
            .insert_events(group_id, events, self.event_quota_limit(group_id))
            .await;
        if let Err(error) = &result
            && error.chain().any(|cause| cause.is::<QuotaExceeded>())
        {
            self.request_quota_cleanup(group_id, self.quota_low_watermark());
        }
        result
    }

    async fn cleanup_all_groups(&self) -> Result<()> {
        for group_id in self.repository.list_group_ids().await? {
            let requested_quota_target = self.pending_quota_cleanup_target(&group_id);
            let cleanup_target = requested_quota_target
                .or((self.max_group_bytes > 0).then_some(self.max_group_bytes));
            let mut blocked_by_upload = false;
            loop {
                let now = now_ms();
                let retention_cutoff_ms = (self.event_retention_days > 0)
                    .then(|| now.saturating_sub(i64::from(self.event_retention_days) * 86_400_000));
                let result = {
                    let group_lock = self.group_sync_lock(&group_id).await;
                    let _group_guard = group_lock.lock().await;
                    // An upload writes its final file before recording metadata. Avoid reclaiming
                    // that path between those two steps, and keep metadata/file deletion atomic
                    // with respect to the next upload reservation.
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
                                legacy_blob_cutoff_ms: retention_cutoff_ms,
                                max_events: (self.max_events_per_group > 0)
                                    .then_some(self.max_events_per_group),
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
                if result.deleted_events > 0 || !result.deleted_blob_ids.is_empty() {
                    info!(
                        group_ref = %group_log_ref(&group_id),
                        deleted_events = result.deleted_events,
                        deleted_blobs = result.deleted_blob_ids.len(),
                        "Hub retention batch completed"
                    );
                }
                if !result.has_more {
                    break;
                }
                tokio::task::yield_now().await;
            }
            if let Some(target) = requested_quota_target
                && !blocked_by_upload
                && self.repository.group_used_bytes(&group_id).await? <= target
            {
                self.complete_quota_cleanup(&group_id, target);
            }
        }
        Ok(())
    }

    async fn register_watch(
        &self,
        group_id: &str,
        endpoint_id: &str,
        watch_slot: u8,
    ) -> (u64, oneshot::Receiver<()>) {
        let key = (group_id.to_owned(), endpoint_id.to_owned(), watch_slot);
        let generation = WATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let (cancel, receiver) = oneshot::channel();
        let mut watches = self.active_watches.lock().await;
        if let Some(previous) = watches.insert(key, ActiveWatch { generation, cancel }) {
            let _ = previous.cancel.send(());
        }
        (generation, receiver)
    }

    async fn unregister_watch(
        &self,
        group_id: &str,
        endpoint_id: &str,
        watch_slot: u8,
        generation: u64,
    ) {
        let key = (group_id.to_owned(), endpoint_id.to_owned(), watch_slot);
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
            .filter(|(watch_group_id, _, _)| watch_group_id == group_id)
            .map(|(_, endpoint_id, _)| endpoint_id.clone())
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
        let _request_permits = if matches!(
            &request,
            Request::WatchGroupStream { .. }
                | Request::WatchGroupStreamV2 { .. }
                | Request::WatchGroupStreamV3 { .. }
        ) {
            Some((
                self.watch_concurrency.clone().acquire_owned().await?,
                connection_limits.watches.acquire_owned().await?,
            ))
        } else if matches!(
            &request,
            Request::PutBlob { .. }
                | Request::PutSourceIcon { .. }
                | Request::PutManagedBlob { .. }
                | Request::PutManagedSourceIcon { .. }
                | Request::GetBlob { .. }
        ) {
            Some((
                self.blob_concurrency.clone().acquire_owned().await?,
                connection_limits.blobs.acquire_owned().await?,
            ))
        } else {
            Some((
                self.request_concurrency.clone().acquire_owned().await?,
                connection_limits.requests.acquire_owned().await?,
            ))
        };
        drop(admission_permits);
        if let Err(error) = self
            .dispatch(remote_endpoint_id, &mut send, &mut recv, request)
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
    ) -> Result<()> {
        let versioned_watch = matches!(
            &request,
            Request::WatchGroupStreamV2 { .. } | Request::WatchGroupStreamV3 { .. }
        );
        let watch_slot = match &request {
            Request::WatchGroupStreamV3 { watch_slot, .. } => *watch_slot,
            _ => PRIMARY_WATCH_SLOT,
        };
        match request {
            Request::Health => {
                write_frame(
                    send,
                    &Response::Health {
                        protocol_version: ecopaste_sync_protocol::PROTOCOL_VERSION,
                        server_time_ms: now_ms(),
                    },
                )
                .await?;
            }
            Request::HealthV2 => {
                write_frame(
                    send,
                    &Response::HealthV2 {
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
            Request::Sync {
                group_id,
                access_token,
                device,
                after_cursor,
                events,
                limit,
                byte_limited,
                events_complete,
                bounded_hub,
            } => {
                let started_at = Instant::now();
                let submitted_event_count = events.len();
                let newest_submitted_event_at_ms =
                    events.iter().map(|event| event.created_at_ms).max();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                require_typed_events_client(events_complete)?;
                if device.endpoint_id != remote_endpoint_id {
                    bail!("device endpoint identity does not match the connection");
                }
                if events.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                    bail!("too many events");
                }
                let group_lock = self.group_sync_lock(&group_id).await;
                let group_guard = group_lock.lock().await;
                if bounded_hub == Some(true) {
                    self.repository
                        .ensure_cursor_available(&group_id, after_cursor)
                        .await?;
                }
                self.repository.upsert_device(&group_id, &device).await?;
                let accepted_event_ids = self
                    .insert_events_with_quota_recovery(&group_id, &events)
                    .await?;
                let events_changed = !accepted_event_ids.is_empty();
                let events = self
                    .repository
                    .list_events(
                        &group_id,
                        after_cursor,
                        limit.clamp(1, MAX_EVENTS_PER_BATCH),
                    )
                    .await?;
                let latest_cursor = events
                    .last()
                    .map(|item| item.cursor)
                    .unwrap_or(after_cursor.max(self.repository.latest_cursor(&group_id).await?));
                let group_latest_cursor = if events_changed {
                    self.repository.latest_cursor(&group_id).await?
                } else {
                    latest_cursor
                };
                let peers = self
                    .repository
                    .list_peers(&group_id, &device.device_id)
                    .await?;
                drop(group_guard);
                if events_changed {
                    self.notify_group_change(&group_id).await;
                }
                let mut response = Response::Synced {
                    accepted_event_ids,
                    events,
                    peers,
                    latest_cursor,
                    has_more: None,
                };
                limit_sync_response(
                    &mut response,
                    after_cursor,
                    limit,
                    byte_limited == Some(true),
                )?;
                let Response::Synced {
                    accepted_event_ids,
                    events,
                    latest_cursor,
                    ..
                } = &response
                else {
                    unreachable!();
                };
                info!(
                    protocol = "legacy",
                    group_ref = %group_log_ref(&group_id),
                    source_endpoint_id = remote_endpoint_id,
                    submitted_events = submitted_event_count,
                    accepted_events = accepted_event_ids.len(),
                    returned_events = events.len(),
                    latest_cursor,
                    group_latest_cursor,
                    newest_submitted_event_at_ms = ?newest_submitted_event_at_ms,
                    accepted_at_ms = now_ms(),
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "cloud sync request completed"
                );
                write_frame(send, &response).await?;
            }
            Request::SyncV2 {
                group_id,
                access_token,
                device,
                after_cursor,
                events,
                removed_devices,
                limit,
                byte_limited,
                events_complete,
                bounded_hub,
            } => {
                let started_at = Instant::now();
                let submitted_event_count = events.len();
                let submitted_removal_count = removed_devices.len();
                let newest_submitted_event_at_ms =
                    events.iter().map(|event| event.created_at_ms).max();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                require_typed_events_client(events_complete)?;
                if device.endpoint_id != remote_endpoint_id {
                    bail!("device endpoint identity does not match the connection");
                }
                if events.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                    bail!("too many events");
                }
                if removed_devices.len() > 256 {
                    bail!("too many removed devices");
                }
                let group_lock = self.group_sync_lock(&group_id).await;
                let group_guard = group_lock.lock().await;
                if bounded_hub == Some(true) {
                    self.repository
                        .ensure_cursor_available(&group_id, after_cursor)
                        .await?;
                }

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
                        protocol = "v2",
                        submitted_events = submitted_event_count,
                        submitted_removals = submitted_removal_count,
                        returned_removals = merged_removals.len(),
                        elapsed_ms = started_at.elapsed().as_millis(),
                        "rejected cloud sync from removed endpoint"
                    );
                    write_frame(
                        send,
                        &Response::SyncedV2 {
                            accepted_event_ids: Vec::new(),
                            events: Vec::new(),
                            peers: Vec::new(),
                            latest_cursor: after_cursor,
                            removed_devices: merged_removals,
                            has_more: byte_limited.filter(|enabled| *enabled).map(|_| false),
                        },
                    )
                    .await?;
                    return Ok(());
                }

                self.repository.upsert_device(&group_id, &device).await?;
                let accepted_event_ids = self
                    .insert_events_with_quota_recovery(&group_id, &events)
                    .await?;
                let events_changed = !accepted_event_ids.is_empty();
                let events = self
                    .repository
                    .list_events(
                        &group_id,
                        after_cursor,
                        limit.clamp(1, MAX_EVENTS_PER_BATCH),
                    )
                    .await?;
                let latest_cursor = events
                    .last()
                    .map(|item| item.cursor)
                    .unwrap_or(after_cursor.max(self.repository.latest_cursor(&group_id).await?));
                let group_latest_cursor = if events_changed {
                    self.repository.latest_cursor(&group_id).await?
                } else {
                    latest_cursor
                };
                let peers = self
                    .repository
                    .list_peers(&group_id, &device.device_id)
                    .await?;
                drop(group_guard);
                if removals_changed || events_changed {
                    self.notify_group_change(&group_id).await;
                }
                let returned_removal_count = merged_removals.len();
                let mut response = Response::SyncedV2 {
                    accepted_event_ids,
                    events,
                    peers,
                    latest_cursor,
                    removed_devices: merged_removals,
                    has_more: None,
                };
                limit_sync_response(
                    &mut response,
                    after_cursor,
                    limit,
                    byte_limited == Some(true),
                )?;
                let Response::SyncedV2 {
                    accepted_event_ids,
                    events,
                    latest_cursor,
                    ..
                } = &response
                else {
                    unreachable!();
                };
                info!(
                    protocol = "v2",
                    group_ref = %group_log_ref(&group_id),
                    source_endpoint_id = remote_endpoint_id,
                    submitted_events = submitted_event_count,
                    accepted_events = accepted_event_ids.len(),
                    returned_events = events.len(),
                    submitted_removals = submitted_removal_count,
                    returned_removals = returned_removal_count,
                    latest_cursor,
                    group_latest_cursor,
                    newest_submitted_event_at_ms = ?newest_submitted_event_at_ms,
                    accepted_at_ms = now_ms(),
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "cloud sync request completed"
                );
                write_frame(send, &response).await?;
            }
            request @ (Request::PutBlob { .. }
            | Request::PutSourceIcon { .. }
            | Request::PutManagedBlob { .. }
            | Request::PutManagedSourceIcon { .. }) => {
                let notify_source_icon = matches!(
                    request,
                    Request::PutSourceIcon { .. } | Request::PutManagedSourceIcon { .. }
                );
                let managed = matches!(
                    request,
                    Request::PutManagedBlob { .. } | Request::PutManagedSourceIcon { .. }
                );
                let (group_id, access_token, blob_id, size) = match request {
                    Request::PutBlob {
                        group_id,
                        access_token,
                        blob_id,
                        size,
                    }
                    | Request::PutSourceIcon {
                        group_id,
                        access_token,
                        blob_id,
                        size,
                    }
                    | Request::PutManagedBlob {
                        group_id,
                        access_token,
                        blob_id,
                        size,
                    }
                    | Request::PutManagedSourceIcon {
                        group_id,
                        access_token,
                        blob_id,
                        size,
                    } => (group_id, access_token, blob_id, size),
                    _ => unreachable!(),
                };
                let started_at = Instant::now();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                validate_blob(&blob_id, size, self.max_blob_bytes)?;
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
                        kind = if notify_source_icon { "source-icon" } else { "content" },
                        outcome = "already-stored",
                        blob_bytes = size,
                        uploaded_bytes = 0_u64,
                        lookup_ms = lookup_elapsed.as_millis(),
                        response_write_ms = response_started_at.elapsed().as_millis(),
                        total_ms = started_at.elapsed().as_millis(),
                        "cloud blob request completed"
                    );
                } else {
                    let reservation = reservation.expect("missing blob quota reservation");
                    let readiness_started_at = Instant::now();
                    write_frame(send, &Response::BlobReady { size: 0 }).await?;
                    let readiness_elapsed = readiness_started_at.elapsed();
                    let receive_started_at = Instant::now();
                    receive_blob(recv, &destination, &blob_id, size).await?;
                    let receive_elapsed = receive_started_at.elapsed();
                    {
                        let group_lock = self.group_sync_lock(&group_id).await;
                        let _group_guard = group_lock.lock().await;
                        self.repository
                            .record_blob(&group_id, &blob_id, size, managed)
                            .await?;
                        drop(reservation);
                    }
                    if notify_source_icon {
                        self.notify_group_change(&group_id).await;
                    }
                    let confirmation_started_at = Instant::now();
                    write_frame(send, &Response::BlobStored).await?;
                    info!(
                        group_ref = %group_log_ref(&group_id),
                        source_endpoint_id = remote_endpoint_id,
                        blob_ref = %blob_log_ref(&blob_id),
                        kind = if notify_source_icon { "source-icon" } else { "content" },
                        outcome = "uploaded",
                        blob_bytes = size,
                        uploaded_bytes = size,
                        lookup_ms = lookup_elapsed.as_millis(),
                        response_write_ms = readiness_elapsed.as_millis(),
                        receive_ms = receive_elapsed.as_millis(),
                        confirmation_write_ms = confirmation_started_at.elapsed().as_millis(),
                        watchers_notified = notify_source_icon,
                        total_ms = started_at.elapsed().as_millis(),
                        "cloud blob request completed"
                    );
                }
            }
            Request::GetBlob {
                group_id,
                access_token,
                blob_id,
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
                write_frame(send, &Response::BlobReady { size }).await?;
                copy_with_idle_timeout(&mut file, send, BLOB_IDLE_TIMEOUT)
                    .await
                    .context("send encrypted blob")?;
            }
            Request::Watch {
                group_id,
                access_token,
                after_cursor,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                let mut changes = self.group_changes(&group_id).await;
                let mut latest_cursor = self.repository.latest_cursor(&group_id).await?;
                if latest_cursor <= after_cursor {
                    let _ = tokio::time::timeout(Duration::from_secs(60), changes.changed()).await;
                    latest_cursor = self.repository.latest_cursor(&group_id).await?;
                }
                write_frame(send, &Response::Changed { latest_cursor }).await?;
            }
            Request::WatchGroup {
                group_id,
                access_token,
                after_cursor,
                after_removed_at_ms,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                let mut changes = self.group_changes(&group_id).await;
                let (mut latest_cursor, mut latest_removed_at_ms) =
                    self.repository.group_watermarks(&group_id).await?;
                if latest_cursor <= after_cursor && latest_removed_at_ms <= after_removed_at_ms {
                    let _ = tokio::time::timeout(Duration::from_secs(60), changes.changed()).await;
                    (latest_cursor, latest_removed_at_ms) =
                        self.repository.group_watermarks(&group_id).await?;
                }
                write_frame(
                    send,
                    &Response::GroupChanged {
                        latest_cursor,
                        latest_removed_at_ms,
                    },
                )
                .await?;
            }
            Request::WatchGroupStream {
                group_id,
                access_token,
                after_cursor,
                after_removed_at_ms,
            }
            | Request::WatchGroupStreamV2 {
                group_id,
                access_token,
                after_cursor,
                after_removed_at_ms,
            }
            | Request::WatchGroupStreamV3 {
                group_id,
                access_token,
                after_cursor,
                after_removed_at_ms,
                watch_slot: _,
            } => {
                if watch_slot > BACKUP_WATCH_SLOT {
                    bail!("invalid cloud watch slot");
                }
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                let mut changes = self.group_changes(&group_id).await;
                let (mut latest_cursor, mut latest_removed_at_ms) =
                    self.repository.group_watermarks(&group_id).await?;
                self.repository
                    .touch_device(&group_id, remote_endpoint_id)
                    .await?;
                let (watch_generation, mut cancelled) = self
                    .register_watch(&group_id, remote_endpoint_id, watch_slot)
                    .await;
                let group_ref = group_log_ref(&group_id);
                let watch_result: Result<()> = async {
                    tokio::time::timeout(
                        FRAME_WRITE_TIMEOUT,
                        write_frame(
                            send,
                            &group_changed_response(
                                versioned_watch,
                                latest_cursor,
                                latest_removed_at_ms,
                            ),
                        ),
                    )
                    .await
                    .context("write initial watch response timeout")??;
                    info!(
                        group_ref = %group_ref,
                        target_endpoint_id = remote_endpoint_id,
                        watch_generation,
                        watch_slot,
                        latest_cursor,
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
                            if watch_slot == BACKUP_WATCH_SLOT {
                                tokio::select! {
                                    _ = &mut cancelled => return Ok(()),
                                    _ = tokio::time::sleep(BACKUP_WATCH_NOTIFICATION_DELAY) => {},
                                }
                            }
                            (latest_cursor, latest_removed_at_ms) =
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
                                &group_changed_response(
                                    versioned_watch,
                                    latest_cursor.max(after_cursor),
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
                                watch_slot,
                                notification_version = *changes.borrow(),
                                latest_cursor,
                                latest_removed_at_ms,
                                sent_at_ms = now_ms(),
                                write_ms = write_started_at.elapsed().as_millis(),
                                "cloud watch notification sent"
                            );
                        }
                    }
                }
                .await;
                self.unregister_watch(&group_id, remote_endpoint_id, watch_slot, watch_generation)
                    .await;
                watch_result?;
            }
            Request::ListEvents {
                group_id,
                access_token,
                before_cursor,
                limit,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                let limit = limit.clamp(1, 100);
                let (events, next_before_cursor) = self
                    .repository
                    .list_events_before(&group_id, before_cursor, limit)
                    .await?;
                let total = self.repository.event_count(&group_id).await?;
                write_frame(
                    send,
                    &Response::EventsPage {
                        events,
                        next_before_cursor,
                        total,
                    },
                )
                .await?;
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
                            event_count: usage.event_count,
                            blob_count: usage.blob_count,
                            oldest_event_received_at_ms: usage.oldest_event_received_at_ms,
                            retention_days: self.event_retention_days,
                            max_events: self.max_events_per_group,
                        }),
                    },
                )
                .await?;
            }
            Request::BootstrapPlan {
                group_id,
                access_token,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                let (start_cursor, latest_cursor) =
                    self.repository.bootstrap_cursor_bounds(&group_id).await?;
                write_frame(
                    send,
                    &Response::BootstrapPlan {
                        start_cursor,
                        latest_cursor,
                    },
                )
                .await?;
            }
            Request::SyncRemovedDevices {
                group_id,
                access_token,
                devices,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                if devices.len() > 256 {
                    bail!("too many removed devices");
                }
                let group_lock = self.group_sync_lock(&group_id).await;
                let group_guard = group_lock.lock().await;
                let (removed, changed) = if self
                    .repository
                    .is_removed_endpoint(&group_id, remote_endpoint_id)
                    .await?
                {
                    (
                        self.repository.list_removed_devices(&group_id).await?,
                        false,
                    )
                } else {
                    self.repository
                        .merge_removed_devices(&group_id, &devices)
                        .await?
                };
                drop(group_guard);
                if changed {
                    self.notify_group_change(&group_id).await;
                }
                write_frame(send, &Response::RemovedDevices { devices: removed }).await?;
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
            blobs: Arc::new(Semaphore::new(2)),
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

fn validate_blob(blob_id: &str, size: u64, maximum: u64) -> Result<()> {
    validate_identifier("blob_id", blob_id)?;
    if blob_id.len() != 64 || !blob_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("invalid blob_id");
    }
    if size == 0 || size > maximum {
        bail!("blob is too large");
    }
    Ok(())
}

fn blob_path(root: &Path, group_id: &str, blob_id: &str) -> PathBuf {
    let group_directory = blake3::hash(group_id.as_bytes()).to_hex().to_string();
    root.join(group_directory).join(&blob_id[..2]).join(blob_id)
}

async fn receive_blob(
    recv: &mut RecvStream,
    destination: &Path,
    expected_hash: &str,
    size: u64,
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
            let read = tokio::time::timeout(BLOB_IDLE_TIMEOUT, recv.read(&mut buffer[..requested]))
                .await
                .context("blob upload idle timeout")??
                .context("blob ended early")?;
            file.write_all(&buffer[..read]).await?;
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        file.flush().await?;
        drop(file);
        let actual_hash = hasher.finalize().to_hex().to_string();
        if actual_hash != expected_hash {
            bail!("blob hash mismatch");
        }
        tokio::fs::rename(&temporary, destination)
            .await
            .context("commit encrypted blob")?;
        cleanup.armed = false;
        Ok(())
    }
    .await
}

async fn copy_with_idle_timeout<R, W>(
    reader: &mut R,
    writer: &mut W,
    idle_timeout: Duration,
) -> Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut copied = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = tokio::time::timeout(idle_timeout, reader.read(&mut buffer))
            .await
            .context("blob read idle timeout")??;
        if read == 0 {
            break;
        }
        tokio::time::timeout(idle_timeout, writer.write_all(&buffer[..read]))
            .await
            .context("blob write idle timeout")??;
        copied += read as u64;
    }
    writer.flush().await?;
    Ok(copied)
}

fn response_for_error(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    let code = if error.chain().any(|cause| cause.is::<HistoryGap>()) {
        ErrorCode::HistoryGap
    } else if error.chain().any(|cause| cause.is::<QuotaExceeded>()) {
        ErrorCode::QuotaExceeded
    } else if message == "unauthorized" {
        ErrorCode::Unauthorized
    } else if message.contains("not found") {
        ErrorCode::NotFound
    } else if message.contains("too large") || message.contains("too many") {
        ErrorCode::TooLarge
    } else {
        ErrorCode::InvalidRequest
    };
    Response::Error { code, message }
}

/// 协议 9 的发送端总会携带批次完成标记；缺失表示旧客户端，不能继续解码类型化事件。
fn require_typed_events_client(events_complete: Option<bool>) -> Result<()> {
    if events_complete.is_none() {
        bail!("同步客户端版本过低，请升级后重试");
    }

    Ok(())
}

fn group_log_ref(group_id: &str) -> String {
    blake3::hash(group_id.as_bytes()).to_hex()[..12].to_owned()
}

fn blob_log_ref(blob_id: &str) -> &str {
    blob_id.get(..12).unwrap_or(blob_id)
}

fn group_changed_response(
    versioned: bool,
    latest_cursor: u64,
    latest_removed_at_ms: i64,
) -> Response {
    if versioned {
        return Response::GroupChangedV2 {
            latest_cursor,
            latest_removed_at_ms,
            server_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
    }

    Response::GroupChanged {
        latest_cursor,
        latest_removed_at_ms,
    }
}

fn is_frame_io_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| matches!(cause.downcast_ref::<FrameError>(), Some(FrameError::Io(_))))
}

#[cfg(test)]
mod tests {
    use ecopaste_sync_protocol::{EncryptedEvent, SyncEventKind};

    use super::*;

    #[tokio::test]
    async fn quota_pressure_reclaims_headroom_before_the_next_blob_retry() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        repository
            .insert_events(
                "group_123",
                &[EncryptedEvent {
                    event_id: "event_123".into(),
                    origin_device_id: "device_123".into(),
                    origin_sequence: 1,
                    created_at_ms: 1,
                    nonce: vec![1; 24],
                    ciphertext: vec![2; 16],
                    kind: SyncEventKind::Content,
                    blob_ids: Vec::new(),
                    optional_blob_ids: Vec::new(),
                }],
                None,
            )
            .await
            .unwrap();
        let service = HubService::new(
            repository.clone(),
            directory.path().join("blobs"),
            64,
            64,
            0,
            0,
        );

        let error = match service.reserve_blob_quota_locked("group_123", 40).await {
            Ok(_) => panic!("quota reservation unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.downcast_ref::<QuotaExceeded>().is_some());
        assert_eq!(service.pending_quota_cleanup_target("group_123"), Some(24));

        service.cleanup_all_groups().await.unwrap();

        assert_eq!(repository.group_used_bytes("group_123").await.unwrap(), 0);
        assert_eq!(service.pending_quota_cleanup_target("group_123"), None);
        let reservation = service
            .reserve_blob_quota_locked("group_123", 40)
            .await
            .unwrap();
        drop(reservation);
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
            .record_blob("group_123", &blob_id, 48, true)
            .await
            .unwrap();
        repository
            .insert_events(
                "group_123",
                &[EncryptedEvent {
                    event_id: "event_123".into(),
                    origin_device_id: "device_123".into(),
                    origin_sequence: 1,
                    created_at_ms: 1,
                    nonce: vec![1; 24],
                    ciphertext: vec![2; 16],
                    kind: SyncEventKind::Content,
                    blob_ids: vec![blob_id],
                    optional_blob_ids: Vec::new(),
                }],
                None,
            )
            .await
            .unwrap();
        let service = HubService::new(
            repository.clone(),
            directory.path().join("blobs"),
            64,
            96,
            0,
            0,
        );

        let error = match service.reserve_blob_quota_locked("group_123", 32).await {
            Ok(_) => panic!("quota reservation unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.downcast_ref::<QuotaExceeded>().is_some());
        service.cleanup_all_groups().await.unwrap();

        assert_eq!(repository.group_used_bytes("group_123").await.unwrap(), 0);
        assert_eq!(service.pending_quota_cleanup_target("group_123"), None);
        let reservation = service
            .reserve_blob_quota_locked("group_123", 32)
            .await
            .unwrap();
        drop(reservation);
    }

    #[tokio::test]
    async fn quota_cleanup_target_remains_until_usage_reaches_it() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        repository
            .record_blob("group_123", &"d".repeat(64), 48, false)
            .await
            .unwrap();
        let service = HubService::new(
            repository.clone(),
            directory.path().join("blobs"),
            64,
            64,
            0,
            0,
        );

        let error = match service.reserve_blob_quota_locked("group_123", 32).await {
            Ok(_) => panic!("quota reservation unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.downcast_ref::<QuotaExceeded>().is_some());
        service.cleanup_all_groups().await.unwrap();

        assert_eq!(repository.group_used_bytes("group_123").await.unwrap(), 48);
        assert_eq!(service.pending_quota_cleanup_target("group_123"), Some(32));
    }
}
