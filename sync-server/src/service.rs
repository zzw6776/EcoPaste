use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use ecopaste_sync_protocol::{
    ErrorCode, FrameError, MAX_EVENTS_PER_BATCH, Request, Response, read_frame, write_frame,
};
use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tracing::{debug, info, warn};

use crate::repository::{Repository, now_ms, validate_identifier};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static WATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const BLOB_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const GLOBAL_STREAM_ADMISSION: usize = 256;
const GLOBAL_REQUEST_CONCURRENCY: usize = 128;
const GLOBAL_BLOB_CONCURRENCY: usize = 8;
const GLOBAL_WATCH_CONCURRENCY: usize = 512;

#[derive(Debug)]
struct ActiveWatch {
    generation: u64,
    cancel: oneshot::Sender<()>,
}

struct TemporaryFileCleanup {
    path: PathBuf,
    armed: bool,
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
    group_notifications: Arc<Mutex<HashMap<String, watch::Sender<u64>>>>,
    group_sync_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    stream_admission: Arc<Semaphore>,
    request_concurrency: Arc<Semaphore>,
    blob_concurrency: Arc<Semaphore>,
    watch_concurrency: Arc<Semaphore>,
    active_watches: Arc<Mutex<HashMap<(String, String), ActiveWatch>>>,
}

impl HubService {
    pub fn new(repository: Repository, blob_root: PathBuf, max_blob_bytes: u64) -> Self {
        Self {
            repository,
            blob_root: Arc::new(blob_root),
            max_blob_bytes,
            group_notifications: Arc::new(Mutex::new(HashMap::new())),
            group_sync_locks: Arc::new(Mutex::new(HashMap::new())),
            stream_admission: Arc::new(Semaphore::new(GLOBAL_STREAM_ADMISSION)),
            request_concurrency: Arc::new(Semaphore::new(GLOBAL_REQUEST_CONCURRENCY)),
            blob_concurrency: Arc::new(Semaphore::new(GLOBAL_BLOB_CONCURRENCY)),
            watch_concurrency: Arc::new(Semaphore::new(GLOBAL_WATCH_CONCURRENCY)),
            active_watches: Arc::new(Mutex::new(HashMap::new())),
        }
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

    async fn handle_stream(
        &self,
        remote_endpoint_id: &str,
        mut send: SendStream,
        mut recv: RecvStream,
        admission_permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
        connection_requests: Arc<Semaphore>,
        connection_blobs: Arc<Semaphore>,
        connection_watches: Arc<Semaphore>,
    ) -> Result<()> {
        let request: Request = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
            .await
            .context("read request frame timeout")?
            .context("read request frame")?;
        let _request_permits = if matches!(
            &request,
            Request::WatchGroupStream { .. } | Request::WatchGroupStreamV2 { .. }
        ) {
            Some((
                self.watch_concurrency.clone().acquire_owned().await?,
                connection_watches.acquire_owned().await?,
            ))
        } else if matches!(&request, Request::PutBlob { .. } | Request::GetBlob { .. }) {
            Some((
                self.blob_concurrency.clone().acquire_owned().await?,
                connection_blobs.acquire_owned().await?,
            ))
        } else {
            Some((
                self.request_concurrency.clone().acquire_owned().await?,
                connection_requests.acquire_owned().await?,
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
        let versioned_watch = matches!(&request, Request::WatchGroupStreamV2 { .. });
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
            } => {
                let started_at = Instant::now();
                let submitted_event_count = events.len();
                let newest_submitted_event_at_ms =
                    events.iter().map(|event| event.created_at_ms).max();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                if device.endpoint_id != remote_endpoint_id {
                    bail!("device endpoint identity does not match the connection");
                }
                if events.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                    bail!("too many events");
                }
                let group_lock = self.group_sync_lock(&group_id).await;
                let group_guard = group_lock.lock().await;
                self.repository.upsert_device(&group_id, &device).await?;
                let accepted_event_ids = self.repository.insert_events(&group_id, &events).await?;
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
                    .unwrap_or(after_cursor);
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
                write_frame(
                    send,
                    &Response::Synced {
                        accepted_event_ids,
                        events,
                        peers,
                        latest_cursor,
                    },
                )
                .await?;
            }
            Request::SyncV2 {
                group_id,
                access_token,
                device,
                after_cursor,
                events,
                removed_devices,
                limit,
            } => {
                let started_at = Instant::now();
                let submitted_event_count = events.len();
                let submitted_removal_count = removed_devices.len();
                let newest_submitted_event_at_ms =
                    events.iter().map(|event| event.created_at_ms).max();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
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
                        },
                    )
                    .await?;
                    return Ok(());
                }

                self.repository.upsert_device(&group_id, &device).await?;
                let accepted_event_ids = self.repository.insert_events(&group_id, &events).await?;
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
                    .unwrap_or(after_cursor);
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
                info!(
                    protocol = "v2",
                    group_ref = %group_log_ref(&group_id),
                    source_endpoint_id = remote_endpoint_id,
                    submitted_events = submitted_event_count,
                    accepted_events = accepted_event_ids.len(),
                    returned_events = events.len(),
                    submitted_removals = submitted_removal_count,
                    returned_removals = merged_removals.len(),
                    latest_cursor,
                    group_latest_cursor,
                    newest_submitted_event_at_ms = ?newest_submitted_event_at_ms,
                    accepted_at_ms = now_ms(),
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "cloud sync request completed"
                );
                write_frame(
                    send,
                    &Response::SyncedV2 {
                        accepted_event_ids,
                        events,
                        peers,
                        latest_cursor,
                        removed_devices: merged_removals,
                    },
                )
                .await?;
            }
            Request::PutBlob {
                group_id,
                access_token,
                blob_id,
                size,
            } => {
                let started_at = Instant::now();
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                validate_blob(&blob_id, size, self.max_blob_bytes)?;
                let destination = blob_path(&self.blob_root, &group_id, &blob_id);
                let lookup_started_at = Instant::now();
                let already_stored = self.repository.blob_size(&group_id, &blob_id).await?
                    == Some(size)
                    && destination.is_file();
                let lookup_elapsed = lookup_started_at.elapsed();
                if already_stored {
                    let response_started_at = Instant::now();
                    write_frame(send, &Response::BlobStored).await?;
                    info!(
                        group_ref = %group_log_ref(&group_id),
                        source_endpoint_id = remote_endpoint_id,
                        blob_ref = %blob_log_ref(&blob_id),
                        outcome = "already-stored",
                        blob_bytes = size,
                        uploaded_bytes = 0_u64,
                        lookup_ms = lookup_elapsed.as_millis(),
                        response_write_ms = response_started_at.elapsed().as_millis(),
                        total_ms = started_at.elapsed().as_millis(),
                        "cloud blob request completed"
                    );
                } else {
                    let readiness_started_at = Instant::now();
                    write_frame(send, &Response::BlobReady { size: 0 }).await?;
                    let readiness_elapsed = readiness_started_at.elapsed();
                    let receive_started_at = Instant::now();
                    receive_blob(recv, &destination, &blob_id, size).await?;
                    let receive_elapsed = receive_started_at.elapsed();
                    self.repository
                        .record_blob(&group_id, &blob_id, size)
                        .await?;
                    let confirmation_started_at = Instant::now();
                    write_frame(send, &Response::BlobStored).await?;
                    info!(
                        group_ref = %group_log_ref(&group_id),
                        source_endpoint_id = remote_endpoint_id,
                        blob_ref = %blob_log_ref(&blob_id),
                        outcome = "uploaded",
                        blob_bytes = size,
                        uploaded_bytes = size,
                        lookup_ms = lookup_elapsed.as_millis(),
                        response_write_ms = readiness_elapsed.as_millis(),
                        receive_ms = receive_elapsed.as_millis(),
                        confirmation_write_ms = confirmation_started_at.elapsed().as_millis(),
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
                let size = self
                    .repository
                    .blob_size(&group_id, &blob_id)
                    .await?
                    .context("blob not found")?;
                let source = blob_path(&self.blob_root, &group_id, &blob_id);
                write_frame(send, &Response::BlobReady { size }).await?;
                let mut file = tokio::fs::File::open(source)
                    .await
                    .context("open encrypted blob")?;
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
                let mut latest_cursor = self.repository.latest_cursor(&group_id).await?;
                let mut latest_removed_at_ms =
                    self.repository.latest_removed_at_ms(&group_id).await?;
                if latest_cursor <= after_cursor && latest_removed_at_ms <= after_removed_at_ms {
                    let _ = tokio::time::timeout(Duration::from_secs(60), changes.changed()).await;
                    latest_cursor = self.repository.latest_cursor(&group_id).await?;
                    latest_removed_at_ms = self.repository.latest_removed_at_ms(&group_id).await?;
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
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                self.ensure_endpoint_active(&group_id, remote_endpoint_id)
                    .await?;
                let mut changes = self.group_changes(&group_id).await;
                let mut latest_cursor = self.repository.latest_cursor(&group_id).await?;
                let mut latest_removed_at_ms =
                    self.repository.latest_removed_at_ms(&group_id).await?;
                let (watch_generation, mut cancelled) =
                    self.register_watch(&group_id, remote_endpoint_id).await;
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
                            latest_cursor = self.repository.latest_cursor(&group_id).await?;
                            latest_removed_at_ms =
                                self.repository.latest_removed_at_ms(&group_id).await?;
                        }
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
                self.unregister_watch(&group_id, remote_endpoint_id, watch_generation)
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
        let connection_requests = Arc::new(Semaphore::new(16));
        let connection_blobs = Arc::new(Semaphore::new(2));
        let connection_watches = Arc::new(Semaphore::new(4));
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
            let connection_requests = connection_requests.clone();
            let connection_blobs = connection_blobs.clone();
            let connection_watches = connection_watches.clone();
            tokio::spawn(async move {
                if let Err(error) = service
                    .handle_stream(
                        &remote_endpoint_id,
                        send,
                        recv,
                        (global_admission, connection_admission),
                        connection_requests,
                        connection_blobs,
                        connection_watches,
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
    let result = async {
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
    .await;
    result
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
    let code = if message == "unauthorized" {
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
