use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use ecopaste_sync_protocol::{
    ErrorCode, MAX_EVENTS_PER_BATCH, Request, Response, read_frame, write_frame,
};
use iroh::{
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, warn};

use crate::repository::{Repository, now_ms, validate_identifier};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct HubService {
    repository: Repository,
    blob_root: Arc<PathBuf>,
    max_blob_bytes: u64,
    group_notifications: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl HubService {
    pub fn new(repository: Repository, blob_root: PathBuf, max_blob_bytes: u64) -> Self {
        Self {
            repository,
            blob_root: Arc::new(blob_root),
            max_blob_bytes,
            group_notifications: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn group_notification(&self, group_id: &str) -> Arc<Notify> {
        let mut notifications = self.group_notifications.lock().await;
        notifications
            .entry(group_id.to_owned())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    async fn handle_stream(&self, mut send: SendStream, mut recv: RecvStream) -> Result<()> {
        if let Err(error) = self.dispatch(&mut send, &mut recv).await {
            warn!(?error, "sync request failed");
            write_frame(&mut send, &response_for_error(&error))
                .await
                .context("write error response")?;
        }
        send.finish().context("finish response stream")?;
        Ok(())
    }

    async fn dispatch(&self, send: &mut SendStream, recv: &mut RecvStream) -> Result<()> {
        let request: Request = read_frame(recv).await.context("read request frame")?;
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
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                if events.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                    bail!("too many events");
                }
                self.repository.upsert_device(&group_id, &device).await?;
                let accepted_event_ids = self.repository.insert_events(&group_id, &events).await?;
                if !accepted_event_ids.is_empty() {
                    self.group_notification(&group_id).await.notify_waiters();
                }
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
                let peers = self
                    .repository
                    .list_peers(&group_id, &device.device_id)
                    .await?;
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
            Request::PutBlob {
                group_id,
                access_token,
                blob_id,
                size,
            } => {
                self.repository
                    .authenticate(&group_id, &access_token)
                    .await?;
                validate_blob(&blob_id, size, self.max_blob_bytes)?;
                let destination = blob_path(&self.blob_root, &group_id, &blob_id);
                if self.repository.blob_size(&group_id, &blob_id).await? == Some(size)
                    && destination.is_file()
                {
                    write_frame(send, &Response::BlobStored).await?;
                } else {
                    write_frame(send, &Response::BlobReady { size: 0 }).await?;
                    receive_blob(recv, &destination, &blob_id, size).await?;
                    self.repository
                        .record_blob(&group_id, &blob_id, size)
                        .await?;
                    write_frame(send, &Response::BlobStored).await?;
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
                tokio::io::copy(&mut file, send)
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
                let notification = self.group_notification(&group_id).await;
                let notified = notification.notified();
                tokio::pin!(notified);
                let mut latest_cursor = self.repository.latest_cursor(&group_id).await?;
                if latest_cursor <= after_cursor {
                    let _ = tokio::time::timeout(Duration::from_secs(60), &mut notified).await;
                    latest_cursor = self.repository.latest_cursor(&group_id).await?;
                }
                write_frame(send, &Response::Changed { latest_cursor }).await?;
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
        }
        Ok(())
    }
}

impl ProtocolHandler for HubService {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        debug!(remote = %connection.remote_id(), "accepted sync connection");
        loop {
            let (send, recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };
            let service = self.clone();
            tokio::spawn(async move {
                if let Err(error) = service.handle_stream(send, recv).await {
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
            let read = recv
                .read(&mut buffer[..requested])
                .await?
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
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
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
