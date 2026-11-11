use super::*;

#[derive(Default)]
pub(super) struct BlobUploadTimings {
    open: Duration,
    write: Duration,
    response: Duration,
    transfer: Duration,
    confirmation: Duration,
    total: Duration,
}

#[derive(Clone, Copy)]
pub(super) struct BlobDownloadProgress<'a> {
    pub(super) completed_bytes: u64,
    pub(super) handle: &'a super::super::progress::TransferHandle,
    pub(super) transferred: &'a AtomicU64,
}

impl BlobDownloadProgress<'_> {
    pub(super) fn canceled(self) -> bool {
        self.handle.canceled()
    }

    pub(super) fn transferred(self, current_bytes: u64) {
        let total = self.completed_bytes.saturating_add(current_bytes);
        self.transferred.store(total, Ordering::Release);
        self.handle.transferred(total);
    }
}

#[derive(Clone, Copy)]
pub(super) enum BlobUploadKind {
    Content,
    SourceIcon,
}

impl BlobUploadKind {
    fn label(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::SourceIcon => "source-icon",
        }
    }
}

#[derive(Clone)]
pub(super) struct BlobUploadOptions {
    pub(super) kind: BlobUploadKind,
    pub(super) managed_hub_blob: bool,
    pub(super) is_cloud: bool,
    pub(super) language: Language,
    pub(super) started: Option<mpsc::UnboundedSender<String>>,
    pub(super) transfer: Option<super::super::progress::TransferHandle>,
}

/// Uploads one attachment, checking a stalled cloud path before discarding its connection.
pub(super) async fn upload_blob(
    connection: &Connection,
    group: &GroupSecrets,
    blob: &StoredBlob,
    options: BlobUploadOptions,
) -> Result<()> {
    let _sync_activity = SyncActivityGuard::acquire();
    let started_at = Instant::now();
    if let Some(transfer) = &options.transfer {
        transfer.connecting();
    }
    let mut progress = super::super::upload::UploadProgress::new(
        connection.stable_id(),
        blob_log_ref(&blob.blob_id),
        options.transfer.clone(),
    );
    let (address, transport) = connection_path(connection);
    log::info!(
        "sync blob upload started: connectionId={} blobRef={} kind={} blobBytes={} cloud={} address={} transport={} atMs={}",
        connection.stable_id(), blob_log_ref(&blob.blob_id), options.kind.label(), blob.size,
        options.is_cloud, address.as_deref().unwrap_or("unknown"), transport.unwrap_or("unknown"), Utc::now().timestamp_millis(),
    );
    let result = upload_blob_inner(connection, group, blob, &options, &mut progress).await;
    if let Err(error) = &result {
        let probe = if options.is_cloud && super::super::upload::is_network_timeout(error) {
            check_stalled_cloud_upload(connection).await
        } else {
            "not-needed"
        };
        log::warn!(
            "sync blob upload failed: connectionId={} blobRef={} phase={:?} writtenBytes={} firstWriteMs={:?} totalMs={} probe={} closeReason={:?} stats={:?} error={error:#}",
            connection.stable_id(), blob_log_ref(&blob.blob_id), progress.phase,
            progress.written_bytes, progress.first_write_ms, started_at.elapsed().as_millis(),
            probe, connection.close_reason(), connection.stats(),
        );
    }
    if let Some(transfer) = &options.transfer {
        match &result {
            Ok(()) => transfer.completed(blob.size),
            Err(_) if transfer.canceled() => transfer.mark_canceled(progress.written_bytes),
            Err(error) => transfer.failed(progress.written_bytes, error.to_string()),
        }
    }
    result
}

/// A successful bounded control exchange distinguishes one stalled stream from a dead connection.
pub(super) async fn check_stalled_cloud_upload(connection: &Connection) -> &'static str {
    if connection.close_reason().is_some() {
        return "already-closed";
    }
    let health = tokio::time::timeout(
        CLOUD_UPLOAD_PROBE_TIMEOUT,
        call(connection, Request::CloudHealth),
    )
    .await;
    if matches!(health, Ok(Ok(Response::CloudHealth { .. }))) {
        return "healthy";
    }
    log::warn!(
        "cloud attachment stalled and health probe failed: connectionId={}",
        connection.stable_id()
    );
    connection.close(
        0_u32.into(),
        b"cloud upload stalled and health probe failed",
    );
    "failed-closed"
}

pub(super) async fn upload_blob_inner(
    connection: &Connection,
    group: &GroupSecrets,
    blob: &StoredBlob,
    options: &BlobUploadOptions,
    progress: &mut super::super::upload::UploadProgress,
) -> Result<()> {
    use super::super::upload::{self, UploadPhase};
    let total_started_at = Instant::now();
    let open_started_at = Instant::now();
    let control_timeout = if options.is_cloud {
        CLOUD_CONTROL_REQUEST_TIMEOUT
    } else {
        FIRST_FRAME_TIMEOUT
    };
    progress.phase = UploadPhase::OpenStream;
    let (mut send, mut recv) = upload::timed(
        control_timeout,
        progress.phase,
        options.language,
        connection.open_bi(),
    )
    .await?;
    let open_elapsed = open_started_at.elapsed();
    let result = async {
        let write_started_at = Instant::now();
        let streaming = blob.encrypted_path.is_empty();
        let request = match (options.kind, options.managed_hub_blob, streaming) {
            (BlobUploadKind::Content, true, true) => Request::PutManagedStreamBlob {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
            },
            (BlobUploadKind::Content, false, true) => Request::PutStreamBlob {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
            },
            (BlobUploadKind::SourceIcon, true, false) => Request::PutManagedSourceIcon {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
            },
            (BlobUploadKind::SourceIcon, false, false) => Request::PutSourceIcon {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
            },
            (BlobUploadKind::Content, true, false) => Request::PutManagedBlob {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
            },
            (BlobUploadKind::Content, false, false) => Request::PutBlob {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
            },
            (BlobUploadKind::SourceIcon, _, true) => {
                bail!("source icons cannot use streaming upload")
            }
        };
        progress.phase = UploadPhase::SendRequest;
        upload::timed(
            control_timeout,
            progress.phase,
            options.language,
            write_frame(&mut send, &request),
        )
        .await?;
        let write_elapsed = write_started_at.elapsed();
        let response_started_at = Instant::now();
        progress.phase = UploadPhase::WaitForReady;
        match upload::timed(
            control_timeout,
            progress.phase,
            options.language,
            read_frame::<_, Response>(&mut recv),
        )
        .await?
        {
            Response::BlobStored => {
                if progress.canceled() {
                    bail!("transfer canceled");
                }
                send.finish()?;
                log_blob_upload_completed(
                    connection,
                    blob,
                    options.kind,
                    "already-stored",
                    0,
                    BlobUploadTimings {
                        open: open_elapsed,
                        write: write_elapsed,
                        response: response_started_at.elapsed(),
                        total: total_started_at.elapsed(),
                        ..Default::default()
                    },
                );
                return Ok(());
            }
            Response::BlobReady { .. } => {}
            Response::Error { message, .. } => bail!(message),
            _ => bail!("invalid blob upload response"),
        }
        if progress.canceled() {
            bail!("transfer canceled");
        }
        let readiness_elapsed = response_started_at.elapsed();
        if let Some(started) = &options.started {
            let _ = started.send(blob.blob_id.clone());
        }
        log::info!(
            "sync blob upload ready: connectionId={} blobRef={} readyMs={}",
            connection.stable_id(),
            blob_log_ref(&blob.blob_id),
            total_started_at.elapsed().as_millis()
        );
        let transfer_started_at = Instant::now();
        let idle_timeout = if options.is_cloud {
            CLOUD_UPLOAD_IDLE_TIMEOUT
        } else {
            BLOB_IDLE_TIMEOUT
        };
        if streaming {
            let source = blob
                .source_path
                .as_deref()
                .map(Path::new)
                .context("streaming upload source is unavailable")?;
            let stream_nonce = blob
                .stream_nonce
                .as_deref()
                .context("streaming upload nonce is unavailable")?;
            upload::encrypt_file(
                source,
                &mut send,
                upload::FileEncryptionSpec {
                    expected_encrypted_size: blob.size,
                    expected_blob_id: &blob.blob_id,
                    key: group.content_key_bytes()?,
                    stream_nonce,
                    idle_timeout,
                    language: options.language,
                },
                progress,
            )
            .await?;
            write_frame(
                &mut send,
                &Request::CommitStreamBlob {
                    blob_id: blob.blob_id.clone(),
                },
            )
            .await?;
        } else {
            progress.phase = UploadPhase::ReadFile;
            let mut file = tokio::fs::File::open(&blob.encrypted_path).await?;
            upload::copy(
                &mut file,
                &mut send,
                idle_timeout,
                options.language,
                progress,
            )
            .await?;
        }
        send.finish()?;
        let transfer_elapsed = transfer_started_at.elapsed();
        let confirmation_started_at = Instant::now();
        progress.phase = UploadPhase::WaitForConfirmation;
        match upload::timed(
            idle_timeout,
            progress.phase,
            options.language,
            read_frame(&mut recv),
        )
        .await?
        {
            Response::BlobStored => {
                if progress.canceled() {
                    bail!("transfer canceled");
                }
                log_blob_upload_completed(
                    connection,
                    blob,
                    options.kind,
                    "uploaded",
                    blob.size,
                    BlobUploadTimings {
                        open: open_elapsed,
                        write: write_elapsed,
                        response: readiness_elapsed,
                        transfer: transfer_elapsed,
                        confirmation: confirmation_started_at.elapsed(),
                        total: total_started_at.elapsed(),
                    },
                );
                Ok(())
            }
            Response::Error { message, .. } => bail!(message),
            _ => bail!("invalid blob stored response"),
        }
    }
    .await;
    if result.is_err() {
        let _ = send.reset(0_u32.into());
        let _ = recv.stop(0_u32.into());
    }
    result
}

/// Records which stage of a blob existence check or upload consumed the elapsed time.
pub(super) fn log_blob_upload_completed(
    connection: &Connection,
    blob: &StoredBlob,
    kind: BlobUploadKind,
    outcome: &str,
    uploaded_bytes: u64,
    timings: BlobUploadTimings,
) {
    let (address, transport) = connection_path(connection);
    let rtt = connection
        .paths()
        .iter()
        .find(|path| path.is_selected())
        .map(|path| path.rtt().as_millis().to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    log::info!(
        "sync blob upload completed: blobRef={} kind={} outcome={outcome} blobBytes={} uploadedBytes={uploaded_bytes} openMs={} writeMs={} responseMs={} transferMs={} confirmationMs={} totalMs={} address={} transport={} rttMs={rtt}",
        blob_log_ref(&blob.blob_id),
        kind.label(),
        blob.size,
        timings.open.as_millis(),
        timings.write.as_millis(),
        timings.response.as_millis(),
        timings.transfer.as_millis(),
        timings.confirmation.as_millis(),
        timings.total.as_millis(),
        address.as_deref().unwrap_or("unknown"),
        transport.unwrap_or("unknown"),
    );
}

pub(super) fn blob_log_ref(blob_id: &str) -> &str {
    blob_id.get(..12).unwrap_or(blob_id)
}

/// Separates best-effort source icons from blobs required to apply clipboard content.
pub(super) fn split_source_icon_blobs(
    blobs: Vec<StoredBlob>,
    source_icon_blob_ids: &HashSet<String>,
) -> (Vec<StoredBlob>, Vec<StoredBlob>) {
    blobs
        .into_iter()
        .partition(|blob| !source_icon_blob_ids.contains(&blob.blob_id))
}

pub(super) fn source_icon_blob_ids(events: &[StoredSyncEvent], key: &[u8; 32]) -> HashSet<String> {
    events
        .iter()
        .filter_map(|stored| crypto::decrypt_event(key, &stored.event).ok())
        .filter_map(|envelope| envelope.source_app.as_ref().and_then(valid_source_icon))
        .map(|icon| icon.blob_id)
        .collect()
}

/// Marks source icons as deferrable so metadata delivery does not wait for decorative assets.
pub(super) fn mark_optional_source_icon_refs(events: &mut [StoredSyncEvent], key: &[u8; 32]) {
    for stored in events {
        let Ok(envelope) = crypto::decrypt_event(key, &stored.event) else {
            continue;
        };
        let Some(icon) = envelope.source_app.as_ref().and_then(valid_source_icon) else {
            continue;
        };
        if stored.event.blob_ids.contains(&icon.blob_id)
            && !envelope
                .blobs
                .iter()
                .any(|blob| blob.blob_id == icon.blob_id)
        {
            stored.event.optional_blob_ids.push(icon.blob_id);
        }
    }
}

/// Makes oversized file payloads optional for the Hub while retaining their encrypted metadata.
pub(super) fn mark_deferred_file_blob_refs(
    events: &mut [StoredSyncEvent],
    key: &[u8; 32],
    deferred_event_ids: &HashSet<String>,
) {
    for stored in events {
        if !deferred_event_ids.contains(&stored.event.event_id) {
            continue;
        }
        let Ok(envelope) = crypto::decrypt_event(key, &stored.event) else {
            continue;
        };
        let file_blob_ids = envelope
            .blobs
            .iter()
            .filter(|blob| blob.role == BlobRole::File)
            .map(|blob| blob.blob_id.as_str())
            .collect::<HashSet<_>>();
        for blob_id in &stored.event.blob_ids {
            if file_blob_ids.contains(blob_id.as_str())
                && !stored.event.optional_blob_ids.contains(blob_id)
            {
                stored.event.optional_blob_ids.push(blob_id.clone());
            }
        }
    }
}

/// Uploads independent encrypted blobs over a small number of parallel QUIC streams.
pub(super) async fn upload_blobs(
    manager: Arc<SyncManager>,
    connection: &Connection,
    group: &GroupSecrets,
    blobs: Vec<StoredBlob>,
    options: BlobUploadOptions,
) -> Result<()> {
    let mut waiting = blobs.into_iter();
    let mut tasks = tokio::task::JoinSet::new();
    let mut failure = None;
    loop {
        while tasks.len() < MAX_CONCURRENT_BLOB_TRANSFERS {
            if let Some(reason) = connection.close_reason() {
                failure.get_or_insert_with(|| anyhow::anyhow!(reason));
                break;
            }
            let Some(blob) = waiting.next() else {
                break;
            };
            let connection = connection.clone();
            let group = group.clone();
            let options = options.clone();
            let manager = manager.clone();
            tasks.spawn(async move {
                let (staged, _staging_lease) =
                    manager.lease_outgoing_staging(&group, &blob).await?;
                upload_blob(&connection, &group, &staged, options).await
            });
        }
        let Some(result) = tasks.join_next().await else {
            break;
        };
        if let Err(error) = result
            .context("blob upload task failed")
            .and_then(|value| value)
        {
            failure.get_or_insert(error);
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) async fn download_blob(
    connection: &Connection,
    group: &GroupSecrets,
    manifest: &BlobManifest,
    destination: &Path,
    progress: Option<BlobDownloadProgress<'_>>,
) -> Result<()> {
    let _sync_activity = SyncActivityGuard::acquire();
    if progress.is_some_and(BlobDownloadProgress::canceled) {
        bail!("transfer canceled");
    }
    let (mut send, mut recv) = tokio::time::timeout(FIRST_FRAME_TIMEOUT, connection.open_bi())
        .await
        .context("open blob download stream timeout")??;
    write_frame(
        &mut send,
        &Request::GetBlob {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            blob_id: manifest.blob_id.clone(),
        },
    )
    .await?;
    send.finish()?;
    let response = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("wait for blob download response timeout")??;
    let Response::BlobReady { size } = response else {
        bail!("blob is unavailable");
    };
    if size != manifest.encrypted_size {
        bail!("encrypted blob size mismatch");
    }
    let stream_commitment = manifest.stream_commitment;
    receive_blob(
        &mut recv,
        destination,
        &manifest.blob_id,
        size,
        !stream_commitment,
        false,
        progress,
    )
    .await
}

/// Streams one authenticated remote blob directly into the user-selected destination.
#[cfg(not(target_os = "android"))]
pub(super) async fn download_blob_to_external_path(
    manifest: &BlobManifest,
    destination: &Path,
    context: RemoteBlobTransferContext<'_>,
) -> Result<ExternalDownloadCommit> {
    let parent = destination
        .parent()
        .context("remote file destination has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let archive_or_file = parent.join(format!(
        ".{}.part-{suffix}",
        destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("ecopaste-download")
    ));
    let mut temporary_cleanup = TemporaryBlobCleanup {
        path: archive_or_file.clone(),
        armed: true,
    };
    let mut output = tokio::fs::File::create(&archive_or_file).await?;
    stream_remote_blob_to_writer(manifest, &mut output, context)
        .await
        .map_err(RemoteBlobTransportFailure::from_error)?;
    drop(output);
    if context.handle.canceled() {
        bail!("transfer canceled");
    }

    if manifest.is_directory_archive {
        let extraction = parent.join(format!(
            ".{}.part-dir-{suffix}",
            destination
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("ecopaste-directory")
        ));
        let mut extraction_cleanup = TemporaryDirectoryCleanup {
            path: extraction.clone(),
            armed: true,
        };
        let extraction_for_task = extraction.clone();
        let archive_for_task = archive_or_file.clone();
        let extraction_handle = context.handle.clone();
        tauri::async_runtime::spawn_blocking(move || {
            crypto::extract_directory_archive_with_cancel(
                &archive_for_task,
                &extraction_for_task,
                || extraction_handle.canceled(),
            )
        })
        .await
        .context("directory extraction task failed")??;
        if context.handle.canceled() {
            bail!("transfer canceled");
        }
        let commit = commit_external_download(&extraction, destination).await?;
        extraction_cleanup.armed = false;
        match tokio::fs::remove_file(&archive_or_file).await {
            Ok(()) => temporary_cleanup.armed = false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                temporary_cleanup.armed = false;
            }
            Err(error) => {
                log::warn!("remove downloaded directory archive failed: {error}");
            }
        }
        Ok(commit)
    } else {
        let commit = commit_external_download(&archive_or_file, destination).await?;
        temporary_cleanup.armed = false;
        Ok(commit)
    }
}

/// Replaces an existing external destination without exposing a delete-before-rename window.
#[cfg(not(target_os = "android"))]
pub(super) async fn commit_external_download(
    temporary: &Path,
    destination: &Path,
) -> Result<ExternalDownloadCommit> {
    if !tokio::fs::try_exists(destination).await? {
        tokio::fs::rename(temporary, destination).await?;
        return Ok(ExternalDownloadCommit {
            backup: None,
            destination: destination.to_path_buf(),
        });
    }

    let parent = destination
        .parent()
        .context("remote file destination has no parent")?;
    let backup = parent.join(format!(
        ".{}.backup-{}",
        destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("ecopaste-download"),
        Uuid::new_v4().simple()
    ));
    tokio::fs::rename(destination, &backup).await?;
    match tokio::fs::rename(temporary, destination).await {
        Ok(()) => Ok(ExternalDownloadCommit {
            backup: Some(backup),
            destination: destination.to_path_buf(),
        }),
        Err(commit_error) => match tokio::fs::rename(&backup, destination).await {
            Ok(()) => Err(commit_error.into()),
            Err(restore_error) => bail!(
                "下载文件替换失败，原文件保留在 {}：写入错误：{}；恢复错误：{}",
                backup.display(),
                commit_error,
                restore_error
            ),
        },
    }
}

#[cfg(not(target_os = "android"))]
pub(super) struct ExternalDownloadCommit {
    backup: Option<PathBuf>,
    destination: PathBuf,
}

#[cfg(not(target_os = "android"))]
impl ExternalDownloadCommit {
    pub(super) fn destination(&self) -> &Path {
        &self.destination
    }

    pub(super) async fn finalize(self) {
        let Some(backup) = self.backup else {
            return;
        };
        if let Err(error) = remove_external_path(&backup).await {
            log::warn!(
                "remove replaced remote download backup {} failed: {error}",
                backup.display()
            );
        }
    }

    pub(super) async fn rollback(self) {
        match remove_external_path(&self.destination).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                log::warn!(
                    "rollback remote download {} failed: {error}",
                    self.destination.display()
                );
                return;
            }
        }
        let Some(backup) = self.backup else {
            return;
        };
        if let Err(error) = tokio::fs::rename(&backup, &self.destination).await {
            log::warn!(
                "restore replaced remote download {} from {} failed: {error}",
                self.destination.display(),
                backup.display()
            );
        }
    }
}

#[cfg(not(target_os = "android"))]
pub(super) async fn remove_external_path(path: &Path) -> std::io::Result<()> {
    if tokio::fs::metadata(path).await?.is_dir() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    }
}

#[cfg(not(target_os = "android"))]
pub(super) async fn finalize_external_downloads(commits: Vec<ExternalDownloadCommit>) {
    for commit in commits {
        commit.finalize().await;
    }
}

#[cfg(not(target_os = "android"))]
pub(super) async fn rollback_external_downloads(commits: Vec<ExternalDownloadCommit>) {
    for commit in commits.into_iter().rev() {
        commit.rollback().await;
    }
}

/// Authenticates a remote encrypted blob while streaming plaintext to an arbitrary destination.
pub(super) async fn stream_remote_blob_to_writer<W>(
    manifest: &BlobManifest,
    output: &mut W,
    context: RemoteBlobTransferContext<'_>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    validate_blob_manifest(manifest)?;
    let (mut send, mut recv) =
        tokio::time::timeout(FIRST_FRAME_TIMEOUT, context.connection.open_bi())
            .await
            .context("open remote file download stream timeout")??;
    write_frame(
        &mut send,
        &Request::GetBlob {
            group_id: context.group.group_id.clone(),
            access_token: context.group.access_token_bytes()?,
            blob_id: manifest.blob_id.clone(),
        },
    )
    .await?;
    send.finish()?;
    let response = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("wait for remote file download response timeout")??;
    let size = match response {
        Response::BlobReady { size } => size,
        Response::Error {
            code: ErrorCode::NotFound,
            ..
        } => return Err(RemoteBlobUnavailable.into()),
        Response::Error { message, .. } => bail!(message),
        _ => bail!("blob is unavailable"),
    };
    if size != manifest.encrypted_size {
        bail!("encrypted blob size mismatch");
    }

    crypto::decrypt_blob_stream(
        &mut recv,
        output,
        crypto::BlobStreamSpec {
            original_size: manifest.original_size,
            encrypted_size: manifest.encrypted_size,
            expected_hash: &manifest.blob_id,
            stream_commitment: manifest.stream_commitment,
            key: context.key,
            idle_timeout: BLOB_IDLE_TIMEOUT,
        },
        |current| {
            let total = context.completed_bytes.saturating_add(current);
            context.transferred.store(total, Ordering::Release);
            context.handle.transferred(total);
        },
        || context.handle.canceled(),
    )
    .await?;
    Ok(())
}

#[cfg(target_os = "android")]
pub(super) async fn download_blob_to_android_destination(
    manifest: &BlobManifest,
    destination: &crate::commands::android::AndroidRemoteDownloadTarget,
    context: RemoteBlobTransferContext<'_>,
) -> Result<String> {
    use std::os::fd::FromRawFd;

    let sink = crate::commands::android::open_android_remote_download_sink(
        destination,
        &sanitize_name(&manifest.name),
        "application/octet-stream",
        manifest.is_directory_archive,
    )?;
    let standard_file = unsafe { std::fs::File::from_raw_fd(sink.fd) };
    let mut output = tokio::fs::File::from_std(standard_file);
    let result = stream_remote_blob_to_writer(manifest, &mut output, context)
        .await
        .map_err(RemoteBlobTransportFailure::from_error);
    drop(output);

    let success = result.is_ok() && !context.handle.canceled();
    let token = sink.token;
    if !success {
        let cancel_token = token.clone();
        if let Err(error) = tauri::async_runtime::spawn_blocking(move || {
            crate::commands::android::cancel_android_remote_download_sink(&cancel_token)
        })
        .await
        .context("Android remote file cancellation task failed")
        .and_then(|value| value.map_err(anyhow::Error::from))
        {
            log::warn!("signal Android remote file cancellation failed: {error:#}");
        }
    }
    let finish_token = token.clone();
    let finish = tauri::async_runtime::spawn_blocking(move || {
        crate::commands::android::finish_android_remote_download_sink(&finish_token, success)
    });
    tokio::pin!(finish);
    let mut cancellation_sent = !success;
    let finish = loop {
        tokio::select! {
            joined = &mut finish => {
                break joined.context("Android remote file finalization task failed")?;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)), if !cancellation_sent => {
                if context.handle.canceled() {
                    let cancel_token = token.clone();
                    if let Err(error) = tauri::async_runtime::spawn_blocking(move || {
                        crate::commands::android::cancel_android_remote_download_sink(&cancel_token)
                    }).await.context("Android remote file cancellation task failed").and_then(|value| value.map_err(anyhow::Error::from)) {
                        log::warn!("signal Android remote file cancellation failed: {error:#}");
                    }
                    cancellation_sent = true;
                }
            }
        }
    };
    result?;
    let committed_uri = finish?;
    if context.handle.canceled() {
        if !committed_uri.is_empty() {
            let rollback_uri = committed_uri.clone();
            tauri::async_runtime::spawn_blocking(move || {
                crate::commands::android::discard_android_remote_download_document(&rollback_uri)
            })
            .await
            .context("Android canceled download rollback task failed")??;
        }
        bail!("transfer canceled");
    }
    Ok(committed_uri)
}

#[cfg(not(target_os = "android"))]
pub(super) fn non_conflicting_external_path(root: &Path, name: &str) -> PathBuf {
    let candidate = root.join(name);
    if !candidate.exists() {
        return candidate;
    }
    let path = Path::new(name);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("file");
    let extension = path.extension().and_then(|value| value.to_str());
    for index in 1..=999_u16 {
        let file_name = match extension {
            Some(extension) => format!("{stem} ({index}).{extension}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = root.join(file_name);
        if !candidate.exists() {
            return candidate;
        }
    }
    root.join(format!("{stem}-{}", Uuid::new_v4().simple()))
}

pub(super) async fn receive_blob(
    recv: &mut RecvStream,
    destination: &Path,
    expected_hash: &str,
    size: u64,
    verify_ciphertext_hash: bool,
    expect_commit: bool,
    progress: Option<BlobDownloadProgress<'_>>,
) -> Result<()> {
    let _sync_activity = SyncActivityGuard::acquire();
    let parent = destination.parent().context("blob path has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = destination.with_extension(format!("part-{}", uuid::Uuid::new_v4()));
    let mut cleanup = TemporaryBlobCleanup {
        path: temporary.clone(),
        armed: true,
    };
    let result = async {
        let mut file = tokio::fs::File::create(&temporary).await?;
        let mut remaining = size;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        while remaining > 0 {
            if progress.is_some_and(BlobDownloadProgress::canceled) {
                bail!("transfer canceled");
            }
            let requested =
                usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let read = tokio::time::timeout(BLOB_IDLE_TIMEOUT, recv.read(&mut buffer[..requested]))
                .await
                .context("blob download idle timeout")??
                .context("blob ended early")?;
            file.write_all(&buffer[..read]).await?;
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
            if let Some(progress) = progress {
                progress.transferred(size.saturating_sub(remaining));
            }
        }
        if progress.is_some_and(BlobDownloadProgress::canceled) {
            bail!("transfer canceled");
        }
        file.flush().await?;
        drop(file);
        if verify_ciphertext_hash && hasher.finalize().to_hex().as_str() != expected_hash {
            bail!("blob hash mismatch");
        }
        if expect_commit {
            let commit: Request = tokio::time::timeout(BLOB_IDLE_TIMEOUT, read_frame(recv))
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
        tokio::fs::rename(&temporary, destination).await?;
        cleanup.armed = false;
        Ok(())
    }
    .await;
    result
}

pub(super) struct TemporaryBlobCleanup {
    pub(super) path: PathBuf,
    pub(super) armed: bool,
}

#[cfg(not(target_os = "android"))]
pub(super) struct TemporaryDirectoryCleanup {
    path: PathBuf,
    armed: bool,
}

impl Drop for TemporaryBlobCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(not(target_os = "android"))]
impl Drop for TemporaryDirectoryCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
