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
pub(super) enum BlobUploadOutcome {
    AlreadyAvailable,
    Uploaded,
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

/// Encrypts either a native path or an Android SAF document directly into the active stream.
pub(super) async fn encrypt_upload_source<W>(
    source: &str,
    writer: &mut W,
    spec: super::super::upload::FileEncryptionSpec<'_>,
    progress: &mut super::super::upload::UploadProgress,
) -> Result<String>
where
    W: AsyncWrite + Unpin,
{
    #[cfg(target_os = "android")]
    if source.starts_with("content://") {
        let original_size = super::super::crypto::original_blob_size(spec.expected_encrypted_size)?;
        let uri = source.to_owned();
        let file = tauri::async_runtime::spawn_blocking(move || {
            crate::commands::android::open_android_remote_download_source(&uri)
        })
        .await
        .context("Android upload source task failed")??;
        return super::super::upload::encrypt_std_file(file, original_size, writer, spec, progress)
            .await;
    }

    super::super::upload::encrypt_file(Path::new(source), writer, spec, progress).await
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

pub(super) const BLOB_PRIORITY_REALTIME: i32 = -1;
pub(super) const BLOB_PRIORITY_BACKFILL: i32 = -10;
pub(super) const BLOB_PRIORITY_SOURCE_ICON: i32 = -20;

#[derive(Clone)]
pub(super) struct BlobUploadOptions {
    pub(super) kind: BlobUploadKind,
    pub(super) file_content: bool,
    pub(super) is_cloud: bool,
    pub(super) language: Language,
    pub(super) stream_priority: i32,
    pub(super) started: Option<mpsc::UnboundedSender<String>>,
    pub(super) transfer: Option<super::super::progress::TransferHandle>,
}

/// Uploads one attachment without closing the connection used by other queues on a task timeout.
pub(super) async fn upload_blob(
    connection: &Connection,
    group: &GroupSecrets,
    blob: &StoredBlob,
    options: BlobUploadOptions,
) -> Result<()> {
    let _slot = match options.kind {
        BlobUploadKind::Content => {
            let limits = attachment_limits(connection);
            limits.acquire(options.file_content, true).await?
        }
        BlobUploadKind::SourceIcon => source_icon_limits(connection).acquire_owned().await?,
    };
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
        connection.stable_id(),
        blob_log_ref(&blob.blob_id),
        options.kind.label(),
        blob.size,
        options.is_cloud,
        address.as_deref().unwrap_or("unknown"),
        transport.unwrap_or("unknown"),
        Utc::now().timestamp_millis(),
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
            connection.stable_id(),
            blob_log_ref(&blob.blob_id),
            progress.phase,
            progress.written_bytes,
            progress.first_write_ms,
            started_at.elapsed().as_millis(),
            probe,
            connection.close_reason(),
            connection.stats(),
        );
    }
    if let Some(transfer) = &options.transfer {
        match &result {
            Ok(BlobUploadOutcome::AlreadyAvailable) => transfer.already_available(blob.size),
            Ok(BlobUploadOutcome::Uploaded) => transfer.completed(blob.size),
            Err(_) if transfer.canceled() => transfer.mark_canceled(progress.written_bytes),
            Err(error) => transfer.failed(progress.written_bytes, error.to_string()),
        }
    }
    result.map(|_| ())
}

/// Diagnoses a stalled upload; the watch supervisor owns connection recovery.
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
        "cloud attachment stalled and health probe failed; retaining shared connection: connectionId={}",
        connection.stable_id()
    );
    "failed-retained"
}

pub(super) async fn upload_blob_inner(
    connection: &Connection,
    group: &GroupSecrets,
    blob: &StoredBlob,
    options: &BlobUploadOptions,
    progress: &mut super::super::upload::UploadProgress,
) -> Result<BlobUploadOutcome> {
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
        let source_mode = blob.outgoing_source().ok_or(RemoteBlobUnavailable)?;
        let streaming = matches!(source_mode, OutgoingBlobSource::DirectStream { .. });
        let transfer_id = rand::random();
        let request = match (options.kind, streaming) {
            (BlobUploadKind::Content, true) => Request::PutManagedStreamBlob {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
                transfer_id,
                file_content: options.file_content,
            },
            (BlobUploadKind::SourceIcon, false) => Request::PutManagedSourceIcon {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
                transfer_id,
                file_content: options.file_content,
            },
            (BlobUploadKind::Content, false) => Request::PutManagedBlob {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                blob_id: blob.blob_id.clone(),
                size: blob.size,
                transfer_id,
                file_content: options.file_content,
            },
            (BlobUploadKind::SourceIcon, true) => {
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
                return Ok(BlobUploadOutcome::AlreadyAvailable);
            }
            Response::BlobReady { .. } => {}
            Response::Error { code, message } => {
                return Err(RemoteSyncError { code, message }.into());
            }
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
        let peer = PartPeer::client(connection.clone(), transfer_id)
            .with_priority(options.stream_priority);
        let (mut encrypted_reader, mut encrypted_writer) = tokio::io::duplex(2 * 1024 * 1024);
        progress.report_transferred = false;
        let produce = async {
            match source_mode {
                OutgoingBlobSource::DirectStream {
                    source_path,
                    stream_nonce,
                } => {
                    if !source_path.starts_with("content://") && !Path::new(source_path).is_file() {
                        return Err(RemoteBlobUnavailable.into());
                    }
                    encrypt_upload_source(
                        source_path,
                        &mut encrypted_writer,
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
                }
                OutgoingBlobSource::StagedLease => {
                    let mut file = tokio::fs::File::open(&blob.encrypted_path).await?;
                    upload::copy(
                        &mut file,
                        &mut encrypted_writer,
                        idle_timeout,
                        options.language,
                        progress,
                    )
                    .await?;
                }
            }
            encrypted_writer.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        };
        let transmit = multipart::send(&mut encrypted_reader, peer, blob.size, |bytes| {
            if let Some(handle) = &options.transfer {
                handle.transferred(bytes);
            }
        });
        cancelable_transfer(
            async {
                tokio::try_join!(produce, transmit)?;
                Ok(())
            },
            || {
                options
                    .transfer
                    .as_ref()
                    .is_some_and(|handle| handle.canceled())
            },
        )
        .await?;
        if streaming {
            write_frame(
                &mut send,
                &Request::CommitStreamBlob {
                    blob_id: blob.blob_id.clone(),
                },
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
                Ok(BlobUploadOutcome::Uploaded)
            }
            Response::Error { code, message } => Err(RemoteSyncError { code, message }.into()),
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

fn is_remote_blob_unavailable_error(code: ErrorCode, message: &str) -> bool {
    code == ErrorCode::NotFound
        || (message.contains("blob") && message.contains("unavailable"))
        || message.contains("sync source content changed before upload")
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
    let limits = attachment_limits(connection);
    let _slot = limits
        .acquire(manifest.role == BlobRole::File, false)
        .await?;
    let _sync_activity = SyncActivityGuard::acquire();
    if progress.is_some_and(BlobDownloadProgress::canceled) {
        bail!("transfer canceled");
    }
    let (mut send, mut recv) = tokio::time::timeout(FIRST_FRAME_TIMEOUT, connection.open_bi())
        .await
        .context("open blob download stream timeout")??;
    let transfer_id = rand::random();
    write_frame(
        &mut send,
        &Request::GetBlob {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            blob_id: manifest.blob_id.clone(),
            transfer_id,
            file_content: manifest.role == BlobRole::File,
        },
    )
    .await?;
    send.finish()?;
    let response = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("wait for blob download response timeout")??;
    let size = match response {
        Response::BlobReady { size } => size,
        Response::Error { code, message } if is_remote_blob_unavailable_error(code, &message) => {
            return Err(RemoteBlobUnavailable.into());
        }
        Response::Error { code, message } => return Err(RemoteSyncError { code, message }.into()),
        _ => bail!("blob is unavailable"),
    };
    if size != manifest.encrypted_size {
        bail!("encrypted blob size mismatch");
    }
    let (mut reader, mut writer) = tokio::io::duplex(64 * 1024);
    let receive = multipart::receive(
        &mut writer,
        PartPeer::client(connection.clone(), transfer_id),
        size,
        |_| {},
    );
    let save = receive_blob(
        &mut reader,
        None,
        destination,
        &manifest.blob_id,
        size,
        !manifest.stream_commitment,
        progress,
    );
    cancelable_transfer(
        async {
            tokio::try_join!(receive, save)?;
            confirm_download(&mut recv).await
        },
        || progress.is_some_and(BlobDownloadProgress::canceled),
    )
    .await
}

/// Streams one authenticated remote blob directly into the user-selected destination.
#[cfg(not(target_os = "android"))]
pub(super) async fn download_blob_to_external_path(
    manifest: &BlobManifest,
    destination: &Path,
    context: RemoteBlobTransferContext<'_>,
) -> Result<(ExternalDownloadCommit, crypto::DecryptedBlobStream)> {
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
    let decrypted = stream_remote_blob_to_writer(manifest, &mut output, context)
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
        Ok((commit, decrypted))
    } else {
        let commit = commit_external_download(&archive_or_file, destination).await?;
        temporary_cleanup.armed = false;
        Ok((commit, decrypted))
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
) -> Result<crypto::DecryptedBlobStream>
where
    W: AsyncWrite + Unpin,
{
    let limits = attachment_limits(context.connection);
    let _slot = limits
        .acquire(manifest.role == BlobRole::File, false)
        .await?;
    validate_blob_manifest(manifest)?;
    let (mut send, mut recv) =
        tokio::time::timeout(FIRST_FRAME_TIMEOUT, context.connection.open_bi())
            .await
            .context("open remote file download stream timeout")??;
    let transfer_id = rand::random();
    write_frame(
        &mut send,
        &Request::GetBlob {
            group_id: context.group.group_id.clone(),
            access_token: context.group.access_token_bytes()?,
            blob_id: manifest.blob_id.clone(),
            transfer_id,
            file_content: manifest.role == BlobRole::File,
        },
    )
    .await?;
    send.finish()?;
    let response = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("wait for remote file download response timeout")??;
    let size = match response {
        Response::BlobReady { size } => size,
        Response::Error { code, message } if is_remote_blob_unavailable_error(code, &message) => {
            return Err(RemoteBlobUnavailable.into());
        }
        Response::Error { message, .. } => bail!(message),
        _ => bail!("blob is unavailable"),
    };
    if size != manifest.encrypted_size {
        bail!("encrypted blob size mismatch");
    }

    let (mut reader, mut writer) = tokio::io::duplex(2 * 1024 * 1024);
    let receive = multipart::receive(
        &mut writer,
        PartPeer::client(context.connection.clone(), transfer_id),
        size,
        |_| {},
    );
    let decrypt = crypto::decrypt_blob_stream(
        &mut reader,
        output,
        crypto::BlobStreamSpec {
            original_size: manifest.original_size,
            encrypted_size: manifest.encrypted_size,
            expected_hash: &manifest.blob_id,
            stream_commitment: manifest.stream_commitment,
            key: context.key,
            write_timeout: BLOB_IDLE_TIMEOUT,
        },
        |current| {
            let total = context.completed_bytes.saturating_add(current);
            context.transferred.store(total, Ordering::Release);
            context.handle.transferred(total);
        },
        || context.handle.canceled(),
    );
    cancelable_transfer(
        async {
            let ((), decrypted) = tokio::try_join!(receive, decrypt)?;
            confirm_download(&mut recv).await?;
            Ok(decrypted)
        },
        || context.handle.canceled(),
    )
    .await
}

#[cfg(target_os = "android")]
pub(super) async fn download_blob_to_android_destination(
    manifest: &BlobManifest,
    destination: &crate::commands::android::AndroidRemoteDownloadTarget,
    context: RemoteBlobTransferContext<'_>,
) -> Result<(String, crypto::DecryptedBlobStream)> {
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
    let decrypted = result?;
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
    Ok((committed_uri, decrypted))
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

pub(super) async fn receive_blob<R: AsyncRead + Unpin>(
    recv: &mut R,
    commit: Option<&mut RecvStream>,
    destination: &Path,
    expected_hash: &str,
    size: u64,
    verify_ciphertext_hash: bool,
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
            // Multipart owns the network deadline; this pipe waits for an entire ordered part.
            let read = recv.read(&mut buffer[..requested]).await?;
            if read == 0 {
                bail!("blob ended early");
            }
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

/// Keeps the parent stream alive until the sender has validated its source and finished all parts.
async fn confirm_download(recv: &mut RecvStream) -> Result<()> {
    match tokio::time::timeout(BLOB_IDLE_TIMEOUT, read_frame(recv))
        .await
        .context("download confirmation timeout")??
    {
        Response::BlobStored => Ok(()),
        Response::Error { code, message } => Err(RemoteSyncError { code, message }.into()),
        _ => bail!("invalid download confirmation"),
    }
}

/// Keeps optional source-icon uploads on a single low-priority slot isolated from content images.
fn source_icon_limits(connection: &Connection) -> Arc<Semaphore> {
    static LIMITS: std::sync::OnceLock<StdMutex<HashMap<usize, Weak<Semaphore>>>> =
        std::sync::OnceLock::new();
    let mut limits = LIMITS
        .get_or_init(StdMutex::default)
        .lock()
        .expect("source icon limits poisoned");
    limits.retain(|_, value| value.strong_count() > 0);
    let entry = limits.entry(connection.stable_id()).or_default();
    if let Some(existing) = entry.upgrade() {
        return existing;
    }
    let value = Arc::new(Semaphore::new(1));
    *entry = Arc::downgrade(&value);
    value
}

/// Shares attachment limits across automatic and manual transfers on the same connection.
fn attachment_limits(connection: &Connection) -> Arc<TransferLimits> {
    static LIMITS: std::sync::OnceLock<StdMutex<HashMap<usize, Weak<TransferLimits>>>> =
        std::sync::OnceLock::new();
    let mut limits = LIMITS
        .get_or_init(StdMutex::default)
        .lock()
        .expect("transfer limits poisoned");
    limits.retain(|_, value| value.strong_count() > 0);
    let entry = limits.entry(connection.stable_id()).or_default();
    if let Some(existing) = entry.upgrade() {
        return existing;
    }
    let value = Arc::new(TransferLimits::default());
    *entry = Arc::downgrade(&value);
    value
}

/// Cancels all part streams even while network I/O is waiting for a response.
pub(super) async fn cancelable_transfer<F, T>(work: F, canceled: impl Fn() -> bool) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    tokio::pin!(work);
    loop {
        tokio::select! {
            result = &mut work => return result,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if canceled() { bail!("transfer canceled"); }
            }
        }
    }
}
