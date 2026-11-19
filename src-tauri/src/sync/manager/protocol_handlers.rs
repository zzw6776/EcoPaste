use super::*;

#[derive(Clone)]
pub(super) struct PeerService {
    pub(super) manager: Weak<SyncManager>,
}

#[derive(Clone)]
pub(super) struct JoinService {
    pub(super) manager: Weak<SyncManager>,
}

impl std::fmt::Debug for JoinService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JoinService")
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for JoinService {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Some(manager) = self.manager.upgrade() else {
            return Ok(());
        };
        if let Err(error) = handle_join_connection(manager, connection).await {
            log::warn!("join request failed: {error}");
        }
        Ok(())
    }
}

/// Completes a four-step handshake before allowing Iroh's close-on-drop to release the connection.
async fn handle_join_connection(manager: Arc<SyncManager>, connection: Connection) -> Result<()> {
    let (mut send, mut recv) = tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, connection.accept_bi())
        .await
        .context("等待局域网加入请求超时")??;
    let request: JoinRequest = tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, read_frame(&mut recv))
        .await
        .context("读取局域网加入请求超时")??;
    let request_id = request.request_id.clone();
    let response = process_join_request(&manager, &connection, request).await;
    let (response, approved_peer) = match response {
        Ok((response, approved_peer)) => (response, approved_peer),
        Err(error) => (
            JoinResponse::Error {
                message: error.to_string(),
            },
            None,
        ),
    };
    write_frame(&mut send, &response).await?;

    let acknowledgement: JoinAcknowledgement =
        tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, read_frame(&mut recv))
            .await
            .context("等待申请设备确认超时")??;
    if acknowledgement.version != JOIN_PROTOCOL_VERSION || acknowledgement.request_id != request_id
    {
        bail!("加入申请确认无效");
    }

    let commit_result = if !acknowledgement.accepted {
        Err(anyhow::anyhow!(
            "申请设备未能保存配对结果：{}",
            acknowledgement.error.as_deref().unwrap_or("未知错误")
        ))
    } else if let Some(peer) = approved_peer.as_ref() {
        manager.commit_approved_peer(peer).await
    } else {
        Ok(())
    };
    let completion = match &commit_result {
        Ok(()) => JoinCompletion::Committed,
        Err(error) => JoinCompletion::Error {
            message: error.to_string(),
        },
    };
    write_frame(&mut send, &completion).await?;
    send.finish()?;

    let trailing = tokio::time::timeout(JOIN_COMMIT_TIMEOUT, recv.read_to_end(0))
        .await
        .context("等待申请设备完成握手超时")??;
    if !trailing.is_empty() {
        bail!("加入申请包含多余数据");
    }
    let stopped = tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, send.stopped())
        .await
        .context("等待申请设备接收完成结果超时")??;
    if stopped.is_some() {
        bail!("申请设备中止了加入确认");
    }
    commit_result?;
    if approved_peer.is_some() {
        manager.wake_transfer();
        manager.emit_updated();
    }
    Ok(())
}

async fn process_join_request(
    manager: &Arc<SyncManager>,
    connection: &Connection,
    request: JoinRequest,
) -> Result<(JoinResponse, Option<PeerAnnouncement>)> {
    let settings = manager.app.state::<SettingsStore>().snapshot().sync;
    if !settings.enabled || manager.identity.snapshot().group.is_none() {
        bail!("该设备当前不接受加入申请");
    }
    if request.version != JOIN_PROTOCOL_VERSION {
        bail!("加入协议版本不兼容");
    }
    let remote_endpoint_id = connection.remote_id().to_string();
    if request.endpoint_id != remote_endpoint_id {
        bail!("申请设备身份与加密连接不一致");
    }
    if Uuid::parse_str(&request.request_id).is_err()
        || request.nonce.len() != 16
        || request.device_id.trim().is_empty()
        || request.device_id.len() > 128
        || request.device_name.trim().is_empty()
        || request.device_name.len() > 128
        || request.platform.len() > 32
        || request.direct_addresses.len() > 16
        || request.relay_urls.len() > 16
    {
        bail!("加入申请内容无效");
    }
    let invitation = match (&request.invitation_id, &request.invitation_secret) {
        (Some(invitation_id), Some(invitation_secret)) => {
            Some((invitation_id.as_str(), invitation_secret.as_slice()))
        }
        (None, None) => None,
        _ => bail!("加入申请中的配对凭证无效"),
    };
    let is_invitation = invitation.is_some();
    if let Some((invitation_id, invitation_secret)) = invitation {
        manager
            .consume_pairing_invitation(invitation_id, invitation_secret)
            .await?;
    } else {
        if !settings.lan_enabled {
            bail!("该设备当前不接受局域网加入申请");
        }
        ensure_lan_connection(connection, LAN_PATH_SELECTION_TIMEOUT).await?;
    }
    let own_identity = manager.identity.snapshot();
    if request.device_id == own_identity.device_id {
        bail!("不能申请加入当前设备");
    }
    {
        let now = Instant::now();
        let mut limits = manager
            .join_rate_limits
            .write()
            .expect("join rate limits poisoned");
        limits.retain(|_, last| now.duration_since(*last) < Duration::from_secs(120));
        if limits
            .get(&remote_endpoint_id)
            .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(5))
        {
            bail!("申请过于频繁，请稍后重试");
        }
        limits.insert(remote_endpoint_id.clone(), now);
    }

    let pool = manager.pool().await;
    let previously_removed =
        repository::is_peer_removed(&pool, &request.device_id, &remote_endpoint_id).await?;
    let comparison_code = comparison_code(
        &request.request_id,
        &request.nonce,
        &remote_endpoint_id,
        &manager.identity.secret_key()?.public().to_string(),
    );
    let public = IncomingJoinRequest {
        request_id: request.request_id.clone(),
        device_id: request.device_id.clone(),
        device_name: request.device_name.clone(),
        platform: request.platform.clone(),
        endpoint_id: remote_endpoint_id.clone(),
        comparison_code,
        previously_removed,
        expires_at: (Utc::now() + chrono::Duration::seconds(JOIN_TIMEOUT_SECS as i64)).to_rfc3339(),
    };
    let announcement = PeerAnnouncement {
        device_id: request.device_id,
        device_name: request.device_name,
        platform: request.platform,
        endpoint_id: remote_endpoint_id,
        direct_addresses: request.direct_addresses,
        relay_urls: request.relay_urls,
        last_seen_ms: Utc::now().timestamp_millis(),
    };
    if !is_invitation {
        peer_endpoint_addr(&announcement).context("加入申请包含无效的连接地址")?;
    }
    let approved_peer = announcement;
    let (sender, receiver) = oneshot::channel();
    {
        let mut pending = manager.incoming_join_requests.lock().await;
        if pending.values().any(|value| {
            value.public.device_id == public.device_id
                || value.public.endpoint_id == public.endpoint_id
        }) {
            bail!("该设备已有待处理申请");
        }
        pending.insert(
            public.request_id.clone(),
            PendingIncomingJoinRequest {
                public: public.clone(),
                responder: sender,
            },
        );
    }
    if let Err(error) = manager.app.emit(JOIN_REQUESTED_EVENT, &public) {
        log::debug!("emit incoming join request failed: {error}");
    }

    match tokio::time::timeout(Duration::from_secs(JOIN_TIMEOUT_SECS), receiver).await {
        Ok(Ok(response @ JoinResponse::Approved { .. })) => Ok((response, Some(approved_peer))),
        Ok(Ok(response)) => Ok((response, None)),
        Ok(Err(_)) => Ok((JoinResponse::Expired, None)),
        Err(_) => {
            manager
                .incoming_join_requests
                .lock()
                .await
                .remove(&public.request_id);
            Ok((JoinResponse::Expired, None))
        }
    }
}

impl std::fmt::Debug for PeerService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerService")
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for PeerService {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Some(manager) = self.manager.upgrade() else {
            return Ok(());
        };
        if let Err(error) = serve_peer_connection(manager, connection).await {
            log::warn!("LAN peer connection failed: {error}");
        }
        Ok(())
    }
}

/// Starts the same request server on a locally initiated QUIC connection so either peer can pull.
pub(super) fn spawn_peer_connection_service(manager: Arc<SyncManager>, connection: Connection) {
    tauri::async_runtime::spawn(async move {
        if let Err(error) = serve_peer_connection(manager, connection).await {
            log::warn!("outgoing LAN peer connection failed: {error}");
        }
    });
}

async fn serve_peer_connection(manager: Arc<SyncManager>, connection: Connection) -> Result<()> {
    let stable_id = connection.stable_id();
    let claimed = manager
        .runtime_state
        .lan_served_connections
        .write()
        .expect("served LAN connections poisoned")
        .insert(stable_id);
    if !claimed {
        return Ok(());
    }
    let result = serve_claimed_peer_connection(manager.clone(), connection).await;
    manager
        .runtime_state
        .lan_served_connections
        .write()
        .expect("served LAN connections poisoned")
        .remove(&stable_id);
    result
}

async fn serve_claimed_peer_connection(
    manager: Arc<SyncManager>,
    connection: Connection,
) -> Result<()> {
    if let Err(error) = ensure_lan_connection(&connection, LAN_PATH_SELECTION_TIMEOUT).await {
        bail!("rejected non-LAN sync connection: {error}");
    }
    let connection_admission = Arc::new(Semaphore::new(8));
    let connection_blobs = Arc::new(TransferLimits::default());
    let transfers = Arc::new(Transfers::default());
    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(value) => value,
            Err(_) => break,
        };
        let global_admission = match manager
            .runtime_state
            .inbound_stream_admission
            .clone()
            .acquire_owned()
            .await
        {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let connection_admission = match connection_admission.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let manager = manager.clone();
        let connection = connection.clone();
        let connection_blobs = connection_blobs.clone();
        let transfers = transfers.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_peer_stream(
                manager,
                connection,
                send,
                recv,
                (global_admission, connection_admission),
                connection_blobs,
                transfers,
            )
            .await
            {
                log::warn!("LAN sync request failed: {error}");
            }
        });
    }
    Ok(())
}

async fn handle_peer_stream(
    manager: Arc<SyncManager>,
    connection: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    admission_permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
    connection_blobs: Arc<TransferLimits>,
    transfers: Arc<Transfers>,
) -> Result<()> {
    let request: Request = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("LAN request frame timeout")??;
    drop(admission_permits);
    if let Request::BlobPart { transfer_id, index } = request {
        return transfers.route(transfer_id, index, send, recv).await;
    }
    let lane = request.attachment_lane();
    let blob_request = lane.is_some();
    let _blob_permit = if let Some((files, sending)) = lane {
        Some(connection_blobs.acquire(files, sending).await?)
    } else {
        None
    };
    let _cleanup_guard = if blob_request {
        Some(manager.sync_activity_guard().await)
    } else {
        None
    };
    let result = dispatch_peer(
        &manager,
        &connection,
        &mut send,
        &mut recv,
        request,
        &transfers,
    )
    .await;
    if let Err(error) = result {
        let code = error
            .downcast_ref::<RemoteSyncError>()
            .map(|failure| failure.code)
            .unwrap_or(ErrorCode::InvalidRequest);
        write_frame(
            &mut send,
            &Response::Error {
                code,
                message: error.to_string(),
            },
        )
        .await?;
    }
    send.finish()?;
    Ok(())
}

async fn dispatch_peer(
    manager: &Arc<SyncManager>,
    connection: &Connection,
    send: &mut SendStream,
    _recv: &mut RecvStream,
    request: Request,
    transfers: &Arc<Transfers>,
) -> Result<()> {
    let _sync_activity = SyncActivityGuard::acquire();
    let settings = manager.app.state::<SettingsStore>().snapshot();
    if !settings.sync.enabled || !settings.sync.lan_enabled {
        bail!("LAN sync is disabled");
    }
    match request {
        Request::Health => {
            write_frame(
                send,
                &Response::Health {
                    protocol_version: PROTOCOL_VERSION,
                    server_time_ms: Utc::now().timestamp_millis(),
                },
            )
            .await?
        }
        Request::SyncItems {
            group_id,
            access_token,
            device,
            after_seq,
            before_seq,
            items,
            limit,
        } => {
            let group = authenticate_local(manager, &group_id, &access_token)?;
            let remote_endpoint_id = connection.remote_id().to_string();
            if device.endpoint_id != remote_endpoint_id {
                bail!("device endpoint identity does not match the connection");
            }
            if items.len() > usize::from(ecopaste_sync_protocol::MAX_ITEMS_PER_BATCH) {
                bail!("too many sync items");
            }
            let pool = manager.pool().await;
            if repository::is_peer_removed(&pool, &device.device_id, &remote_endpoint_id).await? {
                bail!("device was removed; pair it again before syncing");
            }
            repository::upsert_peer_identity(
                &pool,
                &PeerAnnouncement {
                    device_id: device.device_id.clone(),
                    device_name: device.device_name.clone(),
                    platform: device.platform.clone(),
                    endpoint_id: device.endpoint_id.clone(),
                    direct_addresses: device.direct_addresses.clone(),
                    relay_urls: device.relay_urls.clone(),
                    last_seen_ms: Utc::now().timestamp_millis(),
                },
            )
            .await?;
            let (connected_address, transport) = connection_path(connection);
            manager.store_lan_connection(&device.device_id, connection.clone());
            repository::mark_peer_online(
                &pool,
                &device.device_id,
                connected_address.as_deref(),
                transport,
            )
            .await?;
            manager.peer_connection_ready(&device.device_id);
            manager.set_channel_status(SyncTarget::Lan, SyncChannelState::Online, None, true);

            // Ingest incoming writes, immediately apply inline items (1 RTT -> green WiFi!),
            // and collect inline LanItemAcks.
            let (acks, needs_blob_pull, needs_repush) = manager
                .ingest_lan_writes(&pool, &group, &settings.sync, &device.device_id, &items)
                .await?;

            // Serve our newest-first sliding window back to the peer.
            let (outgoing, latest_seq, had_more) = repository::list_items_window_for_peer(
                &pool,
                after_seq,
                before_seq,
                limit.clamp(1, ecopaste_sync_protocol::MAX_ITEMS_PER_BATCH),
            )
            .await?;
            let mut response = Response::LanSynced {
                acks,
                items: outgoing,
                latest_seq,
                next_before_seq: None,
            };
            ecopaste_sync_protocol::limit_sync_response(&mut response, had_more)?;
            write_frame(send, &response).await?;

            manager.emit_updated();
            if needs_blob_pull {
                manager.wake_attachments();
            }
            if needs_repush {
                manager.wake_lan_transfer();
            }
        }
        Request::AckLanItems {
            group_id,
            access_token,
            device,
            acks,
        } => {
            authenticate_local(manager, &group_id, &access_token)?;
            if acks.len() > usize::from(ecopaste_sync_protocol::MAX_ITEMS_PER_BATCH) {
                bail!("too many LAN item acks");
            }
            let remote_endpoint_id = connection.remote_id().to_string();
            if device.endpoint_id != remote_endpoint_id {
                bail!("device endpoint identity does not match the connection");
            }
            let pool = manager.pool().await;
            if repository::is_peer_removed(&pool, &device.device_id, &remote_endpoint_id).await? {
                bail!("device was removed; pair it again before syncing");
            }
            let target = SyncConnectionTarget::peer(device.device_id);
            repository::record_lan_acks(&pool, target.delivery_target(), &acks).await?;
            manager.emit_updated();
            write_frame(send, &Response::LanAcknowledged).await?;
        }
        Request::SyncRemovedDevices {
            group_id,
            access_token,
            devices,
        } => {
            authenticate_local(manager, &group_id, &access_token)?;
            if devices.len() > 256 {
                bail!("too many removed devices");
            }
            let pool = manager.pool().await;
            let remote_endpoint_id = connection.remote_id().to_string();
            let remote_was_removed =
                repository::is_peer_removed(&pool, "", &remote_endpoint_id).await?;
            if !remote_was_removed {
                repository::merge_removed_devices(&pool, &devices).await?;
            }
            let removed = repository::removed_devices(&pool).await?;
            let identity = manager.identity.snapshot();
            let own_endpoint_id = manager.identity.secret_key()?.public().to_string();
            let removed_self = removed.iter().any(|device| {
                device.is_removed()
                    && (device.device_id == identity.device_id
                        || device.endpoint_id == own_endpoint_id)
            });
            write_frame(send, &Response::RemovedDevices { devices: removed }).await?;
            manager.emit_updated();
            if removed_self {
                manager.leave_after_removal(&pool).await?;
            }
        }
        Request::GetBlob {
            group_id,
            access_token,
            blob_id,
            transfer_id,
            file_content: _,
        } => {
            let group = authenticate_local(manager, &group_id, &access_token)?;
            let pool = manager.pool().await;
            if repository::is_peer_removed(&pool, "", &connection.remote_id().to_string()).await? {
                bail!("device was removed; pair it again before syncing");
            }
            let Some(blob) = repository::outgoing_blob(&pool, &blob_id).await? else {
                write_frame(
                    send,
                    &Response::Error {
                        code: ErrorCode::NotFound,
                        message: "blob is unavailable".to_owned(),
                    },
                )
                .await?;
                return Ok(());
            };
            let streaming_source = if blob.encrypted_path.is_empty() {
                let Some(source) = blob.source_path.as_deref() else {
                    write_frame(
                        send,
                        &Response::Error {
                            code: ErrorCode::NotFound,
                            message: "blob source is unavailable".to_owned(),
                        },
                    )
                    .await?;
                    return Ok(());
                };
                let nonce = blob
                    .stream_nonce
                    .as_deref()
                    .context("streaming blob nonce is unavailable")?;
                Some((source.to_owned(), nonce.to_vec()))
            } else {
                None
            };
            let staged = if streaming_source.is_none() {
                match manager.lease_outgoing_staging(&group, &blob).await {
                    Ok(staged) => Some(staged),
                    Err(error) if error.downcast_ref::<RemoteBlobUnavailable>().is_some() => {
                        write_frame(
                            send,
                            &Response::Error {
                                code: ErrorCode::NotFound,
                                message: "blob is unavailable".to_owned(),
                            },
                        )
                        .await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            let upload = begin_lan_pull_upload(manager, connection, &pool, &group, &blob).await?;
            if let Some(upload) = &upload {
                upload.handle.connecting();
            }
            let mut progress = super::super::upload::UploadProgress::new(
                connection.stable_id(),
                blob_log_ref(&blob.blob_id),
                upload.as_ref().map(|upload| upload.handle.clone()),
            );
            progress.report_transferred = false;
            let result = async {
                let session = transfers.register(transfer_id, blob.size)?;
                write_frame(send, &Response::BlobReady { size: blob.size }).await?;
                let (mut reader, mut writer) = tokio::io::duplex(64 * 1024);
                let produce = async {
                    if let Some((source, nonce)) = streaming_source {
                        encrypt_upload_source(
                            &source,
                            &mut writer,
                            super::super::upload::FileEncryptionSpec {
                                expected_encrypted_size: blob.size,
                                expected_blob_id: &blob.blob_id,
                                key: group.content_key_bytes()?,
                                stream_nonce: &nonce,
                                idle_timeout: BLOB_IDLE_TIMEOUT,
                                language: crate::i18n::current_language(&manager.app),
                            },
                            &mut progress,
                        )
                        .await?;
                    } else if let Some((staged, _staging_lease)) = staged {
                        let path = PathBuf::from(&staged.encrypted_path);
                        let mut file = tokio::fs::File::open(path).await?;
                        super::super::upload::copy(
                            &mut file,
                            &mut writer,
                            BLOB_IDLE_TIMEOUT,
                            crate::i18n::current_language(&manager.app),
                            &mut progress,
                        )
                        .await?;
                    }
                    writer.shutdown().await?;
                    Ok::<(), anyhow::Error>(())
                };
                let transmit = multipart::send(&mut reader, session.peer(), blob.size, |bytes| {
                    if let Some(upload) = &upload {
                        upload.handle.transferred(bytes);
                    }
                });
                cancelable_transfer(
                    multipart::until_stopped(send, async {
                        tokio::try_join!(produce, transmit)?;
                        Ok(())
                    }),
                    || {
                        upload
                            .as_ref()
                            .is_some_and(|upload| upload.handle.canceled())
                    },
                )
                .await?;
                write_frame(send, &Response::BlobStored).await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Some(upload) = upload {
                match &result {
                    Ok(()) => upload.handle.completed(upload.total_bytes),
                    Err(_) if upload.handle.canceled() => {
                        upload.handle.mark_canceled(progress.written_bytes);
                    }
                    Err(error) => upload
                        .handle
                        .failed(progress.written_bytes, error.to_string()),
                }
                if let Err(error) = repository::unlock_transfer_item(&pool, &upload.task_id).await {
                    log::warn!("release LAN pull transfer protection failed: {error:#}");
                }
            }
            result?;
        }
        Request::BlobPart { .. }
        | Request::CreateGroup { .. }
        | Request::CloudHealth
        | Request::CloudSyncItems { .. }
        | Request::WatchCloud { .. }
        | Request::ListItems { .. }
        | Request::ListDevices { .. }
        | Request::PutManagedBlob { .. }
        | Request::PutManagedSourceIcon { .. }
        | Request::PutManagedStreamBlob { .. }
        | Request::CommitStreamBlob { .. } => {
            bail!("LAN peer does not support this cloud operation")
        }
    }
    Ok(())
}

struct LanPullUpload {
    handle: super::super::progress::TransferHandle,
    task_id: String,
    total_bytes: u64,
}

/// Registers a receiver-initiated LAN pull in the same sender-side task lifecycle as a push.
async fn begin_lan_pull_upload(
    manager: &SyncManager,
    connection: &Connection,
    pool: &SqlitePool,
    group: &GroupSecrets,
    blob: &StoredBlob,
) -> Result<Option<LanPullUpload>> {
    let Some((sync_key, item_id)) = repository::item_id_for_blob(pool, &blob.blob_id).await? else {
        return Ok(None);
    };
    let Some(meta) = repository::load_sync_item_meta(pool, &sync_key).await? else {
        return Ok(None);
    };
    let (Some(item_key), Some(nonce), Some(ciphertext)) = (
        meta.item_key.as_deref(),
        meta.nonce.as_deref(),
        meta.ciphertext.as_deref(),
    ) else {
        return Ok(None);
    };
    let snapshot = crypto::decrypt_item_snapshot(
        &group.content_key_bytes()?,
        item_key,
        meta.deleted,
        meta.protected,
        nonce,
        ciphertext,
    )?;
    validate_snapshot(&snapshot)?;
    let file_manifests = snapshot
        .blobs
        .iter()
        .filter(|manifest| manifest.role == BlobRole::File)
        .collect::<Vec<_>>();
    let Some(manifest) = file_manifests
        .iter()
        .find(|manifest| manifest.blob_id == blob.blob_id)
    else {
        return Ok(None);
    };
    let Some(peer_device_id) =
        repository::peer_device_id_by_endpoint(pool, &connection.remote_id().to_string()).await?
    else {
        return Ok(None);
    };
    let target = format!("peer:{peer_device_id}");
    let operation_total_bytes = file_manifests.iter().fold(0_u64, |total, manifest| {
        total.saturating_add(manifest.encrypted_size)
    });
    let task_key = format!("{sync_key}\0{target}\0{}\0lan-pull", blob.blob_id);
    let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
    repository::lock_transfer_item(pool, &task_id, &item_id).await?;
    let handle = manager
        .delivery
        .start_transient(super::super::progress::TransferTask {
            id: task_id.clone(),
            revision: 0,
            started_at_ms: chrono::Utc::now().timestamp_millis(),
            operation_id: sync_key,
            item_id,
            direction: super::super::progress::TransferDirection::Upload,
            route: super::super::progress::TransferRoute::Lan,
            target,
            file_name: (file_manifests.len() == 1).then(|| manifest.name.clone()),
            phase: super::super::progress::TransferPhase::Queued,
            transferred_bytes: 0,
            total_bytes: operation_total_bytes,
            speed_bytes_per_second: 0,
            elapsed_ms: 0,
            eta_seconds: None,
            error: None,
            cancellable: true,
        });
    Ok(Some(LanPullUpload {
        handle,
        task_id,
        total_bytes: manifest.encrypted_size,
    }))
}
