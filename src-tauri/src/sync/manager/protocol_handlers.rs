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
        if connection.close_reason().is_some() {
            log::debug!("LAN peer connection closed before path selection: {error}");
            return Ok(());
        }
        bail!("rejected non-LAN sync connection: {error}");
    }
    let connection_admission = Arc::new(Semaphore::new(8));
    let connection_blobs = Arc::new(TransferLimits::default());
    let transfers = Arc::new(Transfers::default());
    let protocol_error_logged = Arc::new(AtomicBool::new(false));
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
        let protocol_error_logged = protocol_error_logged.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_peer_stream(
                manager,
                connection,
                send,
                recv,
                (global_admission, connection_admission),
                connection_blobs,
                transfers,
                protocol_error_logged,
            )
            .await
            {
                log::warn!("LAN sync request failed: {error}");
            }
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_peer_stream(
    manager: Arc<SyncManager>,
    connection: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    admission_permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
    connection_blobs: Arc<TransferLimits>,
    transfers: Arc<Transfers>,
    protocol_error_logged: Arc<AtomicBool>,
) -> Result<()> {
    let request: Request = match tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("LAN request frame timeout")?
    {
        Ok(request) => request,
        Err(error @ (FrameError::Decode(_) | FrameError::TooLarge { .. })) => {
            // Incompatible peer versions otherwise spin-retry streams on the same QUIC connection.
            connection.close(1_u32.into(), b"incompatible sync protocol frame");
            if !protocol_error_logged.swap(true, Ordering::AcqRel) {
                log::warn!(
                    "closed incompatible LAN peer connection {}: {error}",
                    connection.remote_id()
                );
            }
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
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
            let verified_routes = verified_lan_peer_addresses(connection, &device.direct_addresses);
            let _ =
                repository::update_peer_routes(&pool, &remote_endpoint_id, &verified_routes, true)
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

            let (served_count, next_before_seq) = lan_sync_serve_progress(&response);
            let remaining_count = match next_before_seq {
                Some(next_before) => {
                    repository::count_items_remaining_for_peer(&pool, after_seq, next_before)
                        .await?
                }
                None => 0,
            };
            manager.record_peer_catchup_serve(
                &device.device_id,
                &device.device_name,
                served_count,
                remaining_count,
                next_before_seq.is_some(),
            );
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
            item_sync_key,
        } => {
            let group = authenticate_local(manager, &group_id, &access_token)?;
            let pool = manager.pool().await;
            if repository::is_peer_removed(&pool, "", &connection.remote_id().to_string()).await? {
                bail!("device was removed; pair it again before syncing");
            }
            let serve_info =
                repository::blob_serve_info(&pool, &blob_id, item_sync_key.as_deref()).await?;
            let mut resolved_source = None;
            for _ in 0..32 {
                let Some((row_sync_key, candidate)) =
                    repository::outgoing_blob(&pool, &blob_id, item_sync_key.as_deref()).await?
                else {
                    break;
                };
                match candidate.outgoing_source() {
                    Some(OutgoingBlobSource::DirectStream {
                        source_path,
                        stream_nonce,
                    }) => {
                        if !source_path.starts_with("content://")
                            && !Path::new(source_path).is_file()
                        {
                            if let Some(ref key) = row_sync_key {
                                let _ = repository::mark_blob_local_unavailable(
                                    &pool,
                                    key,
                                    &candidate.blob_id,
                                )
                                .await;
                                continue;
                            }
                            break;
                        }
                        resolved_source = Some((
                            row_sync_key,
                            candidate.clone(),
                            Some((source_path.to_owned(), stream_nonce.to_vec())),
                            None,
                        ));
                        break;
                    }
                    Some(OutgoingBlobSource::StagedLease) => {
                        match manager.lease_outgoing_staging(&group, &candidate).await {
                            Ok(staged) => {
                                resolved_source =
                                    Some((row_sync_key, candidate, None, Some(staged)));
                                break;
                            }
                            Err(error)
                                if error.downcast_ref::<RemoteBlobUnavailable>().is_some() =>
                            {
                                if let Some(ref key) = row_sync_key {
                                    let _ = repository::mark_blob_local_unavailable(
                                        &pool,
                                        key,
                                        &candidate.blob_id,
                                    )
                                    .await;
                                    continue;
                                }
                                break;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    None => {
                        if let Some(ref key) = row_sync_key {
                            let _ = repository::mark_blob_local_unavailable(
                                &pool,
                                key,
                                &candidate.blob_id,
                            )
                            .await;
                            continue;
                        }
                        break;
                    }
                }
            }
            let Some((actual_sync_key, blob, streaming_source, staged)) = resolved_source else {
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
            let peer_info =
                repository::peer_by_endpoint_id(&pool, &connection.remote_id().to_string()).await?;
            if let Some((ref peer_device_id, ref peer_name)) = peer_info {
                manager.touch_peer_catchup(peer_device_id, peer_name, false);
            }
            let now_ms = Utc::now().timestamp_millis();
            let serve_handle = match (&peer_info, &serve_info) {
                (Some((peer_device_id, _)), Some(info))
                    if super::tracks_individual_transfer(
                        &info.kind,
                        !info.history_backfill,
                        info.manual_share,
                        info.payload_updated_at_ms,
                        now_ms,
                    ) =>
                {
                    let target = format!("peer:{peer_device_id}");
                    let task_key = format!("{}\0{target}\0serve\0{}", info.sync_key, blob.blob_id);
                    let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
                    let handle =
                        manager
                            .delivery
                            .start_serve(super::super::progress::TransferTask {
                                id: task_id,
                                revision: 0,
                                started_at_ms: now_ms,
                                operation_id: info.sync_key.clone(),
                                item_id: info.item_id.clone(),
                                direction: super::super::progress::TransferDirection::Serve,
                                route: super::super::progress::TransferRoute::Lan,
                                target,
                                file_name: info.single_file_name.clone(),
                                phase: super::super::progress::TransferPhase::Connecting,
                                transferred_bytes: 0,
                                total_bytes: blob.size,
                                speed_bytes_per_second: 0,
                                elapsed_ms: 0,
                                eta_seconds: None,
                                error: None,
                                cancellable: true,
                            });
                    handle.transferred(0);
                    Some(handle)
                }
                _ => None,
            };
            manager.emit_updated();
            let lock_sync_key = actual_sync_key.as_deref().or(item_sync_key.as_deref());
            let transfer_lock_id = lock_lan_pull_item(&pool, &blob, lock_sync_key).await?;
            let language = crate::i18n::current_language(&manager.app);
            let mut progress = super::super::upload::UploadProgress::new(
                connection.stable_id(),
                blob_log_ref(&blob.blob_id),
                serve_handle.clone(),
            );
            progress.report_transferred = false;
            let result = async {
                let session = transfers.register(transfer_id, blob.size)?;
                write_frame(send, &Response::BlobReady { size: blob.size }).await?;
                let (mut reader, mut writer) = tokio::io::duplex(2 * 1024 * 1024);
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
                                language,
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
                            language,
                            &mut progress,
                        )
                        .await?;
                    }
                    writer.shutdown().await?;
                    Ok::<(), anyhow::Error>(())
                };
                let transmit = multipart::send(
                    &mut reader,
                    session.peer_with_connection(connection.clone()),
                    blob.size,
                    |bytes| {
                        if let Some(handle) = &serve_handle {
                            handle.transferred(bytes);
                        }
                    },
                );
                super::cancelable_transfer(
                    multipart::until_stopped(send, async {
                        tokio::try_join!(produce, transmit)?;
                        Ok(())
                    }),
                    || {
                        serve_handle
                            .as_ref()
                            .is_some_and(|handle| handle.canceled())
                    },
                )
                .await?;
                write_frame(send, &Response::BlobStored).await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Some(handle) = &serve_handle {
                let reported = handle.reported_bytes();
                match &result {
                    Ok(()) => handle.completed(blob.size),
                    Err(_) if handle.canceled() => handle.mark_canceled(reported),
                    Err(error) => handle.failed(reported, error.to_string()),
                }
            }
            if let Some((ref peer_device_id, ref peer_name)) = peer_info {
                manager.touch_peer_catchup(
                    peer_device_id,
                    peer_name,
                    result.is_ok() && serve_info.is_some(),
                );
            }
            manager.emit_updated();
            if let Some(lock_id) = transfer_lock_id {
                if let Err(error) = repository::unlock_transfer_item(&pool, &lock_id).await {
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
        | Request::CommitStreamBlob { .. }
        | Request::CheckCloudBlobs { .. } => {
            bail!("LAN peer does not support this cloud operation")
        }
    }
    Ok(())
}

/// Protects the local clipboard item from history cleanup while a LAN peer streams its blob.
async fn lock_lan_pull_item(
    pool: &SqlitePool,
    blob: &StoredBlob,
    item_sync_key: Option<&str>,
) -> Result<Option<String>> {
    let Some((sync_key, item_id)) =
        repository::item_id_for_blob(pool, &blob.blob_id, item_sync_key).await?
    else {
        return Ok(None);
    };
    let lock_id = format!(
        "lan-pull:{}:{}:{}",
        sync_key,
        blob.blob_id,
        Uuid::new_v4().simple()
    );
    repository::lock_transfer_item(pool, &lock_id, &item_id).await?;
    Ok(Some(lock_id))
}

fn lan_sync_serve_progress(response: &Response) -> (u64, Option<u64>) {
    match response {
        Response::LanSynced {
            items,
            next_before_seq,
            ..
        } => (u64::try_from(items.len()).unwrap_or(0), *next_before_seq),
        _ => (0, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_record(seq: u64) -> ecopaste_sync_protocol::SyncItemRecord {
        ecopaste_sync_protocol::SyncItemRecord {
            seq,
            hub_rev: 0,
            item_key: format!("key-{seq}"),
            origin_device_id: "dev-a".to_owned(),
            updated_at_ms: 1_700_000_000,
            deleted: false,
            protected: false,
            nonce: vec![1; 12],
            ciphertext: vec![2; 16],
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
            available_blob_ids: Vec::new(),
        }
    }

    #[test]
    fn lan_sync_serve_progress_reads_actual_sent_items_and_continuation() {
        let truncated_page = Response::LanSynced {
            acks: Vec::new(),
            items: vec![dummy_record(20), dummy_record(19)],
            latest_seq: 20,
            next_before_seq: Some(19),
        };
        assert_eq!(lan_sync_serve_progress(&truncated_page), (2, Some(19)));

        let final_catchup_page = Response::LanSynced {
            acks: Vec::new(),
            items: vec![dummy_record(3), dummy_record(2), dummy_record(1)],
            latest_seq: 20,
            next_before_seq: None,
        };
        assert_eq!(lan_sync_serve_progress(&final_catchup_page), (3, None));
    }
}
