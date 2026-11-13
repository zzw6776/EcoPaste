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
    let connection_blobs = Arc::new(Semaphore::new(2));
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
        tokio::spawn(async move {
            if let Err(error) = handle_peer_stream(
                manager,
                connection,
                send,
                recv,
                (global_admission, connection_admission),
                connection_blobs,
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
    connection_blobs: Arc<Semaphore>,
) -> Result<()> {
    let request: Request = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("LAN request frame timeout")??;
    let blob_request = matches!(
        &request,
        Request::PutBlob { .. }
            | Request::PutSourceIcon { .. }
            | Request::PutStreamBlob { .. }
            | Request::GetBlob { .. }
    );
    let _blob_permits = if blob_request {
        Some((
            manager
                .runtime_state
                .inbound_blob_concurrency
                .clone()
                .acquire_owned()
                .await?,
            connection_blobs.acquire_owned().await?,
        ))
    } else {
        None
    };
    let _cleanup_guard = if blob_request {
        Some(manager.runtime_state.lan_cycle_lock.read().await)
    } else {
        None
    };
    drop(admission_permits);
    let result = dispatch_peer(&manager, &connection, &mut send, &mut recv, request).await;
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
    recv: &mut RecvStream,
    request: Request,
) -> Result<()> {
    let _sync_activity = SyncActivityGuard::acquire();
    let settings = manager.app.state::<SettingsStore>().snapshot();
    if !settings.sync.enabled || !settings.sync.lan_enabled {
        bail!("LAN sync is disabled");
    }
    let streaming_request = matches!(&request, Request::PutStreamBlob { .. });
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
        Request::PlanLanContent {
            group_id,
            access_token,
            device_id,
            events,
        } => {
            let group = authenticate_local(manager, &group_id, &access_token)?;
            if events.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                bail!("too many sync events");
            }
            let pool = manager.pool().await;
            let remote_endpoint_id = connection.remote_id().to_string();
            if repository::is_peer_removed(&pool, &device_id, &remote_endpoint_id).await? {
                bail!("device was removed; pair it again before syncing");
            }
            let key = group.content_key_bytes()?;
            let resources = crate::core::paths::resources_dir(&manager.app)?;
            let mut metadata_only_event_ids = Vec::new();
            let mut available_event_ids = Vec::new();
            for event in events {
                let decrypted = crypto::decrypt_sync_event(&key, &event)?;
                let DecryptedSyncEvent::Clipboard(envelope) = decrypted else {
                    continue;
                };
                validate_envelope(&envelope)?;
                if envelope.item.kind != "files" {
                    continue;
                }
                let source_revision = envelope
                    .item
                    .source_revision
                    .as_deref()
                    .unwrap_or(&event.event_id);
                let existing = available_materialized_file_item(
                    &pool,
                    &resources,
                    &envelope.item.sync_key,
                    source_revision,
                    envelope.content_identity.as_deref(),
                )
                .await;
                let existing = match existing {
                    Ok(item) => item,
                    Err(error) => {
                        log::warn!(
                            "defer LAN card content plan: eventId={} error={error:#}",
                            event.event_id
                        );
                        metadata_only_event_ids.push(event.event_id);
                        continue;
                    }
                };
                if existing.is_some() {
                    available_event_ids.push(event.event_id);
                } else if should_defer_remote_file_download(
                    &envelope,
                    settings.sync.auto_download_max_mb,
                    settings.clipboard.history.max_storage_bytes,
                ) {
                    metadata_only_event_ids.push(event.event_id);
                }
            }
            write_frame(
                send,
                &Response::LanContentPlan {
                    metadata_only_event_ids,
                    available_event_ids,
                },
            )
            .await?;
        }
        Request::Sync {
            group_id,
            access_token,
            device,
            after_cursor,
            events,
            limit,
            events_complete,
        } => {
            let group = authenticate_local(manager, &group_id, &access_token)?;
            let remote_endpoint_id = connection.remote_id().to_string();
            if device.endpoint_id != remote_endpoint_id {
                bail!("device endpoint identity does not match the connection");
            }
            if events.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                bail!("too many sync events");
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
            let source = SyncConnectionTarget::peer(device.device_id.clone());
            manager.peer_connection_ready(&device.device_id);
            manager.set_channel_status(SyncTarget::Lan, SyncChannelState::Online, None, true);
            manager.emit_updated();
            let key = group.content_key_bytes()?;
            let metadata_only_event_ids = metadata_only_content_event_ids(&events, &key);
            let stored = repository::store_remote_batch_with_content_policy(
                &pool,
                repository::RemoteBatchSource::Peer {
                    device_id: &device.device_id,
                },
                None,
                &events,
                &manager.identity.snapshot().device_id,
                &[],
                &metadata_only_event_ids,
            )
            .await?;
            for (event_id, origin_device_id) in stored.conflicts {
                log::warn!(
                    "ignore LAN sync event {event_id} with a reused origin sequence from {origin_device_id}"
                );
            }
            let accepted = stored.inserted_event_ids;
            let received_events = !events.is_empty();
            let mut response_cursor = after_cursor;
            let response = loop {
                let (mut outgoing, scanned_cursor) = repository::peer_events_after_cursor(
                    &pool,
                    &device.device_id,
                    response_cursor,
                    limit.clamp(1, MAX_EVENTS_PER_BATCH),
                )
                .await?;
                let first = outgoing
                    .first()
                    .map(|stored| (stored.cursor, stored.event.event_id.clone()));
                let routed_away =
                    repository::event_ids_routed_away_from(&pool, &outgoing, "lan").await?;
                repository::attach_event_blob_ids(&pool, &mut outgoing).await?;
                mark_metadata_only_blob_refs(&mut outgoing, &key, &routed_away);
                let cursor_events = outgoing
                    .into_iter()
                    .map(|value| CursorEvent {
                        cursor: value.cursor,
                        event: value.event,
                    })
                    .collect::<Vec<_>>();
                let only_filtered_events =
                    cursor_events.is_empty() && scanned_cursor > response_cursor;
                let mut response = Response::Synced {
                    accepted_event_ids: accepted.clone(),
                    events: cursor_events,
                    peers: Vec::new(),
                    latest_cursor: scanned_cursor,
                    has_more: false,
                };
                if let Err(error) = limit_sync_response(&mut response, response_cursor, limit) {
                    let Some((cursor, event_id)) = first else {
                        return Err(error.into());
                    };
                    log::warn!("skip oversized LAN card: eventId={event_id} error={error}");
                    manager
                        .delivery
                        .blocked(
                            &pool,
                            source.delivery_target(),
                            std::slice::from_ref(&event_id),
                            &error.to_string(),
                        )
                        .await?;
                    manager.emit_updated();
                    response_cursor = cursor;
                    continue;
                }
                if only_filtered_events {
                    let Response::Synced { latest_cursor, .. } = &mut response else {
                        unreachable!();
                    };
                    *latest_cursor = scanned_cursor;
                }
                break response;
            };
            write_frame(send, &response).await?;
            let content_has_more = manager
                .apply_pending_events(
                    connection,
                    &pool,
                    &group,
                    &settings.sync,
                    &source,
                    events_complete,
                )
                .await?;
            if received_events || content_has_more {
                manager.wake_lan_transfer();
            }
        }
        Request::AcknowledgeLan {
            group_id,
            access_token,
            device,
            event_ids,
            complete_event_ids,
        } => {
            authenticate_local(manager, &group_id, &access_token)?;
            if event_ids.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                bail!("too many LAN receipt events");
            }
            let complete_event_ids = complete_event_ids.into_iter().collect::<HashSet<_>>();
            if complete_event_ids.len() > event_ids.len()
                || !complete_event_ids
                    .iter()
                    .all(|event_id| event_ids.contains(event_id))
            {
                bail!("LAN content receipt is invalid for the acknowledged batch");
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
            repository::confirm_lan_receipts(
                &pool,
                target.delivery_target(),
                &event_ids,
                &complete_event_ids,
            )
            .await?;
            manager
                .release_staging_for_events(&pool, &event_ids)
                .await?;
            manager.emit_updated();
            write_frame(send, &Response::LanAcknowledged).await?;
        }
        Request::CloudSync { .. } => {
            bail!("combined sync is only supported by the cloud Hub");
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
                // Membership history alone is not evidence that a LAN route is available.
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
        | Request::PutStreamBlob {
            group_id,
            access_token,
            blob_id,
            size,
        } => {
            authenticate_local(manager, &group_id, &access_token)?;
            let pool = manager.pool().await;
            if repository::is_peer_removed(&pool, "", &connection.remote_id().to_string()).await? {
                bail!("device was removed; pair it again before syncing");
            }
            if size == 0 {
                bail!("invalid blob");
            }
            let root = crate::core::paths::resources_dir(&manager.app)?.join(SYNC_STAGING_DIR);
            let path = crypto::blob_path(&root, &blob_id)?;
            if path.is_file() && path.metadata()?.len() == size {
                write_frame(send, &Response::BlobStored).await?;
            } else {
                write_frame(send, &Response::BlobReady { size: 0 }).await?;
                receive_blob(
                    recv,
                    &path,
                    &blob_id,
                    size,
                    !streaming_request,
                    streaming_request,
                    None,
                )
                .await?;
                write_frame(send, &Response::BlobStored).await?;
            }
        }
        Request::GetBlob {
            group_id,
            access_token,
            blob_id,
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
            let result = async {
                if let Some((source, nonce)) = streaming_source {
                    write_frame(send, &Response::BlobReady { size: blob.size }).await?;
                    encrypt_upload_source(
                        &source,
                        send,
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
                    let size = path.metadata()?.len();
                    write_frame(send, &Response::BlobReady { size }).await?;
                    let mut file = tokio::fs::File::open(path).await?;
                    super::super::upload::copy(
                        &mut file,
                        send,
                        BLOB_IDLE_TIMEOUT,
                        crate::i18n::current_language(&manager.app),
                        &mut progress,
                    )
                    .await?;
                }
                Ok::<(), anyhow::Error>(())
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
        Request::CreateGroup { .. }
        | Request::CloudHealth
        | Request::WatchCloud { .. }
        | Request::ListEvents { .. }
        | Request::ListDevices { .. }
        | Request::BootstrapPlan { .. }
        | Request::RetentionPlan { .. }
        | Request::SetRetention { .. }
        | Request::PutManagedBlob { .. }
        | Request::PutManagedSourceIcon { .. }
        | Request::PutManagedStreamBlob { .. }
        | Request::SyncCloudAvailability { .. }
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
    let Some((item_id, stored)) =
        repository::outgoing_file_event_for_blob(pool, &blob.blob_id).await?
    else {
        return Ok(None);
    };
    let decrypted = crypto::decrypt_sync_event(&group.content_key_bytes()?, &stored.event)?;
    let DecryptedSyncEvent::Clipboard(envelope) = decrypted else {
        return Ok(None);
    };
    validate_envelope(&envelope)?;
    let file_manifests = envelope
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
    let task_key = format!(
        "{}\0{}\0{}\0lan-pull",
        stored.event.event_id, target, blob.blob_id
    );
    let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
    repository::lock_transfer_item(pool, &task_id, &item_id).await?;
    let handle = manager
        .delivery
        .start_transient(super::super::progress::TransferTask {
            id: task_id.clone(),
            revision: 0,
            started_at_ms: chrono::Utc::now().timestamp_millis(),
            operation_id: stored.event.event_id,
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
