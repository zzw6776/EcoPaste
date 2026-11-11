use super::*;

pub(super) async fn local_outbox_worker(manager: Arc<SyncManager>) {
    loop {
        manager.runtime_state.local_outbox_wake.notified().await;
        loop {
            match manager
                .materialize_local_operations(false, 0, None, None)
                .await
            {
                Ok(result) if result.deferred_content => break,
                Ok(result) if result.processed == 0 => {
                    let settings = manager.app.state::<SettingsStore>().snapshot().sync;
                    if !settings.enabled || manager.identity.snapshot().group.is_none() {
                        break;
                    }
                    let pool = manager.pool().await;
                    let retry_at =
                        match db::sync_queue::next_content_retry_at(&pool, settings.sync_sensitive)
                            .await
                        {
                            Ok(retry_at) => retry_at,
                            Err(error) => {
                                log::warn!("read local sync outbox retry failed: {error:#}");
                                None
                            }
                        };
                    let Some(retry_at) = retry_at else {
                        break;
                    };
                    let delay = (retry_at - Utc::now()).to_std().unwrap_or(Duration::ZERO);
                    tokio::select! {
                        _ = manager.runtime_state.local_outbox_wake.notified() => {}
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
                Ok(_) => continue,
                Err(error) => {
                    log::warn!("materialize local sync outbox failed: {error:#}");
                    tokio::select! {
                        _ = manager.runtime_state.local_outbox_wake.notified() => {}
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    }
                }
            }
        }
    }
}

pub(super) async fn cloud_transfer_worker(manager: Arc<SyncManager>) {
    let mut cloud_failure_count = 0_usize;
    let mut cloud_next_at: Option<Instant> = None;
    loop {
        if let Some(next_at) = cloud_next_at {
            loop {
                tokio::select! {
                    _ = manager.runtime_state.cloud_transfer_wake.notified() => {
                        if manager.runtime_state.cloud_force_reconnect.swap(false, Ordering::AcqRel) {
                            cloud_failure_count = 0;
                            break;
                        }
                    },
                    _ = tokio::time::sleep_until(next_at.into()) => break,
                }
            }
        } else {
            manager.runtime_state.cloud_transfer_wake.notified().await;
            manager
                .runtime_state
                .cloud_force_reconnect
                .store(false, Ordering::Release);
        }

        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled || !server_is_configured(&settings) {
            manager.set_channel_status(SyncTarget::Cloud, SyncChannelState::Disabled, None, false);
            cloud_failure_count = 0;
            cloud_next_at = None;
            continue;
        }
        if manager.cached_cloud_connection().is_none() {
            cloud_next_at = None;
            manager.set_channel_status(SyncTarget::Cloud, SyncChannelState::Idle, None, false);
            manager.emit_updated();
            continue;
        }
        match manager.run_cloud_cycle().await {
            Ok(()) => {
                cloud_failure_count = 0;
                cloud_next_at = None;
            }
            Err(error) => {
                cloud_failure_count = cloud_failure_count.saturating_add(1);
                let (delay, next_retry_at) = retry_schedule(cloud_failure_count);
                cloud_next_at = Some(Instant::now() + delay);
                manager.set_channel_retry_status(
                    SyncTarget::Cloud,
                    &error.to_string(),
                    &next_retry_at,
                );
                log::warn!(
                    "cloud clipboard sync cycle failed: attempt={} retryInSecs={} error={error:#}",
                    cloud_failure_count,
                    delay.as_secs(),
                );
                manager.emit_updated();
            }
        }
    }
}

pub(super) async fn lan_transfer_worker(manager: Arc<SyncManager>) {
    loop {
        manager.runtime_state.lan_transfer_wake.notified().await;
        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled || !settings.lan_enabled {
            manager.set_channel_status(SyncTarget::Lan, SyncChannelState::Disabled, None, false);
            manager.stop_lan_peer_actors();
            if !settings.enabled || !server_is_configured(&settings) {
                if let Err(error) = manager.stop_runtime().await {
                    log::warn!("stop disabled sync runtime failed: {error}");
                }
            }
            continue;
        }
        if let Err(error) = dispatch_lan_peer_actors(&manager).await {
            log::warn!("dispatch LAN peer sync failed: {error}");
            manager.emit_updated();
        }
    }
}

/// Reconciles peer actors and schedules only newly created workers.
pub(super) async fn dispatch_lan_peer_actors(manager: &Arc<SyncManager>) -> Result<()> {
    let started = reconcile_lan_peer_actors(manager).await?;
    for actor in started {
        actor.queue_work();
    }
    Ok(())
}

/// Keeps the actor registry aligned with paired devices without scheduling duplicate work.
pub(super) async fn reconcile_lan_peer_actors(
    manager: &Arc<SyncManager>,
) -> Result<Vec<Arc<LanPeerActor>>> {
    let pool = manager.pool().await;
    let own_device_id = manager.identity.snapshot().device_id;
    let device_ids = repository::list_peers(&pool)
        .await?
        .into_iter()
        .map(|peer| peer.announcement.device_id)
        .filter(|device_id| device_id != &own_device_id)
        .collect::<HashSet<_>>();
    let mut started = Vec::new();
    let removed = {
        let mut actors = manager
            .runtime_state
            .lan_peer_actors
            .write()
            .expect("LAN peer actors poisoned");
        let removed = actors
            .keys()
            .filter(|device_id| !device_ids.contains(*device_id))
            .cloned()
            .collect::<Vec<_>>();
        for device_id in &removed {
            if let Some(actor) = actors.remove(device_id) {
                actor.stop();
            }
        }
        for device_id in device_ids {
            if !actors.contains_key(&device_id) {
                let (actor, receiver) = LanPeerActor::new();
                let actor = Arc::new(actor);
                actors.insert(device_id.clone(), actor.clone());
                started.push((device_id, actor, receiver));
            }
        }
        removed
    };
    for device_id in removed {
        manager.invalidate_lan_connection_by_device(&device_id);
    }
    let mut started_actors = Vec::with_capacity(started.len());
    for (device_id, actor, receiver) in started {
        tauri::async_runtime::spawn(lan_peer_worker(manager.clone(), device_id, receiver));
        started_actors.push(actor);
    }
    Ok(started_actors)
}

#[derive(Clone, Copy)]
enum LanPeerPhase {
    Idle,
    Offline,
    Backoff { retry_at: Instant },
}

pub(super) async fn lan_peer_worker(
    manager: Arc<SyncManager>,
    device_id: String,
    mut signals: mpsc::UnboundedReceiver<LanPeerSignal>,
) {
    let mut phase = LanPeerPhase::Idle;
    let mut pending_work = false;
    let mut failure_count = 0_usize;
    loop {
        let retry_at = match phase {
            LanPeerPhase::Backoff { retry_at } => Some(retry_at),
            LanPeerPhase::Idle | LanPeerPhase::Offline => None,
        };
        let signal = if let Some(retry_at) = retry_at {
            tokio::select! {
                signal = signals.recv() => signal,
                _ = tokio::time::sleep_until(retry_at.into()) => Some(LanPeerSignal::RetryElapsed),
            }
        } else {
            signals.recv().await
        };
        let Some(signal) = signal else { break };

        let mut reconnect_reply = None;
        let should_run = match signal {
            LanPeerSignal::WorkQueued => {
                pending_work = true;
                matches!(phase, LanPeerPhase::Idle)
            }
            LanPeerSignal::RetryElapsed => {
                phase = LanPeerPhase::Idle;
                pending_work = true;
                true
            }
            LanPeerSignal::RouteAvailable { changed } => {
                let should_run = changed || matches!(phase, LanPeerPhase::Offline);
                if should_run {
                    phase = LanPeerPhase::Idle;
                }
                should_run
            }
            LanPeerSignal::ConnectionReady => {
                phase = LanPeerPhase::Idle;
                failure_count = 0;
                pending_work
            }
            LanPeerSignal::ConnectionClosed => {
                phase = LanPeerPhase::Offline;
                failure_count = 0;
                false
            }
            LanPeerSignal::ManualReconnect(reply) => {
                phase = LanPeerPhase::Idle;
                failure_count = 0;
                pending_work = true;
                reconnect_reply = Some(reply);
                true
            }
            LanPeerSignal::Stop => break,
        };
        if !should_run {
            continue;
        }

        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled || !settings.lan_enabled {
            if let Some(reply) = reconnect_reply {
                let _ = reply.send(Err("局域网同步已关闭".to_owned()));
            }
            continue;
        }

        pending_work = false;
        let outcome = manager
            .run_lan_peer_actor_cycle(&device_id, LAN_EVENT_CONNECT_TIMEOUT)
            .await;
        let response = match outcome {
            Ok(LanPeerSyncOutcome::Succeeded) => {
                phase = LanPeerPhase::Idle;
                failure_count = 0;
                Ok(())
            }
            Ok(LanPeerSyncOutcome::ConnectionFailed(message)) => {
                phase = LanPeerPhase::Offline;
                failure_count = 0;
                pending_work = true;
                log::debug!(
                    "LAN peer {device_id} is offline; wait for a discovery event: {message}"
                );
                manager.emit_updated();
                Err(message)
            }
            Ok(LanPeerSyncOutcome::TransferFailed(message)) => {
                failure_count = failure_count.saturating_add(1);
                let delay = retry_delay(failure_count);
                phase = LanPeerPhase::Backoff {
                    retry_at: Instant::now() + delay,
                };
                pending_work = true;
                log::warn!(
                    "LAN peer transfer failed: deviceId={device_id} attempt={failure_count} retryInSecs={} error={message}",
                    delay.as_secs(),
                );
                manager.emit_updated();
                Err(message)
            }
            Err(error) => {
                let pool = manager.pool().await;
                let message = error.to_string();
                if let Err(state_error) =
                    repository::mark_peer_error(&pool, &device_id, &message).await
                {
                    log::warn!(
                        "record retryable LAN peer failure: deviceId={device_id} error={state_error:#}"
                    );
                }
                failure_count = failure_count.saturating_add(1);
                let delay = retry_delay(failure_count);
                phase = LanPeerPhase::Backoff {
                    retry_at: Instant::now() + delay,
                };
                pending_work = true;
                log::warn!(
                    "LAN peer cycle failed: deviceId={device_id} attempt={failure_count} retryInSecs={} error={error:#}",
                    delay.as_secs(),
                );
                manager.emit_updated();
                Err(message)
            }
        };
        if let Some(reply) = reconnect_reply {
            let _ = reply.send(response);
        }
    }
}

/// Connects only to the explicit LAN routes supplied by current discovery metadata.
pub(super) async fn connect_peer(
    endpoint: &Endpoint,
    discovered_address: EndpointAddr,
    total_timeout: Duration,
) -> Result<Connection> {
    if discovered_address.is_empty() {
        bail!("当前没有可用的局域网地址");
    }
    let discovered_ips = discovered_address
        .ip_addrs()
        .map(SocketAddr::ip)
        .collect::<HashSet<_>>();
    match tokio::time::timeout(total_timeout, async {
        let connection = endpoint.connect(discovered_address, ALPN).await?;
        ensure_discovered_lan_connection(&connection, LAN_PATH_SELECTION_TIMEOUT, &discovered_ips)
            .await?;
        Ok::<_, anyhow::Error>(connection)
    })
    .await
    {
        Ok(Ok(connection)) => Ok(connection),
        Ok(Err(error)) => Err(error),
        Err(_) => bail!("连接超时"),
    }
}

/// Wakes synchronization when our route changes or a paired endpoint is rediscovered by mDNS.
pub(super) fn spawn_endpoint_watchers(
    manager: Weak<SyncManager>,
    endpoint: &Endpoint,
    mdns: Option<MdnsAddressLookup>,
) {
    let mut address_stream = endpoint.watch_addr().stream();
    let address_closed = endpoint.closed();
    let address_manager = manager.clone();
    let address_mdns = mdns.clone();
    tauri::async_runtime::spawn(address_closed.run_until(async move {
        let mut initial = true;
        while let Some(address) = address_stream.next().await {
            let Some(manager) = address_manager.upgrade() else {
                break;
            };
            if let Some(mdns) = address_mdns.as_ref() {
                manager.configure_mdns_multicast(mdns, &address).await;
            }
            if initial {
                initial = false;
                continue;
            }
            manager.notify_lan_connectivity_changed();
            #[cfg(not(target_os = "android"))]
            manager.notify_cloud_connectivity_changed();
        }
    }));

    let Some(mdns) = mdns else {
        return;
    };
    let pending_events = Arc::new(RwLock::new(HashMap::<String, PendingMdnsEvent>::new()));
    let pending_wake = Arc::new(Notify::new());
    let subscriber_events = pending_events.clone();
    let subscriber_wake = pending_wake.clone();
    let subscriber_closed = endpoint.closed();
    tauri::async_runtime::spawn(subscriber_closed.run_until(async move {
        let mut events = mdns.subscribe().await;
        while let Some(event) = events.next().await {
            let event = match event {
                DiscoveryEvent::Discovered { endpoint_info, .. } => {
                    let metadata = endpoint_info
                        .data
                        .user_data()
                        .and_then(|value| DiscoveryMetadata::decode(value).ok());
                    let endpoint_id = endpoint_info.endpoint_id.to_string();
                    PendingMdnsEvent::Discovered {
                        endpoint_id,
                        metadata,
                        address: endpoint_info.into(),
                    }
                }
                DiscoveryEvent::Expired { endpoint_id } => PendingMdnsEvent::Expired {
                    endpoint_id: endpoint_id.to_string(),
                },
                _ => continue,
            };
            let endpoint_id = match &event {
                PendingMdnsEvent::Discovered { endpoint_id, .. }
                | PendingMdnsEvent::Expired { endpoint_id } => endpoint_id.clone(),
            };
            subscriber_events
                .write()
                .expect("pending mDNS events poisoned")
                .insert(endpoint_id, event);
            subscriber_wake.notify_one();
        }
    }));

    let processor_closed = endpoint.closed();
    tauri::async_runtime::spawn(processor_closed.run_until(async move {
        loop {
            pending_wake.notified().await;
            let events = std::mem::take(
                &mut *pending_events
                    .write()
                    .expect("pending mDNS events poisoned"),
            );
            for event in events.into_values() {
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                process_mdns_event(&manager, event).await;
            }
        }
    }));
}

/// Applies coalesced mDNS state without ever blocking the discovery subscription.
pub(super) async fn process_mdns_event(manager: &Arc<SyncManager>, event: PendingMdnsEvent) {
    match event {
        PendingMdnsEvent::Expired { endpoint_id } => {
            // Discovery presence is not connection liveness. Android may filter multicast while
            // a direct QUIC path remains healthy, so only the connection watcher marks it offline.
            manager
                .runtime_state
                .nearby_devices
                .write()
                .expect("nearby devices poisoned")
                .remove(&endpoint_id);
        }
        PendingMdnsEvent::Discovered {
            endpoint_id,
            metadata,
            address,
        } => {
            let (newly_discovered, presence_changed) = if let Some(metadata) = metadata {
                let presence_nonce = metadata.presence_nonce;
                let previous = manager
                    .runtime_state
                    .nearby_devices
                    .write()
                    .expect("nearby devices poisoned")
                    .insert(
                        endpoint_id.clone(),
                        NearbyDeviceEntry {
                            metadata,
                            address: address.clone(),
                            last_seen_at: Utc::now(),
                        },
                    );
                manager.runtime_state.nearby_wake.notify_waiters();
                let presence_changed = previous.as_ref().is_some_and(|entry| {
                    presence_nonce_changed(entry.metadata.presence_nonce, presence_nonce)
                });
                (previous.is_none(), presence_changed)
            } else {
                (false, false)
            };
            let direct_addresses = address
                .ip_addrs()
                .filter(|address| is_lan_ip(address.ip()))
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let pool = manager.pool().await;
            match repository::update_peer_routes(
                &pool,
                &endpoint_id,
                &direct_addresses,
                newly_discovered || presence_changed,
            )
            .await
            {
                Ok(routes_changed) => {
                    match repository::peer_device_id_by_endpoint(&pool, &endpoint_id).await {
                        Ok(Some(device_id)) => manager.peer_route_available(
                            &device_id,
                            routes_changed || newly_discovered || presence_changed,
                        ),
                        Ok(_) => {}
                        Err(error) => log::debug!("resolve discovered LAN peer failed: {error}"),
                    }
                }
                Err(error) => log::debug!("refresh discovered peer route failed: {error}"),
            }
        }
    }
}

pub(super) fn presence_nonce_changed(previous: Option<u64>, current: Option<u64>) -> bool {
    current.is_some() && previous != current
}

/// Returns every private or link-local IPv4 address on which mDNS should operate.
#[cfg(any(not(target_os = "android"), test))]
pub(super) fn lan_multicast_interfaces_v4(address: &EndpointAddr) -> BTreeSet<Ipv4Addr> {
    collect_lan_multicast_interfaces_v4(address.ip_addrs().map(|address| address.ip()))
}

pub(super) fn ipv4_direct_addr_filter() -> AddrFilter {
    AddrFilter::new(|addresses| {
        std::borrow::Cow::Owned(
            addresses
                .iter()
                .filter(|address| matches!(address, TransportAddr::Ip(socket) if socket.is_ipv4()))
                .cloned()
                .collect(),
        )
    })
}

#[cfg(any(not(target_os = "android"), test))]
pub(super) fn collect_lan_multicast_interfaces_v4(
    addresses: impl IntoIterator<Item = IpAddr>,
) -> BTreeSet<Ipv4Addr> {
    addresses
        .into_iter()
        .filter_map(|address| match address {
            IpAddr::V4(ip) if !ip.is_loopback() && is_lan_ip(ip.into()) => Some(ip),
            _ => None,
        })
        .collect()
}

#[cfg(any(target_os = "android", test))]
pub(super) fn parse_android_lan_multicast_interfaces(value: &str) -> BTreeSet<Ipv4Addr> {
    value
        .split(',')
        .filter_map(|address| address.parse::<Ipv4Addr>().ok())
        .collect()
}

/// Publishes and downloads optional source icons without delaying clipboard event delivery.
pub(super) async fn source_asset_worker(manager: Arc<SyncManager>) {
    let mut upload_failure_count = 0_usize;
    loop {
        if upload_failure_count == 0 {
            manager.runtime_state.source_asset_wake.notified().await;
        } else {
            tokio::select! {
                _ = manager.runtime_state.source_asset_wake.notified() => {},
                _ = tokio::time::sleep(retry_delay(upload_failure_count)) => {},
            }
        }
        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        let Some(group) = manager.identity.snapshot().group else {
            upload_failure_count = 0;
            continue;
        };
        if !settings.enabled {
            upload_failure_count = 0;
            continue;
        }

        // Local retention takes the write side before reclaiming temporary ciphertext.
        let _staging_guard = manager.runtime_state.lan_cycle_lock.read().await;
        let connection = manager.cached_cloud_connection();
        if settings.cloud_enabled {
            let publish_started_at = Instant::now();
            match manager
                .publish_pending_source_icons(connection.as_ref(), &group)
                .await
            {
                Ok(0) => upload_failure_count = 0,
                Ok(uploaded_count) => {
                    upload_failure_count = 0;
                    log::info!(
                        "queued source app icon publish completed: icons={uploaded_count} totalMs={}",
                        publish_started_at.elapsed().as_millis()
                    );
                    if uploaded_count == usize::from(MAX_SOURCE_ICON_UPLOADS_PER_BATCH) {
                        manager.runtime_state.source_asset_wake.notify_one();
                    }
                }
                Err(error) => {
                    upload_failure_count = upload_failure_count.saturating_add(1);
                    log::warn!(
                        "publish queued source app icons failed: attempt={} retryInSecs={} error={error}",
                        upload_failure_count,
                        retry_delay(upload_failure_count).as_secs()
                    );
                }
            }
        } else {
            upload_failure_count = 0;
        }

        let started_at = Instant::now();
        match manager
            .refresh_source_app_assets(connection.as_ref(), &group)
            .await
        {
            Ok(0) => {}
            Ok(pending_count) => log::info!(
                "source app asset refresh completed: pendingAssets={pending_count} cloudConnection={} totalMs={}",
                connection.is_some(),
                started_at.elapsed().as_millis()
            ),
            Err(error) => log::warn!("refresh source app icons failed: {error}"),
        }
    }
}

pub(super) async fn cloud_watch_worker(manager: Arc<SyncManager>) {
    let mut failure_count = 0_usize;
    loop {
        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        let Some(group) = manager.identity.snapshot().group else {
            failure_count = 0;
            manager.set_cloud_watch_status(SyncChannelState::Disabled, None, false);
            manager.runtime_state.watch_wake.notified().await;
            continue;
        };
        if !settings.enabled || !settings.cloud_enabled {
            failure_count = 0;
            manager.set_cloud_watch_status(SyncChannelState::Disabled, None, false);
            manager.runtime_state.watch_wake.notified().await;
            continue;
        }
        let server = match server_endpoint_addr(&settings).await {
            Ok(Some(server)) => server,
            Ok(None) => {
                failure_count = 0;
                manager.set_cloud_watch_status(SyncChannelState::Disabled, None, false);
                manager.runtime_state.watch_wake.notified().await;
                continue;
            }
            Err(error) => {
                failure_count = failure_count.saturating_add(1);
                let (delay, next_retry_at) = retry_schedule(failure_count);
                manager.set_cloud_watch_retry_status(&error.to_string(), &next_retry_at);
                log::warn!(
                    "resolve cloud Hub address for watch failed: attempt={} retryInSecs={} error={error:#}",
                    failure_count,
                    delay.as_secs(),
                );
                manager.emit_updated();
                tokio::select! {
                    _ = manager.runtime_state.watch_wake.notified() => failure_count = 0,
                    _ = tokio::time::sleep(delay) => {},
                }
                continue;
            }
        };
        let mut watch_ready = false;
        let result = async {
            let endpoint = manager.ensure_endpoint(&settings).await?;
            manager.set_cloud_watch_status(SyncChannelState::Connecting, None, false);
            manager.emit_updated();
            let connection = manager.connect_cloud(&endpoint, server).await?;
            let connection_result = async {
                manager.ensure_cloud_group(&connection, &group).await?;
                manager.set_cloud_path(&connection);
                manager.notify_cloud_connected();
                let pool = manager.pool().await;
                watch_cloud_group(&manager, &connection, &group, &pool, &mut watch_ready).await
            }
            .await;
            if connection_result.is_err() {
                manager.invalidate_cloud_connection(connection.stable_id());
            }
            connection_result
        }
        .await;
        match result {
            Ok(()) => failure_count = 0,
            Err(error) => {
                failure_count = next_cloud_watch_failure_count(failure_count, watch_ready);
                let (delay, next_retry_at) = retry_schedule(failure_count);
                manager.set_cloud_watch_retry_status(&error.to_string(), &next_retry_at);
                log::warn!(
                    "cloud watch failed: attempt={} retryInSecs={} watchReady={} error={error:#}",
                    failure_count,
                    delay.as_secs(),
                    watch_ready,
                );
                manager.emit_updated();
                tokio::select! {
                    _ = manager.runtime_state.watch_wake.notified() => failure_count = 0,
                    _ = tokio::time::sleep(delay) => {},
                }
            }
        }
    }
}

/// Watches one cloud group through a persistent change stream.
pub(super) async fn watch_cloud_group(
    manager: &SyncManager,
    connection: &Connection,
    group: &GroupSecrets,
    pool: &SqlitePool,
    watch_ready: &mut bool,
) -> Result<()> {
    let cursor = repository::cloud_cursor(pool).await?;
    let availability_cursor = repository::cloud_availability_cursor(pool).await?;
    let removed_at_ms = repository::latest_removed_at_ms(pool).await?;
    let mut cursor = cursor;
    let mut availability_cursor = availability_cursor;
    let mut removed_at_ms = removed_at_ms;
    let request = Request::WatchCloud {
        group_id: group.group_id.clone(),
        access_token: group.access_token_bytes()?,
        after_cursor: cursor,
        after_removed_at_ms: removed_at_ms,
    };
    let (mut recv, initial_response) = tokio::select! {
        _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
        result = tokio::time::timeout(FIRST_FRAME_TIMEOUT, async {
            let (mut send, mut recv) = connection.open_bi().await?;
            write_frame(&mut send, &request).await?;
            send.finish()?;
            let response = read_frame(&mut recv).await?;
            Ok::<_, anyhow::Error>((recv, response))
        }) => result.context("读取云端订阅首帧超时")??,
    };
    apply_cloud_watch_response(
        manager,
        initial_response,
        &mut cursor,
        &mut availability_cursor,
        &mut removed_at_ms,
    )?;
    mark_cloud_watch_ready(manager, watch_ready);
    log::info!("cloud watch ready: connectionId={}", connection.stable_id(),);

    loop {
        let response = tokio::select! {
            _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
            result = tokio::time::timeout(
                CLOUD_WATCH_RESPONSE_TIMEOUT,
                read_frame(&mut recv),
            ) => result.context("等待云端持续订阅心跳超时")??,
        };
        apply_cloud_watch_response(
            manager,
            response,
            &mut cursor,
            &mut availability_cursor,
            &mut removed_at_ms,
        )?;
    }
}

/// Marks a watch healthy only after the Hub has returned a valid subscription frame.
pub(super) fn mark_cloud_watch_ready(manager: &SyncManager, watch_ready: &mut bool) {
    if *watch_ready {
        return;
    }
    *watch_ready = true;
    manager.set_cloud_watch_status(SyncChannelState::Online, None, true);
    manager.emit_updated();
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct CloudWatchChanges {
    pub(super) events_changed: bool,
    pub(super) availability_changed: bool,
    pub(super) removals_changed: bool,
}

/// Advances the local watch watermarks and reports work only when the Hub moved either one.
pub(super) fn advance_cloud_watch_watermarks(
    latest_cursor: u64,
    latest_availability_cursor: u64,
    latest_removed_at_ms: i64,
    cursor: &mut u64,
    availability_cursor: &mut u64,
    removed_at_ms: &mut i64,
) -> Option<CloudWatchChanges> {
    let changes = CloudWatchChanges {
        events_changed: latest_cursor > *cursor,
        availability_changed: latest_availability_cursor > *availability_cursor,
        removals_changed: latest_removed_at_ms > *removed_at_ms,
    };
    *cursor = (*cursor).max(latest_cursor);
    *availability_cursor = (*availability_cursor).max(latest_availability_cursor);
    *removed_at_ms = (*removed_at_ms).max(latest_removed_at_ms);

    (changes.events_changed || changes.availability_changed || changes.removals_changed)
        .then_some(changes)
}

/// Wakes change-driven workers only for a real event or removal watermark advance.
pub(super) fn apply_cloud_watch_watermarks(
    manager: &SyncManager,
    latest_cursor: u64,
    latest_availability_cursor: u64,
    latest_removed_at_ms: i64,
    cursor: &mut u64,
    availability_cursor: &mut u64,
    removed_at_ms: &mut i64,
) {
    let Some(changes) = advance_cloud_watch_watermarks(
        latest_cursor,
        latest_availability_cursor,
        latest_removed_at_ms,
        cursor,
        availability_cursor,
        removed_at_ms,
    ) else {
        return;
    };
    let received_at_ms = Utc::now().timestamp_millis();
    let events_changed = changes.events_changed;
    let availability_changed = changes.availability_changed;
    let removals_changed = changes.removals_changed;
    log::info!(
        "cloud watch change received: eventsChanged={events_changed} availabilityChanged={availability_changed} removalsChanged={removals_changed} latestCursor={latest_cursor} latestAvailabilityCursor={latest_availability_cursor} latestRemovedAtMs={latest_removed_at_ms} receivedAtMs={received_at_ms}"
    );
    manager.runtime_state.source_asset_wake.notify_one();
    manager.wake_cloud_transfer();
}

pub(super) fn apply_cloud_watch_response(
    manager: &SyncManager,
    response: Response,
    cursor: &mut u64,
    availability_cursor: &mut u64,
    removed_at_ms: &mut i64,
) -> Result<()> {
    match response {
        Response::CloudChanged {
            latest_cursor,
            latest_removed_at_ms,
            server_version,
            latest_availability_cursor,
        } => {
            manager.update_cloud_server_version(Some(server_version));
            apply_cloud_watch_watermarks(
                manager,
                latest_cursor,
                latest_availability_cursor,
                latest_removed_at_ms,
                cursor,
                availability_cursor,
                removed_at_ms,
            );
            Ok(())
        }
        Response::Error { message, .. } => bail!(message),
        _ => bail!("云端返回了无效的订阅响应"),
    }
}

pub(super) fn retry_delay(failure_count: usize) -> Duration {
    let index = failure_count.saturating_sub(1).min(RETRY_SECONDS.len() - 1);
    let base = RETRY_SECONDS[index];
    let jitter = (Utc::now().timestamp_subsec_millis() as u64) % (base / 4 + 1);
    Duration::from_secs(base + jitter)
}

pub(super) fn retry_schedule(failure_count: usize) -> (Duration, String) {
    let delay = retry_delay(failure_count);
    (delay, (Utc::now() + delay).to_rfc3339())
}

/// Counts only failures that occurred without an intervening confirmed watch response.
pub(super) fn next_cloud_watch_failure_count(failure_count: usize, watch_ready: bool) -> usize {
    if watch_ready {
        return 1;
    }
    failure_count.saturating_add(1)
}
