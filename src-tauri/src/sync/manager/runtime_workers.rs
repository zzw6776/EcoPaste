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
        match manager
            .run_cycle(SyncTarget::Cloud, None, Duration::from_secs(8))
            .await
        {
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

/// Reconciles long-lived per-peer actors and fans out one event-driven synchronization wake.
pub(super) async fn dispatch_lan_peer_actors(manager: &Arc<SyncManager>) -> Result<()> {
    let pool = manager.pool().await;
    let own_device_id = manager.identity.snapshot().device_id;
    let device_ids = repository::list_peers(&pool)
        .await?
        .into_iter()
        .map(|peer| peer.announcement.device_id)
        .filter(|device_id| device_id != &own_device_id)
        .collect::<HashSet<_>>();
    let mut started = Vec::new();
    let (actors, removed) = {
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
                let actor = Arc::new(LanPeerActor::new());
                actors.insert(device_id.clone(), actor.clone());
                started.push((device_id, actor));
            }
        }
        (actors.values().cloned().collect::<Vec<_>>(), removed)
    };
    for device_id in removed {
        manager.invalidate_lan_connection_by_device(&device_id);
    }
    for (device_id, actor) in started {
        tauri::async_runtime::spawn(lan_peer_worker(manager.clone(), device_id, actor.clone()));
        actor.resume();
    }
    for actor in actors {
        actor.notify();
    }
    Ok(())
}

pub(super) async fn lan_peer_worker(
    manager: Arc<SyncManager>,
    device_id: String,
    actor: Arc<LanPeerActor>,
) {
    loop {
        actor.wake.notified().await;
        let force_connect = actor.force_connect.swap(false, Ordering::AcqRel);
        let connection_ready = actor.connection_ready.swap(false, Ordering::AcqRel);
        let pending_work = actor.pending_work.swap(false, Ordering::AcqRel);
        if actor.stopped.load(Ordering::Acquire) {
            break;
        }
        if connection_ready && !force_connect && !pending_work {
            continue;
        }
        let suspended = manager
            .runtime_state
            .lan_suspended_peers
            .read()
            .expect("LAN suspended peers poisoned")
            .contains(&device_id);
        if !should_run_lan_peer_cycle(force_connect, pending_work, suspended) {
            continue;
        }
        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled || !settings.lan_enabled {
            continue;
        }
        if force_connect {
            manager.clear_peer_suspension(&device_id);
        }
        match manager
            .run_cycle(SyncTarget::Lan, Some(&device_id), LAN_EVENT_CONNECT_TIMEOUT)
            .await
        {
            Ok(()) => {
                actor.force_connect.store(false, Ordering::Release);
            }
            Err(error) => {
                manager.suspend_peer(&device_id);
                log::debug!("LAN peer {device_id} is offline; wait for an online event: {error}");
                manager.emit_updated();
            }
        }
    }
}

/// Pending clipboard work must not turn an offline peer back into a polling loop.
pub(super) fn should_run_lan_peer_cycle(
    force_connect: bool,
    pending_work: bool,
    suspended: bool,
) -> bool {
    (force_connect || pending_work) && (!suspended || force_connect)
}

/// Connects only to the explicit LAN routes supplied by mDNS/cached peer metadata.
pub(super) async fn connect_peer(
    endpoint: &Endpoint,
    cached_address: EndpointAddr,
    total_timeout: Duration,
) -> Result<Connection> {
    if cached_address.is_empty() {
        bail!("当前没有可用的局域网地址");
    }
    match tokio::time::timeout(total_timeout, async {
        let connection = endpoint.connect(cached_address, ALPN).await?;
        ensure_lan_connection(&connection, LAN_PATH_SELECTION_TIMEOUT).await?;
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
                        Ok(Some(device_id))
                            if routes_changed || newly_discovered || presence_changed =>
                        {
                            manager.resume_peer_from_discovery(&device_id);
                        }
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

/// Watches one cloud group on a persistent response stream and falls back for older hubs.
pub(super) async fn watch_cloud_group(
    manager: &SyncManager,
    connection: &Connection,
    group: &GroupSecrets,
    pool: &SqlitePool,
    watch_ready: &mut bool,
) -> Result<()> {
    let mut cursor = repository::cloud_cursor(pool).await?;
    let mut removed_at_ms = repository::latest_removed_at_ms(pool).await?;
    let access_token = group.access_token_bytes()?;
    if manager.cloud_redundant_watch_supported(connection, &group.group_id) {
        return watch_cloud_group_redundant(
            manager,
            connection,
            group,
            access_token,
            cursor,
            removed_at_ms,
            watch_ready,
        )
        .await;
    }

    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::WatchGroupStreamV2 {
            group_id: group.group_id.clone(),
            access_token: access_token.clone(),
            after_cursor: cursor,
            after_removed_at_ms: removed_at_ms,
        },
    )
    .await?;
    send.finish()?;

    let first_response = tokio::select! {
        _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
        response = tokio::time::timeout(Duration::from_secs(15), read_frame(&mut recv)) => {
            response
        },
    };
    let first_response = match first_response {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            log::debug!("Hub does not support versioned stream watch: {error}");
            return watch_cloud_group_legacy(
                manager,
                connection,
                group,
                access_token,
                cursor,
                removed_at_ms,
                watch_ready,
            )
            .await;
        }
        Err(error) => return Err(error).context("读取云端持续订阅首帧超时"),
    };
    if matches!(first_response, Response::Error { .. }) {
        return watch_cloud_group_legacy(
            manager,
            connection,
            group,
            access_token,
            cursor,
            removed_at_ms,
            watch_ready,
        )
        .await;
    }
    apply_cloud_watch_response(manager, first_response, &mut cursor, &mut removed_at_ms)?;
    mark_cloud_watch_ready(manager, watch_ready);

    loop {
        let response = tokio::select! {
            _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
            response = tokio::time::timeout(CLOUD_WATCH_RESPONSE_TIMEOUT, read_frame(&mut recv)) => {
                response.context("等待云端持续订阅心跳超时")??
            },
        };
        apply_cloud_watch_response(manager, response, &mut cursor, &mut removed_at_ms)?;
    }
}

/// Keeps two independently framed watch streams so one delayed packet does not gate wake-up.
pub(super) async fn watch_cloud_group_redundant(
    manager: &SyncManager,
    connection: &Connection,
    group: &GroupSecrets,
    access_token: Vec<u8>,
    mut cursor: u64,
    mut removed_at_ms: i64,
    watch_ready: &mut bool,
) -> Result<()> {
    let (responses, mut response_rx) = mpsc::channel(4);
    let mut readers = CloudWatchReaderGuard::new(Vec::new());
    let mut active_slots = [false; 2];
    let mut slot_failure_counts = [0_usize; 2];
    let mut slot_retry_at = [None; 2];
    let mut last_initial_error = None;
    let initial_streams = tokio::select! {
        _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
        streams = async {
            tokio::join!(
                open_cloud_watch_stream(
                    connection,
                    group,
                    &access_token,
                    cursor,
                    removed_at_ms,
                    PRIMARY_CLOUD_WATCH_SLOT,
                ),
                open_cloud_watch_stream(
                    connection,
                    group,
                    &access_token,
                    cursor,
                    removed_at_ms,
                    BACKUP_CLOUD_WATCH_SLOT,
                ),
            )
        } => streams,
    };
    for (watch_slot, stream) in [
        (PRIMARY_CLOUD_WATCH_SLOT, initial_streams.0),
        (BACKUP_CLOUD_WATCH_SLOT, initial_streams.1),
    ] {
        let slot_index = usize::from(watch_slot);
        match stream {
            Ok((recv, response)) => {
                match apply_cloud_watch_response(manager, response, &mut cursor, &mut removed_at_ms)
                {
                    Ok(()) => {
                        active_slots[slot_index] = true;
                        readers.push(
                            watch_slot,
                            spawn_cloud_watch_reader(watch_slot, recv, responses.clone()),
                        );
                    }
                    Err(error) => {
                        slot_failure_counts[slot_index] = 1;
                        let delay = retry_delay(slot_failure_counts[slot_index]);
                        slot_retry_at[slot_index] = Some(Instant::now() + delay);
                        log::warn!(
                            "cloud watch slot initial response invalid: slot={} retryInSecs={} error={error:#}",
                            watch_slot,
                            delay.as_secs(),
                        );
                        last_initial_error = Some(error);
                    }
                }
            }
            Err(error) => {
                slot_failure_counts[slot_index] = 1;
                let delay = retry_delay(slot_failure_counts[slot_index]);
                slot_retry_at[slot_index] = Some(Instant::now() + delay);
                log::warn!(
                    "cloud watch slot initial connection failed: slot={} retryInSecs={} error={error:#}",
                    watch_slot,
                    delay.as_secs(),
                );
                last_initial_error = Some(error);
            }
        }
    }
    if !active_slots.iter().any(|active| *active) {
        let error = last_initial_error.context("云端冗余订阅未返回错误原因")?;
        return Err(error.context("云端冗余持续订阅均不可用"));
    }

    mark_cloud_watch_ready(manager, watch_ready);
    log::info!(
        "redundant cloud watch ready: connectionId={} activeSlots={}",
        connection.stable_id(),
        active_slots.iter().filter(|active| **active).count(),
    );

    loop {
        let next_retry = next_cloud_watch_slot_retry(&slot_retry_at);
        tokio::select! {
            _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
            response = response_rx.recv() => {
                let (watch_slot, response) =
                    response.context("云端持续订阅读取任务已结束")?;
                let slot_index = usize::from(watch_slot);
                let response = response.and_then(|response| {
                    log::debug!("cloud watch frame received: slot={watch_slot}");
                    apply_cloud_watch_response(
                        manager,
                        response,
                        &mut cursor,
                        &mut removed_at_ms,
                    )
                });
                if let Err(error) = response {
                    readers.abort_slot(watch_slot);
                    active_slots[slot_index] = false;
                    if connection.close_reason().is_some()
                        || !active_slots.iter().any(|active| *active)
                    {
                        return Err(error.context("云端冗余持续订阅均不可用"));
                    }

                    slot_failure_counts[slot_index] =
                        slot_failure_counts[slot_index].saturating_add(1);
                    let delay = retry_delay(slot_failure_counts[slot_index]);
                    slot_retry_at[slot_index] = Some(Instant::now() + delay);
                    log::warn!(
                        "cloud watch slot failed; surviving slot remains online: slot={} retryInSecs={} error={error:#}",
                        watch_slot,
                        delay.as_secs(),
                    );
                }
            }
            _ = wait_for_cloud_watch_slot_retry(next_retry.map(|(_, retry_at)| retry_at)) => {
                let (slot_index, _) = next_retry.context("云端订阅重试状态不存在")?;
                let watch_slot = slot_index as u8;
                let restored_stream = tokio::select! {
                    _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
                    stream = open_cloud_watch_stream(
                        connection,
                        group,
                        &access_token,
                        cursor,
                        removed_at_ms,
                        watch_slot,
                    ) => stream,
                };
                match restored_stream {
                    Ok((recv, response)) => {
                        match apply_cloud_watch_response(
                            manager,
                            response,
                            &mut cursor,
                            &mut removed_at_ms,
                        ) {
                            Ok(()) => {
                                active_slots[slot_index] = true;
                                slot_failure_counts[slot_index] = 0;
                                slot_retry_at[slot_index] = None;
                                readers.push(
                                    watch_slot,
                                    spawn_cloud_watch_reader(
                                        watch_slot,
                                        recv,
                                        responses.clone(),
                                    ),
                                );
                                log::info!("cloud watch slot restored: slot={watch_slot}");
                            }
                            Err(error) => {
                                slot_failure_counts[slot_index] =
                                    slot_failure_counts[slot_index].saturating_add(1);
                                let delay = retry_delay(slot_failure_counts[slot_index]);
                                slot_retry_at[slot_index] = Some(Instant::now() + delay);
                                log::warn!(
                                    "cloud watch slot restore response invalid: slot={} attempt={} retryInSecs={} error={error:#}",
                                    watch_slot,
                                    slot_failure_counts[slot_index],
                                    delay.as_secs(),
                                );
                            }
                        }
                    }
                    Err(error) => {
                        if connection.close_reason().is_some() {
                            return Err(error.context("云端共享连接已失效"));
                        }

                        slot_failure_counts[slot_index] =
                            slot_failure_counts[slot_index].saturating_add(1);
                        let delay = retry_delay(slot_failure_counts[slot_index]);
                        slot_retry_at[slot_index] = Some(Instant::now() + delay);
                        log::warn!(
                            "cloud watch slot restore failed: slot={} attempt={} retryInSecs={} error={error:#}",
                            watch_slot,
                            slot_failure_counts[slot_index],
                            delay.as_secs(),
                        );
                    }
                }
            }
        }
    }
}

pub(super) fn next_cloud_watch_slot_retry(
    retry_at: &[Option<Instant>; 2],
) -> Option<(usize, Instant)> {
    retry_at
        .iter()
        .enumerate()
        .filter_map(|(slot, retry_at)| retry_at.map(|retry_at| (slot, retry_at)))
        .min_by_key(|(_, retry_at)| *retry_at)
}

pub(super) async fn wait_for_cloud_watch_slot_retry(retry_at: Option<Instant>) {
    let Some(retry_at) = retry_at else {
        std::future::pending::<()>().await;
        return;
    };

    tokio::time::sleep_until(retry_at.into()).await;
}

/// Opens one fixed watch slot and confirms that its initial response is readable.
pub(super) async fn open_cloud_watch_stream(
    connection: &Connection,
    group: &GroupSecrets,
    access_token: &[u8],
    after_cursor: u64,
    after_removed_at_ms: i64,
    watch_slot: u8,
) -> Result<(RecvStream, Response)> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::WatchGroupStreamV3 {
            group_id: group.group_id.clone(),
            access_token: access_token.to_vec(),
            after_cursor,
            after_removed_at_ms,
            watch_slot,
        },
    )
    .await?;
    send.finish()?;
    let response = tokio::time::timeout(Duration::from_secs(15), read_frame(&mut recv))
        .await
        .context("读取云端冗余订阅首帧超时")??;
    if let Response::Error { message, .. } = &response {
        bail!("{message}");
    }
    Ok((recv, response))
}

/// Owns watch reader tasks and stops both whenever their shared consumer exits.
struct CloudWatchReaderGuard {
    handles: Vec<(u8, tokio::task::JoinHandle<()>)>,
}

impl CloudWatchReaderGuard {
    fn new(handles: Vec<(u8, tokio::task::JoinHandle<()>)>) -> Self {
        Self { handles }
    }

    fn push(&mut self, watch_slot: u8, handle: tokio::task::JoinHandle<()>) {
        self.handles.retain(|(_, handle)| !handle.is_finished());
        self.handles.push((watch_slot, handle));
    }

    fn abort_slot(&mut self, watch_slot: u8) {
        self.handles.retain(|(slot, handle)| {
            if *slot != watch_slot {
                return true;
            }

            handle.abort();
            false
        });
    }
}

impl Drop for CloudWatchReaderGuard {
    fn drop(&mut self) {
        for (_, handle) in &self.handles {
            handle.abort();
        }
    }
}

/// Reads one framed watch stream without cancellation corrupting its frame boundary.
pub(super) fn spawn_cloud_watch_reader(
    watch_slot: u8,
    mut recv: RecvStream,
    responses: mpsc::Sender<(u8, Result<Response>)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let response =
                match tokio::time::timeout(CLOUD_WATCH_RESPONSE_TIMEOUT, read_frame(&mut recv))
                    .await
                {
                    Ok(response) => response.context("读取云端持续订阅响应"),
                    Err(error) => {
                        Err(anyhow::Error::new(error).context("等待云端持续订阅心跳超时"))
                    }
                };
            let should_stop = response.is_err();
            if responses.send((watch_slot, response)).await.is_err() || should_stop {
                return;
            }
        }
    })
}

/// Retains event-driven long watches when connecting to a hub without stream-watch support.
pub(super) async fn watch_cloud_group_legacy(
    manager: &SyncManager,
    connection: &Connection,
    group: &GroupSecrets,
    access_token: Vec<u8>,
    mut cursor: u64,
    mut removed_at_ms: i64,
    watch_ready: &mut bool,
) -> Result<()> {
    loop {
        let response = tokio::select! {
            _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
            response = call(
                connection,
                Request::WatchGroup {
                    group_id: group.group_id.clone(),
                    access_token: access_token.clone(),
                    after_cursor: cursor,
                    after_removed_at_ms: removed_at_ms,
                },
            ) => response,
        };
        match response {
            Ok(response @ Response::GroupChanged { .. }) => {
                apply_cloud_watch_response(manager, response, &mut cursor, &mut removed_at_ms)?;
                mark_cloud_watch_ready(manager, watch_ready);
            }
            Ok(Response::Error { .. }) | Err(_) => {
                let response = tokio::select! {
                    _ = manager.runtime_state.watch_wake.notified() => return Ok(()),
                    response = call(
                        connection,
                        Request::Watch {
                            group_id: group.group_id.clone(),
                            access_token: access_token.clone(),
                            after_cursor: cursor,
                        },
                    ) => response?,
                };
                apply_cloud_watch_response(manager, response, &mut cursor, &mut removed_at_ms)?;
                mark_cloud_watch_ready(manager, watch_ready);
            }
            Ok(_) => bail!("云端返回了无效的订阅响应"),
        }
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
    pub(super) removals_changed: bool,
}

/// Advances the local watch watermarks and reports work only when the Hub moved either one.
pub(super) fn advance_cloud_watch_watermarks(
    latest_cursor: u64,
    latest_removed_at_ms: i64,
    cursor: &mut u64,
    removed_at_ms: &mut i64,
) -> Option<CloudWatchChanges> {
    let changes = CloudWatchChanges {
        events_changed: latest_cursor > *cursor,
        removals_changed: latest_removed_at_ms > *removed_at_ms,
    };
    *cursor = (*cursor).max(latest_cursor);
    *removed_at_ms = (*removed_at_ms).max(latest_removed_at_ms);

    (changes.events_changed || changes.removals_changed).then_some(changes)
}

/// Wakes change-driven workers only for a real event or removal watermark advance.
pub(super) fn apply_cloud_watch_watermarks(
    manager: &SyncManager,
    latest_cursor: u64,
    latest_removed_at_ms: i64,
    cursor: &mut u64,
    removed_at_ms: &mut i64,
) {
    let Some(changes) =
        advance_cloud_watch_watermarks(latest_cursor, latest_removed_at_ms, cursor, removed_at_ms)
    else {
        return;
    };
    let received_at_ms = Utc::now().timestamp_millis();
    let events_changed = changes.events_changed;
    let removals_changed = changes.removals_changed;
    log::info!(
        "cloud watch change received: eventsChanged={events_changed} removalsChanged={removals_changed} latestCursor={latest_cursor} latestRemovedAtMs={latest_removed_at_ms} receivedAtMs={received_at_ms}"
    );
    manager.runtime_state.source_asset_wake.notify_one();
    manager.wake_cloud_transfer();
}

pub(super) fn apply_cloud_watch_response(
    manager: &SyncManager,
    response: Response,
    cursor: &mut u64,
    removed_at_ms: &mut i64,
) -> Result<()> {
    match response {
        Response::GroupChanged {
            latest_cursor,
            latest_removed_at_ms,
        } => {
            apply_cloud_watch_watermarks(
                manager,
                latest_cursor,
                latest_removed_at_ms,
                cursor,
                removed_at_ms,
            );
            Ok(())
        }
        Response::GroupChangedV2 {
            latest_cursor,
            latest_removed_at_ms,
            server_version,
        } => {
            manager.update_cloud_server_version(Some(server_version));
            apply_cloud_watch_watermarks(
                manager,
                latest_cursor,
                latest_removed_at_ms,
                cursor,
                removed_at_ms,
            );
            Ok(())
        }
        Response::Changed { latest_cursor } => {
            if latest_cursor > *cursor {
                log::info!(
                    "cloud watch change received: eventsChanged=true removalsChanged=false latestCursor={latest_cursor} receivedAtMs={}",
                    Utc::now().timestamp_millis()
                );
                manager.wake_cloud_transfer();
            }
            *cursor = (*cursor).max(latest_cursor);
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
