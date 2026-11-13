use super::*;

impl SyncManager {
    pub(super) async fn run_cloud_cycle(self: &Arc<Self>) -> Result<()> {
        let _guard = self.runtime_state.cloud_cycle_lock.lock().await;
        self.run_cycle_locked(SyncTarget::Cloud, None, Duration::ZERO)
            .await?;
        Ok(())
    }

    /// Runs one actor-owned peer cycle and preserves the transport failure category.
    pub(super) async fn run_lan_peer_actor_cycle(
        self: &Arc<Self>,
        device_id: &str,
        connect_timeout: Duration,
    ) -> Result<LanPeerSyncOutcome> {
        let _guard = self.runtime_state.lan_cycle_lock.read().await;
        self.run_cycle_locked(SyncTarget::Lan, Some(device_id), connect_timeout)
            .await?
            .context("LAN peer cycle produced no outcome")
    }

    pub(super) async fn run_cycle_locked(
        self: &Arc<Self>,
        target: SyncTarget,
        peer_device_id: Option<&str>,
        lan_connect_timeout: Duration,
    ) -> Result<Option<LanPeerSyncOutcome>> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let identity = self.identity.snapshot();
        if !settings.enabled {
            self.set_channel_status(SyncTarget::Lan, SyncChannelState::Disabled, None, false);
            self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Disabled, None, false);
            self.stop_runtime().await?;
            return Ok(None);
        }
        let Some(group) = identity.group else {
            self.set_channel_status(SyncTarget::Lan, SyncChannelState::Disabled, None, false);
            self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Disabled, None, false);
            self.stop_runtime().await?;
            return Ok(None);
        };
        if !settings.lan_enabled {
            self.set_channel_status(SyncTarget::Lan, SyncChannelState::Disabled, None, false);
        }
        if !settings.lan_enabled && !server_is_configured(&settings) {
            self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Disabled, None, false);
            self.stop_runtime().await?;
            self.emit_updated();
            if target == SyncTarget::Cloud {
                bail!("云端同步未配置");
            }
            return Ok(None);
        }
        let endpoint = self.ensure_endpoint(&settings).await?;
        let pool = self.pool().await;
        let cycle = SyncCycleContext {
            target,
            settings: settings.clone(),
            device_id: identity.device_id.clone(),
            device: self.device_announcement_for(
                &endpoint,
                &identity.device_id,
                &identity.device_name,
            ),
            group: group.clone(),
            endpoint: endpoint.clone(),
            pool: pool.clone(),
        };
        {
            let _maintenance_guard = self.maintenance_lock.lock().await;
            self.backfill_recent_history(&pool, &group).await?;
            let _ = self
                .materialize_local_operations(false, 0, None, Some(&cycle))
                .await?;
            self.repair_history_timestamps(&pool, &group).await?;
        }
        let mut lan_succeeded = false;
        let mut lan_transfer_error = None;
        let mut lan_connection_error = None;
        let mut lan_attempted = false;
        let mut lan_failed = false;
        let mut requested_lan_outcome = None;
        if settings.lan_enabled && cycle.target != SyncTarget::Cloud {
            let peers = repository::list_peers(&pool)
                .await?
                .into_iter()
                .filter(|peer| peer.announcement.device_id != identity.device_id)
                .filter(|peer| {
                    peer_device_id.is_none_or(|device_id| peer.announcement.device_id == device_id)
                })
                .collect::<Vec<_>>();
            if peer_device_id.is_some() && peers.is_empty() {
                bail!("未找到指定的已配对设备");
            }
            let has_live_connection = peers.iter().any(|peer| {
                self.cached_lan_connection(&peer.announcement.device_id)
                    .is_some()
            });
            if peers.is_empty() {
                self.set_channel_status(SyncTarget::Lan, SyncChannelState::Idle, None, false);
            } else if !has_live_connection {
                self.set_channel_status(SyncTarget::Lan, SyncChannelState::Connecting, None, false);
            }
            lan_attempted = !peers.is_empty();
            for batch in peers.chunks(3) {
                let mut tasks = tokio::task::JoinSet::new();
                for peer in batch.iter().cloned() {
                    tasks.spawn(self.clone().sync_lan_peer(
                        cycle.clone(),
                        peer,
                        lan_connect_timeout,
                    ));
                }
                while let Some(result) = tasks.join_next().await {
                    let outcome = result.context("LAN peer sync task failed")??;
                    if peer_device_id.is_some() {
                        requested_lan_outcome = Some(outcome.clone());
                    }
                    match outcome {
                        LanPeerSyncOutcome::Succeeded => lan_succeeded = true,
                        LanPeerSyncOutcome::ConnectionFailed(message) => {
                            lan_failed = true;
                            lan_connection_error.get_or_insert(message);
                        }
                        LanPeerSyncOutcome::TransferFailed(message) => {
                            lan_failed = true;
                            lan_transfer_error = Some(message);
                        }
                    }
                }
            }
            let any_peer_online = repository::list_peer_statuses(&pool)
                .await?
                .into_iter()
                .any(|peer| peer.state == SyncChannelState::Online);
            if lan_succeeded || any_peer_online {
                self.set_channel_status(SyncTarget::Lan, SyncChannelState::Online, None, true);
            } else if let Some(error) = lan_transfer_error.as_deref() {
                self.set_channel_status(
                    SyncTarget::Lan,
                    SyncChannelState::Error,
                    Some(error),
                    false,
                );
            } else {
                self.set_channel_status(SyncTarget::Lan, SyncChannelState::Idle, None, false);
            }
        }

        let mut cloud_succeeded = false;
        let mut cloud_error = None;
        if cycle.target != SyncTarget::Lan {
            let cloud_cycle_started_at = Instant::now();
            match self.configured_cloud_connection(&settings) {
                Ok(Some(connection)) => {
                    let group_started_at = Instant::now();
                    let result = async {
                        let initialized_group =
                            self.ensure_cloud_group(&connection, &group).await?;
                        let group_elapsed = group_started_at.elapsed();
                        let transfer_started_at = Instant::now();
                        let (cursor, bootstrap_target) = self
                            .prepare_cloud_cursor(&connection, &group, &pool, false)
                            .await?;
                        let latest = self
                            .sync_connection(
                                &connection,
                                &SyncConnectionTarget::Cloud,
                                cursor,
                                &cycle,
                            )
                            .await?;
                        let bootstrap_reached = if let Some(target) = bootstrap_target {
                            repository::cloud_cursor(&pool).await? >= target
                        } else {
                            false
                        };
                        if bootstrap_reached {
                            let local_cleanup_succeeded = self
                                .run_local_retention_cleanup(CleanupTrigger::CloudBootstrap)
                                .await;
                            if local_cleanup_succeeded {
                                repository::mark_cloud_bootstrap_completed(&pool, &group.group_id)
                                    .await?;
                            }
                        }
                        if latest.has_more {
                            self.wake_cloud_transfer();
                        }
                        Ok::<_, anyhow::Error>((
                            initialized_group,
                            group_elapsed,
                            transfer_started_at.elapsed(),
                        ))
                    }
                    .await;
                    match result {
                        Ok((initialized_group, group_elapsed, transfer_elapsed)) => {
                            self.set_cloud_path(&connection);
                            cloud_succeeded = true;
                            log::info!(
                                "cloud sync cycle completed: initializedGroup={initialized_group} groupMs={} transferMs={} totalMs={}",
                                group_elapsed.as_millis(),
                                transfer_elapsed.as_millis(),
                                cloud_cycle_started_at.elapsed().as_millis()
                            );
                        }
                        Err(error) => {
                            if should_invalidate_cloud_connection(&connection, &error) {
                                self.invalidate_cloud_connection(connection.stable_id());
                                self.runtime_state.watch_wake.notify_one();
                            }
                            cloud_error = Some(error.to_string());
                        }
                    }
                    if cloud_succeeded {
                        self.set_channel_status(
                            SyncTarget::Cloud,
                            SyncChannelState::Online,
                            None,
                            true,
                        );
                    } else {
                        let pending = repository::pending_cloud_events_for_target(
                            &pool,
                            CLOUD_TARGET,
                            &identity.device_id,
                            MAX_EVENTS_PER_BATCH,
                        )
                        .await?;
                        let event_ids = pending
                            .into_iter()
                            .map(|stored| stored.event.event_id)
                            .collect::<Vec<_>>();
                        if let Some(error) = cloud_error.as_deref() {
                            self.delivery
                                .failed(&pool, CLOUD_TARGET, &event_ids, error)
                                .await?;
                        }
                        self.set_channel_status(
                            SyncTarget::Cloud,
                            SyncChannelState::Error,
                            cloud_error.as_deref(),
                            false,
                        );
                    }
                }
                Ok(None) => self.set_channel_status(
                    SyncTarget::Cloud,
                    SyncChannelState::Disabled,
                    None,
                    false,
                ),
                Err(error) => {
                    cloud_error = Some(error.to_string());
                    self.set_channel_status(
                        SyncTarget::Cloud,
                        SyncChannelState::Error,
                        cloud_error.as_deref(),
                        false,
                    );
                }
            }
        }

        if lan_succeeded || cloud_succeeded {
            repository::mark_success(&pool).await?;
        }
        self.emit_updated();
        if requested_lan_outcome.is_some() {
            return Ok(requested_lan_outcome);
        }
        if settings.lan_enabled && cycle.target == SyncTarget::Lan && !lan_succeeded {
            bail!(
                "{}",
                lan_transfer_error
                    .or(lan_connection_error)
                    .unwrap_or_else(|| "当前没有可用的局域网设备".to_owned())
            );
        }
        if cycle.target == SyncTarget::Cloud && !cloud_succeeded {
            bail!(
                "{}",
                cloud_error.unwrap_or_else(|| "云端同步未配置".to_owned())
            );
        }
        if cycle.target != SyncTarget::Lan && server_is_configured(&settings) && !cloud_succeeded {
            bail!(
                "{}",
                cloud_error.unwrap_or_else(|| "云端同步失败".to_owned())
            );
        }
        if settings.lan_enabled && lan_attempted && lan_failed {
            if let Some(error) = lan_transfer_error.or(lan_connection_error) {
                bail!(error);
            }
        }
        if !lan_succeeded && !cloud_succeeded {
            if let Some(error) = cloud_error {
                bail!(error);
            }
        }
        Ok(None)
    }

    pub(super) async fn sync_lan_peer(
        self: Arc<Self>,
        cycle: SyncCycleContext,
        peer: SyncPeer,
        connect_timeout: Duration,
    ) -> Result<LanPeerSyncOutcome> {
        let pool = cycle.pool.clone();
        let endpoint = cycle.endpoint.clone();
        let device_id = peer.announcement.device_id.clone();
        let peer_lock = self.lan_peer_lock(&device_id);
        let _peer_guard = peer_lock.lock().await;
        let target = SyncConnectionTarget::peer(device_id.clone());
        let address = match peer_endpoint_addr(&peer.announcement) {
            Ok(value) => value,
            Err(error) => {
                log::debug!("ignore invalid discovered peer route: {error}");
                let message = error.to_string();
                repository::mark_peer_offline(&pool, &device_id, Some(&message)).await?;
                return Ok(LanPeerSyncOutcome::ConnectionFailed(message));
            }
        };
        let cached_connection = self.cached_lan_connection(&device_id).filter(|connection| {
            if lan_connection_matches_discovery(connection, &address) {
                return true;
            }
            self.invalidate_lan_connection(&device_id, connection.stable_id());
            connection.close(0_u32.into(), b"LAN route no longer discovered");
            false
        });
        let connection = match cached_connection {
            Some(connection) => connection,
            None => {
                repository::mark_peer_connecting(&pool, &device_id).await?;
                match connect_peer(&endpoint, address, connect_timeout).await {
                    Ok(connection) => {
                        super::protocol_handlers::spawn_peer_connection_service(
                            self.clone(),
                            connection.clone(),
                        );
                        self.store_lan_connection(&device_id, connection.clone());
                        connection
                    }
                    Err(error) => {
                        let message = error.to_string();
                        log::debug!("LAN peer {device_id} unavailable: {message}");
                        repository::mark_peer_offline(&pool, &device_id, Some(&message)).await?;
                        return Ok(LanPeerSyncOutcome::ConnectionFailed(message));
                    }
                }
            }
        };
        match self
            .sync_connection(&connection, &target, peer.pull_cursor, &cycle)
            .await
        {
            Ok(latest) => {
                let (connected_address, transport) = connection_path(&connection);
                repository::mark_peer_online(
                    &pool,
                    &device_id,
                    connected_address.as_deref(),
                    transport,
                )
                .await?;
                if latest.has_more {
                    if let Some(actor) = self
                        .runtime_state
                        .lan_peer_actors
                        .read()
                        .expect("LAN peer actors poisoned")
                        .get(&device_id)
                    {
                        actor.queue_work();
                    } else {
                        self.wake_lan_transfer();
                    }
                }
                Ok(LanPeerSyncOutcome::Succeeded)
            }
            Err(error) => {
                let message = error.to_string();
                let connection_failed =
                    is_lan_attempt_timeout(&error) || connection.close_reason().is_some();
                let invalidate = connection_failed || is_shared_sync_failure(&connection, &error);
                if invalidate && self.invalidate_lan_connection(&device_id, connection.stable_id())
                {
                    connection.close(0_u32.into(), b"LAN connection invalidated");
                    if connection_failed {
                        repository::mark_peer_offline(&pool, &device_id, Some(&message)).await?;
                    } else {
                        repository::mark_peer_error(&pool, &device_id, &message).await?;
                    }
                } else if !invalidate {
                    repository::mark_peer_error(&pool, &device_id, &message).await?;
                }
                if connection_failed {
                    Ok(LanPeerSyncOutcome::ConnectionFailed(message))
                } else {
                    Ok(LanPeerSyncOutcome::TransferFailed(message))
                }
            }
        }
    }

    pub(super) fn lan_peer_lock(&self, device_id: &str) -> Arc<Mutex<()>> {
        if let Some(lock) = self
            .runtime_state
            .lan_peer_locks
            .read()
            .expect("LAN peer locks poisoned")
            .get(device_id)
            .cloned()
        {
            return lock;
        }
        self.runtime_state
            .lan_peer_locks
            .write()
            .expect("LAN peer locks poisoned")
            .entry(device_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(super) fn cached_lan_connection(&self, device_id: &str) -> Option<Connection> {
        let connection = self
            .runtime_state
            .lan_connections
            .read()
            .expect("LAN connection cache poisoned")
            .get(device_id)
            .cloned();
        connection.filter(|connection| connection.close_reason().is_none())
    }

    pub(super) fn store_lan_connection(self: &Arc<Self>, device_id: &str, connection: Connection) {
        let stable_id = connection.stable_id();
        let should_watch = {
            let mut connections = self
                .runtime_state
                .lan_connections
                .write()
                .expect("LAN connection cache poisoned");
            if connections
                .get(device_id)
                .is_some_and(|current| current.stable_id() == stable_id)
            {
                false
            } else {
                connections.insert(device_id.to_owned(), connection.clone());
                true
            }
        };
        if !should_watch {
            return;
        }

        let manager = Arc::downgrade(self);
        let device_id = device_id.to_owned();
        tauri::async_runtime::spawn(async move {
            let reason = connection.closed().await.to_string();
            let Some(manager) = manager.upgrade() else {
                return;
            };
            manager
                .handle_lan_connection_closed(&device_id, stable_id, &reason)
                .await;
        });
    }

    /// Applies one connection-close event without letting an old path overwrite a replacement.
    pub(super) async fn handle_lan_connection_closed(
        &self,
        device_id: &str,
        stable_id: usize,
        reason: &str,
    ) {
        if !self.invalidate_lan_connection(device_id, stable_id) {
            return;
        }
        if self.cached_lan_connection(device_id).is_some() {
            return;
        }

        let pool = self.pool().await;
        if let Err(error) = repository::mark_peer_offline(&pool, device_id, Some(reason)).await {
            log::debug!("mark closed LAN peer offline failed: {error}");
        }
        if let Some(connection) = self.cached_lan_connection(device_id) {
            let (connected_address, transport) = connection_path(&connection);
            if let Err(error) = repository::mark_peer_online(
                &pool,
                device_id,
                connected_address.as_deref(),
                transport,
            )
            .await
            {
                log::debug!("restore replaced LAN peer status failed: {error}");
            }
            self.peer_connection_ready(device_id);
            self.set_channel_status(SyncTarget::Lan, SyncChannelState::Online, None, true);
        } else {
            self.peer_connection_closed(device_id);
            match repository::list_peer_statuses(&pool).await {
                Ok(peers)
                    if peers
                        .iter()
                        .any(|peer| peer.state == SyncChannelState::Online) => {}
                Ok(_) => {
                    self.set_channel_status(SyncTarget::Lan, SyncChannelState::Idle, None, false)
                }
                Err(error) => log::debug!("refresh closed LAN channel status failed: {error}"),
            }
        }
        self.emit_updated();
    }

    pub(super) fn invalidate_lan_connection(&self, device_id: &str, stable_id: usize) -> bool {
        let mut connections = self
            .runtime_state
            .lan_connections
            .write()
            .expect("LAN connection cache poisoned");
        if connections
            .get(device_id)
            .is_some_and(|connection| connection.stable_id() == stable_id)
        {
            connections.remove(device_id);
            return true;
        }
        false
    }

    pub(super) fn invalidate_lan_connection_by_device(&self, device_id: &str) {
        self.runtime_state
            .lan_connections
            .write()
            .expect("LAN connection cache poisoned")
            .remove(device_id);
    }

    pub(super) fn stop_lan_peer_actors(&self) {
        let actors = self
            .runtime_state
            .lan_peer_actors
            .write()
            .expect("LAN peer actors poisoned")
            .drain()
            .map(|(_, actor)| actor)
            .collect::<Vec<_>>();
        for actor in actors {
            actor.stop();
        }
        self.runtime_state
            .lan_connections
            .write()
            .expect("LAN connection cache poisoned")
            .clear();
    }

    /// Delivers discovery evidence to the peer actor that owns reconnect decisions.
    pub(super) fn peer_route_available(&self, device_id: &str, changed: bool) {
        let actor = self
            .runtime_state
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .get(device_id)
            .cloned();
        let Some(actor) = actor else {
            self.wake_lan_transfer();
            return;
        };
        actor.route_available(changed);
    }

    /// Records a proven live inbound connection without starting a reciprocal empty sync.
    pub(super) fn peer_connection_ready(&self, device_id: &str) {
        if let Some(actor) = self
            .runtime_state
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .get(device_id)
        {
            actor.connection_ready();
        }
    }

    /// Moves one peer actor to offline until discovery or a manual reconnect arrives.
    pub(super) fn peer_connection_closed(&self, device_id: &str) {
        if let Some(actor) = self
            .runtime_state
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .get(device_id)
        {
            actor.connection_closed();
        }
    }

    pub(super) async fn ensure_cloud_group(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
    ) -> Result<bool> {
        if self.cloud_group_is_ready(connection, &group.group_id) {
            return Ok(false);
        }
        let _guard = self.runtime_state.cloud_session_lock.lock().await;
        if self.cloud_group_is_ready(connection, &group.group_id) {
            return Ok(false);
        }

        let language = crate::i18n::current_language(&self.app);
        let metadata_response = call_cloud(connection, Request::CloudHealth, language).await?;
        let protocol_version = match metadata_response {
            Response::CloudHealth {
                protocol_version,
                server_version,
                ..
            } => {
                log::info!(
                    "cloud Hub metadata received: protocolVersion={protocol_version} serverVersion={server_version}"
                );
                self.update_cloud_server_version(Some(server_version));
                protocol_version
            }
            Response::Error { message, .. } => bail!(message),
            _ => bail!("云端返回了无效的协议响应"),
        };
        if protocol_version != PROTOCOL_VERSION {
            bail!(
                "同步协议版本不一致：客户端 {PROTOCOL_VERSION}，服务端 {protocol_version}，请同时升级客户端和 Hub"
            );
        }
        match call_cloud(
            connection,
            Request::CreateGroup {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
            },
            language,
        )
        .await?
        {
            Response::GroupCreated => {}
            Response::Error { message, .. } => bail!(message),
            _ => bail!("云端返回了无效的建组响应"),
        }
        *self
            .runtime_state
            .cloud_session
            .write()
            .expect("cloud session state poisoned") = Some(CloudSessionState {
            connection_id: connection.stable_id(),
            group_id: group.group_id.clone(),
        });
        self.runtime_state.source_asset_wake.notify_one();
        Ok(true)
    }

    pub(super) fn cloud_group_is_ready(&self, connection: &Connection, group_id: &str) -> bool {
        self.runtime_state
            .cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .is_some_and(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
    }

    pub(super) async fn prepare_cloud_cursor(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        pool: &SqlitePool,
        force: bool,
    ) -> Result<(u64, Option<u64>)> {
        let current = repository::cloud_cursor(pool).await?;
        if !force && repository::cloud_bootstrap_completed(pool, &group.group_id).await? {
            return Ok((current, None));
        }
        if !force && repository::cloud_bootstrap_pending(pool, &group.group_id).await? {
            let target = repository::cloud_bootstrap_target(pool, &group.group_id)
                .await?
                .context("missing cloud bootstrap target cursor")?;
            return Ok((current, Some(target)));
        }
        if !force && current > 0 {
            repository::mark_cloud_bootstrap_completed(pool, &group.group_id).await?;
            return Ok((current, None));
        }

        let response = call_cloud(
            connection,
            Request::BootstrapPlan {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
            },
            crate::i18n::current_language(&self.app),
        )
        .await?;
        let Response::BootstrapPlan {
            start_cursor,
            latest_cursor,
        } = response
        else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("云端返回了无效的初始同步范围");
        };
        repository::start_cloud_bootstrap(pool, &group.group_id, start_cursor, latest_cursor)
            .await?;
        Ok((start_cursor, Some(latest_cursor)))
    }

    /// Exchanges removal tombstones with one LAN peer before its normal event batch.
    pub(super) async fn sync_lan_removed_devices(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        own_device_id: &str,
        own_endpoint_id: &str,
    ) -> Result<()> {
        let local = repository::removed_devices(pool).await?;
        let response = call_lan(
            connection,
            Request::SyncRemovedDevices {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                devices: local,
            },
        )
        .await?;
        let Response::RemovedDevices { devices } = response else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("局域网设备返回了无效的成员状态响应");
        };
        self.apply_removed_devices(pool, devices, own_device_id, own_endpoint_id)
            .await
    }

    /// Publishes an approved membership change through the current combined cloud request.
    pub(super) async fn publish_cloud_membership(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        device: &DeviceAnnouncement,
    ) -> Result<()> {
        let response = call_cloud(
            connection,
            Request::CloudSync {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                device: self.cloud_device_announcement_for(device),
                after_cursor: repository::cloud_cursor(pool).await?,
                events: Vec::new(),
                removed_devices: repository::removed_devices(pool).await?,
                limit: 1,
                bounded_hub: false,
            },
            crate::i18n::current_language(&self.app),
        )
        .await?;
        let Response::CloudSynced {
            removed_devices, ..
        } = response
        else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("云端返回了无效的成员状态响应");
        };
        self.apply_removed_devices(
            pool,
            removed_devices,
            &device.device_id,
            &device.endpoint_id,
        )
        .await
    }

    pub(super) async fn apply_removed_devices(
        &self,
        pool: &SqlitePool,
        devices: Vec<RemovedDevice>,
        own_device_id: &str,
        own_endpoint_id: &str,
    ) -> Result<()> {
        let changed = repository::merge_removed_devices(pool, &devices).await?;
        let removed_self = repository::removed_devices(pool)
            .await?
            .iter()
            .any(|device| {
                device.is_removed()
                    && (device.device_id == own_device_id || device.endpoint_id == own_endpoint_id)
            });
        // Membership history is not evidence that a LAN route is currently reachable.
        if changed {
            self.emit_updated();
        }
        if removed_self {
            self.leave_after_removal(pool).await?;
            bail!("本设备已被移出同步空间");
        }
        Ok(())
    }

    /// Clears only synchronization state when another group member removes this device.
    pub(super) async fn leave_after_removal(&self, pool: &SqlitePool) -> Result<()> {
        let _enqueue_guard = self.delivery.lock_enqueue().await;
        self.identity.set_group(None)?;
        repository::clear_group_state(pool).await?;
        self.pending_observations
            .lock()
            .expect("pending clipboard observations poisoned")
            .clear();
        let settings = self
            .app
            .state::<SettingsStore>()
            .update(serde_json::json!({ "sync": { "enabled": false } }))?;
        crate::commands::emit_settings_updated(&self.app, &settings);
        self.wake();
        self.emit_updated();
        Ok(())
    }
}
