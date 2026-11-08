use super::*;

impl SyncManager {
    pub(super) async fn run_cycle(
        self: &Arc<Self>,
        target: SyncTarget,
        peer_device_id: Option<&str>,
        lan_connect_timeout: Duration,
    ) -> Result<()> {
        match target {
            SyncTarget::Lan => {
                let _guard = self.runtime_state.lan_cycle_lock.read().await;
                self.run_cycle_locked(target, peer_device_id, lan_connect_timeout)
                    .await
            }
            SyncTarget::Cloud => {
                let _guard = self.runtime_state.cloud_cycle_lock.lock().await;
                self.run_cycle_locked(target, peer_device_id, lan_connect_timeout)
                    .await
            }
        }
    }

    pub(super) async fn run_cycle_locked(
        self: &Arc<Self>,
        target: SyncTarget,
        peer_device_id: Option<&str>,
        lan_connect_timeout: Duration,
    ) -> Result<()> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let identity = self.identity.snapshot();
        if !settings.enabled {
            self.set_channel_status(SyncTarget::Lan, SyncChannelState::Disabled, None, false);
            self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Disabled, None, false);
            self.stop_runtime().await?;
            return Ok(());
        }
        let Some(group) = identity.group else {
            self.set_channel_status(SyncTarget::Lan, SyncChannelState::Disabled, None, false);
            self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Disabled, None, false);
            self.stop_runtime().await?;
            return Ok(());
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
            return Ok(());
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
        if settings.lan_enabled && cycle.target != SyncTarget::Cloud {
            let suspended_peers = self
                .runtime_state
                .lan_suspended_peers
                .read()
                .expect("LAN suspended peers poisoned")
                .clone();
            let peers = repository::list_peers(&pool)
                .await?
                .into_iter()
                .filter(|peer| peer.announcement.device_id != identity.device_id)
                .filter(|peer| {
                    peer_device_id.is_none_or(|device_id| peer.announcement.device_id == device_id)
                })
                .filter(|peer| {
                    peer_device_id.is_some()
                        || !suspended_peers.contains(&peer.announcement.device_id)
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
                    match result.context("LAN peer sync task failed")?? {
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
                        self.sync_removed_devices(
                            &connection,
                            &pool,
                            &group,
                            &cycle.device_id,
                            &cycle.device.endpoint_id,
                            true,
                        )
                        .await?;
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
                        let pending = repository::pending_origin_events_for_target(
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
        Ok(())
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
        let preferred = repository::preferred_peer_address(&pool, &device_id).await?;
        let address =
            match peer_endpoint_addr_with_preferred(&peer.announcement, preferred.as_deref()) {
                Ok(value) => value,
                Err(error) => {
                    log::debug!("ignore invalid cached peer route: {error}");
                    let message = error.to_string();
                    repository::mark_peer_offline(&pool, &device_id, Some(&message)).await?;
                    return Ok(LanPeerSyncOutcome::ConnectionFailed(message));
                }
            };
        let connection = match self.cached_lan_connection(&device_id) {
            Some(connection) => connection,
            None => {
                repository::mark_peer_connecting(&pool, &device_id).await?;
                match connect_peer(&endpoint, address, connect_timeout).await {
                    Ok(connection) => {
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
                self.clear_peer_suspension(&device_id);
                if latest.has_more {
                    if let Some(actor) = self
                        .runtime_state
                        .lan_peer_actors
                        .read()
                        .expect("LAN peer actors poisoned")
                        .get(&device_id)
                    {
                        actor.notify();
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
                if self.invalidate_lan_connection(&device_id, connection.stable_id()) {
                    connection.close(0_u32.into(), b"LAN connection invalidated");
                    if connection_failed {
                        repository::mark_peer_offline(&pool, &device_id, Some(&message)).await?;
                    } else {
                        repository::mark_peer_error(&pool, &device_id, &message).await?;
                    }
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
        self.suspend_peer(device_id);
        if self.cached_lan_connection(device_id).is_some() {
            self.clear_peer_suspension(device_id);
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
            self.clear_peer_suspension(device_id);
            self.set_channel_status(SyncTarget::Lan, SyncChannelState::Online, None, true);
        } else {
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

    pub(super) fn clear_peer_suspension(&self, device_id: &str) {
        self.runtime_state
            .lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .remove(device_id);
    }

    pub(super) fn suspend_peer(&self, device_id: &str) {
        self.runtime_state
            .lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .insert(device_id.to_owned());
    }

    pub(super) fn resume_peer(&self, device_id: &str) {
        self.clear_peer_suspension(device_id);
        if let Some(actor) = self
            .runtime_state
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .get(device_id)
        {
            actor.resume();
        }
    }

    /// Reconnects a suspended peer once when mDNS proves that it is reachable again.
    pub(super) fn resume_peer_from_discovery(&self, device_id: &str) {
        if self.cached_lan_connection(device_id).is_some() {
            return;
        }
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
        self.clear_peer_suspension(device_id);
        actor.resume();
    }

    /// Records a proven live inbound connection without starting a reciprocal empty sync.
    pub(super) fn peer_connection_ready(&self, device_id: &str) {
        self.clear_peer_suspension(device_id);
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

    pub(super) fn resume_all_peers(&self) {
        self.runtime_state
            .lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .clear();
        for actor in self
            .runtime_state
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .values()
        {
            actor.resume();
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
        let create_group = call_cloud(
            connection,
            Request::CreateGroup {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
            },
            language,
        );
        let read_metadata = call_cloud(connection, Request::HealthV2, language);
        let (group_response, metadata_response) = tokio::join!(create_group, read_metadata);
        match group_response? {
            Response::GroupCreated => {}
            Response::Error { message, .. } => bail!(message),
            _ => bail!("云端返回了无效的建组响应"),
        }
        let protocol_version = match metadata_response {
            Ok(Response::HealthV2 {
                protocol_version,
                server_version,
                ..
            }) => {
                log::info!(
                    "cloud Hub metadata received: protocolVersion={protocol_version} serverVersion={server_version}"
                );
                self.update_cloud_server_version(Some(server_version));
                protocol_version
            }
            Ok(_) => {
                log::debug!("cloud Hub returned an invalid health response; using legacy sync");
                0
            }
            Err(error)
                if is_cloud_attempt_timeout(&error) || connection.close_reason().is_some() =>
            {
                return Err(error);
            }
            Err(error) => {
                log::debug!("query cloud Hub metadata failed; using legacy sync: {error}");
                0
            }
        };
        if protocol_version < STREAMING_BLOB_PROTOCOL_VERSION {
            bail!("同步服务版本过低，请升级到支持新版同步协议的版本");
        }
        *self
            .runtime_state
            .cloud_session
            .write()
            .expect("cloud session state poisoned") = Some(CloudSessionState {
            connection_id: connection.stable_id(),
            group_id: group.group_id.clone(),
            protocol_version,
            sync_v2_supported: protocol_version >= SYNC_V2_PROTOCOL_VERSION,
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

    pub(super) fn cloud_sync_v2_supported(&self, connection: &Connection, group_id: &str) -> bool {
        self.runtime_state
            .cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.sync_v2_supported)
    }

    pub(super) fn cloud_async_source_icons_supported(
        &self,
        connection: &Connection,
        group_id: &str,
    ) -> bool {
        self.runtime_state
            .cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.protocol_version >= ASYNC_SOURCE_ICON_PROTOCOL_VERSION)
    }

    pub(super) fn cloud_redundant_watch_supported(
        &self,
        connection: &Connection,
        group_id: &str,
    ) -> bool {
        self.runtime_state
            .cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.protocol_version >= REDUNDANT_WATCH_PROTOCOL_VERSION)
    }

    pub(super) fn bounded_hub_supported(&self, connection: &Connection, group_id: &str) -> bool {
        self.runtime_state
            .cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.protocol_version >= BOUNDED_HUB_PROTOCOL_VERSION)
    }

    pub(super) fn cloud_optional_blob_refs_supported(
        &self,
        connection: &Connection,
        group_id: &str,
    ) -> bool {
        self.runtime_state
            .cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.protocol_version >= OPTIONAL_BLOB_REFS_PROTOCOL_VERSION)
    }

    pub(super) async fn prepare_cloud_cursor(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        pool: &SqlitePool,
        force: bool,
    ) -> Result<(u64, Option<u64>)> {
        let current = repository::cloud_cursor(pool).await?;
        if !self.bounded_hub_supported(connection, &group.group_id) {
            return Ok((current, None));
        }
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

    /// Exchanges group-wide removal tombstones before normal events so a removed device leaves
    /// before it can upload another clipboard batch.
    pub(super) async fn sync_removed_devices(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        own_device_id: &str,
        own_endpoint_id: &str,
        cloud: bool,
    ) -> Result<()> {
        let local = repository::removed_devices(pool).await?;
        let request = Request::SyncRemovedDevices {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            devices: local,
        };
        let result = if cloud {
            call_cloud(
                connection,
                request,
                crate::i18n::current_language(&self.app),
            )
            .await
        } else {
            call_lan(connection, request).await
        };
        let response = match result {
            Ok(response) => response,
            Err(error) if cloud && is_cloud_attempt_timeout(&error) => return Err(error),
            Err(error)
                if !cloud
                    && (is_lan_attempt_timeout(&error) || connection.close_reason().is_some()) =>
            {
                return Err(error);
            }
            Err(error) => {
                // 旧版对端不识别新请求时仍保留原有剪贴板同步能力。
                log::debug!("peer does not support removed-device exchange: {error}");
                return Ok(());
            }
        };
        let Response::RemovedDevices { devices } = response else {
            if let Response::Error { message, .. } = response {
                log::debug!("removed-device exchange was rejected: {message}");
            }
            return Ok(());
        };
        self.apply_removed_devices(pool, devices, own_device_id, own_endpoint_id)
            .await
    }

    pub(super) async fn apply_removed_devices(
        &self,
        pool: &SqlitePool,
        devices: Vec<RemovedDevice>,
        own_device_id: &str,
        own_endpoint_id: &str,
    ) -> Result<()> {
        let removed_self = devices.iter().any(|device| {
            device.is_removed()
                && (device.device_id == own_device_id || device.endpoint_id == own_endpoint_id)
        });
        repository::merge_removed_devices(pool, &devices).await?;
        // The full membership ledger is replayed on every cloud cycle; it is not a LAN online event.
        self.emit_updated();
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
