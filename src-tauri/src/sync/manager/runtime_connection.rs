use super::*;

impl SyncManager {
    /// Starts one Iroh endpoint with target-scoped discovery and the configured cloud Relay mode.
    pub(super) async fn ensure_endpoint(
        self: &Arc<Self>,
        settings: &SyncSettings,
    ) -> Result<Endpoint> {
        let relay_token = self.identity.cloud_relay_auth_token();
        let config = endpoint_runtime_config(settings, relay_token.as_deref());
        let relay_mode = cloud_relay_mode(settings, relay_token.as_deref())?;
        let secret_key = self.identity.secret_key()?;
        let mut runtime = self.runtime_state.endpoint.lock().await;
        if let Some(runtime) = runtime.as_ref() {
            if runtime.config == config {
                self.update_discovery_metadata(&runtime.endpoint, settings.lan_enabled)?;
                return Ok(runtime.endpoint.clone());
            }
        }
        let mdns = if settings.lan_enabled {
            let builder = MdnsAddressLookup::builder()
                .service_name("ecopaste-v1")
                .addr_filter(ipv4_direct_addr_filter());
            #[cfg(target_os = "android")]
            let builder = {
                let interfaces = self
                    .runtime_state
                    .android_lan_multicast_interfaces
                    .read()
                    .expect("Android LAN multicast interfaces poisoned")
                    .clone();
                builder
                    .multicast_enabled(!interfaces.is_empty())
                    .multicast_interfaces_v4(interfaces)
            };
            match builder.build(secret_key.public()) {
                Ok(mdns) => Some(mdns),
                Err(error) => {
                    log::warn!("LAN discovery is unavailable: {error}");
                    None
                }
            }
        } else {
            None
        };
        if let Some(previous) = runtime.take() {
            previous
                .router
                .shutdown()
                .await
                .context("failed to restart Iroh sync endpoint")?;
        }
        self.clear_connection_caches();
        *self
            .runtime_state
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute::default();
        let mut builder = Endpoint::builder(presets::Minimal)
            .transport_config(sync_transport_config(false))
            .clear_ip_transports()
            .bind_addr((Ipv4Addr::UNSPECIFIED, 0))
            .context("configure IPv4-only Iroh sync endpoint")?
            .secret_key(secret_key)
            .relay_mode(relay_mode);
        if settings.cloud_enabled && settings.cloud_relay_mode == CloudRelayMode::Public {
            if let Ok(target) = settings.server_endpoint_id.trim().parse() {
                builder = builder.address_lookup(CloudPkarrResolverBuilder { target });
            }
        }
        if let Some(mdns) = mdns.as_ref() {
            builder = builder.address_lookup(mdns.clone());
        }
        let endpoint = builder
            .bind()
            .await
            .context("failed to bind Iroh sync endpoint")?;
        log::info!(
            "Iroh sync endpoint bound IPv4-only: sockets={:?}",
            endpoint.bound_sockets()
        );
        if let Some(mdns) = mdns.as_ref() {
            self.configure_mdns_multicast(mdns, &endpoint.addr()).await;
        }
        self.update_discovery_metadata(&endpoint, settings.lan_enabled)?;
        let router = Router::builder(endpoint.clone())
            .accept(
                ALPN,
                PeerService {
                    manager: Arc::downgrade(self),
                },
            )
            .accept(
                JOIN_ALPN,
                JoinService {
                    manager: Arc::downgrade(self),
                },
            )
            .spawn();
        spawn_endpoint_watchers(Arc::downgrade(self), &endpoint, mdns.clone());
        *runtime = Some(EndpointRuntime {
            endpoint: endpoint.clone(),
            router,
            config,
            #[cfg(target_os = "android")]
            mdns,
        });
        Ok(endpoint)
    }

    /// Releases sockets, relay connections and the protocol router while sync is disabled.
    pub(super) async fn stop_runtime(&self) -> Result<()> {
        if self.pairing_sessions.load(Ordering::Acquire) > 0 {
            return Ok(());
        }
        let runtime = self.runtime_state.endpoint.lock().await.take();
        if let Some(runtime) = runtime {
            runtime
                .router
                .shutdown()
                .await
                .context("failed to stop Iroh sync endpoint")?;
        }
        self.clear_connection_caches();
        *self
            .runtime_state
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute::default();
        Ok(())
    }

    pub(super) async fn pool(&self) -> SqlitePool {
        self.app.state::<db::DatabaseState>().pool().await
    }

    pub(super) fn set_channel_status(
        &self,
        target: SyncTarget,
        state: SyncChannelState,
        error: Option<&str>,
        succeeded: bool,
    ) {
        let lock = match target {
            SyncTarget::Lan => &self.runtime_state.lan_status,
            SyncTarget::Cloud => &self.runtime_state.cloud_status,
        };
        update_channel_status(lock, state, error, succeeded, None);
    }

    pub(super) fn set_channel_retry_status(
        &self,
        target: SyncTarget,
        error: &str,
        next_retry_at: &str,
    ) {
        let lock = match target {
            SyncTarget::Lan => &self.runtime_state.lan_status,
            SyncTarget::Cloud => &self.runtime_state.cloud_status,
        };
        update_channel_status(
            lock,
            SyncChannelState::Error,
            Some(error),
            false,
            Some(next_retry_at),
        );
    }

    pub(super) fn set_cloud_watch_status(
        &self,
        state: SyncChannelState,
        error: Option<&str>,
        succeeded: bool,
    ) {
        update_channel_status(
            &self.runtime_state.cloud_watch_status,
            state,
            error,
            succeeded,
            None,
        );
    }

    pub(super) fn set_cloud_watch_retry_status(&self, error: &str, next_retry_at: &str) {
        update_channel_status(
            &self.runtime_state.cloud_watch_status,
            SyncChannelState::Error,
            Some(error),
            false,
            Some(next_retry_at),
        );
    }

    pub(super) fn set_cloud_path(&self, connection: &Connection) {
        let (address, transport) = connection_path(connection);
        *self
            .runtime_state
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute {
            address,
            transport: transport.map(str::to_owned),
        };
    }

    pub(super) fn update_cloud_server_version(&self, version: Option<String>) {
        let Some(version) = version else {
            return;
        };
        let mut current = self
            .runtime_state
            .cloud_server_version
            .write()
            .expect("cloud server version poisoned");
        if current.as_ref() == Some(&version) {
            return;
        }

        *current = Some(version);
        drop(current);
        self.emit_updated();
    }

    pub(super) async fn connect_cloud(
        self: &Arc<Self>,
        endpoint: &Endpoint,
        server: EndpointAddr,
    ) -> Result<Connection> {
        if let Some(connection) = self.cached_cloud_connection() {
            return Ok(connection);
        }
        let _guard = self.runtime_state.cloud_connect_lock.lock().await;
        if let Some(connection) = self.cached_cloud_connection() {
            return Ok(connection);
        }
        let transport_config = sync_transport_config(true);
        let options = ConnectOptions::new().with_transport_config(transport_config);
        let connection = tokio::time::timeout(Duration::from_secs(8), async {
            let connecting = endpoint.connect_with_opts(server, ALPN, options).await?;
            Ok::<_, anyhow::Error>(connecting.await?)
        })
        .await
        .context("云端同步连接超时")??;
        log::info!(
            "cloud connection established: connectionId={} keepAliveMs={} pathKeepAliveMs={} segmentationOffload=true atMs={}",
            connection.stable_id(),
            CLOUD_PATH_KEEP_ALIVE_INTERVAL.as_millis(),
            CLOUD_PATH_KEEP_ALIVE_INTERVAL.as_millis(),
            Utc::now().timestamp_millis(),
        );
        *self
            .runtime_state
            .cloud_connection
            .write()
            .expect("cloud connection cache poisoned") = Some(connection.clone());
        self.runtime_state.cloud_connection_ready.notify_waiters();
        self.runtime_state.source_asset_wake.notify_one();
        self.runtime_state
            .cloud_session
            .write()
            .expect("cloud session state poisoned")
            .take();
        self.runtime_state
            .cloud_server_version
            .write()
            .expect("cloud server version poisoned")
            .take();
        Ok(connection)
    }

    /// Explicit user actions may interrupt backoff, but only the watch worker creates connections.
    pub(super) async fn request_cloud_connection(
        &self,
        wait_timeout: Duration,
    ) -> Result<Connection> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled
            || self.identity.snapshot().group.is_none()
            || !server_is_configured(&settings)
        {
            bail!("云端同步未配置");
        }
        if let Some(connection) = self.cached_cloud_connection() {
            return Ok(connection);
        }

        self.runtime_state.watch_wake.notify_one();
        tokio::time::timeout(wait_timeout, async {
            loop {
                let notified = self.runtime_state.cloud_connection_ready.notified();
                if let Some(connection) = self.cached_cloud_connection() {
                    return connection;
                }
                notified.await;
            }
        })
        .await
        .context("云端同步连接超时")
    }

    /// Background transfers consume only the connection published by the watch supervisor.
    pub(super) fn configured_cloud_connection(
        &self,
        settings: &SyncSettings,
    ) -> Result<Option<Connection>> {
        if !server_is_configured(settings) {
            return Ok(None);
        }

        self.cached_cloud_connection()
            .context("云端同步连接尚未就绪")
            .map(Some)
    }

    pub(super) fn cached_cloud_connection(&self) -> Option<Connection> {
        self.runtime_state
            .cloud_connection
            .read()
            .expect("cloud connection cache poisoned")
            .as_ref()
            .filter(|connection| connection.close_reason().is_none())
            .cloned()
    }

    pub(super) fn invalidate_cloud_connection(&self, stable_id: usize) {
        let removed = {
            let mut connection = self
                .runtime_state
                .cloud_connection
                .write()
                .expect("cloud connection cache poisoned");
            if connection
                .as_ref()
                .is_some_and(|connection| connection.stable_id() == stable_id)
            {
                if let Some(connection) = connection.take() {
                    connection.close(0_u32.into(), b"cloud connection invalidated");
                }
                true
            } else {
                false
            }
        };
        if removed {
            self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Idle, None, false);
            self.runtime_state
                .confirmed_cloud_source_icons
                .write()
                .expect("confirmed cloud source icons poisoned")
                .clear_connection(stable_id);
            let mut session = self
                .runtime_state
                .cloud_session
                .write()
                .expect("cloud session state poisoned");
            if session
                .as_ref()
                .is_some_and(|session| session.connection_id == stable_id)
            {
                session.take();
            }
        }
    }

    /// Atomically evicts the shared cloud connection after an external network-path change.
    #[cfg(target_os = "android")]
    pub(super) fn invalidate_current_cloud_connection(&self) -> Option<usize> {
        let connection = self
            .runtime_state
            .cloud_connection
            .write()
            .expect("cloud connection cache poisoned")
            .take()?;
        let stable_id = connection.stable_id();
        connection.close(0_u32.into(), b"cloud network path changed");
        self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Idle, None, false);
        self.runtime_state
            .confirmed_cloud_source_icons
            .write()
            .expect("confirmed cloud source icons poisoned")
            .clear_connection(stable_id);
        let mut session = self
            .runtime_state
            .cloud_session
            .write()
            .expect("cloud session state poisoned");
        if session
            .as_ref()
            .is_some_and(|session| session.connection_id == stable_id)
        {
            session.take();
        }
        drop(session);
        self.runtime_state
            .cloud_server_version
            .write()
            .expect("cloud server version poisoned")
            .take();
        *self
            .runtime_state
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute::default();
        Some(stable_id)
    }

    pub(super) fn clear_connection_caches(&self) {
        self.runtime_state
            .cloud_connection
            .write()
            .expect("cloud connection cache poisoned")
            .take();
        self.runtime_state
            .cloud_session
            .write()
            .expect("cloud session state poisoned")
            .take();
        *self
            .runtime_state
            .confirmed_cloud_source_icons
            .write()
            .expect("confirmed cloud source icons poisoned") = ConfirmedCloudSourceIcons::default();
        self.runtime_state
            .cloud_server_version
            .write()
            .expect("cloud server version poisoned")
            .take();
        self.runtime_state
            .lan_connections
            .write()
            .expect("LAN connection cache poisoned")
            .clear();
    }

    /// 合并同一轮任务产生的无载荷刷新通知；消费者仍读取最后的完整状态。
    pub(super) fn emit_updated(&self) {
        if self
            .runtime_state
            .update_notification_pending
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        let pending = self.runtime_state.update_notification_pending.clone();
        let app = self.app.clone();
        tauri::async_runtime::spawn(async move {
            tokio::task::yield_now().await;
            pending.store(false, Ordering::Release);
            if let Err(error) = app.emit(SYNC_UPDATED_EVENT, ()) {
                log::debug!("emit sync update failed: {error}");
            }
            #[cfg(target_os = "android")]
            crate::commands::android::notify_overlay_sync_status_changed();
        });
    }
}
