use super::*;

impl SyncManager {
    /// Redeems a short-lived invitation over an encrypted Iroh connection.
    pub async fn join_group(
        self: &Arc<Self>,
        code: &PairingCode,
        replace_existing: bool,
    ) -> Result<()> {
        let current_group = self.identity.snapshot().group;
        let same_group = current_group
            .as_ref()
            .and_then(|group| discovery_space_id(group).ok())
            .is_some_and(|space_id| space_id == code.space_id);
        if current_group.is_some() && !same_group && !replace_existing {
            bail!("本机已连接到其他设备，请先确认是否加入二维码所属设备");
        }

        let current_settings = self.app.state::<SettingsStore>().snapshot().sync;
        let mut connection_settings = current_settings.clone();
        connection_settings.cloud_enabled = code.cloud_relay_mode != CloudRelayMode::Off;
        connection_settings.cloud_relay_mode = code.cloud_relay_mode;
        connection_settings.server_relay_urls = code.server_relay_urls.clone();
        self.pairing_sessions.fetch_add(1, Ordering::AcqRel);
        let result = async {
            let endpoint = self.ensure_endpoint(&connection_settings).await?;
            let target = pairing_endpoint_addr(code)?;
            let connection =
                tokio::time::timeout(Duration::from_secs(8), endpoint.connect(target, JOIN_ALPN))
                    .await
                    .context("连接二维码设备超时")??;
            let announcement = self.device_announcement(&endpoint);
            let request_id = Uuid::new_v4().to_string();
            let mut nonce = vec![0_u8; 16];
            rand::rngs::OsRng.fill_bytes(&mut nonce);
            let request = JoinRequest {
                version: JOIN_PROTOCOL_VERSION,
                request_id,
                nonce,
                device_id: announcement.device_id,
                device_name: announcement.device_name,
                platform: announcement.platform,
                endpoint_id: announcement.endpoint_id,
                direct_addresses: announcement.direct_addresses,
                relay_urls: announcement.relay_urls,
                invitation_id: Some(code.invitation_id.clone()),
                invitation_secret: Some(code.invitation_secret.clone()),
            };
            let response = tokio::time::timeout(
                Duration::from_secs(JOIN_TIMEOUT_SECS) + JOIN_HANDSHAKE_TIMEOUT * 2,
                call_join(&connection, request, Some((self, replace_existing, true))),
            )
            .await
            .context("二维码加入申请已超时")??;
            match response {
                JoinResponse::Approved { .. } => Ok(()),
                JoinResponse::Rejected => bail!("二维码设备拒绝了加入申请"),
                JoinResponse::Expired => bail!("配对码已过期，请重新生成"),
                JoinResponse::Error { message } => bail!(message),
            }
        }
        .await;
        self.pairing_sessions.fetch_sub(1, Ordering::AcqRel);
        if let Err(error) = self.stop_runtime().await {
            log::debug!("reset pairing endpoint failed: {error}");
        }
        if self.app.state::<SettingsStore>().snapshot().sync.enabled {
            self.wake();
            self.emit_updated();
        }
        result
    }

    /// Applies approved group bootstrap data and clears routes from a previous sync space.
    pub(super) async fn apply_pairing_bootstrap(
        &self,
        bootstrap: &PairingBootstrap,
        replace_existing: bool,
        preserve_runtime: bool,
    ) -> Result<()> {
        let _cloud_guard = self.runtime_state.cloud_cycle_lock.lock().await;
        let _lan_guard = self.runtime_state.lan_cycle_lock.write().await;
        let current_group = self.identity.snapshot().group;
        let switching_group = current_group
            .as_ref()
            .is_some_and(|group| group != &bootstrap.group);
        if switching_group && !replace_existing {
            bail!("本机已连接到其他设备，请先确认是否加入二维码所属设备");
        }
        if current_group.as_ref() != Some(&bootstrap.group) {
            if !preserve_runtime {
                self.stop_runtime().await?;
            }
            let pool = self.pool().await;
            let _enqueue_guard = self.delivery.lock_enqueue().await;
            if switching_group {
                repository::clear_group_state(&pool).await?;
            }
            self.pending_observations
                .lock()
                .expect("pending clipboard observations poisoned")
                .clear();
            self.identity.set_group(Some(bootstrap.group.clone()))?;
        }
        let pool = self.pool().await;
        repository::clear_cloud_retention_snapshot(&pool).await?;
        repository::restore_and_upsert_peer(&pool, &bootstrap.inviter).await?;
        let cloud_enabled = !bootstrap.server_endpoint_id.trim().is_empty();
        let settings = self
            .app
            .state::<SettingsStore>()
            .update(serde_json::json!({
                "sync": {
                    "enabled": true,
                    "cloudEnabled": cloud_enabled,
                    "cloudRelayMode": bootstrap.cloud_relay_mode,
                    "serverEndpointId": bootstrap.server_endpoint_id,
                    "serverDirectAddresses": bootstrap.server_direct_addresses,
                    "serverRelayUrls": bootstrap.server_relay_urls,
                }
            }))?;
        crate::commands::emit_settings_updated(&self.app, &settings);
        Ok(())
    }

    pub async fn remove_peer(self: &Arc<Self>, device_id: &str) -> Result<()> {
        let device_id = device_id.trim();
        if device_id.is_empty() || device_id == self.identity.snapshot().device_id {
            bail!("无法删除该设备");
        }
        let _cloud_guard = self.runtime_state.cloud_cycle_lock.lock().await;
        let _lan_guard = self.runtime_state.lan_cycle_lock.write().await;
        let pool = self.pool().await;
        let peer = repository::list_peers(&pool)
            .await?
            .into_iter()
            .find(|peer| peer.announcement.device_id == device_id);
        if repository::remove_peer(&pool, device_id).await?.is_none() {
            bail!("未找到指定的已配对设备");
        }
        self.clear_peer_suspension(device_id);

        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let identity = self.identity.snapshot();
        let group = identity.group.clone();
        if settings.enabled && settings.lan_enabled {
            if let (Some(peer), Some(group)) = (peer, group) {
                let result = async {
                    let endpoint = self.ensure_endpoint(&settings).await?;
                    let address = peer_endpoint_addr(&peer.announcement)?;
                    let connection =
                        connect_peer(&endpoint, address, Duration::from_secs(8)).await?;
                    self.sync_removed_devices(
                        &connection,
                        &pool,
                        &group,
                        &identity.device_id,
                        &endpoint.id().to_string(),
                        false,
                    )
                    .await
                }
                .await;
                if let Err(error) = result {
                    // 删除已经在本地生效；直投失败后仍由 Hub 或其他设备继续传播。
                    log::debug!("direct removed-device notification failed: {error}");
                }
            }
        }
        self.invalidate_lan_connection_by_device(device_id);
        self.wake_transfer();
        self.emit_updated();
        Ok(())
    }

    pub async fn leave_group(&self) -> Result<()> {
        let _cloud_guard = self.runtime_state.cloud_cycle_lock.lock().await;
        let _lan_guard = self.runtime_state.lan_cycle_lock.write().await;
        self.stop_runtime().await?;
        let pool = self.pool().await;
        let _enqueue_guard = self.delivery.lock_enqueue().await;
        self.identity.set_group(None)?;
        repository::clear_group_state(&pool).await?;
        self.pending_observations
            .lock()
            .expect("pending clipboard observations poisoned")
            .clear();
        self.runtime_state
            .lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .clear();
        self.wake();
        Ok(())
    }

    pub fn set_device_name(&self, name: String) -> Result<()> {
        self.identity.update_device_name(name)?;
        if let Ok(runtime) = self.runtime_state.endpoint.try_lock() {
            if let Some(runtime) = runtime.as_ref() {
                self.update_discovery_metadata(&runtime.endpoint, runtime.config.lan_enabled)?;
            }
        }
        self.wake_transfer();
        Ok(())
    }

    pub fn set_cloud_relay_auth_token(&self, token: Option<String>) -> Result<()> {
        self.identity.set_cloud_relay_auth_token(token)?;
        self.wake();
        self.emit_updated();
        Ok(())
    }

    pub async fn pairing_code(self: &Arc<Self>) -> Result<String> {
        self.build_pairing_code().await?.encode()
    }

    pub(super) async fn build_pairing_code(self: &Arc<Self>) -> Result<PairingCode> {
        let identity = self.identity.snapshot();
        let group = identity.group.context("请先启用同步或连接已有设备")?;
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let endpoint = self.ensure_endpoint(&settings).await?;
        let invitation_id = Uuid::new_v4().to_string();
        let mut invitation_secret = vec![0_u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut invitation_secret);
        let expires_at_ms = Utc::now().timestamp_millis()
            + i64::try_from(PAIRING_INVITATION_TTL_SECS * 1_000).context("配对码有效期无效")?;
        let mut invitations = self.pairing_invitations.lock().await;
        invitations.clear();
        invitations.insert(
            invitation_id.clone(),
            PairingInvitation {
                secret_hash: *blake3::hash(&invitation_secret).as_bytes(),
                expires_at_ms,
            },
        );
        drop(invitations);
        let mut inviter = self.announcement(&endpoint);
        inviter.relay_urls = endpoint
            .addr()
            .relay_urls()
            .map(ToString::to_string)
            .collect();
        Ok(PairingCode {
            version: JOIN_PROTOCOL_VERSION,
            invitation_id,
            invitation_secret,
            expires_at_ms,
            space_id: discovery_space_id(&group)?,
            inviter,
            cloud_relay_mode: if settings.cloud_enabled {
                settings.cloud_relay_mode
            } else {
                CloudRelayMode::Off
            },
            server_relay_urls: if settings.cloud_enabled {
                settings.server_relay_urls
            } else {
                Vec::new()
            },
        })
    }

    pub(super) async fn build_pairing_bootstrap(&self) -> Result<PairingBootstrap> {
        let identity = self.identity.snapshot();
        let group = identity.group.context("请先启用同步或连接已有设备")?;
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let endpoint = self
            .runtime_state
            .endpoint
            .lock()
            .await
            .as_ref()
            .map(|runtime| runtime.endpoint.clone())
            .context("同步端点尚未就绪")?;
        Ok(PairingBootstrap {
            group,
            server_endpoint_id: if settings.cloud_enabled {
                settings.server_endpoint_id
            } else {
                String::new()
            },
            server_direct_addresses: if settings.cloud_enabled {
                settings.server_direct_addresses
            } else {
                Vec::new()
            },
            server_relay_urls: if settings.cloud_enabled {
                settings.server_relay_urls
            } else {
                Vec::new()
            },
            inviter: self.announcement(&endpoint),
            cloud_relay_mode: if settings.cloud_enabled {
                settings.cloud_relay_mode
            } else {
                CloudRelayMode::Off
            },
        })
    }

    pub(super) async fn consume_pairing_invitation(
        &self,
        invitation_id: &str,
        secret: &[u8],
    ) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        let mut invitations = self.pairing_invitations.lock().await;
        invitations.retain(|_, invitation| invitation.expires_at_ms > now);
        let invitation = invitations
            .get(invitation_id)
            .context("配对码已过期或已使用，请重新生成")?;
        if invitation.expires_at_ms <= now
            || secret.len() != 32
            || invitation.secret_hash != *blake3::hash(secret).as_bytes()
        {
            bail!("配对码无效，请重新生成");
        }
        invitations.remove(invitation_id);
        Ok(())
    }

    /// Scans briefly and returns nearby sync spaces grouped by their anonymous identifier.
    pub async fn discover_nearby_spaces(self: &Arc<Self>) -> Result<Vec<NearbySyncSpace>> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.lan_enabled {
            bail!("请先开启局域网同步");
        }
        self.pairing_sessions.fetch_add(1, Ordering::AcqRel);
        let result = async {
            #[cfg(target_os = "android")]
            let _lan_discovery_guard = AndroidLanDiscoveryGuard::acquire();
            let _endpoint = self.ensure_endpoint(&settings).await?;
            let deadline = Instant::now() + NEARBY_DISCOVERY_TIMEOUT;
            loop {
                // Register before reading the cache so an event between the read and await
                // cannot be lost. Existing fresh results still return immediately.
                let discovered = self.runtime_state.nearby_wake.notified();
                tokio::pin!(discovered);
                discovered.as_mut().enable();
                let spaces = self.nearby_spaces();
                if !spaces.is_empty() {
                    break Ok(spaces);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() || tokio::time::timeout(remaining, discovered).await.is_err()
                {
                    break Ok(self.nearby_spaces());
                }
            }
        }
        .await;
        self.pairing_sessions.fetch_sub(1, Ordering::AcqRel);
        if !settings.enabled {
            self.stop_runtime().await?;
        }
        result
    }

    pub fn outgoing_join_attempt(&self, request_id: &str) -> Option<NearbyJoinAttempt> {
        self.outgoing_join_attempts
            .read()
            .expect("outgoing join attempts poisoned")
            .get(request_id)
            .cloned()
    }

    pub async fn incoming_join_requests(&self) -> Vec<IncomingJoinRequest> {
        self.incoming_join_requests
            .lock()
            .await
            .values()
            .map(|pending| pending.public.clone())
            .collect()
    }

    /// Starts an encrypted join request without blocking the command for the approval timeout.
    pub async fn request_nearby_join(
        self: &Arc<Self>,
        endpoint_id: &str,
        replace_existing: bool,
    ) -> Result<NearbyJoinAttempt> {
        let entry = self
            .runtime_state
            .nearby_devices
            .read()
            .expect("nearby devices poisoned")
            .get(endpoint_id)
            .cloned()
            .context("附近设备已离线，请重新扫描")?;
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.lan_enabled {
            bail!("请先开启局域网同步");
        }
        let current_group = self.identity.snapshot().group;
        let same_group = current_group
            .as_ref()
            .and_then(|group| discovery_space_id(group).ok())
            .is_some_and(|space_id| space_id == entry.metadata.space_id);
        if current_group.is_some() && !same_group && !replace_existing {
            bail!("本机已连接到其他设备，请先确认是否加入附近设备");
        }
        let join_address = EndpointAddr::from_parts(
            entry.address.id,
            entry
                .address
                .ip_addrs()
                .filter(|address| is_lan_ip(address.ip()))
                .map(|address| TransportAddr::Ip(*address)),
        );
        if join_address.is_empty() {
            bail!("附近设备没有可用的局域网地址");
        }
        self.pairing_sessions.fetch_add(1, Ordering::AcqRel);
        let endpoint = match self.ensure_endpoint(&settings).await {
            Ok(endpoint) => endpoint,
            Err(error) => {
                self.pairing_sessions.fetch_sub(1, Ordering::AcqRel);
                return Err(error);
            }
        };
        let request_id = Uuid::new_v4().to_string();
        let mut nonce = vec![0_u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        let comparison_code =
            comparison_code(&request_id, &nonce, &endpoint.id().to_string(), endpoint_id);
        let expires_at = Utc::now() + chrono::Duration::seconds(JOIN_TIMEOUT_SECS as i64);
        let attempt = NearbyJoinAttempt {
            request_id: request_id.clone(),
            target_device_name: entry.metadata.device_name.clone(),
            comparison_code,
            state: NearbyJoinState::Pending,
            expires_at: expires_at.to_rfc3339(),
            last_error: None,
        };
        {
            let mut attempts = self
                .outgoing_join_attempts
                .write()
                .expect("outgoing join attempts poisoned");
            if attempts.len() >= 16 {
                attempts.retain(|_, value| value.state == NearbyJoinState::Pending);
            }
            attempts.insert(request_id.clone(), attempt.clone());
        }

        let announcement = self.device_announcement(&endpoint);
        let request = JoinRequest {
            version: JOIN_PROTOCOL_VERSION,
            request_id: request_id.clone(),
            nonce,
            device_id: announcement.device_id,
            device_name: announcement.device_name,
            platform: announcement.platform,
            endpoint_id: announcement.endpoint_id,
            direct_addresses: announcement.direct_addresses,
            relay_urls: announcement.relay_urls,
            invitation_id: None,
            invitation_secret: None,
        };
        let manager = self.clone();
        tauri::async_runtime::spawn(async move {
            let response = async {
                let connection = tokio::time::timeout(Duration::from_secs(8), async {
                    let connection = endpoint.connect(join_address, JOIN_ALPN).await?;
                    ensure_lan_connection(&connection, LAN_PATH_SELECTION_TIMEOUT).await?;
                    Ok::<_, anyhow::Error>(connection)
                })
                .await
                .context("连接附近设备超时")??;
                tokio::time::timeout(
                    Duration::from_secs(JOIN_TIMEOUT_SECS) + JOIN_HANDSHAKE_TIMEOUT * 2,
                    call_join(
                        &connection,
                        request,
                        Some((manager.as_ref(), replace_existing, true)),
                    ),
                )
                .await
                .context("加入申请已超时")?
            }
            .await;
            manager.finish_outgoing_join(&request_id, response);
            manager.pairing_sessions.fetch_sub(1, Ordering::AcqRel);
            if let Err(error) = manager.stop_runtime().await {
                log::debug!("reset nearby pairing endpoint failed: {error}");
            }
            let settings = manager.app.state::<SettingsStore>().snapshot().sync;
            if settings.enabled {
                manager.wake();
                manager.emit_updated();
            }
        });
        Ok(attempt)
    }

    pub async fn respond_nearby_join(
        self: &Arc<Self>,
        request_id: &str,
        approved: bool,
    ) -> Result<()> {
        let Some(pending) = self.incoming_join_requests.lock().await.remove(request_id) else {
            bail!("加入申请已失效");
        };
        if !approved {
            let _ = pending.responder.send(JoinResponse::Rejected);
            return Ok(());
        }

        let bootstrap = match self.build_pairing_bootstrap().await {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                let _ = pending.responder.send(JoinResponse::Error {
                    message: error.to_string(),
                });
                return Err(error);
            }
        };
        pending
            .responder
            .send(JoinResponse::Approved {
                bootstrap: Box::new(bootstrap),
            })
            .map_err(|_| anyhow::anyhow!("申请设备已断开"))?;
        Ok(())
    }
}
