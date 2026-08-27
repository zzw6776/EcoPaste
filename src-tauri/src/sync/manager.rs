use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use ecopaste_sync_protocol::{
    read_frame, write_frame, CloudEvent, DeviceAnnouncement, EncryptedEvent, ErrorCode,
    PeerAnnouncement, Request, Response, ALPN, MAX_EVENTS_PER_BATCH,
};
use iroh::{
    endpoint::{presets, Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, EndpointAddr, Watcher,
};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use sqlx::SqlitePool;
use tauri::{AppHandle, Emitter, Manager};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, Notify},
};

use crate::{
    clipboard::{ImageStore, WritebackGuard},
    db::{
        self,
        models::{ClipboardItem, ClipboardKind, ClipboardSubKind, Platform},
    },
    settings::{SettingsStore, SyncSettings},
};

use super::{
    crypto,
    identity::{
        peer_endpoint_addr, server_endpoint_addr, server_is_configured, GroupSecrets,
        IdentityStore, PairingCode,
    },
    model::{
        BlobManifest, BlobRole, ClipboardEnvelope, CloudRecord, CloudRecordPage, StoredBlob,
        SyncChannelState, SyncChannelStatus, SyncItemStatus, SyncPairingPreview, SyncStatus,
        SyncTarget, SyncedClipboardItem,
    },
    repository,
};

const CLOUD_TARGET: &str = "cloud";
const SYNC_UPDATED_EVENT: &str = "sync://updated";
const MAX_SYNC_BLOB_BYTES: u64 = 2 * 1024 * 1024 * 1024;

pub struct SyncManager {
    app: AppHandle,
    identity: Arc<IdentityStore>,
    runtime: Mutex<Option<EndpointRuntime>>,
    wake: Notify,
    watch_wake: Notify,
    cycle_lock: Mutex<()>,
    apply_lock: Mutex<()>,
    lan_status: RwLock<SyncChannelStatus>,
    cloud_status: RwLock<SyncChannelStatus>,
    lan_retry_peers: RwLock<HashSet<String>>,
}

struct EndpointRuntime {
    endpoint: Endpoint,
    router: Router,
    _mdns: Option<MdnsAddressLookup>,
}

#[cfg(target_os = "android")]
struct AndroidLanDiscoveryGuard;

#[cfg(target_os = "android")]
impl AndroidLanDiscoveryGuard {
    fn acquire() -> Self {
        crate::commands::android::set_lan_discovery_enabled(true);
        Self
    }
}

#[cfg(target_os = "android")]
impl Drop for AndroidLanDiscoveryGuard {
    fn drop(&mut self) {
        crate::commands::android::set_lan_discovery_enabled(false);
    }
}

pub async fn init(app: &AppHandle) -> crate::core::Result<()> {
    let identity = Arc::new(IdentityStore::load_or_create(app)?);
    let manager = Arc::new(SyncManager {
        app: app.clone(),
        identity,
        runtime: Mutex::new(None),
        wake: Notify::new(),
        watch_wake: Notify::new(),
        cycle_lock: Mutex::new(()),
        apply_lock: Mutex::new(()),
        lan_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Idle)),
        cloud_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Disabled)),
        lan_retry_peers: RwLock::new(HashSet::new()),
    });
    app.manage(manager.clone());
    tauri::async_runtime::spawn(worker(manager.clone()));
    tauri::async_runtime::spawn(cloud_watch_worker(manager.clone()));
    manager.wake();
    Ok(())
}

impl SyncManager {
    pub fn wake(&self) {
        self.wake.notify_one();
        self.watch_wake.notify_one();
    }

    pub fn create_group(&self) -> Result<GroupSecrets> {
        let group = GroupSecrets::generate();
        self.identity.set_group(Some(group.clone()))?;
        self.wake();
        Ok(group)
    }

    pub fn pairing_preview(&self, code: &PairingCode) -> SyncPairingPreview {
        let same_group = self
            .identity
            .snapshot()
            .group
            .as_ref()
            .is_some_and(|group| group == code.group());
        SyncPairingPreview {
            inviter_device_name: code.inviter_device_name().to_owned(),
            same_group,
        }
    }

    /// Joins the scanned device and clears routes encrypted for a previous sync space.
    pub async fn join_group(&self, code: &PairingCode, replace_existing: bool) -> Result<()> {
        let _guard = self.cycle_lock.lock().await;
        let current_group = self.identity.snapshot().group;
        let switching_group = current_group
            .as_ref()
            .is_some_and(|group| group != code.group());
        if switching_group && !replace_existing {
            bail!("本机已连接到其他设备，请先确认是否加入二维码所属设备");
        }
        if current_group.as_ref() != Some(code.group()) {
            self.stop_runtime().await?;
            let pool = self.pool().await;
            repository::clear_group_state(&pool).await?;
            self.identity.set_group(Some(code.group().clone()))?;
        }
        let pool = self.pool().await;
        repository::upsert_peer(&pool, &code.inviter).await?;
        Ok(())
    }

    pub async fn leave_group(&self) -> Result<()> {
        let _guard = self.cycle_lock.lock().await;
        self.stop_runtime().await?;
        self.identity.set_group(None)?;
        let pool = self.pool().await;
        repository::clear_group_state(&pool).await?;
        self.lan_retry_peers
            .write()
            .expect("LAN retry peers poisoned")
            .clear();
        self.wake();
        Ok(())
    }

    pub fn set_device_name(&self, name: String) -> Result<()> {
        self.identity.update_device_name(name)?;
        self.wake();
        Ok(())
    }

    pub async fn pairing_code(self: &Arc<Self>) -> Result<String> {
        let identity = self.identity.snapshot();
        let group = identity.group.context("请先启用同步或连接已有设备")?;
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let endpoint = self.ensure_endpoint().await?;
        PairingCode {
            version: 1,
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
        }
        .encode()
    }

    pub async fn status(&self) -> Result<SyncStatus> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let identity = self.identity.snapshot();
        let pool = self.pool().await;
        let (pending_events, pending_manual_items, peer_count) =
            repository::status_counts(&pool).await?;
        let mut lan = self
            .lan_status
            .read()
            .expect("LAN sync status poisoned")
            .clone();
        let mut cloud = self
            .cloud_status
            .read()
            .expect("cloud sync status poisoned")
            .clone();
        if !settings.enabled || identity.group.is_none() {
            lan = SyncChannelStatus::new(SyncChannelState::Disabled);
            cloud = SyncChannelStatus::new(SyncChannelState::Disabled);
        } else if !server_is_configured(&settings) {
            cloud = SyncChannelStatus::new(SyncChannelState::Disabled);
        }
        Ok(SyncStatus {
            enabled: settings.enabled,
            cloud_enabled: settings.cloud_enabled,
            paired: identity.group.is_some(),
            device_id: identity.device_id,
            device_name: identity.device_name,
            group_id: identity.group.map(|group| group.group_id),
            endpoint_id: self
                .runtime
                .lock()
                .await
                .as_ref()
                .map(|runtime| runtime.endpoint.id().to_string())
                .unwrap_or_default(),
            cloud_endpoint_id: settings.server_endpoint_id,
            cloud_direct_addresses: settings.server_direct_addresses,
            cloud_relay_urls: settings.server_relay_urls,
            pending_events,
            pending_manual_items,
            peer_count,
            last_success_at: repository::last_success(&pool).await?,
            lan,
            cloud,
            peers: repository::list_peer_statuses(&pool).await?,
        })
    }

    pub async fn run_now(self: &Arc<Self>) -> Result<()> {
        self.run_cycle(None, None, Duration::from_secs(8), false)
            .await
    }

    /// Immediately reconnects all paired devices or one selected device.
    pub async fn reconnect_peer(self: &Arc<Self>, device_id: Option<String>) -> Result<()> {
        self.run_cycle(
            Some(SyncTarget::Lan),
            device_id.as_deref(),
            Duration::from_secs(8),
            false,
        )
        .await
    }

    pub async fn enqueue_item(&self, item: ClipboardItem, force_files: bool) -> Result<()> {
        self.enqueue_item_inner(item, force_files, true).await?;
        Ok(())
    }

    pub async fn sync_item_now(
        self: &Arc<Self>,
        item: ClipboardItem,
        target: SyncTarget,
    ) -> Result<SyncItemStatus> {
        self.enqueue_item_inner(item.clone(), true, false)
            .await?
            .context("此记录未满足当前同步策略")?;
        self.run_cycle(Some(target), None, Duration::from_secs(8), false)
            .await?;
        let pool = self.pool().await;
        repository::item_statuses(&pool, &[item.id])
            .await?
            .into_iter()
            .next()
            .context("同步状态不存在")
    }

    pub async fn item_statuses(&self, item_ids: &[String]) -> Result<Vec<SyncItemStatus>> {
        repository::item_statuses(&self.pool().await, item_ids).await
    }

    /// Reads encrypted Hub history without advancing the normal synchronization cursor.
    pub async fn cloud_records(
        self: &Arc<Self>,
        before_cursor: Option<u64>,
        limit: u16,
    ) -> Result<CloudRecordPage> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled || !settings.cloud_enabled {
            bail!("请先启用多端同步和云端同步");
        }
        let group = self
            .identity
            .snapshot()
            .group
            .context("请先创建或加入同步设备组")?;
        let server = server_endpoint_addr(&settings)
            .await?
            .context("请先完成云端 Hub 配置")?;
        let endpoint = self.ensure_endpoint().await?;
        let connection =
            tokio::time::timeout(Duration::from_secs(8), endpoint.connect(server, ALPN))
                .await
                .context("读取云端记录连接超时")??;
        self.ensure_cloud_group(&connection, &group).await?;
        let response = call(
            &connection,
            Request::ListEvents {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                before_cursor,
                limit: limit.clamp(1, 100),
            },
        )
        .await?;
        let Response::EventsPage {
            events,
            next_before_cursor,
            total,
        } = response
        else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("云端返回了无效的记录响应");
        };

        let identity = self.identity.snapshot();
        let mut device_names = repository::list_peer_statuses(&self.pool().await)
            .await?
            .into_iter()
            .map(|peer| (peer.device_id, peer.device_name))
            .collect::<HashMap<_, _>>();
        device_names.insert(identity.device_id, identity.device_name);
        let key = group.content_key_bytes()?;
        let records = events
            .into_iter()
            .filter_map(|cloud_event| {
                let event = cloud_event.event;
                let envelope = match crypto::decrypt_event(&key, &event).and_then(|envelope| {
                    validate_envelope(&envelope)?;
                    Ok(envelope)
                }) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        log::warn!("skip invalid cloud record {}: {error}", event.event_id);
                        return None;
                    }
                };
                let device_name = device_names
                    .get(&event.origin_device_id)
                    .cloned()
                    .unwrap_or_else(|| short_device_id(&event.origin_device_id));
                Some(cloud_record(
                    cloud_event.cursor,
                    event,
                    envelope,
                    device_name,
                ))
            })
            .collect();
        Ok(CloudRecordPage {
            records,
            next_before_cursor,
            total,
        })
    }

    async fn enqueue_item_inner(
        &self,
        item: ClipboardItem,
        force_files: bool,
        should_wake: bool,
    ) -> Result<Option<String>> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let identity = self.identity.snapshot();
        if !settings.enabled && !force_files {
            return Ok(None);
        }
        let Some(group) = identity.group else {
            if force_files {
                bail!("请先在偏好设置中启用同步或连接已有设备");
            }
            return Ok(None);
        };
        if item.is_sensitive && !settings.sync_sensitive {
            return Ok(None);
        }
        let pool = self.pool().await;
        if let Some(event_id) = repository::event_for_item(&pool, &item.id).await? {
            repository::clear_pending_item(&pool, &item.id).await?;
            if should_wake {
                self.wake();
            }
            return Ok(Some(event_id));
        }
        let event_id = uuid::Uuid::new_v4().simple().to_string();
        let (envelope, blobs) = match self
            .build_envelope(&event_id, &item, &settings, &group, force_files)
            .await?
        {
            Some(value) => value,
            None => {
                repository::mark_pending_item(&pool, &item.id, "文件超过自动同步阈值").await?;
                self.emit_updated();
                return Ok(None);
            }
        };
        let sequence = repository::next_origin_sequence(&pool).await?;
        let created_at_ms = Utc::now().timestamp_millis();
        let key = group.content_key_bytes()?;
        let event = crypto::encrypt_event(
            &key,
            event_id,
            identity.device_id,
            sequence,
            created_at_ms,
            &envelope,
        )?;
        repository::insert_event(&pool, &event, true, &blobs).await?;
        repository::link_event_to_item(&pool, &item.id, &event.event_id, "local").await?;
        repository::clear_pending_item(&pool, &item.id).await?;
        if should_wake {
            self.wake();
        }
        self.emit_updated();
        Ok(Some(event.event_id))
    }

    async fn build_envelope(
        &self,
        _event_id: &str,
        item: &ClipboardItem,
        settings: &SyncSettings,
        group: &GroupSecrets,
        force_files: bool,
    ) -> Result<Option<(ClipboardEnvelope, Vec<StoredBlob>)>> {
        let key = group.content_key_bytes()?;
        let blob_root = crate::core::paths::resources_dir(&self.app)?.join("sync-blobs");
        let mut manifests = Vec::new();
        let mut stored_blobs = Vec::new();
        match item.kind {
            ClipboardKind::Text => {}
            ClipboardKind::Image => {
                let source = self.app.state::<ImageStore>().origin_path(&item.content);
                let source_for_task = source.clone();
                let root_for_task = blob_root.clone();
                let blob = tauri::async_runtime::spawn_blocking(move || {
                    crypto::encrypt_blob(&source_for_task, &root_for_task, &key)
                })
                .await
                .context("image encryption task failed")??;
                manifests.push(BlobManifest {
                    blob_id: blob.blob_id.clone(),
                    name: item.content.clone(),
                    original_size: source.metadata()?.len(),
                    encrypted_size: blob.size,
                    role: BlobRole::Image,
                    file_index: None,
                    is_directory_archive: false,
                });
                stored_blobs.push(blob);
            }
            ClipboardKind::Files => {
                let paths = item
                    .content
                    .lines()
                    .filter(|value| !value.is_empty())
                    .map(PathBuf::from)
                    .collect::<Vec<_>>();
                if paths.is_empty() {
                    bail!("没有可同步的文件");
                }
                let threshold = u64::from(settings.auto_upload_max_mb) * 1024 * 1024;
                if !force_files
                    && paths.iter().any(|path| {
                        path.metadata()
                            .map(|metadata| {
                                metadata.is_dir() || threshold == 0 || metadata.len() > threshold
                            })
                            .unwrap_or(true)
                    })
                {
                    return Ok(None);
                }
                for (index, source) in paths.into_iter().enumerate() {
                    let name = safe_file_name(&source);
                    let source_for_task = source.clone();
                    let root_for_task = blob_root.clone();
                    let key_for_task = key;
                    let is_directory = source.is_dir();
                    let (blob, original_size) = tauri::async_runtime::spawn_blocking(move || {
                        if is_directory {
                            let temporary = tempfile::tempdir()?;
                            let archive_path = temporary.path().join("directory.zip");
                            crypto::archive_directory(&source_for_task, &archive_path)?;
                            let size = archive_path.metadata()?.len();
                            let blob =
                                crypto::encrypt_blob(&archive_path, &root_for_task, &key_for_task)?;
                            Ok::<_, anyhow::Error>((blob, size))
                        } else {
                            let size = source_for_task.metadata()?.len();
                            let blob = crypto::encrypt_blob(
                                &source_for_task,
                                &root_for_task,
                                &key_for_task,
                            )?;
                            Ok((blob, size))
                        }
                    })
                    .await
                    .context("file encryption task failed")??;
                    manifests.push(BlobManifest {
                        blob_id: blob.blob_id.clone(),
                        name,
                        original_size,
                        encrypted_size: blob.size,
                        role: BlobRole::File,
                        file_index: Some(index as u32),
                        is_directory_archive: is_directory,
                    });
                    stored_blobs.push(blob);
                }
            }
        }
        Ok(Some((
            ClipboardEnvelope {
                version: 1,
                item: SyncedClipboardItem {
                    kind: kind_string(item.kind).into(),
                    sub_kind: item.sub_kind.map(sub_kind_string).map(str::to_owned),
                    content: item.content.clone(),
                    search_text: item.search_text.clone(),
                    summary: item.summary.clone(),
                    file_types: item.file_types.clone(),
                    size: item.size,
                    width: item.width,
                    height: item.height,
                    is_sensitive: item.is_sensitive,
                    source_platform: platform_string(item.platform).into(),
                    created_at_ms: item.created_at.timestamp_millis(),
                    content_hash: item.content_hash.clone(),
                },
                blobs: manifests,
            },
            stored_blobs,
        )))
    }

    async fn run_cycle(
        self: &Arc<Self>,
        target: Option<SyncTarget>,
        peer_device_id: Option<&str>,
        lan_connect_timeout: Duration,
        retry_only: bool,
    ) -> Result<()> {
        let _guard = self.cycle_lock.lock().await;
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
        let endpoint = self.ensure_endpoint().await?;
        let pool = self.pool().await;
        let mut lan_succeeded = false;
        let mut lan_transfer_error = None;
        let mut lan_connection_error = None;
        let mut lan_attempted = false;
        let mut lan_failed = false;
        if target != Some(SyncTarget::Cloud) {
            #[cfg(target_os = "android")]
            let _lan_discovery_guard = AndroidLanDiscoveryGuard::acquire();
            let retry_peers = self
                .lan_retry_peers
                .read()
                .expect("LAN retry peers poisoned")
                .clone();
            let peers = repository::list_peers(&pool)
                .await?
                .into_iter()
                .filter(|peer| peer.announcement.device_id != identity.device_id)
                .filter(|peer| {
                    peer_device_id.is_none_or(|device_id| peer.announcement.device_id == device_id)
                })
                .filter(|peer| !retry_only || retry_peers.contains(&peer.announcement.device_id))
                .collect::<Vec<_>>();
            if peer_device_id.is_some() && peers.is_empty() {
                bail!("未找到指定的已配对设备");
            }
            if peers.is_empty() {
                if !retry_only {
                    self.set_channel_status(SyncTarget::Lan, SyncChannelState::Idle, None, false);
                }
            } else {
                self.set_channel_status(SyncTarget::Lan, SyncChannelState::Connecting, None, false);
            }
            for peer in peers {
                lan_attempted = true;
                let device_id = peer.announcement.device_id.clone();
                let target_id = format!("peer:{device_id}");
                let address = match peer_endpoint_addr(&peer.announcement) {
                    Ok(value) => value,
                    Err(error) => {
                        log::debug!("ignore invalid cached peer route: {error}");
                        let message = error.to_string();
                        repository::mark_peer_offline(&pool, &device_id, Some(&message)).await?;
                        self.mark_peer_retry(&device_id);
                        lan_failed = true;
                        lan_connection_error.get_or_insert(message);
                        continue;
                    }
                };
                repository::mark_peer_connecting(&pool, &device_id).await?;
                match connect_peer(&endpoint, address, lan_connect_timeout).await {
                    Ok(connection) => {
                        match self
                            .sync_connection(
                                &connection,
                                &target_id,
                                peer.pull_cursor,
                                &group,
                                &endpoint,
                                true,
                            )
                            .await
                        {
                            Ok(latest) => {
                                repository::set_peer_cursor(&pool, &device_id, latest).await?;
                                let (connected_address, transport) = connection_path(&connection);
                                repository::mark_peer_online(
                                    &pool,
                                    &device_id,
                                    connected_address.as_deref(),
                                    transport,
                                )
                                .await?;
                                self.clear_peer_retry(&device_id);
                                lan_succeeded = true;
                            }
                            Err(error) => {
                                let message = error.to_string();
                                repository::mark_peer_error(&pool, &device_id, &message).await?;
                                self.mark_peer_retry(&device_id);
                                lan_failed = true;
                                lan_transfer_error = Some(message);
                            }
                        }
                    }
                    Err(error) => {
                        let message = error.to_string();
                        log::debug!("LAN peer {device_id} unavailable: {message}");
                        repository::mark_peer_offline(&pool, &device_id, Some(&message)).await?;
                        self.mark_peer_retry(&device_id);
                        lan_failed = true;
                        lan_connection_error.get_or_insert(message);
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
        if target != Some(SyncTarget::Lan) {
            if let Some(server) = server_endpoint_addr(&settings).await? {
                self.set_channel_status(
                    SyncTarget::Cloud,
                    SyncChannelState::Connecting,
                    None,
                    false,
                );
                let send_pending = target == Some(SyncTarget::Cloud) || !lan_succeeded;
                match tokio::time::timeout(Duration::from_secs(8), endpoint.connect(server, ALPN))
                    .await
                {
                    Ok(Ok(connection)) => {
                        let result = async {
                            self.ensure_cloud_group(&connection, &group).await?;
                            let cursor = repository::cloud_cursor(&pool).await?;
                            let latest = self
                                .sync_connection(
                                    &connection,
                                    CLOUD_TARGET,
                                    cursor,
                                    &group,
                                    &endpoint,
                                    send_pending,
                                )
                                .await?;
                            repository::set_cloud_cursor(&pool, latest).await
                        }
                        .await;
                        match result {
                            Ok(()) => cloud_succeeded = true,
                            Err(error) => cloud_error = Some(error.to_string()),
                        }
                    }
                    Ok(Err(error)) => cloud_error = Some(format!("云端同步连接失败: {error}")),
                    Err(_) => cloud_error = Some("云端同步连接超时".to_owned()),
                }
                if cloud_succeeded {
                    self.set_channel_status(
                        SyncTarget::Cloud,
                        SyncChannelState::Online,
                        None,
                        true,
                    );
                } else {
                    if send_pending {
                        let pending = repository::pending_events_for_target(
                            &pool,
                            CLOUD_TARGET,
                            MAX_EVENTS_PER_BATCH,
                        )
                        .await?;
                        let event_ids = pending
                            .into_iter()
                            .map(|stored| stored.event.event_id)
                            .collect::<Vec<_>>();
                        if let Some(error) = cloud_error.as_deref() {
                            repository::mark_delivery_error(&pool, CLOUD_TARGET, &event_ids, error)
                                .await?;
                        }
                    }
                    self.set_channel_status(
                        SyncTarget::Cloud,
                        SyncChannelState::Error,
                        cloud_error.as_deref(),
                        false,
                    );
                }
            } else {
                self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Disabled, None, false);
            }
        }

        if lan_succeeded || cloud_succeeded {
            repository::mark_success(&pool).await?;
        }
        self.emit_updated();
        if target == Some(SyncTarget::Lan) && !lan_succeeded {
            bail!(
                "{}",
                lan_transfer_error
                    .or(lan_connection_error)
                    .unwrap_or_else(|| "当前没有可用的局域网设备".to_owned())
            );
        }
        if target == Some(SyncTarget::Cloud) && !cloud_succeeded {
            bail!(
                "{}",
                cloud_error.unwrap_or_else(|| "云端同步未配置".to_owned())
            );
        }
        if lan_attempted && lan_failed {
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

    fn mark_peer_retry(&self, device_id: &str) {
        self.lan_retry_peers
            .write()
            .expect("LAN retry peers poisoned")
            .insert(device_id.to_owned());
    }

    fn clear_peer_retry(&self, device_id: &str) {
        self.lan_retry_peers
            .write()
            .expect("LAN retry peers poisoned")
            .remove(device_id);
    }

    async fn ensure_cloud_group(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
    ) -> Result<()> {
        match call(
            connection,
            Request::CreateGroup {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
            },
        )
        .await?
        {
            Response::GroupCreated => Ok(()),
            Response::Error { message, .. } => bail!(message),
            _ => bail!("云端返回了无效的建组响应"),
        }
    }

    async fn sync_connection(
        &self,
        connection: &Connection,
        target_id: &str,
        after_cursor: u64,
        group: &GroupSecrets,
        endpoint: &Endpoint,
        send_pending: bool,
    ) -> Result<u64> {
        let pool = self.pool().await;
        let pending = if send_pending {
            repository::pending_events_for_target(&pool, target_id, MAX_EVENTS_PER_BATCH).await?
        } else {
            Vec::new()
        };
        let event_ids = pending
            .iter()
            .map(|item| item.event.event_id.clone())
            .collect::<Vec<_>>();
        repository::mark_delivery_syncing(&pool, target_id, &event_ids).await?;
        self.emit_updated();
        let result = async {
            for blob in repository::blobs_for_events(&pool, &event_ids).await? {
                upload_blob(connection, group, &blob).await?;
            }
            call(
                connection,
                Request::Sync {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    device: self.device_announcement(endpoint),
                    after_cursor,
                    events: pending.into_iter().map(|item| item.event).collect(),
                    limit: MAX_EVENTS_PER_BATCH,
                },
            )
            .await
        }
        .await;
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                repository::mark_delivery_error(&pool, target_id, &event_ids, &error.to_string())
                    .await?;
                self.emit_updated();
                return Err(error);
            }
        };
        let Response::Synced {
            events,
            peers,
            latest_cursor,
            ..
        } = response
        else {
            if let Response::Error { message, .. } = response {
                repository::mark_delivery_error(&pool, target_id, &event_ids, &message).await?;
                self.emit_updated();
                bail!(message);
            }
            bail!("同步端返回了无效响应");
        };
        repository::mark_delivered(&pool, target_id, &event_ids).await?;
        for peer in peers {
            if peer.device_id != self.identity.snapshot().device_id {
                repository::upsert_peer(&pool, &peer).await?;
            }
        }
        self.accept_remote_events(connection, events, group, target_id)
            .await?;
        Ok(latest_cursor)
    }

    async fn accept_remote_events(
        &self,
        connection: &Connection,
        events: Vec<CloudEvent>,
        group: &GroupSecrets,
        source_target: &str,
    ) -> Result<()> {
        let pool = self.pool().await;
        let received_event_ids = events
            .iter()
            .map(|cloud_event| cloud_event.event.event_id.clone())
            .collect::<Vec<_>>();
        for cloud_event in events {
            let event = cloud_event.event;
            if event.origin_device_id == self.identity.snapshot().device_id {
                repository::insert_event(&pool, &event, true, &[]).await?;
                continue;
            }
            repository::insert_event(&pool, &event, false, &[]).await?;
        }
        repository::mark_delivered(&pool, source_target, &received_event_ids).await?;

        // Replaying every still-unapplied event makes a download/apply interruption
        // recoverable even when the hub no longer returns the duplicate event.
        // LAN 入站请求与本机主动拉取可能同时看到同一事件；从查询到标记完成必须串行，
        // 否则同一远端事件会被并发写入剪贴板历史两次。
        let _apply_guard = self.apply_lock.lock().await;
        let pending_apply = repository::unapplied_events(&pool, MAX_EVENTS_PER_BATCH).await?;
        for stored in pending_apply {
            let event = stored.event;
            let envelope = match crypto::decrypt_event(&group.content_key_bytes()?, &event)
                .and_then(|envelope| {
                    validate_envelope(&envelope)?;
                    Ok(envelope)
                }) {
                Ok(envelope) => envelope,
                Err(error) => {
                    log::warn!("discard invalid sync event {}: {error}", event.event_id);
                    repository::mark_applied(&pool, &event.event_id).await?;
                    continue;
                }
            };
            let mut stored_blobs = Vec::new();
            for manifest in &envelope.blobs {
                let path = self.ensure_blob(connection, group, manifest).await?;
                stored_blobs.push(StoredBlob {
                    blob_id: manifest.blob_id.clone(),
                    encrypted_path: path.to_string_lossy().into_owned(),
                    size: manifest.encrypted_size,
                });
            }
            repository::attach_event_blobs(&pool, &event.event_id, &stored_blobs).await?;
            let item_id = self.apply_envelope(&event, envelope, group).await?;
            repository::link_event_to_item(&pool, &item_id, &event.event_id, "remote").await?;
            repository::mark_applied(&pool, &event.event_id).await?;
        }
        Ok(())
    }

    async fn ensure_blob(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        manifest: &BlobManifest,
    ) -> Result<PathBuf> {
        validate_blob_manifest(manifest)?;
        let root = crate::core::paths::resources_dir(&self.app)?.join("sync-blobs");
        let path = crypto::blob_path(&root, &manifest.blob_id)?;
        if path.is_file() && path.metadata()?.len() == manifest.encrypted_size {
            return Ok(path);
        }
        download_blob(connection, group, manifest, &path).await?;
        Ok(path)
    }

    async fn apply_envelope(
        &self,
        event: &EncryptedEvent,
        envelope: ClipboardEnvelope,
        group: &GroupSecrets,
    ) -> Result<String> {
        if envelope.version != 1 {
            bail!("不支持的同步事件版本");
        }
        let kind = parse_kind(&envelope.item.kind)?;
        let blob_root = crate::core::paths::resources_dir(&self.app)?.join("sync-blobs");
        let content = match kind {
            ClipboardKind::Text => envelope.item.content.clone(),
            ClipboardKind::Image => {
                let manifest = envelope
                    .blobs
                    .iter()
                    .find(|blob| blob.role == BlobRole::Image)
                    .context("同步图片缺少数据")?;
                let image_name = sanitize_name(&manifest.name);
                let destination = self.app.state::<ImageStore>().origin_path(&image_name);
                let encrypted = crypto::blob_path(&blob_root, &manifest.blob_id)?;
                let key = group.content_key_bytes()?;
                let original_size = manifest.original_size;
                tauri::async_runtime::spawn_blocking(move || {
                    crypto::decrypt_blob(&encrypted, &destination, original_size, &key)
                })
                .await
                .context("image decryption task failed")??;
                image_name
            }
            ClipboardKind::Files => {
                let directory = crate::core::paths::resources_dir(&self.app)?
                    .join("sync-files")
                    .join(&event.event_id);
                let mut paths = Vec::new();
                for manifest in &envelope.blobs {
                    if manifest.role != BlobRole::File {
                        continue;
                    }
                    let destination = directory.join(format!(
                        "{:03}-{}",
                        manifest.file_index.unwrap_or(0),
                        sanitize_name(&manifest.name)
                    ));
                    let encrypted = crypto::blob_path(&blob_root, &manifest.blob_id)?;
                    let key = group.content_key_bytes()?;
                    let size = manifest.original_size;
                    let destination_for_task = destination.clone();
                    let is_directory = manifest.is_directory_archive;
                    tauri::async_runtime::spawn_blocking(move || {
                        if is_directory {
                            let archive_path =
                                destination_for_task.with_extension("ecopaste-dir.zip");
                            crypto::decrypt_blob(&encrypted, &archive_path, size, &key)?;
                            crypto::extract_directory_archive(
                                &archive_path,
                                &destination_for_task,
                            )?;
                            fs::remove_file(archive_path).ok();
                            Ok(())
                        } else {
                            crypto::decrypt_blob(&encrypted, &destination_for_task, size, &key)
                        }
                    })
                    .await
                    .context("file decryption task failed")??;
                    paths.push(destination.to_string_lossy().into_owned());
                }
                paths.join("\n")
            }
        };
        let now = Utc::now();
        let created_at = Utc
            .timestamp_millis_opt(envelope.item.created_at_ms)
            .single()
            .unwrap_or(now);
        let item = ClipboardItem {
            id: uuid::Uuid::new_v4().to_string(),
            kind,
            sub_kind: envelope
                .item
                .sub_kind
                .as_deref()
                .map(parse_sub_kind)
                .transpose()?,
            group_id: None,
            source_app_id: None,
            content_hash: if kind == ClipboardKind::Files {
                db::items::content_hash(kind, &content)
            } else {
                envelope.item.content_hash
            },
            content,
            search_text: envelope.item.search_text,
            summary: envelope.item.summary,
            file_types: envelope.item.file_types,
            size: envelope.item.size,
            width: envelope.item.width,
            height: envelope.item.height,
            use_count: 1,
            is_favorite: false,
            is_pinned: false,
            is_sensitive: envelope.item.is_sensitive,
            platform: parse_platform(&envelope.item.source_platform),
            note: None,
            created_at,
            updated_at: now,
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        };
        let pool = self.pool().await;
        let result = db::items::upsert_item(&pool, &item).await?;
        self.app.emit(crate::clipboard::CLIPBOARD_UPDATED_EVENT, serde_json::json!({"id": result.id, "kind": item.kind, "deduplicated": result.deduplicated}))?;
        if self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .sync
            .auto_write_clipboard
        {
            let guard = self.app.state::<Arc<WritebackGuard>>();
            #[cfg(target_os = "android")]
            if item.kind == ClipboardKind::Text {
                crate::clipboard::write_to_clipboard_app(&self.app, &guard, &item)?;
            }
            #[cfg(not(target_os = "android"))]
            crate::clipboard::write_to_clipboard(
                &self.app.state::<ImageStore>(),
                &guard,
                &item,
                false,
            )?;
        }
        Ok(result.id)
    }

    fn announcement(&self, endpoint: &Endpoint) -> PeerAnnouncement {
        let device = self.device_announcement(endpoint);
        PeerAnnouncement {
            device_id: device.device_id,
            device_name: device.device_name,
            platform: device.platform,
            endpoint_id: device.endpoint_id,
            direct_addresses: device.direct_addresses,
            relay_urls: device.relay_urls,
            last_seen_ms: Utc::now().timestamp_millis(),
        }
    }

    fn device_announcement(&self, endpoint: &Endpoint) -> DeviceAnnouncement {
        let identity = self.identity.snapshot();
        let address = endpoint.addr();
        DeviceAnnouncement {
            device_id: identity.device_id,
            device_name: identity.device_name,
            platform: std::env::consts::OS.into(),
            endpoint_id: endpoint.id().to_string(),
            direct_addresses: address.ip_addrs().map(ToString::to_string).collect(),
            relay_urls: address.relay_urls().map(ToString::to_string).collect(),
        }
    }

    /// Starts the Iroh endpoint only after sync is enabled or a pairing code is requested.
    async fn ensure_endpoint(self: &Arc<Self>) -> Result<Endpoint> {
        let mut runtime = self.runtime.lock().await;
        if let Some(runtime) = runtime.as_ref() {
            return Ok(runtime.endpoint.clone());
        }
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(self.identity.secret_key()?)
            .bind()
            .await
            .context("failed to bind Iroh sync endpoint")?;
        let mdns = match MdnsAddressLookup::builder()
            .service_name("ecopaste-v1")
            .build(endpoint.id())
        {
            Ok(mdns) => {
                endpoint
                    .address_lookup()
                    .context("failed to access Iroh address lookup")?
                    .add(mdns.clone());
                Some(mdns)
            }
            Err(error) => {
                log::warn!("LAN discovery is unavailable: {error}");
                None
            }
        };
        let router = Router::builder(endpoint.clone())
            .accept(
                ALPN,
                PeerService {
                    manager: Arc::downgrade(self),
                },
            )
            .spawn();
        spawn_endpoint_watchers(Arc::downgrade(self), &endpoint, mdns.clone());
        *runtime = Some(EndpointRuntime {
            endpoint: endpoint.clone(),
            router,
            _mdns: mdns,
        });
        Ok(endpoint)
    }

    /// Releases sockets, relay connections and the protocol router while sync is disabled.
    async fn stop_runtime(&self) -> Result<()> {
        let runtime = self.runtime.lock().await.take();
        if let Some(runtime) = runtime {
            runtime
                .router
                .shutdown()
                .await
                .context("failed to stop Iroh sync endpoint")?;
        }
        #[cfg(target_os = "android")]
        crate::commands::android::set_lan_discovery_enabled(false);
        Ok(())
    }

    async fn pool(&self) -> SqlitePool {
        self.app.state::<db::DatabaseState>().pool().await
    }

    fn set_channel_status(
        &self,
        target: SyncTarget,
        state: SyncChannelState,
        error: Option<&str>,
        succeeded: bool,
    ) {
        let lock = match target {
            SyncTarget::Lan => &self.lan_status,
            SyncTarget::Cloud => &self.cloud_status,
        };
        let mut status = lock.write().expect("sync channel status poisoned");
        if state == SyncChannelState::Disabled {
            *status = SyncChannelStatus::new(state);
            return;
        }
        let now = Utc::now().to_rfc3339();
        status.state = state;
        if matches!(
            state,
            SyncChannelState::Connecting | SyncChannelState::Online | SyncChannelState::Error
        ) {
            status.last_attempt_at = Some(now.clone());
        }
        if succeeded {
            status.last_success_at = Some(now);
        }
        status.last_error = error.map(str::to_owned);
    }

    fn emit_updated(&self) {
        if let Err(error) = self.app.emit(SYNC_UPDATED_EVENT, ()) {
            log::debug!("emit sync update failed: {error}");
        }
        #[cfg(target_os = "android")]
        crate::commands::android::notify_overlay_sync_status_changed();
    }
}

async fn worker(manager: Arc<SyncManager>) {
    let mut failure_count = 0_usize;
    loop {
        let (target, connect_timeout, retry_only) =
            if manager.app.state::<SettingsStore>().snapshot().sync.enabled {
                let delay = if failure_count == 0 {
                    Duration::from_secs(10 * 60)
                } else {
                    retry_delay(failure_count)
                };
                tokio::select! {
                    _ = manager.wake.notified() => {
                        (None, Duration::from_secs(8), false)
                    },
                    _ = tokio::time::sleep(delay) => {
                        (
                            (failure_count > 0).then_some(SyncTarget::Lan),
                            Duration::from_secs(5),
                            failure_count > 0,
                        )
                    },
                }
            } else {
                manager.wake.notified().await;
                (None, Duration::from_secs(8), false)
            };
        match manager
            .run_cycle(target, None, connect_timeout, retry_only)
            .await
        {
            Ok(()) => failure_count = 0,
            Err(error) => {
                failure_count = failure_count.saturating_add(1);
                log::warn!("clipboard sync cycle failed: {error}");
                manager.emit_updated();
            }
        }
    }
}

/// Tries fresh endpoint discovery first, then falls back to cached direct and relay routes.
async fn connect_peer(
    endpoint: &Endpoint,
    cached_address: EndpointAddr,
    total_timeout: Duration,
) -> Result<Connection> {
    let discovery_timeout = if total_timeout >= Duration::from_secs(8) {
        Duration::from_secs(5)
    } else {
        Duration::from_secs(3)
    };
    let discovered_address = EndpointAddr::new(cached_address.id);
    let discovery_result = tokio::time::timeout(
        discovery_timeout,
        endpoint.connect(discovered_address, ALPN),
    )
    .await;
    if let Ok(Ok(connection)) = discovery_result {
        return Ok(connection);
    }
    if cached_address.is_empty() {
        return match discovery_result {
            Ok(Err(error)) => Err(error.into()),
            Err(_) => bail!("连接超时"),
            Ok(Ok(_)) => unreachable!(),
        };
    }

    let fallback_timeout = total_timeout
        .saturating_sub(discovery_timeout)
        .max(Duration::from_secs(1));
    match tokio::time::timeout(fallback_timeout, endpoint.connect(cached_address, ALPN)).await {
        Ok(Ok(connection)) => Ok(connection),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => bail!("连接超时"),
    }
}

/// Wakes synchronization when our route changes or a paired endpoint is rediscovered by mDNS.
fn spawn_endpoint_watchers(
    manager: Weak<SyncManager>,
    endpoint: &Endpoint,
    mdns: Option<MdnsAddressLookup>,
) {
    let mut address_stream = endpoint.watch_addr().stream();
    let address_closed = endpoint.closed();
    let address_manager = manager.clone();
    tauri::async_runtime::spawn(address_closed.run_until(async move {
        let mut initial = true;
        while address_stream.next().await.is_some() {
            if initial {
                initial = false;
                continue;
            }
            let Some(manager) = address_manager.upgrade() else {
                break;
            };
            manager.wake();
        }
    }));

    let Some(mdns) = mdns else {
        return;
    };
    let mdns_closed = endpoint.closed();
    tauri::async_runtime::spawn(mdns_closed.run_until(async move {
        let mut events = mdns.subscribe().await;
        while let Some(event) = events.next().await {
            let Some(manager) = manager.upgrade() else {
                break;
            };
            let DiscoveryEvent::Discovered { endpoint_info, .. } = event else {
                continue;
            };
            let endpoint_id = endpoint_info.endpoint_id.to_string();
            let address: EndpointAddr = endpoint_info.into();
            let direct_addresses = address
                .ip_addrs()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let relay_urls = address
                .relay_urls()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let pool = manager.pool().await;
            match repository::update_peer_routes(
                &pool,
                &endpoint_id,
                &direct_addresses,
                &relay_urls,
            )
            .await
            {
                Ok(true) => manager.wake(),
                Ok(false) => {}
                Err(error) => log::debug!("refresh discovered peer route failed: {error}"),
            }
        }
    }));
}

async fn cloud_watch_worker(manager: Arc<SyncManager>) {
    let mut failure_count = 0_usize;
    loop {
        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        let Some(group) = manager.identity.snapshot().group else {
            manager.watch_wake.notified().await;
            continue;
        };
        if !settings.enabled || !settings.cloud_enabled {
            manager.watch_wake.notified().await;
            continue;
        }
        let server = match server_endpoint_addr(&settings).await {
            Ok(Some(server)) => server,
            Ok(None) => {
                manager.watch_wake.notified().await;
                continue;
            }
            Err(error) => {
                manager.set_channel_status(
                    SyncTarget::Cloud,
                    SyncChannelState::Error,
                    Some(&error.to_string()),
                    false,
                );
                manager.emit_updated();
                tokio::time::sleep(retry_delay(failure_count.saturating_add(1))).await;
                continue;
            }
        };
        let result = async {
            let endpoint = manager.ensure_endpoint().await?;
            manager.set_channel_status(
                SyncTarget::Cloud,
                SyncChannelState::Connecting,
                None,
                false,
            );
            manager.emit_updated();
            let connection =
                tokio::time::timeout(Duration::from_secs(8), endpoint.connect(server, ALPN))
                    .await
                    .context("云端状态连接超时")??;
            manager.ensure_cloud_group(&connection, &group).await?;
            manager.set_channel_status(SyncTarget::Cloud, SyncChannelState::Online, None, true);
            manager.emit_updated();
            let cursor = repository::cloud_cursor(&manager.pool().await).await?;
            match call(
                &connection,
                Request::Watch {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    after_cursor: cursor,
                },
            )
            .await?
            {
                Response::Changed { latest_cursor } => {
                    if latest_cursor > cursor {
                        manager.wake.notify_one();
                    }
                    Ok(())
                }
                Response::Error { message, .. } => bail!(message),
                _ => bail!("云端返回了无效的订阅响应"),
            }
        }
        .await;
        match result {
            Ok(()) => failure_count = 0,
            Err(error) => {
                failure_count = failure_count.saturating_add(1);
                manager.set_channel_status(
                    SyncTarget::Cloud,
                    SyncChannelState::Error,
                    Some(&error.to_string()),
                    false,
                );
                manager.emit_updated();
                tokio::select! {
                    _ = manager.watch_wake.notified() => {},
                    _ = tokio::time::sleep(retry_delay(failure_count)) => {},
                }
            }
        }
    }
}

fn retry_delay(failure_count: usize) -> Duration {
    const RETRY_SECONDS: [u64; 6] = [2, 5, 15, 30, 60, 300];
    let index = failure_count.saturating_sub(1).min(RETRY_SECONDS.len() - 1);
    let base = RETRY_SECONDS[index];
    let jitter = (Utc::now().timestamp_subsec_millis() as u64) % (base / 4 + 1);
    Duration::from_secs(base + jitter)
}

#[derive(Clone)]
struct PeerService {
    manager: Weak<SyncManager>,
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
        loop {
            let (send, recv) = match connection.accept_bi().await {
                Ok(value) => value,
                Err(_) => break,
            };
            let manager = manager.clone();
            let connection = connection.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_peer_stream(manager, connection, send, recv).await {
                    log::warn!("LAN sync request failed: {error}");
                }
            });
        }
        Ok(())
    }
}

async fn handle_peer_stream(
    manager: Arc<SyncManager>,
    connection: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let request: Request = read_frame(&mut recv).await?;
    let result = dispatch_peer(&manager, &connection, &mut send, &mut recv, request).await;
    if let Err(error) = result {
        write_frame(
            &mut send,
            &Response::Error {
                code: ErrorCode::InvalidRequest,
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
    match request {
        Request::Health => {
            write_frame(
                send,
                &Response::Health {
                    protocol_version: 1,
                    server_time_ms: Utc::now().timestamp_millis(),
                },
            )
            .await?
        }
        Request::Sync {
            group_id,
            access_token,
            device,
            after_cursor,
            events,
            limit,
        } => {
            let group = authenticate_local(manager, &group_id, &access_token)?;
            if events.len() > usize::from(MAX_EVENTS_PER_BATCH) {
                bail!("too many sync events");
            }
            let pool = manager.pool().await;
            repository::upsert_peer(
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
            repository::mark_peer_online(
                &pool,
                &device.device_id,
                connected_address.as_deref(),
                transport,
            )
            .await?;
            manager.clear_peer_retry(&device.device_id);
            manager.set_channel_status(SyncTarget::Lan, SyncChannelState::Online, None, true);
            manager.emit_updated();
            let incoming_event_ids = events
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<Vec<_>>();
            let mut accepted = Vec::new();
            for event in events {
                if repository::insert_event(&pool, &event, false, &[]).await? {
                    accepted.push(event.event_id.clone());
                }
            }
            repository::mark_delivered(
                &pool,
                &format!("peer:{}", device.device_id),
                &incoming_event_ids,
            )
            .await?;
            let outgoing = repository::events_after_cursor(
                &pool,
                after_cursor,
                limit.clamp(1, MAX_EVENTS_PER_BATCH),
            )
            .await?;
            let latest_cursor = outgoing
                .last()
                .map(|value| value.cursor)
                .unwrap_or(after_cursor);
            let cloud_events = outgoing
                .into_iter()
                .map(|value| CloudEvent {
                    cursor: value.cursor,
                    event: value.event,
                })
                .collect();
            write_frame(
                send,
                &Response::Synced {
                    accepted_event_ids: accepted,
                    events: cloud_events,
                    peers: Vec::new(),
                    latest_cursor,
                },
            )
            .await?;
            let _apply_guard = manager.apply_lock.lock().await;
            let pending_apply = repository::unapplied_events(&pool, MAX_EVENTS_PER_BATCH).await?;
            for stored in pending_apply {
                let envelope =
                    match crypto::decrypt_event(&group.content_key_bytes()?, &stored.event)
                        .and_then(|envelope| {
                            validate_envelope(&envelope)?;
                            Ok(envelope)
                        }) {
                        Ok(envelope) => envelope,
                        Err(error) => {
                            log::warn!(
                                "discard invalid LAN sync event {}: {error}",
                                stored.event.event_id
                            );
                            repository::mark_applied(&pool, &stored.event.event_id).await?;
                            continue;
                        }
                    };
                let root = crate::core::paths::resources_dir(&manager.app)?.join("sync-blobs");
                let mut blobs = Vec::with_capacity(envelope.blobs.len());
                let mut all_blobs_available = true;
                for blob in &envelope.blobs {
                    let path = crypto::blob_path(&root, &blob.blob_id)?;
                    if !path.is_file() || path.metadata()?.len() != blob.encrypted_size {
                        all_blobs_available = false;
                        break;
                    }
                    blobs.push(StoredBlob {
                        blob_id: blob.blob_id.clone(),
                        encrypted_path: path.to_string_lossy().into_owned(),
                        size: blob.encrypted_size,
                    });
                }
                if all_blobs_available {
                    repository::attach_event_blobs(&pool, &stored.event.event_id, &blobs).await?;
                    if let Ok(item_id) = manager
                        .apply_envelope(&stored.event, envelope, &group)
                        .await
                    {
                        repository::link_event_to_item(
                            &pool,
                            &item_id,
                            &stored.event.event_id,
                            "remote",
                        )
                        .await
                        .ok();
                        repository::mark_applied(&pool, &stored.event.event_id)
                            .await
                            .ok();
                    }
                }
            }
        }
        Request::PutBlob {
            group_id,
            access_token,
            blob_id,
            size,
        } => {
            authenticate_local(manager, &group_id, &access_token)?;
            if size == 0 || size > MAX_SYNC_BLOB_BYTES {
                bail!("invalid blob");
            }
            let root = crate::core::paths::resources_dir(&manager.app)?.join("sync-blobs");
            let path = crypto::blob_path(&root, &blob_id)?;
            if path.is_file() && path.metadata()?.len() == size {
                write_frame(send, &Response::BlobStored).await?;
            } else {
                write_frame(send, &Response::BlobReady { size: 0 }).await?;
                receive_blob(recv, &path, &blob_id, size).await?;
                write_frame(send, &Response::BlobStored).await?;
            }
        }
        Request::GetBlob {
            group_id,
            access_token,
            blob_id,
        } => {
            authenticate_local(manager, &group_id, &access_token)?;
            let path = crypto::blob_path(
                &crate::core::paths::resources_dir(&manager.app)?.join("sync-blobs"),
                &blob_id,
            )?;
            let size = path.metadata()?.len();
            write_frame(send, &Response::BlobReady { size }).await?;
            let mut file = tokio::fs::File::open(path).await?;
            tokio::io::copy(&mut file, send).await?;
        }
        Request::CreateGroup { .. } | Request::Watch { .. } | Request::ListEvents { .. } => {
            bail!("LAN peer does not support this cloud operation")
        }
    }
    Ok(())
}

fn authenticate_local(
    manager: &SyncManager,
    group_id: &str,
    access_token: &[u8],
) -> Result<GroupSecrets> {
    let group = manager
        .identity
        .snapshot()
        .group
        .context("device is not paired")?;
    if group.group_id != group_id || group.access_token_bytes()? != access_token {
        bail!("unauthorized");
    }
    Ok(group)
}

async fn call(connection: &Connection, request: Request) -> Result<Response> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv).await.context("read sync response")
}

async fn upload_blob(
    connection: &Connection,
    group: &GroupSecrets,
    blob: &StoredBlob,
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::PutBlob {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            blob_id: blob.blob_id.clone(),
            size: blob.size,
        },
    )
    .await?;
    match read_frame::<_, Response>(&mut recv).await? {
        Response::BlobStored => {
            send.finish()?;
            return Ok(());
        }
        Response::BlobReady { .. } => {}
        Response::Error { message, .. } => bail!(message),
        _ => bail!("invalid blob upload response"),
    }
    let mut file = tokio::fs::File::open(&blob.encrypted_path).await?;
    tokio::io::copy(&mut file, &mut send).await?;
    send.finish()?;
    match read_frame(&mut recv).await? {
        Response::BlobStored => Ok(()),
        Response::Error { message, .. } => bail!(message),
        _ => bail!("invalid blob stored response"),
    }
}

async fn download_blob(
    connection: &Connection,
    group: &GroupSecrets,
    manifest: &BlobManifest,
    destination: &Path,
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::GetBlob {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            blob_id: manifest.blob_id.clone(),
        },
    )
    .await?;
    send.finish()?;
    let Response::BlobReady { size } = read_frame(&mut recv).await? else {
        bail!("blob is unavailable");
    };
    if size != manifest.encrypted_size {
        bail!("encrypted blob size mismatch");
    }
    receive_blob(&mut recv, destination, &manifest.blob_id, size).await
}

async fn receive_blob(
    recv: &mut RecvStream,
    destination: &Path,
    expected_hash: &str,
    size: u64,
) -> Result<()> {
    let parent = destination.parent().context("blob path has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = destination.with_extension(format!("part-{}", uuid::Uuid::new_v4()));
    let result = async {
        let mut file = tokio::fs::File::create(&temporary).await?;
        let mut remaining = size;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        while remaining > 0 {
            let requested =
                usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let read = recv
                .read(&mut buffer[..requested])
                .await?
                .context("blob ended early")?;
            file.write_all(&buffer[..read]).await?;
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        file.flush().await?;
        drop(file);
        if hasher.finalize().to_hex().as_str() != expected_hash {
            bail!("blob hash mismatch");
        }
        tokio::fs::rename(&temporary, destination).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        tokio::fs::remove_file(&temporary).await.ok();
    }
    result
}

fn connection_path(connection: &Connection) -> (Option<String>, Option<&'static str>) {
    let paths = connection.paths();
    let path = paths
        .iter()
        .find(|path| path.is_selected())
        .or_else(|| paths.iter().next());
    let Some(path) = path else {
        return (None, Some("unknown"));
    };
    let transport = if path.is_ip() {
        "direct"
    } else if path.is_relay() {
        "relay"
    } else {
        "unknown"
    };
    (Some(path.remote_addr().to_string()), Some(transport))
}

fn validate_envelope(envelope: &ClipboardEnvelope) -> Result<()> {
    if envelope.version != 1 {
        bail!("unsupported sync event version");
    }
    let kind = parse_kind(&envelope.item.kind)?;
    envelope
        .item
        .sub_kind
        .as_deref()
        .map(parse_sub_kind)
        .transpose()?;
    for manifest in &envelope.blobs {
        validate_blob_manifest(manifest)?;
    }
    match kind {
        ClipboardKind::Text => {
            if !envelope.blobs.is_empty() {
                bail!("sync text contains unexpected blobs");
            }
        }
        ClipboardKind::Image => {
            if envelope.blobs.len() != 1
                || envelope.blobs[0].role != BlobRole::Image
                || envelope.blobs[0].file_index.is_some()
                || envelope.blobs[0].is_directory_archive
            {
                bail!("sync image has an invalid blob manifest");
            }
        }
        ClipboardKind::Files => {
            if envelope.blobs.is_empty() || envelope.blobs.len() > 1024 {
                bail!("sync files are missing their blobs");
            }
            let mut indexes = HashSet::with_capacity(envelope.blobs.len());
            for blob in &envelope.blobs {
                let Some(index) = blob.file_index else {
                    bail!("sync file is missing its index");
                };
                if blob.role != BlobRole::File || !indexes.insert(index) {
                    bail!("sync file has an invalid blob manifest");
                }
            }
        }
    }
    Ok(())
}

fn cloud_record(
    cursor: u64,
    event: EncryptedEvent,
    envelope: ClipboardEnvelope,
    device_name: String,
) -> CloudRecord {
    let is_sensitive = envelope.item.is_sensitive;
    let file_count = envelope.blobs.len().try_into().unwrap_or(u32::MAX);
    let total_size = envelope.blobs.iter().map(|blob| blob.original_size).sum();
    let preview = match envelope.item.kind.as_str() {
        "files" => envelope
            .blobs
            .iter()
            .map(|blob| blob.name.as_str())
            .take(3)
            .collect::<Vec<_>>()
            .join("、"),
        "image" => envelope
            .item
            .summary
            .clone()
            .unwrap_or_else(|| "图片".to_owned()),
        _ => envelope
            .item
            .summary
            .clone()
            .or(envelope.item.search_text.clone())
            .unwrap_or_else(|| envelope.item.content.clone()),
    };
    CloudRecord {
        cursor,
        event_id: event.event_id,
        device_name,
        kind: envelope.item.kind,
        preview: if is_sensitive {
            String::new()
        } else {
            truncate_preview(&preview)
        },
        file_count,
        total_size,
        created_at: Utc
            .timestamp_millis_opt(event.created_at_ms)
            .single()
            .unwrap_or_else(Utc::now)
            .to_rfc3339(),
        is_sensitive,
    }
}

fn truncate_preview(value: &str) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = compact.chars();
    let preview = chars.by_ref().take(160).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}…")
    } else {
        preview
    }
}

fn short_device_id(device_id: &str) -> String {
    let short = device_id.chars().take(8).collect::<String>();
    format!("设备 {short}")
}

fn validate_blob_manifest(manifest: &BlobManifest) -> Result<()> {
    crypto::blob_path(Path::new("."), &manifest.blob_id)?;
    if manifest.encrypted_size == 0 || manifest.encrypted_size > MAX_SYNC_BLOB_BYTES {
        bail!("invalid encrypted blob size");
    }
    if manifest.original_size > MAX_SYNC_BLOB_BYTES {
        bail!("invalid original blob size");
    }
    Ok(())
}

fn safe_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(sanitize_name)
        .unwrap_or_else(|| "file".into())
}
fn sanitize_name(value: &str) -> String {
    let name = value.replace(['/', '\\', '\0'], "_");
    if name.is_empty() || name == "." || name == ".." {
        "file".into()
    } else {
        name.chars().take(180).collect()
    }
}
fn kind_string(value: ClipboardKind) -> &'static str {
    match value {
        ClipboardKind::Text => "text",
        ClipboardKind::Image => "image",
        ClipboardKind::Files => "files",
    }
}
fn parse_kind(value: &str) -> Result<ClipboardKind> {
    match value {
        "text" => Ok(ClipboardKind::Text),
        "image" => Ok(ClipboardKind::Image),
        "files" => Ok(ClipboardKind::Files),
        _ => bail!("invalid clipboard kind"),
    }
}
fn sub_kind_string(value: ClipboardSubKind) -> &'static str {
    match value {
        ClipboardSubKind::Rtf => "rtf",
        ClipboardSubKind::Html => "html",
        ClipboardSubKind::Url => "url",
        ClipboardSubKind::Email => "email",
        ClipboardSubKind::Color => "color",
        ClipboardSubKind::Path => "path",
    }
}
fn parse_sub_kind(value: &str) -> Result<ClipboardSubKind> {
    match value {
        "rtf" => Ok(ClipboardSubKind::Rtf),
        "html" => Ok(ClipboardSubKind::Html),
        "url" => Ok(ClipboardSubKind::Url),
        "email" => Ok(ClipboardSubKind::Email),
        "color" => Ok(ClipboardSubKind::Color),
        "path" => Ok(ClipboardSubKind::Path),
        _ => bail!("invalid clipboard sub kind"),
    }
}
fn platform_string(value: Platform) -> &'static str {
    match value {
        Platform::Macos => "macos",
        Platform::Windows => "windows",
        Platform::Android => "android",
    }
}
fn parse_platform(value: &str) -> Platform {
    match value {
        "windows" => Platform::Windows,
        "android" => Platform::Android,
        _ => Platform::Macos,
    }
}
