use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock, Weak,
    },
    time::{Duration, Instant},
};

#[cfg(any(not(target_os = "android"), test))]
use std::net::IpAddr;

use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use ecopaste_sync_protocol::{
    read_frame, write_frame, CloudEvent, DeviceAnnouncement, EncryptedEvent, ErrorCode,
    PeerAnnouncement, RemovedDevice, Request, Response, ALPN, ASYNC_SOURCE_ICON_PROTOCOL_VERSION,
    MAX_EVENTS_PER_BATCH, MAX_FRAME_BYTES, PROTOCOL_VERSION, REDUNDANT_WATCH_PROTOCOL_VERSION,
    SYNC_V2_PROTOCOL_VERSION,
};
use iroh::{
    address_lookup::{
        AddrFilter, AddressLookup, AddressLookupBuilder, AddressLookupBuilderError,
        Error as AddressLookupError, Item as AddressLookupItem, PkarrResolver, UserData,
    },
    endpoint::{presets, ConnectOptions, Connection, QuicTransportConfig, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, EndpointAddr, EndpointId, RelayMap, RelayMode, RelayUrl, TransportAddr, Watcher,
};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::{boxed::BoxStream, StreamExt};
use rand::RngCore;
use sqlx::SqlitePool;
use tauri::{AppHandle, Emitter, Manager};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot, Mutex, Notify, OwnedSemaphorePermit, RwLock as AsyncRwLock, Semaphore},
};
use uuid::Uuid;

use crate::{
    clipboard::{
        calculate_files_total_size, fallback_accent_colors, AppIconStore, ClipboardFingerprint,
        ClipboardFingerprintState, ClipboardObservation, FileEntryFingerprint, ImageStore,
        WritebackGuard,
    },
    db::{
        self,
        models::{ClipboardApp, ClipboardItem, ClipboardKind, ClipboardSubKind, Platform},
    },
    settings::{CloudRelayMode, SettingsStore, SyncSettings},
};

use super::{
    crypto,
    identity::{
        is_lan_ip, peer_endpoint_addr, peer_endpoint_addr_with_preferred, server_endpoint_addr,
        server_is_configured, GroupSecrets, IdentityStore, PairingCode,
    },
    model::{
        BlobManifest, BlobRole, ClipboardEnvelope, CloudRecord, CloudRecordPage,
        IncomingJoinRequest, LinkedSyncEvent, NearbyJoinAttempt, NearbyJoinState, NearbySyncDevice,
        NearbySyncSpace, SourceAppRef, SourceIconRef, StoredBlob, StoredSyncEvent,
        SyncChannelState, SyncChannelStatus, SyncItemStatus, SyncPairingPreview, SyncPeer,
        SyncStatus, SyncTarget, SyncedClipboardItem,
    },
    pairing::{
        comparison_code, discovery_space_id, DiscoveryMetadata, JoinAcknowledgement,
        JoinCompletion, JoinRequest, JoinResponse, JOIN_ALPN, JOIN_PROTOCOL_VERSION,
        JOIN_TIMEOUT_SECS,
    },
    repository,
};

const CLOUD_TARGET: &str = "cloud";
const SYNC_UPDATED_EVENT: &str = "sync://updated";
const JOIN_REQUESTED_EVENT: &str = "sync://join-requested";
const JOIN_ATTEMPT_UPDATED_EVENT: &str = "sync://join-attempt-updated";
const NEARBY_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(8);
const JOIN_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(8);
const LAN_EVENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const LAN_PATH_SELECTION_TIMEOUT: Duration = Duration::from_secs(2);
const LAN_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLOUD_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const CLOUD_SYNC_HEDGE_DELAY: Duration = Duration::from_millis(300);
const CLOUD_RECORD_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CLOUD_WATCH_RESPONSE_TIMEOUT: Duration = Duration::from_secs(90);
const PRIMARY_CLOUD_WATCH_SLOT: u8 = 0;
const BACKUP_CLOUD_WATCH_SLOT: u8 = 1;
const HISTORY_BACKFILL_LIMIT: u16 = 100;
const MAX_SYNC_BLOB_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_SYNC_BATCHES_PER_CONNECTION: usize = 8;
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const BLOB_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_CONCURRENT_BLOB_TRANSFERS: usize = 2;

/// 自动文件同步只看整张卡片的原始总大小；0 明确表示全部手动。
fn permits_automatic_file_upload(total_size: u64, limit_mb: u32) -> bool {
    limit_mb > 0 && total_size <= u64::from(limit_mb) * 1024 * 1024
}

const MAX_SOURCE_ICON_UPLOADS_PER_BATCH: u16 = 64;
const CLOUD_PATH_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);
const RETRY_SECONDS: [u64; 6] = [2, 5, 15, 30, 60, 300];

pub struct SyncManager {
    app: AppHandle,
    identity: Arc<IdentityStore>,
    runtime: Mutex<Option<EndpointRuntime>>,
    lan_transfer_wake: Notify,
    cloud_transfer_wake: Notify,
    watch_wake: Notify,
    source_asset_wake: Notify,
    nearby_wake: Notify,
    lan_cycle_lock: AsyncRwLock<()>,
    cloud_cycle_lock: Mutex<()>,
    cloud_connect_lock: Mutex<()>,
    cloud_connection_ready: Notify,
    cloud_session_lock: Mutex<()>,
    cloud_force_reconnect: AtomicBool,
    maintenance_lock: Mutex<()>,
    apply_lock: Mutex<()>,
    inbound_stream_admission: Arc<Semaphore>,
    inbound_blob_concurrency: Arc<Semaphore>,
    lan_status: RwLock<SyncChannelStatus>,
    cloud_status: RwLock<SyncChannelStatus>,
    cloud_watch_status: RwLock<SyncChannelStatus>,
    cloud_path: RwLock<ConnectionRoute>,
    cloud_server_version: RwLock<Option<String>>,
    cloud_connection: RwLock<Option<Connection>>,
    cloud_session: RwLock<Option<CloudSessionState>>,
    confirmed_cloud_source_icons: RwLock<ConfirmedCloudSourceIcons>,
    lan_connections: RwLock<HashMap<String, Connection>>,
    lan_peer_actors: RwLock<HashMap<String, Arc<LanPeerActor>>>,
    lan_peer_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    lan_suspended_peers: RwLock<HashSet<String>>,
    #[cfg(target_os = "android")]
    android_lan_multicast_interfaces: RwLock<BTreeSet<Ipv4Addr>>,
    lan_presence_nonce: AtomicU64,
    nearby_devices: RwLock<HashMap<String, NearbyDeviceEntry>>,
    incoming_join_requests: Mutex<HashMap<String, PendingIncomingJoinRequest>>,
    outgoing_join_attempts: RwLock<HashMap<String, NearbyJoinAttempt>>,
    join_rate_limits: RwLock<HashMap<String, Instant>>,
    pairing_sessions: AtomicUsize,
}

#[derive(Clone)]
struct NearbyDeviceEntry {
    metadata: DiscoveryMetadata,
    address: EndpointAddr,
    last_seen_at: chrono::DateTime<Utc>,
}

enum PendingMdnsEvent {
    Discovered {
        endpoint_id: String,
        metadata: Option<DiscoveryMetadata>,
        address: EndpointAddr,
    },
    Expired {
        endpoint_id: String,
    },
}

struct PendingIncomingJoinRequest {
    public: IncomingJoinRequest,
    responder: oneshot::Sender<JoinResponse>,
}

struct EndpointRuntime {
    endpoint: Endpoint,
    router: Router,
    config: EndpointRuntimeConfig,
    #[cfg(target_os = "android")]
    mdns: Option<MdnsAddressLookup>,
}

#[derive(Clone, Debug)]
struct CloudSessionState {
    connection_id: usize,
    group_id: String,
    protocol_version: u16,
    sync_v2_supported: bool,
}

/// Coalesces optional source icons already acknowledged or queued on the current Hub connection.
#[derive(Default)]
struct ConfirmedCloudSourceIcons {
    connection_id: Option<usize>,
    group_id: Option<String>,
    blob_ids: HashSet<String>,
}

impl ConfirmedCloudSourceIcons {
    /// Drops confirmations whenever either the connection or encrypted sync space changes.
    fn reset_for(&mut self, connection_id: usize, group_id: &str) {
        if self.connection_id == Some(connection_id) && self.group_id.as_deref() == Some(group_id) {
            return;
        }

        self.connection_id = Some(connection_id);
        self.group_id = Some(group_id.to_owned());
        self.blob_ids.clear();
    }

    /// Returns only icons that still require a remote existence check or upload.
    fn retain_unconfirmed(
        &mut self,
        connection_id: usize,
        group_id: &str,
        blobs: Vec<StoredBlob>,
    ) -> (Vec<StoredBlob>, usize) {
        self.reset_for(connection_id, group_id);
        let original_count = blobs.len();
        let unconfirmed = blobs
            .into_iter()
            .filter(|blob| !self.blob_ids.contains(&blob.blob_id))
            .collect::<Vec<_>>();
        let cache_hits = original_count.saturating_sub(unconfirmed.len());
        (unconfirmed, cache_hits)
    }

    fn confirm(&mut self, connection_id: usize, group_id: &str, blob_ids: &[String]) {
        self.reset_for(connection_id, group_id);
        self.blob_ids.extend(blob_ids.iter().cloned());
    }

    fn clear_connection(&mut self, connection_id: usize) {
        if self.connection_id != Some(connection_id) {
            return;
        }

        *self = Self::default();
    }
}

struct LanPeerActor {
    wake: Notify,
    pending_work: AtomicBool,
    force_connect: AtomicBool,
    connection_ready: AtomicBool,
    stopped: AtomicBool,
}

impl LanPeerActor {
    fn new() -> Self {
        Self {
            wake: Notify::new(),
            pending_work: AtomicBool::new(false),
            force_connect: AtomicBool::new(false),
            connection_ready: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }
    }

    fn notify(&self) {
        self.pending_work.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    fn resume(&self) {
        self.force_connect.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    fn connection_ready(&self) {
        self.force_connect.store(false, Ordering::Release);
        self.connection_ready.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}

enum LanPeerSyncOutcome {
    Succeeded,
    ConnectionFailed(String),
    TransferFailed(String),
}

struct SyncBatchResponse {
    events: Vec<CloudEvent>,
    peers: Vec<PeerAnnouncement>,
    latest_cursor: u64,
    removed_devices: Vec<RemovedDevice>,
}

#[derive(Debug)]
struct CloudAttemptTimeout;

impl std::fmt::Display for CloudAttemptTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("cloud attempt timed out")
    }
}

impl std::error::Error for CloudAttemptTimeout {}

#[derive(Debug)]
struct LanAttemptTimeout;

impl std::fmt::Display for LanAttemptTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LAN attempt timed out")
    }
}

impl std::error::Error for LanAttemptTimeout {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EndpointRuntimeConfig {
    lan_enabled: bool,
    cloud_enabled: bool,
    cloud_relay_mode: CloudRelayMode,
    server_endpoint_id: String,
    server_direct_addresses: Vec<String>,
    server_relay_urls: Vec<String>,
    relay_token_hash: Option<String>,
}

#[derive(Clone, Default)]
struct ConnectionRoute {
    address: Option<String>,
    transport: Option<String>,
}

#[derive(Debug)]
struct CloudPkarrResolverBuilder {
    target: EndpointId,
}

impl AddressLookupBuilder for CloudPkarrResolverBuilder {
    fn into_address_lookup(
        self,
        endpoint: &Endpoint,
    ) -> std::result::Result<impl AddressLookup, AddressLookupBuilderError> {
        let inner = AddressLookupBuilder::into_address_lookup(PkarrResolver::n0_dns(), endpoint)?;
        Ok(CloudPkarrResolver {
            target: self.target,
            inner,
        })
    }
}

#[derive(Debug)]
struct CloudPkarrResolver<T> {
    target: EndpointId,
    inner: T,
}

impl<T: AddressLookup> AddressLookup for CloudPkarrResolver<T> {
    fn resolve(
        &self,
        endpoint_id: EndpointId,
    ) -> Option<BoxStream<std::result::Result<AddressLookupItem, AddressLookupError>>> {
        (endpoint_id == self.target)
            .then(|| self.inner.resolve(endpoint_id))
            .flatten()
    }
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
    let mut presence_nonce = [0_u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut presence_nonce);
    let manager = Arc::new(SyncManager {
        app: app.clone(),
        identity,
        runtime: Mutex::new(None),
        lan_transfer_wake: Notify::new(),
        cloud_transfer_wake: Notify::new(),
        watch_wake: Notify::new(),
        source_asset_wake: Notify::new(),
        nearby_wake: Notify::new(),
        lan_cycle_lock: AsyncRwLock::new(()),
        cloud_cycle_lock: Mutex::new(()),
        cloud_connect_lock: Mutex::new(()),
        cloud_connection_ready: Notify::new(),
        cloud_session_lock: Mutex::new(()),
        cloud_force_reconnect: AtomicBool::new(false),
        maintenance_lock: Mutex::new(()),
        apply_lock: Mutex::new(()),
        inbound_stream_admission: Arc::new(Semaphore::new(128)),
        inbound_blob_concurrency: Arc::new(Semaphore::new(4)),
        lan_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Idle)),
        cloud_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Disabled)),
        cloud_watch_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Disabled)),
        cloud_path: RwLock::new(ConnectionRoute::default()),
        cloud_server_version: RwLock::new(None),
        cloud_connection: RwLock::new(None),
        cloud_session: RwLock::new(None),
        confirmed_cloud_source_icons: RwLock::new(ConfirmedCloudSourceIcons::default()),
        lan_connections: RwLock::new(HashMap::new()),
        lan_peer_actors: RwLock::new(HashMap::new()),
        lan_peer_locks: RwLock::new(HashMap::new()),
        lan_suspended_peers: RwLock::new(HashSet::new()),
        #[cfg(target_os = "android")]
        android_lan_multicast_interfaces: RwLock::new(BTreeSet::new()),
        lan_presence_nonce: AtomicU64::new(u64::from_le_bytes(presence_nonce)),
        nearby_devices: RwLock::new(HashMap::new()),
        incoming_join_requests: Mutex::new(HashMap::new()),
        outgoing_join_attempts: RwLock::new(HashMap::new()),
        join_rate_limits: RwLock::new(HashMap::new()),
        pairing_sessions: AtomicUsize::new(0),
    });
    app.manage(manager.clone());
    tauri::async_runtime::spawn(lan_transfer_worker(manager.clone()));
    tauri::async_runtime::spawn(cloud_transfer_worker(manager.clone()));
    tauri::async_runtime::spawn(cloud_watch_worker(manager.clone()));
    tauri::async_runtime::spawn(source_asset_worker(manager.clone()));
    manager.wake();
    Ok(())
}

impl SyncManager {
    pub fn wake(&self) {
        self.cloud_force_reconnect.store(true, Ordering::Release);
        self.wake_transfer();
        self.watch_wake.notify_one();
        self.source_asset_wake.notify_one();
    }

    /// Reconnects LAN peers after an endpoint address or mDNS presence change.
    fn notify_lan_connectivity_changed(&self) {
        self.resume_all_peers();
        self.wake_lan_transfer();
    }

    /// Bypasses cloud backoff and restarts both transfer and persistent watch workers.
    #[cfg(target_os = "android")]
    fn notify_cloud_network_changed(&self) {
        self.cloud_force_reconnect.store(true, Ordering::Release);
        self.wake_cloud_transfer();
        self.watch_wake.notify_one();
    }

    /// Preserves the existing desktop connection unless Iroh has already marked it unavailable.
    #[cfg(not(target_os = "android"))]
    fn notify_cloud_connectivity_changed(&self) {
        self.cloud_force_reconnect.store(true, Ordering::Release);
        self.wake_cloud_transfer();
        let watch_online = self
            .cloud_watch_status
            .read()
            .expect("cloud watch status poisoned")
            .state
            == SyncChannelState::Online;
        if !watch_online || self.cached_cloud_connection().is_none() {
            self.watch_wake.notify_one();
        }
    }

    /// Migrates cloud synchronization immediately when Android changes its default network.
    #[cfg(target_os = "android")]
    pub fn notify_default_network_changed(self: &Arc<Self>) {
        self.handle_default_network_event("changed");
    }

    /// Stops reusing the old cloud path when Android loses its current default network.
    #[cfg(target_os = "android")]
    pub fn notify_default_network_lost(self: &Arc<Self>) {
        self.handle_default_network_event("lost");
    }

    #[cfg(target_os = "android")]
    fn handle_default_network_event(self: &Arc<Self>, event: &'static str) {
        let manager = Arc::clone(self);
        tauri::async_runtime::spawn(async move {
            let endpoint = manager
                .runtime
                .lock()
                .await
                .as_ref()
                .map(|runtime| runtime.endpoint.clone());
            let endpoint_refreshed = endpoint.is_some();
            if let Some(endpoint) = endpoint {
                endpoint.network_change().await;
            }
            let invalidated_connection = manager.invalidate_current_cloud_connection();
            manager.notify_cloud_network_changed();
            log::info!(
                "Android default network {event}: endpointRefreshed={} invalidatedCloudConnection={}",
                endpoint_refreshed,
                invalidated_connection
                    .map(|connection_id| connection_id.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
            );
        });
    }

    /// Refreshes only LAN discovery and peer routes when Android Wi-Fi appears or changes.
    #[cfg(target_os = "android")]
    pub fn notify_lan_network_changed(self: &Arc<Self>, interface_addresses: &str) {
        let interfaces = parse_android_lan_multicast_interfaces(interface_addresses);
        if interfaces.is_empty() {
            log::warn!("ignore Android LAN network change without IPv4 interfaces");
            return;
        }
        *self
            .android_lan_multicast_interfaces
            .write()
            .expect("Android LAN multicast interfaces poisoned") = interfaces;
        let manager = Arc::clone(self);
        tauri::async_runtime::spawn(async move {
            let runtime = manager
                .runtime
                .lock()
                .await
                .as_ref()
                .map(|runtime| (runtime.endpoint.clone(), runtime.mdns.clone()));
            let mut interface_count = 0;
            if let Some((endpoint, mdns)) = runtime {
                endpoint.network_change().await;
                if let Some(mdns) = mdns {
                    interface_count = manager
                        .configure_mdns_multicast(&mdns, &endpoint.addr())
                        .await;
                }
                let settings = manager.app.state::<SettingsStore>().snapshot().sync;
                if settings.enabled && settings.lan_enabled {
                    manager.advance_presence_nonce();
                    if let Err(error) = manager.update_discovery_metadata(&endpoint, true) {
                        log::debug!("publish changed Android LAN route failed: {error}");
                    }
                }
            }
            manager.notify_lan_connectivity_changed();
            log::info!(
                "Android LAN network changed: multicastInterfaces={interface_count} cloudConnectionPreserved=true"
            );
        });
    }

    /// Invalidates LAN connectivity and removes stale mDNS interfaces after Android Wi-Fi loss.
    #[cfg(target_os = "android")]
    pub fn notify_lan_network_lost(self: &Arc<Self>) {
        let manager = Arc::clone(self);
        tauri::async_runtime::spawn(async move {
            manager
                .android_lan_multicast_interfaces
                .write()
                .expect("Android LAN multicast interfaces poisoned")
                .clear();
            let runtime = manager
                .runtime
                .lock()
                .await
                .as_ref()
                .map(|runtime| (runtime.endpoint.clone(), runtime.mdns.clone()));
            if let Some((endpoint, mdns)) = runtime {
                endpoint.network_change().await;
                if let Some(mdns) = mdns {
                    manager
                        .configure_mdns_multicast(&mdns, &endpoint.addr())
                        .await;
                }
            }

            let connections = manager
                .lan_connections
                .write()
                .expect("LAN connection cache poisoned")
                .drain()
                .map(|(_, connection)| connection)
                .collect::<Vec<_>>();
            for connection in &connections {
                connection.close(0_u32.into(), b"Android Wi-Fi route lost");
            }

            manager
                .nearby_devices
                .write()
                .expect("nearby devices poisoned")
                .clear();
            let pool = manager.pool().await;
            let own_device_id = manager.identity.snapshot().device_id;
            match repository::list_peers(&pool).await {
                Ok(peers) => {
                    for peer in peers {
                        let device_id = peer.announcement.device_id;
                        if device_id == own_device_id {
                            continue;
                        }
                        manager.suspend_peer(&device_id);
                        if let Err(error) =
                            repository::mark_peer_offline(&pool, &device_id, None).await
                        {
                            log::debug!(
                                "mark peer offline after Android Wi-Fi loss failed: {error}"
                            );
                        }
                    }
                }
                Err(error) => log::debug!("load peers after Android Wi-Fi loss failed: {error}"),
            }
            manager.set_channel_status(SyncTarget::Lan, SyncChannelState::Idle, None, false);
            manager.emit_updated();
            log::info!(
                "Android LAN network lost: multicastInterfaces=0 closedLanConnections={} cloudConnectionPreserved=true",
                connections.len(),
            );
        });
    }

    fn active_lan_multicast_interfaces_v4(&self, address: &EndpointAddr) -> BTreeSet<Ipv4Addr> {
        #[cfg(target_os = "android")]
        {
            let _ = address;
            return self
                .android_lan_multicast_interfaces
                .read()
                .expect("Android LAN multicast interfaces poisoned")
                .clone();
        }
        #[cfg(not(target_os = "android"))]
        {
            lan_multicast_interfaces_v4(address)
        }
    }

    async fn configure_mdns_multicast(
        &self,
        mdns: &MdnsAddressLookup,
        address: &EndpointAddr,
    ) -> usize {
        let interfaces = self.active_lan_multicast_interfaces_v4(address);
        let interface_count = interfaces.len();
        if interfaces.is_empty() {
            mdns.set_multicast_enabled(false).await;
            mdns.set_multicast_interfaces_v4(interfaces).await;
        } else {
            mdns.set_multicast_interfaces_v4(interfaces).await;
            mdns.set_multicast_enabled(true).await;
        }
        interface_count
    }

    /// Makes visible Android surfaces reread committed status without changing network state.
    #[cfg(target_os = "android")]
    pub fn notify_status_refresh(&self) {
        self.emit_updated();
    }

    #[cfg(target_os = "android")]
    fn advance_presence_nonce(&self) {
        self.lan_presence_nonce.fetch_add(1, Ordering::AcqRel);
    }

    fn wake_transfer(&self) {
        self.lan_transfer_wake.notify_one();
        self.cloud_transfer_wake.notify_one();
    }

    fn wake_lan_transfer(&self) {
        self.lan_transfer_wake.notify_one();
    }

    fn wake_cloud_transfer(&self) {
        self.cloud_transfer_wake.notify_one();
    }

    fn notify_cloud_connected(&self) {
        self.cloud_force_reconnect.store(true, Ordering::Release);
        self.wake_cloud_transfer();
    }

    /// Android 原生 Context 晚于 Rust setup 就绪，完成注入后刷新自动设备名。
    #[cfg(target_os = "android")]
    pub fn refresh_system_device_name(&self, name: String) -> Result<()> {
        if self.identity.refresh_system_device_name(name)? {
            self.wake_transfer();
            self.emit_updated();
        }
        Ok(())
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
        let _cloud_guard = self.cloud_cycle_lock.lock().await;
        let _lan_guard = self.lan_cycle_lock.write().await;
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
        repository::restore_and_upsert_peer(&pool, &code.inviter).await?;
        Ok(())
    }

    pub async fn remove_peer(self: &Arc<Self>, device_id: &str) -> Result<()> {
        let device_id = device_id.trim();
        if device_id.is_empty() || device_id == self.identity.snapshot().device_id {
            bail!("无法删除该设备");
        }
        let _cloud_guard = self.cloud_cycle_lock.lock().await;
        let _lan_guard = self.lan_cycle_lock.write().await;
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
        let group = self.identity.snapshot().group;
        if settings.enabled && settings.lan_enabled {
            if let (Some(peer), Some(group)) = (peer, group) {
                let result = async {
                    let endpoint = self.ensure_endpoint(&settings).await?;
                    let address = peer_endpoint_addr(&peer.announcement)?;
                    let connection =
                        connect_peer(&endpoint, address, Duration::from_secs(8)).await?;
                    self.sync_removed_devices(&connection, &group, false).await
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
        let _cloud_guard = self.cloud_cycle_lock.lock().await;
        let _lan_guard = self.lan_cycle_lock.write().await;
        self.stop_runtime().await?;
        self.identity.set_group(None)?;
        let pool = self.pool().await;
        repository::clear_group_state(&pool).await?;
        self.lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .clear();
        self.wake();
        Ok(())
    }

    pub fn set_device_name(&self, name: String) -> Result<()> {
        self.identity.update_device_name(name)?;
        if let Ok(runtime) = self.runtime.try_lock() {
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

    async fn build_pairing_code(self: &Arc<Self>) -> Result<PairingCode> {
        let identity = self.identity.snapshot();
        let group = identity.group.context("请先启用同步或连接已有设备")?;
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let endpoint = self.ensure_endpoint(&settings).await?;
        Ok(PairingCode {
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
            cloud_relay_mode: if settings.cloud_enabled {
                settings.cloud_relay_mode
            } else {
                CloudRelayMode::Off
            },
        })
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
                let discovered = self.nearby_wake.notified();
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
    ) -> Result<NearbyJoinAttempt> {
        let entry = self
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
            pairing_code: None,
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
                    call_join(&connection, request),
                )
                .await
                .context("加入申请已超时")?
            }
            .await;
            manager.finish_outgoing_join(&request_id, response);
            manager.pairing_sessions.fetch_sub(1, Ordering::AcqRel);
            let settings = manager.app.state::<SettingsStore>().snapshot().sync;
            if !settings.enabled {
                if let Err(error) = manager.stop_runtime().await {
                    log::debug!("stop temporary pairing endpoint failed: {error}");
                }
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

        let pairing_code = match async { self.build_pairing_code().await?.encode() }.await {
            Ok(pairing_code) => pairing_code,
            Err(error) => {
                let _ = pending.responder.send(JoinResponse::Error {
                    message: error.to_string(),
                });
                return Err(error);
            }
        };
        pending
            .responder
            .send(JoinResponse::Approved { pairing_code })
            .map_err(|_| anyhow::anyhow!("申请设备已断开"))?;
        Ok(())
    }

    pub async fn status(&self) -> Result<SyncStatus> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let identity = self.identity.snapshot();
        let pool = self.pool().await;
        let (pending_events, pending_manual_items, peer_count) =
            repository::status_counts(&pool).await?;
        let mut peers = repository::list_peer_statuses(&pool).await?;
        let mut lan = self
            .lan_status
            .read()
            .expect("LAN sync status poisoned")
            .clone();
        let mut cloud_transfer = self
            .cloud_status
            .read()
            .expect("cloud sync status poisoned")
            .clone();
        let mut cloud_watch = self
            .cloud_watch_status
            .read()
            .expect("cloud watch status poisoned")
            .clone();
        for peer in &mut peers {
            peer.relay_urls.clear();
            if peer.transport.as_deref() == Some("relay") {
                peer.state = SyncChannelState::Idle;
                peer.connected_address = None;
                peer.transport = None;
            }
            if let Some(connection) = self.cached_lan_connection(&peer.device_id) {
                let (connected_address, transport) = connection_path(&connection);
                peer.state = SyncChannelState::Online;
                peer.connected_address = connected_address;
                peer.transport = transport.map(str::to_owned);
            } else if peer.state == SyncChannelState::Online {
                peer.state = SyncChannelState::Idle;
                peer.connected_address = None;
                peer.transport = None;
            }
        }
        if !settings.enabled || identity.group.is_none() {
            lan = SyncChannelStatus::new(SyncChannelState::Disabled);
            cloud_transfer = SyncChannelStatus::new(SyncChannelState::Disabled);
            cloud_watch = SyncChannelStatus::new(SyncChannelState::Disabled);
            for peer in &mut peers {
                peer.state = SyncChannelState::Disabled;
            }
        } else {
            if !settings.lan_enabled {
                lan = SyncChannelStatus::new(SyncChannelState::Disabled);
                for peer in &mut peers {
                    peer.state = SyncChannelState::Disabled;
                }
            } else if peers
                .iter()
                .any(|peer| peer.state == SyncChannelState::Online)
            {
                lan.state = SyncChannelState::Online;
                lan.last_error = None;
            } else if lan.state == SyncChannelState::Online {
                lan.state = SyncChannelState::Idle;
                lan.last_error = None;
            }
            if !server_is_configured(&settings) {
                cloud_transfer = SyncChannelStatus::new(SyncChannelState::Disabled);
                cloud_watch = SyncChannelStatus::new(SyncChannelState::Disabled);
            }
        }
        for peer in &mut peers {
            if peer.state != SyncChannelState::Error {
                peer.last_error = None;
            }
        }
        peers.sort_by_key(|peer| peer.state != SyncChannelState::Online);
        let cloud = merge_cloud_status(&cloud_transfer, &cloud_watch);
        let cloud_path = self
            .cloud_path
            .read()
            .expect("cloud connection route poisoned")
            .clone();
        Ok(SyncStatus {
            enabled: settings.enabled,
            lan_enabled: settings.lan_enabled,
            cloud_enabled: settings.cloud_enabled,
            cloud_relay_mode: settings.cloud_relay_mode,
            cloud_relay_auth_configured: identity.cloud_relay_auth_token.is_some(),
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
            cloud_watch,
            cloud_connected_address: cloud_path.address,
            cloud_transport: cloud_path.transport,
            cloud_server_version: self
                .cloud_server_version
                .read()
                .expect("cloud server version poisoned")
                .clone(),
            peers,
        })
    }

    pub async fn run_now(self: &Arc<Self>) -> Result<()> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let cloud_enabled = server_is_configured(&settings);
        if !settings.lan_enabled && !cloud_enabled {
            return Ok(());
        }
        #[cfg(target_os = "android")]
        if settings.lan_enabled {
            self.refresh_android_lan_routes().await;
        }
        if cloud_enabled {
            self.request_cloud_connection(Duration::from_secs(8))
                .await?;
        }
        match (cloud_enabled, settings.lan_enabled) {
            (true, true) => {
                let (cloud, lan) = tokio::join!(
                    self.run_cycle(Some(SyncTarget::Cloud), None, Duration::from_secs(8)),
                    self.run_cycle(Some(SyncTarget::Lan), None, Duration::from_secs(8)),
                );
                cloud.and(lan)
            }
            (true, false) => {
                self.run_cycle(Some(SyncTarget::Cloud), None, Duration::from_secs(8))
                    .await
            }
            (false, true) => {
                self.run_cycle(Some(SyncTarget::Lan), None, Duration::from_secs(8))
                    .await
            }
            (false, false) => Ok(()),
        }
    }

    /// Immediately reconnects all paired devices or one selected device.
    pub async fn reconnect_peer(self: &Arc<Self>, device_id: Option<String>) -> Result<()> {
        if !self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .sync
            .lan_enabled
        {
            bail!("局域网同步已关闭");
        }
        if let Some(device_id) = device_id.as_deref() {
            self.resume_peer(device_id);
        } else {
            self.resume_all_peers();
        }
        #[cfg(target_os = "android")]
        self.refresh_android_lan_routes().await;
        self.run_cycle(
            Some(SyncTarget::Lan),
            device_id.as_deref(),
            Duration::from_secs(8),
        )
        .await
    }

    /// Bypasses cloud backoff and starts fresh transfer and watch attempts.
    pub fn reconnect_cloud(&self) -> Result<()> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled
            || self.identity.snapshot().group.is_none()
            || !server_is_configured(&settings)
        {
            bail!("云端同步未配置");
        }

        if let Some(connection) = self.cached_cloud_connection() {
            self.invalidate_cloud_connection(connection.stable_id());
        }
        self.cloud_force_reconnect.store(true, Ordering::Release);
        self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Connecting, None, false);
        self.set_cloud_watch_status(SyncChannelState::Connecting, None, false);
        self.emit_updated();
        self.wake_cloud_transfer();
        self.watch_wake.notify_one();
        Ok(())
    }

    #[cfg(target_os = "android")]
    async fn refresh_android_lan_routes(&self) {
        let _guard = AndroidLanDiscoveryGuard::acquire();
        let _ = tokio::time::timeout(Duration::from_secs(2), self.nearby_wake.notified()).await;
    }

    pub async fn enqueue_item(&self, item: ClipboardItem, force_files: bool) -> Result<()> {
        self.enqueue_item_inner(item, force_files, true, false, None)
            .await?;
        Ok(())
    }

    /// Enqueues a clipboard event whose file fingerprint may finish asynchronously.
    pub async fn enqueue_observed_item(
        &self,
        item: ClipboardItem,
        observation: ClipboardObservation,
    ) -> Result<()> {
        self.enqueue_item_inner(item, false, true, false, Some(observation))
            .await?;
        Ok(())
    }

    pub async fn sync_item_now(
        self: &Arc<Self>,
        item: ClipboardItem,
        target: SyncTarget,
    ) -> Result<SyncItemStatus> {
        if target == SyncTarget::Lan
            && !self
                .app
                .state::<SettingsStore>()
                .snapshot()
                .sync
                .lan_enabled
        {
            bail!("局域网同步已关闭");
        }
        self.enqueue_item_inner(item.clone(), true, false, false, None)
            .await?
            .context("此记录未满足当前同步策略")?;
        if target == SyncTarget::Cloud {
            self.request_cloud_connection(Duration::from_secs(8))
                .await?;
        }
        self.run_cycle(Some(target), None, Duration::from_secs(8))
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
        let connection = self
            .request_cloud_connection(Duration::from_secs(8))
            .await?;
        let request = Request::ListEvents {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            before_cursor,
            limit: limit.clamp(1, MAX_EVENTS_PER_BATCH),
        };
        let (connection, response) = self
            .request_cloud_records_page(connection, &group, &request)
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
        let mut records = Vec::new();
        for cloud_event in events {
            let event = cloud_event.event;
            let envelope = match crypto::decrypt_event(&key, &event).and_then(|envelope| {
                validate_envelope(&envelope)?;
                Ok(envelope)
            }) {
                Ok(envelope) => envelope,
                Err(error) => {
                    log::warn!("skip invalid cloud record {}: {error}", event.event_id);
                    continue;
                }
            };
            let device_name = device_names
                .get(&event.origin_device_id)
                .cloned()
                .unwrap_or_else(|| short_device_id(&event.origin_device_id));
            let image_path = if envelope.item.kind == "image" {
                match self
                    .cloud_record_image_path(&connection, &group, &envelope)
                    .await
                {
                    Ok(path) => path,
                    Err(error) => {
                        log::warn!("load cloud record image {} failed: {error}", event.event_id);
                        None
                    }
                }
            } else {
                None
            };
            records.push(cloud_record(
                cloud_event.cursor,
                event,
                envelope,
                device_name,
                image_path,
            ));
        }
        Ok(CloudRecordPage {
            records,
            next_before_cursor,
            total,
        })
    }

    /// Retries one idempotent history read after evicting a stale shared cloud connection.
    async fn request_cloud_records_page(
        self: &Arc<Self>,
        mut connection: Connection,
        group: &GroupSecrets,
        request: &Request,
    ) -> Result<(Connection, Response)> {
        for attempt in 0..2 {
            let result: Result<Response> = async {
                self.ensure_cloud_group(&connection, group).await?;
                tokio::time::timeout(
                    CLOUD_RECORD_REQUEST_TIMEOUT,
                    call(&connection, request.clone()),
                )
                .await
                .context("读取云端记录超时")?
            }
            .await;
            match result {
                Ok(response) => return Ok((connection, response)),
                Err(error) if attempt == 0 => {
                    self.invalidate_cloud_connection(connection.stable_id());
                    log::debug!("retry cloud records on a fresh connection: {error}");
                    connection = self
                        .request_cloud_connection(Duration::from_secs(8))
                        .await?;
                }
                Err(error) => {
                    self.invalidate_cloud_connection(connection.stable_id());
                    self.watch_wake.notify_one();
                    return Err(error);
                }
            }
        }

        unreachable!("cloud records retry loop always returns")
    }

    async fn enqueue_item_inner(
        &self,
        item: ClipboardItem,
        force_files: bool,
        should_wake: bool,
        history_backfill: bool,
        observation: Option<ClipboardObservation>,
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
        if let Some(stored) = repository::latest_local_event_for_item(&pool, &item.id).await? {
            if event_matches_item_timestamp(&stored.event, group.content_key_bytes()?, &item) {
                repository::clear_pending_item(&pool, &item.id).await?;
                if should_wake {
                    self.wake_transfer();
                }
                return Ok(Some(stored.event.event_id));
            }
        } else if history_backfill {
            if let Some(event_id) = repository::event_for_item(&pool, &item.id).await? {
                repository::clear_pending_item(&pool, &item.id).await?;
                return Ok(Some(event_id));
            }
        }
        let event_id = uuid::Uuid::new_v4().simple().to_string();
        let (envelope, blobs, fingerprint) = match self
            .build_envelope(&event_id, &item, &settings, &group, force_files)
            .await?
        {
            Some(value) => value,
            None => {
                repository::mark_pending_item(&pool, &item.id, "文件卡片总大小超过自动同步阈值")
                    .await?;
                self.emit_updated();
                return Ok(None);
            }
        };
        if let (Some(observation), Some(fingerprint)) = (observation.as_ref(), fingerprint) {
            self.app
                .state::<Arc<ClipboardFingerprintState>>()
                .commit_observation(observation, fingerprint);
        }
        if item.kind == ClipboardKind::Files {
            if let Some(size) = envelope.item.size {
                if item.size != Some(size)
                    && db::items::update_file_item_size(
                        &pool,
                        &item.id,
                        &item.source_revision,
                        size,
                    )
                    .await?
                {
                    if let Err(error) = self.app.emit(
                        crate::clipboard::CLIPBOARD_UPDATED_EVENT,
                        serde_json::json!({
                            "id": item.id,
                            "kind": item.kind,
                            "deduplicated": true,
                        }),
                    ) {
                        log::warn!("emit clipboard file size update failed: {error}");
                    }
                    #[cfg(target_os = "android")]
                    crate::commands::android::notify_overlay_clipboard_changed();
                }
            }
        }
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
        let stored = repository::insert_event(&pool, &event, true, &blobs).await?;
        if !stored.stored {
            bail!("同步事件序号冲突，请重新复制内容");
        }
        repository::link_event_to_item(&pool, &item.id, &event.event_id, "local").await?;
        repository::clear_pending_item(&pool, &item.id).await?;
        if should_wake {
            self.wake_transfer();
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
    ) -> Result<
        Option<(
            ClipboardEnvelope,
            Vec<StoredBlob>,
            Option<ClipboardFingerprint>,
        )>,
    > {
        let key = group.content_key_bytes()?;
        let blob_root = crate::core::paths::resources_dir(&self.app)?.join("sync-blobs");
        let mut manifests = Vec::new();
        let mut stored_blobs = Vec::new();
        let mut envelope_size = item.size;
        let mut clipboard_fingerprint = None;
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
                if !force_files {
                    if settings.auto_upload_max_mb == 0 {
                        return Ok(None);
                    }
                    let content = item.content.clone();
                    let total_size = tauri::async_runtime::spawn_blocking(move || {
                        calculate_files_total_size(&content)
                    })
                    .await
                    .context("file size verification task failed")?;
                    let total_size = match total_size {
                        Ok(size) => u64::try_from(size).context("invalid clipboard file size")?,
                        Err(error) => {
                            log::debug!(
                                "defer automatic file sync because size is unavailable: {error:#}"
                            );
                            return Ok(None);
                        }
                    };
                    if !permits_automatic_file_upload(total_size, settings.auto_upload_max_mb) {
                        return Ok(None);
                    }
                }
                let mut packed_total_size = 0_u64;
                let mut file_fingerprints = Vec::with_capacity(paths.len());
                for (index, source) in paths.into_iter().enumerate() {
                    let name = safe_file_name(&source);
                    let source_for_task = source.clone();
                    let root_for_task = blob_root.clone();
                    let key_for_task = key;
                    let is_directory = source.is_dir();
                    let (blob, original_size, content_size, content_hash) =
                        tauri::async_runtime::spawn_blocking(move || {
                            if is_directory {
                                let temporary = tempfile::tempdir()?;
                                let archive_path = temporary.path().join("directory.zip");
                                let archived =
                                    crypto::archive_directory(&source_for_task, &archive_path)?;
                                let archive_size = archive_path.metadata()?.len();
                                let blob = crypto::encrypt_blob(
                                    &archive_path,
                                    &root_for_task,
                                    &key_for_task,
                                )?;
                                Ok::<_, anyhow::Error>((
                                    blob,
                                    archive_size,
                                    archived.content_size,
                                    archived.fingerprint,
                                ))
                            } else {
                                let size = source_for_task.metadata()?.len();
                                let encrypted = crypto::encrypt_blob_with_fingerprint(
                                    &source_for_task,
                                    &root_for_task,
                                    &key_for_task,
                                )?;
                                Ok((encrypted.blob, size, size, encrypted.plaintext_hash))
                            }
                        })
                        .await
                        .context("file encryption task failed")??;
                    packed_total_size = packed_total_size
                        .checked_add(content_size)
                        .context("clipboard file size overflow")?;
                    file_fingerprints.push(FileEntryFingerprint {
                        name: name.clone(),
                        is_directory,
                        content_hash,
                    });
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
                if !force_files
                    && !permits_automatic_file_upload(
                        packed_total_size,
                        settings.auto_upload_max_mb,
                    )
                {
                    return Ok(None);
                }
                envelope_size = Some(
                    i64::try_from(packed_total_size)
                        .context("clipboard file size exceeds supported range")?,
                );
                clipboard_fingerprint = ClipboardFingerprint::from_file_entries(&file_fingerprints);
            }
        }
        let source_app = self
            .build_source_app_ref(item, group, &blob_root, &mut stored_blobs)
            .await?;
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
                    size: envelope_size,
                    width: item.width,
                    height: item.height,
                    is_sensitive: item.is_sensitive,
                    source_platform: platform_string(item.platform).into(),
                    created_at_ms: item.created_at.timestamp_millis(),
                    content_hash: item.content_hash.clone(),
                    updated_at_ms: Some(item.updated_at.timestamp_millis()),
                    source_revision: Some(item.source_revision.clone()),
                },
                blobs: manifests,
                source_app,
            },
            stored_blobs,
            clipboard_fingerprint,
        )))
    }

    /// Builds a privacy-preserving source reference and reuses the existing blob transport.
    async fn build_source_app_ref(
        &self,
        item: &ClipboardItem,
        group: &GroupSecrets,
        blob_root: &Path,
        stored_blobs: &mut Vec<StoredBlob>,
    ) -> Result<Option<SourceAppRef>> {
        let Some(app_id) = item.source_app_id.as_deref() else {
            return Ok(None);
        };
        let pool = self.pool().await;
        let Some(mut app) = db::apps::find_app_by_id(&pool, app_id).await? else {
            return Ok(None);
        };
        let key = group.content_key_bytes()?;
        let source_key = source_app_key(&key, app.platform, &app.id);
        let mut icon_ref = None;
        if let Some(icon_file) = app.icon_file.clone() {
            let store = self.app.state::<AppIconStore>();
            match store.refresh_metadata(&icon_file) {
                Ok(metadata) => {
                    app.icon_file = Some(metadata.file_name.clone());
                    app.icon_hash = Some(metadata.icon_hash.clone());
                    app.accent_start = Some(metadata.accent_start.clone());
                    app.accent_end = Some(metadata.accent_end.clone());
                    if let Err(error) = db::apps::upsert_app(&pool, &app).await {
                        log::warn!("persist normalized source app icon metadata failed: {error}");
                    }
                    let source = store.icon_path(&metadata.file_name);
                    let root = blob_root.to_path_buf();
                    let hash = metadata.icon_hash.clone();
                    let encrypted = tauri::async_runtime::spawn_blocking(move || {
                        crypto::encrypt_stable_blob(&source, &root, &key, &hash)
                    })
                    .await;
                    match encrypted {
                        Ok(Ok(blob)) => {
                            icon_ref = Some(SourceIconRef {
                                icon_hash: metadata.icon_hash,
                                blob_id: blob.blob_id.clone(),
                                original_size: metadata.original_size,
                                encrypted_size: blob.size,
                            });
                            stored_blobs.push(blob);
                        }
                        Ok(Err(error)) => {
                            log::warn!("encrypt source app icon failed: {error}");
                        }
                        Err(error) => {
                            log::warn!("source app icon encryption task failed: {error}");
                        }
                    }
                }
                Err(error) => {
                    log::warn!("normalize source app icon {icon_file} failed: {error}");
                }
            }
        }
        let (fallback_start, fallback_end) = fallback_accent_colors(&source_key);
        Ok(Some(SourceAppRef {
            version: 1,
            source_key,
            platform: platform_string(app.platform).to_owned(),
            display_name: sanitize_source_app_name(&app.name),
            icon: icon_ref,
            accent_start: Some(app.accent_start.unwrap_or(fallback_start)),
            accent_end: Some(app.accent_end.unwrap_or(fallback_end)),
        }))
    }

    /// Creates sync events for the latest local history once per sync space.
    async fn backfill_recent_history(&self, pool: &SqlitePool, group: &GroupSecrets) -> Result<()> {
        if repository::history_backfill_completed(pool, &group.group_id).await? {
            return Ok(());
        }

        let item_ids = db::items::recent_item_ids_for_sync(pool, HISTORY_BACKFILL_LIMIT).await?;
        for item_id in item_ids.into_iter().rev() {
            let Some(item) = db::items::find_item_by_id(pool, &item_id).await? else {
                continue;
            };
            if let Err(error) = self
                .enqueue_item_inner(item, false, false, true, None)
                .await
            {
                log::warn!("backfill clipboard item {item_id} failed: {error}");
                repository::mark_pending_item(pool, &item_id, "历史记录无法自动同步，请手动重试")
                    .await?;
            }
        }
        repository::mark_history_backfill_completed(pool, &group.group_id).await?;
        self.emit_updated();
        Ok(())
    }

    /// Repairs records already applied by older clients from retained versioned events.
    async fn repair_history_timestamps(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
    ) -> Result<()> {
        if repository::history_timestamp_repair_completed(pool, &group.group_id).await? {
            return Ok(());
        }

        let linked = repository::linked_item_events(pool).await?;
        let reconciled = self
            .reconcile_linked_timestamps(pool, group, linked)
            .await?;
        repository::mark_history_timestamp_repair_completed(pool, &group.group_id).await?;
        if reconciled > 0 {
            self.emit_clipboard_reconciled();
        }
        Ok(())
    }

    async fn reconcile_item_timestamps(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        item_id: &str,
    ) -> Result<()> {
        let linked = repository::linked_events_for_item(pool, item_id)
            .await?
            .into_iter()
            .map(|stored| LinkedSyncEvent {
                item_id: item_id.to_owned(),
                stored,
            })
            .collect();
        let reconciled = self
            .reconcile_linked_timestamps(pool, group, linked)
            .await?;
        if reconciled > 0 {
            self.emit_clipboard_reconciled();
        }
        Ok(())
    }

    async fn reconcile_linked_timestamps(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        linked: Vec<LinkedSyncEvent>,
    ) -> Result<usize> {
        let key = group.content_key_bytes()?;
        let mut timestamps = HashMap::new();
        for linked_event in linked {
            let Ok(envelope) = crypto::decrypt_event(&key, &linked_event.stored.event) else {
                continue;
            };
            if envelope.item.updated_at_ms.is_none() {
                continue;
            }
            let (created_at, updated_at) =
                synced_item_timestamps(&envelope.item, linked_event.stored.event.created_at_ms);
            timestamps
                .entry(linked_event.item_id)
                .and_modify(
                    |current: &mut (chrono::DateTime<Utc>, chrono::DateTime<Utc>)| {
                        current.0 = current.0.min(created_at);
                        current.1 = current.1.max(updated_at);
                    },
                )
                .or_insert((created_at, updated_at));
        }
        let reconciled = timestamps.len();
        for (item_id, (created_at, updated_at)) in timestamps {
            db::items::set_synced_item_timestamps(pool, &item_id, created_at, updated_at).await?;
        }
        Ok(reconciled)
    }

    /// Notifies the list to reload after sync metadata changes item ordering.
    fn emit_clipboard_reconciled(&self) {
        if let Err(error) = self.app.emit(
            crate::clipboard::CLIPBOARD_UPDATED_EVENT,
            serde_json::json!({ "reconciled": true }),
        ) {
            log::warn!("emit clipboard reconciliation update failed: {error}");
        }
    }

    async fn run_cycle(
        self: &Arc<Self>,
        target: Option<SyncTarget>,
        peer_device_id: Option<&str>,
        lan_connect_timeout: Duration,
    ) -> Result<()> {
        match target {
            Some(SyncTarget::Lan) => {
                let _guard = self.lan_cycle_lock.read().await;
                self.run_cycle_locked(target, peer_device_id, lan_connect_timeout)
                    .await
            }
            Some(SyncTarget::Cloud) => {
                let _guard = self.cloud_cycle_lock.lock().await;
                self.run_cycle_locked(target, peer_device_id, lan_connect_timeout)
                    .await
            }
            None => bail!("sync cycle target is required"),
        }
    }

    async fn run_cycle_locked(
        self: &Arc<Self>,
        target: Option<SyncTarget>,
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
            if target == Some(SyncTarget::Cloud) {
                bail!("云端同步未配置");
            }
            return Ok(());
        }
        let endpoint = self.ensure_endpoint(&settings).await?;
        let pool = self.pool().await;
        {
            let _maintenance_guard = self.maintenance_lock.lock().await;
            self.backfill_recent_history(&pool, &group).await?;
            self.repair_history_timestamps(&pool, &group).await?;
        }
        let mut lan_succeeded = false;
        let mut lan_transfer_error = None;
        let mut lan_connection_error = None;
        let mut lan_attempted = false;
        let mut lan_failed = false;
        if settings.lan_enabled && target != Some(SyncTarget::Cloud) {
            let suspended_peers = self
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
                        pool.clone(),
                        group.clone(),
                        endpoint.clone(),
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
        if target != Some(SyncTarget::Lan) {
            let cloud_cycle_started_at = Instant::now();
            match self.configured_cloud_connection(&settings) {
                Ok(Some(connection)) => {
                    let group_started_at = Instant::now();
                    let result = async {
                        let initialized_group =
                            self.ensure_cloud_group(&connection, &group).await?;
                        let group_elapsed = group_started_at.elapsed();
                        let transfer_started_at = Instant::now();
                        let cursor = repository::cloud_cursor(&pool).await?;
                        let latest = self
                            .sync_connection(&connection, CLOUD_TARGET, cursor, &group, &endpoint)
                            .await?;
                        repository::set_cloud_cursor(&pool, latest).await?;
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
                            self.invalidate_cloud_connection(connection.stable_id());
                            self.watch_wake.notify_one();
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
                            repository::mark_delivery_error(&pool, CLOUD_TARGET, &event_ids, error)
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
        if settings.lan_enabled && target == Some(SyncTarget::Lan) && !lan_succeeded {
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
        if target != Some(SyncTarget::Lan) && server_is_configured(&settings) && !cloud_succeeded {
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

    async fn sync_lan_peer(
        self: Arc<Self>,
        pool: SqlitePool,
        group: GroupSecrets,
        endpoint: Endpoint,
        peer: SyncPeer,
        connect_timeout: Duration,
    ) -> Result<LanPeerSyncOutcome> {
        let device_id = peer.announcement.device_id.clone();
        let peer_lock = self.lan_peer_lock(&device_id);
        let _peer_guard = peer_lock.lock().await;
        let target_id = format!("peer:{device_id}");
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
            .sync_connection(&connection, &target_id, peer.pull_cursor, &group, &endpoint)
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
                self.clear_peer_suspension(&device_id);
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

    fn lan_peer_lock(&self, device_id: &str) -> Arc<Mutex<()>> {
        if let Some(lock) = self
            .lan_peer_locks
            .read()
            .expect("LAN peer locks poisoned")
            .get(device_id)
            .cloned()
        {
            return lock;
        }
        self.lan_peer_locks
            .write()
            .expect("LAN peer locks poisoned")
            .entry(device_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn cached_lan_connection(&self, device_id: &str) -> Option<Connection> {
        let connection = self
            .lan_connections
            .read()
            .expect("LAN connection cache poisoned")
            .get(device_id)
            .cloned();
        connection.filter(|connection| connection.close_reason().is_none())
    }

    fn store_lan_connection(self: &Arc<Self>, device_id: &str, connection: Connection) {
        let stable_id = connection.stable_id();
        let should_watch = {
            let mut connections = self
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
    async fn handle_lan_connection_closed(&self, device_id: &str, stable_id: usize, reason: &str) {
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

    fn invalidate_lan_connection(&self, device_id: &str, stable_id: usize) -> bool {
        let mut connections = self
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

    fn invalidate_lan_connection_by_device(&self, device_id: &str) {
        self.lan_connections
            .write()
            .expect("LAN connection cache poisoned")
            .remove(device_id);
    }

    fn stop_lan_peer_actors(&self) {
        let actors = self
            .lan_peer_actors
            .write()
            .expect("LAN peer actors poisoned")
            .drain()
            .map(|(_, actor)| actor)
            .collect::<Vec<_>>();
        for actor in actors {
            actor.stop();
        }
        self.lan_connections
            .write()
            .expect("LAN connection cache poisoned")
            .clear();
    }

    fn clear_peer_suspension(&self, device_id: &str) {
        self.lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .remove(device_id);
    }

    fn suspend_peer(&self, device_id: &str) {
        self.lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .insert(device_id.to_owned());
    }

    fn resume_peer(&self, device_id: &str) {
        self.clear_peer_suspension(device_id);
        if let Some(actor) = self
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .get(device_id)
        {
            actor.resume();
        }
    }

    /// Reconnects a suspended peer once when mDNS proves that it is reachable again.
    fn resume_peer_from_discovery(&self, device_id: &str) {
        if self.cached_lan_connection(device_id).is_some() {
            return;
        }
        let actor = self
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
    fn peer_connection_ready(&self, device_id: &str) {
        self.clear_peer_suspension(device_id);
        if let Some(actor) = self
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .get(device_id)
        {
            actor.connection_ready();
        }
    }

    fn resume_all_peers(&self) {
        self.lan_suspended_peers
            .write()
            .expect("LAN suspended peers poisoned")
            .clear();
        for actor in self
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .values()
        {
            actor.resume();
        }
    }

    async fn ensure_cloud_group(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
    ) -> Result<bool> {
        if self.cloud_group_is_ready(connection, &group.group_id) {
            return Ok(false);
        }
        let _guard = self.cloud_session_lock.lock().await;
        if self.cloud_group_is_ready(connection, &group.group_id) {
            return Ok(false);
        }

        let create_group = call_cloud(
            connection,
            Request::CreateGroup {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
            },
        );
        let read_metadata = call_cloud(connection, Request::HealthV2);
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
            Err(error) => {
                log::debug!("query cloud Hub metadata failed; using legacy sync: {error}");
                0
            }
        };
        *self
            .cloud_session
            .write()
            .expect("cloud session state poisoned") = Some(CloudSessionState {
            connection_id: connection.stable_id(),
            group_id: group.group_id.clone(),
            protocol_version,
            sync_v2_supported: protocol_version >= SYNC_V2_PROTOCOL_VERSION,
        });
        self.source_asset_wake.notify_one();
        Ok(true)
    }

    fn cloud_group_is_ready(&self, connection: &Connection, group_id: &str) -> bool {
        self.cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .is_some_and(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
    }

    fn cloud_sync_v2_supported(&self, connection: &Connection, group_id: &str) -> bool {
        self.cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.sync_v2_supported)
    }

    fn cloud_async_source_icons_supported(&self, connection: &Connection, group_id: &str) -> bool {
        self.cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.protocol_version >= ASYNC_SOURCE_ICON_PROTOCOL_VERSION)
    }

    fn cloud_redundant_watch_supported(&self, connection: &Connection, group_id: &str) -> bool {
        self.cloud_session
            .read()
            .expect("cloud session state poisoned")
            .as_ref()
            .filter(|session| {
                session.connection_id == connection.stable_id() && session.group_id == group_id
            })
            .is_some_and(|session| session.protocol_version >= REDUNDANT_WATCH_PROTOCOL_VERSION)
    }

    async fn sync_connection(
        self: &Arc<Self>,
        connection: &Connection,
        target_id: &str,
        after_cursor: u64,
        group: &GroupSecrets,
        endpoint: &Endpoint,
    ) -> Result<u64> {
        let pool = self.pool().await;
        let identity = self.identity.snapshot();
        let use_sync_v2 =
            target_id == CLOUD_TARGET && self.cloud_sync_v2_supported(connection, &group.group_id);
        let publish_source_icons_after_event = target_id == CLOUD_TARGET
            && self.cloud_async_source_icons_supported(connection, &group.group_id);
        let removed_devices = if use_sync_v2 {
            repository::removed_devices(&pool).await?
        } else {
            self.sync_removed_devices(connection, group, target_id == CLOUD_TARGET)
                .await?;
            Vec::new()
        };
        let mut cursor = after_cursor;
        for _ in 0..MAX_SYNC_BATCHES_PER_CONNECTION {
            let batch_started_at = Instant::now();
            let pending = if target_id == CLOUD_TARGET {
                repository::pending_origin_events_for_target(
                    &pool,
                    target_id,
                    &identity.device_id,
                    MAX_EVENTS_PER_BATCH,
                )
                .await?
            } else {
                repository::pending_events_for_target(&pool, target_id, MAX_EVENTS_PER_BATCH)
                    .await?
            };
            let pending_count = pending.len();
            let event_ids = pending
                .iter()
                .map(|item| item.event.event_id.clone())
                .collect::<Vec<_>>();
            let source_icon_blob_ids = source_icon_blob_ids(&pending, &group.content_key_bytes()?);
            repository::mark_delivery_syncing(&pool, target_id, &event_ids).await?;
            self.emit_updated();
            let blob_started_at = Instant::now();
            let blobs = match repository::blobs_for_events(&pool, &event_ids).await {
                Ok(blobs) => blobs,
                Err(error) => {
                    repository::mark_delivery_error(
                        &pool,
                        target_id,
                        &event_ids,
                        &error.to_string(),
                    )
                    .await?;
                    self.emit_updated();
                    return Err(error);
                }
            };
            let (required_blobs, source_icon_blobs) =
                split_source_icon_blobs(blobs, &source_icon_blob_ids);
            let required_blob_count = required_blobs.len();
            let source_icon_blob_count = source_icon_blobs.len();
            let (source_icon_blobs, source_icon_cache_hits) = if target_id == CLOUD_TARGET {
                self.confirmed_cloud_source_icons
                    .write()
                    .expect("confirmed cloud source icons poisoned")
                    .retain_unconfirmed(connection.stable_id(), &group.group_id, source_icon_blobs)
            } else {
                (source_icon_blobs, 0)
            };
            let source_icon_blob_checks = source_icon_blobs.len();
            let checked_source_icon_blob_ids = source_icon_blobs
                .iter()
                .map(|blob| blob.blob_id.clone())
                .collect::<Vec<_>>();
            if let Err(error) =
                upload_blobs(connection, group, required_blobs, BlobUploadKind::Content).await
            {
                repository::mark_delivery_error(&pool, target_id, &event_ids, &error.to_string())
                    .await?;
                self.emit_updated();
                return Err(error);
            }
            let blob_elapsed = blob_started_at.elapsed();
            let mut deferred_source_icon_blobs = source_icon_blobs;
            let mut source_icon_elapsed = Duration::ZERO;
            let mut queued_source_icon_count = 0;
            if !publish_source_icons_after_event {
                let source_icon_started_at = Instant::now();
                match upload_blobs(
                    connection,
                    group,
                    std::mem::take(&mut deferred_source_icon_blobs),
                    BlobUploadKind::SourceIcon,
                )
                .await
                {
                    Ok(()) if target_id == CLOUD_TARGET => {
                        self.confirmed_cloud_source_icons
                            .write()
                            .expect("confirmed cloud source icons poisoned")
                            .confirm(
                                connection.stable_id(),
                                &group.group_id,
                                &checked_source_icon_blob_ids,
                            );
                    }
                    Ok(()) => {}
                    Err(error) => log::warn!("publish optional source app icons failed: {error}"),
                }
                source_icon_elapsed = source_icon_started_at.elapsed();
            }
            let outgoing_events = pending
                .iter()
                .map(|item| item.event.clone())
                .collect::<Vec<_>>();
            let request_started_at = Instant::now();
            let result = async {
                let mut protocol = "legacy";
                let response = if use_sync_v2 {
                    let response = call_cloud_hedged(
                        connection,
                        Request::SyncV2 {
                            group_id: group.group_id.clone(),
                            access_token: group.access_token_bytes()?,
                            device: self.device_announcement(endpoint),
                            after_cursor: cursor,
                            events: outgoing_events.clone(),
                            removed_devices: removed_devices.clone(),
                            limit: MAX_EVENTS_PER_BATCH,
                        },
                    )
                    .await?;
                    protocol = "v2";
                    response
                } else {
                    let request = Request::Sync {
                        group_id: group.group_id.clone(),
                        access_token: group.access_token_bytes()?,
                        device: self.device_announcement(endpoint),
                        after_cursor: cursor,
                        events: outgoing_events,
                        limit: MAX_EVENTS_PER_BATCH,
                    };
                    if target_id == CLOUD_TARGET {
                        call_cloud(connection, request).await?
                    } else {
                        call_lan(connection, request).await?
                    }
                };
                let response = match response {
                    Response::Synced {
                        events,
                        peers,
                        latest_cursor,
                        ..
                    } => SyncBatchResponse {
                        events,
                        peers,
                        latest_cursor,
                        removed_devices: Vec::new(),
                    },
                    Response::SyncedV2 {
                        events,
                        peers,
                        latest_cursor,
                        removed_devices,
                        ..
                    } => SyncBatchResponse {
                        events,
                        peers,
                        latest_cursor,
                        removed_devices,
                    },
                    Response::Error { message, .. } => bail!(message),
                    _ => bail!("同步端返回了无效响应"),
                };
                Ok::<_, anyhow::Error>((response, protocol))
            }
            .await;
            let (response, protocol) = match result {
                Ok(response) => response,
                Err(error) => {
                    if !is_cloud_attempt_timeout(&error) {
                        repository::mark_delivery_error(
                            &pool,
                            target_id,
                            &event_ids,
                            &error.to_string(),
                        )
                        .await?;
                        self.emit_updated();
                    }
                    return Err(error);
                }
            };
            let request_elapsed = request_started_at.elapsed();
            let received_count = response.events.len();
            let removed_count = response.removed_devices.len();
            if !response.removed_devices.is_empty() {
                self.apply_removed_devices(&pool, response.removed_devices)
                    .await?;
            }
            if publish_source_icons_after_event && !deferred_source_icon_blobs.is_empty() {
                repository::enqueue_source_icon_uploads(
                    &pool,
                    &group.group_id,
                    &deferred_source_icon_blobs,
                )
                .await?;
            }
            repository::mark_delivered(&pool, target_id, &event_ids).await?;
            for peer in response.peers {
                if peer.device_id != identity.device_id {
                    repository::upsert_peer(&pool, &peer).await?;
                }
            }
            let apply_started_at = Instant::now();
            self.accept_remote_events(connection, response.events, group, target_id)
                .await?;
            let apply_elapsed = apply_started_at.elapsed();
            if publish_source_icons_after_event {
                queued_source_icon_count = deferred_source_icon_blobs.len();
                if queued_source_icon_count > 0 {
                    self.source_asset_wake.notify_one();
                }
            }
            cursor = response.latest_cursor;
            if target_id == CLOUD_TARGET {
                log::info!(
                    "cloud sync batch completed: protocol={protocol} sentEvents={pending_count} receivedEvents={received_count} removals={removed_count} requiredBlobs={required_blob_count} sourceIconBlobs={source_icon_blob_count} sourceIconBlobChecks={source_icon_blob_checks} sourceIconBlobCacheHits={source_icon_cache_hits} eventFirstSourceIcons={publish_source_icons_after_event} queuedSourceIcons={queued_source_icon_count} blobMs={} requestMs={} applyMs={} sourceIconMs={} totalMs={}",
                    blob_elapsed.as_millis(),
                    request_elapsed.as_millis(),
                    apply_elapsed.as_millis(),
                    source_icon_elapsed.as_millis(),
                    batch_started_at.elapsed().as_millis()
                );
            }
            if pending_count < usize::from(MAX_EVENTS_PER_BATCH)
                && received_count < usize::from(MAX_EVENTS_PER_BATCH)
            {
                break;
            }
        }
        self.source_asset_wake.notify_one();
        Ok(cursor)
    }

    /// Exchanges group-wide removal tombstones before normal events so a removed device leaves
    /// before it can upload another clipboard batch.
    async fn sync_removed_devices(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        cloud: bool,
    ) -> Result<()> {
        let pool = self.pool().await;
        let local = repository::removed_devices(&pool).await?;
        let request = Request::SyncRemovedDevices {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            devices: local,
        };
        let result = if cloud {
            call_cloud(connection, request).await
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
        self.apply_removed_devices(&pool, devices).await
    }

    async fn apply_removed_devices(
        &self,
        pool: &SqlitePool,
        devices: Vec<RemovedDevice>,
    ) -> Result<()> {
        let identity = self.identity.snapshot();
        let endpoint_id = self.identity.secret_key()?.public().to_string();
        let removed_self = devices.iter().any(|device| {
            device.is_removed()
                && (device.device_id == identity.device_id || device.endpoint_id == endpoint_id)
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
    async fn leave_after_removal(&self, pool: &SqlitePool) -> Result<()> {
        self.identity.set_group(None)?;
        repository::clear_group_state(pool).await?;
        let settings = self
            .app
            .state::<SettingsStore>()
            .update(serde_json::json!({ "sync": { "enabled": false } }))?;
        crate::commands::emit_settings_updated(&self.app, &settings);
        self.wake();
        self.emit_updated();
        Ok(())
    }

    async fn accept_remote_events(
        &self,
        connection: &Connection,
        events: Vec<CloudEvent>,
        group: &GroupSecrets,
        source_target: &str,
    ) -> Result<()> {
        let pool = self.pool().await;
        let removed_peers = repository::removed_peer_ids(&pool).await?;
        let mut received_event_ids = Vec::new();
        let mut remote_event_ids = Vec::new();
        let mut received_new_events = false;
        for cloud_event in events {
            let event = cloud_event.event;
            if removed_peers.contains(&event.origin_device_id) {
                continue;
            }
            let result = repository::insert_event(
                &pool,
                &event,
                event.origin_device_id == self.identity.snapshot().device_id,
                &[],
            )
            .await?;
            if !result.stored {
                log::warn!(
                    "ignore sync event {} with a reused origin sequence from {}",
                    event.event_id,
                    event.origin_device_id
                );
                continue;
            }
            received_event_ids.push(event.event_id.clone());
            if event.origin_device_id == self.identity.snapshot().device_id {
                continue;
            }
            remote_event_ids.push(event.event_id.clone());
            received_new_events |= result.inserted;
        }
        repository::mark_delivered(&pool, source_target, &received_event_ids).await?;
        if source_target != CLOUD_TARGET {
            repository::mark_delivered(&pool, CLOUD_TARGET, &remote_event_ids).await?;
        }

        // Replaying every still-unapplied event makes a download/apply interruption
        // recoverable even when the hub no longer returns the duplicate event.
        // LAN 入站请求与本机主动拉取可能同时看到同一事件；从查询到标记完成必须串行，
        // 否则同一远端事件会被并发写入剪贴板历史两次。
        let _apply_guard = self.apply_lock.lock().await;
        let pending_apply = repository::unapplied_events(&pool, MAX_EVENTS_PER_BATCH).await?;
        let mut source_assets_pending = false;
        let mut latest_clipboard_item = None;
        #[cfg(target_os = "android")]
        let mut applied_any = false;
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
            let mut all_blobs_available = true;
            for manifest in &envelope.blobs {
                match self.ensure_blob(connection, group, manifest).await {
                    Ok(path) => {
                        stored_blobs.push(StoredBlob {
                            blob_id: manifest.blob_id.clone(),
                            encrypted_path: path.to_string_lossy().into_owned(),
                            size: manifest.encrypted_size,
                        });
                    }
                    Err(error) => {
                        log::debug!(
                            "defer sync event {} until blob {} is available: {error}",
                            event.event_id,
                            manifest.blob_id
                        );
                        all_blobs_available = false;
                        break;
                    }
                }
            }
            if !all_blobs_available {
                continue;
            }
            repository::attach_event_blobs(&pool, &event.event_id, &stored_blobs).await?;
            let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
            let (item_id, item, fingerprint) = self.apply_envelope(&event, envelope, group).await?;
            repository::link_event_to_item(&pool, &item_id, &event.event_id, "remote").await?;
            self.reconcile_item_timestamps(&pool, group, &item_id)
                .await?;
            repository::mark_applied(&pool, &event.event_id).await?;
            latest_clipboard_item = Some((item, fingerprint));
            if let Some(icon) = source_icon {
                match self.source_icon_blob_record(&icon) {
                    Ok(blob) => {
                        if let Err(error) =
                            repository::attach_event_blobs(&pool, &event.event_id, &[blob]).await
                        {
                            log::warn!("attach optional source app icon blob failed: {error}");
                        } else {
                            source_assets_pending = true;
                        }
                    }
                    Err(error) => log::warn!("prepare optional source app icon failed: {error}"),
                }
            }
            #[cfg(target_os = "android")]
            {
                applied_any = true;
            }
        }
        if let Some((item, fingerprint)) = latest_clipboard_item.as_ref() {
            self.write_latest_synced_item_to_clipboard(item, fingerprint.as_ref());
        }
        drop(_apply_guard);
        #[cfg(target_os = "android")]
        if applied_any {
            crate::commands::android::notify_overlay_clipboard_changed();
        }
        if source_assets_pending {
            self.source_asset_wake.notify_one();
        }
        if received_new_events && source_target == CLOUD_TARGET {
            self.wake_lan_transfer();
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

    /// Downloads and decrypts a cloud image into the shared image cache without importing history.
    async fn cloud_record_image_path(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        envelope: &ClipboardEnvelope,
    ) -> Result<Option<String>> {
        let Some(manifest) = envelope
            .blobs
            .iter()
            .find(|blob| blob.role == BlobRole::Image)
        else {
            return Ok(None);
        };
        let encrypted = self.ensure_blob(connection, group, manifest).await?;
        let image_name = sanitize_name(&manifest.name);
        let destination = self.app.state::<ImageStore>().origin_path(&image_name);
        if !destination.is_file() || destination.metadata()?.len() != manifest.original_size {
            let destination_for_task = destination.clone();
            let original_size = manifest.original_size;
            let key = group.content_key_bytes()?;
            tauri::async_runtime::spawn_blocking(move || {
                crypto::decrypt_blob(&encrypted, &destination_for_task, original_size, &key)
            })
            .await
            .context("cloud image decryption task failed")??;
        }

        Ok(Some(destination.to_string_lossy().into_owned()))
    }

    async fn apply_envelope(
        &self,
        event: &EncryptedEvent,
        envelope: ClipboardEnvelope,
        group: &GroupSecrets,
    ) -> Result<(String, ClipboardItem, Option<ClipboardFingerprint>)> {
        if envelope.version != 1 {
            bail!("不支持的同步事件版本");
        }
        let kind = parse_kind(&envelope.item.kind)?;
        let blob_root = crate::core::paths::resources_dir(&self.app)?.join("sync-blobs");
        let (content, files_fingerprint) = match kind {
            ClipboardKind::Text => (envelope.item.content.clone(), None),
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
                (image_name, None)
            }
            ClipboardKind::Files => {
                let directory = crate::core::paths::resources_dir(&self.app)?
                    .join("sync-files")
                    .join(&event.event_id);
                let mut paths = Vec::new();
                let mut file_fingerprints = Vec::new();
                for manifest in &envelope.blobs {
                    if manifest.role != BlobRole::File {
                        continue;
                    }
                    // 不在逻辑文件名前加序号：系统剪贴板及 RustDesk 会继续
                    // 传播落盘后的 basename，改名会让同一内容在往返中产生不同指纹。
                    // 用独立序号目录处理同名根项，同时保留原始 basename。
                    let destination = synced_file_destination(
                        &directory,
                        manifest.file_index.unwrap_or(0),
                        &manifest.name,
                    );
                    let encrypted = crypto::blob_path(&blob_root, &manifest.blob_id)?;
                    let key = group.content_key_bytes()?;
                    let size = manifest.original_size;
                    let destination_for_task = destination.clone();
                    let is_directory = manifest.is_directory_archive;
                    let content_hash = tauri::async_runtime::spawn_blocking(move || {
                        if is_directory {
                            let archive_path =
                                destination_for_task.with_extension("ecopaste-dir.zip");
                            crypto::decrypt_blob(&encrypted, &archive_path, size, &key)?;
                            let fingerprint = crypto::extract_directory_archive(
                                &archive_path,
                                &destination_for_task,
                            )?;
                            fs::remove_file(archive_path).ok();
                            Ok(fingerprint)
                        } else {
                            crypto::decrypt_blob_with_fingerprint(
                                &encrypted,
                                &destination_for_task,
                                size,
                                &key,
                            )
                        }
                    })
                    .await
                    .context("file decryption task failed")??;
                    file_fingerprints.push(FileEntryFingerprint {
                        name: manifest.name.clone(),
                        is_directory,
                        content_hash,
                    });
                    paths.push(destination.to_string_lossy().into_owned());
                }
                (
                    paths.join("\n"),
                    ClipboardFingerprint::from_file_entries(&file_fingerprints),
                )
            }
        };
        let (created_at, updated_at) = synced_item_timestamps(&envelope.item, event.created_at_ms);
        let source_revision = envelope
            .item
            .source_revision
            .clone()
            .unwrap_or_else(|| event.event_id.clone());
        let pool = self.pool().await;
        let source_app_id = if let Some(source_app) = envelope.source_app.as_ref() {
            match self
                .resolve_received_source_app(&pool, group, source_app, updated_at, &source_revision)
                .await
            {
                Ok(app_id) => Some(app_id),
                Err(error) => {
                    log::warn!("ignore invalid synchronized source app: {error}");
                    None
                }
            }
        } else {
            None
        };
        let mut item = ClipboardItem {
            id: uuid::Uuid::new_v4().to_string(),
            kind,
            sub_kind: envelope
                .item
                .sub_kind
                .as_deref()
                .map(parse_sub_kind)
                .transpose()?,
            group_id: None,
            source_app_id,
            source_revision,
            content_hash: if kind == ClipboardKind::Files {
                db::items::content_hash(kind, &content)
            } else {
                envelope.item.content_hash
            },
            content,
            search_text: envelope.item.search_text,
            summary: envelope.item.summary,
            text_char_count: None,
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
            updated_at,
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            source_app_accent_start: None,
            source_app_accent_end: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        };
        let result = db::items::upsert_synced_item(&pool, &item).await?;
        item.id.clone_from(&result.id);
        self.app.emit(crate::clipboard::CLIPBOARD_UPDATED_EVENT, serde_json::json!({"id": result.id, "kind": item.kind, "deduplicated": result.deduplicated}))?;
        let fingerprint = files_fingerprint.or_else(|| ClipboardFingerprint::from_text_item(&item));
        Ok((result.id, item, fingerprint))
    }

    /// A remote batch updates the system clipboard at most once. Clipboard ownership is
    /// independent from transport health, so a busy OS clipboard must never fail synchronization.
    fn write_latest_synced_item_to_clipboard(
        &self,
        item: &ClipboardItem,
        fingerprint: Option<&ClipboardFingerprint>,
    ) {
        if !self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .sync
            .auto_write_clipboard
        {
            return;
        }

        let guard = self.app.state::<Arc<WritebackGuard>>();
        let fingerprints = self.app.state::<Arc<ClipboardFingerprintState>>();
        #[cfg(target_os = "android")]
        let result = if item.kind == ClipboardKind::Text {
            crate::clipboard::write_synced_item_to_clipboard_app(
                &self.app,
                &guard,
                &fingerprints,
                item,
            )
        } else {
            Ok(false)
        };
        #[cfg(not(target_os = "android"))]
        let result = crate::clipboard::write_synced_item_to_clipboard(
            &self.app.state::<ImageStore>(),
            &guard,
            &fingerprints,
            item,
            fingerprint,
        );

        if let Err(error) = result {
            log::warn!(
                "synchronized item {} was persisted but clipboard writeback failed: {error}",
                item.id
            );
        }
    }

    /// Creates or refreshes the local alias before the clipboard row references it.
    async fn resolve_received_source_app(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        source: &SourceAppRef,
        source_updated_at: chrono::DateTime<Utc>,
        source_revision: &str,
    ) -> Result<String> {
        validate_source_app_ref(source)?;
        let icon = source
            .icon
            .as_ref()
            .filter(|icon| validate_source_icon_ref(icon).is_ok());
        let icon_file = icon.and_then(|icon| {
            self.app
                .state::<AppIconStore>()
                .icon_file_for_hash(&icon.icon_hash)
        });
        let icon_hash = icon_file
            .as_ref()
            .and_then(|_| icon.map(|icon| icon.icon_hash.clone()));
        let now = Utc::now();
        let app = ClipboardApp {
            id: String::new(),
            name: sanitize_source_app_name(&source.display_name),
            icon_file,
            icon_hash,
            accent_start: source.accent_start.clone(),
            accent_end: source.accent_end.clone(),
            platform: parse_platform(&source.platform),
            created_at: now,
            updated_at: now,
        };
        let icon = icon.map(|icon| {
            (
                icon.icon_hash.as_str(),
                icon.blob_id.as_str(),
                icon.original_size,
                icon.encrypted_size,
            )
        });
        let app = db::apps::resolve_synced_app(
            pool,
            &group.group_id,
            &source.source_key,
            app,
            icon,
            source_updated_at,
            source_revision,
        )
        .await?;
        Ok(app.id)
    }

    /// Retries all missing source icons through the currently available peer or Hub connection.
    async fn refresh_source_app_assets(
        &self,
        connection: Option<&Connection>,
        group: &GroupSecrets,
    ) -> Result<usize> {
        let pool = self.pool().await;
        let store = self.app.state::<AppIconStore>();
        let mut pending_count = 0;
        for asset in db::apps::source_app_assets(&pool, &group.group_id).await? {
            let icon = SourceIconRef {
                icon_hash: asset.icon_hash.clone(),
                blob_id: asset.blob_id.clone(),
                original_size: asset.original_size,
                encrypted_size: asset.encrypted_size,
            };
            let encrypted_available =
                self.source_icon_blob_record(&icon)
                    .ok()
                    .is_some_and(|encrypted| {
                        Path::new(&encrypted.encrypted_path)
                            .metadata()
                            .is_ok_and(|metadata| metadata.len() == encrypted.size)
                    });
            if encrypted_available
                && asset.is_attached
                && store.icon_file_for_hash(&asset.icon_hash).is_some()
            {
                continue;
            }
            pending_count += 1;
            if let Err(error) = self
                .refresh_source_app_asset(connection, group, &pool, &asset)
                .await
            {
                log::debug!(
                    "source app icon {} remains pending: {error}",
                    asset.icon_hash
                );
            }
        }
        Ok(pending_count)
    }

    /// Publishes queued local source icons and removes them only after the Hub acknowledges them.
    async fn publish_pending_source_icons(
        &self,
        connection: Option<&Connection>,
        group: &GroupSecrets,
    ) -> Result<usize> {
        let Some(connection) = connection else {
            return Ok(0);
        };
        if !self.cloud_async_source_icons_supported(connection, &group.group_id) {
            return Ok(0);
        }

        let pool = self.pool().await;
        let blobs = repository::pending_source_icon_uploads(
            &pool,
            &group.group_id,
            MAX_SOURCE_ICON_UPLOADS_PER_BATCH,
        )
        .await?;
        if blobs.is_empty() {
            return Ok(0);
        }

        let blob_ids = blobs
            .iter()
            .map(|blob| blob.blob_id.clone())
            .collect::<Vec<_>>();
        upload_blobs(
            connection,
            group,
            blobs,
            BlobUploadKind::PublishedSourceIcon,
        )
        .await?;
        repository::complete_source_icon_uploads(&pool, &group.group_id, &blob_ids).await?;
        self.confirmed_cloud_source_icons
            .write()
            .expect("confirmed cloud source icons poisoned")
            .confirm(connection.stable_id(), &group.group_id, &blob_ids);
        Ok(blob_ids.len())
    }

    async fn refresh_source_app_asset(
        &self,
        connection: Option<&Connection>,
        group: &GroupSecrets,
        pool: &SqlitePool,
        asset: &db::apps::SourceAppAsset,
    ) -> Result<()> {
        let icon = SourceIconRef {
            icon_hash: asset.icon_hash.clone(),
            blob_id: asset.blob_id.clone(),
            original_size: asset.original_size,
            encrypted_size: asset.encrypted_size,
        };
        let blob = self
            .ensure_source_icon_blob(connection, group, &icon)
            .await?;
        let encrypted = PathBuf::from(&blob.encrypted_path);

        let store = self.app.state::<AppIconStore>();
        let metadata = match store.synced_metadata_for_hash(&asset.icon_hash) {
            Ok(metadata) => metadata,
            Err(_) => {
                let temporary = tempfile::tempdir()?;
                let destination = temporary.path().join("source-icon.png");
                let encrypted_for_task = encrypted.clone();
                let destination_for_task = destination.clone();
                let key = group.content_key_bytes()?;
                let original_size = asset.original_size;
                tauri::async_runtime::spawn_blocking(move || {
                    crypto::decrypt_blob(
                        &encrypted_for_task,
                        &destination_for_task,
                        original_size,
                        &key,
                    )
                })
                .await
                .context("source app icon decryption task failed")??;
                let bytes = fs::read(destination)?;
                store.store_synced_png(&bytes, &asset.icon_hash)?
            }
        };
        let updated = db::apps::update_synced_app_icon(
            pool,
            &asset.app_id,
            &metadata.file_name,
            &metadata.icon_hash,
            &metadata.accent_start,
            &metadata.accent_end,
        )
        .await?;
        if !updated {
            return Ok(());
        }
        if let Err(error) = self.app.emit(
            crate::clipboard::SOURCE_APP_UPDATED_EVENT,
            serde_json::json!({ "appId": asset.app_id }),
        ) {
            log::warn!("emit source app update failed: {error}");
        }
        #[cfg(target_os = "android")]
        crate::commands::android::notify_overlay_clipboard_changed();
        Ok(())
    }

    async fn ensure_source_icon_blob(
        &self,
        connection: Option<&Connection>,
        group: &GroupSecrets,
        icon: &SourceIconRef,
    ) -> Result<StoredBlob> {
        let expected = self.source_icon_blob_record(icon)?;
        let manifest = BlobManifest {
            blob_id: icon.blob_id.clone(),
            name: format!("{}.png", icon.icon_hash),
            original_size: icon.original_size,
            encrypted_size: icon.encrypted_size,
            role: BlobRole::Image,
            file_index: None,
            is_directory_archive: false,
        };
        let encrypted = PathBuf::from(&expected.encrypted_path);
        if encrypted.is_file() {
            let is_valid = encrypted.metadata()?.len() == icon.encrypted_size
                && crypto::hash_file(&encrypted)? == icon.blob_id;
            if is_valid {
                return Ok(expected);
            }
            fs::remove_file(&encrypted)
                .with_context(|| format!("remove invalid source icon blob {encrypted:?}"))?;
        }

        let store = self.app.state::<AppIconStore>();
        if let Some(file_name) = store.icon_file_for_hash(&icon.icon_hash) {
            let source = store.icon_path(&file_name);
            let root = crate::core::paths::resources_dir(&self.app)?.join("sync-blobs");
            let root_for_task = root.clone();
            let key = group.content_key_bytes()?;
            let icon_hash = icon.icon_hash.clone();
            let blob = tauri::async_runtime::spawn_blocking(move || {
                crypto::encrypt_stable_blob(&source, &root_for_task, &key, &icon_hash)
            })
            .await
            .context("cached source app icon encryption task failed")??;
            if blob.blob_id == icon.blob_id && blob.size == icon.encrypted_size {
                return Ok(blob);
            }
            log::warn!("cached source app icon does not match synchronized blob reference");
        }

        let connection = connection.context("source app icon blob is unavailable")?;
        download_blob(connection, group, &manifest, &encrypted).await?;
        Ok(expected)
    }

    fn source_icon_blob_record(&self, icon: &SourceIconRef) -> Result<StoredBlob> {
        validate_source_icon_ref(icon)?;
        let root = crate::core::paths::resources_dir(&self.app)?.join("sync-blobs");
        let encrypted = crypto::blob_path(&root, &icon.blob_id)?;
        Ok(StoredBlob {
            blob_id: icon.blob_id.clone(),
            encrypted_path: encrypted.to_string_lossy().into_owned(),
            size: icon.encrypted_size,
        })
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
            direct_addresses: address
                .ip_addrs()
                .filter(|address| is_lan_ip(address.ip()))
                .map(ToString::to_string)
                .collect(),
            relay_urls: Vec::new(),
        }
    }

    fn update_discovery_metadata(&self, endpoint: &Endpoint, lan_enabled: bool) -> Result<()> {
        let identity = self.identity.snapshot();
        let metadata = match (lan_enabled, identity.group.as_ref()) {
            (true, Some(group)) => Some(DiscoveryMetadata {
                version: JOIN_PROTOCOL_VERSION,
                space_id: discovery_space_id(group)?,
                device_name: identity.device_name.clone(),
                platform: std::env::consts::OS.into(),
                presence_nonce: Some(self.lan_presence_nonce.load(Ordering::Acquire)),
            }),
            _ => None,
        };
        let user_data: Option<UserData> = metadata.map(|value| value.encode()).transpose()?;
        endpoint.set_user_data_for_address_lookup(user_data);
        Ok(())
    }

    fn nearby_spaces(&self) -> Vec<NearbySyncSpace> {
        let cutoff = Utc::now() - chrono::Duration::seconds(30);
        let own_endpoint_id = self
            .identity
            .secret_key()
            .ok()
            .map(|key| key.public().to_string());
        let current_space_id = self
            .identity
            .snapshot()
            .group
            .as_ref()
            .and_then(|group| discovery_space_id(group).ok());
        let mut spaces = HashMap::<String, NearbySyncSpace>::new();
        for entry in self
            .nearby_devices
            .read()
            .expect("nearby devices poisoned")
            .values()
        {
            if entry.last_seen_at < cutoff
                || own_endpoint_id
                    .as_ref()
                    .is_some_and(|own| own == &entry.address.id.to_string())
            {
                continue;
            }
            let space = spaces
                .entry(entry.metadata.space_id.clone())
                .or_insert_with(|| NearbySyncSpace {
                    space_id: entry.metadata.space_id.clone(),
                    same_group: current_space_id.as_ref() == Some(&entry.metadata.space_id),
                    devices: Vec::new(),
                });
            space.devices.push(NearbySyncDevice {
                device_name: entry.metadata.device_name.clone(),
                platform: entry.metadata.platform.clone(),
                endpoint_id: entry.address.id.to_string(),
                direct_addresses: entry
                    .address
                    .ip_addrs()
                    .filter(|address| is_lan_ip(address.ip()))
                    .map(ToString::to_string)
                    .collect(),
                relay_urls: Vec::new(),
                last_seen_at: entry.last_seen_at.to_rfc3339(),
            });
        }
        let mut values = spaces.into_values().collect::<Vec<_>>();
        values.sort_by(|left, right| left.space_id.cmp(&right.space_id));
        for space in &mut values {
            space
                .devices
                .sort_by(|left, right| left.device_name.cmp(&right.device_name));
        }
        values
    }

    fn finish_outgoing_join(&self, request_id: &str, result: Result<JoinResponse>) {
        let mut attempts = self
            .outgoing_join_attempts
            .write()
            .expect("outgoing join attempts poisoned");
        let Some(attempt) = attempts.get_mut(request_id) else {
            return;
        };
        match result {
            Ok(JoinResponse::Approved { pairing_code }) => {
                attempt.state = NearbyJoinState::Approved;
                attempt.pairing_code = Some(pairing_code);
                attempt.last_error = None;
            }
            Ok(JoinResponse::Rejected) => attempt.state = NearbyJoinState::Rejected,
            Ok(JoinResponse::Expired) => attempt.state = NearbyJoinState::Expired,
            Ok(JoinResponse::Error { message }) => {
                attempt.state = NearbyJoinState::Error;
                attempt.last_error = Some(message);
            }
            Err(error) => {
                let message = error.to_string();
                attempt.state = if message.contains("超时") {
                    NearbyJoinState::Expired
                } else {
                    NearbyJoinState::Error
                };
                attempt.last_error = Some(message);
            }
        }
        let updated = attempt.clone();
        drop(attempts);
        if let Err(error) = self.app.emit(JOIN_ATTEMPT_UPDATED_EVENT, updated) {
            log::debug!("emit join attempt update failed: {error}");
        }
    }

    /// Starts one Iroh endpoint with target-scoped discovery and the configured cloud Relay mode.
    async fn ensure_endpoint(self: &Arc<Self>, settings: &SyncSettings) -> Result<Endpoint> {
        let relay_token = self.identity.cloud_relay_auth_token();
        let config = endpoint_runtime_config(settings, relay_token.as_deref());
        let relay_mode = cloud_relay_mode(settings, relay_token.as_deref())?;
        let secret_key = self.identity.secret_key()?;
        let mut runtime = self.runtime.lock().await;
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
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute::default();
        let mut builder = Endpoint::builder(presets::Minimal)
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
    async fn stop_runtime(&self) -> Result<()> {
        if self.pairing_sessions.load(Ordering::Acquire) > 0 {
            return Ok(());
        }
        let runtime = self.runtime.lock().await.take();
        if let Some(runtime) = runtime {
            runtime
                .router
                .shutdown()
                .await
                .context("failed to stop Iroh sync endpoint")?;
        }
        self.clear_connection_caches();
        *self
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute::default();
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
        update_channel_status(lock, state, error, succeeded, None);
    }

    fn set_channel_retry_status(&self, target: SyncTarget, error: &str, next_retry_at: &str) {
        let lock = match target {
            SyncTarget::Lan => &self.lan_status,
            SyncTarget::Cloud => &self.cloud_status,
        };
        update_channel_status(
            lock,
            SyncChannelState::Error,
            Some(error),
            false,
            Some(next_retry_at),
        );
    }

    fn set_cloud_watch_status(
        &self,
        state: SyncChannelState,
        error: Option<&str>,
        succeeded: bool,
    ) {
        update_channel_status(&self.cloud_watch_status, state, error, succeeded, None);
    }

    fn set_cloud_watch_retry_status(&self, error: &str, next_retry_at: &str) {
        update_channel_status(
            &self.cloud_watch_status,
            SyncChannelState::Error,
            Some(error),
            false,
            Some(next_retry_at),
        );
    }

    fn set_cloud_path(&self, connection: &Connection) {
        let (address, transport) = connection_path(connection);
        *self
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute {
            address,
            transport: transport.map(str::to_owned),
        };
    }

    fn update_cloud_server_version(&self, version: Option<String>) {
        let Some(version) = version else {
            return;
        };
        let mut current = self
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

    async fn connect_cloud(
        self: &Arc<Self>,
        endpoint: &Endpoint,
        server: EndpointAddr,
    ) -> Result<Connection> {
        if let Some(connection) = self.cached_cloud_connection() {
            return Ok(connection);
        }
        let _guard = self.cloud_connect_lock.lock().await;
        if let Some(connection) = self.cached_cloud_connection() {
            return Ok(connection);
        }
        let transport_config = QuicTransportConfig::builder()
            .keep_alive_interval(CLOUD_PATH_KEEP_ALIVE_INTERVAL)
            .default_path_keep_alive_interval(CLOUD_PATH_KEEP_ALIVE_INTERVAL)
            .build();
        let options = ConnectOptions::new().with_transport_config(transport_config);
        let connection = tokio::time::timeout(Duration::from_secs(8), async {
            let connecting = endpoint.connect_with_opts(server, ALPN, options).await?;
            Ok::<_, anyhow::Error>(connecting.await?)
        })
        .await
        .context("云端同步连接超时")??;
        log::info!(
            "cloud connection established: connectionId={} keepAliveMs={} pathKeepAliveMs={}",
            connection.stable_id(),
            CLOUD_PATH_KEEP_ALIVE_INTERVAL.as_millis(),
            CLOUD_PATH_KEEP_ALIVE_INTERVAL.as_millis(),
        );
        *self
            .cloud_connection
            .write()
            .expect("cloud connection cache poisoned") = Some(connection.clone());
        self.cloud_connection_ready.notify_waiters();
        self.source_asset_wake.notify_one();
        self.cloud_session
            .write()
            .expect("cloud session state poisoned")
            .take();
        self.cloud_server_version
            .write()
            .expect("cloud server version poisoned")
            .take();
        Ok(connection)
    }

    /// Explicit user actions may interrupt backoff, but only the watch worker creates connections.
    async fn request_cloud_connection(&self, wait_timeout: Duration) -> Result<Connection> {
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

        self.watch_wake.notify_one();
        tokio::time::timeout(wait_timeout, async {
            loop {
                let notified = self.cloud_connection_ready.notified();
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
    fn configured_cloud_connection(&self, settings: &SyncSettings) -> Result<Option<Connection>> {
        if !server_is_configured(settings) {
            return Ok(None);
        }

        self.cached_cloud_connection()
            .context("云端同步连接尚未就绪")
            .map(Some)
    }

    fn cached_cloud_connection(&self) -> Option<Connection> {
        self.cloud_connection
            .read()
            .expect("cloud connection cache poisoned")
            .as_ref()
            .filter(|connection| connection.close_reason().is_none())
            .cloned()
    }

    fn invalidate_cloud_connection(&self, stable_id: usize) {
        let removed = {
            let mut connection = self
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
            self.confirmed_cloud_source_icons
                .write()
                .expect("confirmed cloud source icons poisoned")
                .clear_connection(stable_id);
            let mut session = self
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
    fn invalidate_current_cloud_connection(&self) -> Option<usize> {
        let connection = self
            .cloud_connection
            .write()
            .expect("cloud connection cache poisoned")
            .take()?;
        let stable_id = connection.stable_id();
        connection.close(0_u32.into(), b"cloud network path changed");
        self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Idle, None, false);
        self.confirmed_cloud_source_icons
            .write()
            .expect("confirmed cloud source icons poisoned")
            .clear_connection(stable_id);
        let mut session = self
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
        self.cloud_server_version
            .write()
            .expect("cloud server version poisoned")
            .take();
        *self
            .cloud_path
            .write()
            .expect("cloud connection route poisoned") = ConnectionRoute::default();
        Some(stable_id)
    }

    fn clear_connection_caches(&self) {
        self.cloud_connection
            .write()
            .expect("cloud connection cache poisoned")
            .take();
        self.cloud_session
            .write()
            .expect("cloud session state poisoned")
            .take();
        *self
            .confirmed_cloud_source_icons
            .write()
            .expect("confirmed cloud source icons poisoned") = ConfirmedCloudSourceIcons::default();
        self.cloud_server_version
            .write()
            .expect("cloud server version poisoned")
            .take();
        self.lan_connections
            .write()
            .expect("LAN connection cache poisoned")
            .clear();
    }

    fn emit_updated(&self) {
        if let Err(error) = self.app.emit(SYNC_UPDATED_EVENT, ()) {
            log::debug!("emit sync update failed: {error}");
        }
        #[cfg(target_os = "android")]
        crate::commands::android::notify_overlay_sync_status_changed();
    }
}

fn update_channel_status(
    lock: &RwLock<SyncChannelStatus>,
    state: SyncChannelState,
    error: Option<&str>,
    succeeded: bool,
    next_retry_at: Option<&str>,
) {
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
    status.next_retry_at = next_retry_at.map(str::to_owned);
}

fn merge_cloud_status(
    transfer: &SyncChannelStatus,
    watch: &SyncChannelStatus,
) -> SyncChannelStatus {
    if transfer.state == SyncChannelState::Disabled && watch.state == SyncChannelState::Disabled {
        return SyncChannelStatus::new(SyncChannelState::Disabled);
    }
    let transfer_healthy = transfer.state == SyncChannelState::Online;
    let watch_healthy = watch.state == SyncChannelState::Online;
    let state = match (transfer_healthy, watch_healthy) {
        (true, true) => SyncChannelState::Online,
        (true, false) if watch.state == SyncChannelState::Error => SyncChannelState::Degraded,
        (false, true) if transfer.state == SyncChannelState::Error => SyncChannelState::Degraded,
        (true, false) | (false, true) => SyncChannelState::Online,
        (false, false)
            if transfer.state == SyncChannelState::Connecting
                || watch.state == SyncChannelState::Connecting =>
        {
            SyncChannelState::Connecting
        }
        (false, false)
            if transfer.state == SyncChannelState::Error
                || watch.state == SyncChannelState::Error =>
        {
            SyncChannelState::Error
        }
        _ => SyncChannelState::Idle,
    };
    SyncChannelStatus {
        state,
        last_attempt_at: latest_timestamp(
            transfer.last_attempt_at.as_ref(),
            watch.last_attempt_at.as_ref(),
        ),
        last_success_at: latest_timestamp(
            transfer.last_success_at.as_ref(),
            watch.last_success_at.as_ref(),
        ),
        last_error: if transfer_healthy && !watch_healthy {
            watch.last_error.clone()
        } else if watch_healthy && !transfer_healthy {
            transfer.last_error.clone()
        } else {
            transfer
                .last_error
                .clone()
                .or_else(|| watch.last_error.clone())
        },
        next_retry_at: earliest_timestamp(
            transfer.next_retry_at.as_ref(),
            watch.next_retry_at.as_ref(),
        ),
    }
}

fn latest_timestamp(left: Option<&String>, right: Option<&String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right).clone()),
        (Some(value), None) | (None, Some(value)) => Some(value.clone()),
        (None, None) => None,
    }
}

fn earliest_timestamp(left: Option<&String>, right: Option<&String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right).clone()),
        (Some(value), None) | (None, Some(value)) => Some(value.clone()),
        (None, None) => None,
    }
}

fn endpoint_runtime_config(
    settings: &SyncSettings,
    relay_token: Option<&str>,
) -> EndpointRuntimeConfig {
    EndpointRuntimeConfig {
        lan_enabled: settings.lan_enabled,
        cloud_enabled: settings.cloud_enabled,
        cloud_relay_mode: settings.cloud_relay_mode,
        server_endpoint_id: settings.server_endpoint_id.trim().to_owned(),
        server_direct_addresses: settings
            .server_direct_addresses
            .iter()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .collect(),
        server_relay_urls: settings
            .server_relay_urls
            .iter()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .collect(),
        relay_token_hash: relay_token
            .map(|value| blake3::hash(value.as_bytes()).to_hex().to_string()),
    }
}

fn cloud_relay_mode(settings: &SyncSettings, token: Option<&str>) -> Result<RelayMode> {
    if !settings.cloud_enabled || settings.cloud_relay_mode == CloudRelayMode::Off {
        return Ok(RelayMode::Disabled);
    }
    if settings.cloud_relay_mode == CloudRelayMode::Public {
        return Ok(RelayMode::Default);
    }

    let relay_urls = settings
        .server_relay_urls
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.parse::<RelayUrl>().context("invalid Iroh relay URL"))
        .collect::<Result<Vec<_>>>()?;
    if relay_urls.is_empty() {
        bail!("自定义 Relay 模式至少需要一个 Relay 地址");
    }
    let relay_map = RelayMap::from_iter(relay_urls);
    let relay_map = token
        .filter(|value| !value.is_empty())
        .map(|value| relay_map.clone().with_auth_token(value))
        .unwrap_or(relay_map);
    Ok(RelayMode::Custom(relay_map))
}

async fn cloud_transfer_worker(manager: Arc<SyncManager>) {
    let mut cloud_failure_count = 0_usize;
    let mut cloud_next_at: Option<Instant> = None;
    loop {
        if let Some(next_at) = cloud_next_at {
            loop {
                tokio::select! {
                    _ = manager.cloud_transfer_wake.notified() => {
                        if manager.cloud_force_reconnect.swap(false, Ordering::AcqRel) {
                            cloud_failure_count = 0;
                            break;
                        }
                    },
                    _ = tokio::time::sleep_until(next_at.into()) => break,
                }
            }
        } else {
            manager.cloud_transfer_wake.notified().await;
            manager
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
            .run_cycle(Some(SyncTarget::Cloud), None, Duration::from_secs(8))
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

async fn lan_transfer_worker(manager: Arc<SyncManager>) {
    loop {
        manager.lan_transfer_wake.notified().await;
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
async fn dispatch_lan_peer_actors(manager: &Arc<SyncManager>) -> Result<()> {
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

async fn lan_peer_worker(manager: Arc<SyncManager>, device_id: String, actor: Arc<LanPeerActor>) {
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
            .run_cycle(
                Some(SyncTarget::Lan),
                Some(&device_id),
                LAN_EVENT_CONNECT_TIMEOUT,
            )
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
fn should_run_lan_peer_cycle(force_connect: bool, pending_work: bool, suspended: bool) -> bool {
    (force_connect || pending_work) && (!suspended || force_connect)
}

/// Connects only to the explicit LAN routes supplied by mDNS/cached peer metadata.
async fn connect_peer(
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
fn spawn_endpoint_watchers(
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
async fn process_mdns_event(manager: &Arc<SyncManager>, event: PendingMdnsEvent) {
    match event {
        PendingMdnsEvent::Expired { endpoint_id } => {
            // Discovery presence is not connection liveness. Android may filter multicast while
            // a direct QUIC path remains healthy, so only the connection watcher marks it offline.
            manager
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
                manager.nearby_wake.notify_waiters();
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

fn presence_nonce_changed(previous: Option<u64>, current: Option<u64>) -> bool {
    current.is_some() && previous != current
}

/// Returns every private or link-local IPv4 address on which mDNS should operate.
#[cfg(any(not(target_os = "android"), test))]
fn lan_multicast_interfaces_v4(address: &EndpointAddr) -> BTreeSet<Ipv4Addr> {
    collect_lan_multicast_interfaces_v4(address.ip_addrs().map(|address| address.ip()))
}

fn ipv4_direct_addr_filter() -> AddrFilter {
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
fn collect_lan_multicast_interfaces_v4(
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
fn parse_android_lan_multicast_interfaces(value: &str) -> BTreeSet<Ipv4Addr> {
    value
        .split(',')
        .filter_map(|address| address.parse::<Ipv4Addr>().ok())
        .collect()
}

/// Publishes and downloads optional source icons without delaying clipboard event delivery.
async fn source_asset_worker(manager: Arc<SyncManager>) {
    let mut upload_failure_count = 0_usize;
    loop {
        if upload_failure_count == 0 {
            manager.source_asset_wake.notified().await;
        } else {
            tokio::select! {
                _ = manager.source_asset_wake.notified() => {},
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
                        manager.source_asset_wake.notify_one();
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

async fn cloud_watch_worker(manager: Arc<SyncManager>) {
    let mut failure_count = 0_usize;
    loop {
        let settings = manager.app.state::<SettingsStore>().snapshot().sync;
        let Some(group) = manager.identity.snapshot().group else {
            failure_count = 0;
            manager.set_cloud_watch_status(SyncChannelState::Disabled, None, false);
            manager.watch_wake.notified().await;
            continue;
        };
        if !settings.enabled || !settings.cloud_enabled {
            failure_count = 0;
            manager.set_cloud_watch_status(SyncChannelState::Disabled, None, false);
            manager.watch_wake.notified().await;
            continue;
        }
        let server = match server_endpoint_addr(&settings).await {
            Ok(Some(server)) => server,
            Ok(None) => {
                failure_count = 0;
                manager.set_cloud_watch_status(SyncChannelState::Disabled, None, false);
                manager.watch_wake.notified().await;
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
                    _ = manager.watch_wake.notified() => failure_count = 0,
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
                    _ = manager.watch_wake.notified() => failure_count = 0,
                    _ = tokio::time::sleep(delay) => {},
                }
            }
        }
    }
}

/// Watches one cloud group on a persistent response stream and falls back for older hubs.
async fn watch_cloud_group(
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
        _ = manager.watch_wake.notified() => return Ok(()),
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
            _ = manager.watch_wake.notified() => return Ok(()),
            response = tokio::time::timeout(CLOUD_WATCH_RESPONSE_TIMEOUT, read_frame(&mut recv)) => {
                response.context("等待云端持续订阅心跳超时")??
            },
        };
        apply_cloud_watch_response(manager, response, &mut cursor, &mut removed_at_ms)?;
    }
}

/// Keeps two independently framed watch streams so one delayed packet does not gate wake-up.
async fn watch_cloud_group_redundant(
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
        _ = manager.watch_wake.notified() => return Ok(()),
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
            _ = manager.watch_wake.notified() => return Ok(()),
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
                    _ = manager.watch_wake.notified() => return Ok(()),
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

fn next_cloud_watch_slot_retry(retry_at: &[Option<Instant>; 2]) -> Option<(usize, Instant)> {
    retry_at
        .iter()
        .enumerate()
        .filter_map(|(slot, retry_at)| retry_at.map(|retry_at| (slot, retry_at)))
        .min_by_key(|(_, retry_at)| *retry_at)
}

async fn wait_for_cloud_watch_slot_retry(retry_at: Option<Instant>) {
    let Some(retry_at) = retry_at else {
        std::future::pending::<()>().await;
        return;
    };

    tokio::time::sleep_until(retry_at.into()).await;
}

/// Opens one fixed watch slot and confirms that its initial response is readable.
async fn open_cloud_watch_stream(
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
fn spawn_cloud_watch_reader(
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
async fn watch_cloud_group_legacy(
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
            _ = manager.watch_wake.notified() => return Ok(()),
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
                    _ = manager.watch_wake.notified() => return Ok(()),
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
fn mark_cloud_watch_ready(manager: &SyncManager, watch_ready: &mut bool) {
    if *watch_ready {
        return;
    }
    *watch_ready = true;
    manager.set_cloud_watch_status(SyncChannelState::Online, None, true);
    manager.emit_updated();
}

#[derive(Debug, Eq, PartialEq)]
struct CloudWatchChanges {
    events_changed: bool,
    removals_changed: bool,
}

/// Advances the local watch watermarks and reports work only when the Hub moved either one.
fn advance_cloud_watch_watermarks(
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
fn apply_cloud_watch_watermarks(
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
    manager.source_asset_wake.notify_one();
    manager.wake_cloud_transfer();
}

fn apply_cloud_watch_response(
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

fn retry_delay(failure_count: usize) -> Duration {
    let index = failure_count.saturating_sub(1).min(RETRY_SECONDS.len() - 1);
    let base = RETRY_SECONDS[index];
    let jitter = (Utc::now().timestamp_subsec_millis() as u64) % (base / 4 + 1);
    Duration::from_secs(base + jitter)
}

fn retry_schedule(failure_count: usize) -> (Duration, String) {
    let delay = retry_delay(failure_count);
    (delay, (Utc::now() + delay).to_rfc3339())
}

/// Counts only failures that occurred without an intervening confirmed watch response.
fn next_cloud_watch_failure_count(failure_count: usize, watch_ready: bool) -> usize {
    if watch_ready {
        return 1;
    }
    failure_count.saturating_add(1)
}

#[derive(Clone)]
struct PeerService {
    manager: Weak<SyncManager>,
}

#[derive(Clone)]
struct JoinService {
    manager: Weak<SyncManager>,
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
        if let Err(error) = ensure_lan_connection(&connection, LAN_PATH_SELECTION_TIMEOUT).await {
            log::warn!("rejected non-LAN join connection: {error}");
            return Ok(());
        }
        if let Err(error) = handle_join_connection(manager, connection).await {
            log::warn!("LAN join request failed: {error}");
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

    let commit_result = if let Some(peer) = approved_peer.as_ref() {
        let pool = manager.pool().await;
        repository::restore_and_upsert_peer(&pool, peer).await
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

    let trailing = tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, recv.read_to_end(0))
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
    if !settings.enabled || !settings.lan_enabled || manager.identity.snapshot().group.is_none() {
        bail!("该设备当前不接受局域网加入申请");
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
    peer_endpoint_addr(&announcement).context("加入申请包含无效的连接地址")?;
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
        if let Err(error) = ensure_lan_connection(&connection, LAN_PATH_SELECTION_TIMEOUT).await {
            log::warn!("rejected non-LAN sync connection: {error}");
            return Ok(());
        }
        let connection_admission = Arc::new(Semaphore::new(8));
        let connection_blobs = Arc::new(Semaphore::new(2));
        loop {
            let (send, recv) = match connection.accept_bi().await {
                Ok(value) => value,
                Err(_) => break,
            };
            let global_admission = match manager
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
    let _blob_permits = if matches!(
        &request,
        Request::PutBlob { .. } | Request::PutSourceIcon { .. } | Request::GetBlob { .. }
    ) {
        Some((
            manager
                .inbound_blob_concurrency
                .clone()
                .acquire_owned()
                .await?,
            connection_blobs.acquire_owned().await?,
        ))
    } else {
        None
    };
    drop(admission_permits);
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
    let settings = manager.app.state::<SettingsStore>().snapshot().sync;
    if !settings.enabled || !settings.lan_enabled {
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
        Request::Sync {
            group_id,
            access_token,
            device,
            after_cursor,
            events,
            limit,
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
            manager.emit_updated();
            let mut incoming_event_ids = Vec::new();
            let mut remote_event_ids = Vec::new();
            let mut accepted = Vec::new();
            for event in events {
                let result = repository::insert_event(&pool, &event, false, &[]).await?;
                if !result.stored {
                    log::warn!(
                        "ignore LAN sync event {} with a reused origin sequence from {}",
                        event.event_id,
                        event.origin_device_id
                    );
                    continue;
                }
                incoming_event_ids.push(event.event_id.clone());
                if event.origin_device_id != manager.identity.snapshot().device_id {
                    remote_event_ids.push(event.event_id.clone());
                }
                if result.inserted {
                    accepted.push(event.event_id.clone());
                }
            }
            let received_new_events = !accepted.is_empty();
            repository::mark_delivered(
                &pool,
                &format!("peer:{}", device.device_id),
                &incoming_event_ids,
            )
            .await?;
            repository::mark_delivered(&pool, CLOUD_TARGET, &remote_event_ids).await?;
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
            let apply_guard = manager.apply_lock.lock().await;
            let pending_apply = repository::unapplied_events(&pool, MAX_EVENTS_PER_BATCH).await?;
            let mut source_assets_pending = false;
            let mut latest_clipboard_item = None;
            #[cfg(target_os = "android")]
            let mut applied_any = false;
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
                    let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
                    repository::attach_event_blobs(&pool, &stored.event.event_id, &blobs).await?;
                    if let Ok((item_id, item, fingerprint)) = manager
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
                        manager
                            .reconcile_item_timestamps(&pool, &group, &item_id)
                            .await
                            .ok();
                        repository::mark_applied(&pool, &stored.event.event_id)
                            .await
                            .ok();
                        latest_clipboard_item = Some((item, fingerprint));
                        if let Some(icon) = source_icon {
                            if let Ok(blob) = manager.source_icon_blob_record(&icon) {
                                repository::attach_event_blobs(
                                    &pool,
                                    &stored.event.event_id,
                                    &[blob],
                                )
                                .await
                                .ok();
                                source_assets_pending = true;
                            }
                        }
                        #[cfg(target_os = "android")]
                        {
                            applied_any = true;
                        }
                    }
                }
            }
            if let Some((item, fingerprint)) = latest_clipboard_item.as_ref() {
                manager.write_latest_synced_item_to_clipboard(item, fingerprint.as_ref());
            }
            drop(apply_guard);
            #[cfg(target_os = "android")]
            if applied_any {
                crate::commands::android::notify_overlay_clipboard_changed();
            }
            if source_assets_pending {
                manager.source_asset_wake.notify_one();
            }
            if received_new_events {
                manager.wake_lan_transfer();
            }
        }
        Request::SyncV2 { .. } => {
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
                // Membership history alone must not revive a suspended LAN connection attempt.
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
        } => {
            authenticate_local(manager, &group_id, &access_token)?;
            let pool = manager.pool().await;
            if repository::is_peer_removed(&pool, "", &connection.remote_id().to_string()).await? {
                bail!("device was removed; pair it again before syncing");
            }
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
            let pool = manager.pool().await;
            if repository::is_peer_removed(&pool, "", &connection.remote_id().to_string()).await? {
                bail!("device was removed; pair it again before syncing");
            }
            let path = crypto::blob_path(
                &crate::core::paths::resources_dir(&manager.app)?.join("sync-blobs"),
                &blob_id,
            )?;
            let size = path.metadata()?.len();
            write_frame(send, &Response::BlobReady { size }).await?;
            let mut file = tokio::fs::File::open(path).await?;
            copy_with_idle_timeout(&mut file, send, BLOB_IDLE_TIMEOUT).await?;
        }
        Request::CreateGroup { .. }
        | Request::HealthV2
        | Request::Watch { .. }
        | Request::WatchGroup { .. }
        | Request::WatchGroupStream { .. }
        | Request::WatchGroupStreamV2 { .. }
        | Request::WatchGroupStreamV3 { .. }
        | Request::ListEvents { .. } => {
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

/// Bounds LAN control-response latency without constraining request or blob transfer duration.
async fn call_lan(connection: &Connection, request: Request) -> Result<Response> {
    let (mut send, mut recv) =
        tokio::time::timeout(LAN_CONTROL_REQUEST_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| anyhow::Error::new(LanAttemptTimeout).context("打开局域网同步流超时"))??;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    let length = match tokio::time::timeout(LAN_CONTROL_REQUEST_TIMEOUT, recv.read_u32()).await {
        Ok(length) => length.context("读取局域网同步响应长度")? as usize,
        Err(_) => {
            return Err(anyhow::Error::new(LanAttemptTimeout).context("等待局域网同步响应超时"));
        }
    };
    if length > MAX_FRAME_BYTES {
        bail!("LAN response frame is too large: {length} bytes");
    }
    let mut encoded = vec![0; length];
    match tokio::time::timeout(BLOB_IDLE_TIMEOUT, recv.read_exact(&mut encoded)).await {
        Ok(result) => result.context("读取局域网同步响应")?,
        Err(_) => {
            return Err(anyhow::Error::new(LanAttemptTimeout).context("读取局域网同步响应超时"));
        }
    }
    minicbor::decode(&encoded).context("decode LAN sync response")
}

/// Bounds short cloud control requests without constraining LAN or long-lived watch streams.
async fn call_cloud(connection: &Connection, request: Request) -> Result<Response> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    let length = match tokio::time::timeout(CLOUD_CONTROL_REQUEST_TIMEOUT, recv.read_u32()).await {
        Ok(length) => length.context("read cloud response frame length")? as usize,
        Err(_) => {
            return Err(anyhow::Error::new(CloudAttemptTimeout).context("等待云端同步响应超时"));
        }
    };
    if length > ecopaste_sync_protocol::MAX_FRAME_BYTES {
        bail!("cloud response frame is too large: {length} bytes");
    }
    let mut encoded = vec![0; length];
    tokio::time::timeout(BLOB_IDLE_TIMEOUT, recv.read_exact(&mut encoded))
        .await
        .context("读取云端同步响应超时")??;
    minicbor::decode(&encoded).context("decode cloud sync response")
}

/// Starts a duplicate idempotent sync request only when the primary exceeds the normal tail.
async fn call_cloud_hedged(connection: &Connection, request: Request) -> Result<Response> {
    #[derive(Clone, Copy)]
    enum HedgeRole {
        Primary,
        Backup,
    }

    impl HedgeRole {
        fn as_str(self) -> &'static str {
            match self {
                Self::Primary => "primary",
                Self::Backup => "backup",
            }
        }
    }

    let started_at = Instant::now();
    let stats_before = connection.stats();
    let mut primary = Box::pin(call_cloud(connection, request.clone()));
    match tokio::time::timeout(CLOUD_SYNC_HEDGE_DELAY, &mut primary).await {
        Ok(result) => return result,
        Err(_) => log::info!(
            "cloud sync hedge started: connectionId={} delayMs={} lostPackets={}",
            connection.stable_id(),
            CLOUD_SYNC_HEDGE_DELAY.as_millis(),
            stats_before.lost_packets,
        ),
    }

    let mut backup = Box::pin(call_cloud(connection, request));
    let (first_role, first_result) = tokio::select! {
        result = &mut primary => (HedgeRole::Primary, result),
        result = &mut backup => (HedgeRole::Backup, result),
    };
    let (winner, first_failed, result) = match first_result {
        Ok(response) => (first_role, false, Ok(response)),
        Err(first_error) => {
            log::debug!(
                "cloud sync hedge first attempt failed: role={} error={first_error}",
                first_role.as_str()
            );
            let (second_role, second_result) = match first_role {
                HedgeRole::Primary => (HedgeRole::Backup, backup.await),
                HedgeRole::Backup => (HedgeRole::Primary, primary.await),
            };
            (
                second_role,
                true,
                second_result.with_context(|| {
                    format!(
                        "cloud sync hedge failed after {} attempt: {first_error}",
                        first_role.as_str()
                    )
                }),
            )
        }
    };
    let stats_after = connection.stats();
    log::info!(
        "cloud sync hedge completed: connectionId={} winner={} firstFailed={} elapsedMs={} lostPacketsDelta={} sentDatagramsDelta={} receivedDatagramsDelta={}",
        connection.stable_id(),
        winner.as_str(),
        first_failed,
        started_at.elapsed().as_millis(),
        stats_after
            .lost_packets
            .saturating_sub(stats_before.lost_packets),
        stats_after
            .udp_tx
            .datagrams
            .saturating_sub(stats_before.udp_tx.datagrams),
        stats_after
            .udp_rx
            .datagrams
            .saturating_sub(stats_before.udp_rx.datagrams),
    );
    result
}

fn is_cloud_attempt_timeout(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CloudAttemptTimeout>().is_some()
}

fn is_lan_attempt_timeout(error: &anyhow::Error) -> bool {
    error.downcast_ref::<LanAttemptTimeout>().is_some()
}

/// Confirms both the approval response and its durable commit before returning it to the UI.
async fn call_join(connection: &Connection, request: JoinRequest) -> Result<JoinResponse> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let request_id = request.request_id.clone();
    write_frame(&mut send, &request).await?;
    let response = read_frame(&mut recv).await.context("读取加入申请结果")?;
    write_frame(
        &mut send,
        &JoinAcknowledgement {
            version: JOIN_PROTOCOL_VERSION,
            request_id,
        },
    )
    .await?;
    let completion: JoinCompletion =
        tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, read_frame(&mut recv))
            .await
            .context("确认加入申请结果超时")?
            .context("确认加入申请结果")?;
    send.finish()?;
    let stopped = tokio::time::timeout(JOIN_HANDSHAKE_TIMEOUT, send.stopped())
        .await
        .context("等待批准设备接收确认超时")??;
    if stopped.is_some() {
        bail!("批准设备中止了加入确认");
    }
    match completion {
        JoinCompletion::Committed => Ok(response),
        JoinCompletion::Error { message } => Ok(JoinResponse::Error { message }),
    }
}

#[derive(Default)]
struct BlobUploadTimings {
    open: Duration,
    write: Duration,
    response: Duration,
    transfer: Duration,
    confirmation: Duration,
    total: Duration,
}

#[derive(Clone, Copy)]
enum BlobUploadKind {
    Content,
    SourceIcon,
    PublishedSourceIcon,
}

impl BlobUploadKind {
    fn label(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::SourceIcon | Self::PublishedSourceIcon => "source-icon",
        }
    }
}

async fn upload_blob(
    connection: &Connection,
    group: &GroupSecrets,
    blob: &StoredBlob,
    kind: BlobUploadKind,
) -> Result<()> {
    let total_started_at = Instant::now();
    let open_started_at = Instant::now();
    let (mut send, mut recv) = tokio::time::timeout(FIRST_FRAME_TIMEOUT, connection.open_bi())
        .await
        .context("open blob upload stream timeout")??;
    let open_elapsed = open_started_at.elapsed();
    let write_started_at = Instant::now();
    let request = match kind {
        BlobUploadKind::PublishedSourceIcon => Request::PutSourceIcon {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            blob_id: blob.blob_id.clone(),
            size: blob.size,
        },
        BlobUploadKind::Content | BlobUploadKind::SourceIcon => Request::PutBlob {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            blob_id: blob.blob_id.clone(),
            size: blob.size,
        },
    };
    write_frame(&mut send, &request).await?;
    let write_elapsed = write_started_at.elapsed();
    let response_started_at = Instant::now();
    match tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame::<_, Response>(&mut recv))
        .await
        .context("wait for blob upload readiness timeout")??
    {
        Response::BlobStored => {
            let response_elapsed = response_started_at.elapsed();
            send.finish()?;
            log_blob_upload_completed(
                connection,
                blob,
                kind,
                "already-stored",
                0,
                BlobUploadTimings {
                    open: open_elapsed,
                    write: write_elapsed,
                    response: response_elapsed,
                    total: total_started_at.elapsed(),
                    ..Default::default()
                },
            );
            return Ok(());
        }
        Response::BlobReady { .. } => {}
        Response::Error { message, .. } => bail!(message),
        _ => bail!("invalid blob upload response"),
    }
    let readiness_elapsed = response_started_at.elapsed();
    let transfer_started_at = Instant::now();
    let mut file = tokio::fs::File::open(&blob.encrypted_path).await?;
    copy_with_idle_timeout(&mut file, &mut send, BLOB_IDLE_TIMEOUT).await?;
    send.finish()?;
    let transfer_elapsed = transfer_started_at.elapsed();
    let confirmation_started_at = Instant::now();
    match tokio::time::timeout(BLOB_IDLE_TIMEOUT, read_frame(&mut recv))
        .await
        .context("wait for blob storage confirmation timeout")??
    {
        Response::BlobStored => {
            log_blob_upload_completed(
                connection,
                blob,
                kind,
                "uploaded",
                blob.size,
                BlobUploadTimings {
                    open: open_elapsed,
                    write: write_elapsed,
                    response: readiness_elapsed,
                    transfer: transfer_elapsed,
                    confirmation: confirmation_started_at.elapsed(),
                    total: total_started_at.elapsed(),
                },
            );
            Ok(())
        }
        Response::Error { message, .. } => bail!(message),
        _ => bail!("invalid blob stored response"),
    }
}

/// Records which stage of a blob existence check or upload consumed the elapsed time.
fn log_blob_upload_completed(
    connection: &Connection,
    blob: &StoredBlob,
    kind: BlobUploadKind,
    outcome: &str,
    uploaded_bytes: u64,
    timings: BlobUploadTimings,
) {
    let (address, transport) = connection_path(connection);
    let rtt = connection
        .paths()
        .iter()
        .find(|path| path.is_selected())
        .map(|path| path.rtt().as_millis().to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    log::info!(
        "sync blob upload completed: blobRef={} kind={} outcome={outcome} blobBytes={} uploadedBytes={uploaded_bytes} openMs={} writeMs={} responseMs={} transferMs={} confirmationMs={} totalMs={} address={} transport={} rttMs={rtt}",
        blob_log_ref(&blob.blob_id),
        kind.label(),
        blob.size,
        timings.open.as_millis(),
        timings.write.as_millis(),
        timings.response.as_millis(),
        timings.transfer.as_millis(),
        timings.confirmation.as_millis(),
        timings.total.as_millis(),
        address.as_deref().unwrap_or("unknown"),
        transport.unwrap_or("unknown"),
    );
}

fn blob_log_ref(blob_id: &str) -> &str {
    blob_id.get(..12).unwrap_or(blob_id)
}

/// Separates best-effort source icons from blobs required to apply clipboard content.
fn split_source_icon_blobs(
    blobs: Vec<StoredBlob>,
    source_icon_blob_ids: &HashSet<String>,
) -> (Vec<StoredBlob>, Vec<StoredBlob>) {
    blobs
        .into_iter()
        .partition(|blob| !source_icon_blob_ids.contains(&blob.blob_id))
}

fn source_icon_blob_ids(events: &[StoredSyncEvent], key: &[u8; 32]) -> HashSet<String> {
    events
        .iter()
        .filter_map(|stored| crypto::decrypt_event(key, &stored.event).ok())
        .filter_map(|envelope| envelope.source_app.as_ref().and_then(valid_source_icon))
        .map(|icon| icon.blob_id)
        .collect()
}

/// Uploads independent encrypted blobs over a small number of parallel QUIC streams.
async fn upload_blobs(
    connection: &Connection,
    group: &GroupSecrets,
    blobs: Vec<StoredBlob>,
    kind: BlobUploadKind,
) -> Result<()> {
    for batch in blobs.chunks(MAX_CONCURRENT_BLOB_TRANSFERS) {
        let mut tasks = tokio::task::JoinSet::new();
        for blob in batch {
            let connection = connection.clone();
            let group = group.clone();
            let blob = blob.clone();
            tasks.spawn(async move { upload_blob(&connection, &group, &blob, kind).await });
        }
        while let Some(result) = tasks.join_next().await {
            result.context("blob upload task failed")??;
        }
    }
    Ok(())
}

async fn download_blob(
    connection: &Connection,
    group: &GroupSecrets,
    manifest: &BlobManifest,
    destination: &Path,
) -> Result<()> {
    let (mut send, mut recv) = tokio::time::timeout(FIRST_FRAME_TIMEOUT, connection.open_bi())
        .await
        .context("open blob download stream timeout")??;
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
    let response = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut recv))
        .await
        .context("wait for blob download response timeout")??;
    let Response::BlobReady { size } = response else {
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
    let mut cleanup = TemporaryBlobCleanup {
        path: temporary.clone(),
        armed: true,
    };
    let result = async {
        let mut file = tokio::fs::File::create(&temporary).await?;
        let mut remaining = size;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        while remaining > 0 {
            let requested =
                usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let read = tokio::time::timeout(BLOB_IDLE_TIMEOUT, recv.read(&mut buffer[..requested]))
                .await
                .context("blob download idle timeout")??
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
        cleanup.armed = false;
        Ok(())
    }
    .await;
    result
}

struct TemporaryBlobCleanup {
    path: PathBuf,
    armed: bool,
}

impl Drop for TemporaryBlobCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

async fn copy_with_idle_timeout<R, W>(
    reader: &mut R,
    writer: &mut W,
    idle_timeout: Duration,
) -> Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut copied = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = tokio::time::timeout(idle_timeout, reader.read(&mut buffer))
            .await
            .context("blob read idle timeout")??;
        if read == 0 {
            break;
        }
        tokio::time::timeout(idle_timeout, writer.write_all(&buffer[..read]))
            .await
            .context("blob write idle timeout")??;
        copied += read as u64;
    }
    writer.flush().await?;
    Ok(copied)
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

async fn ensure_lan_connection(connection: &Connection, selection_timeout: Duration) -> Result<()> {
    let mut paths = connection.paths_stream();
    tokio::time::timeout(selection_timeout, async {
        while let Some(paths) = paths.next().await {
            let selected = paths
                .iter()
                .find(|path| path.is_selected())
                .map(|path| path.remote_addr().clone());
            if lan_path_snapshot_is_usable(
                selected,
                paths.iter().map(|path| path.remote_addr().clone()),
            )? {
                return Ok(());
            }
        }
        bail!("连接已关闭")
    })
    .await
    .context("连接没有可用路径")?
}

/// Accepts Iroh's selected-path-less snapshot only when every open path is LAN-private.
fn lan_path_snapshot_is_usable(
    selected: Option<TransportAddr>,
    paths: impl IntoIterator<Item = TransportAddr>,
) -> Result<bool> {
    if let Some(selected) = selected {
        ensure_lan_transport_addr(&selected)?;
        return Ok(true);
    }

    let mut has_path = false;
    for path in paths {
        has_path = true;
        if ensure_lan_transport_addr(&path).is_err() {
            return Ok(false);
        }
    }
    Ok(has_path)
}

/// 校验 Iroh 的结构化传输地址，避免把 `ip:地址` 展示文本误当作 SocketAddr 解析。
fn ensure_lan_transport_addr(transport: &TransportAddr) -> Result<()> {
    let TransportAddr::Ip(address) = transport else {
        bail!("局域网同步禁止使用 Relay");
    };
    if !is_lan_ip(address.ip()) {
        bail!("局域网同步禁止使用公网地址");
    }

    Ok(())
}

fn event_matches_item_timestamp(
    event: &EncryptedEvent,
    key: [u8; 32],
    item: &ClipboardItem,
) -> bool {
    crypto::decrypt_event(&key, event).is_ok_and(|envelope| {
        envelope.item.updated_at_ms == Some(item.updated_at.timestamp_millis())
    })
}

fn synced_item_timestamps(
    item: &SyncedClipboardItem,
    event_created_at_ms: i64,
) -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let fallback = Utc
        .timestamp_millis_opt(event_created_at_ms)
        .single()
        .unwrap_or_else(Utc::now);
    let created_at = Utc
        .timestamp_millis_opt(item.created_at_ms)
        .single()
        .unwrap_or(fallback);
    let updated_at = item
        .updated_at_ms
        .and_then(|value| Utc.timestamp_millis_opt(value).single())
        .unwrap_or(created_at)
        .max(created_at);

    (created_at, updated_at)
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

fn validate_source_app_ref(source: &SourceAppRef) -> Result<()> {
    if source.version != 1
        || !is_hex_identifier(&source.source_key)
        || source.display_name.trim().is_empty()
        || source.display_name.chars().count() > 100
        || source.display_name.chars().any(char::is_control)
    {
        bail!("invalid synchronized source app");
    }
    if !matches!(source.platform.as_str(), "macos" | "windows" | "android") {
        bail!("invalid synchronized source app platform");
    }
    for accent in [source.accent_start.as_deref(), source.accent_end.as_deref()]
        .into_iter()
        .flatten()
    {
        if !is_hex_color(accent) {
            bail!("invalid synchronized source app accent");
        }
    }
    Ok(())
}

fn validate_source_icon_ref(icon: &SourceIconRef) -> Result<()> {
    if !is_hex_identifier(&icon.icon_hash)
        || !is_hex_identifier(&icon.blob_id)
        || icon.original_size == 0
        || icon.original_size > crate::clipboard::MAX_APP_ICON_BYTES as u64
        || icon.encrypted_size == 0
        || icon.encrypted_size > crate::clipboard::MAX_APP_ICON_BYTES as u64 + 128
    {
        bail!("invalid synchronized source app icon");
    }
    Ok(())
}

fn valid_source_icon(source: &SourceAppRef) -> Option<SourceIconRef> {
    validate_source_app_ref(source).ok()?;
    let icon = source.icon.as_ref()?;
    validate_source_icon_ref(icon).ok()?;
    Some(icon.clone())
}

fn is_hex_identifier(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_hex_color(value: &str) -> bool {
    value.len() == 7
        && value.starts_with('#')
        && value[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn source_app_key(key: &[u8; 32], platform: Platform, local_id: &str) -> String {
    let source_key_key = blake3::derive_key("EcoPaste source application key v1", key);
    let mut input = Vec::with_capacity(local_id.len() + 16);
    input.extend_from_slice(platform_string(platform).as_bytes());
    input.push(0);
    input.extend_from_slice(local_id.as_bytes());
    blake3::keyed_hash(&source_key_key, &input)
        .to_hex()
        .to_string()
}

fn sanitize_source_app_name(value: &str) -> String {
    let sanitized = value
        .chars()
        .filter(|character| !character.is_control())
        .take(100)
        .collect::<String>();
    let sanitized = sanitized.trim();
    if sanitized.is_empty() {
        "Unknown".to_owned()
    } else {
        sanitized.to_owned()
    }
}

fn cloud_record(
    cursor: u64,
    event: EncryptedEvent,
    envelope: ClipboardEnvelope,
    device_name: String,
    image_path: Option<String>,
) -> CloudRecord {
    let is_sensitive = envelope.item.is_sensitive;
    let file_count = envelope.blobs.len().try_into().unwrap_or(u32::MAX);
    let total_size = envelope.blobs.iter().map(|blob| blob.original_size).sum();
    let preview = match envelope.item.kind.as_str() {
        "files" => envelope
            .blobs
            .iter()
            .map(|blob| blob.name.as_str())
            .collect::<Vec<_>>()
            .join("、"),
        "image" => envelope
            .item
            .summary
            .clone()
            .unwrap_or_else(|| "图片".to_owned()),
        _ => envelope
            .item
            .search_text
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| envelope.item.content.clone()),
    };
    CloudRecord {
        cursor,
        event_id: event.event_id,
        device_name,
        kind: envelope.item.kind,
        preview,
        image_path,
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

/// 用父目录隔离同名根项，避免改变系统剪贴板会继续传播的 basename。
fn synced_file_destination(root: &Path, file_index: u32, name: &str) -> PathBuf {
    root.join(format!("{file_index:03}"))
        .join(sanitize_name(name))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_file_upload_uses_the_whole_card_size() {
        assert!(permits_automatic_file_upload(10 * 1024 * 1024, 10));
        assert!(!permits_automatic_file_upload(10 * 1024 * 1024 + 1, 10));
        assert!(!permits_automatic_file_upload(1, 0));
    }

    #[test]
    fn synchronized_files_keep_their_logical_basename() {
        let root = Path::new("sync-files/event");
        let first = synced_file_destination(root, 0, "report.txt");
        let second = synced_file_destination(root, 1, "report.txt");

        assert_ne!(first, second);
        assert_eq!(first.file_name().unwrap(), "report.txt");
        assert_eq!(second.file_name().unwrap(), "report.txt");
    }

    #[test]
    fn lan_multicast_uses_all_private_ipv4_interfaces() {
        let interfaces = collect_lan_multicast_interfaces_v4([
            "192.168.50.84".parse().unwrap(),
            "172.31.128.1".parse().unwrap(),
            "198.18.0.1".parse().unwrap(),
            "127.0.0.1".parse().unwrap(),
            "fe80::1".parse().unwrap(),
        ]);

        assert_eq!(
            interfaces,
            [
                Ipv4Addr::new(172, 31, 128, 1),
                Ipv4Addr::new(192, 168, 50, 84),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn android_lan_multicast_uses_only_reported_ipv4_interfaces() {
        let interfaces =
            parse_android_lan_multicast_interfaces("10.120.90.161,invalid,fe80::1,192.168.50.84");

        assert_eq!(
            interfaces,
            [
                Ipv4Addr::new(10, 120, 90, 161),
                Ipv4Addr::new(192, 168, 50, 84),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn mdns_publishes_only_ipv4_direct_addresses() {
        let addresses = vec![
            TransportAddr::Ip("192.168.50.84:44820".parse().unwrap()),
            TransportAddr::Ip("[fd00::84]:44820".parse().unwrap()),
            TransportAddr::Relay("https://relay.example.com".parse().unwrap()),
        ];

        assert_eq!(
            ipv4_direct_addr_filter().apply(&addresses).as_ref(),
            &[TransportAddr::Ip("192.168.50.84:44820".parse().unwrap())]
        );
    }

    fn channel(state: SyncChannelState, error: Option<&str>) -> SyncChannelStatus {
        SyncChannelStatus {
            state,
            last_attempt_at: Some("2026-08-28T08:00:00Z".to_owned()),
            last_success_at: (state == SyncChannelState::Online)
                .then(|| "2026-08-28T07:59:00Z".to_owned()),
            last_error: error.map(str::to_owned),
            next_retry_at: error.map(|_| "2026-08-28T08:00:30Z".to_owned()),
        }
    }

    #[test]
    fn cloud_status_is_degraded_when_watch_fails_after_transfer_success() {
        let transfer = channel(SyncChannelState::Online, None);
        let watch = channel(SyncChannelState::Error, Some("watch reset"));

        let merged = merge_cloud_status(&transfer, &watch);

        assert_eq!(merged.state, SyncChannelState::Degraded);
        assert_eq!(merged.last_error.as_deref(), Some("watch reset"));
        assert!(merged.last_success_at.is_some());
        assert_eq!(
            merged.next_retry_at.as_deref(),
            Some("2026-08-28T08:00:30Z")
        );
    }

    #[test]
    fn confirmed_cloud_watch_resets_consecutive_failure_count() {
        assert_eq!(next_cloud_watch_failure_count(5, true), 1);
        assert_eq!(next_cloud_watch_failure_count(5, false), 6);
    }

    #[test]
    fn unchanged_cloud_group_watch_heartbeat_schedules_no_work() {
        let mut cursor = 42;
        let mut removed_at_ms = 1_000;

        let changes = advance_cloud_watch_watermarks(42, 1_000, &mut cursor, &mut removed_at_ms);

        assert_eq!(changes, None);
        assert_eq!(cursor, 42);
        assert_eq!(removed_at_ms, 1_000);
    }

    #[test]
    fn cloud_group_watch_reports_only_advanced_watermarks() {
        let mut cursor = 42;
        let mut removed_at_ms = 1_000;

        assert_eq!(
            advance_cloud_watch_watermarks(43, 900, &mut cursor, &mut removed_at_ms),
            Some(CloudWatchChanges {
                events_changed: true,
                removals_changed: false,
            })
        );
        assert_eq!(cursor, 43);
        assert_eq!(removed_at_ms, 1_000);
        assert_eq!(
            advance_cloud_watch_watermarks(40, 1_001, &mut cursor, &mut removed_at_ms),
            Some(CloudWatchChanges {
                events_changed: false,
                removals_changed: true,
            })
        );
        assert_eq!(cursor, 43);
        assert_eq!(removed_at_ms, 1_001);
    }

    #[test]
    fn redundant_cloud_watch_restarts_the_earliest_failed_slot() {
        let now = Instant::now();
        let retry_at = [
            Some(now + Duration::from_secs(30)),
            Some(now + Duration::from_secs(2)),
        ];

        assert_eq!(
            next_cloud_watch_slot_retry(&retry_at).map(|(slot, _)| slot),
            Some(1)
        );
        assert!(next_cloud_watch_slot_retry(&[None, None]).is_none());
    }

    #[test]
    fn custom_relay_token_is_attached_only_to_custom_relay_map() {
        let settings = SyncSettings {
            cloud_enabled: true,
            cloud_relay_mode: CloudRelayMode::Custom,
            server_relay_urls: vec!["https://relay.example.com".to_owned()],
            ..Default::default()
        };

        let RelayMode::Custom(map) = cloud_relay_mode(&settings, Some("secret-token")).unwrap()
        else {
            panic!("expected custom relay mode");
        };

        assert_eq!(map.relays::<Vec<_>>().len(), 1);
        assert_eq!(
            map.relays::<Vec<_>>()[0].auth_token.as_deref(),
            Some("secret-token")
        );
    }

    #[test]
    fn relay_is_disabled_by_default() {
        assert!(matches!(
            cloud_relay_mode(&SyncSettings::default(), None).unwrap(),
            RelayMode::Disabled
        ));
    }

    #[test]
    fn hub_direct_address_change_rebuilds_endpoint_runtime() {
        let mut settings = SyncSettings {
            cloud_enabled: true,
            server_endpoint_id: "hub-endpoint".to_owned(),
            server_direct_addresses: vec![" 10.120.90.36:4443 ".to_owned()],
            ..Default::default()
        };
        let before = endpoint_runtime_config(&settings, None);

        settings.server_direct_addresses = vec!["10.120.90.129:4443".to_owned()];
        let after = endpoint_runtime_config(&settings, None);

        assert_eq!(before.server_direct_addresses, vec!["10.120.90.36:4443"]);
        assert_ne!(before, after);
    }

    #[test]
    fn lan_transport_accepts_private_ip_without_string_round_trip() {
        let transport = TransportAddr::Ip("192.168.50.84:56786".parse().unwrap());

        assert_eq!(transport.to_string(), "ip:192.168.50.84:56786");
        ensure_lan_transport_addr(&transport).unwrap();
    }

    #[test]
    fn lan_transport_rejects_relay_and_public_ip() {
        let relay = TransportAddr::Relay("https://relay.example.com".parse().unwrap());
        let public = TransportAddr::Ip("8.8.8.8:443".parse().unwrap());

        assert_eq!(
            ensure_lan_transport_addr(&relay).unwrap_err().to_string(),
            "局域网同步禁止使用 Relay"
        );
        assert_eq!(
            ensure_lan_transport_addr(&public).unwrap_err().to_string(),
            "局域网同步禁止使用公网地址"
        );
    }

    #[test]
    fn lan_path_snapshot_accepts_private_paths_without_a_selected_path() {
        let private = [
            TransportAddr::Ip("192.168.50.84:56786".parse().unwrap()),
            TransportAddr::Ip("10.0.0.8:56786".parse().unwrap()),
        ];

        assert!(lan_path_snapshot_is_usable(None, private).unwrap());
    }

    #[test]
    fn lan_path_snapshot_waits_when_unselected_paths_are_not_all_private() {
        let mixed = [
            TransportAddr::Ip("192.168.50.84:56786".parse().unwrap()),
            TransportAddr::Relay("https://relay.example.com".parse().unwrap()),
        ];
        let public = [TransportAddr::Ip("8.8.8.8:443".parse().unwrap())];

        assert!(!lan_path_snapshot_is_usable(None, mixed).unwrap());
        assert!(!lan_path_snapshot_is_usable(None, public).unwrap());
        assert!(!lan_path_snapshot_is_usable(None, []).unwrap());
    }

    #[test]
    fn lan_path_snapshot_rejects_a_selected_non_lan_path() {
        let relay = TransportAddr::Relay("https://relay.example.com".parse().unwrap());

        assert_eq!(
            lan_path_snapshot_is_usable(Some(relay.clone()), [relay])
                .unwrap_err()
                .to_string(),
            "局域网同步禁止使用 Relay"
        );
    }

    #[test]
    fn suspended_lan_peer_requires_an_explicit_wake_event() {
        assert!(!should_run_lan_peer_cycle(false, true, true));
        assert!(should_run_lan_peer_cycle(true, false, true));
        assert!(should_run_lan_peer_cycle(false, true, false));
        assert!(!should_run_lan_peer_cycle(false, false, false));
    }

    #[test]
    fn only_a_new_presence_nonce_wakes_an_existing_peer() {
        assert!(presence_nonce_changed(Some(1), Some(2)));
        assert!(presence_nonce_changed(None, Some(1)));
        assert!(!presence_nonce_changed(Some(1), Some(1)));
        assert!(!presence_nonce_changed(Some(1), None));
    }

    #[test]
    fn source_icon_is_used_only_when_the_complete_source_reference_is_valid() {
        let icon = SourceIconRef {
            icon_hash: "1".repeat(64),
            blob_id: "2".repeat(64),
            original_size: 128,
            encrypted_size: 192,
        };
        let mut source = SourceAppRef {
            version: 1,
            source_key: "3".repeat(64),
            platform: "macos".into(),
            display_name: "Example".into(),
            icon: Some(icon.clone()),
            accent_start: Some("#112233".into()),
            accent_end: Some("#445566".into()),
        };

        assert_eq!(valid_source_icon(&source), Some(icon));

        source.display_name.clear();
        assert_eq!(valid_source_icon(&source), None);
    }

    #[test]
    fn source_icon_blobs_are_separated_from_required_content() {
        let required = StoredBlob {
            blob_id: "required".into(),
            encrypted_path: "required.bin".into(),
            size: 10,
        };
        let source_icon = StoredBlob {
            blob_id: "source-icon".into(),
            encrypted_path: "source-icon.bin".into(),
            size: 20,
        };
        let source_icon_ids = HashSet::from([source_icon.blob_id.clone()]);

        let (required_blobs, source_icon_blobs) = split_source_icon_blobs(
            vec![required.clone(), source_icon.clone()],
            &source_icon_ids,
        );

        assert_eq!(required_blobs.len(), 1);
        assert_eq!(required_blobs[0].blob_id, required.blob_id);
        assert_eq!(source_icon_blobs.len(), 1);
        assert_eq!(source_icon_blobs[0].blob_id, source_icon.blob_id);
    }

    #[test]
    fn source_icon_confirmation_cache_is_scoped_to_cloud_connection_and_group() {
        let source_icon = StoredBlob {
            blob_id: "source-icon".into(),
            encrypted_path: "source-icon.bin".into(),
            size: 20,
        };
        let mut cache = ConfirmedCloudSourceIcons::default();
        cache.confirm(7, "group-a", std::slice::from_ref(&source_icon.blob_id));

        let (unconfirmed, hits) = cache.retain_unconfirmed(7, "group-a", vec![source_icon.clone()]);
        assert!(unconfirmed.is_empty());
        assert_eq!(hits, 1);

        let (unconfirmed, hits) = cache.retain_unconfirmed(8, "group-a", vec![source_icon.clone()]);
        assert_eq!(unconfirmed.len(), 1);
        assert_eq!(hits, 0);

        cache.confirm(8, "group-a", std::slice::from_ref(&source_icon.blob_id));
        let (unconfirmed, hits) = cache.retain_unconfirmed(8, "group-b", vec![source_icon]);
        assert_eq!(unconfirmed.len(), 1);
        assert_eq!(hits, 0);
    }

    #[test]
    fn cloud_record_keeps_complete_sensitive_content_and_image_path() {
        let content = "secret-token-".repeat(20);
        let event = EncryptedEvent {
            event_id: "event".into(),
            origin_device_id: "device".into(),
            origin_sequence: 1,
            created_at_ms: 1_700_000_000_000,
            nonce: vec![0; 24],
            ciphertext: vec![1],
        };
        let envelope = ClipboardEnvelope {
            version: 1,
            item: SyncedClipboardItem {
                kind: "text".into(),
                sub_kind: None,
                content: content.clone(),
                search_text: Some(content.clone()),
                summary: Some("short summary".into()),
                file_types: None,
                size: Some(content.len() as i64),
                width: None,
                height: None,
                is_sensitive: true,
                source_platform: "android".into(),
                created_at_ms: 1_700_000_000_000,
                content_hash: "hash".into(),
                updated_at_ms: Some(1_700_000_000_000),
                source_revision: Some("revision".into()),
            },
            blobs: Vec::new(),
            source_app: None,
        };

        let record = cloud_record(
            1,
            event,
            envelope,
            "Android".into(),
            Some("/tmp/image.png".into()),
        );

        assert!(record.is_sensitive);
        assert_eq!(record.preview, content);
        assert_eq!(record.image_path.as_deref(), Some("/tmp/image.png"));
    }
}
