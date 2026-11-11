mod blob_transfer;
mod event_model;
mod inbound;
mod outbound;
mod outbound_delivery;
mod pairing_service;
mod protocol_handlers;
mod protocol_io;
mod remote_download;
mod retention;
mod runtime;
mod runtime_connection;
mod runtime_state;
mod runtime_workers;
mod source_assets;
mod transport;

use self::blob_transfer::*;
use self::event_model::*;
use self::protocol_handlers::{JoinService, PeerService};
use self::protocol_io::*;
use self::runtime_state::RuntimeCoordinator;
use self::runtime_workers::*;
use self::transport::*;
use super::activity::SyncActivityGuard;

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, RwLock, Weak,
    },
    time::{Duration, Instant},
};

use crate::clipboard::cleanup::CleanupTrigger;
use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use ecopaste_sync_protocol::{
    limit_lan_content_plan, limit_sync_request, limit_sync_response, read_frame, write_frame,
    CloudEvent, CursorEvent, DeferredRetentionEvents, DeviceAnnouncement, EncryptedEvent,
    ErrorCode, PeerAnnouncement, RemovedDevice, Request, Response, SyncEventKind, ALPN,
    MAX_EVENTS_PER_BATCH, MAX_FRAME_BYTES, PROTOCOL_VERSION,
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
use n0_future::{boxed::BoxStream, BufferedStreamExt, StreamExt};
use rand::RngCore;
use sqlx::SqlitePool;
use tauri::{AppHandle, Emitter, Manager};
#[cfg(not(target_os = "android"))]
use tauri_plugin_dialog::DialogExt;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot, Mutex, Notify, OwnedSemaphorePermit, RwLock as AsyncRwLock, Semaphore},
};
use uuid::Uuid;

use crate::{
    clipboard::{
        calculate_current_file_item_size, fallback_accent_colors, AppIconStore,
        ClipboardFingerprint, ClipboardFingerprintState, ClipboardObservation,
        FileEntryFingerprint, ImageStore,
    },
    db::{
        self,
        models::{ClipboardApp, ClipboardItem, ClipboardKind, ClipboardSubKind, Platform},
    },
    settings::{CloudRelayMode, Language, SettingsStore, SyncSettings},
};

#[cfg(not(target_os = "android"))]
use crate::clipboard::WritebackGuard;

use super::{
    crypto,
    delivery::{
        DeliveryCancel, DeliveryCoordinator, OutgoingStagingLease, PreparationCommitGuard,
        PreparationProgress, PreparationTarget, TaskStagingCleanup, UploadTransferStart,
    },
    delivery_plan::{
        permits_automatic_file_upload, should_defer_remote_file_download, should_defer_remote_files,
    },
    identity::{
        is_lan_ip, peer_endpoint_addr, server_endpoint_addr, server_is_configured, GroupSecrets,
        IdentityStore, PairingCode,
    },
    model::{
        BlobManifest, BlobRole, ClipboardEnvelope, CloudDeviceDirectory, CloudDeviceStatus,
        CloudHubStatus, CloudRecord, CloudRecordPage, DecryptedSyncEvent, IncomingJoinRequest,
        LinkedSyncEvent, NearbyJoinAttempt, NearbyJoinState, NearbySyncDevice, NearbySyncSpace,
        SourceAppRef, SourceIconRef, StoredBlob, StoredContentObject, StoredSyncEvent,
        SyncChannelState, SyncChannelStatus, SyncContentAvailability, SyncItemStatus,
        SyncLocalContentState, SyncPairingPreview, SyncPeer, SyncStatus, SyncTarget,
        SyncedClipboardItem, SyncedItemStatePatch,
    },
    pairing::{
        comparison_code, discovery_space_id, DiscoveryMetadata, JoinAcknowledgement,
        JoinCompletion, JoinRequest, JoinResponse, PairingBootstrap, JOIN_ALPN,
        JOIN_PROTOCOL_VERSION, JOIN_TIMEOUT_SECS, PAIRING_INVITATION_TTL_SECS,
    },
    repository,
};

const CLOUD_TARGET: &str = "cloud";
const SYNC_UPDATED_EVENT: &str = "sync://updated";
const JOIN_REQUESTED_EVENT: &str = "sync://join-requested";
const JOIN_ATTEMPT_UPDATED_EVENT: &str = "sync://join-attempt-updated";
const NEARBY_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(8);
const JOIN_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(8);
const JOIN_COMMIT_TIMEOUT: Duration = Duration::from_secs(20);
const LAN_EVENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const LAN_PATH_SELECTION_TIMEOUT: Duration = Duration::from_secs(2);
const LAN_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLOUD_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const CLOUD_CONTROL_REQUEST_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const CLOUD_RECORD_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CLOUD_WATCH_RESPONSE_TIMEOUT: Duration = Duration::from_secs(90);
const HISTORY_BACKFILL_LIMIT: u16 = 100;
const MAX_SYNC_BATCHES_PER_CONNECTION: usize = 8;
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const BLOB_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_CONCURRENT_BLOB_TRANSFERS: usize = 2;
const CLOUD_UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const CLOUD_UPLOAD_PROBE_TIMEOUT: Duration = Duration::from_secs(4);
const CLOUD_RECORD_CACHE_DIR: &str = "cloud-records";
pub(crate) const SYNC_STAGING_DIR: &str = "sync-staging";
pub(crate) const LEGACY_SYNC_BLOB_DIR: &str = "sync-blobs";

/// Reuses a same-content local file record only while every referenced path is still available.
async fn available_materialized_file_item(
    pool: &SqlitePool,
    resources: &Path,
    sync_key: &str,
    source_revision: &str,
    content_identity: Option<&str>,
) -> Result<Option<ClipboardItem>> {
    let Some(item) = db::items::find_item_by_sync_key(pool, sync_key)
        .await?
        .filter(|item| item.kind == ClipboardKind::Files && item.remote_file_state.is_none())
    else {
        return Ok(None);
    };
    let same_content = if item.source_revision == source_revision {
        true
    } else if let Some(content_identity) = content_identity {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM sync_item_events WHERE item_id = ? AND content_identity = ?)",
        )
        .bind(&item.id)
        .bind(content_identity)
        .fetch_one(pool)
        .await?
    } else {
        false
    };
    if !same_content {
        return Ok(None);
    }
    let values = item
        .content
        .lines()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(None);
    }
    for value in values {
        let Ok(path) = crate::clipboard::shared_files::resolve_path(resources, value) else {
            return Ok(None);
        };
        if !tokio::fs::try_exists(path).await.unwrap_or(false) {
            return Ok(None);
        }
    }
    Ok(Some(item))
}

const MAX_SOURCE_ICON_UPLOADS_PER_BATCH: u16 = 64;
const LOCAL_SYNC_GC_BATCH_SIZE: u32 = 500;
const CLOUD_PATH_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);
const RETRY_SECONDS: [u64; 6] = [2, 5, 15, 30, 60, 300];

/// Applies cloud keep-alive intervals while the maintained transport handles GSO fallback.
fn sync_transport_config(cloud: bool) -> QuicTransportConfig {
    let builder = QuicTransportConfig::builder();
    let builder = if cloud {
        builder
            .keep_alive_interval(CLOUD_PATH_KEEP_ALIVE_INTERVAL)
            .default_path_keep_alive_interval(CLOUD_PATH_KEEP_ALIVE_INTERVAL)
    } else {
        builder
    };
    builder.build()
}

fn remove_unreferenced_sync_staging(root: &Path, live_blob_ids: &HashSet<String>) -> Result<u64> {
    if !root.exists() {
        return Ok(0);
    }
    let mut removed = 0_u64;
    for entry in walkdir::WalkDir::new(root)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                log::warn!("scan local sync blob for reclamation failed: {error}");
                continue;
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let Some(blob_id) = entry.file_name().to_str() else {
            continue;
        };
        if live_blob_ids.contains(blob_id)
            || crypto::blob_path(root, blob_id).ok().as_deref() != Some(entry.path())
        {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed = removed.saturating_add(1),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => log::warn!(
                "remove unreferenced local sync blob {} failed: {error}",
                entry.path().display()
            ),
        }
    }
    Ok(removed)
}

#[cfg(not(target_os = "android"))]
async fn external_download_target_available(target: &repository::ExternalDownloadTarget) -> bool {
    if target.destination_kind != "path" {
        return false;
    }
    let Ok(metadata) = tokio::fs::metadata(&target.destination).await else {
        return false;
    };
    if target.is_directory {
        metadata.is_dir()
    } else {
        metadata.is_file() && metadata.len() == target.expected_size
    }
}

#[cfg(target_os = "android")]
async fn external_download_target_available(target: &repository::ExternalDownloadTarget) -> bool {
    if target.destination_kind != "uri" {
        return false;
    }
    let destination = target.destination.clone();
    let expected_size = target.expected_size;
    let is_directory = target.is_directory;
    tauri::async_runtime::spawn_blocking(move || {
        crate::commands::android::is_android_remote_download_document_available(
            &destination,
            expected_size,
            is_directory,
        )
        .unwrap_or(false)
    })
    .await
    .unwrap_or(false)
}

pub struct SyncManager {
    app: AppHandle,
    identity: Arc<IdentityStore>,
    runtime_state: RuntimeCoordinator,
    maintenance_lock: Mutex<()>,
    apply_lock: Mutex<()>,
    delivery: DeliveryCoordinator,
    pending_observations: StdMutex<HashMap<(String, String), ClipboardObservation>>,
    blob_download_locks: Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>,
    incoming_join_requests: Mutex<HashMap<String, PendingIncomingJoinRequest>>,
    outgoing_join_attempts: RwLock<HashMap<String, NearbyJoinAttempt>>,
    pairing_invitations: Mutex<HashMap<String, PairingInvitation>>,
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

struct PairingInvitation {
    secret_hash: [u8; 32],
    expires_at_ms: i64,
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
}

#[derive(Debug, Clone)]
enum SyncConnectionTarget {
    Cloud,
    Peer {
        device_id: String,
        delivery_target: String,
    },
}

impl SyncConnectionTarget {
    fn peer(device_id: String) -> Self {
        Self::Peer {
            delivery_target: format!("peer:{device_id}"),
            device_id,
        }
    }

    fn is_cloud(&self) -> bool {
        matches!(self, Self::Cloud)
    }

    fn delivery_target(&self) -> &str {
        match self {
            Self::Cloud => CLOUD_TARGET,
            Self::Peer {
                delivery_target, ..
            } => delivery_target,
        }
    }

    fn peer_device_id(&self) -> Option<&str> {
        match self {
            Self::Cloud => None,
            Self::Peer { device_id, .. } => Some(device_id),
        }
    }

    fn remote_batch_source(&self) -> repository::RemoteBatchSource<'_> {
        match self {
            Self::Cloud => repository::RemoteBatchSource::Cloud,
            Self::Peer { device_id, .. } => repository::RemoteBatchSource::Peer { device_id },
        }
    }
}

enum LanPeerSignal {
    WorkQueued,
    RetryElapsed,
    RouteAvailable { changed: bool },
    ConnectionReady,
    ConnectionClosed,
    ManualReconnect(oneshot::Sender<std::result::Result<(), String>>),
    Stop,
}

struct LanPeerActor {
    signals: mpsc::UnboundedSender<LanPeerSignal>,
}

impl LanPeerActor {
    fn new() -> (Self, mpsc::UnboundedReceiver<LanPeerSignal>) {
        let (signals, receiver) = mpsc::unbounded_channel();
        (Self { signals }, receiver)
    }

    fn queue_work(&self) {
        let _ = self.signals.send(LanPeerSignal::WorkQueued);
    }

    fn route_available(&self, changed: bool) {
        let _ = self.signals.send(LanPeerSignal::RouteAvailable { changed });
    }

    fn connection_ready(&self) {
        let _ = self.signals.send(LanPeerSignal::ConnectionReady);
    }

    fn connection_closed(&self) {
        let _ = self.signals.send(LanPeerSignal::ConnectionClosed);
    }

    async fn reconnect(&self) -> Result<()> {
        let (reply, response) = oneshot::channel();
        if self
            .signals
            .send(LanPeerSignal::ManualReconnect(reply))
            .is_err()
        {
            bail!("局域网同步任务已停止");
        }
        match response.await.context("wait for LAN peer reconnect")? {
            Ok(()) => Ok(()),
            Err(message) => bail!(message),
        }
    }

    fn stop(&self) {
        let _ = self.signals.send(LanPeerSignal::Stop);
    }
}

#[derive(Clone)]
enum LanPeerSyncOutcome {
    Succeeded,
    ConnectionFailed(String),
    TransferFailed(String),
}

struct SyncConnectionOutcome {
    has_more: bool,
}

#[derive(Clone)]
struct SyncCycleContext {
    target: SyncTarget,
    settings: SyncSettings,
    device_id: String,
    device: DeviceAnnouncement,
    group: GroupSecrets,
    endpoint: Endpoint,
    pool: SqlitePool,
}

#[derive(Clone, Copy)]
struct SyncExchange<'a> {
    connection: &'a Connection,
    target: &'a SyncConnectionTarget,
    group: &'a GroupSecrets,
    pool: &'a SqlitePool,
    device_id: &'a str,
    device: &'a DeviceAnnouncement,
    settings: &'a SyncSettings,
}

struct SyncReadyOutcome {
    latest_cursor: u64,
    has_more: bool,
    event_ids: Vec<String>,
}

struct SyncBatchResponse {
    events: Vec<ReceivedSyncEvent>,
    peers: Vec<PeerAnnouncement>,
    latest_cursor: u64,
    removed_devices: Vec<RemovedDevice>,
    has_more: bool,
}

struct ReceivedSyncEvent {
    event: EncryptedEvent,
    cloud_blob_ids: Option<Vec<String>>,
}

impl From<CursorEvent> for ReceivedSyncEvent {
    fn from(value: CursorEvent) -> Self {
        Self {
            event: value.event,
            cloud_blob_ids: None,
        }
    }
}

impl From<CloudEvent> for ReceivedSyncEvent {
    fn from(value: CloudEvent) -> Self {
        Self {
            event: value.event,
            cloud_blob_ids: Some(value.available_blob_ids),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CloudAttemptPhase {
    OpenStream,
    WriteRequest,
    ReadResponseHeader,
    ReadResponseBody,
}

impl CloudAttemptPhase {
    fn label(self, language: Language) -> &'static str {
        use crate::i18n::sync::Key;

        let key = match self {
            Self::OpenStream => Key::OpenStream,
            Self::WriteRequest => Key::SendRequest,
            Self::ReadResponseHeader => Key::WaitForResponse,
            Self::ReadResponseBody => Key::ReadResponse,
        };
        crate::i18n::sync::label(language, key)
    }
}

#[derive(Debug)]
struct CloudAttemptTimeout {
    phase: CloudAttemptPhase,
    language: Language,
}

impl std::fmt::Display for CloudAttemptTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.phase.label(self.language))
    }
}

impl std::error::Error for CloudAttemptTimeout {}

#[derive(Debug)]
struct CloudHistoryGap;

impl std::fmt::Display for CloudHistoryGap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("云端较早的同步事件已按保留策略清理")
    }
}

impl std::error::Error for CloudHistoryGap {}

#[derive(Debug)]
struct RemoteBlobUnavailable;

impl std::fmt::Display for RemoteBlobUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("远端文件内容暂不可用，请稍后重试")
    }
}

impl std::error::Error for RemoteBlobUnavailable {}

#[derive(Debug)]
struct RemoteBlobTransportFailure {
    message: String,
    unavailable: bool,
}

impl RemoteBlobTransportFailure {
    fn from_error(error: anyhow::Error) -> Self {
        Self {
            unavailable: error.downcast_ref::<RemoteBlobUnavailable>().is_some(),
            message: format!("{error:#}"),
        }
    }
}

impl std::fmt::Display for RemoteBlobTransportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RemoteBlobTransportFailure {}

fn conclude_remote_blob_failure(error: anyhow::Error, only_unavailable: bool) -> anyhow::Error {
    if only_unavailable
        && error
            .downcast_ref::<RemoteBlobTransportFailure>()
            .is_some_and(|failure| failure.unavailable)
    {
        return RemoteBlobUnavailable.into();
    }
    error
}

struct MaterializationRun {
    processed: usize,
    deferred_content: bool,
    staging: TaskStagingCleanup,
}

struct ContentMaterializationContext<'a> {
    settings: &'a SyncSettings,
    device_id: &'a str,
    group: &'a GroupSecrets,
    allow_content: bool,
    preparation_target: Option<&'a PreparationTarget>,
}

enum ContentMaterialization {
    Completed,
    Deferred,
    Stored(TaskStagingCleanup),
}

#[derive(Clone)]
enum RemoteDownloadDestination {
    #[cfg(not(target_os = "android"))]
    Single(PathBuf),
    #[cfg(not(target_os = "android"))]
    Directory(PathBuf),
    #[cfg(target_os = "android")]
    Android(crate::commands::android::AndroidRemoteDownloadTarget),
}

struct RemoteDownloadRequest<'a> {
    event: &'a EncryptedEvent,
    group: &'a GroupSecrets,
    manifests: &'a [BlobManifest],
    destination: RemoteDownloadDestination,
    route: super::progress::TransferRoute,
}

#[derive(Clone, Copy)]
struct RemoteBlobTransferContext<'a> {
    connection: &'a Connection,
    group: &'a GroupSecrets,
    key: &'a [u8; 32],
    completed_bytes: u64,
    handle: &'a super::progress::TransferHandle,
    transferred: &'a AtomicU64,
}

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

pub fn init(app: &AppHandle) -> crate::core::Result<()> {
    let identity = Arc::new(IdentityStore::load_or_create(app)?);
    let mut presence_nonce = [0_u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut presence_nonce);
    let manager = Arc::new(SyncManager {
        app: app.clone(),
        identity,
        runtime_state: RuntimeCoordinator::new(u64::from_le_bytes(presence_nonce)),
        maintenance_lock: Mutex::new(()),
        apply_lock: Mutex::new(()),
        delivery: DeliveryCoordinator::new(app.clone()),
        pending_observations: StdMutex::new(HashMap::new()),
        blob_download_locks: Mutex::new(HashMap::new()),
        incoming_join_requests: Mutex::new(HashMap::new()),
        outgoing_join_attempts: RwLock::new(HashMap::new()),
        pairing_invitations: Mutex::new(HashMap::new()),
        join_rate_limits: RwLock::new(HashMap::new()),
        pairing_sessions: AtomicUsize::new(0),
    });
    app.manage(manager.clone());
    tauri::async_runtime::spawn(start_runtime(manager));
    Ok(())
}

/** 启动不影响首屏命令的维护任务，再拉起长驻同步工作线程。 */
async fn start_runtime(manager: Arc<SyncManager>) {
    clear_cloud_record_cache(&manager.app).await;
    remove_legacy_sync_blob_directory(&manager.app).await;
    let pool = manager.pool().await;
    if let Err(error) = repository::clear_stale_transfer_locks(&pool).await {
        log::warn!("clear stale sync transfer locks failed: {error:#}");
    }
    tauri::async_runtime::spawn(lan_transfer_worker(manager.clone()));
    tauri::async_runtime::spawn(cloud_transfer_worker(manager.clone()));
    tauri::async_runtime::spawn(cloud_watch_worker(manager.clone()));
    tauri::async_runtime::spawn(source_asset_worker(manager.clone()));
    tauri::async_runtime::spawn(local_outbox_worker(manager.clone()));
    manager.wake();
}

async fn clear_cloud_record_cache(app: &AppHandle) {
    let cache = match app.path().app_cache_dir() {
        Ok(cache) => cache.join(CLOUD_RECORD_CACHE_DIR),
        Err(error) => {
            log::warn!("resolve cloud record preview cache failed: {error}");
            return;
        }
    };
    if !cache.exists() {
        return;
    }

    let result = tauri::async_runtime::spawn_blocking(move || {
        fs::remove_dir_all(cache).context("clear cloud record preview cache")
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::warn!("clear cloud record preview cache failed: {error:#}"),
        Err(error) => log::warn!("cloud record cache cleanup task failed: {error}"),
    }
}

/// Deletes the retired durable ciphertext directory without delaying application startup.
async fn remove_legacy_sync_blob_directory(app: &AppHandle) {
    let legacy = match crate::core::paths::resources_dir(app) {
        Ok(resources) => resources.join(LEGACY_SYNC_BLOB_DIR),
        Err(error) => {
            log::warn!("resolve retired sync blob directory failed: {error:#}");
            return;
        }
    };
    let result =
        tauri::async_runtime::spawn_blocking(move || remove_legacy_sync_blob_path(&legacy)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::warn!("remove retired sync blob directory failed: {error:#}"),
        Err(error) => log::warn!("retired sync blob cleanup task failed: {error}"),
    }
}

/// Removes every form of the retired path, including a stale file or symbolic link.
fn remove_legacy_sync_blob_path(legacy: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(legacy) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect retired sync blob path {legacy:?}"));
        }
    };
    let result = if metadata.is_dir() {
        fs::remove_dir_all(legacy)
    } else {
        fs::remove_file(legacy)
    };
    result.with_context(|| format!("remove retired sync blob path {legacy:?}"))
}

impl SyncManager {
    /// Android shares use LAN only while synchronization and a direct peer are both available.
    #[cfg(target_os = "android")]
    pub fn has_connected_lan_peer(&self) -> bool {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled || !settings.lan_enabled || self.identity.snapshot().group.is_none() {
            return false;
        }
        self.runtime_state
            .lan_connections
            .read()
            .expect("LAN connection cache poisoned")
            .values()
            .any(|connection| connection.close_reason().is_none())
    }

    #[cfg(target_os = "android")]
    pub(crate) fn start_shared_file_import(
        &self,
        item_id: &str,
        total_bytes: u64,
        route: super::progress::TransferRoute,
    ) -> String {
        let task_id = blake3::hash(format!("android-share-import\0{item_id}").as_bytes())
            .to_hex()
            .to_string();
        self.delivery
            .start_local_import(super::progress::TransferTask {
                id: task_id.clone(),
                revision: 0,
                operation_id: format!("android-share:{item_id}"),
                item_id: item_id.to_owned(),
                direction: super::progress::TransferDirection::Upload,
                route,
                target: "import".to_owned(),
                file_name: None,
                phase: super::progress::TransferPhase::Preparing,
                transferred_bytes: 0,
                total_bytes,
                speed_bytes_per_second: 0,
                elapsed_ms: 0,
                eta_seconds: None,
                error: None,
                cancellable: true,
            });
        task_id
    }

    #[cfg(target_os = "android")]
    pub(crate) fn shared_file_import_canceled(&self, task_id: &str) -> bool {
        self.delivery.local_import_canceled(task_id)
    }

    #[cfg(target_os = "android")]
    pub(crate) fn update_shared_file_import(&self, task_id: &str, bytes: u64) {
        self.delivery.update_local_import(task_id, bytes);
    }

    #[cfg(target_os = "android")]
    pub(crate) fn finish_shared_file_import(&self, task_id: &str, total_bytes: u64) {
        self.delivery.finish_local_import(task_id, total_bytes);
    }

    #[cfg(target_os = "android")]
    pub(crate) fn bind_shared_file_import(
        &self,
        task_id: &str,
        operation_order: i64,
        source_revision: String,
    ) {
        self.delivery
            .bind_local_import(task_id, operation_order, source_revision);
    }

    #[cfg(target_os = "android")]
    pub(crate) fn fail_shared_file_import(&self, task_id: &str, bytes: u64, error: String) {
        self.delivery.fail_local_import(task_id, bytes, error);
    }

    pub fn transfer_tasks(&self) -> Vec<super::progress::TransferTask> {
        self.delivery.tasks()
    }

    pub(crate) fn item_has_active_local_import(&self, item_id: &str) -> bool {
        self.delivery.item_has_active_local_import(item_id)
    }

    pub async fn cancel_transfer(&self, task_id: &str) -> Result<bool> {
        match self.delivery.cancel(&self.pool().await, task_id).await? {
            DeliveryCancel::NotFound => Ok(false),
            DeliveryCancel::Canceled => Ok(true),
            DeliveryCancel::Preparation {
                item_id,
                source_revision,
            } => {
                self.discard_pending_observation(&item_id, &source_revision);
                Ok(true)
            }
        }
    }

    pub fn wake(&self) {
        self.runtime_state
            .cloud_force_reconnect
            .store(true, Ordering::Release);
        self.wake_transfer();
        self.runtime_state.watch_wake.notify_one();
        self.runtime_state.source_asset_wake.notify_one();
        self.runtime_state.local_outbox_wake.notify_one();
    }

    /// Reconnects LAN peers after an endpoint address or mDNS presence change.
    fn notify_lan_connectivity_changed(&self) {
        let actors = self
            .runtime_state
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if actors.is_empty() {
            self.wake_lan_transfer();
            return;
        }
        for actor in actors {
            actor.route_available(true);
        }
    }

    /// Bypasses cloud backoff and restarts both transfer and persistent watch workers.
    #[cfg(target_os = "android")]
    fn notify_cloud_network_changed(&self) {
        self.runtime_state
            .cloud_force_reconnect
            .store(true, Ordering::Release);
        self.wake_cloud_transfer();
        self.runtime_state.watch_wake.notify_one();
    }

    /// Preserves the existing desktop connection unless Iroh has already marked it unavailable.
    #[cfg(not(target_os = "android"))]
    fn notify_cloud_connectivity_changed(&self) {
        self.runtime_state
            .cloud_force_reconnect
            .store(true, Ordering::Release);
        self.wake_cloud_transfer();
        let watch_online = self
            .runtime_state
            .cloud_watch_status
            .read()
            .expect("cloud watch status poisoned")
            .state
            == SyncChannelState::Online;
        if !watch_online || self.cached_cloud_connection().is_none() {
            self.runtime_state.watch_wake.notify_one();
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
                .runtime_state
                .endpoint
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
            .runtime_state
            .android_lan_multicast_interfaces
            .write()
            .expect("Android LAN multicast interfaces poisoned") = interfaces;
        let manager = Arc::clone(self);
        tauri::async_runtime::spawn(async move {
            let runtime = manager
                .runtime_state
                .endpoint
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
                .runtime_state
                .android_lan_multicast_interfaces
                .write()
                .expect("Android LAN multicast interfaces poisoned")
                .clear();
            let runtime = manager
                .runtime_state
                .endpoint
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
                .runtime_state
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
                .runtime_state
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
                        manager.peer_connection_closed(&device_id);
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
                .runtime_state
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
        self.runtime_state
            .lan_presence_nonce
            .fetch_add(1, Ordering::AcqRel);
    }

    fn wake_transfer(&self) {
        self.wake_lan_transfer();
        self.runtime_state.cloud_transfer_wake.notify_one();
    }

    fn schedule_transfer_wake(&self, delay: chrono::Duration) {
        let delay = delay.to_std().unwrap_or(Duration::ZERO);
        let app = self.app.clone();
        tauri::async_runtime::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Some(manager) = app.try_state::<Arc<SyncManager>>() {
                manager.wake_transfer();
            }
        });
    }

    fn wake_lan_transfer(&self) {
        let actors = self
            .runtime_state
            .lan_peer_actors
            .read()
            .expect("LAN peer actors poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for actor in actors {
            actor.queue_work();
        }
        self.runtime_state.lan_transfer_wake.notify_one();
    }

    fn wake_cloud_transfer(&self) {
        self.runtime_state.cloud_transfer_wake.notify_one();
    }

    fn notify_cloud_connected(&self) {
        self.runtime_state
            .cloud_force_reconnect
            .store(true, Ordering::Release);
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
            .and_then(|group| discovery_space_id(group).ok())
            .is_some_and(|space_id| space_id == code.space_id);
        SyncPairingPreview {
            inviter_device_name: code.inviter_device_name().to_owned(),
            same_group,
        }
    }

    pub async fn status(&self) -> Result<SyncStatus> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let identity = self.identity.snapshot();
        let pool = self.pool().await;
        let (pending_events, pending_manual_items, peer_count) =
            repository::status_counts(&pool).await?;
        let mut peers = repository::list_peer_statuses(&pool).await?;
        let mut lan = self
            .runtime_state
            .lan_status
            .read()
            .expect("LAN sync status poisoned")
            .clone();
        let mut cloud_transfer = self
            .runtime_state
            .cloud_status
            .read()
            .expect("cloud sync status poisoned")
            .clone();
        let mut cloud_watch = self
            .runtime_state
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
            .runtime_state
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
                .runtime_state
                .endpoint
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
                .runtime_state
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
                let (cloud, lan) =
                    tokio::join!(self.run_cloud_cycle(), self.run_lan_peer_actors(None),);
                cloud.and(lan)
            }
            (true, false) => self.run_cloud_cycle().await,
            (false, true) => self.run_lan_peer_actors(None).await,
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
        #[cfg(target_os = "android")]
        self.refresh_android_lan_routes().await;
        self.run_lan_peer_actors(device_id.as_deref()).await
    }

    /// Sends an explicit reconnect request through each selected peer actor.
    async fn run_lan_peer_actors(self: &Arc<Self>, device_id: Option<&str>) -> Result<()> {
        let _ = reconcile_lan_peer_actors(self).await?;
        let actors = {
            let actors = self
                .runtime_state
                .lan_peer_actors
                .read()
                .expect("LAN peer actors poisoned");
            match device_id {
                Some(device_id) => vec![actors
                    .get(device_id)
                    .cloned()
                    .context("未找到指定的已配对设备")?],
                None => actors.values().cloned().collect::<Vec<_>>(),
            }
        };
        if actors.is_empty() {
            bail!(crate::i18n::commands::label(
                crate::i18n::current_language(&self.app),
                crate::i18n::commands::Key::SyncNoLanDevice,
            ));
        }

        let mut first_error = None;
        let mut succeeded = false;
        for batch in actors.chunks(3) {
            let mut tasks = tokio::task::JoinSet::new();
            for actor in batch {
                let actor = actor.clone();
                tasks.spawn(async move { actor.reconnect().await });
            }
            while let Some(result) = tasks.join_next().await {
                match result.context("LAN reconnect task failed")? {
                    Ok(()) => succeeded = true,
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        if succeeded {
            return Ok(());
        }
        match (device_id, first_error) {
            (Some(_), Some(error)) => Err(error),
            (None, Some(_)) => bail!(crate::i18n::commands::label(
                crate::i18n::current_language(&self.app),
                crate::i18n::commands::Key::SyncNoLanDevice,
            )),
            (_, None) => Ok(()),
        }
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
        self.runtime_state
            .cloud_force_reconnect
            .store(true, Ordering::Release);
        self.set_channel_status(SyncTarget::Cloud, SyncChannelState::Connecting, None, false);
        self.set_cloud_watch_status(SyncChannelState::Connecting, None, false);
        self.emit_updated();
        self.wake_cloud_transfer();
        self.runtime_state.watch_wake.notify_one();
        Ok(())
    }

    #[cfg(target_os = "android")]
    async fn refresh_android_lan_routes(&self) {
        let _guard = AndroidLanDiscoveryGuard::acquire();
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            self.runtime_state.nearby_wake.notified(),
        )
        .await;
    }

    /// Attaches ephemeral watcher context to an already durable content operation and wakes its
    /// materializer. Losing this context on process exit is safe because the outbox is persistent.
    pub fn notify_content_staged(
        &self,
        item: &ClipboardItem,
        observation: Option<ClipboardObservation>,
        ready: bool,
    ) {
        if let Some(observation) = observation {
            self.pending_observations
                .lock()
                .expect("pending clipboard observations poisoned")
                .insert((item.id.clone(), item.source_revision.clone()), observation);
        }
        if ready {
            self.wake_content_outbox();
        }
    }

    /// Content is materialized inside an active route cycle, so both the ordering worker and
    /// transport workers must wake after a durable content operation becomes ready.
    pub fn wake_content_outbox(&self) {
        self.runtime_state.local_outbox_wake.notify_one();
        self.wake_transfer();
    }

    /// Item-state events can be materialized without opening a content transfer route.
    pub fn wake_local_outbox(&self) {
        self.runtime_state.local_outbox_wake.notify_one();
    }

    pub async fn sync_item_now(
        self: &Arc<Self>,
        item: ClipboardItem,
        target: SyncTarget,
    ) -> Result<SyncItemStatus> {
        use crate::i18n::commands::Key;

        let lang = crate::i18n::current_language(&self.app);
        if self.item_has_active_local_import(&item.id) {
            bail!(crate::i18n::commands::label(
                lang,
                Key::FileImportInProgress
            ));
        }
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled {
            bail!(crate::i18n::commands::label(lang, Key::SyncDisabled));
        }
        if self.identity.snapshot().group.is_none() {
            bail!(crate::i18n::commands::label(lang, Key::SyncGroupRequired));
        }
        if item.remote_file_state.is_some() {
            bail!(crate::i18n::commands::label(
                lang,
                Key::RemoteFileNotDownloaded,
            ));
        }
        if item.is_sensitive && !settings.sync_sensitive {
            bail!(crate::i18n::commands::label(
                lang,
                Key::SyncSensitiveDisabled
            ));
        }
        if target == SyncTarget::Lan && !settings.lan_enabled {
            bail!(crate::i18n::commands::label(lang, Key::SyncLanDisabled));
        }
        if target == SyncTarget::Cloud && !server_is_configured(&settings) {
            bail!("云端同步未配置");
        }
        let pool = self.pool().await;
        if target == SyncTarget::Lan && repository::list_peers(&pool).await?.is_empty() {
            bail!(crate::i18n::commands::label(lang, Key::SyncNoLanDevice));
        }
        repository::resume_item_delivery(&pool, &item.id, target == SyncTarget::Cloud).await?;
        db::sync_queue::stage_existing_content(
            &pool,
            &item.id,
            &item.sync_key,
            &item.source_revision,
            crate::db::sync_queue::ContentSyncOptions {
                force_files: true,
                delivery_route: Some(if target == SyncTarget::Cloud {
                    crate::db::sync_queue::ContentDeliveryRoute::Cloud
                } else {
                    crate::db::sync_queue::ContentDeliveryRoute::Lan
                }),
                ..Default::default()
            },
        )
        .await?;
        if target == SyncTarget::Cloud {
            self.wake_cloud_transfer();
        } else {
            self.wake_lan_transfer();
        }
        self.item_statuses(&[item.id])
            .await?
            .into_iter()
            .next()
            .context("同步状态不存在")
    }

    pub async fn item_statuses(&self, item_ids: &[String]) -> Result<Vec<SyncItemStatus>> {
        let pool = self.pool().await;
        let mut statuses = repository::item_statuses(&pool, item_ids).await?;
        let sessions = repository::external_download_sessions(&pool, item_ids).await?;
        let mut known_external_items = HashSet::new();
        let mut valid_items = HashSet::new();
        for session in sessions {
            known_external_items.insert(session.item_id.clone());
            let mut valid = !session.targets.is_empty();
            for target in &session.targets {
                if !external_download_target_available(target).await {
                    valid = false;
                    break;
                }
            }
            if valid {
                valid_items.insert(session.item_id);
            }
        }
        for status in &mut statuses {
            if status.origin != super::model::SyncItemOrigin::Remote
                || status.local_state == SyncLocalContentState::Managed
            {
                continue;
            }
            if valid_items.contains(&status.item_id) {
                status.local_state = SyncLocalContentState::External;
            } else if known_external_items.contains(&status.item_id) {
                status.local_state = SyncLocalContentState::ExternalMissing;
            }
        }
        Ok(statuses)
    }

    /// Reads the Hub member directory without affecting synchronization cursors.
    pub async fn cloud_devices(self: &Arc<Self>) -> Result<CloudDeviceDirectory> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if !settings.enabled || !settings.cloud_enabled {
            bail!("请先启用多端同步和云端同步");
        }
        let identity = self.identity.snapshot();
        let group = identity
            .group
            .as_ref()
            .context("请先创建或加入同步设备组")?;
        let endpoint = self.ensure_endpoint(&settings).await?;
        let connection = self
            .request_cloud_connection(Duration::from_secs(8))
            .await?;
        let request = Request::ListDevices {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            device: self.cloud_device_announcement(&endpoint),
        };
        let (_, response) = self
            .request_cloud_query(connection, group, &request)
            .await?;
        let Response::Devices {
            devices,
            hub_status,
        } = response
        else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("云端返回了无效的设备响应");
        };
        let mut devices = devices
            .into_iter()
            .map(|device| {
                let last_seen_at = Utc
                    .timestamp_millis_opt(device.last_seen_ms)
                    .single()
                    .context("云端设备时间无效")?
                    .to_rfc3339();
                Ok(CloudDeviceStatus {
                    current: device.device_id == identity.device_id,
                    device_id: device.device_id,
                    device_name: device.device_name,
                    platform: device.platform,
                    app_version: device.app_version,
                    online: device.online,
                    last_seen_at,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        devices.sort_by(|left, right| {
            right
                .current
                .cmp(&left.current)
                .then_with(|| right.online.cmp(&left.online))
                .then_with(|| left.device_name.cmp(&right.device_name))
        });
        let hub_status = hub_status
            .map(|status| {
                let oldest_event_at = status
                    .oldest_event_received_at_ms
                    .map(|timestamp| {
                        Utc.timestamp_millis_opt(timestamp)
                            .single()
                            .context("云端保留时间无效")
                            .map(|value| value.to_rfc3339())
                    })
                    .transpose()?;
                Ok::<_, anyhow::Error>(CloudHubStatus {
                    used_bytes: status.used_bytes,
                    max_bytes: status.max_bytes,
                    event_count: status.event_count,
                    blob_count: status.blob_count,
                    oldest_event_at,
                    retention_days: status.retention_days,
                    max_events: status.max_events,
                })
            })
            .transpose()?;
        Ok(CloudDeviceDirectory {
            devices,
            hub_status,
        })
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
            include_states: None,
        };
        let (connection, response) = self
            .request_cloud_query(connection, &group, &request)
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

    /// Retries one idempotent Hub query after evicting a stale shared cloud connection.
    async fn request_cloud_query(
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
                .context("读取云端数据超时")?
            }
            .await;
            match result {
                Ok(response) => return Ok((connection, response)),
                Err(error) if attempt == 0 => {
                    self.invalidate_cloud_connection(connection.stable_id());
                    log::debug!("retry cloud query on a fresh connection: {error}");
                    connection = self
                        .request_cloud_connection(Duration::from_secs(8))
                        .await?;
                }
                Err(error) => {
                    self.invalidate_cloud_connection(connection.stable_id());
                    self.runtime_state.watch_wake.notify_one();
                    return Err(error);
                }
            }
        }

        unreachable!("cloud query retry loop always returns")
    }

    /// Holds cleanup until recreation and the active transfer have both finished.
    async fn lease_outgoing_staging(
        &self,
        group: &GroupSecrets,
        blob: &StoredBlob,
    ) -> Result<(StoredBlob, OutgoingStagingLease)> {
        let mut lease = self.delivery.acquire_staging(&blob.blob_id);
        let staged = self.ensure_outgoing_staging(group, blob).await?;
        if staged.source_path.is_some() && staged.stream_nonce.is_some() {
            lease.release_on_drop(PathBuf::from(&staged.encrypted_path));
        }
        Ok((staged, lease))
    }

    /// Recreates outgoing ciphertext only while an upload or LAN pull actually needs it.
    async fn ensure_outgoing_staging(
        &self,
        group: &GroupSecrets,
        blob: &StoredBlob,
    ) -> Result<StoredBlob> {
        let expected = PathBuf::from(&blob.encrypted_path);
        let rebuild_lock = self.delivery.staging_rebuild_lock(&blob.blob_id);
        let original = blob.clone();
        let source = blob.source_path.as_deref().map(PathBuf::from);
        let nonce = blob.stream_nonce.clone();
        let root = crate::core::paths::resources_dir(&self.app)?.join(SYNC_STAGING_DIR);
        let key = group.content_key_bytes()?;
        let source_is_directory = blob.source_is_directory;
        let rebuilt = tauri::async_runtime::spawn_blocking(move || -> Result<StoredBlob> {
            let _rebuild_guard = rebuild_lock
                .lock()
                .expect("outgoing staging rebuild lock poisoned");
            if expected.is_file() {
                if expected.metadata()?.len() == original.size {
                    return Ok(original);
                }
                fs::remove_file(&expected)?;
            }
            let source = source.ok_or(RemoteBlobUnavailable)?;
            let nonce = nonce.ok_or(RemoteBlobUnavailable)?;
            let rebuilt = if source_is_directory {
                let temporary = tempfile::tempdir()?;
                let archive = temporary.path().join("directory.zip");
                crypto::archive_directory(&source, &archive)?;
                crypto::encrypt_blob_with_stream_nonce(&archive, &root, &key, &nonce)
            } else {
                crypto::encrypt_blob_with_stream_nonce(&source, &root, &key, &nonce)
            }?;
            if rebuilt.blob_id != original.blob_id || rebuilt.size != original.size {
                if rebuilt.blob_id != original.blob_id {
                    let _ = fs::remove_file(&rebuilt.encrypted_path);
                }
                return Err(RemoteBlobUnavailable.into());
            }
            Ok(StoredBlob {
                source_path: original.source_path,
                source_is_directory: original.source_is_directory,
                stream_nonce: original.stream_nonce,
                ..rebuilt
            })
        })
        .await
        .context("sync staging recreation task failed")?
        .map_err(|error| {
            log::debug!("sync staging source is unavailable: {error:#}");
            anyhow::Error::new(RemoteBlobUnavailable)
        })?;
        Ok(rebuilt)
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
        self.device_announcement_for(endpoint, &identity.device_id, &identity.device_name)
    }

    fn device_announcement_for(
        &self,
        endpoint: &Endpoint,
        device_id: &str,
        device_name: &str,
    ) -> DeviceAnnouncement {
        let address = endpoint.addr();
        DeviceAnnouncement {
            device_id: device_id.to_owned(),
            device_name: device_name.to_owned(),
            platform: std::env::consts::OS.into(),
            endpoint_id: endpoint.id().to_string(),
            direct_addresses: address
                .ip_addrs()
                .filter(|address| is_lan_ip(address.ip()))
                .map(ToString::to_string)
                .collect(),
            relay_urls: Vec::new(),
            app_version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }

    /// Announces cloud membership without exporting volatile LAN transport routes.
    fn cloud_device_announcement(&self, endpoint: &Endpoint) -> DeviceAnnouncement {
        self.cloud_device_announcement_for(&self.device_announcement(endpoint))
    }

    fn cloud_device_announcement_for(
        &self,
        announcement: &DeviceAnnouncement,
    ) -> DeviceAnnouncement {
        let mut device = announcement.clone();
        device.direct_addresses.clear();
        device.relay_urls.clear();
        device
    }

    fn update_discovery_metadata(&self, endpoint: &Endpoint, lan_enabled: bool) -> Result<()> {
        let identity = self.identity.snapshot();
        let metadata = match (lan_enabled, identity.group.as_ref()) {
            (true, Some(group)) => Some(DiscoveryMetadata {
                version: JOIN_PROTOCOL_VERSION,
                space_id: discovery_space_id(group)?,
                device_name: identity.device_name.clone(),
                platform: std::env::consts::OS.into(),
                presence_nonce: Some(
                    self.runtime_state
                        .lan_presence_nonce
                        .load(Ordering::Acquire),
                ),
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
            .runtime_state
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
            Ok(JoinResponse::Approved { .. }) => {
                attempt.state = NearbyJoinState::Approved;
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

/// Builds the one-time invitation route; unlike LAN synchronization it may use a configured Relay.
fn pairing_endpoint_addr(code: &PairingCode) -> Result<EndpointAddr> {
    let endpoint_id: EndpointId = code
        .inviter
        .endpoint_id
        .trim()
        .parse()
        .context("配对码中的设备身份无效")?;
    let mut addresses = Vec::new();
    for direct in &code.inviter.direct_addresses {
        let address: SocketAddr = direct.parse().context("配对码中的直连地址无效")?;
        if address.is_ipv4() {
            addresses.push(TransportAddr::Ip(address));
        }
    }
    for relay in &code.inviter.relay_urls {
        addresses.push(TransportAddr::Relay(
            relay.parse().context("配对码中的 Relay 地址无效")?,
        ));
    }
    if addresses.is_empty() {
        bail!("配对码中的设备当前没有可用连接地址");
    }
    Ok(EndpointAddr::from_parts(endpoint_id, addresses))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Establishes an isolated loopback QUIC session without discovery or external relays.
    async fn upload_test_connection() -> (Endpoint, Endpoint, Connection, Connection) {
        let server = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr((Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let client = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr((Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let addr = EndpointAddr::new(server.id()).with_ip_addr(server.bound_sockets()[0]);
        let (outgoing, incoming) = tokio::join!(client.connect(addr, ALPN), async {
            server.accept().await.unwrap().await.unwrap()
        },);
        (client, server, outgoing.unwrap(), incoming)
    }

    #[tokio::test]
    async fn attachment_stream_timeout_preserves_a_responsive_connection() {
        let (_client, _server, connection, incoming) = upload_test_connection().await;
        let peer = tokio::spawn(async move {
            let (held_send, mut held_recv) = incoming.accept_bi().await.unwrap();
            assert!(matches!(
                read_frame(&mut held_recv).await.unwrap(),
                Request::PutManagedBlob { .. }
            ));
            let (mut send, mut recv) = incoming.accept_bi().await.unwrap();
            assert!(matches!(
                read_frame(&mut recv).await.unwrap(),
                Request::CloudHealth
            ));
            write_frame(
                &mut send,
                &Response::CloudHealth {
                    protocol_version: PROTOCOL_VERSION,
                    server_version: "test".into(),
                    server_time_ms: 0,
                },
            )
            .await
            .unwrap();
            send.finish().unwrap();
            let _ = send.stopped().await;
            drop(held_send);
            incoming
        });
        let (started, mut notifications) = mpsc::unbounded_channel();
        let blob = StoredBlob {
            blob_id: "a".repeat(64),
            encrypted_path: "missing-file".into(),
            size: 1,
            source_path: None,
            source_is_directory: false,
            stream_nonce: None,
        };
        let error = upload_blob(
            &connection,
            &GroupSecrets::generate(),
            &blob,
            BlobUploadOptions {
                kind: BlobUploadKind::Content,
                managed_hub_blob: true,
                is_cloud: true,
                language: Language::ZhCN,
                started: Some(started),
                transfer: None,
            },
        )
        .await
        .unwrap_err();
        let incoming = peer.await.unwrap();
        assert!(super::super::upload::is_network_timeout(&error));
        assert_eq!(error.to_string(), "等待服务端允许上传附件超时");
        assert!(connection.close_reason().is_none());
        assert!(notifications.try_recv().is_err());
        drop(incoming);
    }

    #[tokio::test]
    async fn unresponsive_upload_connection_is_closed_after_the_bounded_probe() {
        let (_client, _server, connection, incoming) = upload_test_connection().await;
        let peer = tokio::spawn(async move {
            let (_send, mut recv) = incoming.accept_bi().await.unwrap();
            assert!(matches!(
                read_frame(&mut recv).await.unwrap(),
                Request::CloudHealth
            ));
            incoming.closed().await;
        });
        assert_eq!(
            check_stalled_cloud_upload(&connection).await,
            "failed-closed"
        );
        assert!(connection.close_reason().is_some());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn uploaded_attachment_is_confirmed_and_reports_upload_readiness() {
        let (_client, _server, connection, incoming) = upload_test_connection().await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("encrypted");
        let bytes = vec![7_u8; 65_539];
        tokio::fs::write(&path, &bytes).await.unwrap();
        let expected = bytes.clone();
        let peer = tokio::spawn(async move {
            let (mut send, mut recv) = incoming.accept_bi().await.unwrap();
            assert!(matches!(
                read_frame(&mut recv).await.unwrap(),
                Request::PutManagedBlob { .. }
            ));
            write_frame(&mut send, &Response::BlobReady { size: 0 })
                .await
                .unwrap();
            assert_eq!(recv.read_to_end(100_000).await.unwrap(), expected);
            write_frame(&mut send, &Response::BlobStored).await.unwrap();
            send.finish().unwrap();
            let _ = send.stopped().await;
            incoming
        });
        let (started, mut notifications) = mpsc::unbounded_channel();
        let blob = StoredBlob {
            blob_id: "b".repeat(64),
            encrypted_path: path.to_string_lossy().into_owned(),
            size: bytes.len() as u64,
            source_path: None,
            source_is_directory: false,
            stream_nonce: None,
        };
        upload_blob(
            &connection,
            &GroupSecrets::generate(),
            &blob,
            BlobUploadOptions {
                kind: BlobUploadKind::Content,
                managed_hub_blob: true,
                is_cloud: true,
                language: Language::ZhCN,
                started: Some(started),
                transfer: None,
            },
        )
        .await
        .unwrap();
        let incoming = peer.await.unwrap();
        assert_eq!(notifications.try_recv().unwrap(), blob.blob_id);
        assert!(connection.close_reason().is_none());
        drop(incoming);
    }

    #[test]
    fn cloud_attempt_timeout_uses_the_selected_language() {
        let zh = CloudAttemptTimeout {
            phase: CloudAttemptPhase::OpenStream,
            language: Language::ZhCN,
        };
        let en = CloudAttemptTimeout {
            phase: CloudAttemptPhase::ReadResponseBody,
            language: Language::EnUS,
        };

        assert_eq!(zh.to_string(), "打开云端同步流超时");
        assert_eq!(en.to_string(), "Timed out reading the cloud sync response");
    }

    #[tokio::test]
    async fn cloud_request_write_times_out_without_progress() {
        let (mut writer, _reader) = tokio::io::duplex(1);
        let error = write_cloud_request(
            &mut writer,
            &Request::CloudHealth,
            Duration::from_millis(20),
            Language::ZhCN,
        )
        .await
        .unwrap_err();

        assert!(is_cloud_attempt_timeout(&error), "{error:#}");
        assert_eq!(error.to_string(), "发送云端同步请求超时");
    }

    #[tokio::test]
    async fn cloud_request_writer_preserves_the_protocol_frame() {
        let request = Request::CreateGroup {
            group_id: "group".into(),
            access_token: vec![7; 32],
        };
        let (mut writer, mut reader) = tokio::io::duplex(256);

        write_cloud_request(
            &mut writer,
            &request,
            Duration::from_millis(50),
            Language::ZhCN,
        )
        .await
        .unwrap();
        let decoded = read_frame(&mut reader).await.unwrap();

        assert_eq!(request, decoded);
    }

    #[tokio::test]
    async fn cloud_request_write_resets_timeout_after_progress() {
        let bytes = vec![7_u8; 20];
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let reader_task = tokio::spawn(async move {
            let mut byte = [0_u8; 1];
            for _ in 0..20 {
                reader.read_exact(&mut byte).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        write_all_with_cloud_idle_timeout(
            &mut writer,
            &bytes,
            Duration::from_millis(50),
            Language::ZhCN,
        )
        .await
        .unwrap();
        reader_task.await.unwrap();
    }

    #[tokio::test]
    async fn cloud_response_header_times_out_without_progress() {
        let (_writer, mut reader) = tokio::io::duplex(8);
        let error = read_cloud_response(
            &mut reader,
            Duration::from_millis(20),
            Duration::from_millis(20),
            Language::ZhCN,
        )
        .await
        .unwrap_err();

        assert!(is_cloud_attempt_timeout(&error), "{error:#}");
        assert_eq!(error.to_string(), "等待云端同步响应超时");
    }

    #[tokio::test]
    async fn cloud_response_body_times_out_after_partial_progress() {
        let (mut writer, mut reader) = tokio::io::duplex(8);
        writer.write_all(&4_u32.to_be_bytes()).await.unwrap();
        writer.write_all(&[0_u8]).await.unwrap();

        let error = read_cloud_response(
            &mut reader,
            Duration::from_millis(20),
            Duration::from_millis(20),
            Language::ZhCN,
        )
        .await
        .unwrap_err();

        assert!(is_cloud_attempt_timeout(&error), "{error:#}");
        assert_eq!(error.to_string(), "读取云端同步响应超时");
    }

    #[test]
    fn automatic_file_upload_uses_the_whole_card_size() {
        assert!(permits_automatic_file_upload(10 * 1024 * 1024, 10));
        assert!(!permits_automatic_file_upload(10 * 1024 * 1024 + 1, 10));
        assert!(!permits_automatic_file_upload(1, 0));
    }

    #[test]
    fn local_sync_staging_cleanup_keeps_only_live_managed_files() {
        let directory = tempfile::tempdir().unwrap();
        let live_id = "a".repeat(64);
        let stale_id = "b".repeat(64);
        let live_path = crypto::blob_path(directory.path(), &live_id).unwrap();
        let stale_path = crypto::blob_path(directory.path(), &stale_id).unwrap();
        fs::create_dir_all(live_path.parent().unwrap()).unwrap();
        fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
        fs::write(&live_path, b"live").unwrap();
        fs::write(&stale_path, b"stale").unwrap();
        fs::write(directory.path().join("unmanaged"), b"keep").unwrap();

        let removed =
            remove_unreferenced_sync_staging(directory.path(), &HashSet::from([live_id])).unwrap();

        assert_eq!(removed, 1);
        assert!(live_path.is_file());
        assert!(!stale_path.exists());
        assert!(directory.path().join("unmanaged").is_file());
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
        let mut availability_cursor = 7;
        let mut removed_at_ms = 1_000;

        let changes = advance_cloud_watch_watermarks(
            42,
            7,
            1_000,
            &mut cursor,
            &mut availability_cursor,
            &mut removed_at_ms,
        );

        assert_eq!(changes, None);
        assert_eq!(cursor, 42);
        assert_eq!(availability_cursor, 7);
        assert_eq!(removed_at_ms, 1_000);
    }

    #[test]
    fn cloud_group_watch_reports_only_advanced_watermarks() {
        let mut cursor = 42;
        let mut availability_cursor = 7;
        let mut removed_at_ms = 1_000;

        assert_eq!(
            advance_cloud_watch_watermarks(
                43,
                7,
                900,
                &mut cursor,
                &mut availability_cursor,
                &mut removed_at_ms,
            ),
            Some(CloudWatchChanges {
                events_changed: true,
                availability_changed: false,
                removals_changed: false,
            })
        );
        assert_eq!(cursor, 43);
        assert_eq!(availability_cursor, 7);
        assert_eq!(removed_at_ms, 1_000);
        assert_eq!(
            advance_cloud_watch_watermarks(
                40,
                8,
                1_001,
                &mut cursor,
                &mut availability_cursor,
                &mut removed_at_ms,
            ),
            Some(CloudWatchChanges {
                events_changed: false,
                availability_changed: true,
                removals_changed: true,
            })
        );
        assert_eq!(cursor, 43);
        assert_eq!(availability_cursor, 8);
        assert_eq!(removed_at_ms, 1_001);
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
        ensure_lan_transport_addr(&transport, None).unwrap();
    }

    #[test]
    fn lan_transport_rejects_relay_and_public_ip() {
        let relay = TransportAddr::Relay("https://relay.example.com".parse().unwrap());
        let public = TransportAddr::Ip("8.8.8.8:443".parse().unwrap());

        assert_eq!(
            ensure_lan_transport_addr(&relay, None)
                .unwrap_err()
                .to_string(),
            "局域网同步禁止使用 Relay"
        );
        assert_eq!(
            ensure_lan_transport_addr(&public, None)
                .unwrap_err()
                .to_string(),
            "局域网同步禁止使用公网地址"
        );
    }

    #[test]
    fn lan_path_snapshot_accepts_private_paths_without_a_selected_path() {
        let private = [
            TransportAddr::Ip("192.168.50.84:56786".parse().unwrap()),
            TransportAddr::Ip("10.0.0.8:56786".parse().unwrap()),
        ];

        assert!(lan_path_snapshot_is_usable(None, private, None).unwrap());
    }

    #[test]
    fn lan_path_snapshot_waits_when_unselected_paths_are_not_all_private() {
        let mixed = [
            TransportAddr::Ip("192.168.50.84:56786".parse().unwrap()),
            TransportAddr::Relay("https://relay.example.com".parse().unwrap()),
        ];
        let public = [TransportAddr::Ip("8.8.8.8:443".parse().unwrap())];

        assert!(!lan_path_snapshot_is_usable(None, mixed, None).unwrap());
        assert!(!lan_path_snapshot_is_usable(None, public, None).unwrap());
        assert!(!lan_path_snapshot_is_usable(None, [], None).unwrap());
    }

    #[test]
    fn lan_path_snapshot_rejects_a_selected_non_lan_path() {
        let relay = TransportAddr::Relay("https://relay.example.com".parse().unwrap());

        assert_eq!(
            lan_path_snapshot_is_usable(Some(relay.clone()), [relay], None)
                .unwrap_err()
                .to_string(),
            "局域网同步禁止使用 Relay"
        );
    }

    #[test]
    fn lan_path_snapshot_rejects_an_unannounced_private_path() {
        let discovered = "192.168.182.147".parse().unwrap();
        let vpn = TransportAddr::Ip("10.159.23.179:55916".parse().unwrap());

        assert_eq!(
            lan_path_snapshot_is_usable(
                Some(vpn.clone()),
                [vpn],
                Some(&HashSet::from([discovered])),
            )
            .unwrap_err()
            .to_string(),
            "局域网连接未使用当前发现地址"
        );
    }

    #[test]
    fn lan_path_snapshot_allows_a_new_port_on_the_discovered_ip() {
        let discovered = "192.168.182.147".parse().unwrap();
        let migrated = TransportAddr::Ip("192.168.182.147:51403".parse().unwrap());

        assert!(lan_path_snapshot_is_usable(
            Some(migrated.clone()),
            [migrated],
            Some(&HashSet::from([discovered])),
        )
        .unwrap());
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
            source_path: None,
            source_is_directory: false,
            stream_nonce: None,
        };
        let source_icon = StoredBlob {
            blob_id: "source-icon".into(),
            encrypted_path: "source-icon.bin".into(),
            size: 20,
            source_path: None,
            source_is_directory: false,
            stream_nonce: None,
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
    fn only_source_icons_are_marked_optional_on_existing_encrypted_events() {
        let key = [7; 32];
        let content_id = "a".repeat(64);
        let icon_id = "b".repeat(64);
        let mut envelope = ClipboardEnvelope {
            version: 1,
            item: SyncedClipboardItem {
                kind: "image".into(),
                sub_kind: None,
                content: String::new(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(128),
                width: Some(1),
                height: Some(1),
                is_sensitive: false,
                source_platform: "windows".into(),
                created_at_ms: 100,
                sync_key: "c".repeat(64),
                updated_at_ms: Some(100),
                source_revision: Some("revision".into()),
            },
            blobs: vec![BlobManifest {
                blob_id: content_id.clone(),
                name: "image.png".into(),
                original_size: 128,
                encrypted_size: 192,
                role: BlobRole::Image,
                file_index: None,
                is_directory_archive: false,
                stream_commitment: false,
            }],
            source_app: Some(SourceAppRef {
                version: 1,
                source_key: "d".repeat(64),
                platform: "windows".into(),
                display_name: "Example".into(),
                icon: Some(SourceIconRef {
                    icon_hash: "e".repeat(64),
                    blob_id: icon_id.clone(),
                    original_size: 128,
                    encrypted_size: 192,
                }),
                accent_start: None,
                accent_end: None,
            }),
            content_identity: Some("c".repeat(64)),
        };
        let mut manifest = envelope.blobs[0].clone();
        for (index, kind) in ["text", "image", "files"].into_iter().enumerate() {
            envelope.item.kind = kind.into();
            manifest.role = if kind == "files" {
                BlobRole::File
            } else {
                BlobRole::Image
            };
            manifest.file_index = (kind == "files").then_some(0);
            envelope.blobs = if kind == "text" {
                Vec::new()
            } else {
                vec![manifest.clone()]
            };
            let expected_blob_ids = envelope
                .blobs
                .iter()
                .map(|blob| blob.blob_id.clone())
                .chain(std::iter::once(icon_id.clone()))
                .collect::<Vec<_>>();
            let mut event = crypto::encrypt_event(
                &key,
                "event".into(),
                "device".into(),
                index as u64 + 1,
                100,
                &envelope,
            )
            .unwrap();
            event.blob_ids = expected_blob_ids.clone();
            let ciphertext = event.ciphertext.clone();
            let mut pending = vec![StoredSyncEvent { cursor: 1, event }];

            mark_optional_source_icon_refs(&mut pending, &key);

            assert_eq!(pending[0].event.optional_blob_ids, vec![icon_id.clone()]);
            assert_eq!(pending[0].event.blob_ids, expected_blob_ids);
            assert_eq!(pending[0].event.ciphertext, ciphertext);
        }

        envelope
            .source_app
            .as_mut()
            .unwrap()
            .icon
            .as_mut()
            .unwrap()
            .blob_id = content_id.clone();
        let mut event =
            crypto::encrypt_event(&key, "event".into(), "device".into(), 4, 100, &envelope)
                .unwrap();
        event.blob_ids = vec![content_id];
        let mut pending = vec![StoredSyncEvent { cursor: 1, event }];

        mark_optional_source_icon_refs(&mut pending, &key);

        assert!(pending[0].event.optional_blob_ids.is_empty());
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
            kind: ecopaste_sync_protocol::SyncEventKind::Content,
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
            retention_key: None,
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
                sync_key: "hash".into(),
                updated_at_ms: Some(1_700_000_000_000),
                source_revision: Some("revision".into()),
            },
            blobs: Vec::new(),
            source_app: None,
            content_identity: Some("a".repeat(64)),
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

    #[tokio::test]
    async fn external_download_commit_replaces_existing_file_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("download.bin");
        let temporary = directory.path().join("download.part");
        fs::write(&destination, b"old").unwrap();
        fs::write(&temporary, b"new").unwrap();

        let commit = commit_external_download(&temporary, &destination)
            .await
            .unwrap();
        commit.finalize().await;

        assert_eq!(fs::read(&destination).unwrap(), b"new");
        assert!(!temporary.exists());
        assert_eq!(
            fs::read_dir(directory.path())
                .unwrap()
                .filter_map(std::result::Result::ok)
                .count(),
            1
        );
    }

    #[test]
    fn retired_sync_blob_path_is_deleted_instead_of_ignored() {
        let directory = tempfile::tempdir().unwrap();
        let legacy = directory.path().join("sync-blobs");
        fs::create_dir(&legacy).unwrap();
        fs::write(legacy.join("ciphertext"), b"legacy").unwrap();

        remove_legacy_sync_blob_path(&legacy).unwrap();

        assert!(!legacy.exists());
    }

    #[tokio::test]
    async fn failed_multi_file_download_rolls_back_committed_outputs() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("one.txt");
        let folder = directory.path().join("two");
        let temporary_file = directory.path().join("one.part");
        let temporary_folder = directory.path().join("two.part");
        fs::write(&temporary_file, b"one").unwrap();
        fs::create_dir(&temporary_folder).unwrap();
        fs::write(temporary_folder.join("nested.txt"), b"two").unwrap();
        let file_commit = commit_external_download(&temporary_file, &file)
            .await
            .unwrap();
        let folder_commit = commit_external_download(&temporary_folder, &folder)
            .await
            .unwrap();

        rollback_external_downloads(vec![file_commit, folder_commit]).await;

        assert!(!file.exists());
        assert!(!folder.exists());
    }

    #[tokio::test]
    async fn canceled_external_download_restores_replaced_destination() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("download.bin");
        let temporary = directory.path().join("download.part");
        fs::write(&destination, b"old").unwrap();
        fs::write(&temporary, b"new").unwrap();
        let commit = commit_external_download(&temporary, &destination)
            .await
            .unwrap();

        commit.rollback().await;

        assert_eq!(fs::read(&destination).unwrap(), b"old");
        assert_eq!(
            fs::read_dir(directory.path())
                .unwrap()
                .filter_map(std::result::Result::ok)
                .count(),
            1
        );
    }

    #[test]
    fn only_explicit_not_found_failures_expire_remote_files() {
        let unavailable = anyhow::Error::new(RemoteBlobTransportFailure::from_error(
            RemoteBlobUnavailable.into(),
        ));
        let unavailable = conclude_remote_blob_failure(unavailable, true);
        assert!(unavailable
            .downcast_ref::<RemoteBlobUnavailable>()
            .is_some());

        let transport = anyhow::Error::new(RemoteBlobTransportFailure::from_error(
            anyhow::anyhow!("connection closed"),
        ));
        let transport = conclude_remote_blob_failure(transport, true);
        assert!(transport
            .downcast_ref::<RemoteBlobTransportFailure>()
            .is_some());

        let mixed = anyhow::Error::new(RemoteBlobTransportFailure::from_error(
            RemoteBlobUnavailable.into(),
        ));
        let mixed = conclude_remote_blob_failure(mixed, false);
        assert!(mixed.downcast_ref::<RemoteBlobTransportFailure>().is_some());
    }
}
