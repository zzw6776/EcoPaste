use super::*;

#[derive(Clone)]
pub(super) struct PreparedSourceIconCache {
    pub(super) icon_ref: SourceIconRef,
    pub(super) blob: StoredBlob,
}

#[derive(Clone, Debug)]
pub(super) struct ServingCatchupEntry {
    pub(super) device_id: String,
    pub(super) device_name: String,
    pub(super) served_items: u64,
    pub(super) remaining_items: u64,
    pub(super) served_attachments: u64,
    pub(super) active_window: bool,
    pub(super) last_activity: Instant,
}

/// Owns all volatile connection, worker and channel state for one sync manager.
pub(super) struct RuntimeCoordinator {
    pub(super) update_notification_pending: Arc<AtomicBool>,
    pub(super) storage_suspended: watch::Sender<bool>,
    pub(super) storage_epoch: AtomicU64,
    pub(super) endpoint: Mutex<Option<EndpointRuntime>>,
    pub(super) lan_transfer_wake: Notify,
    pub(super) cloud_transfer_wake: Notify,
    pub(super) watch_wake: Notify,
    pub(super) source_asset_wake: Notify,
    /// `0` = inline/image worker, `1` = backfill files worker, `2` = realtime files worker.
    pub(super) local_outbox_wake: [Notify; 3],
    pub(super) attachment_wakes: [Notify; 4],
    pub(super) exchange_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    pub(super) sync_key_locks:
        std::sync::Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    pub(super) nearby_wake: Notify,
    pub(super) cleanup_ready: Notify,
    /// Highest cloud Hub sequence observed via `WatchCloud`, enabling batch-0 pull preemption.
    pub(super) cloud_observed_latest_seq: AtomicU64,
    /// Session-scoped memoization for `(group_id, icon_file)` encryption during backfill.
    pub(super) prepared_source_icons: RwLock<HashMap<(String, String), PreparedSourceIconCache>>,
    /// Session-scoped set of `(group_id, blob_id)` icons already confirmed stored on Cloud Hub.
    pub(super) uploaded_source_icons: RwLock<HashSet<(String, String)>>,
    /// Per-connection memoization of `(connection_id, blob_id)` checked via `CheckCloudBlobs`.
    pub(super) checked_cloud_blobs: RwLock<HashSet<(usize, String)>>,
    pub(super) lan_cycle_lock: AsyncRwLock<()>,
    pub(super) cloud_cycle_lock: Mutex<()>,
    pub(super) cloud_connect_lock: Mutex<()>,
    pub(super) cloud_data_connect_lock: Mutex<()>,
    pub(super) cloud_connection_ready: Notify,
    pub(super) cloud_session_lock: Mutex<()>,
    pub(super) cloud_force_reconnect: AtomicBool,
    pub(super) inbound_stream_admission: Arc<Semaphore>,
    pub(super) lan_status: RwLock<SyncChannelStatus>,
    pub(super) cloud_status: RwLock<SyncChannelStatus>,
    pub(super) cloud_watch_status: RwLock<SyncChannelStatus>,
    pub(super) cloud_path: RwLock<ConnectionRoute>,
    pub(super) cloud_server_version: RwLock<Option<String>>,
    /// Dedicated low-latency control QUIC connection to Cloud Hub (`CloudSyncItems`, `WatchCloud`).
    pub(super) cloud_connection: RwLock<Option<Connection>>,
    /// Lazy secondary QUIC connection dedicated exclusively to Cloud blob uploads & downloads.
    pub(super) cloud_data_connection: RwLock<Option<Connection>>,
    pub(super) cloud_data_active_transfers: AtomicUsize,
    pub(super) cloud_data_last_active_at: RwLock<Option<Instant>>,
    pub(super) cloud_session: RwLock<Option<CloudSessionState>>,
    /// Dedicated low-latency control QUIC connections to LAN peers (`SyncItems`, `AckLanItems`).
    pub(super) lan_connections: RwLock<HashMap<String, Connection>>,
    /// Lazy secondary QUIC connections dedicated exclusively to LAN blob pulls (`GetBlob`).
    pub(super) lan_data_connections: RwLock<HashMap<String, Connection>>,
    pub(super) lan_served_connections: RwLock<HashSet<usize>>,
    pub(super) lan_peer_actors: RwLock<HashMap<String, Arc<LanPeerActor>>>,
    pub(super) lan_peer_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    #[cfg(target_os = "android")]
    pub(super) android_lan_multicast_interfaces: RwLock<BTreeSet<Ipv4Addr>>,
    pub(super) lan_presence_nonce: AtomicU64,
    pub(super) nearby_devices: RwLock<HashMap<String, NearbyDeviceEntry>>,
    pub(super) inbound_metadata_peak_bp: AtomicU64,
    pub(super) inbound_attachment_completed: AtomicU64,
    pub(super) serving_catchup_sessions: RwLock<HashMap<String, ServingCatchupEntry>>,
}

impl RuntimeCoordinator {
    pub(super) fn new(presence_nonce: u64) -> Self {
        Self {
            update_notification_pending: Arc::new(AtomicBool::new(false)),
            storage_suspended: watch::channel(false).0,
            storage_epoch: AtomicU64::new(0),
            endpoint: Mutex::new(None),
            lan_transfer_wake: Notify::new(),
            cloud_transfer_wake: Notify::new(),
            watch_wake: Notify::new(),
            source_asset_wake: Notify::new(),
            local_outbox_wake: std::array::from_fn(|_| Notify::new()),
            attachment_wakes: std::array::from_fn(|_| Notify::new()),
            exchange_locks: Mutex::new(HashMap::new()),
            sync_key_locks: std::sync::Mutex::new(HashMap::new()),
            nearby_wake: Notify::new(),
            cleanup_ready: Notify::new(),
            cloud_observed_latest_seq: AtomicU64::new(0),
            prepared_source_icons: RwLock::new(HashMap::new()),
            uploaded_source_icons: RwLock::new(HashSet::new()),
            checked_cloud_blobs: RwLock::new(HashSet::new()),
            lan_cycle_lock: AsyncRwLock::new(()),
            cloud_cycle_lock: Mutex::new(()),
            cloud_connect_lock: Mutex::new(()),
            cloud_data_connect_lock: Mutex::new(()),
            cloud_connection_ready: Notify::new(),
            cloud_session_lock: Mutex::new(()),
            cloud_force_reconnect: AtomicBool::new(false),
            inbound_stream_admission: Arc::new(Semaphore::new(128)),
            lan_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Idle)),
            cloud_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Disabled)),
            cloud_watch_status: RwLock::new(SyncChannelStatus::new(SyncChannelState::Disabled)),
            cloud_path: RwLock::new(ConnectionRoute::default()),
            cloud_server_version: RwLock::new(None),
            cloud_connection: RwLock::new(None),
            cloud_data_connection: RwLock::new(None),
            cloud_data_active_transfers: AtomicUsize::new(0),
            cloud_data_last_active_at: RwLock::new(None),
            cloud_session: RwLock::new(None),
            lan_connections: RwLock::new(HashMap::new()),
            lan_data_connections: RwLock::new(HashMap::new()),
            lan_served_connections: RwLock::new(HashSet::new()),
            lan_peer_actors: RwLock::new(HashMap::new()),
            lan_peer_locks: RwLock::new(HashMap::new()),
            #[cfg(target_os = "android")]
            android_lan_multicast_interfaces: RwLock::new(BTreeSet::new()),
            lan_presence_nonce: AtomicU64::new(presence_nonce),
            nearby_devices: RwLock::new(HashMap::new()),
            inbound_metadata_peak_bp: AtomicU64::new(0),
            inbound_attachment_completed: AtomicU64::new(0),
            serving_catchup_sessions: RwLock::new(HashMap::new()),
        }
    }

    pub(super) fn clear_group_caches(&self) {
        self.cloud_observed_latest_seq.store(0, Ordering::Release);
        self.inbound_metadata_peak_bp.store(0, Ordering::Release);
        self.inbound_attachment_completed
            .store(0, Ordering::Release);
        self.prepared_source_icons
            .write()
            .expect("prepared source icons poisoned")
            .clear();
        self.uploaded_source_icons
            .write()
            .expect("uploaded source icons poisoned")
            .clear();
        self.checked_cloud_blobs
            .write()
            .expect("checked cloud blobs poisoned")
            .clear();
    }

    /// Acquires an exclusive per-`sync_key` async lock to serialize CRDT merges, local snapshot
    /// commits, and blob-pull application for the same logical clipboard item.
    pub(super) async fn lock_sync_key(&self, sync_key: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let mutex = {
            let mut locks = self.sync_key_locks.lock().expect("sync key locks poisoned");
            locks.retain(|_, weak| weak.strong_count() > 0);
            if let Some(existing) = locks.get(sync_key).and_then(std::sync::Weak::upgrade) {
                existing
            } else {
                let created = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(sync_key.to_owned(), Arc::downgrade(&created));
                created
            }
        };
        mutex.lock_owned().await
    }

    /// Returns the sorted device IDs of all LAN peers with an active open QUIC control connection.
    pub(super) fn online_lan_peer_ids(&self) -> Vec<String> {
        let mut peers = self
            .lan_connections
            .read()
            .expect("LAN connections poisoned")
            .iter()
            .filter(|(_, connection)| connection.close_reason().is_none())
            .map(|(device_id, _)| device_id.clone())
            .collect::<Vec<_>>();
        peers.sort();
        peers
    }
}
