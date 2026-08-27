use ecopaste_sync_protocol::PeerAnnouncement;
use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct ClipboardEnvelope {
    #[n(0)]
    pub version: u16,
    #[n(1)]
    pub item: SyncedClipboardItem,
    #[n(2)]
    pub blobs: Vec<BlobManifest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct SyncedClipboardItem {
    #[n(0)]
    pub kind: String,
    #[n(1)]
    pub sub_kind: Option<String>,
    #[n(2)]
    pub content: String,
    #[n(3)]
    pub search_text: Option<String>,
    #[n(4)]
    pub summary: Option<String>,
    #[n(5)]
    pub file_types: Option<String>,
    #[n(6)]
    pub size: Option<i64>,
    #[n(7)]
    pub width: Option<i64>,
    #[n(8)]
    pub height: Option<i64>,
    #[n(9)]
    pub is_sensitive: bool,
    #[n(10)]
    pub source_platform: String,
    #[n(11)]
    pub created_at_ms: i64,
    #[n(12)]
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct BlobManifest {
    #[n(0)]
    pub blob_id: String,
    #[n(1)]
    pub name: String,
    #[n(2)]
    pub original_size: u64,
    #[n(3)]
    pub encrypted_size: u64,
    #[n(4)]
    pub role: BlobRole,
    #[n(5)]
    pub file_index: Option<u32>,
    #[n(6)]
    pub is_directory_archive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum BlobRole {
    #[n(0)]
    Image,
    #[n(1)]
    File,
}

#[derive(Debug, Clone)]
pub struct StoredSyncEvent {
    pub cursor: u64,
    pub event: ecopaste_sync_protocol::EncryptedEvent,
}

#[derive(Debug, Clone)]
pub struct StoredBlob {
    pub blob_id: String,
    pub encrypted_path: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatus {
    pub enabled: bool,
    pub cloud_enabled: bool,
    pub paired: bool,
    pub device_id: String,
    pub device_name: String,
    pub group_id: Option<String>,
    pub endpoint_id: String,
    pub cloud_endpoint_id: String,
    pub cloud_direct_addresses: Vec<String>,
    pub cloud_relay_urls: Vec<String>,
    pub pending_events: u64,
    pub pending_manual_items: u64,
    pub peer_count: u64,
    pub last_success_at: Option<String>,
    pub lan: SyncChannelStatus,
    pub cloud: SyncChannelStatus,
    pub peers: Vec<SyncPeerStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncChannelState {
    Disabled,
    Idle,
    Connecting,
    Online,
    Error,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncChannelStatus {
    pub state: SyncChannelState,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error: Option<String>,
}

impl SyncChannelStatus {
    pub fn new(state: SyncChannelState) -> Self {
        Self {
            state,
            last_attempt_at: None,
            last_success_at: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPeerStatus {
    pub device_id: String,
    pub device_name: String,
    pub platform: String,
    pub endpoint_id: String,
    pub direct_addresses: Vec<String>,
    pub relay_urls: Vec<String>,
    pub state: SyncChannelState,
    pub connected_address: Option<String>,
    pub transport: Option<String>,
    pub last_seen_at: Option<String>,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncTarget {
    Lan,
    Cloud,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncItemState {
    Idle,
    Syncing,
    Success,
    Manual,
    Error,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncItemChannelStatus {
    pub state: SyncItemState,
    pub delivered_targets: u64,
    pub total_targets: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncItemStatus {
    pub item_id: String,
    pub lan: SyncItemChannelStatus,
    pub cloud: SyncItemChannelStatus,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudRecord {
    pub cursor: u64,
    pub event_id: String,
    pub device_name: String,
    pub kind: String,
    pub preview: String,
    pub file_count: u32,
    pub total_size: u64,
    pub created_at: String,
    pub is_sensitive: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudRecordPage {
    pub records: Vec<CloudRecord>,
    pub next_before_cursor: Option<u64>,
    pub total: u64,
}

impl SyncItemStatus {
    pub fn idle(item_id: String) -> Self {
        let idle = SyncItemChannelStatus {
            state: SyncItemState::Idle,
            delivered_targets: 0,
            total_targets: 0,
            last_error: None,
        };
        Self {
            item_id,
            lan: idle.clone(),
            cloud: idle,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPairingPreview {
    pub inviter_device_name: String,
    pub same_group: bool,
}

#[derive(Debug, Clone)]
pub struct SyncPeer {
    pub announcement: PeerAnnouncement,
    pub pull_cursor: u64,
}
