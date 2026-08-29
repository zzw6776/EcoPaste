use ecopaste_sync_protocol::PeerAnnouncement;
use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::settings::CloudRelayMode;

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
    #[n(13)]
    pub updated_at_ms: Option<i64>,
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
pub struct LinkedSyncEvent {
    pub item_id: String,
    pub stored: StoredSyncEvent,
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
    pub lan_enabled: bool,
    pub cloud_enabled: bool,
    pub cloud_relay_mode: CloudRelayMode,
    pub cloud_relay_auth_configured: bool,
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
    pub cloud_watch: SyncChannelStatus,
    pub cloud_connected_address: Option<String>,
    pub cloud_transport: Option<String>,
    pub cloud_server_version: Option<String>,
    pub peers: Vec<SyncPeerStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncChannelState {
    Disabled,
    Idle,
    Connecting,
    Online,
    Degraded,
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
    pub image_path: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Encode, Decode)]
    #[cbor(map)]
    struct LegacySyncedClipboardItem {
        #[n(0)]
        kind: String,
        #[n(1)]
        sub_kind: Option<String>,
        #[n(2)]
        content: String,
        #[n(3)]
        search_text: Option<String>,
        #[n(4)]
        summary: Option<String>,
        #[n(5)]
        file_types: Option<String>,
        #[n(6)]
        size: Option<i64>,
        #[n(7)]
        width: Option<i64>,
        #[n(8)]
        height: Option<i64>,
        #[n(9)]
        is_sensitive: bool,
        #[n(10)]
        source_platform: String,
        #[n(11)]
        created_at_ms: i64,
        #[n(12)]
        content_hash: String,
    }

    #[test]
    fn synced_item_defaults_missing_updated_at_for_legacy_events() {
        let legacy = LegacySyncedClipboardItem {
            kind: "text".into(),
            sub_kind: None,
            content: "hello".into(),
            search_text: None,
            summary: Some("hello".into()),
            file_types: None,
            size: Some(5),
            width: None,
            height: None,
            is_sensitive: false,
            source_platform: "macos".into(),
            created_at_ms: 123,
            content_hash: "hash".into(),
        };

        let encoded = minicbor::to_vec(legacy).unwrap();
        let decoded: SyncedClipboardItem = minicbor::decode(&encoded).unwrap();

        assert_eq!(decoded.created_at_ms, 123);
        assert_eq!(decoded.updated_at_ms, None);
    }

    #[test]
    fn legacy_decoder_ignores_updated_at_from_new_events() {
        let current = SyncedClipboardItem {
            kind: "text".into(),
            sub_kind: None,
            content: "hello".into(),
            search_text: None,
            summary: Some("hello".into()),
            file_types: None,
            size: Some(5),
            width: None,
            height: None,
            is_sensitive: false,
            source_platform: "macos".into(),
            created_at_ms: 123,
            content_hash: "hash".into(),
            updated_at_ms: Some(456),
        };

        let encoded = minicbor::to_vec(current).unwrap();
        let decoded: LegacySyncedClipboardItem = minicbor::decode(&encoded).unwrap();

        assert_eq!(decoded.created_at_ms, 123);
        assert_eq!(decoded.content_hash, "hash");
    }
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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NearbySyncDevice {
    pub device_name: String,
    pub platform: String,
    pub endpoint_id: String,
    pub direct_addresses: Vec<String>,
    pub relay_urls: Vec<String>,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NearbySyncSpace {
    pub space_id: String,
    pub same_group: bool,
    pub devices: Vec<NearbySyncDevice>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum NearbyJoinState {
    Pending,
    Approved,
    Rejected,
    Expired,
    Error,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NearbyJoinAttempt {
    pub request_id: String,
    pub target_device_name: String,
    pub comparison_code: String,
    pub state: NearbyJoinState,
    pub expires_at: String,
    pub pairing_code: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingJoinRequest {
    pub request_id: String,
    pub device_id: String,
    pub device_name: String,
    pub platform: String,
    pub endpoint_id: String,
    pub comparison_code: String,
    pub previously_removed: bool,
    pub expires_at: String,
}

#[derive(Debug, Clone)]
pub struct SyncPeer {
    pub announcement: PeerAnnouncement,
    pub pull_cursor: u64,
}
