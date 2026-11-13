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
    #[n(3)]
    #[cbor(default)]
    pub source_app: Option<SourceAppRef>,
    #[n(4)]
    #[cbor(default)]
    pub content_identity: Option<String>,
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
    pub sync_key: String,
    #[n(13)]
    pub updated_at_ms: Option<i64>,
    #[n(14)]
    #[cbor(default)]
    pub source_revision: Option<String>,
    #[n(15)]
    #[cbor(default)]
    pub source_device_id: Option<String>,
    #[n(16)]
    #[cbor(default)]
    pub source_device_name: Option<String>,
    #[n(17)]
    #[cbor(default)]
    pub source_updated_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct SyncedItemStatePatch {
    #[n(0)]
    pub sync_key: String,
    #[n(1)]
    #[cbor(default)]
    pub is_favorite: Option<bool>,
    #[n(2)]
    #[cbor(default)]
    pub is_pinned: Option<bool>,
    #[n(3)]
    #[cbor(default)]
    pub note: Option<String>,
    #[n(4)]
    #[cbor(default)]
    pub note_changed: bool,
    #[n(5)]
    #[cbor(default)]
    pub is_present: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecryptedSyncEvent {
    Clipboard(Box<ClipboardEnvelope>),
    ItemState(SyncedItemStatePatch),
}

impl DecryptedSyncEvent {
    pub fn sync_key(&self) -> &str {
        match self {
            Self::Clipboard(envelope) => &envelope.item.sync_key,
            Self::ItemState(patch) => &patch.sync_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct SourceAppRef {
    #[n(0)]
    pub version: u16,
    #[n(1)]
    pub source_key: String,
    #[n(2)]
    pub platform: String,
    #[n(3)]
    pub display_name: String,
    #[n(4)]
    pub icon: Option<SourceIconRef>,
    #[n(5)]
    pub accent_start: Option<String>,
    #[n(6)]
    pub accent_end: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct SourceIconRef {
    #[n(0)]
    pub icon_hash: String,
    #[n(1)]
    pub blob_id: String,
    #[n(2)]
    pub original_size: u64,
    #[n(3)]
    pub encrypted_size: u64,
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
    #[n(7)]
    #[cbor(default)]
    pub stream_commitment: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum BlobRole {
    #[n(0)]
    Image,
    #[n(1)]
    File,
}

#[cfg(test)]
mod blob_manifest_tests {
    use super::*;

    #[derive(Encode)]
    #[cbor(map)]
    struct LegacyBlobManifest {
        #[n(0)]
        blob_id: String,
        #[n(1)]
        name: String,
        #[n(2)]
        original_size: u64,
        #[n(3)]
        encrypted_size: u64,
        #[n(4)]
        role: BlobRole,
        #[n(5)]
        file_index: Option<u32>,
        #[n(6)]
        is_directory_archive: bool,
    }

    #[test]
    fn legacy_blob_manifest_defaults_to_ciphertext_identity() {
        let encoded = minicbor::to_vec(LegacyBlobManifest {
            blob_id: "a".repeat(64),
            name: "example.bin".into(),
            original_size: 4,
            encrypted_size: 47,
            role: BlobRole::File,
            file_index: Some(0),
            is_directory_archive: false,
        })
        .unwrap();
        let decoded: BlobManifest = minicbor::decode(&encoded).unwrap();

        assert!(!decoded.stream_commitment);
    }
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
    pub source_path: Option<String>,
    pub source_is_directory: bool,
    pub stream_nonce: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContentObject {
    pub plaintext_hash: String,
    pub blob_id: String,
    pub stream_nonce: Vec<u8>,
    pub original_size: u64,
    pub encrypted_size: u64,
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
    pub next_retry_at: Option<String>,
}

impl SyncChannelStatus {
    pub fn new(state: SyncChannelState) -> Self {
        Self {
            state,
            last_attempt_at: None,
            last_success_at: None,
            last_error: None,
            next_retry_at: None,
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
pub enum SyncItemOrigin {
    Local,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncContentAvailability {
    None,
    MetadataOnly,
    Partial,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncLocalContentState {
    Managed,
    External,
    ExternalMissing,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncItemActivity {
    Idle,
    Queued,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncItemAction {
    None,
    Upload,
    Download,
    Retry,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncItemChannelStatus {
    pub availability: SyncContentAvailability,
    pub activity: SyncItemActivity,
    pub action: SyncItemAction,
    pub delivered_targets: u64,
    pub content_targets: u64,
    pub total_targets: u64,
    pub last_error: Option<String>,
}

impl SyncItemChannelStatus {
    pub fn idle() -> Self {
        Self {
            availability: SyncContentAvailability::None,
            activity: SyncItemActivity::Idle,
            action: SyncItemAction::None,
            delivered_targets: 0,
            content_targets: 0,
            total_targets: 0,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncItemStatus {
    pub item_id: String,
    pub origin: SyncItemOrigin,
    pub local_state: SyncLocalContentState,
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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudDeviceStatus {
    pub device_id: String,
    pub device_name: String,
    pub platform: String,
    pub app_version: String,
    pub online: bool,
    pub current: bool,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudHubStatus {
    pub used_bytes: u64,
    pub max_bytes: u64,
    pub event_count: u64,
    pub blob_count: u64,
    pub oldest_event_at: Option<String>,
    pub retention_days: u32,
    pub max_events: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudDeviceDirectory {
    pub devices: Vec<CloudDeviceStatus>,
    pub hub_status: Option<CloudHubStatus>,
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

    #[derive(Encode, Decode)]
    #[cbor(map)]
    struct LegacyClipboardEnvelope {
        #[n(0)]
        version: u16,
        #[n(1)]
        item: LegacySyncedClipboardItem,
        #[n(2)]
        blobs: Vec<BlobManifest>,
    }

    fn legacy_item() -> LegacySyncedClipboardItem {
        LegacySyncedClipboardItem {
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
        }
    }

    #[test]
    fn synced_item_defaults_missing_updated_at_for_legacy_events() {
        let legacy = legacy_item();

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
            sync_key: "hash".into(),
            updated_at_ms: Some(456),
            source_revision: Some("revision".into()),
            source_device_id: None,
            source_device_name: None,
            source_updated_at_ms: None,
        };

        let encoded = minicbor::to_vec(current).unwrap();
        let decoded: LegacySyncedClipboardItem = minicbor::decode(&encoded).unwrap();

        assert_eq!(decoded.created_at_ms, 123);
        assert_eq!(decoded.content_hash, "hash");
    }

    #[test]
    fn current_envelope_defaults_source_app_from_legacy_event() {
        let legacy = LegacyClipboardEnvelope {
            version: 1,
            item: legacy_item(),
            blobs: Vec::new(),
        };

        let encoded = minicbor::to_vec(legacy).unwrap();
        let decoded: ClipboardEnvelope = minicbor::decode(&encoded).unwrap();

        assert_eq!(decoded.item.content, "hello");
        assert_eq!(decoded.source_app, None);
        assert_eq!(decoded.content_identity, None);
    }

    #[test]
    fn legacy_envelope_ignores_source_app_from_current_event() {
        let current = ClipboardEnvelope {
            version: 1,
            item: SyncedClipboardItem {
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
                sync_key: "hash".into(),
                updated_at_ms: Some(456),
                source_revision: Some("revision".into()),
                source_device_id: None,
                source_device_name: None,
                source_updated_at_ms: None,
            },
            blobs: Vec::new(),
            source_app: Some(SourceAppRef {
                version: 1,
                source_key: "a".repeat(64),
                platform: "macos".into(),
                display_name: "Example".into(),
                icon: None,
                accent_start: Some("#112233".into()),
                accent_end: Some("#445566".into()),
            }),
            content_identity: Some("b".repeat(64)),
        };

        let encoded = minicbor::to_vec(current).unwrap();
        let decoded: LegacyClipboardEnvelope = minicbor::decode(&encoded).unwrap();

        assert_eq!(decoded.item.content, "hello");
        assert!(decoded.blobs.is_empty());
    }
}

impl SyncItemStatus {
    pub fn idle(item_id: String) -> Self {
        let idle = SyncItemChannelStatus::idle();
        Self {
            item_id,
            origin: SyncItemOrigin::Local,
            local_state: SyncLocalContentState::Managed,
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
