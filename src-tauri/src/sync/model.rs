use ecopaste_sync_protocol::PeerAnnouncement;
use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::settings::CloudRelayMode;

pub const SNAPSHOT_VERSION: u16 = 1;

/// Lamport clock + device-id tie-breaker for one independently mergeable CRDT field.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
#[cbor(map)]
pub struct FieldVersion {
    #[n(0)]
    pub seq: u64,
    #[n(1)]
    pub device_id: String,
}

impl FieldVersion {
    pub fn new(seq: u64, device_id: impl Into<String>) -> Self {
        Self {
            seq,
            device_id: device_id.into(),
        }
    }
}

/// Complete encrypted per-item snapshot carried inside `SyncItemWrite` / `SyncItemRecord`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct SyncedItemSnapshot {
    #[n(0)]
    pub version: u16,
    #[n(1)]
    pub sync_key: String,
    #[n(2)]
    pub content_version: FieldVersion,
    #[n(3)]
    pub presence_version: FieldVersion,
    #[n(4)]
    pub favorite_version: FieldVersion,
    #[n(5)]
    pub pinned_version: FieldVersion,
    #[n(6)]
    pub note_version: FieldVersion,
    #[n(7)]
    pub is_present: bool,
    #[n(8)]
    pub is_favorite: bool,
    #[n(9)]
    pub is_pinned: bool,
    #[n(10)]
    pub note: Option<String>,
    #[n(11)]
    pub item: Option<SyncedClipboardItem>,
    #[n(12)]
    pub blobs: Vec<BlobManifest>,
    #[n(13)]
    #[cbor(default)]
    pub source_app: Option<SourceAppRef>,
    #[n(14)]
    #[cbor(default)]
    pub content_identity: Option<String>,
}

impl SyncedItemSnapshot {
    pub fn deleted(&self) -> bool {
        !self.is_present
    }

    pub fn protected(&self) -> bool {
        self.is_present && (self.is_favorite || self.is_pinned)
    }

    pub fn max_lamport_seq(&self) -> u64 {
        self.content_version
            .seq
            .max(self.presence_version.seq)
            .max(self.favorite_version.seq)
            .max(self.pinned_version.seq)
            .max(self.note_version.seq)
    }

    pub fn updated_at_ms(&self) -> i64 {
        self.item
            .as_ref()
            .map(|item| item.updated_at_ms.unwrap_or(item.created_at_ms))
            .unwrap_or(0)
    }
}

/// Outcome of a commutative per-field CRDT join between a local snapshot and a remote snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedSnapshot {
    pub snapshot: SyncedItemSnapshot,
    /// True when the merged result differs from `local` (local DB/UI needs update).
    pub local_changed: bool,
    /// True when the merged result differs from `remote` (caller should push merged state back).
    pub remote_changed: bool,
}

/// Joins two snapshots for the same `sync_key` field-by-field using Lamport LWW versions.
pub fn merge_snapshots(local: &SyncedItemSnapshot, remote: &SyncedItemSnapshot) -> MergedSnapshot {
    let take_remote_presence = match remote.presence_version.cmp(&local.presence_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        // Tombstones win deterministically on equal presence versions so merges stay commutative.
        std::cmp::Ordering::Equal => !remote.is_present && local.is_present,
    };
    let (presence_version, is_present) = if take_remote_presence {
        (remote.presence_version.clone(), remote.is_present)
    } else {
        (local.presence_version.clone(), local.is_present)
    };

    let take_remote_favorite = match remote.favorite_version.cmp(&local.favorite_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => remote.is_favorite && !local.is_favorite,
    };
    let (favorite_version, is_favorite) = if take_remote_favorite {
        (remote.favorite_version.clone(), remote.is_favorite)
    } else {
        (local.favorite_version.clone(), local.is_favorite)
    };

    let take_remote_pinned = match remote.pinned_version.cmp(&local.pinned_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => remote.is_pinned && !local.is_pinned,
    };
    let (pinned_version, is_pinned) = if take_remote_pinned {
        (remote.pinned_version.clone(), remote.is_pinned)
    } else {
        (local.pinned_version.clone(), local.is_pinned)
    };

    let take_remote_note = match remote.note_version.cmp(&local.note_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => remote.note > local.note,
    };
    let (note_version, note) = if take_remote_note {
        (remote.note_version.clone(), remote.note.clone())
    } else {
        (local.note_version.clone(), local.note.clone())
    };

    let take_remote_content = match remote.content_version.cmp(&local.content_version) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => match (&local.item, &remote.item) {
            (None, Some(_)) => true,
            (Some(_), None) => false,
            _ => content_tiebreak_key(remote, local) > content_tiebreak_key(local, remote),
        },
    };
    let (content_version, mut item, mut blobs, mut source_app, mut content_identity) =
        if take_remote_content {
            (
                remote.content_version.clone(),
                remote.item.clone(),
                remote.blobs.clone(),
                remote.source_app.clone(),
                remote.content_identity.clone(),
            )
        } else {
            (
                local.content_version.clone(),
                local.item.clone(),
                local.blobs.clone(),
                local.source_app.clone(),
                local.content_identity.clone(),
            )
        };

    // Preserve earliest creation time and latest reuse time when both sides carry content.
    if let (Some(merged_item), Some(other_item)) = (
        item.as_mut(),
        if take_remote_content {
            local.item.as_ref()
        } else {
            remote.item.as_ref()
        },
    ) {
        merged_item.created_at_ms = merged_item.created_at_ms.min(other_item.created_at_ms);
        let merged_updated = merged_item
            .updated_at_ms
            .unwrap_or(merged_item.created_at_ms);
        let other_updated = other_item.updated_at_ms.unwrap_or(other_item.created_at_ms);
        merged_item.updated_at_ms = Some(merged_updated.max(other_updated));
    }

    // Tombstones carry no clipboard payload or blob references.
    if !is_present {
        item = None;
        blobs.clear();
        source_app = None;
        content_identity = None;
    }

    let snapshot = SyncedItemSnapshot {
        version: SNAPSHOT_VERSION,
        sync_key: local.sync_key.clone(),
        content_version,
        presence_version,
        favorite_version,
        pinned_version,
        note_version,
        is_present,
        is_favorite,
        is_pinned,
        note,
        item,
        blobs,
        source_app,
        content_identity,
    };

    let local_changed = &snapshot != local;
    let remote_changed = &snapshot != remote;
    MergedSnapshot {
        snapshot,
        local_changed,
        remote_changed,
    }
}

/// Produces an order-independent canonical key for equal-version content tie-breaking so two peers
/// holding divergent metadata (such as `source_app`) never ping-pong forever.
fn content_tiebreak_key(candidate: &SyncedItemSnapshot, peer: &SyncedItemSnapshot) -> Vec<u8> {
    let mut normalized_item = candidate.item.clone();
    if let (Some(item), Some(other)) = (normalized_item.as_mut(), peer.item.as_ref()) {
        item.created_at_ms = item.created_at_ms.min(other.created_at_ms);
        let item_updated = item.updated_at_ms.unwrap_or(item.created_at_ms);
        let other_updated = other.updated_at_ms.unwrap_or(other.created_at_ms);
        item.updated_at_ms = Some(item_updated.max(other_updated));
    }
    minicbor::to_vec((
        &normalized_item,
        &candidate.blobs,
        &candidate.source_app,
        &candidate.content_identity,
    ))
    .unwrap_or_default()
}

/// Sliding-window pagination cursor for newest-first synchronization without backlog HoL blocking.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncWindowCursor {
    /// Every sequence `<= synced_seq` has been completely pulled.
    pub synced_seq: u64,
    /// When catching up a multi-page gap, the next older page requests `seq < catchup_before`.
    pub catchup_before: Option<u64>,
    /// Upper bound of the active catch-up window; once `catchup_before` drains to `None`,
    /// `synced_seq` advances atomically to `window_high`.
    pub window_high: u64,
}

impl SyncWindowCursor {
    /// Selects `(after_seq, before_seq)` for the next pull RPC.
    ///
    /// When `remote_latest_seq > window_high`, freshly created items preempt downward catch-up
    /// (`before_seq = None`) so realtime copies are never delayed by historical gaps.
    pub fn next_pull_params(&self, remote_latest_seq: Option<u64>) -> (u64, Option<u64>) {
        if let Some(latest) = remote_latest_seq {
            if latest > self.window_high && self.catchup_before.is_some() {
                // Preempt catch-up with the newest slice above `window_high`.
                return (self.window_high, None);
            }
        }
        (self.synced_seq, self.catchup_before)
    }

    /// Advances the sliding window after receiving one newest-first response.
    pub fn apply_response(
        &mut self,
        requested_after: u64,
        requested_before: Option<u64>,
        response_latest_seq: u64,
        pruned_through_seq: u64,
        next_before_seq: Option<u64>,
    ) {
        if pruned_through_seq > self.synced_seq {
            self.synced_seq = pruned_through_seq;
            if self
                .catchup_before
                .is_some_and(|before| before <= pruned_through_seq)
            {
                self.catchup_before = None;
            }
        }
        if requested_before.is_none() {
            // Top-of-stream fetch.
            if requested_after == self.window_high && self.catchup_before.is_some() {
                // Preemptive fetch of brand-new items above an in-progress catch-up window.
                if let Some(next_before) = next_before_seq {
                    // Gap also exists above old `window_high`; widen catch-up smoothly by keeping
                    // the lower cursor so nothing between `synced_seq` and `response_latest_seq` is lost.
                    self.window_high = self.window_high.max(response_latest_seq);
                    self.catchup_before = Some(next_before);
                } else {
                    // All new items above `window_high` were fetched in one shot.
                    self.window_high = self.window_high.max(response_latest_seq);
                }
            } else if let Some(next_before) = next_before_seq {
                self.window_high = self.window_high.max(response_latest_seq);
                self.catchup_before = Some(next_before);
            } else {
                self.window_high = self.window_high.max(response_latest_seq);
                self.synced_seq = self.synced_seq.max(self.window_high);
                self.catchup_before = None;
            }
        } else if let Some(next_before) = next_before_seq {
            self.window_high = self.window_high.max(response_latest_seq);
            self.catchup_before = Some(next_before);
        } else {
            // Catch-up window drained all the way down to `requested_after`, so everything
            // `<= window_high` is now synced. Keep `synced_seq` capped at the previous `window_high`
            // so any items written above `window_high` during catch-up are pulled next.
            self.synced_seq = self.synced_seq.max(self.window_high);
            self.window_high = self.synced_seq;
            self.catchup_before = None;
        }
    }

    pub fn has_catchup_pending(&self) -> bool {
        self.catchup_before.is_some()
    }

    /// Returns `(completed, total)` for an active catch-up window while collapsing sparse sequence
    /// holes above `catchup_before` using the count of records already fetched in that window.
    pub fn catchup_counts(&self, pulled_in_window: u64) -> (u64, u64) {
        let Some(before) = self.catchup_before else {
            return (0, 0);
        };
        let high = self.window_high.max(self.synced_seq).max(before);
        let span = high.saturating_sub(self.synced_seq);
        if span == 0 {
            return (0, 0);
        }
        let raw_completed = high.saturating_sub(before).min(span);
        let completed = if pulled_in_window > 0 {
            pulled_in_window.min(raw_completed).max(1)
        } else {
            raw_completed
        };
        let remaining = before.saturating_sub(self.synced_seq);
        (completed, completed.saturating_add(remaining))
    }
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

#[derive(Debug, Clone)]
pub struct StoredBlob {
    pub blob_id: String,
    pub encrypted_path: String,
    pub size: u64,
    pub source_path: Option<String>,
    pub source_is_directory: bool,
    pub stream_nonce: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutgoingBlobSource<'a> {
    /// Regular file encrypted on the fly from `source_path` with a stream-committed `blob_id`.
    DirectStream {
        source_path: &'a str,
        stream_nonce: &'a [u8],
    },
    /// Ciphertext-hashed blob backed by an on-disk staging file or recreatable via `lease_outgoing_staging`.
    StagedLease,
}

impl StoredBlob {
    /// Resolves how an outgoing blob is supplied to the wire, returning `None` when a streaming
    /// blob lacks its local source path or deterministic stream nonce.
    pub fn outgoing_source(&self) -> Option<OutgoingBlobSource<'_>> {
        if self.encrypted_path.is_empty() {
            if self.source_is_directory {
                return Some(OutgoingBlobSource::StagedLease);
            }
            let source_path = self
                .source_path
                .as_deref()
                .filter(|path| !path.is_empty())?;
            let stream_nonce = self
                .stream_nonce
                .as_deref()
                .filter(|nonce| !nonce.is_empty())?;
            Some(OutgoingBlobSource::DirectStream {
                source_path,
                stream_nonce,
            })
        } else {
            Some(OutgoingBlobSource::StagedLease)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContentObject {
    pub plaintext_hash: String,
    pub blob_id: String,
    pub stream_nonce: Vec<u8>,
    pub original_size: u64,
    pub encrypted_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBackfillProgress {
    pub completed: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncInboundCatchupSources {
    pub cloud: bool,
    pub peers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncInboundCatchupProgress {
    pub metadata_completed: u64,
    pub metadata_total: u64,
    pub metadata_sources: SyncInboundCatchupSources,
    pub attachment_completed: u64,
    pub attachment_total: u64,
    pub attachment_sources: SyncInboundCatchupSources,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPeerCatchupStatus {
    pub device_id: String,
    pub device_name: String,
    pub served_items: u64,
    pub remaining_items: u64,
    pub served_attachments: u64,
    pub active_transfers: u64,
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
    pub backfill: Option<SyncBackfillProgress>,
    pub inbound_catchup: Option<SyncInboundCatchupProgress>,
    pub serving_catchups: Vec<SyncPeerCatchupStatus>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot(device: &str) -> SyncedItemSnapshot {
        SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: "sync-key-1".into(),
            content_version: FieldVersion::new(1, device),
            presence_version: FieldVersion::new(1, device),
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: Some(SyncedClipboardItem {
                kind: "text".into(),
                sub_kind: None,
                content: "hello".into(),
                search_text: Some("hello".into()),
                summary: Some("hello".into()),
                file_types: None,
                size: Some(5),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "macos".into(),
                created_at_ms: 100,
                sync_key: "sync-key-1".into(),
                updated_at_ms: Some(100),
                source_revision: Some("rev-1".into()),
                source_device_id: Some(device.into()),
                source_device_name: Some("Mac".into()),
                source_updated_at_ms: Some(100),
            }),
            blobs: Vec::new(),
            source_app: None,
            content_identity: None,
        }
    }

    #[test]
    fn merge_snapshots_is_commutative_and_preserves_independent_fields() {
        let mut device_a = sample_snapshot("dev-a");
        device_a.is_favorite = true;
        device_a.favorite_version = FieldVersion::new(2, "dev-a");

        let mut device_b = sample_snapshot("dev-a");
        device_b.note = Some("important".into());
        device_b.note_version = FieldVersion::new(2, "dev-b");

        let ab = merge_snapshots(&device_a, &device_b);
        let ba = merge_snapshots(&device_b, &device_a);
        assert_eq!(ab.snapshot, ba.snapshot);
        assert!(ab.snapshot.is_favorite);
        assert_eq!(ab.snapshot.note.as_deref(), Some("important"));
        assert!(ab.local_changed && ab.remote_changed);
    }

    #[test]
    fn merge_snapshots_strips_payload_when_tombstone_wins() {
        let local = sample_snapshot("dev-a");
        let mut remote = sample_snapshot("dev-b");
        remote.is_present = false;
        remote.presence_version = FieldVersion::new(5, "dev-b");
        remote.item = None;

        let merged = merge_snapshots(&local, &remote);
        assert!(merged.snapshot.deleted());
        assert!(merged.snapshot.item.is_none());
        assert!(merged.snapshot.blobs.is_empty());
        assert!(merged.local_changed);
    }

    #[test]
    fn merge_snapshots_equal_versions_converge_commutatively() {
        let mut device_a = sample_snapshot("dev-a");
        device_a.source_app = Some(SourceAppRef {
            version: 1,
            source_key: "app-a".into(),
            platform: "macos".into(),
            display_name: "App A".into(),
            icon: None,
            accent_start: Some("#111111".into()),
            accent_end: None,
        });
        device_a.note = Some("alpha".into());

        let mut device_b = sample_snapshot("dev-a");
        device_b.source_app = Some(SourceAppRef {
            version: 1,
            source_key: "app-b".into(),
            platform: "android".into(),
            display_name: "App B".into(),
            icon: None,
            accent_start: Some("#222222".into()),
            accent_end: None,
        });
        device_b.note = Some("beta".into());
        device_b.is_favorite = true;

        let ab = merge_snapshots(&device_a, &device_b);
        let ba = merge_snapshots(&device_b, &device_a);
        assert_eq!(ab.snapshot, ba.snapshot);
        assert!(ab.snapshot.is_favorite);
        assert_eq!(ab.snapshot.note.as_deref(), Some("beta"));

        // Merging either side with the winning snapshot reaches a fixed point with no re-push.
        let converged_a = merge_snapshots(&ab.snapshot, &ba.snapshot);
        assert!(!converged_a.local_changed);
        assert!(!converged_a.remote_changed);
    }

    #[test]
    fn sliding_window_preempts_catchup_when_newer_items_arrive() {
        let mut window = SyncWindowCursor::default();
        // Initial pull sees latest=100, gets 100..=51, next_before=51.
        let (after, before) = window.next_pull_params(None);
        assert_eq!((after, before), (0, None));
        window.apply_response(after, before, 100, 0, Some(51));
        assert_eq!(window.synced_seq, 0);
        assert_eq!(window.catchup_before, Some(51));
        assert_eq!(window.window_high, 100);

        // Brand-new item 101 arrives -> preempts downward catch-up!
        let (after, before) = window.next_pull_params(Some(101));
        assert_eq!((after, before), (100, None));
        window.apply_response(after, before, 101, 0, None);
        assert_eq!(window.window_high, 101);
        assert_eq!(window.catchup_before, Some(51));

        // Now resume catch-up for 50..=1 and finish.
        let (after, before) = window.next_pull_params(Some(101));
        assert_eq!((after, before), (0, Some(51)));
        window.apply_response(after, before, 101, 0, None);
        assert_eq!(window.synced_seq, 101);
        assert_eq!(window.catchup_before, None);
    }

    #[test]
    fn catchup_counts_collapses_sparse_sequence_holes() {
        let sparse_window = SyncWindowCursor {
            synced_seq: 0,
            catchup_before: Some(483),
            window_high: 317_729,
        };
        assert_eq!(sparse_window.catchup_counts(256), (256, 739));
        assert_eq!(sparse_window.catchup_counts(0), (317_246, 317_729));
    }
}
