//! EcoPaste sync wire protocol.
//!
//! The cloud service only handles encrypted event and blob payloads. Clipboard
//! plaintext and the group content key never cross this boundary.

use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(test)]
mod batch_tests;

pub const ALPN: &[u8] = b"ecopaste/sync/2";
pub const PROTOCOL_VERSION: u16 = 27;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_EVENTS_PER_BATCH: u16 = 256;

/// Default usage percentage that starts quota cleanup.
pub const DEFAULT_QUOTA_TRIGGER_PERCENT: u32 = 90;
/// Default usage percentage retained after quota cleanup.
pub const DEFAULT_QUOTA_TARGET_PERCENT: u32 = 70;

/// Shared quota hysteresis for local storage and Hub group storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaPolicy {
    trigger_percent: u32,
    target_percent: u32,
}

impl QuotaPolicy {
    pub fn new(trigger_percent: u32, target_percent: u32) -> Result<Self, InvalidQuotaPolicy> {
        if !(2..=100).contains(&trigger_percent)
            || target_percent == 0
            || target_percent >= trigger_percent
        {
            return Err(InvalidQuotaPolicy);
        }
        Ok(Self {
            trigger_percent,
            target_percent,
        })
    }

    pub const fn trigger_percent(self) -> u32 {
        self.trigger_percent
    }

    pub const fn target_percent(self) -> u32 {
        self.target_percent
    }

    pub fn trigger_bytes(self, limit_bytes: u64) -> u64 {
        percent_bytes(limit_bytes, self.trigger_percent)
    }

    pub fn target_bytes(self, limit_bytes: u64) -> u64 {
        percent_bytes(limit_bytes, self.target_percent)
    }

    /// Returns the cleanup target only after usage has crossed the configured trigger.
    pub fn cleanup_target_bytes(self, limit_bytes: u64, used_bytes: u64) -> Option<u64> {
        (limit_bytes > 0 && used_bytes > self.trigger_bytes(limit_bytes))
            .then(|| self.target_bytes(limit_bytes))
    }
}

impl Default for QuotaPolicy {
    fn default() -> Self {
        Self {
            trigger_percent: DEFAULT_QUOTA_TRIGGER_PERCENT,
            target_percent: DEFAULT_QUOTA_TARGET_PERCENT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("quota trigger must be between 2 and 100, and target must be between 1 and trigger - 1")]
pub struct InvalidQuotaPolicy;

fn percent_bytes(limit_bytes: u64, percent: u32) -> u64 {
    let scaled = u128::from(limit_bytes) * u128::from(percent) / 100;
    scaled.min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cbor(map)]
pub struct DeviceAnnouncement {
    #[n(0)]
    pub device_id: String,
    #[n(1)]
    pub device_name: String,
    #[n(2)]
    pub platform: String,
    #[n(3)]
    pub endpoint_id: String,
    #[n(4)]
    pub direct_addresses: Vec<String>,
    #[n(5)]
    pub relay_urls: Vec<String>,
    #[n(6)]
    pub app_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cbor(map)]
pub struct CloudDevice {
    #[n(0)]
    pub device_id: String,
    #[n(1)]
    pub device_name: String,
    #[n(2)]
    pub platform: String,
    #[n(3)]
    pub endpoint_id: String,
    #[n(4)]
    pub app_version: String,
    #[n(5)]
    pub last_seen_ms: i64,
    #[n(6)]
    pub online: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cbor(map)]
pub struct PeerAnnouncement {
    #[n(0)]
    pub device_id: String,
    #[n(1)]
    pub device_name: String,
    #[n(2)]
    pub platform: String,
    #[n(3)]
    pub endpoint_id: String,
    #[n(4)]
    pub direct_addresses: Vec<String>,
    #[n(5)]
    pub relay_urls: Vec<String>,
    #[n(6)]
    pub last_seen_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cbor(map)]
pub struct RemovedDevice {
    #[n(0)]
    pub device_id: String,
    #[n(1)]
    pub endpoint_id: String,
    #[n(2)]
    pub removed_at_ms: i64,
    /// A later restoration supersedes the tombstone while preserving conflict history.
    #[n(3)]
    pub restored_at_ms: Option<i64>,
}

impl RemovedDevice {
    pub fn is_removed(&self) -> bool {
        self.restored_at_ms
            .is_none_or(|restored_at_ms| self.removed_at_ms > restored_at_ms)
    }
}

/// Identifies the encrypted payload without exposing clipboard content to transport services.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub enum SyncEventKind {
    #[n(0)]
    #[default]
    Content,
    #[n(1)]
    ItemState,
}

impl SyncEventKind {
    pub const fn as_i64(self) -> i64 {
        match self {
            Self::Content => 0,
            Self::ItemState => 1,
        }
    }

    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::Content),
            1 => Some(Self::ItemState),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct EncryptedEvent {
    #[n(0)]
    pub event_id: String,
    #[n(1)]
    pub origin_device_id: String,
    #[n(2)]
    pub origin_sequence: u64,
    #[n(3)]
    pub created_at_ms: i64,
    #[n(4)]
    pub nonce: Vec<u8>,
    #[n(5)]
    pub ciphertext: Vec<u8>,
    #[n(6)]
    pub kind: SyncEventKind,
    /// Opaque encrypted blob identifiers referenced by this event.
    /// The Hub uses only this authenticated-group metadata for reference counting.
    #[n(7)]
    pub blob_ids: Vec<String>,
    /// Upload-time subset of `blob_ids` that may arrive after the event, such as source icons or
    /// file content above the sender's automatic cloud-upload limit. These references retain the
    /// same lifetime as required blobs.
    #[n(8)]
    pub optional_blob_ids: Vec<String>,
    /// Group-keyed anonymous item identity used only for cloud retention.
    #[n(9)]
    pub retention_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct RetentionMapping {
    #[n(0)]
    pub event_id: String,
    #[n(1)]
    pub item_key: String,
    #[n(2)]
    pub blob_ids: Vec<String>,
}

/// Records unparseable history for one device/decoder without changing its retention protection.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct DeferredRetentionEvents {
    #[n(0)]
    pub decoder_revision: u32,
    #[n(1)]
    pub event_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cbor(map)]
pub struct HubStatus {
    #[n(0)]
    pub used_bytes: u64,
    #[n(1)]
    pub max_bytes: u64,
    #[n(2)]
    pub event_count: u64,
    #[n(3)]
    pub blob_count: u64,
    #[n(4)]
    pub oldest_event_received_at_ms: Option<i64>,
    #[n(5)]
    pub retention_days: u32,
    #[n(6)]
    pub max_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct CloudEvent {
    /// Opaque cloud delivery cursor. It is not used for conflict resolution.
    #[n(0)]
    pub cursor: u64,
    #[n(1)]
    pub event: EncryptedEvent,
    /// Blob references that are currently readable from the Hub.
    #[n(2)]
    pub available_blob_ids: Vec<String>,
}

/// Durable Hub-side change to the blobs currently readable for one immutable event.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct CloudAvailabilityUpdate {
    #[n(0)]
    pub cursor: u64,
    #[n(1)]
    pub event_id: String,
    #[n(2)]
    pub available_blob_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct CursorEvent {
    /// Opaque peer-local delivery cursor. It is not used for conflict resolution.
    #[n(0)]
    pub cursor: u64,
    #[n(1)]
    pub event: EncryptedEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cbor(map)]
pub struct ServerTicket {
    #[n(0)]
    pub endpoint_id: String,
    #[n(1)]
    pub direct_addresses: Vec<String>,
    #[n(2)]
    pub relay_urls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum Request {
    #[n(0)]
    Health,
    #[n(1)]
    CreateGroup {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
    },
    #[n(2)]
    Sync {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        after_cursor: u64,
        #[n(4)]
        events: Vec<EncryptedEvent>,
        #[n(5)]
        limit: u16,
        /// Confirms that this request contains the end of the sender's current event outbox.
        #[n(6)]
        events_complete: bool,
    },
    #[n(3)]
    PutBlob {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
        #[n(3)]
        size: u64,
    },
    #[n(4)]
    GetBlob {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
    },
    #[n(6)]
    ListEvents {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        before_cursor: Option<u64>,
        #[n(3)]
        limit: u16,
        /// Includes metadata/presence events for delivery verification; history browsing omits it.
        #[n(4)]
        include_states: Option<bool>,
    },
    #[n(7)]
    SyncRemovedDevices {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        devices: Vec<RemovedDevice>,
    },
    #[n(11)]
    CloudHealth,
    /// Exchanges membership tombstones and clipboard events in one cloud round trip.
    #[n(12)]
    CloudSync {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        after_cursor: u64,
        #[n(4)]
        events: Vec<EncryptedEvent>,
        #[n(5)]
        removed_devices: Vec<RemovedDevice>,
        #[n(6)]
        limit: u16,
        /// Rejects a cursor that has fallen behind the Hub's retained history window.
        #[n(7)]
        bounded_hub: bool,
    },
    /// Stores an optional source application icon and wakes group watchers after a new upload.
    #[n(13)]
    PutSourceIcon {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
        #[n(3)]
        size: u64,
    },
    /// Keeps one cloud change stream open for the current device.
    #[n(14)]
    WatchCloud {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        after_cursor: u64,
        #[n(3)]
        after_removed_at_ms: i64,
    },
    /// Lists active group members and their current Hub presence.
    #[n(15)]
    ListDevices {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
    },
    /// Calculates the first retained cursor that a new or repaired client should consume.
    #[n(16)]
    BootstrapPlan {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
    },
    /// Stores a blob that participates in bounded Hub reference reclamation.
    #[n(17)]
    PutManagedBlob {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
        #[n(3)]
        size: u64,
    },
    /// Reads a bounded page of legacy events whose retention identity is still unknown.
    #[n(19)]
    RetentionPlan {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        decoder_revision: Option<u32>,
    },
    /// Replaces a device's protection snapshot and registers identities without revealing content.
    #[n(20)]
    SetRetention {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        generation: u64,
        #[n(4)]
        protected_keys: Option<Vec<String>>,
        #[n(5)]
        mappings: Vec<RetentionMapping>,
        #[n(6)]
        deferred: Option<DeferredRetentionEvents>,
    },
    /// Stores an optional source icon that participates in reference reclamation.
    #[n(18)]
    PutManagedSourceIcon {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
        #[n(3)]
        size: u64,
    },
    /// Lets a LAN receiver choose metadata-only or already available content before transfer.
    #[n(21)]
    PlanLanContent {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device_id: String,
        #[n(3)]
        events: Vec<EncryptedEvent>,
    },
    /// Streams ciphertext produced directly from a local source without sender-side staging.
    #[n(22)]
    PutStreamBlob {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
        #[n(3)]
        size: u64,
    },
    /// Streaming counterpart of `PutManagedBlob` for bounded Hub retention.
    #[n(23)]
    PutManagedStreamBlob {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
        #[n(3)]
        size: u64,
    },
    /// Commits a fully received streaming blob after the sender validates its source hash.
    #[n(24)]
    CommitStreamBlob {
        #[n(0)]
        blob_id: String,
    },
    /// Confirms durable LAN events and identifies routes with complete content.
    #[n(25)]
    AcknowledgeLan {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        event_ids: Vec<String>,
        #[n(4)]
        complete_event_ids: Vec<String>,
    },
    /// Reads durable blob-availability changes independently from immutable event delivery.
    #[n(26)]
    SyncCloudAvailability {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        after_cursor: u64,
        #[n(3)]
        limit: u16,
    },
    /// Revalidates known cloud records after Hub retention has removed history.
    #[n(27)]
    CheckCloudAvailability {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        event_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum Response {
    #[n(0)]
    Health {
        #[n(0)]
        protocol_version: u16,
        #[n(1)]
        server_time_ms: i64,
    },
    #[n(1)]
    GroupCreated,
    #[n(2)]
    Synced {
        #[n(0)]
        accepted_event_ids: Vec<String>,
        #[n(1)]
        events: Vec<CursorEvent>,
        #[n(2)]
        peers: Vec<PeerAnnouncement>,
        #[n(3)]
        latest_cursor: u64,
        #[n(4)]
        has_more: bool,
    },
    #[n(3)]
    BlobReady {
        #[n(0)]
        size: u64,
    },
    #[n(4)]
    BlobStored,
    #[n(5)]
    Error {
        #[n(0)]
        code: ErrorCode,
        #[n(1)]
        message: String,
    },
    #[n(7)]
    EventsPage {
        #[n(0)]
        events: Vec<CloudEvent>,
        #[n(1)]
        next_before_cursor: Option<u64>,
        #[n(2)]
        total: u64,
    },
    #[n(8)]
    RemovedDevices {
        #[n(0)]
        devices: Vec<RemovedDevice>,
    },
    #[n(10)]
    CloudChanged {
        #[n(0)]
        latest_cursor: u64,
        #[n(1)]
        latest_removed_at_ms: i64,
        #[n(2)]
        server_version: String,
        #[n(3)]
        latest_availability_cursor: u64,
    },
    #[n(11)]
    CloudHealth {
        #[n(0)]
        protocol_version: u16,
        #[n(1)]
        server_time_ms: i64,
        #[n(2)]
        server_version: String,
    },
    #[n(12)]
    CloudSynced {
        #[n(0)]
        accepted_event_ids: Vec<String>,
        #[n(1)]
        events: Vec<CloudEvent>,
        #[n(2)]
        peers: Vec<PeerAnnouncement>,
        #[n(3)]
        latest_cursor: u64,
        #[n(4)]
        removed_devices: Vec<RemovedDevice>,
        #[n(5)]
        has_more: bool,
    },
    #[n(13)]
    Devices {
        #[n(0)]
        devices: Vec<CloudDevice>,
        #[n(1)]
        hub_status: Option<HubStatus>,
    },
    #[n(14)]
    BootstrapPlan {
        /// Cursor to persist before requesting the first normal sync page.
        #[n(0)]
        start_cursor: u64,
        #[n(1)]
        latest_cursor: u64,
    },
    #[n(15)]
    RetentionPlan {
        #[n(0)]
        epoch: String,
        #[n(1)]
        events: Vec<CloudEvent>,
        #[n(2)]
        has_more: bool,
    },
    #[n(16)]
    RetentionUpdated,
    #[n(17)]
    LanContentPlan {
        #[n(0)]
        metadata_only_event_ids: Vec<String>,
        #[n(1)]
        available_event_ids: Vec<String>,
    },
    #[n(18)]
    LanAcknowledged,
    #[n(19)]
    CloudAvailabilitySynced {
        #[n(0)]
        updates: Vec<CloudAvailabilityUpdate>,
        #[n(1)]
        latest_cursor: u64,
        #[n(2)]
        has_more: bool,
        #[n(3)]
        pruned_availability_cursor: u64,
    },
    #[n(20)]
    CloudAvailabilityChecked {
        #[n(0)]
        updates: Vec<CloudAvailabilityUpdate>,
        #[n(1)]
        missing_event_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum ErrorCode {
    #[n(0)]
    InvalidRequest,
    #[n(1)]
    Unauthorized,
    #[n(2)]
    NotFound,
    #[n(3)]
    Conflict,
    #[n(4)]
    TooLarge,
    #[n(5)]
    Internal,
    #[n(6)]
    HistoryGap,
    #[n(7)]
    QuotaExceeded,
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame is too large: {actual} bytes, maximum is {maximum}")]
    TooLarge { actual: usize, maximum: usize },
    #[error("CBOR encode error: {0}")]
    Encode(String),
    #[error("CBOR decode error: {0}")]
    Decode(String),
}

/// Counts the actual CBOR encoding without allocating a second payload buffer.
#[derive(Default)]
struct EncodedSize(usize);

impl minicbor::encode::Write for EncodedSize {
    type Error = std::convert::Infallible;

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 += bytes.len();
        Ok(())
    }
}

fn encoded_size<T: Encode<()>>(value: &T) -> Result<usize, FrameError> {
    let mut size = EncodedSize::default();
    minicbor::encode(value, &mut size).map_err(|error| FrameError::Encode(error.to_string()))?;
    Ok(size.0)
}

/// Fits a contiguous prefix, including growth of the initially empty CBOR array header.
/// An oversized first event is an error; it must never be skipped or silently truncated.
fn fitting_event_count<T: Encode<()>>(
    events: &[T],
    empty_frame_size: usize,
) -> Result<usize, FrameError> {
    if empty_frame_size > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            actual: empty_frame_size,
            maximum: MAX_FRAME_BYTES,
        });
    }
    let mut payload_size = empty_frame_size;
    let mut count = 0;
    for event in events.iter().take(usize::from(MAX_EVENTS_PER_BATCH)) {
        payload_size += encoded_size(event)?;
        let array_growth = match count + 1 {
            0..=23 => 0,
            24..=255 => 1,
            _ => 2,
        };
        let actual = payload_size + array_growth;
        if actual > MAX_FRAME_BYTES {
            if count == 0 {
                return Err(FrameError::TooLarge {
                    actual,
                    maximum: MAX_FRAME_BYTES,
                });
            }
            break;
        }
        count += 1;
    }
    Ok(count)
}

/// Bounds legacy metadata registration without skipping an oversized encrypted record.
pub fn limit_retention_plan(response: &mut Response) -> Result<(), FrameError> {
    let mut events = match response {
        Response::RetentionPlan { events, .. } => std::mem::take(events),
        _ => return Err(FrameError::Encode("expected a retention plan".into())),
    };
    let count = fitting_event_count(&events, encoded_size(response)?)?;
    if let Response::RetentionPlan {
        events: outgoing,
        has_more,
        ..
    } = response
    {
        *has_more |= count < events.len();
        events.truncate(count);
        *outgoing = events;
    }
    Ok(())
}

/// Keeps newest-first history pages within the frame while retaining their continuation cursor.
pub fn limit_events_page(response: &mut Response) -> Result<(), FrameError> {
    let (mut events, had_more) = match response {
        Response::EventsPage {
            events,
            next_before_cursor,
            ..
        } => {
            let more = next_before_cursor.is_some();
            *next_before_cursor = Some(u64::MAX);
            (std::mem::take(events), more)
        }
        _ => return Err(FrameError::Encode("expected an event page".into())),
    };
    let count = fitting_event_count(&events, encoded_size(response)?)?;
    let has_more = had_more || count < events.len();
    events.truncate(count);
    if let Response::EventsPage {
        events: outgoing,
        next_before_cursor,
        ..
    } = response
    {
        *next_before_cursor =
            has_more.then(|| events.last().expect("non-empty continued page").cursor);
        *outgoing = events;
    }
    Ok(())
}

/// Keeps only the outgoing prefix that fits in one frame. Remaining events stay in the outbox.
pub fn limit_sync_request(request: &mut Request) -> Result<usize, FrameError> {
    let mut events = match request {
        Request::Sync { events, .. } | Request::CloudSync { events, .. } => std::mem::take(events),
        _ => return Err(FrameError::Encode("expected a sync request".into())),
    };
    let count = fitting_event_count(&events, encoded_size(request)?)?;
    events.truncate(count);
    match request {
        Request::Sync {
            events: outgoing, ..
        }
        | Request::CloudSync {
            events: outgoing, ..
        } => {
            *outgoing = events;
        }
        _ => unreachable!(),
    }
    Ok(count)
}

/// Keeps a LAN content plan within one frame and returns the selected event prefix length.
pub fn limit_lan_content_plan(request: &mut Request) -> Result<usize, FrameError> {
    let mut events = match request {
        Request::PlanLanContent { events, .. } => std::mem::take(events),
        _ => return Err(FrameError::Encode("expected a LAN content plan".into())),
    };
    let count = fitting_event_count(&events, encoded_size(request)?)?;
    events.truncate(count);
    if let Request::PlanLanContent {
        events: outgoing, ..
    } = request
    {
        *outgoing = events;
    }
    Ok(count)
}

/// Byte-limits current sync responses and records whether another page remains.
/// The cursor advances through returned events only, including when an acknowledgement is replayed.
pub fn limit_sync_response(
    response: &mut Response,
    after_cursor: u64,
    limit: u16,
) -> Result<(), FrameError> {
    match response {
        Response::Synced {
            events,
            latest_cursor,
            has_more,
            ..
        } => {
            let mut events = std::mem::take(events);
            *latest_cursor = u64::MAX;
            *has_more = false;
            let count = fitting_event_count(&events, encoded_size(response)?)?;
            let has_more = count < events.len()
                || events.len() >= usize::from(limit.clamp(1, MAX_EVENTS_PER_BATCH));
            events.truncate(count);
            let latest_cursor = events
                .last()
                .map(|event| event.cursor)
                .unwrap_or(after_cursor);
            if let Response::Synced {
                events: target,
                latest_cursor: target_cursor,
                has_more: target_has_more,
                ..
            } = response
            {
                *target = events;
                *target_cursor = latest_cursor;
                *target_has_more = has_more;
            }
            Ok(())
        }
        Response::CloudSynced {
            events,
            latest_cursor,
            has_more,
            ..
        } => {
            let mut events = std::mem::take(events);
            *latest_cursor = u64::MAX;
            *has_more = false;
            let count = fitting_event_count(&events, encoded_size(response)?)?;
            let has_more = count < events.len()
                || events.len() >= usize::from(limit.clamp(1, MAX_EVENTS_PER_BATCH));
            events.truncate(count);
            let latest_cursor = events
                .last()
                .map(|event| event.cursor)
                .unwrap_or(after_cursor);
            if let Response::CloudSynced {
                events: target,
                latest_cursor: target_cursor,
                has_more: target_has_more,
                ..
            } = response
            {
                *target = events;
                *target_cursor = latest_cursor;
                *target_has_more = has_more;
            }
            Ok(())
        }
        _ => Err(FrameError::Encode("expected a sync response".into())),
    }
}

/// Writes one length-delimited CBOR message.
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
    T: Encode<()>,
{
    let encoded = minicbor::to_vec(value).map_err(|err| FrameError::Encode(err.to_string()))?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            actual: encoded.len(),
            maximum: MAX_FRAME_BYTES,
        });
    }

    writer.write_u32(encoded.len() as u32).await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads one length-delimited CBOR message with a strict allocation limit.
pub async fn read_frame<R, T>(reader: &mut R) -> Result<T, FrameError>
where
    R: AsyncRead + Unpin,
    T: for<'bytes> Decode<'bytes, ()>,
{
    let length = reader.read_u32().await? as usize;
    if length > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            actual: length,
            maximum: MAX_FRAME_BYTES,
        });
    }

    let mut encoded = vec![0; length];
    reader.read_exact(&mut encoded).await?;
    minicbor::decode(&encoded).map_err(|err| FrameError::Decode(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_policy_uses_shared_defaults_and_binary_byte_math() {
        let policy = QuotaPolicy::default();
        let limit = 1024 * 1024 * 1024;

        assert_eq!(policy.trigger_percent(), 90);
        assert_eq!(policy.target_percent(), 70);
        assert_eq!(policy.trigger_bytes(limit), 966_367_641);
        assert_eq!(policy.target_bytes(limit), 751_619_276);
        assert_eq!(policy.cleanup_target_bytes(limit, 966_367_641), None);
        assert_eq!(
            policy.cleanup_target_bytes(limit, 966_367_642),
            Some(751_619_276)
        );
        assert_eq!(policy.cleanup_target_bytes(0, u64::MAX), None);
    }

    #[test]
    fn quota_policy_rejects_invalid_hysteresis() {
        assert!(QuotaPolicy::new(1, 0).is_err());
        assert!(QuotaPolicy::new(101, 70).is_err());
        assert!(QuotaPolicy::new(90, 0).is_err());
        assert!(QuotaPolicy::new(90, 90).is_err());
        assert!(QuotaPolicy::new(90, 91).is_err());
        assert_eq!(QuotaPolicy::new(90, 70), Ok(QuotaPolicy::default()));
    }

    #[tokio::test]
    async fn request_round_trip() {
        let request = Request::Sync {
            group_id: "group".into(),
            access_token: vec![1; 32],
            device: DeviceAnnouncement {
                device_id: "device".into(),
                device_name: "Mac".into(),
                platform: "macos".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: vec!["192.168.1.2:44820".into()],
                relay_urls: Vec::new(),
                app_version: "1.1.154".into(),
            },
            after_cursor: 7,
            events: Vec::new(),
            limit: 100,
            events_complete: true,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn streaming_blob_requests_round_trip() {
        for request in [
            Request::PutStreamBlob {
                group_id: "group".into(),
                access_token: vec![1; 32],
                blob_id: "a".repeat(64),
                size: 1024,
            },
            Request::PutManagedStreamBlob {
                group_id: "group".into(),
                access_token: vec![1; 32],
                blob_id: "b".repeat(64),
                size: 2048,
            },
            Request::CommitStreamBlob {
                blob_id: "c".repeat(64),
            },
        ] {
            let encoded = minicbor::to_vec(&request).unwrap();
            let decoded: Request = minicbor::decode(&encoded).unwrap();
            assert_eq!(decoded, request);
        }
        assert_eq!(PROTOCOL_VERSION, 27);
    }

    #[tokio::test]
    async fn cloud_history_request_round_trip() {
        let request = Request::ListEvents {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            before_cursor: Some(42),
            limit: 30,
            include_states: Some(true),
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, request);
    }

    #[tokio::test]
    async fn cloud_availability_round_trip() {
        let request = Request::SyncCloudAvailability {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            after_cursor: 42,
            limit: 30,
        };
        let response = Response::CloudAvailabilitySynced {
            updates: vec![CloudAvailabilityUpdate {
                cursor: 43,
                event_id: "event_123".into(),
                available_blob_ids: vec!["a".repeat(64)],
            }],
            latest_cursor: 43,
            has_more: false,
            pruned_availability_cursor: 0,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        assert_eq!(
            read_frame::<_, Request>(&mut server).await.unwrap(),
            request
        );
        write_frame(&mut server, &response).await.unwrap();
        assert_eq!(
            read_frame::<_, Response>(&mut client).await.unwrap(),
            response
        );
    }

    #[tokio::test]
    async fn cloud_device_directory_round_trip() {
        let request = Request::ListDevices {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            device: DeviceAnnouncement {
                device_id: "device_123".into(),
                device_name: "Windows PC".into(),
                platform: "windows".into(),
                endpoint_id: "endpoint_123".into(),
                direct_addresses: Vec::new(),
                relay_urls: Vec::new(),
                app_version: "1.1.154".into(),
            },
        };
        let response = Response::Devices {
            devices: vec![CloudDevice {
                device_id: "device_123".into(),
                device_name: "Windows PC".into(),
                platform: "windows".into(),
                endpoint_id: "endpoint_123".into(),
                app_version: "1.1.154".into(),
                last_seen_ms: 42,
                online: true,
            }],
            hub_status: None,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        assert_eq!(
            read_frame::<_, Request>(&mut server).await.unwrap(),
            request
        );
        write_frame(&mut server, &response).await.unwrap();
        assert_eq!(
            read_frame::<_, Response>(&mut client).await.unwrap(),
            response
        );
    }

    #[tokio::test]
    async fn bootstrap_plan_round_trip() {
        let request = Request::BootstrapPlan {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
        };
        let response = Response::BootstrapPlan {
            start_cursor: 21,
            latest_cursor: 42,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        assert_eq!(
            read_frame::<_, Request>(&mut server).await.unwrap(),
            request
        );
        write_frame(&mut server, &response).await.unwrap();
        assert_eq!(
            read_frame::<_, Response>(&mut client).await.unwrap(),
            response
        );
    }

    #[tokio::test]
    async fn removed_devices_request_round_trip() {
        let request = Request::SyncRemovedDevices {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            devices: vec![RemovedDevice {
                device_id: "device_123".into(),
                endpoint_id: "endpoint_123".into(),
                removed_at_ms: 42,
                restored_at_ms: None,
            }],
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, request);
    }

    #[tokio::test]
    async fn watch_request_round_trip() {
        let request = Request::WatchCloud {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            after_cursor: 42,
            after_removed_at_ms: 84,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, request);
    }
}
