//! EcoPaste sync wire protocol (Per-Item State v32).
//!
//! The cloud service only handles encrypted item snapshots and blob payloads.
//! Clipboard plaintext and the group content key never cross this boundary.

pub mod transfer;

use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(test)]
mod batch_tests;

pub const ALPN: &[u8] = b"ecopaste/sync/2";
pub const PROTOCOL_VERSION: u16 = 32;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ITEMS_PER_BATCH: u16 = 256;

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

/// One encrypted per-item snapshot pushed by a device.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct SyncItemWrite {
    /// Group-keyed 64-hex anonymous identifier derived from the item's `sync_key`.
    #[n(0)]
    pub item_key: String,
    /// Optimistic concurrency revision expected on the Hub (`0` for new items or LAN pushes).
    #[n(1)]
    pub expected_hub_rev: u64,
    /// Sender-local monotonic sequence used for LAN receipt confirmation.
    #[n(2)]
    pub local_seq: u64,
    #[n(3)]
    pub origin_device_id: String,
    #[n(4)]
    pub updated_at_ms: i64,
    /// Marks a deletion tombstone. Hub clears referenced blobs and retains the compact row.
    #[n(5)]
    pub deleted: bool,
    /// Marks favorite or pinned items shielded from automatic Hub quota eviction.
    #[n(6)]
    pub protected: bool,
    #[n(7)]
    pub nonce: Vec<u8>,
    #[n(8)]
    pub ciphertext: Vec<u8>,
    /// Content blob identifiers referenced by this snapshot.
    #[n(9)]
    pub blob_ids: Vec<String>,
    /// Optional source application icon blob identifiers.
    #[n(10)]
    pub optional_blob_ids: Vec<String>,
}

/// One encrypted per-item snapshot returned by the Hub or a LAN peer.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct SyncItemRecord {
    /// Monotonic delivery cursor assigned by the Hub or serving LAN peer.
    #[n(0)]
    pub seq: u64,
    /// Per-item CAS revision assigned by the Hub (`0` on LAN).
    #[n(1)]
    pub hub_rev: u64,
    #[n(2)]
    pub item_key: String,
    #[n(3)]
    pub origin_device_id: String,
    #[n(4)]
    pub updated_at_ms: i64,
    #[n(5)]
    pub deleted: bool,
    #[n(6)]
    pub protected: bool,
    #[n(7)]
    pub nonce: Vec<u8>,
    #[n(8)]
    pub ciphertext: Vec<u8>,
    #[n(9)]
    pub blob_ids: Vec<String>,
    #[n(10)]
    pub optional_blob_ids: Vec<String>,
    /// Blob identifiers currently readable on the serving endpoint.
    #[n(11)]
    pub available_blob_ids: Vec<String>,
}

/// Confirms one accepted Hub item write and returns its newly assigned revision, cursor,
/// and any referenced blobs already stored on the Hub.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct AcceptedItem {
    #[n(0)]
    pub item_key: String,
    #[n(1)]
    pub hub_rev: u64,
    #[n(2)]
    pub seq: u64,
    #[n(3)]
    pub available_blob_ids: Vec<String>,
}

/// Identifies one encrypted content blob whose presence on the Hub can skip re-uploading.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct CloudBlobCheck {
    #[n(0)]
    pub blob_id: String,
    #[n(1)]
    pub size: u64,
}

/// Delivery completeness confirmed by a LAN receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum LanContentLevel {
    #[n(0)]
    Metadata,
    #[n(1)]
    Complete,
}

/// Confirms a LAN peer applied a sender's snapshot or finished pulling its blobs.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct LanItemAck {
    #[n(0)]
    pub item_key: String,
    #[n(1)]
    pub local_seq: u64,
    #[n(2)]
    pub level: LanContentLevel,
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
    /// One bounded part of a transfer authenticated by its parent request on this connection.
    #[n(32)]
    BlobPart {
        #[n(0)]
        transfer_id: u64,
        #[n(1)]
        index: u64,
    },
    #[n(0)]
    Health,
    #[n(1)]
    CreateGroup {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
    },
    /// Bidirectional LAN item sync with newest-first sliding window pagination.
    #[n(2)]
    SyncItems {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        after_seq: u64,
        #[n(4)]
        before_seq: Option<u64>,
        #[n(5)]
        items: Vec<SyncItemWrite>,
        #[n(6)]
        limit: u16,
    },
    /// Downloads one encrypted blob from the Hub or a LAN peer.
    #[n(4)]
    GetBlob {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blob_id: String,
        #[n(3)]
        transfer_id: u64,
        #[n(4)]
        file_content: bool,
        /// Exact `sync_key` of the requesting card when a peer pulls a deduplicated attachment.
        #[n(5)]
        item_sync_key: Option<String>,
    },
    /// Lists newest non-deleted cloud items for the cloud history drawer.
    #[n(6)]
    ListItems {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        before_seq: Option<u64>,
        #[n(3)]
        limit: u16,
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
    /// Bidirectional Cloud item sync with per-item CAS and newest-first sliding window pagination.
    #[n(12)]
    CloudSyncItems {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        after_seq: u64,
        #[n(4)]
        before_seq: Option<u64>,
        #[n(5)]
        items: Vec<SyncItemWrite>,
        #[n(6)]
        removed_devices: Vec<RemovedDevice>,
        #[n(7)]
        limit: u16,
    },
    /// Keeps one cloud change stream open for the current device.
    #[n(14)]
    WatchCloud {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        after_seq: u64,
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
    /// Stores a content blob on the Hub and advances `seq` for referencing items.
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
        #[n(4)]
        transfer_id: u64,
        #[n(5)]
        file_content: bool,
    },
    /// Stores an optional source icon blob on the Hub.
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
        #[n(4)]
        transfer_id: u64,
        #[n(5)]
        file_content: bool,
    },
    /// Streaming counterpart of `PutManagedBlob` without sender-side ciphertext staging.
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
        #[n(4)]
        transfer_id: u64,
        #[n(5)]
        file_content: bool,
    },
    /// Commits a fully received streaming blob after the sender validates its source hash.
    #[n(24)]
    CommitStreamBlob {
        #[n(0)]
        blob_id: String,
    },
    /// Confirms asynchronous LAN blob download completion back to the sending peer.
    #[n(25)]
    AckLanItems {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        acks: Vec<LanItemAck>,
    },
    /// Queries which already-encrypted content blobs already exist on the Cloud Hub.
    #[n(26)]
    CheckCloudBlobs {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        blobs: Vec<CloudBlobCheck>,
    },
}

impl Request {
    /// Direction is relative to the endpoint serving the request.
    pub fn attachment_lane(&self) -> Option<(bool, bool)> {
        match self {
            Self::PutManagedBlob { file_content, .. }
            | Self::PutManagedStreamBlob { file_content, .. }
            | Self::PutManagedSourceIcon { file_content, .. } => Some((*file_content, false)),
            Self::GetBlob { file_content, .. } => Some((*file_content, true)),
            _ => None,
        }
    }
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
    LanSynced {
        #[n(0)]
        acks: Vec<LanItemAck>,
        #[n(1)]
        items: Vec<SyncItemRecord>,
        #[n(2)]
        latest_seq: u64,
        #[n(3)]
        next_before_seq: Option<u64>,
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
    ItemsPage {
        #[n(0)]
        items: Vec<SyncItemRecord>,
        #[n(1)]
        next_before_seq: Option<u64>,
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
        latest_seq: u64,
        #[n(1)]
        latest_removed_at_ms: i64,
        #[n(2)]
        server_version: String,
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
        accepted: Vec<AcceptedItem>,
        #[n(1)]
        conflicts: Vec<SyncItemRecord>,
        #[n(2)]
        items: Vec<SyncItemRecord>,
        #[n(3)]
        peers: Vec<PeerAnnouncement>,
        #[n(4)]
        removed_devices: Vec<RemovedDevice>,
        #[n(5)]
        latest_seq: u64,
        #[n(6)]
        pruned_through_seq: u64,
        #[n(7)]
        next_before_seq: Option<u64>,
    },
    #[n(13)]
    Devices {
        #[n(0)]
        devices: Vec<CloudDevice>,
        #[n(1)]
        hub_status: Option<HubStatus>,
    },
    #[n(18)]
    LanAcknowledged,
    #[n(19)]
    CloudBlobsChecked {
        #[n(0)]
        available_blob_ids: Vec<String>,
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
/// An oversized first item is an error; it must never be skipped or silently truncated.
fn fitting_item_count<T: Encode<()>>(
    items: &[T],
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
    for item in items.iter().take(usize::from(MAX_ITEMS_PER_BATCH)) {
        payload_size += encoded_size(item)?;
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

/// Keeps newest-first history pages within one frame while retaining their continuation cursor.
pub fn limit_items_page(response: &mut Response) -> Result<(), FrameError> {
    let (mut items, had_more) = match response {
        Response::ItemsPage {
            items,
            next_before_seq,
            ..
        } => {
            let more = next_before_seq.is_some();
            *next_before_seq = Some(u64::MAX);
            (std::mem::take(items), more)
        }
        _ => return Err(FrameError::Encode("expected an items page".into())),
    };
    let count = fitting_item_count(&items, encoded_size(response)?)?;
    let has_more = had_more || count < items.len();
    items.truncate(count);
    if let Response::ItemsPage {
        items: outgoing,
        next_before_seq,
        ..
    } = response
    {
        *next_before_seq = has_more.then(|| items.last().expect("non-empty continued page").seq);
        *outgoing = items;
    }
    Ok(())
}

/// Keeps only the outgoing prefix that fits in one frame. Remaining items stay queued.
pub fn limit_sync_request(request: &mut Request) -> Result<usize, FrameError> {
    let mut items = match request {
        Request::SyncItems { items, .. } | Request::CloudSyncItems { items, .. } => {
            std::mem::take(items)
        }
        _ => return Err(FrameError::Encode("expected a sync items request".into())),
    };
    let count = fitting_item_count(&items, encoded_size(request)?)?;
    items.truncate(count);
    match request {
        Request::SyncItems {
            items: outgoing, ..
        }
        | Request::CloudSyncItems {
            items: outgoing, ..
        } => {
            *outgoing = items;
        }
        _ => unreachable!(),
    }
    Ok(count)
}

/// Byte-limits newest-first sync responses while preserving `next_before_seq` continuation.
///
/// Expects `items` sorted in `seq DESC` order and `had_more` indicating whether more older rows
/// exist beyond the initially fetched slice.
pub fn limit_sync_response(response: &mut Response, had_more: bool) -> Result<(), FrameError> {
    match response {
        Response::LanSynced {
            items,
            next_before_seq,
            ..
        } => {
            let mut incoming = std::mem::take(items);
            *next_before_seq = Some(u64::MAX);
            let count = fitting_item_count(&incoming, encoded_size(response)?)?;
            let has_more = had_more || count < incoming.len();
            incoming.truncate(count);
            let next_before =
                has_more.then(|| incoming.last().expect("non-empty continued page").seq);
            if let Response::LanSynced {
                items: target,
                next_before_seq: target_next,
                ..
            } = response
            {
                *target = incoming;
                *target_next = next_before;
            }
            Ok(())
        }
        Response::CloudSynced {
            items,
            next_before_seq,
            ..
        } => {
            let mut incoming = std::mem::take(items);
            *next_before_seq = Some(u64::MAX);
            let count = fitting_item_count(&incoming, encoded_size(response)?)?;
            let has_more = had_more || count < incoming.len();
            incoming.truncate(count);
            let next_before =
                has_more.then(|| incoming.last().expect("non-empty continued page").seq);
            if let Response::CloudSynced {
                items: target,
                next_before_seq: target_next,
                ..
            } = response
            {
                *target = incoming;
                *target_next = next_before;
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
    let len = reader.read_u32().await? as usize;
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            actual: len,
            maximum: MAX_FRAME_BYTES,
        });
    }

    let mut buffer = vec![0_u8; len];
    reader.read_exact(&mut buffer).await?;
    minicbor::decode(&buffer).map_err(|err| FrameError::Decode(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cloud_sync_items_round_trip() {
        let request = Request::CloudSyncItems {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            device: DeviceAnnouncement {
                device_id: "device_123".into(),
                device_name: "Mac".into(),
                platform: "macos".into(),
                endpoint_id: "endpoint_123".into(),
                direct_addresses: Vec::new(),
                relay_urls: Vec::new(),
                app_version: "1.1.231".into(),
            },
            after_seq: 10,
            before_seq: Some(50),
            items: vec![SyncItemWrite {
                item_key: "a".repeat(64),
                expected_hub_rev: 2,
                local_seq: 9,
                origin_device_id: "device_123".into(),
                updated_at_ms: 1_700_000_000_000,
                deleted: false,
                protected: true,
                nonce: vec![1; 24],
                ciphertext: vec![2, 3, 4],
                blob_ids: vec!["b".repeat(64)],
                optional_blob_ids: vec!["c".repeat(64)],
            }],
            removed_devices: Vec::new(),
            limit: 64,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, request);
        assert_eq!(PROTOCOL_VERSION, 32);
    }

    #[tokio::test]
    async fn check_cloud_blobs_round_trip() {
        let request = Request::CheckCloudBlobs {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            blobs: vec![CloudBlobCheck {
                blob_id: "b".repeat(64),
                size: 4096,
            }],
        };
        let response = Response::CloudBlobsChecked {
            available_blob_ids: vec!["b".repeat(64)],
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded_req: Request = read_frame(&mut server).await.unwrap();
        assert_eq!(decoded_req, request);

        write_frame(&mut client, &response).await.unwrap();
        let decoded_resp: Response = read_frame(&mut server).await.unwrap();
        assert_eq!(decoded_resp, response);
    }

    #[tokio::test]
    async fn lan_sync_and_ack_round_trip() {
        let ack = Request::AckLanItems {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            device: DeviceAnnouncement {
                device_id: "device_123".into(),
                device_name: "Windows".into(),
                platform: "windows".into(),
                endpoint_id: "endpoint_456".into(),
                direct_addresses: Vec::new(),
                relay_urls: Vec::new(),
                app_version: "1.1.231".into(),
            },
            acks: vec![LanItemAck {
                item_key: "a".repeat(64),
                local_seq: 42,
                level: LanContentLevel::Complete,
            }],
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &ack).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, ack);
    }

    #[test]
    fn quota_policy_enforces_hysteresis_bounds() {
        let policy = QuotaPolicy::default();
        assert_eq!(policy.trigger_percent(), DEFAULT_QUOTA_TRIGGER_PERCENT);
        assert_eq!(policy.target_percent(), DEFAULT_QUOTA_TARGET_PERCENT);
        assert_eq!(policy.cleanup_target_bytes(1000, 900), None);
        assert_eq!(policy.cleanup_target_bytes(1000, 901), Some(700));
        assert!(QuotaPolicy::new(1, 1).is_err());
        assert!(QuotaPolicy::new(80, 80).is_err());
        assert!(QuotaPolicy::new(80, 0).is_err());
    }
}
