//! EcoPaste sync wire protocol.
//!
//! The cloud service only handles encrypted event and blob payloads. Clipboard
//! plaintext and the group content key never cross this boundary.

use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const ALPN: &[u8] = b"ecopaste/sync/1";
pub const PROTOCOL_VERSION: u16 = 4;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_EVENTS_PER_BATCH: u16 = 256;

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
    #[cbor(default)]
    pub restored_at_ms: Option<i64>,
}

impl RemovedDevice {
    pub fn is_removed(&self) -> bool {
        self.restored_at_ms
            .is_none_or(|restored_at_ms| self.removed_at_ms > restored_at_ms)
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
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct CloudEvent {
    /// Opaque cloud delivery cursor. It is not used for conflict resolution.
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
    #[n(5)]
    Watch {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        after_cursor: u64,
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
    #[n(8)]
    WatchGroup {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        after_cursor: u64,
        #[n(3)]
        after_removed_at_ms: i64,
    },
    /// Keeps one response stream open and pushes group cursors after every change.
    #[n(9)]
    WatchGroupStream {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        after_cursor: u64,
        #[n(3)]
        after_removed_at_ms: i64,
    },
    /// Keeps a version-aware response stream without changing legacy watch frames.
    #[n(10)]
    WatchGroupStreamV2 {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        after_cursor: u64,
        #[n(3)]
        after_removed_at_ms: i64,
    },
    #[n(11)]
    HealthV2,
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
        events: Vec<CloudEvent>,
        #[n(2)]
        peers: Vec<PeerAnnouncement>,
        #[n(3)]
        latest_cursor: u64,
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
    #[n(6)]
    Changed {
        #[n(0)]
        latest_cursor: u64,
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
    #[n(9)]
    GroupChanged {
        #[n(0)]
        latest_cursor: u64,
        #[n(1)]
        latest_removed_at_ms: i64,
    },
    #[n(10)]
    GroupChangedV2 {
        #[n(0)]
        latest_cursor: u64,
        #[n(1)]
        latest_removed_at_ms: i64,
        #[n(2)]
        server_version: String,
    },
    #[n(11)]
    HealthV2 {
        #[n(0)]
        protocol_version: u16,
        #[n(1)]
        server_time_ms: i64,
        #[n(2)]
        server_version: String,
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

    #[derive(Debug, Encode, Decode)]
    #[cbor(map)]
    struct LegacyRemovedDevice {
        #[n(0)]
        device_id: String,
        #[n(1)]
        endpoint_id: String,
        #[n(2)]
        removed_at_ms: i64,
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
            },
            after_cursor: 7,
            events: Vec::new(),
            limit: 100,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, request);
    }

    #[tokio::test]
    async fn cloud_history_request_round_trip() {
        let request = Request::ListEvents {
            group_id: "group_123".into(),
            access_token: vec![7; 32],
            before_cursor: Some(42),
            limit: 30,
        };
        let (mut client, mut server) = tokio::io::duplex(4096);

        write_frame(&mut client, &request).await.unwrap();
        let decoded: Request = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, request);
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

    #[test]
    fn restored_device_field_is_backward_compatible() {
        let current = RemovedDevice {
            device_id: "device_123".into(),
            endpoint_id: "endpoint_123".into(),
            removed_at_ms: 42,
            restored_at_ms: Some(84),
        };
        let encoded = minicbor::to_vec(&current).unwrap();
        let legacy: LegacyRemovedDevice = minicbor::decode(&encoded).unwrap();
        assert_eq!(legacy.device_id, current.device_id);

        let legacy = LegacyRemovedDevice {
            device_id: "device_123".into(),
            endpoint_id: "endpoint_123".into(),
            removed_at_ms: 42,
        };
        let encoded = minicbor::to_vec(&legacy).unwrap();
        let decoded: RemovedDevice = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded.restored_at_ms, None);
    }
}
