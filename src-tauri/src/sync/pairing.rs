use anyhow::{Context, Result};
use data_encoding::BASE64URL_NOPAD;
use iroh::address_lookup::UserData;
use minicbor::{Decode, Encode};

use super::identity::GroupSecrets;

pub const JOIN_ALPN: &[u8] = b"ecopaste/pair/2";
pub const JOIN_PROTOCOL_VERSION: u16 = 2;
pub const JOIN_TIMEOUT_SECS: u64 = 60;
const DISCOVERY_PREFIX: &str = "ecopaste-discovery-v1:";

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct DiscoveryMetadata {
    #[n(0)]
    pub version: u16,
    #[n(1)]
    pub space_id: String,
    #[n(2)]
    pub device_name: String,
    #[n(3)]
    pub platform: String,
    #[n(4)]
    pub presence_nonce: Option<u64>,
}

impl DiscoveryMetadata {
    /// Encodes only public, non-secret discovery metadata into Iroh mDNS user data.
    pub fn encode(&self) -> Result<UserData> {
        let mut metadata = self.clone();
        loop {
            let payload = minicbor::to_vec(&metadata).context("encode LAN discovery metadata")?;
            let value = format!("{DISCOVERY_PREFIX}{}", BASE64URL_NOPAD.encode(&payload));
            match UserData::try_from(value) {
                Ok(value) => return Ok(value),
                Err(_) if !metadata.device_name.is_empty() => {
                    metadata.device_name.pop();
                }
                Err(error) => return Err(error).context("LAN discovery metadata is too large"),
            }
        }
    }

    pub fn decode(value: &UserData) -> Result<Self> {
        let encoded = value
            .as_ref()
            .strip_prefix(DISCOVERY_PREFIX)
            .context("not an EcoPaste discovery announcement")?;
        let payload = BASE64URL_NOPAD
            .decode(encoded.as_bytes())
            .context("invalid LAN discovery metadata")?;
        let metadata: Self =
            minicbor::decode(&payload).context("invalid LAN discovery metadata")?;
        anyhow::ensure!(
            metadata.version == JOIN_PROTOCOL_VERSION,
            "unsupported LAN pairing protocol"
        );
        Ok(metadata)
    }
}

/// Derives a public pseudonymous space identifier without exposing any group secret.
pub fn discovery_space_id(group: &GroupSecrets) -> Result<String> {
    let access_token: [u8; 32] = group
        .access_token_bytes()?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid sync access token length"))?;
    let digest = blake3::keyed_hash(&access_token, b"ecopaste-lan-discovery-v1");
    Ok(data_encoding::HEXLOWER.encode(&digest.as_bytes()[..12]))
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct JoinRequest {
    #[n(0)]
    pub version: u16,
    #[n(1)]
    pub request_id: String,
    #[n(2)]
    pub nonce: Vec<u8>,
    #[n(3)]
    pub device_id: String,
    #[n(4)]
    pub device_name: String,
    #[n(5)]
    pub platform: String,
    #[n(6)]
    pub endpoint_id: String,
    #[n(7)]
    pub direct_addresses: Vec<String>,
    #[n(8)]
    pub relay_urls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum JoinResponse {
    #[n(0)]
    Approved {
        #[n(0)]
        pairing_code: String,
    },
    #[n(1)]
    Rejected,
    #[n(2)]
    Expired,
    #[n(3)]
    Error {
        #[n(0)]
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct JoinAcknowledgement {
    #[n(0)]
    pub version: u16,
    #[n(1)]
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum JoinCompletion {
    #[n(0)]
    Committed,
    #[n(1)]
    Error {
        #[n(0)]
        message: String,
    },
}

/// Creates the six-digit comparison code shown on both devices.
pub fn comparison_code(
    request_id: &str,
    nonce: &[u8],
    applicant_endpoint_id: &str,
    approver_endpoint_id: &str,
) -> String {
    let mut input = Vec::with_capacity(
        request_id.len()
            + nonce.len()
            + applicant_endpoint_id.len()
            + approver_endpoint_id.len()
            + 4,
    );
    for value in [
        request_id.as_bytes(),
        nonce,
        applicant_endpoint_id.as_bytes(),
        approver_endpoint_id.as_bytes(),
    ] {
        input.extend_from_slice(value);
        input.push(0);
    }
    let digest = blake3::hash(&input);
    let number =
        u32::from_be_bytes(digest.as_bytes()[..4].try_into().expect("four hash bytes")) % 1_000_000;
    format!("{number:06}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_metadata_round_trips_without_group_secrets() {
        let metadata = DiscoveryMetadata {
            version: JOIN_PROTOCOL_VERSION,
            space_id: "space".into(),
            device_name: "MacBook Pro".into(),
            platform: "macos".into(),
            presence_nonce: Some(42),
        };
        let encoded = metadata.encode().unwrap();
        assert_eq!(DiscoveryMetadata::decode(&encoded).unwrap(), metadata);
        assert!(!encoded.as_ref().contains("access_token"));
    }

    #[test]
    fn discovery_metadata_truncates_long_multibyte_device_names() {
        let metadata = DiscoveryMetadata {
            version: JOIN_PROTOCOL_VERSION,
            space_id: "a".repeat(24),
            device_name: "设备名称".repeat(20),
            platform: "android".into(),
            presence_nonce: Some(42),
        };
        let encoded = metadata.encode().unwrap();
        let decoded = DiscoveryMetadata::decode(&encoded).unwrap();
        assert!(!decoded.device_name.is_empty());
        assert!(decoded.device_name.len() < metadata.device_name.len());
        assert!(metadata.device_name.starts_with(&decoded.device_name));
        assert!(encoded.as_ref().len() <= UserData::MAX_LENGTH);
    }

    #[test]
    fn discovery_presence_nonce_is_backward_compatible() {
        #[derive(Debug, PartialEq, Eq, Encode, Decode)]
        #[cbor(map)]
        struct LegacyDiscoveryMetadata {
            #[n(0)]
            version: u16,
            #[n(1)]
            space_id: String,
            #[n(2)]
            device_name: String,
            #[n(3)]
            platform: String,
        }

        let legacy = LegacyDiscoveryMetadata {
            version: JOIN_PROTOCOL_VERSION,
            space_id: "space".into(),
            device_name: "Android".into(),
            platform: "android".into(),
        };
        let legacy_payload = minicbor::to_vec(&legacy).unwrap();
        let legacy_value = UserData::try_from(format!(
            "{DISCOVERY_PREFIX}{}",
            BASE64URL_NOPAD.encode(&legacy_payload)
        ))
        .unwrap();
        assert_eq!(
            DiscoveryMetadata::decode(&legacy_value)
                .unwrap()
                .presence_nonce,
            None
        );

        let current = DiscoveryMetadata {
            version: JOIN_PROTOCOL_VERSION,
            space_id: legacy.space_id.clone(),
            device_name: legacy.device_name.clone(),
            platform: legacy.platform.clone(),
            presence_nonce: Some(7),
        };
        let encoded = current.encode().unwrap();
        let payload = BASE64URL_NOPAD
            .decode(
                encoded
                    .as_ref()
                    .strip_prefix(DISCOVERY_PREFIX)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        assert_eq!(
            minicbor::decode::<LegacyDiscoveryMetadata>(&payload).unwrap(),
            legacy
        );
    }

    #[test]
    fn comparison_code_is_stable_and_six_digits() {
        let code = comparison_code("request", b"nonce", "applicant", "approver");
        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|value| value.is_ascii_digit()));
        assert_eq!(
            code,
            comparison_code("request", b"nonce", "applicant", "approver")
        );
    }

    #[test]
    fn join_handshake_messages_round_trip() {
        let acknowledgement = JoinAcknowledgement {
            version: JOIN_PROTOCOL_VERSION,
            request_id: "request".into(),
        };
        let encoded = minicbor::to_vec(&acknowledgement).unwrap();
        let decoded: JoinAcknowledgement = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, acknowledgement);

        let completion = JoinCompletion::Error {
            message: "database unavailable".into(),
        };
        let encoded = minicbor::to_vec(&completion).unwrap();
        let decoded: JoinCompletion = minicbor::decode(&encoded).unwrap();
        assert_eq!(decoded, completion);
    }
}
