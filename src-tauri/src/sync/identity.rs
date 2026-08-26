use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::RwLock,
};

use anyhow::{bail, Context, Result};
use data_encoding::{BASE64URL_NOPAD, HEXLOWER};
use iroh::{EndpointAddr, EndpointId, RelayUrl, SecretKey, TransportAddr};
use minicbor::{Decode, Encode};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tauri::AppHandle;

use crate::settings::SyncSettings;

const IDENTITY_FILENAME: &str = "sync-identity.json";
const PAIRING_PREFIX_V1: &str = "ecopaste-pair-v1:";
const PAIRING_PREFIX_V2: &str = "ecopaste-pair-v2:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "camelCase")]
#[cbor(array)]
pub struct GroupSecrets {
    #[n(0)]
    pub group_id: String,
    #[n(1)]
    pub access_token: String,
    #[n(2)]
    pub content_key: String,
}

impl GroupSecrets {
    pub fn generate() -> Self {
        let mut group_id = [0_u8; 16];
        let mut access_token = [0_u8; 32];
        let mut content_key = [0_u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut group_id);
        rand::rngs::OsRng.fill_bytes(&mut access_token);
        rand::rngs::OsRng.fill_bytes(&mut content_key);
        Self {
            group_id: HEXLOWER.encode(&group_id),
            access_token: HEXLOWER.encode(&access_token),
            content_key: HEXLOWER.encode(&content_key),
        }
    }

    pub fn access_token_bytes(&self) -> Result<Vec<u8>> {
        HEXLOWER
            .decode(self.access_token.as_bytes())
            .context("invalid sync access token")
    }

    pub fn content_key_bytes(&self) -> Result<[u8; 32]> {
        let bytes = HEXLOWER
            .decode(self.content_key.as_bytes())
            .context("invalid sync content key")?;
        bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid sync content key length"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedIdentity {
    pub version: u16,
    pub device_id: String,
    pub device_name: String,
    #[serde(default)]
    pub device_name_customized: Option<bool>,
    pub iroh_secret_key: String,
    pub group: Option<GroupSecrets>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "camelCase")]
#[cbor(array)]
pub struct PairingCode {
    #[n(0)]
    pub version: u16,
    #[n(1)]
    pub group: GroupSecrets,
    #[n(2)]
    pub server_endpoint_id: String,
    #[n(3)]
    pub server_direct_addresses: Vec<String>,
    #[n(4)]
    pub server_relay_urls: Vec<String>,
    #[n(5)]
    pub inviter: ecopaste_sync_protocol::PeerAnnouncement,
}

pub struct IdentityStore {
    path: PathBuf,
    current: RwLock<PersistedIdentity>,
}

impl IdentityStore {
    pub fn load_or_create(app: &AppHandle) -> Result<Self> {
        let directory = crate::core::paths::config_dir(app)?;
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create sync identity directory {directory:?}"))?;
        let path = directory.join(IDENTITY_FILENAME);
        let mut current = if path.exists() {
            let value = fs::read_to_string(&path)
                .with_context(|| format!("failed to read sync identity {path:?}"))?;
            serde_json::from_str(&value).context("failed to parse sync identity")?
        } else {
            let identity = new_identity();
            write_identity(&path, &identity)?;
            identity
        };
        validate_identity(&current)?;
        if refresh_automatic_device_name(&mut current) {
            write_identity(&path, &current)?;
        }
        Ok(Self {
            path,
            current: RwLock::new(current),
        })
    }

    pub fn snapshot(&self) -> PersistedIdentity {
        self.current.read().expect("sync identity poisoned").clone()
    }

    pub fn secret_key(&self) -> Result<SecretKey> {
        SecretKey::from_str(&self.snapshot().iroh_secret_key)
            .context("failed to parse Iroh device identity")
    }

    pub fn set_group(&self, group: Option<GroupSecrets>) -> Result<PersistedIdentity> {
        let mut guard = self.current.write().expect("sync identity poisoned");
        guard.group = group;
        write_identity(&self.path, &guard)?;
        Ok(guard.clone())
    }

    pub fn update_device_name(&self, name: String) -> Result<PersistedIdentity> {
        let name = name.trim();
        if name.is_empty() || name.chars().count() > 80 {
            bail!("设备名称长度必须为 1 到 80 个字符");
        }
        let mut guard = self.current.write().expect("sync identity poisoned");
        guard.device_name = name.to_owned();
        guard.device_name_customized = Some(true);
        write_identity(&self.path, &guard)?;
        Ok(guard.clone())
    }
}

impl PairingCode {
    pub fn encode(&self) -> Result<String> {
        let cbor = minicbor::to_vec(self).context("failed to encode pairing code")?;
        Ok(format!(
            "{PAIRING_PREFIX_V2}{}",
            BASE64URL_NOPAD.encode(&cbor)
        ))
    }

    pub fn decode(value: &str) -> Result<Self> {
        let value = value.trim();
        let (encoded, encoding_version) =
            if let Some(encoded) = value.strip_prefix(PAIRING_PREFIX_V2) {
                (encoded, 2)
            } else if let Some(encoded) = value.strip_prefix(PAIRING_PREFIX_V1) {
                (encoded, 1)
            } else {
                bail!("配对码格式无效");
            };
        let payload = BASE64URL_NOPAD
            .decode(encoded.as_bytes())
            .context("配对码格式无效")?;
        let code: Self = if encoding_version == 2 {
            minicbor::decode(&payload).context("配对码内容无效")?
        } else {
            serde_json::from_slice(&payload).context("配对码内容无效")?
        };
        if code.version != 1 {
            bail!("配对码版本不受支持");
        }
        validate_group(&code.group)?;
        Ok(code)
    }

    pub fn group(&self) -> &GroupSecrets {
        &self.group
    }

    pub fn inviter_device_name(&self) -> &str {
        &self.inviter.device_name
    }
}

pub fn server_endpoint_addr(settings: &SyncSettings) -> Result<Option<EndpointAddr>> {
    endpoint_addr(
        &settings.server_endpoint_id,
        &settings.server_direct_addresses,
        &settings.server_relay_urls,
    )
}

pub fn peer_endpoint_addr(peer: &ecopaste_sync_protocol::PeerAnnouncement) -> Result<EndpointAddr> {
    endpoint_addr(&peer.endpoint_id, &peer.direct_addresses, &peer.relay_urls)?
        .context("peer has no endpoint id")
}

fn endpoint_addr(
    endpoint_id: &str,
    direct_addresses: &[String],
    relay_urls: &[String],
) -> Result<Option<EndpointAddr>> {
    if endpoint_id.trim().is_empty() {
        return Ok(None);
    }
    let endpoint_id = EndpointId::from_str(endpoint_id).context("invalid Iroh endpoint id")?;
    let mut addresses = Vec::new();
    for direct in direct_addresses {
        addresses.push(TransportAddr::Ip(
            direct.parse().context("invalid Iroh direct address")?,
        ));
    }
    for relay in relay_urls {
        addresses.push(TransportAddr::Relay(
            RelayUrl::from_str(relay).context("invalid Iroh relay URL")?,
        ));
    }
    Ok(Some(EndpointAddr::from_parts(endpoint_id, addresses)))
}

fn new_identity() -> PersistedIdentity {
    let secret_key = SecretKey::generate();
    let mut device_id = [0_u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut device_id);
    PersistedIdentity {
        version: 1,
        device_id: HEXLOWER.encode(&device_id),
        device_name: default_device_name(),
        device_name_customized: Some(false),
        iroh_secret_key: HEXLOWER.encode(&secret_key.to_bytes()),
        group: None,
    }
}

fn default_device_name() -> String {
    platform_device_name()
        .or_else(environment_device_name)
        .map(normalize_device_name)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(platform_fallback_device_name)
}

/// 升级旧身份中的自动名称，同时保留用户手动保存过的名称。
fn refresh_automatic_device_name(identity: &mut PersistedIdentity) -> bool {
    let should_refresh = match identity.device_name_customized {
        Some(customized) => !customized,
        None => is_legacy_automatic_name(&identity.device_name),
    };
    if !should_refresh {
        if identity.device_name_customized.is_none() {
            identity.device_name_customized = Some(true);
            return true;
        }
        return false;
    }

    let name = default_device_name();
    let changed = identity.device_name != name || identity.device_name_customized != Some(false);
    identity.device_name = name;
    identity.device_name_customized = Some(false);
    changed
}

fn is_legacy_automatic_name(name: &str) -> bool {
    let name = name.trim();
    if matches!(name, "Mac" | "Windows PC" | "Android" | "EcoPaste Device") {
        return true;
    }

    environment_device_name().is_some_and(|value| value.trim() == name)
        || platform_model_name().is_some_and(|value| value.trim() == name)
}

fn environment_device_name() -> Option<String> {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn normalize_device_name(value: String) -> String {
    value.trim().chars().take(80).collect()
}

fn platform_fallback_device_name() -> String {
    match std::env::consts::OS {
        "macos" => "Mac",
        "windows" => "Windows PC",
        "android" => "Android",
        _ => "EcoPaste Device",
    }
    .to_owned()
}

#[cfg(target_os = "android")]
fn platform_device_name() -> Option<String> {
    crate::commands::android::android_device_name()
}

#[cfg(target_os = "android")]
fn platform_model_name() -> Option<String> {
    crate::commands::android::android_device_model()
}

#[cfg(target_os = "macos")]
fn platform_device_name() -> Option<String> {
    #[allow(deprecated)]
    objc2_foundation::NSHost::currentHost()
        .localizedName()
        .map(|name| name.to_string())
        .filter(|name| !name.trim().is_empty())
}

#[cfg(target_os = "macos")]
fn platform_model_name() -> Option<String> {
    None
}

#[cfg(target_os = "windows")]
fn platform_device_name() -> Option<String> {
    std::env::var("COMPUTERNAME")
        .ok()
        .filter(|name| !name.trim().is_empty())
}

#[cfg(target_os = "windows")]
fn platform_model_name() -> Option<String> {
    None
}

#[cfg(not(any(target_os = "android", target_os = "macos", target_os = "windows")))]
fn platform_device_name() -> Option<String> {
    None
}

#[cfg(not(any(target_os = "android", target_os = "macos", target_os = "windows")))]
fn platform_model_name() -> Option<String> {
    None
}

fn validate_identity(identity: &PersistedIdentity) -> Result<()> {
    if identity.version != 1 || identity.device_id.len() != 32 {
        bail!("invalid sync identity");
    }
    SecretKey::from_str(&identity.iroh_secret_key).context("invalid Iroh secret key")?;
    if let Some(group) = &identity.group {
        validate_group(group)?;
    }
    Ok(())
}

fn validate_group(group: &GroupSecrets) -> Result<()> {
    if group.group_id.len() != 32 {
        bail!("invalid sync group id");
    }
    if group.access_token_bytes()?.len() != 32 || group.content_key_bytes()?.len() != 32 {
        bail!("invalid sync group secrets");
    }
    Ok(())
}

fn write_identity(path: &Path, identity: &PersistedIdentity) -> Result<()> {
    let json = serde_json::to_vec_pretty(identity).context("failed to encode sync identity")?;
    let temporary = path.with_extension("json.tmp");
    {
        let mut file = fs::File::create(&temporary)
            .with_context(|| format!("failed to create sync identity {temporary:?}"))?;
        file.write_all(&json)
            .context("failed to write sync identity")?;
        file.sync_all().ok();
    }
    restrict_permissions(&temporary)?;
    fs::rename(&temporary, path).context("failed to commit sync identity")?;
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .context("failed to restrict sync identity permissions")
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairing_code_fixture() -> PairingCode {
        PairingCode {
            version: 1,
            group: GroupSecrets::generate(),
            server_endpoint_id: "endpoint-server".to_owned(),
            server_direct_addresses: vec!["10.120.90.10:44820".to_owned()],
            server_relay_urls: vec!["https://relay.example.com".to_owned()],
            inviter: ecopaste_sync_protocol::PeerAnnouncement {
                device_id: "00112233445566778899aabbccddeeff".to_owned(),
                device_name: "Work MacBook Pro".to_owned(),
                platform: "macos".to_owned(),
                endpoint_id: "endpoint-inviter".to_owned(),
                direct_addresses: vec!["10.120.90.11:53192".to_owned()],
                relay_urls: vec!["https://relay.example.com".to_owned()],
                last_seen_ms: 1_777_777_777_777,
            },
        }
    }

    #[test]
    fn pairing_code_uses_compact_cbor_and_round_trips() {
        let code = pairing_code_fixture();
        let encoded = code.encode().unwrap();
        let legacy_json = serde_json::to_vec(&code).unwrap();
        let legacy = format!(
            "{PAIRING_PREFIX_V1}{}",
            BASE64URL_NOPAD.encode(&legacy_json)
        );

        assert!(encoded.starts_with(PAIRING_PREFIX_V2));
        assert!(encoded.len() < legacy.len());
        assert_eq!(PairingCode::decode(&encoded).unwrap(), code);
    }

    #[test]
    fn legacy_pairing_code_remains_supported() {
        let code = pairing_code_fixture();
        let json = serde_json::to_vec(&code).unwrap();
        let encoded = format!("{PAIRING_PREFIX_V1}{}", BASE64URL_NOPAD.encode(&json));

        assert_eq!(PairingCode::decode(&encoded).unwrap(), code);
    }

    #[test]
    fn legacy_generic_name_upgrades_to_system_name() {
        let mut identity = new_identity();
        identity.device_name = platform_fallback_device_name();
        identity.device_name_customized = None;

        assert!(refresh_automatic_device_name(&mut identity));
        assert_eq!(identity.device_name, default_device_name());
        assert_eq!(identity.device_name_customized, Some(false));
    }

    #[test]
    fn legacy_custom_name_is_preserved_and_marked() {
        let mut identity = new_identity();
        identity.device_name = "My Personal Device".to_owned();
        identity.device_name_customized = None;

        assert!(refresh_automatic_device_name(&mut identity));
        assert_eq!(identity.device_name, "My Personal Device");
        assert_eq!(identity.device_name_customized, Some(true));
    }

    #[test]
    fn explicitly_customized_name_is_never_replaced() {
        let mut identity = new_identity();
        identity.device_name = "Office Clipboard".to_owned();
        identity.device_name_customized = Some(true);

        assert!(!refresh_automatic_device_name(&mut identity));
        assert_eq!(identity.device_name, "Office Clipboard");
    }

    #[test]
    fn system_name_is_trimmed_to_contract_limit() {
        let value = format!("  {}  ", "x".repeat(100));

        assert_eq!(normalize_device_name(value).chars().count(), 80);
    }

    #[test]
    fn peer_address_keeps_direct_and_relay_routes() {
        let secret = SecretKey::generate();
        let peer = ecopaste_sync_protocol::PeerAnnouncement {
            device_id: "00112233445566778899aabbccddeeff".to_owned(),
            device_name: "Peer".to_owned(),
            platform: "macos".to_owned(),
            endpoint_id: secret.public().to_string(),
            direct_addresses: vec!["10.120.90.11:53192".to_owned()],
            relay_urls: vec!["https://relay.example.com".to_owned()],
            last_seen_ms: 1,
        };

        let address = peer_endpoint_addr(&peer).unwrap();

        assert_eq!(address.ip_addrs().count(), 1);
        assert_eq!(address.relay_urls().count(), 1);
    }
}
