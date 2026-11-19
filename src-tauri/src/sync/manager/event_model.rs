use super::*;
use ecopaste_sync_protocol::SyncItemRecord;

/// Decrypts a locally persisted item snapshot, tolerating flipped `(deleted, protected)` AAD flags
/// from pre-1.1.232 rows while relying on 128-bit AEAD authentication to accept only the exact AAD.
pub(super) fn decrypt_stored_snapshot(
    key: &[u8; 32],
    item_key: &str,
    deleted: bool,
    protected: bool,
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<SyncedItemSnapshot> {
    let candidates = [
        (deleted, protected),
        (deleted, !protected),
        (!deleted, protected),
        (!deleted, !protected),
    ];
    let mut last_error = None;
    for (cand_deleted, cand_protected) in candidates {
        match crypto::decrypt_item_snapshot(
            key,
            item_key,
            cand_deleted,
            cand_protected,
            nonce,
            ciphertext,
        ) {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.expect("non-empty AAD candidates"))
}

/// Overlays newer live CRDT metadata fields from `sync_items` onto a decrypted snapshot so
/// uncommitted local favorite/pin/note/delete mutations are never lost during re-encryption or merge.
pub(super) fn overlay_meta_crdt_fields(
    snapshot: &mut SyncedItemSnapshot,
    meta: &repository::StoredSyncItemMeta,
    local_device_id: &str,
) {
    let updated = repository::snapshot_from_meta(meta, local_device_id);
    if updated.presence_version.seq > 0 && updated.presence_version >= snapshot.presence_version {
        snapshot.presence_version = updated.presence_version;
        snapshot.is_present = updated.is_present;
    }
    if updated.favorite_version.seq > 0 && updated.favorite_version >= snapshot.favorite_version {
        snapshot.favorite_version = updated.favorite_version;
        snapshot.is_favorite = updated.is_favorite;
    }
    if updated.pinned_version.seq > 0 && updated.pinned_version >= snapshot.pinned_version {
        snapshot.pinned_version = updated.pinned_version;
        snapshot.is_pinned = updated.is_pinned;
    }
    if updated.note_version.seq > 0 && updated.note_version >= snapshot.note_version {
        snapshot.note_version = updated.note_version;
        snapshot.note = updated.note;
    }
    if !snapshot.is_present {
        snapshot.item = None;
        snapshot.blobs.clear();
        snapshot.source_app = None;
        snapshot.content_identity = None;
    }
}

pub(super) fn synced_item_timestamps(
    item: &SyncedClipboardItem,
    fallback_ms: i64,
) -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let fallback = Utc
        .timestamp_millis_opt(fallback_ms)
        .single()
        .unwrap_or_else(Utc::now);
    let created_at = Utc
        .timestamp_millis_opt(item.created_at_ms)
        .single()
        .unwrap_or(fallback);
    let updated_at = item
        .updated_at_ms
        .and_then(|value| Utc.timestamp_millis_opt(value).single())
        .unwrap_or(created_at)
        .max(created_at);

    (created_at, updated_at)
}

pub(super) fn validate_snapshot(snapshot: &SyncedItemSnapshot) -> Result<()> {
    if snapshot.version != crate::sync::model::SNAPSHOT_VERSION {
        bail!("unsupported sync snapshot version");
    }
    if !is_hex_identifier(&snapshot.sync_key) {
        bail!("invalid synchronized item key");
    }
    if snapshot
        .content_identity
        .as_deref()
        .is_some_and(|identity| !is_hex_identifier(identity))
    {
        bail!("invalid synchronized content identity");
    }
    if snapshot.deleted() {
        return Ok(());
    }
    let Some(item) = snapshot.item.as_ref() else {
        return Ok(());
    };
    if item.sync_key != snapshot.sync_key {
        bail!("synchronized item key mismatch");
    }
    let kind = parse_kind(&item.kind)?;
    item.sub_kind.as_deref().map(parse_sub_kind).transpose()?;
    for manifest in &snapshot.blobs {
        validate_blob_manifest(manifest)?;
    }
    match kind {
        ClipboardKind::Text => {
            if !snapshot.blobs.is_empty() {
                bail!("sync text contains unexpected blobs");
            }
        }
        ClipboardKind::Image => {
            if snapshot.blobs.len() != 1
                || snapshot.blobs[0].role != BlobRole::Image
                || snapshot.blobs[0].file_index.is_some()
                || snapshot.blobs[0].is_directory_archive
                || snapshot.blobs[0].stream_commitment
            {
                bail!("sync image has an invalid blob manifest");
            }
        }
        ClipboardKind::Files => {
            if snapshot.blobs.is_empty() || snapshot.blobs.len() > 1024 {
                bail!("sync files are missing their blobs");
            }
            let mut indexes = HashSet::with_capacity(snapshot.blobs.len());
            for blob in &snapshot.blobs {
                let Some(index) = blob.file_index else {
                    bail!("sync file is missing its index");
                };
                if blob.role != BlobRole::File
                    || (blob.is_directory_archive && blob.stream_commitment)
                    || !indexes.insert(index)
                {
                    bail!("sync file has an invalid blob manifest");
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_source_app_ref(source: &SourceAppRef) -> Result<()> {
    if source.version != 1
        || !is_hex_identifier(&source.source_key)
        || source.display_name.trim().is_empty()
        || source.display_name.chars().count() > 100
        || source.display_name.chars().any(char::is_control)
    {
        bail!("invalid synchronized source app");
    }
    if !matches!(source.platform.as_str(), "macos" | "windows" | "android") {
        bail!("invalid synchronized source app platform");
    }
    for accent in [source.accent_start.as_deref(), source.accent_end.as_deref()]
        .into_iter()
        .flatten()
    {
        if !is_hex_color(accent) {
            bail!("invalid synchronized source app accent");
        }
    }
    Ok(())
}

pub(super) fn validate_source_icon_ref(icon: &SourceIconRef) -> Result<()> {
    if !is_hex_identifier(&icon.icon_hash)
        || !is_hex_identifier(&icon.blob_id)
        || icon.original_size == 0
        || icon.original_size > crate::clipboard::MAX_APP_ICON_BYTES as u64
        || icon.encrypted_size == 0
        || icon.encrypted_size > crate::clipboard::MAX_APP_ICON_BYTES as u64 + 128
    {
        bail!("invalid synchronized source app icon");
    }
    Ok(())
}

pub(super) fn valid_source_icon(source: &SourceAppRef) -> Option<SourceIconRef> {
    validate_source_app_ref(source).ok()?;
    let icon = source.icon.as_ref()?;
    validate_source_icon_ref(icon).ok()?;
    Some(icon.clone())
}

pub(super) fn is_hex_identifier(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn is_hex_color(value: &str) -> bool {
    value.len() == 7
        && value.starts_with('#')
        && value[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn source_app_key(key: &[u8; 32], platform: Platform, local_id: &str) -> String {
    let source_key_key = blake3::derive_key("EcoPaste source application key v1", key);
    let mut input = Vec::with_capacity(local_id.len() + 16);
    input.extend_from_slice(platform_string(platform).as_bytes());
    input.push(0);
    input.extend_from_slice(local_id.as_bytes());
    blake3::keyed_hash(&source_key_key, &input)
        .to_hex()
        .to_string()
}

pub(super) fn sanitize_source_app_name(value: &str) -> String {
    let sanitized = value
        .chars()
        .filter(|character| !character.is_control())
        .take(100)
        .collect::<String>();
    let sanitized = sanitized.trim();
    if sanitized.is_empty() {
        "Unknown".to_owned()
    } else {
        sanitized.to_owned()
    }
}

pub(super) fn cloud_record(
    record: &SyncItemRecord,
    snapshot: SyncedItemSnapshot,
    device_name: String,
    image_path: Option<String>,
) -> Option<CloudRecord> {
    let item = snapshot.item?;
    let is_sensitive = item.is_sensitive;
    let file_count = snapshot.blobs.len().try_into().unwrap_or(u32::MAX);
    let total_size = snapshot.blobs.iter().map(|blob| blob.original_size).sum();
    let preview = match item.kind.as_str() {
        "files" => snapshot
            .blobs
            .iter()
            .map(|blob| blob.name.as_str())
            .collect::<Vec<_>>()
            .join("、"),
        "image" => item.summary.clone().unwrap_or_else(|| "图片".to_owned()),
        _ => item
            .search_text
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| item.content.clone()),
    };
    Some(CloudRecord {
        cursor: record.seq,
        event_id: record.item_key.clone(),
        device_name,
        kind: item.kind,
        preview,
        image_path,
        file_count,
        total_size,
        created_at: Utc
            .timestamp_millis_opt(item.updated_at_ms.unwrap_or(item.created_at_ms))
            .single()
            .unwrap_or_else(Utc::now)
            .to_rfc3339(),
        is_sensitive,
    })
}

pub(super) fn short_device_id(device_id: &str) -> String {
    let short = device_id.chars().take(8).collect::<String>();
    format!("设备 {short}")
}

pub(super) fn validate_blob_manifest(manifest: &BlobManifest) -> Result<()> {
    crypto::blob_path(Path::new("."), &manifest.blob_id)?;
    if manifest.encrypted_size == 0 {
        bail!("invalid encrypted blob size");
    }
    Ok(())
}

pub(super) fn safe_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(sanitize_name)
        .unwrap_or_else(|| "file".into())
}

/// 用父目录隔离同名根项，避免改变系统剪贴板会继续传播的 basename。
pub(super) fn synced_file_destination(root: &Path, file_index: u32, name: &str) -> PathBuf {
    root.join(format!("{file_index:03}"))
        .join(sanitize_name(name))
}

pub(super) fn sanitize_name(value: &str) -> String {
    let name = value.replace(['/', '\\', ':', '\0'], "_");
    if name.is_empty() || name == "." || name == ".." {
        "file".into()
    } else {
        name.chars().take(180).collect()
    }
}

pub(super) fn kind_string(value: ClipboardKind) -> &'static str {
    match value {
        ClipboardKind::Text => "text",
        ClipboardKind::Image => "image",
        ClipboardKind::Files => "files",
    }
}

pub(super) fn parse_kind(value: &str) -> Result<ClipboardKind> {
    match value {
        "text" => Ok(ClipboardKind::Text),
        "image" => Ok(ClipboardKind::Image),
        "files" => Ok(ClipboardKind::Files),
        _ => bail!("invalid clipboard kind"),
    }
}

pub(super) fn sub_kind_string(value: ClipboardSubKind) -> &'static str {
    match value {
        ClipboardSubKind::Rtf => "rtf",
        ClipboardSubKind::Html => "html",
        ClipboardSubKind::Url => "url",
        ClipboardSubKind::Email => "email",
        ClipboardSubKind::Color => "color",
        ClipboardSubKind::Path => "path",
    }
}

pub(super) fn parse_sub_kind(value: &str) -> Result<ClipboardSubKind> {
    match value {
        "rtf" => Ok(ClipboardSubKind::Rtf),
        "html" => Ok(ClipboardSubKind::Html),
        "url" => Ok(ClipboardSubKind::Url),
        "email" => Ok(ClipboardSubKind::Email),
        "color" => Ok(ClipboardSubKind::Color),
        "path" => Ok(ClipboardSubKind::Path),
        _ => bail!("invalid clipboard sub kind"),
    }
}

pub(super) fn platform_string(value: Platform) -> &'static str {
    match value {
        Platform::Macos => "macos",
        Platform::Windows => "windows",
        Platform::Android => "android",
    }
}

pub(super) fn parse_platform(value: &str) -> Platform {
    match value {
        "windows" => Platform::Windows,
        "android" => Platform::Android,
        _ => Platform::Macos,
    }
}
