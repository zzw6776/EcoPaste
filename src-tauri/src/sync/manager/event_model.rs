use super::*;

pub(super) fn event_matches_item_timestamp(
    event: &EncryptedEvent,
    key: [u8; 32],
    item: &ClipboardItem,
) -> bool {
    crypto::decrypt_event(&key, event).is_ok_and(|envelope| {
        envelope.item.updated_at_ms == Some(item.updated_at.timestamp_millis())
    })
}

pub(super) fn synced_item_timestamps(
    item: &SyncedClipboardItem,
    event_created_at_ms: i64,
) -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let fallback = Utc
        .timestamp_millis_opt(event_created_at_ms)
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

pub(super) fn validate_envelope(envelope: &ClipboardEnvelope) -> Result<()> {
    if envelope.version != 1 {
        bail!("unsupported sync event version");
    }
    if !is_hex_identifier(&envelope.item.sync_key) {
        bail!("invalid synchronized item key");
    }
    let kind = parse_kind(&envelope.item.kind)?;
    envelope
        .item
        .sub_kind
        .as_deref()
        .map(parse_sub_kind)
        .transpose()?;
    for manifest in &envelope.blobs {
        validate_blob_manifest(manifest)?;
    }
    match kind {
        ClipboardKind::Text => {
            if !envelope.blobs.is_empty() {
                bail!("sync text contains unexpected blobs");
            }
        }
        ClipboardKind::Image => {
            if envelope.blobs.len() != 1
                || envelope.blobs[0].role != BlobRole::Image
                || envelope.blobs[0].file_index.is_some()
                || envelope.blobs[0].is_directory_archive
                || envelope.blobs[0].stream_commitment
            {
                bail!("sync image has an invalid blob manifest");
            }
        }
        ClipboardKind::Files => {
            if envelope.blobs.is_empty() || envelope.blobs.len() > 1024 {
                bail!("sync files are missing their blobs");
            }
            let mut indexes = HashSet::with_capacity(envelope.blobs.len());
            for blob in &envelope.blobs {
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

pub(super) fn validate_decrypted_sync_event(event: &DecryptedSyncEvent) -> Result<()> {
    match event {
        DecryptedSyncEvent::Clipboard(envelope) => validate_envelope(envelope),
        DecryptedSyncEvent::ItemState(patch) => {
            if !is_hex_identifier(&patch.sync_key)
                || (patch.is_favorite.is_none()
                    && patch.is_pinned.is_none()
                    && !patch.note_changed
                    && patch.is_present.is_none())
                || patch.is_present == Some(true)
                || (!patch.note_changed && patch.note.is_some())
            {
                bail!("invalid synchronized item state");
            }
            Ok(())
        }
    }
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
    cursor: u64,
    event: EncryptedEvent,
    envelope: ClipboardEnvelope,
    device_name: String,
    image_path: Option<String>,
) -> CloudRecord {
    let is_sensitive = envelope.item.is_sensitive;
    let file_count = envelope.blobs.len().try_into().unwrap_or(u32::MAX);
    let total_size = envelope.blobs.iter().map(|blob| blob.original_size).sum();
    let preview = match envelope.item.kind.as_str() {
        "files" => envelope
            .blobs
            .iter()
            .map(|blob| blob.name.as_str())
            .collect::<Vec<_>>()
            .join("、"),
        "image" => envelope
            .item
            .summary
            .clone()
            .unwrap_or_else(|| "图片".to_owned()),
        _ => envelope
            .item
            .search_text
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| envelope.item.content.clone()),
    };
    CloudRecord {
        cursor,
        event_id: event.event_id,
        device_name,
        kind: envelope.item.kind,
        preview,
        image_path,
        file_count,
        total_size,
        created_at: Utc
            .timestamp_millis_opt(event.created_at_ms)
            .single()
            .unwrap_or_else(Utc::now)
            .to_rfc3339(),
        is_sensitive,
    }
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
