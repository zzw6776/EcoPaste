//! Central policy for deciding whether a route receives metadata or complete file content.

use super::model::ClipboardEnvelope;
#[cfg(any(target_os = "android", test))]
use super::SyncTarget;

/// Uses binary MiB consistently for every upload entry point.
pub(crate) fn permits_automatic_file_upload(total_size: u64, limit_mb: u32) -> bool {
    limit_mb > 0 && total_size <= u64::from(limit_mb) * 1024 * 1024
}

/// Chooses the one allowed route for an oversized Android share; normal shares stay dual-route.
#[cfg(any(target_os = "android", test))]
pub(crate) fn android_share_delivery_target(
    is_file: bool,
    total_size: Option<u64>,
    auto_upload_max_mb: u32,
    lan_connected: bool,
) -> Option<SyncTarget> {
    let total_size = total_size.filter(|_| is_file)?;
    if permits_automatic_file_upload(total_size, auto_upload_max_mb) {
        return None;
    }
    Some(if lan_connected {
        SyncTarget::Lan
    } else {
        SyncTarget::Cloud
    })
}

/// Returns original plaintext bytes for one synchronized file collection.
pub(super) fn remote_file_total_size(envelope: &ClipboardEnvelope) -> u64 {
    envelope
        .item
        .size
        .and_then(|size| u64::try_from(size).ok())
        .unwrap_or_else(|| {
            envelope
                .blobs
                .iter()
                .filter(|blob| blob.role == super::model::BlobRole::File)
                .fold(0_u64, |total, blob| {
                    total.saturating_add(blob.original_size)
                })
        })
}

pub(super) fn should_defer_remote_files(
    envelope: &ClipboardEnvelope,
    auto_upload_max_mb: u32,
) -> bool {
    envelope.item.kind == "files"
        && !permits_automatic_file_upload(remote_file_total_size(envelope), auto_upload_max_mb)
}

/// Files exceeding either receive threshold remain metadata until the user chooses a destination.
pub(super) fn should_defer_remote_file_download(
    envelope: &ClipboardEnvelope,
    automatic_limit_mb: u32,
    local_storage_limit_bytes: u64,
) -> bool {
    if envelope.item.kind != "files" {
        return false;
    }
    let total_size = remote_file_total_size(envelope);
    should_defer_remote_files(envelope, automatic_limit_mb)
        || (local_storage_limit_bytes > 0 && total_size > local_storage_limit_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_share_route_requires_a_known_oversized_file() {
        assert_eq!(android_share_delivery_target(true, None, 100, true), None);
        assert_eq!(
            android_share_delivery_target(true, Some(100 * 1024 * 1024), 100, true),
            None
        );
        assert_eq!(
            android_share_delivery_target(true, Some(101 * 1024 * 1024), 100, true),
            Some(SyncTarget::Lan)
        );
        assert_eq!(
            android_share_delivery_target(true, Some(101 * 1024 * 1024), 100, false),
            Some(SyncTarget::Cloud)
        );
    }
}
