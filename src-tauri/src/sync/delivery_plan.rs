//! Central policy for automatic routes, manual actions and file-content limits.

use super::model::{
    ClipboardEnvelope, SyncContentAvailability, SyncItemAction, SyncItemActivity, SyncItemOrigin,
    SyncLocalContentState, SyncTarget,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct ChannelActionContext {
    pub origin: SyncItemOrigin,
    pub local_state: SyncLocalContentState,
    pub target: SyncTarget,
    pub target_available: bool,
    pub activity: SyncItemActivity,
    pub availability: SyncContentAvailability,
    pub remote_file_available: bool,
    pub needs_complete_delivery: bool,
}

/// Resolves the only user action exposed by one card channel.
pub(super) fn channel_action(context: ChannelActionContext) -> SyncItemAction {
    if context.activity == SyncItemActivity::Error {
        return if context.target_available {
            SyncItemAction::Retry
        } else {
            SyncItemAction::None
        };
    }
    if context.activity != SyncItemActivity::Idle {
        return SyncItemAction::None;
    }
    let local_content_available = matches!(
        context.local_state,
        SyncLocalContentState::Managed | SyncLocalContentState::External
    );

    if context.origin == SyncItemOrigin::Remote {
        if context.target == SyncTarget::Cloud
            && context.target_available
            && local_content_available
            && context.availability != SyncContentAvailability::Complete
        {
            return SyncItemAction::Upload;
        }
        if context.remote_file_available
            && matches!(
                context.availability,
                SyncContentAvailability::Complete | SyncContentAvailability::Partial
            )
        {
            return SyncItemAction::Download;
        }
        return SyncItemAction::None;
    }

    if !local_content_available || !context.target_available {
        return SyncItemAction::None;
    }
    if context.needs_complete_delivery || context.availability != SyncContentAvailability::Complete
    {
        return SyncItemAction::Upload;
    }
    SyncItemAction::None
}

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
