//! Pure projection from durable synchronization facts into the card status contract.

use crate::db::sync_items::CloudBlobPolicy;

use super::{
    delivery_plan::{channel_action, ChannelActionContext},
    model::{
        SyncContentAvailability, SyncItemActivity, SyncItemChannelStatus, SyncItemOrigin,
        SyncItemStatus, SyncLocalContentState, SyncTarget,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ContentLevel {
    None,
    Metadata,
    Complete,
}

impl ContentLevel {
    pub fn parse(value: &str) -> Self {
        match value {
            "complete" => Self::Complete,
            "metadata" => Self::Metadata,
            _ => Self::None,
        }
    }

    fn availability(self) -> SyncContentAvailability {
        match self {
            Self::None => SyncContentAvailability::None,
            Self::Metadata => SyncContentAvailability::MetadataOnly,
            Self::Complete => SyncContentAvailability::Complete,
        }
    }
}

/// Pure per-target confirmation and last-error facts from `sync_item_targets`.
#[derive(Debug, Clone)]
pub(super) struct TargetFact {
    pub target_id: String,
    pub confirmed: ContentLevel,
    pub pushed_local_seq: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct ItemStatusFacts {
    pub item_id: String,
    pub origin: SyncItemOrigin,
    pub local_state: SyncLocalContentState,
    pub local_seq: i64,
    pub prep_pending: bool,
    pub prep_error: Option<String>,
    pub cloud_blob_policy: CloudBlobPolicy,
    pub has_pending_cloud_blobs: bool,
    pub cloud_enabled: bool,
    pub online_peer_ids: Vec<String>,
    pub remote_file_available: bool,
    pub deliveries: Vec<TargetFact>,
}

/// Produces the only card-level interpretation of persisted synchronization state.
pub(super) fn derive_item_status(facts: ItemStatusFacts) -> SyncItemStatus {
    let lan = derive_lan_channel(&facts);
    let cloud = derive_cloud_channel(&facts);
    let mut status = SyncItemStatus::idle(facts.item_id);
    status.origin = facts.origin;
    status.local_state = facts.local_state;
    status.lan = lan;
    status.cloud = cloud;
    status
}

fn derive_cloud_channel(facts: &ItemStatusFacts) -> SyncItemChannelStatus {
    let mut channel = SyncItemChannelStatus::idle();
    let cloud_fact = facts
        .deliveries
        .iter()
        .find(|delivery| delivery.target_id == "cloud");
    let confirmed = cloud_fact
        .map(|delivery| delivery.confirmed)
        .unwrap_or(ContentLevel::None);
    let pushed_local_seq = cloud_fact
        .map(|delivery| delivery.pushed_local_seq)
        .unwrap_or(0);
    let target_error = cloud_fact.and_then(|delivery| delivery.last_error.clone());

    if cloud_fact.is_some() {
        channel.total_targets = 1;
        if confirmed != ContentLevel::None {
            channel.delivered_targets = 1;
        }
        if confirmed == ContentLevel::Complete {
            channel.content_targets = 1;
        }
    }

    let source_availability = confirmed.availability();
    channel.availability = source_availability;

    let satisfied = confirmed == ContentLevel::Complete;
    let control_pending = facts.cloud_enabled
        && (facts.prep_pending || (facts.local_seq > 0 && pushed_local_seq < facts.local_seq));
    let blob_pending = facts.cloud_enabled
        && facts.has_pending_cloud_blobs
        && facts
            .cloud_blob_policy
            .allows_cloud_upload(facts.origin == SyncItemOrigin::Local);

    if facts.prep_pending && facts.cloud_enabled {
        if let Some(error) = &facts.prep_error {
            channel.activity = SyncItemActivity::Error;
            channel.last_error = Some(error.clone());
        } else {
            channel.activity = SyncItemActivity::Queued;
        }
    } else if !satisfied
        && matches!(
            facts.cloud_blob_policy,
            CloudBlobPolicy::Canceled | CloudBlobPolicy::Blocked
        )
    {
        channel.activity = SyncItemActivity::Error;
        channel.last_error = target_error.or_else(|| {
            (facts.cloud_blob_policy == CloudBlobPolicy::Canceled)
                .then(|| "transfer canceled".to_owned())
        });
    } else if control_pending || blob_pending {
        if target_error.is_some() && !satisfied {
            channel.activity = SyncItemActivity::Error;
            channel.last_error = target_error;
        } else {
            channel.activity = SyncItemActivity::Queued;
        }
    }

    channel.action = channel_action(ChannelActionContext {
        origin: facts.origin,
        local_state: facts.local_state,
        target: SyncTarget::Cloud,
        target_available: facts.cloud_enabled,
        activity: channel.activity,
        availability: source_availability,
        remote_file_available: facts.remote_file_available,
    });

    channel
}

fn derive_lan_channel(facts: &ItemStatusFacts) -> SyncItemChannelStatus {
    let mut channel = SyncItemChannelStatus::idle();
    let lan_deliveries: Vec<&TargetFact> = facts
        .deliveries
        .iter()
        .filter(|delivery| delivery.target_id.starts_with("peer:"))
        .collect();

    for delivery in &lan_deliveries {
        channel.total_targets = channel.total_targets.saturating_add(1);
        if delivery.confirmed != ContentLevel::None {
            channel.delivered_targets = channel.delivered_targets.saturating_add(1);
        }
        if delivery.confirmed == ContentLevel::Complete {
            channel.content_targets = channel.content_targets.saturating_add(1);
        }
    }

    let source_availability = if facts.origin == SyncItemOrigin::Remote {
        lan_deliveries
            .iter()
            .map(|delivery| delivery.confirmed)
            .max_by_key(|level| match level {
                ContentLevel::None => 0,
                ContentLevel::Metadata => 1,
                ContentLevel::Complete => 2,
            })
            .unwrap_or(ContentLevel::None)
            .availability()
    } else if channel.delivered_targets == 0 {
        SyncContentAvailability::None
    } else if channel.total_targets > 0 && channel.content_targets == channel.total_targets {
        SyncContentAvailability::Complete
    } else if channel.content_targets > 0 {
        SyncContentAvailability::Partial
    } else {
        SyncContentAvailability::MetadataOnly
    };

    let local_content_available = matches!(
        facts.local_state,
        SyncLocalContentState::Managed | SyncLocalContentState::External
    );
    // On LAN, there is no server holding an independent replica: a remote item only shows green
    // once its bytes have reached this device, while source availability still enables Download.
    channel.availability = if facts.origin == SyncItemOrigin::Remote
        && !local_content_available
        && source_availability == SyncContentAvailability::Complete
    {
        SyncContentAvailability::MetadataOnly
    } else {
        source_availability
    };

    let lan_available = !facts.online_peer_ids.is_empty();
    if lan_available {
        for peer_id in &facts.online_peer_ids {
            let target_id = format!("peer:{peer_id}");
            let fact = lan_deliveries
                .iter()
                .copied()
                .find(|delivery| delivery.target_id == target_id);
            let pushed = fact.map(|delivery| delivery.pushed_local_seq).unwrap_or(0);
            let confirmed = fact
                .map(|delivery| delivery.confirmed)
                .unwrap_or(ContentLevel::None);
            if facts.local_seq > 0 && pushed < facts.local_seq {
                if let Some(error) = fact.and_then(|delivery| delivery.last_error.as_ref()) {
                    if confirmed != ContentLevel::Complete {
                        channel.activity = SyncItemActivity::Error;
                        channel.last_error = Some(error.clone());
                        continue;
                    }
                }
                if channel.activity != SyncItemActivity::Error {
                    channel.activity = SyncItemActivity::Queued;
                }
            }
        }
        if facts.prep_pending {
            if let Some(error) = &facts.prep_error {
                channel.activity = SyncItemActivity::Error;
                channel.last_error = Some(error.clone());
            } else if channel.activity == SyncItemActivity::Idle {
                channel.activity = SyncItemActivity::Queued;
            }
        }
    }

    channel.action = channel_action(ChannelActionContext {
        origin: facts.origin,
        local_state: facts.local_state,
        target: SyncTarget::Lan,
        target_available: lan_available,
        activity: channel.activity,
        availability: source_availability,
        remote_file_available: facts.remote_file_available,
    });

    channel
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::model::SyncItemAction;

    fn local_facts(delivery: TargetFact) -> ItemStatusFacts {
        ItemStatusFacts {
            item_id: "item".into(),
            origin: SyncItemOrigin::Local,
            local_state: SyncLocalContentState::Managed,
            local_seq: 1,
            prep_pending: false,
            prep_error: None,
            cloud_blob_policy: CloudBlobPolicy::Auto,
            has_pending_cloud_blobs: false,
            cloud_enabled: true,
            online_peer_ids: vec!["peer-a".into()],
            remote_file_available: false,
            deliveries: vec![delivery],
        }
    }

    #[test]
    fn terminal_policy_does_not_overwrite_a_satisfied_delivery() {
        for policy in [CloudBlobPolicy::Canceled, CloudBlobPolicy::Blocked] {
            let mut facts = local_facts(TargetFact {
                target_id: "cloud".into(),
                confirmed: ContentLevel::Complete,
                pushed_local_seq: 1,
                last_error: Some("later attempt failed".into()),
            });
            facts.cloud_blob_policy = policy;
            let projected = derive_item_status(facts);

            assert_eq!(projected.cloud.activity, SyncItemActivity::Idle);
            assert_eq!(
                projected.cloud.availability,
                SyncContentAvailability::Complete
            );
            assert_eq!(projected.cloud.last_error, None);
            assert_eq!(projected.cloud.action, SyncItemAction::None);
        }
    }

    #[test]
    fn canceled_or_blocked_cloud_policy_shows_error_and_retry_until_complete() {
        let mut canceled = local_facts(TargetFact {
            target_id: "cloud".into(),
            confirmed: ContentLevel::Metadata,
            pushed_local_seq: 1,
            last_error: None,
        });
        canceled.cloud_blob_policy = CloudBlobPolicy::Canceled;
        canceled.has_pending_cloud_blobs = true;
        let projected = derive_item_status(canceled);
        assert_eq!(projected.cloud.activity, SyncItemActivity::Error);
        assert_eq!(
            projected.cloud.last_error.as_deref(),
            Some("transfer canceled")
        );
        assert_eq!(projected.cloud.action, SyncItemAction::Retry);

        let mut blocked = local_facts(TargetFact {
            target_id: "cloud".into(),
            confirmed: ContentLevel::Metadata,
            pushed_local_seq: 1,
            last_error: Some("quota exceeded".into()),
        });
        blocked.cloud_blob_policy = CloudBlobPolicy::Blocked;
        blocked.has_pending_cloud_blobs = true;
        let projected = derive_item_status(blocked);
        assert_eq!(projected.cloud.activity, SyncItemActivity::Error);
        assert_eq!(
            projected.cloud.last_error.as_deref(),
            Some("quota exceeded")
        );
        assert_eq!(projected.cloud.action, SyncItemAction::Retry);
    }

    #[test]
    fn pending_control_push_remains_visible_when_identical_content_was_already_delivered() {
        let mut facts = local_facts(TargetFact {
            target_id: "cloud".into(),
            confirmed: ContentLevel::Complete,
            pushed_local_seq: 1,
            last_error: None,
        });
        facts.local_seq = 2;
        let projected = derive_item_status(facts);

        assert_eq!(projected.cloud.activity, SyncItemActivity::Queued);
        assert_eq!(
            projected.cloud.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(projected.cloud.action, SyncItemAction::None);
    }

    #[test]
    fn deferred_metadata_only_cloud_fact_stays_idle_and_uploadable() {
        let mut facts = local_facts(TargetFact {
            target_id: "cloud".into(),
            confirmed: ContentLevel::Metadata,
            pushed_local_seq: 1,
            last_error: None,
        });
        facts.cloud_blob_policy = CloudBlobPolicy::Defer;
        facts.has_pending_cloud_blobs = true;
        let projected = derive_item_status(facts);

        assert_eq!(projected.cloud.activity, SyncItemActivity::Idle);
        assert_eq!(
            projected.cloud.availability,
            SyncContentAvailability::MetadataOnly
        );
        assert_eq!(projected.cloud.action, SyncItemAction::Upload);
    }

    #[test]
    fn pure_lan_mode_never_marks_cloud_queued() {
        let mut facts = local_facts(TargetFact {
            target_id: "peer:peer-a".into(),
            confirmed: ContentLevel::Complete,
            pushed_local_seq: 1,
            last_error: None,
        });
        facts.cloud_enabled = false;
        let projected = derive_item_status(facts);

        assert_eq!(projected.cloud.activity, SyncItemActivity::Idle);
        assert_eq!(projected.cloud.action, SyncItemAction::None);
        assert_eq!(projected.lan.activity, SyncItemActivity::Idle);
        assert_eq!(
            projected.lan.availability,
            SyncContentAvailability::Complete
        );
    }

    #[test]
    fn metadata_only_remote_route_is_informational() {
        let mut facts = local_facts(TargetFact {
            target_id: "cloud".into(),
            confirmed: ContentLevel::Metadata,
            pushed_local_seq: 0,
            last_error: None,
        });
        facts.origin = SyncItemOrigin::Remote;
        facts.local_seq = 0;
        facts.local_state = SyncLocalContentState::Missing;
        facts.remote_file_available = true;

        let metadata = derive_item_status(facts.clone());
        assert_eq!(metadata.cloud.action, SyncItemAction::None);

        facts.deliveries[0].confirmed = ContentLevel::Complete;
        let complete = derive_item_status(facts);
        assert_eq!(complete.cloud.action, SyncItemAction::Download);
    }

    #[test]
    fn undownloaded_and_external_missing_remote_lan_file_keep_download_and_blue_lan_icon() {
        let mut facts = ItemStatusFacts {
            item_id: "remote-file".into(),
            origin: SyncItemOrigin::Remote,
            local_state: SyncLocalContentState::Missing,
            local_seq: 0,
            prep_pending: false,
            prep_error: None,
            cloud_blob_policy: CloudBlobPolicy::Defer,
            has_pending_cloud_blobs: false,
            cloud_enabled: true,
            online_peer_ids: vec!["android".into()],
            remote_file_available: true,
            deliveries: vec![
                TargetFact {
                    target_id: "peer:android".into(),
                    confirmed: ContentLevel::Complete,
                    pushed_local_seq: 0,
                    last_error: None,
                },
                TargetFact {
                    target_id: "cloud".into(),
                    confirmed: ContentLevel::Complete,
                    pushed_local_seq: 0,
                    last_error: None,
                },
            ],
        };

        let missing = derive_item_status(facts.clone());
        assert_eq!(
            missing.lan.availability,
            SyncContentAvailability::MetadataOnly
        );
        assert_eq!(missing.lan.activity, SyncItemActivity::Idle);
        assert_eq!(missing.lan.action, SyncItemAction::Download);
        assert_eq!(
            missing.cloud.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(missing.cloud.action, SyncItemAction::Download);

        // Once saved externally or materialized locally, LAN becomes green.
        facts.local_state = SyncLocalContentState::External;
        let downloaded = derive_item_status(facts.clone());
        assert_eq!(
            downloaded.lan.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(downloaded.lan.action, SyncItemAction::Download);

        // If the external file is later deleted on disk, LAN drops back to blue and Download stays usable.
        facts.local_state = SyncLocalContentState::ExternalMissing;
        let ext_missing = derive_item_status(facts);
        assert_eq!(
            ext_missing.lan.availability,
            SyncContentAvailability::MetadataOnly
        );
        assert_eq!(ext_missing.lan.action, SyncItemAction::Download);
        assert_eq!(ext_missing.cloud.action, SyncItemAction::Download);
    }
}
