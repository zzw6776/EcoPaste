//! Pure projection from durable synchronization facts into the card status contract.

use super::{
    delivery_plan::{channel_action, ChannelActionContext},
    model::{
        SyncContentAvailability, SyncItemActivity, SyncItemChannelStatus, SyncItemOrigin,
        SyncItemStatus, SyncLocalContentState, SyncTarget,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ContentLevel {
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

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Metadata => "metadata",
            Self::Complete => "complete",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryStatus {
    Pending,
    AwaitingReceipt,
    Confirmed,
    Blocked,
    Canceled,
}

impl DeliveryStatus {
    pub fn parse(value: &str) -> Self {
        match value {
            "awaiting_receipt" => Self::AwaitingReceipt,
            "confirmed" => Self::Confirmed,
            "blocked" => Self::Blocked,
            "canceled" => Self::Canceled,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DeliveryFact {
    pub target_id: String,
    pub desired: ContentLevel,
    pub confirmed: ContentLevel,
    pub status: DeliveryStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct ItemStatusFacts {
    pub item_id: String,
    pub origin: SyncItemOrigin,
    pub local_state: SyncLocalContentState,
    pub queued_lan: bool,
    pub queued_cloud: bool,
    pub preparation_error: Option<String>,
    pub lan_available: bool,
    pub remote_file_available: bool,
    pub deliveries: Vec<DeliveryFact>,
}

/// Produces the only card-level interpretation of persisted synchronization state.
pub(super) fn derive_item_status(facts: ItemStatusFacts) -> SyncItemStatus {
    let lan = derive_channel(&facts, false);
    let cloud = derive_channel(&facts, true);
    let mut status = SyncItemStatus::idle(facts.item_id);
    status.origin = facts.origin;
    status.local_state = facts.local_state;
    status.lan = lan;
    status.cloud = cloud;
    status
}

fn derive_channel(facts: &ItemStatusFacts, cloud: bool) -> SyncItemChannelStatus {
    let mut channel = SyncItemChannelStatus::idle();
    let deliveries = facts.deliveries.iter().filter(|delivery| {
        if cloud {
            delivery.target_id == "cloud"
        } else {
            delivery.target_id.starts_with("peer:")
        }
    });

    for delivery in deliveries {
        let satisfied = delivery.confirmed >= delivery.desired;
        channel.total_targets = channel.total_targets.saturating_add(1);
        if delivery.confirmed != ContentLevel::None {
            channel.delivered_targets = channel.delivered_targets.saturating_add(1);
        }
        if delivery.confirmed == ContentLevel::Complete {
            channel.content_targets = channel.content_targets.saturating_add(1);
        }
        match delivery.status {
            DeliveryStatus::Pending | DeliveryStatus::AwaitingReceipt => {
                if delivery.last_error.is_some() && !satisfied {
                    channel.activity = SyncItemActivity::Error;
                    channel.last_error = delivery.last_error.clone();
                } else if channel.activity != SyncItemActivity::Error {
                    channel.activity = SyncItemActivity::Queued;
                }
            }
            DeliveryStatus::Blocked | DeliveryStatus::Canceled => {
                if satisfied {
                    continue;
                }
                channel.activity = SyncItemActivity::Error;
                channel.last_error = delivery.last_error.clone().or_else(|| {
                    (delivery.status == DeliveryStatus::Canceled)
                        .then(|| "transfer canceled".to_owned())
                });
            }
            DeliveryStatus::Confirmed => {}
        }
    }

    channel.availability = if facts.origin == SyncItemOrigin::Remote {
        facts
            .deliveries
            .iter()
            .filter(|delivery| {
                if cloud {
                    delivery.target_id == "cloud"
                } else {
                    delivery.target_id.starts_with("peer:")
                }
            })
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

    let queued = if cloud {
        facts.queued_cloud
    } else {
        facts.queued_lan
    };
    if queued && channel.activity == SyncItemActivity::Idle {
        channel.activity = SyncItemActivity::Queued;
    }
    if queued {
        if let Some(error) = &facts.preparation_error {
            channel.activity = SyncItemActivity::Error;
            channel.last_error = Some(error.clone());
        }
    }

    let needs_complete_delivery = facts.deliveries.iter().any(|delivery| {
        let same_channel = if cloud {
            delivery.target_id == "cloud"
        } else {
            delivery.target_id.starts_with("peer:")
        };
        same_channel
            && delivery.desired == ContentLevel::Complete
            && delivery.confirmed != ContentLevel::Complete
    });
    channel.action = channel_action(ChannelActionContext {
        origin: facts.origin,
        local_state: facts.local_state,
        target: if cloud {
            SyncTarget::Cloud
        } else {
            SyncTarget::Lan
        },
        target_available: cloud || facts.lan_available,
        activity: channel.activity,
        availability: channel.availability,
        remote_file_available: facts.remote_file_available,
        needs_complete_delivery,
    });

    channel
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::model::SyncItemAction;

    fn local_facts(delivery: DeliveryFact) -> ItemStatusFacts {
        ItemStatusFacts {
            item_id: "item".into(),
            origin: SyncItemOrigin::Local,
            local_state: SyncLocalContentState::Managed,
            queued_lan: false,
            queued_cloud: false,
            preparation_error: None,
            lan_available: true,
            remote_file_available: false,
            deliveries: vec![delivery],
        }
    }

    #[test]
    fn terminal_attempt_does_not_overwrite_a_satisfied_delivery() {
        for status in [DeliveryStatus::Canceled, DeliveryStatus::Blocked] {
            let projected = derive_item_status(local_facts(DeliveryFact {
                target_id: "cloud".into(),
                desired: ContentLevel::Complete,
                confirmed: ContentLevel::Complete,
                status,
                last_error: Some("later attempt failed".into()),
            }));

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
    fn pending_attempt_remains_visible_when_identical_content_was_already_delivered() {
        let projected = derive_item_status(local_facts(DeliveryFact {
            target_id: "cloud".into(),
            desired: ContentLevel::Complete,
            confirmed: ContentLevel::Complete,
            status: DeliveryStatus::Pending,
            last_error: None,
        }));

        assert_eq!(projected.cloud.activity, SyncItemActivity::Queued);
        assert_eq!(
            projected.cloud.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(projected.cloud.action, SyncItemAction::None);
    }

    #[test]
    fn metadata_only_remote_route_is_informational() {
        let mut facts = local_facts(DeliveryFact {
            target_id: "cloud".into(),
            desired: ContentLevel::Metadata,
            confirmed: ContentLevel::Metadata,
            status: DeliveryStatus::Confirmed,
            last_error: None,
        });
        facts.origin = SyncItemOrigin::Remote;
        facts.local_state = SyncLocalContentState::Missing;
        facts.remote_file_available = true;

        let metadata = derive_item_status(facts.clone());
        assert_eq!(metadata.cloud.action, SyncItemAction::None);

        facts.deliveries[0].confirmed = ContentLevel::Complete;
        let complete = derive_item_status(facts);
        assert_eq!(complete.cloud.action, SyncItemAction::Download);
    }
}
