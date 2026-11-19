use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use ecopaste_sync_protocol::{PeerAnnouncement, RemovedDevice};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection, SqlitePool};

use super::{
    identity::is_lan_ip,
    item_status::{ContentLevel, DeliveryFact, DeliveryStatus, ItemStatusFacts},
    model::{
        StoredBlob, StoredContentObject, SyncChannelState, SyncItemOrigin, SyncItemStatus,
        SyncLocalContentState, SyncPeer, SyncPeerStatus, SyncWindowCursor,
    },
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalDownloadTarget {
    pub destination_kind: String,
    pub destination: String,
    pub expected_size: u64,
    pub is_directory: bool,
}

#[derive(Clone, Debug)]
pub struct ExternalDownloadSession {
    pub item_id: String,
    pub targets: Vec<ExternalDownloadTarget>,
}

mod delivery;
mod peers;
mod state;

pub use delivery::*;
pub use peers::*;
pub use state::*;

const CLOUD_CURSOR_KEY: &str = "cloud_synced_seq";
const CLOUD_CATCHUP_BEFORE_KEY: &str = "cloud_catchup_before";
const CLOUD_WINDOW_HIGH_KEY: &str = "cloud_window_high";
const CLOUD_TARGET: &str = "cloud";
const HISTORY_BACKFILL_GROUP_KEY: &str = "history_backfill_group_id";
const HISTORY_BACKFILL_VERSION: u8 = 4;
const LAST_SUCCESS_KEY: &str = "last_success_at";

async fn state_value(pool: &SqlitePool, key: &str) -> Result<Option<String>> {
    sqlx::query_scalar("SELECT value FROM sync_state WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("read sync state {key}"))
}

async fn set_state_value(pool: &SqlitePool, key: &str, value: &str) -> Result<()> {
    let mut connection = pool
        .acquire()
        .await
        .context("acquire sync state connection")?;
    set_state_value_on(&mut connection, key, value).await
}

async fn set_state_value_on(
    connection: &mut SqliteConnection,
    key: &str,
    value: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        "#,
    )
    .bind(key)
    .bind(value)
    .bind(now)
    .bind(now)
    .execute(connection)
    .await
    .with_context(|| format!("write sync state {key}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        items::{content_hash, upsert_item_with_sync_outbox},
        models::{ClipboardItem, ClipboardKind, Platform},
        sync_items::ContentSyncOptions,
        test_support::memory_pool,
    };
    use crate::sync::{
        crypto,
        model::{
            BlobManifest, BlobRole, FieldVersion, SyncContentAvailability, SyncItemActivity,
            SyncedClipboardItem, SyncedItemSnapshot, SNAPSHOT_VERSION,
        },
    };
    use chrono::DateTime;
    use ecopaste_sync_protocol::{AcceptedItem, LanContentLevel, LanItemAck};

    fn sample_item(id: &str, updated_ts: i64) -> ClipboardItem {
        let ts = DateTime::from_timestamp(updated_ts, 0).unwrap();
        let content = format!("content-{id}");
        ClipboardItem {
            id: id.to_owned(),
            kind: ClipboardKind::Text,
            sub_kind: None,
            group_id: None,
            source_app_id: None,
            source_revision: id.to_owned(),
            source_device_id: None,
            source_device_name: None,
            source_updated_at: Some(ts),
            source_device_is_local: true,
            content_hash: content_hash(ClipboardKind::Text, &content),
            sync_key: content_hash(ClipboardKind::Text, &content),
            content,
            search_text: None,
            summary: None,
            text_char_count: None,
            file_types: None,
            size: Some(10),
            width: None,
            height: None,
            use_count: 1,
            is_favorite: false,
            is_pinned: false,
            is_sensitive: false,
            platform: Platform::Macos,
            note: None,
            created_at: ts,
            updated_at: ts,
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            source_app_accent_start: None,
            source_app_accent_end: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            remote_file_state: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        }
    }

    async fn local_seq_of(pool: &SqlitePool, sync_key: &str) -> i64 {
        sqlx::query_scalar("SELECT local_seq FROM sync_items WHERE sync_key = ?")
            .bind(sync_key)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn realtime_items_always_preempt_history_backfill_in_outbound_queue() {
        let pool = memory_pool().await;
        let key = [7_u8; 32];

        let backfill_item = sample_item("old", 1_700_000_100);
        upsert_item_with_sync_outbox(
            &pool,
            &backfill_item,
            Some(ContentSyncOptions {
                history_backfill: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let realtime_item = sample_item("new", 1_700_000_050);
        upsert_item_with_sync_outbox(&pool, &realtime_item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();

        for item in [&backfill_item, &realtime_item] {
            let local_seq = local_seq_of(&pool, &item.sync_key).await;
            let item_key = crypto::derive_item_key(&key, &item.sync_key);
            let snapshot = SyncedItemSnapshot {
                version: SNAPSHOT_VERSION,
                sync_key: item.sync_key.clone(),
                content_version: FieldVersion::new(1, "dev-a"),
                presence_version: FieldVersion::new(1, "dev-a"),
                favorite_version: FieldVersion::default(),
                pinned_version: FieldVersion::default(),
                note_version: FieldVersion::default(),
                is_present: true,
                is_favorite: false,
                is_pinned: false,
                note: None,
                item: Some(SyncedClipboardItem {
                    kind: "text".into(),
                    sub_kind: None,
                    content: item.content.clone(),
                    search_text: None,
                    summary: None,
                    file_types: None,
                    size: item.size,
                    width: None,
                    height: None,
                    is_sensitive: false,
                    source_platform: "macos".into(),
                    created_at_ms: item.created_at.timestamp_millis(),
                    sync_key: item.sync_key.clone(),
                    updated_at_ms: Some(item.updated_at.timestamp_millis()),
                    source_revision: Some(item.source_revision.clone()),
                    source_device_id: Some("dev-a".into()),
                    source_device_name: Some("Mac".into()),
                    source_updated_at_ms: Some(item.updated_at.timestamp_millis()),
                }),
                blobs: Vec::new(),
                source_app: None,
                content_identity: None,
            };
            let (nonce, ciphertext) =
                crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
            assert!(commit_prepared_item(
                &pool,
                PreparedItemCommit {
                    sync_key: &item.sync_key,
                    item_key: &item_key,
                    item_id: Some(&item.id),
                    expected_local_seq: local_seq,
                    origin_device_id: "dev-a",
                    content_identity: None,
                    group_id: "group-1",
                    snapshot: &snapshot,
                    nonce: &nonce,
                    ciphertext: &ciphertext,
                    blobs: &[],
                    content_objects: &[],
                    cloud_desired: Some(ContentLevel::Complete),
                },
            )
            .await
            .unwrap());
        }

        let writes = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(writes.len(), 2);
        // Realtime item MUST come first even though its timestamp was earlier!
        assert_eq!(
            writes[0].item_key,
            crypto::derive_item_key(&key, &realtime_item.sync_key)
        );
        assert_eq!(
            writes[1].item_key,
            crypto::derive_item_key(&key, &backfill_item.sync_key)
        );

        // Confirm cloud & LAN inline ack -> card status becomes immediately green!
        let seqs = writes
            .iter()
            .map(|write| (write.item_key.clone(), write.local_seq))
            .collect::<HashMap<_, _>>();
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: writes[0].item_key.clone(),
                hub_rev: 1,
                seq: 1,
            }],
            &seqs,
        )
        .await
        .unwrap();
        record_lan_acks(
            &pool,
            "peer:dev-b",
            &[LanItemAck {
                item_key: writes[0].item_key.clone(),
                local_seq: writes[0].local_seq,
                level: LanContentLevel::Complete,
            }],
        )
        .await
        .unwrap();

        let statuses = item_statuses(&pool, std::slice::from_ref(&realtime_item.id))
            .await
            .unwrap();
        assert_eq!(
            statuses[0].cloud.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(statuses[0].cloud.activity, SyncItemActivity::Idle);
        assert_eq!(
            statuses[0].lan.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(statuses[0].lan.activity, SyncItemActivity::Idle);
    }

    #[tokio::test]
    async fn remote_items_never_enter_outbound_writes_and_preserve_peer_remote_seq_for_lan_ack() {
        let pool = memory_pool().await;
        let key = [9_u8; 32];
        let sync_key = "remote-file-sync-key";
        let item_key = crypto::derive_item_key(&key, sync_key);
        let blob_id = "blob-lan-001".to_owned();
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: sync_key.into(),
            content_version: FieldVersion::new(1, "dev-sender"),
            presence_version: FieldVersion::new(1, "dev-sender"),
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: Some(SyncedClipboardItem {
                kind: "files".into(),
                sub_kind: None,
                content: "[\"report.pdf\"]".into(),
                search_text: Some("report.pdf".into()),
                summary: Some("report.pdf".into()),
                file_types: Some("[\"pdf\"]".into()),
                size: Some(2048),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "macos".into(),
                created_at_ms: 1_700_000_000_000,
                sync_key: sync_key.into(),
                updated_at_ms: Some(1_700_000_000_000),
                source_revision: Some("rev-1".into()),
                source_device_id: Some("dev-sender".into()),
                source_device_name: Some("SenderMac".into()),
                source_updated_at_ms: Some(1_700_000_000_000),
            }),
            blobs: vec![BlobManifest {
                blob_id: blob_id.clone(),
                name: "report.pdf".into(),
                original_size: 2048,
                encrypted_size: 2100,
                role: BlobRole::File,
                file_index: Some(0),
                is_directory_archive: false,
                stream_commitment: false,
            }],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();

        store_merged_remote_item(
            &pool,
            StoreMergedRemoteItem {
                sync_key,
                item_key: &item_key,
                origin_device_id: "dev-sender",
                hub_rev: None,
                remote_seq: 42,
                source_target_id: "peer:dev-sender",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                available_blob_ids: std::slice::from_ref(&blob_id),
                needs_apply: true,
                re_push_required: false,
            },
        )
        .await
        .unwrap();

        // Remote-only items (`local_seq == 0`) must NOT be pushed to Cloud or other LAN peers.
        assert!(pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap()
            .is_empty());
        assert!(pending_outbound_writes(&pool, "peer:dev-other", 10)
            .await
            .unwrap()
            .is_empty());

        // The sender's `remote_seq` (42) must be persisted for later `AckLanItems`.
        let acks = lan_remote_seqs_for_item(&pool, sync_key).await.unwrap();
        assert_eq!(acks, vec![("dev-sender".to_owned(), 42)]);
    }

    #[tokio::test]
    async fn metadata_recommit_preserves_deferred_cloud_policy_and_lan_advertises_only_local_blobs()
    {
        let pool = memory_pool().await;
        let key = [11_u8; 32];
        let item = sample_item("large-file", 1_700_000_200);
        upsert_item_with_sync_outbox(&pool, &item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let local_seq = local_seq_of(&pool, &item.sync_key).await;
        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let local_blob = BlobManifest {
            blob_id: "blob-local-ok".into(),
            name: "a.bin".into(),
            original_size: 4096,
            encrypted_size: 4120,
            role: BlobRole::File,
            file_index: Some(0),
            is_directory_archive: false,
            stream_commitment: false,
        };
        let missing_blob = BlobManifest {
            blob_id: "blob-not-local".into(),
            name: "b.bin".into(),
            original_size: 4096,
            encrypted_size: 4120,
            role: BlobRole::File,
            file_index: Some(1),
            is_directory_archive: false,
            stream_commitment: false,
        };
        let mut snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: item.sync_key.clone(),
            content_version: FieldVersion::new(1, "dev-a"),
            presence_version: FieldVersion::new(1, "dev-a"),
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: Some(SyncedClipboardItem {
                kind: "files".into(),
                sub_kind: None,
                content: "[\"a.bin\",\"b.bin\"]".into(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(8192),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "macos".into(),
                created_at_ms: item.created_at.timestamp_millis(),
                sync_key: item.sync_key.clone(),
                updated_at_ms: Some(item.updated_at.timestamp_millis()),
                source_revision: Some(item.source_revision.clone()),
                source_device_id: Some("dev-a".into()),
                source_device_name: Some("Mac".into()),
                source_updated_at_ms: Some(item.updated_at.timestamp_millis()),
            }),
            blobs: vec![local_blob.clone(), missing_blob.clone()],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let stored_blobs = vec![
            SyncItemBlobRow {
                blob_id: local_blob.blob_id.clone(),
                role: "file".into(),
                optional: false,
                size: local_blob.encrypted_size,
                original_size: local_blob.original_size,
                encrypted_path: "/tmp/local.enc".into(),
                source_path: Some("/tmp/a.bin".into()),
                source_is_directory: false,
                stream_nonce: None,
                stream_commitment: false,
                cloud_available: false,
                local_available: true,
            },
            SyncItemBlobRow {
                blob_id: missing_blob.blob_id.clone(),
                role: "file".into(),
                optional: false,
                size: missing_blob.encrypted_size,
                original_size: missing_blob.original_size,
                encrypted_path: "/tmp/missing.enc".into(),
                source_path: Some("/tmp/b.bin".into()),
                source_is_directory: false,
                stream_nonce: None,
                stream_commitment: false,
                cloud_available: false,
                local_available: true,
            },
        ];

        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &item.sync_key,
                item_key: &item_key,
                item_id: Some(&item.id),
                expected_local_seq: local_seq,
                origin_device_id: "dev-a",
                content_identity: None,
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                blobs: &stored_blobs,
                content_objects: &[],
                cloud_desired: Some(ContentLevel::Metadata),
            },
        )
        .await
        .unwrap());

        // Simulate metadata edit (favorite) re-committing with `cloud_desired: None`.
        let mut conn = pool.acquire().await.unwrap();
        crate::db::sync_items::mark_local_favorite_on(
            &mut conn,
            &item.sync_key,
            &item.id,
            false,
            true,
            false,
        )
        .await
        .unwrap();
        drop(conn);
        let local_seq2 = local_seq_of(&pool, &item.sync_key).await;
        snapshot.is_favorite = true;
        snapshot.favorite_version = FieldVersion::new(2, "dev-a");
        let (nonce2, ciphertext2) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, true, &snapshot).unwrap();
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &item.sync_key,
                item_key: &item_key,
                item_id: Some(&item.id),
                expected_local_seq: local_seq2,
                origin_device_id: "dev-a",
                content_identity: None,
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &nonce2,
                ciphertext: &ciphertext2,
                blobs: &stored_blobs,
                content_objects: &[],
                cloud_desired: None,
            },
        )
        .await
        .unwrap());

        // Deferred cloud metadata policy must stay intact (not upgraded to 'complete').
        let desired: String = sqlx::query_scalar(
            "SELECT desired_content FROM sync_item_targets WHERE sync_key = ? AND target_id = 'cloud'",
        )
        .bind(&item.sync_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(desired, "metadata");

        // Mark one blob unavailable locally; LAN outbound write must advertise only local_available=1.
        sqlx::query(
            "UPDATE sync_item_blobs SET local_available = 0 WHERE sync_key = ? AND blob_id = ?",
        )
        .bind(&item.sync_key)
        .bind(&missing_blob.blob_id)
        .execute(&pool)
        .await
        .unwrap();
        let lan_writes = pending_outbound_writes(&pool, "peer:dev-b", 10)
            .await
            .unwrap();
        assert_eq!(lan_writes.len(), 1);
        assert_eq!(lan_writes[0].blob_ids, vec![local_blob.blob_id]);
    }

    #[tokio::test]
    async fn vip_queues_isolate_realtime_uploads_and_recent_blob_pulls_by_source() {
        let pool = memory_pool().await;
        let key = [13_u8; 32];

        // 1. Outbound cloud blob upload: realtime vs backfill separation.
        let backfill = sample_item("backfill-img", 1_700_000_500);
        upsert_item_with_sync_outbox(
            &pool,
            &backfill,
            Some(ContentSyncOptions {
                history_backfill: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let realtime = sample_item("realtime-img", 1_700_000_400);
        upsert_item_with_sync_outbox(&pool, &realtime, Some(ContentSyncOptions::default()))
            .await
            .unwrap();

        for (item, blob_id) in [(&backfill, "blob-bf"), (&realtime, "blob-rt")] {
            let local_seq = local_seq_of(&pool, &item.sync_key).await;
            let item_key = crypto::derive_item_key(&key, &item.sync_key);
            let manifest = BlobManifest {
                blob_id: blob_id.into(),
                name: "pic.png".into(),
                original_size: 512,
                encrypted_size: 540,
                role: BlobRole::Image,
                file_index: None,
                is_directory_archive: false,
                stream_commitment: false,
            };
            let snapshot = SyncedItemSnapshot {
                version: SNAPSHOT_VERSION,
                sync_key: item.sync_key.clone(),
                content_version: FieldVersion::new(1, "dev-a"),
                presence_version: FieldVersion::new(1, "dev-a"),
                favorite_version: FieldVersion::default(),
                pinned_version: FieldVersion::default(),
                note_version: FieldVersion::default(),
                is_present: true,
                is_favorite: false,
                is_pinned: false,
                note: None,
                item: Some(SyncedClipboardItem {
                    kind: "image".into(),
                    sub_kind: None,
                    content: "pic.png".into(),
                    search_text: None,
                    summary: None,
                    file_types: None,
                    size: Some(512),
                    width: Some(10),
                    height: Some(10),
                    is_sensitive: false,
                    source_platform: "macos".into(),
                    created_at_ms: item.created_at.timestamp_millis(),
                    sync_key: item.sync_key.clone(),
                    updated_at_ms: Some(item.updated_at.timestamp_millis()),
                    source_revision: Some(item.source_revision.clone()),
                    source_device_id: Some("dev-a".into()),
                    source_device_name: Some("Mac".into()),
                    source_updated_at_ms: Some(item.updated_at.timestamp_millis()),
                }),
                blobs: vec![manifest],
                source_app: None,
                content_identity: None,
            };
            let (nonce, ciphertext) =
                crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
            let blob = SyncItemBlobRow {
                blob_id: blob_id.into(),
                role: "image".into(),
                optional: false,
                size: 540,
                original_size: 512,
                encrypted_path: format!("/tmp/{blob_id}.enc"),
                source_path: None,
                source_is_directory: false,
                stream_nonce: None,
                stream_commitment: false,
                cloud_available: false,
                local_available: true,
            };
            assert!(commit_prepared_item(
                &pool,
                PreparedItemCommit {
                    sync_key: &item.sync_key,
                    item_key: &item_key,
                    item_id: Some(&item.id),
                    expected_local_seq: local_seq,
                    origin_device_id: "dev-a",
                    content_identity: None,
                    group_id: "group-1",
                    snapshot: &snapshot,
                    nonce: &nonce,
                    ciphertext: &ciphertext,
                    blobs: &[blob],
                    content_objects: &[],
                    cloud_desired: Some(ContentLevel::Complete),
                },
            )
            .await
            .unwrap());
        }

        let vip_uploads = pending_cloud_blob_uploads(&pool, true, 10).await.unwrap();
        assert_eq!(vip_uploads.len(), 1);
        assert_eq!(vip_uploads[0].sync_key, realtime.sync_key);

        let all_uploads = pending_cloud_blob_uploads(&pool, false, 10).await.unwrap();
        assert_eq!(all_uploads.len(), 2);
        assert_eq!(all_uploads[0].sync_key, realtime.sync_key);
        assert_eq!(all_uploads[1].sync_key, backfill.sync_key);

        // 2. Inbound blob pull queue: source isolation + VIP recency cutoff.
        for (sync_key, source_target, updated_ms, blob_id) in [
            ("old-lan-img", "peer:dev-b", 1_700_000_000_000_i64, "b-old"),
            ("new-lan-img", "peer:dev-b", 1_700_000_900_000_i64, "b-new"),
            (
                "new-cloud-img",
                CLOUD_TARGET,
                1_700_000_950_000_i64,
                "b-cld",
            ),
        ] {
            let item_key = crypto::derive_item_key(&key, sync_key);
            let snapshot = SyncedItemSnapshot {
                version: SNAPSHOT_VERSION,
                sync_key: sync_key.into(),
                content_version: FieldVersion::new(1, "dev-b"),
                presence_version: FieldVersion::new(1, "dev-b"),
                favorite_version: FieldVersion::default(),
                pinned_version: FieldVersion::default(),
                note_version: FieldVersion::default(),
                is_present: true,
                is_favorite: false,
                is_pinned: false,
                note: None,
                item: Some(SyncedClipboardItem {
                    kind: "image".into(),
                    sub_kind: None,
                    content: "pic.png".into(),
                    search_text: None,
                    summary: None,
                    file_types: None,
                    size: Some(256),
                    width: Some(8),
                    height: Some(8),
                    is_sensitive: false,
                    source_platform: "macos".into(),
                    created_at_ms: updated_ms,
                    sync_key: sync_key.into(),
                    updated_at_ms: Some(updated_ms),
                    source_revision: Some(sync_key.into()),
                    source_device_id: Some("dev-b".into()),
                    source_device_name: Some("Peer".into()),
                    source_updated_at_ms: Some(updated_ms),
                }),
                blobs: vec![BlobManifest {
                    blob_id: blob_id.into(),
                    name: "pic.png".into(),
                    original_size: 256,
                    encrypted_size: 280,
                    role: BlobRole::Image,
                    file_index: None,
                    is_directory_archive: false,
                    stream_commitment: false,
                }],
                source_app: None,
                content_identity: None,
            };
            let (nonce, ciphertext) =
                crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
            let avail = vec![blob_id.to_owned()];
            store_merged_remote_item(
                &pool,
                StoreMergedRemoteItem {
                    sync_key,
                    item_key: &item_key,
                    origin_device_id: "dev-b",
                    hub_rev: (source_target == CLOUD_TARGET).then_some(1),
                    remote_seq: 1,
                    source_target_id: source_target,
                    snapshot: &snapshot,
                    nonce: &nonce,
                    ciphertext: &ciphertext,
                    available_blob_ids: &avail,
                    needs_apply: true,
                    re_push_required: false,
                },
            )
            .await
            .unwrap();
            mark_item_needs_blob_pull(&pool, sync_key, None, "metadata_only", None)
                .await
                .unwrap();
        }

        // VIP cutoff excludes `old-lan-img` and source filter excludes `new-cloud-img`.
        let vip_lan =
            list_blob_pull_items_for_source(&pool, "peer:dev-b", Some(1_700_000_800_000), 5)
                .await
                .unwrap();
        assert_eq!(vip_lan.len(), 1);
        assert_eq!(vip_lan[0].sync_key, "new-lan-img");

        // General worker sees both LAN items newest-first.
        let general_lan = list_blob_pull_items_for_source(&pool, "peer:dev-b", None, 5)
            .await
            .unwrap();
        assert_eq!(
            general_lan
                .iter()
                .map(|m| m.sync_key.as_str())
                .collect::<Vec<_>>(),
            vec!["new-lan-img", "old-lan-img"]
        );

        // Cloud source only sees the cloud item.
        let cloud_pull = list_blob_pull_items_for_source(&pool, CLOUD_TARGET, None, 5)
            .await
            .unwrap();
        assert_eq!(cloud_pull.len(), 1);
        assert_eq!(cloud_pull[0].sync_key, "new-cloud-img");
    }
}
