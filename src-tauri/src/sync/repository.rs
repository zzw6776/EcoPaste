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
    item_status::{ContentLevel, ItemStatusFacts, TargetFact},
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
        items::{content_hash, upsert_item_with_sync_outbox, upsert_remote_file_item},
        models::{ClipboardItem, ClipboardKind, Platform},
        sync_items::{
            mark_local_deleted_on, mark_local_favorite_on, next_pending_state,
            stage_existing_content, ContentSyncOptions,
        },
        test_support::memory_pool,
    };
    use crate::sync::{
        crypto,
        model::{
            BlobManifest, BlobRole, FieldVersion, SyncContentAvailability,
            SyncInboundCatchupSources, SyncItemAction, SyncItemActivity, SyncedClipboardItem,
            SyncedItemSnapshot, SNAPSHOT_VERSION,
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
                    cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Auto),
                },
            )
            .await
            .unwrap());
        }

        let batch = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        // Realtime item MUST be sent in its own dedicated batch without piggybacking backfill,
        // while flagging `has_more` so the control loop advances to backfill immediately after.
        assert_eq!(batch.writes.len(), 1);
        assert!(batch.has_more);
        assert_eq!(
            batch.writes[0].item_key,
            crypto::derive_item_key(&key, &realtime_item.sync_key)
        );
        let writes = batch.writes;

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
                available_blob_ids: Vec::new(),
            }],
            &seqs,
        )
        .await
        .unwrap();

        // Once realtime is accepted, the next batch serves historical backfill.
        let backfill_batch = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(backfill_batch.writes.len(), 1);
        assert!(!backfill_batch.has_more);
        assert_eq!(
            backfill_batch.writes[0].item_key,
            crypto::derive_item_key(&key, &backfill_item.sync_key)
        );
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

        let statuses = item_statuses(
            &pool,
            std::slice::from_ref(&realtime_item.id),
            true,
            &["dev-b".to_owned()],
            &HashMap::new(),
        )
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
            .writes
            .is_empty());
        assert!(pending_outbound_writes(&pool, "peer:dev-other", 10)
            .await
            .unwrap()
            .writes
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
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
            },
        )
        .await
        .unwrap());

        // Simulate metadata edit (favorite) re-committing with `cloud_blob_policy: None`.
        let mut conn = pool.acquire().await.unwrap();
        crate::db::sync_items::mark_local_favorite_on(
            &mut conn,
            &item.sync_key,
            &item.id,
            "files",
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
                cloud_blob_policy: None,
            },
        )
        .await
        .unwrap());

        // Deferred cloud blob policy must stay intact (not upgraded to 'auto').
        let policy: String =
            sqlx::query_scalar("SELECT cloud_blob_policy FROM sync_items WHERE sync_key = ?")
                .bind(&item.sync_key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(policy, "defer");

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
            .unwrap()
            .writes;
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
                    cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Auto),
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

    #[tokio::test]
    async fn cloud_blob_policy_decouples_metadata_fanout_from_attachment_uploads() {
        let pool = memory_pool().await;
        let key = [17_u8; 32];
        let mut item = sample_item("android-big-file", 1_700_000_600);
        item.kind = ClipboardKind::Files;
        item.size = Some(200 * 1024 * 1024);

        upsert_item_with_sync_outbox(
            &pool,
            &item,
            Some(ContentSyncOptions {
                manual_share: true,
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let local_seq = local_seq_of(&pool, &item.sync_key).await;
        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let manifest = BlobManifest {
            blob_id: "blob-big-lan".into(),
            name: "video.mp4".into(),
            original_size: 200 * 1024 * 1024,
            encrypted_size: 200 * 1024 * 1024 + 64,
            role: BlobRole::File,
            file_index: Some(0),
            is_directory_archive: false,
            stream_commitment: true,
        };
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: item.sync_key.clone(),
            content_version: FieldVersion::new(1, "dev-android"),
            presence_version: FieldVersion::new(1, "dev-android"),
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
                content: "video.mp4".into(),
                search_text: None,
                summary: Some("video.mp4".into()),
                file_types: Some("[\"mp4\"]".into()),
                size: item.size,
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "android".into(),
                created_at_ms: item.created_at.timestamp_millis(),
                sync_key: item.sync_key.clone(),
                updated_at_ms: Some(item.updated_at.timestamp_millis()),
                source_revision: Some(item.source_revision.clone()),
                source_device_id: Some("dev-android".into()),
                source_device_name: Some("Pixel".into()),
                source_updated_at_ms: Some(item.updated_at.timestamp_millis()),
            }),
            blobs: vec![manifest.clone()],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let blob_row = SyncItemBlobRow {
            blob_id: manifest.blob_id.clone(),
            role: "file".into(),
            optional: false,
            size: manifest.encrypted_size,
            original_size: manifest.original_size,
            encrypted_path: String::new(),
            source_path: Some("/data/video.mp4".into()),
            source_is_directory: false,
            stream_nonce: Some(vec![0x55; 24]),
            stream_commitment: true,
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
                origin_device_id: "dev-android",
                content_identity: None,
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                blobs: &[blob_row],
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
            },
        )
        .await
        .unwrap());

        // Control-plane 1 KB metadata fans out to Cloud even under `Defer`, while Data-Plane
        // holds back oversized blob uploads until the user clicks Upload.
        let cloud_batch = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(cloud_batch.writes.len(), 1);
        assert_eq!(cloud_batch.writes[0].item_key, item_key);
        assert!(pending_cloud_blob_uploads(&pool, false, 10)
            .await
            .unwrap()
            .is_empty());

        let mut accepted_seqs = HashMap::new();
        accepted_seqs.insert(item_key.clone(), u64::try_from(local_seq).unwrap());
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: item_key.clone(),
                hub_rev: 1,
                seq: 1,
                available_blob_ids: Vec::new(),
            }],
            &accepted_seqs,
        )
        .await
        .unwrap();

        // Cloud settles cleanly to blue/idle (`MetadataOnly`) and remains clickable; LAN ack with
        // Metadata also settles cleanly to blue/idle.
        record_lan_acks(
            &pool,
            "peer:dev-mac",
            &[LanItemAck {
                item_key: item_key.clone(),
                local_seq: local_seq as u64,
                level: LanContentLevel::Metadata,
            }],
        )
        .await
        .unwrap();
        let statuses = item_statuses(
            &pool,
            std::slice::from_ref(&item.id),
            true,
            &["dev-mac".to_owned()],
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            statuses[0].cloud.availability,
            SyncContentAvailability::MetadataOnly
        );
        assert_eq!(statuses[0].cloud.activity, SyncItemActivity::Idle);
        assert_eq!(statuses[0].cloud.action, SyncItemAction::Upload);
        assert_eq!(
            statuses[0].lan.availability,
            SyncContentAvailability::MetadataOnly
        );
        assert_eq!(statuses[0].lan.activity, SyncItemActivity::Idle);
        assert_eq!(
            status_counts(&pool, false, true, &["dev-mac".to_owned()])
                .await
                .unwrap()
                .0,
            0
        );

        // Clicking Cloud sets `cloud_blob_policy = 'requested'` and queues blob upload immediately.
        assert!(request_item_delivery(&pool, &item.id, true, true)
            .await
            .unwrap());
        assert_eq!(
            pending_cloud_blob_uploads(&pool, false, 10)
                .await
                .unwrap()
                .len(),
            1
        );

        // Conversely, an item flagged `Requested` (e.g. Android oversized share while LAN offline)
        // queues BOTH Cloud blob uploads and LAN 1 KB metadata when LAN reconnects.
        let mut offline_share = sample_item("android-offline-share", 1_700_000_650);
        offline_share.kind = ClipboardKind::Files;
        offline_share.size = Some(200 * 1024 * 1024);
        upsert_item_with_sync_outbox(
            &pool,
            &offline_share,
            Some(ContentSyncOptions {
                manual_share: true,
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Requested),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let offline_seq = local_seq_of(&pool, &offline_share.sync_key).await;
        let offline_key = crypto::derive_item_key(&key, &offline_share.sync_key);
        let (off_nonce, off_cipher) =
            crypto::encrypt_item_snapshot(&key, &offline_key, false, false, &snapshot).unwrap();
        let mut off_blob = SyncItemBlobRow {
            blob_id: "blob-offline-share".into(),
            role: "file".into(),
            optional: false,
            size: 1024,
            original_size: 900,
            encrypted_path: String::new(),
            source_path: Some("/data/offline.mp4".into()),
            source_is_directory: false,
            stream_nonce: Some(vec![0x66; 24]),
            stream_commitment: true,
            cloud_available: false,
            local_available: true,
        };
        off_blob.size = 200 * 1024 * 1024 + 64;
        off_blob.original_size = 200 * 1024 * 1024;
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &offline_share.sync_key,
                item_key: &offline_key,
                item_id: Some(&offline_share.id),
                expected_local_seq: offline_seq,
                origin_device_id: "dev-android",
                content_identity: None,
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &off_nonce,
                ciphertext: &off_cipher,
                blobs: &[off_blob],
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Requested),
            },
        )
        .await
        .unwrap());
        let lan_batch = pending_outbound_writes(&pool, "peer:dev-mac", 10)
            .await
            .unwrap();
        assert!(lan_batch.writes.iter().any(|w| w.item_key == offline_key));
    }

    #[tokio::test]
    async fn favorite_and_delete_preserve_committed_aad_and_reset_policy() {
        let pool = memory_pool().await;
        let key = [19_u8; 32];
        let item = sample_item("fav-aad", 1_700_000_700);
        upsert_item_with_sync_outbox(
            &pool,
            &item,
            Some(ContentSyncOptions {
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
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
                content: "important text".into(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(14),
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
                cloud_blob_policy: None,
            },
        )
        .await
        .unwrap());

        // Toggling favorite must NOT corrupt stored `protected` AAD before re-encryption.
        let mut conn = pool.acquire().await.unwrap();
        mark_local_favorite_on(
            &mut conn,
            &item.sync_key,
            &item.id,
            "text",
            false,
            true,
            false,
        )
        .await
        .unwrap();
        drop(conn);
        let meta = load_sync_item_meta(&pool, &item.sync_key)
            .await
            .unwrap()
            .unwrap();
        assert!(meta.is_favorite);
        assert!(!meta.protected);
        let decrypted = crypto::decrypt_item_snapshot(
            &key,
            meta.item_key.as_deref().unwrap(),
            meta.deleted,
            meta.protected,
            meta.nonce.as_deref().unwrap(),
            meta.ciphertext.as_deref().unwrap(),
        )
        .unwrap();
        assert_eq!(
            decrypted.item.as_ref().map(|i| i.content.as_str()),
            Some("important text")
        );

        // Deleting must reset `cloud_blob_policy` to `'auto'`, keep stored `deleted` AAD intact until re-encrypt,
        // and surface `deleted = true` via `next_pending_state`.
        let mut conn = pool.acquire().await.unwrap();
        mark_local_deleted_on(&mut conn, &item.sync_key, false)
            .await
            .unwrap();
        drop(conn);
        let policy: String =
            sqlx::query_scalar("SELECT cloud_blob_policy FROM sync_items WHERE sync_key = ?")
                .bind(&item.sync_key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(policy, "auto");
        let pending = next_pending_state(&pool, true).await.unwrap().unwrap();
        assert!(pending.deleted);
        assert!(!pending.is_present);
    }

    #[tokio::test]
    async fn remote_metadata_merge_preserves_local_stream_nonce() {
        let pool = memory_pool().await;
        let key = [23_u8; 32];
        let mut item = sample_item("stream-file", 1_700_000_800);
        item.kind = ClipboardKind::Files;
        upsert_item_with_sync_outbox(&pool, &item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let local_seq = local_seq_of(&pool, &item.sync_key).await;
        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let manifest = BlobManifest {
            blob_id: "blob-stream-keep".into(),
            name: "doc.pdf".into(),
            original_size: 4096,
            encrypted_size: 4160,
            role: BlobRole::File,
            file_index: Some(0),
            is_directory_archive: false,
            stream_commitment: true,
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
                content: "doc.pdf".into(),
                search_text: None,
                summary: Some("doc.pdf".into()),
                file_types: Some("[\"pdf\"]".into()),
                size: Some(4096),
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
            blobs: vec![manifest.clone()],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let expected_nonce = vec![0xAB_u8; 24];
        let blob_row = SyncItemBlobRow {
            blob_id: manifest.blob_id.clone(),
            role: "file".into(),
            optional: false,
            size: manifest.encrypted_size,
            original_size: manifest.original_size,
            encrypted_path: String::new(),
            source_path: Some("/tmp/doc.pdf".into()),
            source_is_directory: false,
            stream_nonce: Some(expected_nonce.clone()),
            stream_commitment: true,
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
                blobs: &[blob_row],
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
            },
        )
        .await
        .unwrap());

        // Remote peer favorites the file and pushes back; `stream_nonce` MUST survive!
        snapshot.is_favorite = true;
        snapshot.favorite_version = FieldVersion::new(2, "dev-b");
        let (nonce2, ciphertext2) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, true, &snapshot).unwrap();
        store_merged_remote_item(
            &pool,
            StoreMergedRemoteItem {
                sync_key: &item.sync_key,
                item_key: &item_key,
                origin_device_id: "dev-b",
                hub_rev: Some(2),
                remote_seq: 5,
                source_target_id: "peer:dev-b",
                snapshot: &snapshot,
                nonce: &nonce2,
                ciphertext: &ciphertext2,
                available_blob_ids: &[],
                needs_apply: true,
                re_push_required: false,
            },
        )
        .await
        .unwrap();

        let blobs = load_item_blobs(&pool, &item.sync_key).await.unwrap();
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].stream_nonce, Some(expected_nonce));
        assert!(blobs[0].local_available);
        assert_eq!(blobs[0].source_path.as_deref(), Some("/tmp/doc.pdf"));

        // Merging a metadata-only remote update over an existing `none` cloud target must
        // monotonically promote `confirmed_content` from `none` to `metadata`.
        store_merged_remote_item(
            &pool,
            StoreMergedRemoteItem {
                sync_key: &item.sync_key,
                item_key: &item_key,
                origin_device_id: "dev-b",
                hub_rev: Some(3),
                remote_seq: 6,
                source_target_id: CLOUD_TARGET,
                snapshot: &snapshot,
                nonce: &nonce2,
                ciphertext: &ciphertext2,
                available_blob_ids: &[],
                needs_apply: false,
                re_push_required: false,
            },
        )
        .await
        .unwrap();
        let cloud_confirmed: String = sqlx::query_scalar(
            "SELECT confirmed_content FROM sync_item_targets WHERE sync_key = ? AND target_id = 'cloud'",
        )
        .bind(&item.sync_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(cloud_confirmed, "metadata");
    }

    #[tokio::test]
    async fn external_remote_file_restage_preserves_state_and_manual_delivery_allocates_local_seq()
    {
        let pool = memory_pool().await;
        let key = [29_u8; 32];
        let mut item = sample_item("ext-file", 1_700_000_900);
        item.kind = ClipboardKind::Files;
        item.content = "archive.zip".into();
        item.remote_file_state = Some("available".into());

        let upserted = upsert_remote_file_item(&pool, &item, "dev-android")
            .await
            .unwrap();
        item.id = upserted.id.clone();

        record_external_download_session(
            &pool,
            &item.sync_key,
            &[ExternalDownloadTarget {
                destination_kind: "path".into(),
                destination: "/tmp/archive.zip".into(),
                expected_size: 1024,
                is_directory: false,
            }],
        )
        .await
        .unwrap();

        // Re-staging an externally downloaded remote file for manual Cloud upload must keep
        // `origin = 'remote'`, `local_state = 'external'`, and `remote_file_state = 'available'`.
        stage_existing_content(
            &pool,
            &item,
            ContentSyncOptions {
                force_files: true,
                manual_share: true,
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Requested),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (origin, local_state, remote_file_state): (String, String, Option<String>) =
            sqlx::query_as(
                "SELECT origin, local_state, remote_file_state FROM sync_items WHERE sync_key = ?",
            )
            .bind(&item.sync_key)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(origin, "remote");
        assert_eq!(local_state, "external");
        assert_eq!(remote_file_state.as_deref(), Some("available"));

        // Manually requesting cloud delivery on a ready remote item (`local_seq = 0`) allocates
        // a positive `local_seq` so Control-Plane pushes its snapshot.
        let mut remote_text = sample_item("remote-text-for-cloud", 1_700_000_950);
        let item_key = crypto::derive_item_key(&key, &remote_text.sync_key);
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: remote_text.sync_key.clone(),
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
                kind: "text".into(),
                sub_kind: None,
                content: remote_text.content.clone(),
                search_text: None,
                summary: None,
                file_types: None,
                size: remote_text.size,
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "android".into(),
                created_at_ms: remote_text.created_at.timestamp_millis(),
                sync_key: remote_text.sync_key.clone(),
                updated_at_ms: Some(remote_text.updated_at.timestamp_millis()),
                source_revision: Some(remote_text.source_revision.clone()),
                source_device_id: Some("dev-b".into()),
                source_device_name: Some("Phone".into()),
                source_updated_at_ms: Some(remote_text.updated_at.timestamp_millis()),
            }),
            blobs: Vec::new(),
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let inserted = upsert_item_with_sync_outbox(&pool, &remote_text, None)
            .await
            .unwrap()
            .0;
        remote_text.id = inserted.id;
        store_merged_remote_item(
            &pool,
            StoreMergedRemoteItem {
                sync_key: &remote_text.sync_key,
                item_key: &item_key,
                origin_device_id: "dev-b",
                hub_rev: None,
                remote_seq: 9,
                source_target_id: "peer:dev-b",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                available_blob_ids: &[],
                needs_apply: false,
                re_push_required: false,
            },
        )
        .await
        .unwrap();
        mark_item_applied(
            &pool,
            &remote_text.sync_key,
            Some(&remote_text.id),
            "managed",
            None,
        )
        .await
        .unwrap();
        assert_eq!(local_seq_of(&pool, &remote_text.sync_key).await, 0);
        assert!(request_item_delivery(&pool, &remote_text.id, true, true)
            .await
            .unwrap());
        assert!(local_seq_of(&pool, &remote_text.sync_key).await > 0);
        assert_eq!(
            pending_outbound_writes(&pool, CLOUD_TARGET, 10)
                .await
                .unwrap()
                .writes
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn missing_remote_blob_downgrades_source_and_clears_poison_pill_pull_queue() {
        let pool = memory_pool().await;
        let key = [31_u8; 32];
        let sync_key = "poison-img-key";
        let item_key = crypto::derive_item_key(&key, sync_key);
        let blob_id = "blob-missing-404".to_owned();
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
                content: "gone.png".into(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(128),
                width: Some(4),
                height: Some(4),
                is_sensitive: false,
                source_platform: "macos".into(),
                created_at_ms: 1_700_000_999_000,
                sync_key: sync_key.into(),
                updated_at_ms: Some(1_700_000_999_000),
                source_revision: Some("rev-gone".into()),
                source_device_id: Some("dev-b".into()),
                source_device_name: Some("Peer".into()),
                source_updated_at_ms: Some(1_700_000_999_000),
            }),
            blobs: vec![BlobManifest {
                blob_id: blob_id.clone(),
                name: "gone.png".into(),
                original_size: 128,
                encrypted_size: 160,
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

        for target in ["peer:dev-b", CLOUD_TARGET] {
            store_merged_remote_item(
                &pool,
                StoreMergedRemoteItem {
                    sync_key,
                    item_key: &item_key,
                    origin_device_id: "dev-b",
                    hub_rev: (target == CLOUD_TARGET).then_some(1),
                    remote_seq: 1,
                    source_target_id: target,
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
        }
        mark_item_needs_blob_pull(&pool, sync_key, None, "metadata", None)
            .await
            .unwrap();

        // Missing on LAN downgrades only LAN; Cloud remains eligible for pull.
        let exhausted_lan =
            downgrade_source_availability_on_missing_blob(&pool, sync_key, "peer:dev-b", &blob_id)
                .await
                .unwrap();
        assert!(!exhausted_lan);
        assert!(
            list_blob_pull_items_for_source(&pool, "peer:dev-b", None, 5)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            list_blob_pull_items_for_source(&pool, CLOUD_TARGET, None, 5)
                .await
                .unwrap()
                .len(),
            1
        );

        // Missing on Cloud too exhausts all sources and clears `needs_blob_pull` & `cloud_available`.
        let exhausted_all =
            downgrade_source_availability_on_missing_blob(&pool, sync_key, CLOUD_TARGET, &blob_id)
                .await
                .unwrap();
        assert!(exhausted_all);
        let meta = load_sync_item_meta(&pool, sync_key).await.unwrap().unwrap();
        assert!(!meta.needs_blob_pull);
        let blobs = load_item_blobs(&pool, sync_key).await.unwrap();
        assert!(!blobs[0].cloud_available);
    }

    #[tokio::test]
    async fn downloaded_external_blobs_bind_local_source_and_content_object() {
        use super::super::crypto;
        use super::super::model::{
            BlobManifest, BlobRole, FieldVersion, SyncedClipboardItem, SyncedItemSnapshot,
            SNAPSHOT_VERSION,
        };

        let pool = memory_pool().await;
        let key = [13_u8; 32];
        let sync_key = "downloaded-lan-file";
        let item_key = crypto::derive_item_key(&key, sync_key);
        let plaintext_hash = "d".repeat(64);
        let stream_nonce = vec![9_u8; 19];
        let blob_id = crypto::stream_blob_id(&key, &stream_nonce, &plaintext_hash).unwrap();
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: sync_key.into(),
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
                content: "archive.bin".into(),
                search_text: Some("archive.bin".into()),
                summary: Some("archive.bin".into()),
                file_types: None,
                size: Some(1024),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "android".into(),
                created_at_ms: 100,
                sync_key: sync_key.into(),
                updated_at_ms: Some(100),
                source_revision: None,
                source_device_id: Some("dev-a".into()),
                source_device_name: Some("Phone".into()),
                source_updated_at_ms: Some(100),
            }),
            blobs: vec![BlobManifest {
                blob_id: blob_id.clone(),
                name: "archive.bin".into(),
                original_size: 1024,
                encrypted_size: 1067,
                role: BlobRole::File,
                file_index: Some(0),
                is_directory_archive: false,
                stream_commitment: true,
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
                origin_device_id: "dev-a",
                hub_rev: None,
                remote_seq: 1,
                source_target_id: "peer:dev-a",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                available_blob_ids: std::slice::from_ref(&blob_id),
                needs_apply: false,
                re_push_required: false,
            },
        )
        .await
        .unwrap();

        record_downloaded_external_blobs(
            &pool,
            "group-1",
            sync_key,
            &[DownloadedBlobBinding {
                blob_id: blob_id.clone(),
                plaintext_hash: plaintext_hash.clone(),
                stream_nonce: stream_nonce.clone(),
                original_size: 1024,
                encrypted_size: 1067,
                source_path: "/tmp/archive.bin".into(),
            }],
        )
        .await
        .unwrap();

        let blobs = load_item_blobs(&pool, sync_key).await.unwrap();
        assert_eq!(blobs.len(), 1);
        assert!(blobs[0].local_available);
        assert_eq!(blobs[0].source_path.as_deref(), Some("/tmp/archive.bin"));
        assert_eq!(
            blobs[0].stream_nonce.as_deref(),
            Some(stream_nonce.as_slice())
        );

        let reusable = reusable_content_object(&pool, "group-1", &plaintext_hash)
            .await
            .unwrap()
            .expect("content object recorded");
        assert_eq!(reusable.blob_id, blob_id);
        assert_eq!(reusable.stream_nonce, stream_nonce);
    }

    #[tokio::test]
    async fn backfill_progress_and_backlog_watermarks_track_convergence_cleanly() {
        let pool = memory_pool().await;
        let key = [37_u8; 32];
        let bf_a = sample_item("bf-a", 1_700_000_100);
        let bf_b = sample_item("bf-b", 1_700_000_200);
        let rt = sample_item("rt-now", 1_700_000_300);

        for (item, backfill) in [(&bf_a, true), (&bf_b, true), (&rt, false)] {
            upsert_item_with_sync_outbox(
                &pool,
                item,
                Some(ContentSyncOptions {
                    history_backfill: backfill,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        }

        // Scope isolation: RealtimeOnly never returns backfill items; BackfillOnly never returns realtime.
        let rt_prep = crate::db::sync_items::next_pending_content(
            &pool,
            false,
            Some(crate::db::sync_items::OutboxContentLane::Text),
            crate::db::sync_items::OutboxContentScope::RealtimeOnly,
        )
        .await
        .unwrap()
        .expect("realtime pending content");
        assert_eq!(rt_prep.sync_key, rt.sync_key);

        let bf_prep = crate::db::sync_items::next_pending_content(
            &pool,
            false,
            Some(crate::db::sync_items::OutboxContentLane::Text),
            crate::db::sync_items::OutboxContentScope::BackfillOnly,
        )
        .await
        .unwrap()
        .expect("backfill pending content");
        assert_eq!(bf_prep.sync_key, bf_b.sync_key);

        // Before anything is confirmed, backfill progress reports 0 / 2.
        let progress = backfill_progress(&pool, false, true, &[])
            .await
            .unwrap()
            .expect("active backfill progress");
        assert_eq!(progress.completed, 0);
        assert_eq!(progress.total, 2);
        assert_eq!(
            count_ready_backfill_backlog(&pool, true, &[])
                .await
                .unwrap(),
            (0, 0)
        );

        // Prepare both backfill items -> ready backlog reaches 2.
        for item in [&bf_a, &bf_b] {
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
                    search_text: item.search_text.clone(),
                    summary: item.summary.clone(),
                    file_types: None,
                    size: None,
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
                    cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Auto),
                },
            )
            .await
            .unwrap());
        }

        assert_eq!(
            count_ready_backfill_backlog(&pool, true, &[])
                .await
                .unwrap(),
            (2, 0)
        );

        // Confirm first backfill item -> progress becomes 1 / 2 and ready backlog drops to 1.
        let batch = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(batch.writes.len(), 2);
        let first = &batch.writes[0];
        let mut seqs = HashMap::new();
        seqs.insert(first.item_key.clone(), first.local_seq);
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: first.item_key.clone(),
                hub_rev: 1,
                seq: 1,
                available_blob_ids: Vec::new(),
            }],
            &seqs,
        )
        .await
        .unwrap();

        let progress = backfill_progress(&pool, false, true, &[])
            .await
            .unwrap()
            .expect("halfway backfill progress");
        assert_eq!(progress.completed, 1);
        assert_eq!(progress.total, 2);
        assert_eq!(
            count_ready_backfill_backlog(&pool, true, &[])
                .await
                .unwrap(),
            (1, 0)
        );

        // Confirm second backfill item -> progress disappears (`None`) and backlog hits 0.
        let second = &batch.writes[1];
        seqs.insert(second.item_key.clone(), second.local_seq);
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: second.item_key.clone(),
                hub_rev: 2,
                seq: 2,
                available_blob_ids: Vec::new(),
            }],
            &seqs,
        )
        .await
        .unwrap();

        assert_eq!(
            backfill_progress(&pool, false, true, &[]).await.unwrap(),
            None
        );
        assert_eq!(
            count_ready_backfill_backlog(&pool, true, &[])
                .await
                .unwrap(),
            (0, 0)
        );
    }

    #[tokio::test]
    async fn outgoing_blob_source_and_cloud_availability_gate_control_wake() {
        let pool = memory_pool().await;
        let key = [42_u8; 32];
        let mut item = sample_item("img-item", 1_700_002_000);
        item.kind = ClipboardKind::Image;
        upsert_item_with_sync_outbox(&pool, &item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let local_seq = local_seq_of(&pool, &item.sync_key).await;
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: item.sync_key.clone(),
            content_version: FieldVersion {
                seq: 1,
                device_id: "dev-a".to_owned(),
            },
            presence_version: FieldVersion {
                seq: 1,
                device_id: "dev-a".to_owned(),
            },
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: Some(SyncedClipboardItem {
                kind: "image".to_owned(),
                sub_kind: None,
                content: "img.png".to_owned(),
                search_text: item.search_text.clone(),
                summary: None,
                file_types: None,
                size: Some(128),
                width: Some(10),
                height: Some(10),
                is_sensitive: false,
                source_platform: "windows".to_owned(),
                created_at_ms: 1_700_002_000,
                sync_key: item.sync_key.clone(),
                updated_at_ms: Some(1_700_002_000),
                source_revision: Some("rev-img".to_owned()),
                source_device_id: Some("dev-a".to_owned()),
                source_device_name: Some("PC".to_owned()),
                source_updated_at_ms: Some(1_700_002_000),
            }),
            blobs: vec![crate::sync::model::BlobManifest {
                blob_id: "b".repeat(64),
                name: "img.png".to_owned(),
                original_size: 128,
                encrypted_size: 163,
                role: crate::sync::model::BlobRole::Image,
                file_index: None,
                is_directory_archive: false,
                stream_commitment: false,
            }],
            source_app: None,
            content_identity: Some("hash-img".to_owned()),
        };
        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let blobs = vec![SyncItemBlobRow {
            blob_id: "b".repeat(64),
            role: "image".to_owned(),
            optional: false,
            size: 163,
            original_size: 128,
            encrypted_path: String::new(),
            source_path: Some("C:/tmp/img.png".to_owned()),
            source_is_directory: false,
            stream_nonce: Some(vec![7_u8; 19]),
            stream_commitment: false,
            cloud_available: false,
            local_available: true,
        }];
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &item.sync_key,
                item_key: &item_key,
                item_id: Some(&item.id),
                expected_local_seq: local_seq,
                origin_device_id: "dev-a",
                content_identity: Some("hash-img"),
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                blobs: &blobs,
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Auto),
            },
        )
        .await
        .unwrap());

        // Non-stream-committed image with empty encrypted_path must resolve to StagedLease, never DirectStream.
        let (_, loaded) = outgoing_blob(&pool, &"b".repeat(64), None)
            .await
            .unwrap()
            .expect("outgoing image blob");
        assert_eq!(
            loaded.outgoing_source(),
            Some(crate::sync::model::OutgoingBlobSource::StagedLease)
        );

        // Once metadata is accepted by Cloud, completing blob upload must not request another control push.
        let mut seqs = HashMap::new();
        seqs.insert(item_key.clone(), u64::try_from(local_seq).unwrap());
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key,
                hub_rev: 1,
                seq: 10,
                available_blob_ids: Vec::new(),
            }],
            &seqs,
        )
        .await
        .unwrap();
        let outcome = mark_blob_cloud_available(&pool, &item.sync_key, &"b".repeat(64))
            .await
            .unwrap();
        assert!(outcome.completed);
        assert!(!outcome.needs_control_push);
    }

    #[tokio::test]
    async fn inbound_catchup_and_blob_serve_queries_match_reachable_state() {
        let pool = memory_pool().await;
        let peer = PeerAnnouncement {
            device_id: "dev-peer".to_owned(),
            device_name: "Phone".to_owned(),
            platform: "android".to_owned(),
            endpoint_id: "ep-peer".to_owned(),
            direct_addresses: vec!["192.168.1.50:4443".to_owned()],
            relay_urls: Vec::new(),
            last_seen_ms: 1_700_000_000,
        };
        upsert_peer_identity(&pool, &peer).await.unwrap();
        assert_eq!(
            peer_by_endpoint_id(&pool, "ep-peer").await.unwrap(),
            Some(("dev-peer".to_owned(), "Phone".to_owned()))
        );

        mark_peer_online(&pool, "dev-peer", Some("192.168.1.50:4443"), Some("direct"))
            .await
            .unwrap();
        update_peer_window_cursor(
            &pool,
            "dev-peer",
            SyncWindowCursor {
                synced_seq: 100,
                catchup_before: Some(250),
                window_high: 400,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            inbound_lan_catchup_window(&pool, &["dev-peer".to_owned()])
                .await
                .unwrap(),
            (150, 300, vec!["Phone".to_owned()])
        );

        let mut item = sample_item("/tmp/report.pdf", 1_700_003_000);
        item.kind = ClipboardKind::Files;
        upsert_item_with_sync_outbox(&pool, &item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let local_seq = local_seq_of(&pool, &item.sync_key).await;
        let key = [11_u8; 32];
        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: item.sync_key.clone(),
            content_version: FieldVersion {
                seq: 1,
                device_id: "dev-peer".to_owned(),
            },
            presence_version: FieldVersion {
                seq: 1,
                device_id: "dev-peer".to_owned(),
            },
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: Some(SyncedClipboardItem {
                kind: "files".to_owned(),
                sub_kind: None,
                content: "/tmp/report.pdf".to_owned(),
                search_text: item.search_text.clone(),
                summary: None,
                file_types: None,
                size: Some(2048),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "android".to_owned(),
                created_at_ms: 1_700_003_000,
                sync_key: item.sync_key.clone(),
                updated_at_ms: Some(1_700_003_000),
                source_revision: Some("rev-file".to_owned()),
                source_device_id: Some("dev-peer".to_owned()),
                source_device_name: Some("Phone".to_owned()),
                source_updated_at_ms: Some(1_700_003_000),
            }),
            blobs: vec![crate::sync::model::BlobManifest {
                blob_id: "c".repeat(64),
                name: "report.pdf".to_owned(),
                original_size: 2048,
                encrypted_size: 2083,
                role: crate::sync::model::BlobRole::File,
                file_index: Some(0),
                is_directory_archive: false,
                stream_commitment: true,
            }],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let blobs = vec![SyncItemBlobRow {
            blob_id: "c".repeat(64),
            role: "file".to_owned(),
            optional: false,
            size: 2083,
            original_size: 2048,
            encrypted_path: String::new(),
            source_path: Some("/tmp/report.pdf".to_owned()),
            source_is_directory: false,
            stream_nonce: Some(vec![3_u8; 19]),
            stream_commitment: true,
            cloud_available: true,
            local_available: true,
        }];
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &item.sync_key,
                item_key: &item_key,
                item_id: Some(&item.id),
                expected_local_seq: local_seq,
                origin_device_id: "dev-peer",
                content_identity: None,
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                blobs: &blobs,
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Auto),
            },
        )
        .await
        .unwrap());

        let info = blob_serve_info(&pool, &"c".repeat(64), None)
            .await
            .unwrap()
            .expect("serve info");
        assert_eq!(info.item_id, item.id);
        assert_eq!(info.kind, "files");
        assert!(!info.history_backfill);
        assert_eq!(info.single_file_name.as_deref(), Some("report.pdf"));

        // Mark cloud confirmed and flag `needs_blob_pull`: reachable only when cloud is enabled.
        sqlx::query(
            "INSERT INTO sync_item_targets \
             (sync_key, target_id, confirmed_content, pushed_local_seq, created_at, updated_at) \
             VALUES (?, 'cloud', 'complete', 1, datetime('now'), datetime('now'))",
        )
        .bind(&item.sync_key)
        .execute(&pool)
        .await
        .unwrap();
        mark_item_needs_blob_pull(
            &pool,
            &item.sync_key,
            Some(&item.id),
            "metadata",
            Some("available"),
        )
        .await
        .unwrap();
        let online_peers = vec!["dev-peer".to_owned()];
        assert_eq!(
            count_reachable_pending_blob_pulls(&pool, true, &online_peers)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            reachable_pending_blob_pull_summary(&pool, true, &online_peers)
                .await
                .unwrap(),
            (
                1,
                SyncInboundCatchupSources {
                    cloud: true,
                    peers: Vec::new(),
                },
            )
        );
        assert_eq!(
            count_reachable_pending_blob_pulls(&pool, false, &online_peers)
                .await
                .unwrap(),
            0
        );

        // When an online LAN peer also holds the complete item, attachment summary prefers that peer.
        sqlx::query(
            "INSERT INTO sync_item_targets \
             (sync_key, target_id, confirmed_content, pushed_local_seq, created_at, updated_at) \
             VALUES (?, 'peer:dev-peer', 'complete', 1, datetime('now'), datetime('now'))",
        )
        .bind(&item.sync_key)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            reachable_pending_blob_pull_summary(&pool, true, &online_peers)
                .await
                .unwrap(),
            (
                1,
                SyncInboundCatchupSources {
                    cloud: false,
                    peers: vec!["Phone".to_owned()],
                },
            )
        );
    }

    #[tokio::test]
    async fn cloud_accepted_promotes_metadata_only_item_when_hub_reports_available_blobs() {
        let pool = memory_pool().await;
        let key = [42_u8; 32];
        let mut item = sample_item("oversized-reuse", 1_700_004_000);
        item.kind = ClipboardKind::Files;
        upsert_item_with_sync_outbox(&pool, &item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let local_seq = local_seq_of(&pool, &item.sync_key).await;
        let blob_id = "d".repeat(64);
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: item.sync_key.clone(),
            content_version: FieldVersion {
                seq: 1,
                device_id: "dev-a".to_owned(),
            },
            presence_version: FieldVersion {
                seq: 1,
                device_id: "dev-a".to_owned(),
            },
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: Some(SyncedClipboardItem {
                kind: "files".to_owned(),
                sub_kind: None,
                content: "archive.zip".to_owned(),
                search_text: item.search_text.clone(),
                summary: None,
                file_types: None,
                size: Some(2048),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "macos".to_owned(),
                created_at_ms: 1_700_004_000,
                sync_key: item.sync_key.clone(),
                updated_at_ms: Some(1_700_004_000),
                source_revision: Some("rev-oversized".to_owned()),
                source_device_id: Some("dev-a".to_owned()),
                source_device_name: Some("Mac".to_owned()),
                source_updated_at_ms: Some(1_700_004_000),
            }),
            blobs: vec![BlobManifest {
                blob_id: blob_id.clone(),
                name: "archive.zip".to_owned(),
                original_size: 2048,
                encrypted_size: 2080,
                role: BlobRole::File,
                file_index: Some(0),
                is_directory_archive: false,
                stream_commitment: true,
            }],
            source_app: None,
            content_identity: Some("hash-archive".to_owned()),
        };
        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let blobs = vec![SyncItemBlobRow {
            blob_id: blob_id.clone(),
            role: "file".to_owned(),
            optional: false,
            size: 2080,
            original_size: 2048,
            encrypted_path: String::new(),
            source_path: Some("/tmp/archive.zip".to_owned()),
            source_is_directory: false,
            stream_nonce: Some(vec![9_u8; 19]),
            stream_commitment: true,
            cloud_available: false,
            local_available: true,
        }];
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &item.sync_key,
                item_key: &item_key,
                item_id: Some(&item.id),
                expected_local_seq: local_seq,
                origin_device_id: "dev-a",
                content_identity: Some("hash-archive"),
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                blobs: &blobs,
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
            },
        )
        .await
        .unwrap());

        let mut seqs = HashMap::new();
        seqs.insert(item_key.clone(), u64::try_from(local_seq).unwrap());
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key,
                hub_rev: 1,
                seq: 1,
                available_blob_ids: vec![blob_id],
            }],
            &seqs,
        )
        .await
        .unwrap();

        let statuses = item_statuses(
            &pool,
            std::slice::from_ref(&item.id),
            true,
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            statuses[0].cloud.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(statuses[0].cloud.action, SyncItemAction::None);
        assert_eq!(statuses[0].cloud.activity, SyncItemActivity::Idle);
        assert!(pending_cloud_blob_uploads(&pool, false, 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn existing_cloud_blob_verification_promotes_deferred_and_metadata_items() {
        let pool = memory_pool().await;
        let key = [42_u8; 32];

        // 1. Local oversized item with `cloud_blob_policy = 'defer'`, before Cloud acceptance.
        let mut defer_item = sample_item("lan-file", 1_700_005_000);
        defer_item.kind = ClipboardKind::Files;
        upsert_item_with_sync_outbox(
            &pool,
            &defer_item,
            Some(ContentSyncOptions {
                manual_share: true,
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let lan_seq = local_seq_of(&pool, &defer_item.sync_key).await;
        let lan_blob_id = "e".repeat(64);
        let lan_item_key = crypto::derive_item_key(&key, &defer_item.sync_key);
        let lan_snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: defer_item.sync_key.clone(),
            content_version: FieldVersion {
                seq: 1,
                device_id: "dev-a".to_owned(),
            },
            presence_version: FieldVersion {
                seq: 1,
                device_id: "dev-a".to_owned(),
            },
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: None,
            blobs: Vec::new(),
            source_app: None,
            content_identity: Some("hash-lan".to_owned()),
        };
        let (lan_nonce, lan_ciphertext) =
            crypto::encrypt_item_snapshot(&key, &lan_item_key, false, false, &lan_snapshot)
                .unwrap();
        let lan_blobs = vec![SyncItemBlobRow {
            blob_id: lan_blob_id.clone(),
            role: "file".to_owned(),
            optional: false,
            size: 2048,
            original_size: 2016,
            encrypted_path: String::new(),
            source_path: Some("/tmp/lan.bin".to_owned()),
            source_is_directory: false,
            stream_nonce: Some(vec![3_u8; 19]),
            stream_commitment: true,
            cloud_available: false,
            local_available: true,
        }];
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &defer_item.sync_key,
                item_key: &lan_item_key,
                item_id: Some(&defer_item.id),
                expected_local_seq: lan_seq,
                origin_device_id: "dev-a",
                content_identity: Some("hash-lan"),
                group_id: "group-1",
                snapshot: &lan_snapshot,
                nonce: &lan_nonce,
                ciphertext: &lan_ciphertext,
                blobs: &lan_blobs,
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
            },
        )
        .await
        .unwrap());

        // 2. Local oversized item whose metadata already reached Cloud with `confirmed_content = 'metadata'`.
        let mut meta_item = sample_item("meta-file", 1_700_005_100);
        meta_item.kind = ClipboardKind::Files;
        upsert_item_with_sync_outbox(&pool, &meta_item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let meta_seq = local_seq_of(&pool, &meta_item.sync_key).await;
        let meta_blob_id = "f".repeat(64);
        let meta_item_key = crypto::derive_item_key(&key, &meta_item.sync_key);
        let (meta_nonce, meta_ciphertext) =
            crypto::encrypt_item_snapshot(&key, &meta_item_key, false, false, &lan_snapshot)
                .unwrap();
        let meta_blobs = vec![SyncItemBlobRow {
            blob_id: meta_blob_id.clone(),
            role: "file".to_owned(),
            optional: false,
            size: 4096,
            original_size: 4064,
            encrypted_path: String::new(),
            source_path: Some("/tmp/meta.bin".to_owned()),
            source_is_directory: false,
            stream_nonce: Some(vec![4_u8; 19]),
            stream_commitment: true,
            cloud_available: false,
            local_available: true,
        }];
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &meta_item.sync_key,
                item_key: &meta_item_key,
                item_id: Some(&meta_item.id),
                expected_local_seq: meta_seq,
                origin_device_id: "dev-a",
                content_identity: Some("hash-meta"),
                group_id: "group-1",
                snapshot: &lan_snapshot,
                nonce: &meta_nonce,
                ciphertext: &meta_ciphertext,
                blobs: &meta_blobs,
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Defer),
            },
        )
        .await
        .unwrap());
        let mut seqs = HashMap::new();
        seqs.insert(meta_item_key.clone(), u64::try_from(meta_seq).unwrap());
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: meta_item_key.clone(),
                hub_rev: 1,
                seq: 1,
                available_blob_ids: Vec::new(),
            }],
            &seqs,
        )
        .await
        .unwrap();

        let checks = pending_cloud_blob_verifications(&pool, false, 10)
            .await
            .unwrap();
        assert_eq!(checks.len(), 2);
        assert!(checks
            .iter()
            .any(|check| check.blob_id == lan_blob_id && check.size == 2048));
        assert!(checks
            .iter()
            .any(|check| check.blob_id == meta_blob_id && check.size == 4096));

        let promotion =
            promote_cloud_blobs_available(&pool, &[lan_blob_id.clone(), meta_blob_id.clone()])
                .await
                .unwrap();
        assert_eq!(
            promotion,
            CloudBlobVerifyPromotion {
                promoted_items: 2,
                needs_control_push: true,
            }
        );
        assert!(pending_cloud_blob_verifications(&pool, false, 10)
            .await
            .unwrap()
            .is_empty());

        // `meta_item` immediately turns green without waiting for another push or blob upload.
        let meta_status = item_statuses(
            &pool,
            std::slice::from_ref(&meta_item.id),
            true,
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            meta_status[0].cloud.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(meta_status[0].cloud.action, SyncItemAction::None);

        // `defer_item` pushes 1 KB metadata without queueing attachment upload, then turns green on acceptance.
        assert!(pending_cloud_blob_uploads(&pool, false, 10)
            .await
            .unwrap()
            .is_empty());
        let outbound = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(outbound.writes.len(), 1);
        assert_eq!(outbound.writes[0].item_key, lan_item_key);

        let mut lan_seqs = HashMap::new();
        lan_seqs.insert(lan_item_key.clone(), outbound.writes[0].local_seq);
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: lan_item_key,
                hub_rev: 2,
                seq: 2,
                available_blob_ids: vec![lan_blob_id],
            }],
            &lan_seqs,
        )
        .await
        .unwrap();

        let lan_status = item_statuses(
            &pool,
            std::slice::from_ref(&defer_item.id),
            true,
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            lan_status[0].cloud.availability,
            SyncContentAvailability::Complete
        );
        assert_eq!(lan_status[0].cloud.action, SyncItemAction::None);
    }

    #[tokio::test]
    async fn blob_serve_lookup_binds_exact_sync_key_and_falls_back_to_newest() {
        let pool = memory_pool().await;
        let key = [5_u8; 32];
        let shared_blob_id = "d".repeat(64);
        let shared_blob = vec![SyncItemBlobRow {
            blob_id: shared_blob_id.clone(),
            role: "file".to_owned(),
            optional: false,
            size: 2083,
            original_size: 2048,
            encrypted_path: String::new(),
            source_path: Some("/tmp/shared.apk".to_owned()),
            source_is_directory: false,
            stream_nonce: Some(vec![7_u8; 19]),
            stream_commitment: true,
            cloud_available: false,
            local_available: true,
        }];

        let mut old_item = sample_item("old-apk", 1_700_000_100);
        old_item.kind = ClipboardKind::Files;
        old_item.content = "/tmp/shared.apk".to_owned();
        let mut new_item = sample_item("new-apk", 1_700_000_200);
        new_item.kind = ClipboardKind::Files;
        new_item.content = "/tmp/shared.apk".to_owned();
        let mut deleted_item = sample_item("deleted-apk", 1_700_000_300);
        deleted_item.kind = ClipboardKind::Files;
        deleted_item.content = "/tmp/shared.apk".to_owned();

        for (item, ts, mark_deleted) in [
            (&old_item, 1_700_000_100_i64, false),
            (&new_item, 1_700_000_200_i64, false),
            (&deleted_item, 1_700_000_300_i64, true),
        ] {
            upsert_item_with_sync_outbox(&pool, item, Some(ContentSyncOptions::default()))
                .await
                .unwrap();
            let seq = local_seq_of(&pool, &item.sync_key).await;
            let item_key = crypto::derive_item_key(&key, &item.sync_key);
            let snapshot = SyncedItemSnapshot {
                version: SNAPSHOT_VERSION,
                sync_key: item.sync_key.clone(),
                content_version: FieldVersion {
                    seq: 1,
                    device_id: "dev-a".to_owned(),
                },
                presence_version: FieldVersion {
                    seq: 1,
                    device_id: "dev-a".to_owned(),
                },
                favorite_version: FieldVersion::default(),
                pinned_version: FieldVersion::default(),
                note_version: FieldVersion::default(),
                is_present: true,
                is_favorite: false,
                is_pinned: false,
                note: None,
                item: Some(SyncedClipboardItem {
                    kind: "files".to_owned(),
                    sub_kind: None,
                    content: item.content.clone(),
                    search_text: item.search_text.clone(),
                    summary: None,
                    file_types: None,
                    size: Some(2048),
                    width: None,
                    height: None,
                    is_sensitive: false,
                    source_platform: "android".to_owned(),
                    created_at_ms: ts,
                    sync_key: item.sync_key.clone(),
                    updated_at_ms: Some(ts),
                    source_revision: Some(format!("rev-{}", item.id)),
                    source_device_id: Some("dev-a".to_owned()),
                    source_device_name: Some("Phone".to_owned()),
                    source_updated_at_ms: Some(ts),
                }),
                blobs: vec![crate::sync::model::BlobManifest {
                    blob_id: shared_blob_id.clone(),
                    name: "shared.apk".to_owned(),
                    original_size: 2048,
                    encrypted_size: 2083,
                    role: crate::sync::model::BlobRole::File,
                    file_index: Some(0),
                    is_directory_archive: false,
                    stream_commitment: true,
                }],
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
                    expected_local_seq: seq,
                    origin_device_id: "dev-a",
                    content_identity: None,
                    group_id: "group-1",
                    snapshot: &snapshot,
                    nonce: &nonce,
                    ciphertext: &ciphertext,
                    blobs: &shared_blob,
                    content_objects: &[],
                    cloud_blob_policy: None,
                },
            )
            .await
            .unwrap());
            if mark_deleted {
                sqlx::query("UPDATE sync_items SET deleted = 1 WHERE sync_key = ?")
                    .bind(&item.sync_key)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        }

        let serve_info = blob_serve_info(&pool, &shared_blob_id, None)
            .await
            .unwrap()
            .expect("serve info");
        assert_eq!(serve_info.item_id, new_item.id);
        assert_eq!(serve_info.sync_key, new_item.sync_key);

        let exact_serve_info = blob_serve_info(&pool, &shared_blob_id, Some(&old_item.sync_key))
            .await
            .unwrap()
            .expect("exact serve info");
        assert_eq!(exact_serve_info.item_id, old_item.id);
        assert_eq!(exact_serve_info.sync_key, old_item.sync_key);

        let (resolved_sync_key, resolved_item_id) = item_id_for_blob(&pool, &shared_blob_id, None)
            .await
            .unwrap()
            .expect("item id for blob");
        assert_eq!(resolved_item_id, new_item.id);
        assert_eq!(resolved_sync_key, new_item.sync_key);

        let (exact_sync_key, exact_item_id) =
            item_id_for_blob(&pool, &shared_blob_id, Some(&old_item.sync_key))
                .await
                .unwrap()
                .expect("exact item id for blob");
        assert_eq!(exact_item_id, old_item.id);
        assert_eq!(exact_sync_key, old_item.sync_key);
    }

    #[tokio::test]
    async fn outgoing_blob_prefers_newest_non_deleted_source_when_blob_is_deduplicated() {
        let pool = memory_pool().await;
        let key = [6_u8; 32];
        let shared_blob_id = "e".repeat(64);

        let mut old_item = sample_item("old-source", 1_700_000_100);
        old_item.kind = ClipboardKind::Files;
        old_item.content = "/tmp/old.apk".to_owned();
        let mut new_item = sample_item("new-source", 1_700_000_200);
        new_item.kind = ClipboardKind::Files;
        new_item.content = "/tmp/new.apk".to_owned();
        let mut deleted_item = sample_item("deleted-source", 1_700_000_300);
        deleted_item.kind = ClipboardKind::Files;
        deleted_item.content = "/tmp/deleted.apk".to_owned();

        for (item, ts, source_path, mark_deleted) in [
            (&old_item, 1_700_000_100_i64, "/tmp/old.apk", false),
            (&new_item, 1_700_000_200_i64, "/tmp/new.apk", false),
            (&deleted_item, 1_700_000_300_i64, "/tmp/deleted.apk", true),
        ] {
            upsert_item_with_sync_outbox(&pool, item, Some(ContentSyncOptions::default()))
                .await
                .unwrap();
            let seq = local_seq_of(&pool, &item.sync_key).await;
            let item_key = crypto::derive_item_key(&key, &item.sync_key);
            let snapshot = SyncedItemSnapshot {
                version: SNAPSHOT_VERSION,
                sync_key: item.sync_key.clone(),
                content_version: FieldVersion {
                    seq: 1,
                    device_id: "dev-a".to_owned(),
                },
                presence_version: FieldVersion {
                    seq: 1,
                    device_id: "dev-a".to_owned(),
                },
                favorite_version: FieldVersion::default(),
                pinned_version: FieldVersion::default(),
                note_version: FieldVersion::default(),
                is_present: true,
                is_favorite: false,
                is_pinned: false,
                note: None,
                item: Some(SyncedClipboardItem {
                    kind: "files".to_owned(),
                    sub_kind: None,
                    content: item.content.clone(),
                    search_text: item.search_text.clone(),
                    summary: None,
                    file_types: None,
                    size: Some(2048),
                    width: None,
                    height: None,
                    is_sensitive: false,
                    source_platform: "android".to_owned(),
                    created_at_ms: ts,
                    sync_key: item.sync_key.clone(),
                    updated_at_ms: Some(ts),
                    source_revision: Some(format!("rev-{}", item.id)),
                    source_device_id: Some("dev-a".to_owned()),
                    source_device_name: Some("Phone".to_owned()),
                    source_updated_at_ms: Some(ts),
                }),
                blobs: vec![crate::sync::model::BlobManifest {
                    blob_id: shared_blob_id.clone(),
                    name: "shared.apk".to_owned(),
                    original_size: 2048,
                    encrypted_size: 2083,
                    role: crate::sync::model::BlobRole::File,
                    file_index: Some(0),
                    is_directory_archive: false,
                    stream_commitment: true,
                }],
                source_app: None,
                content_identity: None,
            };
            let (nonce, ciphertext) =
                crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
            let blobs = vec![SyncItemBlobRow {
                blob_id: shared_blob_id.clone(),
                role: "file".to_owned(),
                optional: false,
                size: 2083,
                original_size: 2048,
                encrypted_path: String::new(),
                source_path: Some(source_path.to_owned()),
                source_is_directory: false,
                stream_nonce: Some(vec![7_u8; 19]),
                stream_commitment: true,
                cloud_available: false,
                local_available: true,
            }];
            assert!(commit_prepared_item(
                &pool,
                PreparedItemCommit {
                    sync_key: &item.sync_key,
                    item_key: &item_key,
                    item_id: Some(&item.id),
                    expected_local_seq: seq,
                    origin_device_id: "dev-a",
                    content_identity: None,
                    group_id: "group-1",
                    snapshot: &snapshot,
                    nonce: &nonce,
                    ciphertext: &ciphertext,
                    blobs: &blobs,
                    content_objects: &[],
                    cloud_blob_policy: None,
                },
            )
            .await
            .unwrap());
            if mark_deleted {
                sqlx::query("UPDATE sync_items SET deleted = 1 WHERE sync_key = ?")
                    .bind(&item.sync_key)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        }

        let (_, resolved) = outgoing_blob(&pool, &shared_blob_id, None)
            .await
            .unwrap()
            .expect("outgoing blob");
        assert_eq!(resolved.source_path.as_deref(), Some("/tmp/new.apk"));
    }

    #[tokio::test]
    async fn downloaded_remote_file_never_auto_uploads_to_cloud_unless_explicitly_requested() {
        let pool = memory_pool().await;
        let key = [7_u8; 32];
        let mut item = sample_item("remote-lan-file", 1_700_000_500);
        item.kind = ClipboardKind::Files;
        item.content = "big.bin".to_owned();
        item.size = Some(99 * 1024 * 1024);
        item.remote_file_state = Some("available".to_owned());
        let upserted = upsert_remote_file_item(&pool, &item, "dev-phone")
            .await
            .unwrap();
        item.id = upserted.id.clone();

        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let blob_id = "f".repeat(64);
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: item.sync_key.clone(),
            content_version: FieldVersion::new(1, "dev-phone"),
            presence_version: FieldVersion::new(1, "dev-phone"),
            favorite_version: FieldVersion::default(),
            pinned_version: FieldVersion::default(),
            note_version: FieldVersion::default(),
            is_present: true,
            is_favorite: false,
            is_pinned: false,
            note: None,
            item: Some(SyncedClipboardItem {
                kind: "files".to_owned(),
                sub_kind: None,
                content: "big.bin".to_owned(),
                search_text: None,
                summary: Some("big.bin".to_owned()),
                file_types: None,
                size: item.size,
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "android".to_owned(),
                created_at_ms: item.created_at.timestamp_millis(),
                sync_key: item.sync_key.clone(),
                updated_at_ms: Some(item.updated_at.timestamp_millis()),
                source_revision: Some(item.source_revision.clone()),
                source_device_id: Some("dev-phone".to_owned()),
                source_device_name: Some("Phone".to_owned()),
                source_updated_at_ms: Some(item.updated_at.timestamp_millis()),
            }),
            blobs: vec![BlobManifest {
                blob_id: blob_id.clone(),
                name: "big.bin".to_owned(),
                original_size: 99 * 1024 * 1024,
                encrypted_size: 99 * 1024 * 1024 + 64,
                role: BlobRole::File,
                file_index: Some(0),
                is_directory_archive: false,
                stream_commitment: true,
            }],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        store_merged_remote_item(
            &pool,
            StoreMergedRemoteItem {
                sync_key: &item.sync_key,
                item_key: &item_key,
                origin_device_id: "dev-phone",
                hub_rev: Some(1),
                remote_seq: 1,
                source_target_id: "peer:dev-phone",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                available_blob_ids: std::slice::from_ref(&blob_id),
                needs_apply: false,
                re_push_required: false,
            },
        )
        .await
        .unwrap();
        mark_item_applied(
            &pool,
            &item.sync_key,
            Some(&item.id),
            "external",
            Some("available"),
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE sync_item_blobs SET local_available = 1, source_path = '/tmp/big.bin' \
             WHERE sync_key = ? AND blob_id = ?",
        )
        .bind(&item.sync_key)
        .bind(&blob_id)
        .execute(&pool)
        .await
        .unwrap();

        let policy: String =
            sqlx::query_scalar("SELECT cloud_blob_policy FROM sync_items WHERE sync_key = ?")
                .bind(&item.sync_key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(policy, "defer");
        assert!(pending_cloud_blob_uploads(&pool, false, 10)
            .await
            .unwrap()
            .is_empty());

        // Even legacy rows migrated with `origin = 'remote'` and `cloud_blob_policy = 'auto'`
        // must never auto-upload to Cloud without explicit user request.
        sqlx::query("UPDATE sync_items SET cloud_blob_policy = 'auto' WHERE sync_key = ?")
            .bind(&item.sync_key)
            .execute(&pool)
            .await
            .unwrap();
        assert!(pending_cloud_blob_uploads(&pool, false, 10)
            .await
            .unwrap()
            .is_empty());

        // Explicitly clicking Upload on this device switches policy to `requested` and enqueues it.
        assert!(request_item_delivery(&pool, &item.id, true, true)
            .await
            .unwrap());
        let uploads = pending_cloud_blob_uploads(&pool, false, 10).await.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].sync_key, item.sync_key);
    }

    #[tokio::test]
    async fn canceled_or_blocked_cloud_blob_policy_never_blocks_control_plane_metadata_or_deletion()
    {
        let pool = memory_pool().await;
        let key = [47_u8; 32];
        let mut item = sample_item("cancel-isolation", 1_700_006_000);
        item.kind = ClipboardKind::Files;
        item.size = Some(10 * 1024 * 1024);
        upsert_item_with_sync_outbox(&pool, &item, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let local_seq = local_seq_of(&pool, &item.sync_key).await;
        let item_key = crypto::derive_item_key(&key, &item.sync_key);
        let blob_id = "a".repeat(64);
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
                content: "doc.bin".into(),
                search_text: None,
                summary: Some("doc.bin".into()),
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
            blobs: vec![BlobManifest {
                blob_id: blob_id.clone(),
                name: "doc.bin".into(),
                original_size: 10 * 1024 * 1024,
                encrypted_size: 10 * 1024 * 1024 + 64,
                role: BlobRole::File,
                file_index: Some(0),
                is_directory_archive: false,
                stream_commitment: true,
            }],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &item_key, false, false, &snapshot).unwrap();
        let blob_row = SyncItemBlobRow {
            blob_id: blob_id.clone(),
            role: "file".into(),
            optional: false,
            size: 10 * 1024 * 1024 + 64,
            original_size: 10 * 1024 * 1024,
            encrypted_path: String::new(),
            source_path: Some("/tmp/doc.bin".into()),
            source_is_directory: false,
            stream_nonce: Some(vec![1_u8; 19]),
            stream_commitment: true,
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
                blobs: &[blob_row],
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Requested),
            },
        )
        .await
        .unwrap());

        // User cancels the Cloud attachment upload.
        cancel_delivery(&pool, CLOUD_TARGET, &item.sync_key)
            .await
            .unwrap();
        assert!(is_delivery_canceled(&pool, CLOUD_TARGET, &item.sync_key)
            .await
            .unwrap());
        assert!(pending_cloud_blob_uploads(&pool, false, 10)
            .await
            .unwrap()
            .is_empty());

        // Control-plane 1 KB metadata is NOT blocked by data-plane attachment cancelation!
        let batch = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(batch.writes.len(), 1);
        let mut seqs = HashMap::new();
        seqs.insert(item_key.clone(), batch.writes[0].local_seq);
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: item_key.clone(),
                hub_rev: 1,
                seq: 1,
                available_blob_ids: Vec::new(),
            }],
            &seqs,
        )
        .await
        .unwrap();

        // Deleting the item later MUST still queue the deletion tombstone to Cloud!
        let mut conn = pool.acquire().await.unwrap();
        mark_local_deleted_on(&mut conn, &item.sync_key, false)
            .await
            .unwrap();
        drop(conn);
        let del_seq = local_seq_of(&pool, &item.sync_key).await;
        snapshot.is_present = false;
        snapshot.presence_version = FieldVersion::new(2, "dev-a");
        snapshot.item = None;
        snapshot.blobs.clear();
        let (del_nonce, del_cipher) =
            crypto::encrypt_item_snapshot(&key, &item_key, true, false, &snapshot).unwrap();
        assert!(commit_prepared_item(
            &pool,
            PreparedItemCommit {
                sync_key: &item.sync_key,
                item_key: &item_key,
                item_id: None,
                expected_local_seq: del_seq,
                origin_device_id: "dev-a",
                content_identity: None,
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &del_nonce,
                ciphertext: &del_cipher,
                blobs: &[],
                content_objects: &[],
                cloud_blob_policy: None,
            },
        )
        .await
        .unwrap());
        let tombstone_batch = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(tombstone_batch.writes.len(), 1);
        assert!(tombstone_batch.writes[0].deleted);
    }

    #[tokio::test]
    async fn cancel_content_prep_deletes_uncommitted_row_and_mark_blob_available_waits_for_metadata(
    ) {
        let pool = memory_pool().await;
        let item = sample_item("uncommitted-cancel", 1_700_006_100);
        upsert_item_with_sync_outbox(
            &pool,
            &item,
            Some(ContentSyncOptions {
                history_backfill: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let local_seq = local_seq_of(&pool, &item.sync_key).await;

        // Canceling before any snapshot is committed deletes the half-baked `sync_items` row.
        crate::db::sync_items::cancel_content_prep(&pool, local_seq)
            .await
            .unwrap();
        assert!(load_sync_item_meta(&pool, &item.sync_key)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            backfill_progress(&pool, false, true, &[]).await.unwrap(),
            None
        );
        let statuses = item_statuses(
            &pool,
            std::slice::from_ref(&item.id),
            true,
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(statuses[0].cloud.activity, SyncItemActivity::Idle);

        // `mark_blob_cloud_available` before control metadata acceptance must NOT mark complete.
        let key = [53_u8; 32];
        let mut img = sample_item("blob-before-meta", 1_700_006_200);
        img.kind = ClipboardKind::Image;
        upsert_item_with_sync_outbox(&pool, &img, Some(ContentSyncOptions::default()))
            .await
            .unwrap();
        let img_seq = local_seq_of(&pool, &img.sync_key).await;
        let img_key = crypto::derive_item_key(&key, &img.sync_key);
        let blob_id = "b".repeat(64);
        let snapshot = SyncedItemSnapshot {
            version: SNAPSHOT_VERSION,
            sync_key: img.sync_key.clone(),
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
                content: "a.png".into(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(64),
                width: Some(2),
                height: Some(2),
                is_sensitive: false,
                source_platform: "macos".into(),
                created_at_ms: img.created_at.timestamp_millis(),
                sync_key: img.sync_key.clone(),
                updated_at_ms: Some(img.updated_at.timestamp_millis()),
                source_revision: Some(img.source_revision.clone()),
                source_device_id: Some("dev-a".into()),
                source_device_name: Some("Mac".into()),
                source_updated_at_ms: Some(img.updated_at.timestamp_millis()),
            }),
            blobs: vec![BlobManifest {
                blob_id: blob_id.clone(),
                name: "a.png".into(),
                original_size: 64,
                encrypted_size: 96,
                role: BlobRole::Image,
                file_index: None,
                is_directory_archive: false,
                stream_commitment: false,
            }],
            source_app: None,
            content_identity: None,
        };
        let (nonce, ciphertext) =
            crypto::encrypt_item_snapshot(&key, &img_key, false, false, &snapshot).unwrap();
        let blob_row = SyncItemBlobRow {
            blob_id: blob_id.clone(),
            role: "image".into(),
            optional: false,
            size: 96,
            original_size: 64,
            encrypted_path: "/tmp/a.enc".into(),
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
                sync_key: &img.sync_key,
                item_key: &img_key,
                item_id: Some(&img.id),
                expected_local_seq: img_seq,
                origin_device_id: "dev-a",
                content_identity: None,
                group_id: "group-1",
                snapshot: &snapshot,
                nonce: &nonce,
                ciphertext: &ciphertext,
                blobs: &[blob_row],
                content_objects: &[],
                cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Auto),
            },
        )
        .await
        .unwrap());

        let early = mark_blob_cloud_available(&pool, &img.sync_key, &blob_id)
            .await
            .unwrap();
        assert!(!early.completed);
        assert!(early.needs_control_push);
    }

    #[tokio::test]
    async fn oversized_control_prep_error_skips_head_of_line_and_prune_keeps_unpushed_evicted_items(
    ) {
        let pool = memory_pool().await;
        let key = [7_u8; 32];

        let big = sample_item("oversized-ctrl", 1_700_000_800);
        let normal = sample_item("normal-after", 1_700_000_700);
        for item in [&big, &normal] {
            upsert_item_with_sync_outbox(&pool, item, Some(ContentSyncOptions::default()))
                .await
                .unwrap();
            let seq = local_seq_of(&pool, &item.sync_key).await;
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
                    size: Some(10),
                    width: None,
                    height: None,
                    is_sensitive: false,
                    source_platform: "macos".into(),
                    created_at_ms: 1_700_000_800_000,
                    sync_key: item.sync_key.clone(),
                    updated_at_ms: Some(1_700_000_800_000),
                    source_revision: Some(item.id.clone()),
                    source_device_id: Some("dev-a".into()),
                    source_device_name: Some("Mac".into()),
                    source_updated_at_ms: Some(1_700_000_800_000),
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
                    expected_local_seq: seq,
                    origin_device_id: "dev-a",
                    content_identity: None,
                    group_id: "group-1",
                    snapshot: &snapshot,
                    nonce: &nonce,
                    ciphertext: &ciphertext,
                    blobs: &[],
                    content_objects: &[],
                    cloud_blob_policy: Some(crate::db::sync_items::CloudBlobPolicy::Auto),
                },
            )
            .await
            .unwrap());
        }

        // Recording a terminal prep error for `big` must unblock `normal` on both Cloud and LAN
        // without forging `sync_item_targets`, while keeping `Error + Retry` visible on the card.
        let big_seq = local_seq_of(&pool, &big.sync_key).await;
        assert!(record_terminal_prep_error(
            &pool,
            &big.sync_key,
            Some(big_seq),
            "frame too large",
            false,
        )
        .await
        .unwrap());

        let cloud_batch = pending_outbound_writes(&pool, CLOUD_TARGET, 10)
            .await
            .unwrap();
        assert_eq!(cloud_batch.writes.len(), 1);
        assert_eq!(
            cloud_batch.writes[0].item_key,
            crypto::derive_item_key(&key, &normal.sync_key)
        );
        let lan_batch = pending_outbound_writes(&pool, "peer:dev-b", 10)
            .await
            .unwrap();
        assert_eq!(lan_batch.writes.len(), 1);

        let statuses = item_statuses(
            &pool,
            std::slice::from_ref(&big.id),
            true,
            &["dev-b".into()],
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(statuses[0].cloud.activity, SyncItemActivity::Error);
        assert_eq!(statuses[0].cloud.action, SyncItemAction::Retry);
        assert_eq!(statuses[0].lan.activity, SyncItemActivity::Error);
        assert_eq!(statuses[0].lan.action, SyncItemAction::Retry);
        assert!(
            has_pending_control_delivery(&pool, &big.sync_key, true, &[])
                .await
                .unwrap()
        );
        assert!(request_item_delivery(&pool, &big.id, true, false)
            .await
            .unwrap());
        let retried_meta = load_sync_item_meta(&pool, &big.sync_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried_meta.prep_state, "pending_state");

        // Evicting `normal` from local `clipboard_items` while still unpushed to Cloud must keep its
        // `sync_items` row until outbound delivery finishes.
        sqlx::query("DELETE FROM clipboard_items WHERE id = ?")
            .bind(&normal.id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(prune_released_items(&pool, true, 100).await.unwrap(), 0);
        let normal_seq = local_seq_of(&pool, &normal.sync_key).await;
        let normal_item_key = crypto::derive_item_key(&key, &normal.sync_key);
        let mut seqs = HashMap::new();
        seqs.insert(normal_item_key.clone(), u64::try_from(normal_seq).unwrap());
        record_cloud_accepted(
            &pool,
            &[AcceptedItem {
                item_key: normal_item_key,
                hub_rev: 1,
                seq: 1,
                available_blob_ids: Vec::new(),
            }],
            &seqs,
        )
        .await
        .unwrap();
        assert_eq!(prune_released_items(&pool, true, 100).await.unwrap(), 1);
    }
}
