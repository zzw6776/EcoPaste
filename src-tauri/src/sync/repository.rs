use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
};

use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use ecopaste_sync_protocol::{EncryptedEvent, PeerAnnouncement, RemovedDevice, SyncEventKind};
use sqlx::{Row, SqliteConnection, SqlitePool};

use super::{
    identity::is_lan_ip,
    model::{
        LinkedSyncEvent, StoredBlob, StoredSyncEvent, SyncChannelState, SyncItemState,
        SyncItemStatus, SyncPeer, SyncPeerStatus,
    },
};

mod delivery;
mod peers;
mod state;

pub use delivery::*;
pub use peers::*;
pub use state::*;

const CLOUD_CURSOR_KEY: &str = "cloud_cursor";
const CLOUD_BOOTSTRAP_GROUP_KEY: &str = "cloud_bootstrap_group_id";
const CLOUD_BOOTSTRAP_TARGET_KEY: &str = "cloud_bootstrap_target_cursor";
const CLOUD_TARGET: &str = "cloud";
const HISTORY_BACKFILL_GROUP_KEY: &str = "history_backfill_group_id";
const HISTORY_BACKFILL_VERSION: u8 = 2;
const HISTORY_TIMESTAMP_REPAIR_GROUP_KEY: &str = "history_timestamp_repair_group_id";
const LAST_SUCCESS_KEY: &str = "last_success_at";
const CLOUD_CONFIRMATION_REPAIR_KEY: &str = "cloud_confirmation_repair_before";
const CLOUD_RETENTION_SNAPSHOT_KEY: &str = "cloud_retention_snapshot";

/// Reads protected history identities; flags remain local even when automatic capture is disabled.
pub async fn protected_sync_keys(pool: &SqlitePool) -> Result<Vec<String>> {
    sqlx::query_scalar(
        r#"
        SELECT sync_key FROM clipboard_items
        WHERE (is_favorite = 1 OR is_pinned = 1) AND sync_key IS NOT NULL
        UNION
        SELECT sync_key FROM sync_item_states
        WHERE (is_favorite = 1 OR is_pinned = 1) AND COALESCE(is_present, 1) = 1
        ORDER BY sync_key
        "#,
    )
    .fetch_all(pool)
    .await
    .context("read protected sync identities")
}

pub async fn cloud_retention_snapshot(pool: &SqlitePool) -> Result<Option<String>> {
    state_value(pool, CLOUD_RETENTION_SNAPSHOT_KEY).await
}

pub async fn set_cloud_retention_snapshot(pool: &SqlitePool, fingerprint: &str) -> Result<()> {
    set_state_value(pool, CLOUD_RETENTION_SNAPSHOT_KEY, fingerprint).await
}

/// An approved rejoin may recreate the Hub device without its previous protection snapshot.
pub async fn clear_cloud_retention_snapshot(pool: &SqlitePool) -> Result<()> {
    sqlx::query("DELETE FROM sync_state WHERE key = ?")
        .bind(CLOUD_RETENTION_SNAPSHOT_KEY)
        .execute(pool)
        .await
        .context("clear cloud retention snapshot")?;
    Ok(())
}

/// Distinguishes an inserted/idempotent event from a reused origin sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertEventResult {
    pub inserted: bool,
    pub stored: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum RemoteBatchSource<'a> {
    Cloud,
    Peer { device_id: &'a str },
}

impl RemoteBatchSource<'_> {
    fn delivery_target(self) -> String {
        match self {
            Self::Cloud => CLOUD_TARGET.to_owned(),
            Self::Peer { device_id } => format!("peer:{device_id}"),
        }
    }
}

#[derive(Debug, Default)]
pub struct StoreRemoteBatchResult {
    pub inserted_event_ids: Vec<String>,
    pub received_new_events: bool,
    pub conflicts: Vec<(String, String)>,
}

/// Allocates a monotonically increasing sequence for this device.
pub async fn next_origin_sequence(pool: &SqlitePool) -> Result<u64> {
    let floor = u64::try_from(Utc::now().timestamp_millis()).unwrap_or(1);
    let now = Utc::now();
    let next: Option<String> = sqlx::query_scalar(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES ('origin_sequence', ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = MAX(CAST(sync_state.value AS INTEGER) + 1, CAST(excluded.value AS INTEGER)),
            updated_at = excluded.updated_at
        WHERE sync_state.value <> ''
          AND sync_state.value NOT GLOB '*[^0-9]*'
          AND CAST(sync_state.value AS INTEGER) < 9223372036854775807
        RETURNING value
        "#,
    )
    .bind(floor.to_string())
    .bind(now)
    .bind(now)
    .fetch_optional(pool)
    .await
    .context("update sync origin sequence")?;
    next.context("invalid or exhausted sync origin sequence")?
        .parse::<u64>()
        .context("invalid sync origin sequence")
}

/// Reads the logical clock that belongs to the currently loaded device identity.
pub async fn current_origin_sequence(pool: &SqlitePool) -> Result<Option<u64>> {
    state_value(pool, "origin_sequence")
        .await?
        .map(|value| value.parse::<u64>().context("invalid sync origin sequence"))
        .transpose()
}

/// Advances the local logical clock in the transaction that exposes an authenticated remote state.
pub(crate) async fn observe_origin_sequence_on(
    connection: &mut SqliteConnection,
    sequence: u64,
) -> Result<()> {
    let sequence = i64::try_from(sequence).context("sync origin sequence too large")?;
    let now = Utc::now();
    let updated = sqlx::query(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES ('origin_sequence', ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = MAX(CAST(sync_state.value AS INTEGER), CAST(excluded.value AS INTEGER)),
            updated_at = excluded.updated_at
        WHERE sync_state.value <> ''
          AND sync_state.value NOT GLOB '*[^0-9]*'
        "#,
    )
    .bind(sequence.to_string())
    .bind(now)
    .bind(now)
    .execute(connection)
    .await
    .context("observe synchronized event sequence")?;
    if updated.rows_affected() == 0 {
        bail!("invalid sync origin sequence");
    }
    Ok(())
}

/// Stores an encrypted event in the local append-only log. Duplicate events are idempotent.
#[cfg(test)]
pub async fn insert_event(
    pool: &SqlitePool,
    event: &EncryptedEvent,
    applied: bool,
    blobs: &[StoredBlob],
) -> Result<InsertEventResult> {
    insert_event_with_content_presence(pool, event, applied, blobs, None).await
}

/// Stores a local content event and its implied presence version atomically.
#[cfg(test)]
pub async fn insert_content_event(
    pool: &SqlitePool,
    event: &EncryptedEvent,
    blobs: &[StoredBlob],
    sync_key: &str,
    is_sensitive: bool,
) -> Result<InsertEventResult> {
    insert_event_with_content_presence(pool, event, true, blobs, Some((sync_key, is_sensitive)))
        .await
}

#[cfg(test)]
async fn insert_event_with_content_presence(
    pool: &SqlitePool,
    event: &EncryptedEvent,
    applied: bool,
    blobs: &[StoredBlob],
    content_presence: Option<(&str, bool)>,
) -> Result<InsertEventResult> {
    let mut transaction = pool.begin().await.context("begin sync event transaction")?;
    let result = insert_event_on(
        &mut transaction,
        event,
        applied,
        blobs,
        content_presence.map(|(sync_key, is_sensitive)| (sync_key, is_sensitive, None)),
    )
    .await?;
    transaction.commit().await.context("commit sync event")?;
    Ok(result)
}

async fn insert_event_on(
    connection: &mut SqliteConnection,
    event: &EncryptedEvent,
    applied: bool,
    blobs: &[StoredBlob],
    content_presence: Option<(&str, bool, Option<i64>)>,
) -> Result<InsertEventResult> {
    let now = Utc::now();
    let origin_sequence =
        i64::try_from(event.origin_sequence).context("sync origin sequence too large")?;
    let result = sqlx::query(
        r#"
        INSERT OR IGNORE INTO sync_events (
            event_id, origin_device_id, origin_sequence, event_created_at_ms,
            nonce, ciphertext, event_kind, is_applied, apply_state, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&event.event_id)
    .bind(&event.origin_device_id)
    .bind(origin_sequence)
    .bind(event.created_at_ms)
    .bind(&event.nonce)
    .bind(&event.ciphertext)
    .bind(event.kind.as_i64())
    .bind(applied)
    .bind(if applied { "applied" } else { "pending" })
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .context("insert sync event")?;
    let inserted = result.rows_affected() == 1;
    let stored = if inserted {
        true
    } else {
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM sync_events
                WHERE event_id = ?
                  AND origin_device_id = ?
                  AND origin_sequence = ?
                  AND event_created_at_ms = ?
                  AND nonce = ?
                  AND ciphertext = ?
                  AND event_kind = ?
            )
            "#,
        )
        .bind(&event.event_id)
        .bind(&event.origin_device_id)
        .bind(origin_sequence)
        .bind(event.created_at_ms)
        .bind(&event.nonce)
        .bind(&event.ciphertext)
        .bind(event.kind.as_i64())
        .fetch_one(&mut *connection)
        .await
        .context("check existing sync event")?
            != 0
    };
    if stored {
        observe_origin_sequence_on(connection, event.origin_sequence).await?;
    }
    if inserted {
        for blob in blobs {
            sqlx::query(
                r#"
                INSERT INTO sync_event_blobs (
                    event_id, blob_id, encrypted_path, size, source_path,
                    source_is_directory, stream_nonce, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&event.event_id)
            .bind(&blob.blob_id)
            .bind(&blob.encrypted_path)
            .bind(i64::try_from(blob.size).context("encrypted sync blob is too large")?)
            .bind(&blob.source_path)
            .bind(blob.source_is_directory)
            .bind(&blob.stream_nonce)
            .bind(now)
            .bind(now)
            .execute(&mut *connection)
            .await
            .context("insert sync event blob")?;
        }
    }
    if stored {
        if let Some((sync_key, is_sensitive, operation_order)) = content_presence {
            crate::db::sync_item_state::record_content_presence_at_order(
                connection,
                sync_key,
                is_sensitive,
                &crate::db::sync_item_state::SyncEventVersion {
                    origin_sequence: event.origin_sequence,
                    origin_device_id: event.origin_device_id.clone(),
                },
                operation_order,
            )
            .await
            .context("record synchronized content presence")?;
        }
    }
    Ok(InsertEventResult { inserted, stored })
}

pub struct ContentOutboxCommit<'a> {
    pub operation_order: i64,
    pub item_id: &'a str,
    pub event: &'a EncryptedEvent,
    pub blobs: &'a [StoredBlob],
    pub sync_key: &'a str,
    pub is_sensitive: bool,
    pub delivery_route: Option<&'a str>,
}

/// Atomically publishes one materialized content event and removes its durable outbox entry.
pub async fn commit_content_outbox_event(
    pool: &SqlitePool,
    commit: ContentOutboxCommit<'_>,
) -> Result<InsertEventResult> {
    let ContentOutboxCommit {
        operation_order,
        item_id,
        event,
        blobs,
        sync_key,
        is_sensitive,
        delivery_route,
    } = commit;
    let mut transaction = pool.begin().await.context("begin content outbox commit")?;
    let result = insert_event_on(
        &mut transaction,
        event,
        true,
        blobs,
        Some((sync_key, is_sensitive, Some(operation_order))),
    )
    .await?;
    if !result.stored {
        bail!("同步事件序号冲突，请重新复制内容");
    }
    let now = Utc::now();
    sqlx::query("UPDATE sync_events SET delivery_route = ?, updated_at = ? WHERE event_id = ?")
        .bind(delivery_route)
        .bind(now)
        .bind(&event.event_id)
        .execute(&mut *transaction)
        .await
        .context("persist sync event delivery route")?;
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO sync_item_events (
            item_id, event_id, direction, created_at, updated_at
        )
        SELECT ?, ?, 'local', ?, ?
        WHERE EXISTS (SELECT 1 FROM clipboard_items WHERE id = ?)
        "#,
    )
    .bind(item_id)
    .bind(&event.event_id)
    .bind(now)
    .bind(now)
    .bind(item_id)
    .execute(&mut *transaction)
    .await
    .context("link content outbox event to clipboard item")?;
    sqlx::query("DELETE FROM sync_pending_items WHERE item_id = ?")
        .bind(item_id)
        .execute(&mut *transaction)
        .await
        .context("clear pending clipboard item after content commit")?;
    sqlx::query("DELETE FROM sync_content_outbox WHERE operation_order = ?")
        .bind(operation_order)
        .execute(&mut *transaction)
        .await
        .context("complete content outbox entry")?;
    transaction
        .commit()
        .await
        .context("commit content outbox event")?;
    Ok(result)
}

/// Atomically appends one local item-state event and clears only the serialized state revision.
pub async fn commit_item_state_event(
    pool: &SqlitePool,
    event: &EncryptedEvent,
    state: &crate::db::sync_item_state::DirtyItemState,
) -> Result<InsertEventResult> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin item-state outbox commit")?;
    let result = insert_event_on(&mut transaction, event, true, &[], None).await?;
    if !result.stored {
        bail!("同步状态事件序号冲突");
    }
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO sync_item_events (
            item_id, event_id, direction, created_at, updated_at
        )
        SELECT id, ?, 'local', ?, ?
        FROM clipboard_items
        WHERE sync_key = ?
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(&event.event_id)
    .bind(now)
    .bind(now)
    .bind(&state.sync_key)
    .execute(&mut *transaction)
    .await
    .context("link item-state outbox event")?;
    crate::db::sync_item_state::mark_state_sent_on(
        &mut transaction,
        state,
        &crate::db::sync_item_state::SyncEventVersion {
            origin_sequence: event.origin_sequence,
            origin_device_id: event.origin_device_id.clone(),
        },
    )
    .await?;
    transaction
        .commit()
        .await
        .context("commit item-state outbox event")?;
    Ok(result)
}

/// Associates a clipboard record with the encrypted event that carries it.
pub async fn link_event_to_item(
    pool: &SqlitePool,
    item_id: &str,
    event_id: &str,
    direction: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO sync_item_events (
            item_id, event_id, direction, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        "#,
    )
    .bind(item_id)
    .bind(event_id)
    .bind(direction)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("link clipboard item to sync event")?;
    Ok(())
}

pub async fn event_for_item(pool: &SqlitePool, item_id: &str) -> Result<Option<String>> {
    sqlx::query_scalar(
        "SELECT i.event_id FROM sync_item_events i JOIN sync_events e ON e.event_id = i.event_id \
         WHERE i.item_id = ? AND e.event_kind = ? ORDER BY i.created_at DESC LIMIT 1",
    )
    .bind(item_id)
    .bind(SyncEventKind::Content.as_i64())
    .fetch_optional(pool)
    .await
    .context("find sync event for clipboard item")
}

/// Loads the encrypted content event retained for a metadata-only remote file card.
pub async fn remote_file_event(
    pool: &SqlitePool,
    item_id: &str,
) -> Result<Option<StoredSyncEvent>> {
    let row = sqlx::query(
        r#"
        SELECT e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext, e.event_kind
        FROM sync_remote_file_items remote
        JOIN sync_events e ON e.event_id = remote.event_id
        WHERE remote.item_id = ? AND remote.state = 'available'
        LIMIT 1
        "#,
    )
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .context("load metadata-only synchronized file event")?;

    row.map(event_from_row).transpose()
}

/// Expires the same event a completed download attempt proved unavailable.
pub async fn expire_remote_file_item(
    pool: &SqlitePool,
    item_id: &str,
    event_id: &str,
) -> Result<bool> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin expiring remote file item")?;
    let result = sqlx::query(
        "UPDATE sync_remote_file_items SET event_id = NULL, state = 'expired', updated_at = ? \
         WHERE item_id = ? AND event_id = ? AND state = 'available'",
    )
    .bind(Utc::now())
    .bind(item_id)
    .bind(event_id)
    .execute(&mut *transaction)
    .await
    .context("expire remote file item")?;
    if result.rows_affected() > 0 {
        sqlx::query(
            "DELETE FROM sync_item_events WHERE item_id = ? AND event_id = ? AND direction = 'remote'",
        )
        .bind(item_id)
        .bind(event_id)
        .execute(&mut *transaction)
        .await
        .context("release expired remote file event")?;
    }
    transaction
        .commit()
        .await
        .context("commit expired remote file item")?;
    Ok(result.rows_affected() > 0)
}

fn event_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredSyncEvent> {
    let cursor: i64 = row.try_get("cursor")?;
    let sequence: i64 = row.try_get("origin_sequence")?;
    Ok(StoredSyncEvent {
        cursor: u64::try_from(cursor).context("negative local sync cursor")?,
        event: EncryptedEvent {
            event_id: row.try_get("event_id")?,
            origin_device_id: row.try_get("origin_device_id")?,
            origin_sequence: u64::try_from(sequence).context("negative sync origin sequence")?,
            created_at_ms: row.try_get("event_created_at_ms")?,
            nonce: row.try_get("nonce")?,
            ciphertext: row.try_get("ciphertext")?,
            kind: event_kind_from_row(&row)?,
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
            retention_key: None,
        },
    })
}

fn event_kind_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<SyncEventKind> {
    let value: i64 = row.try_get("event_kind")?;
    SyncEventKind::from_i64(value).context("invalid synchronized event kind")
}

async fn state_value(pool: &SqlitePool, key: &str) -> Result<Option<String>> {
    sqlx::query_scalar("SELECT value FROM sync_state WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
        .context("read sync state")
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
    .context("update sync state")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::test_support::memory_pool;
    use ecopaste_sync_protocol::MAX_EVENTS_PER_BATCH;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::*;

    #[tokio::test]
    async fn legacy_registration_learns_versioned_protection_before_content_sync() {
        use crate::db::sync_item_state::{merge_retention_state, SyncEventVersion};
        let pool = memory_pool().await;
        sqlx::query("INSERT INTO clipboard_items(id,kind,content,content_hash,sync_key,platform,created_at,updated_at) VALUES ('legacy','text','content','legacy-key','legacy-key','macos',?,?)")
            .bind(Utc::now()).bind(Utc::now()).execute(&pool).await.unwrap();
        let mut patch = crate::sync::SyncedItemStatePatch {
            sync_key: "legacy-key".into(),
            is_favorite: Some(true),
            is_pinned: None,
            note: Some("deferred note".into()),
            note_changed: true,
            is_present: None,
        };
        let mut version = SyncEventVersion {
            origin_sequence: 2,
            origin_device_id: "remote".into(),
        };
        merge_retention_state(&pool, &patch, &version)
            .await
            .unwrap();
        assert_eq!(protected_sync_keys(&pool).await.unwrap(), ["legacy-key"]);
        let visible: (bool, Option<String>) =
            sqlx::query_as("SELECT is_favorite, note FROM clipboard_items WHERE id = 'legacy'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(visible, (false, None));
        assert_eq!(cloud_cursor(&pool).await.unwrap(), 0);

        patch.is_favorite = Some(false);
        patch.is_pinned = Some(true);
        version.origin_sequence = 3;
        merge_retention_state(&pool, &patch, &version)
            .await
            .unwrap();
        patch.is_pinned = Some(false);
        version.origin_sequence = 1;
        merge_retention_state(&pool, &patch, &version)
            .await
            .unwrap();
        assert_eq!(protected_sync_keys(&pool).await.unwrap(), ["legacy-key"]);
        version.origin_sequence = 4;
        merge_retention_state(&pool, &patch, &version)
            .await
            .unwrap();
        assert!(protected_sync_keys(&pool).await.unwrap().is_empty());
        let staged: (Option<bool>, Option<String>, i64) = sqlx::query_as("SELECT is_present, note, dirty_mask FROM sync_item_states WHERE sync_key = 'legacy-key'").fetch_one(&pool).await.unwrap();
        assert_eq!(staged, (None, None, 0));
    }

    #[tokio::test]
    async fn legacy_registration_protects_reused_content_before_normal_sync() {
        use crate::db::sync_item_state::{
            apply_remote_patch, merge_retention_state, SyncEventVersion,
        };
        use crate::sync::SyncedItemStatePatch;

        for favorite in [true, false] {
            let pool = memory_pool().await;
            let mut patch = SyncedItemStatePatch {
                sync_key: "reused".into(),
                is_favorite: None,
                is_pinned: None,
                note: None,
                note_changed: false,
                is_present: Some(false),
            };
            let mut version = SyncEventVersion {
                origin_sequence: 10,
                origin_device_id: "remote".into(),
            };
            apply_remote_patch(&pool, &patch, &version).await.unwrap();

            patch.is_present = None;
            patch.is_favorite = Some(favorite);
            patch.is_pinned = Some(!favorite);
            version.origin_sequence = 30;
            merge_retention_state(&pool, &patch, &version)
                .await
                .unwrap();
            assert!(protected_sync_keys(&pool).await.unwrap().is_empty());

            // Content can arrive in a later page even when its causal version precedes the flags.
            patch.is_favorite = None;
            patch.is_pinned = None;
            patch.is_present = Some(true);
            version.origin_sequence = 20;
            merge_retention_state(&pool, &patch, &version)
                .await
                .unwrap();
            assert_eq!(protected_sync_keys(&pool).await.unwrap(), ["reused"]);
            let staged: (Option<bool>, i64, i64) = sqlx::query_as(
                "SELECT is_present, presence_origin_sequence, dirty_mask FROM sync_item_states WHERE sync_key = 'reused'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(staged, (Some(true), 20, 0));
            assert_eq!(cloud_cursor(&pool).await.unwrap(), 0);
            let visible: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM clipboard_items")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(visible, 0);

            patch.is_present = Some(false);
            version.origin_sequence = 10;
            merge_retention_state(&pool, &patch, &version)
                .await
                .unwrap();
            assert_eq!(protected_sync_keys(&pool).await.unwrap(), ["reused"]);

            version.origin_sequence = 40;
            merge_retention_state(&pool, &patch, &version)
                .await
                .unwrap();
            assert!(protected_sync_keys(&pool).await.unwrap().is_empty());
            patch.is_present = Some(true);
            version.origin_sequence = 20;
            merge_retention_state(&pool, &patch, &version)
                .await
                .unwrap();
            assert!(protected_sync_keys(&pool).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn protection_includes_favorites_pins_and_unmaterialized_remote_state() {
        let pool = memory_pool().await;
        for (id, favorite, pinned) in [
            ("favorite", true, false),
            ("pinned", false, true),
            ("ordinary", false, false),
        ] {
            sqlx::query("INSERT INTO clipboard_items(id,kind,content,content_hash,sync_key,platform,is_favorite,is_pinned,created_at,updated_at) VALUES (?,'text',?,?,?,'macos',?,?,?,?)")
                .bind(id).bind(id).bind(id).bind(id).bind(favorite).bind(pinned).bind(Utc::now()).bind(Utc::now())
                .execute(&pool).await.unwrap();
        }
        for (key, present) in [("remote-protected", true), ("remote-deleted", false)] {
            sqlx::query("INSERT INTO sync_item_states(sync_key,is_favorite,is_present,created_at,updated_at) VALUES (?,1,?,?,?)")
                .bind(key).bind(present).bind(Utc::now()).bind(Utc::now()).execute(&pool).await.unwrap();
        }
        assert_eq!(
            protected_sync_keys(&pool).await.unwrap(),
            ["favorite", "pinned", "remote-protected"]
        );
        set_cloud_retention_snapshot(&pool, "snapshot")
            .await
            .unwrap();
        assert_eq!(
            cloud_retention_snapshot(&pool).await.unwrap().as_deref(),
            Some("snapshot")
        );
        let operation = crate::db::sync_queue::stage_existing_content(
            &pool,
            "favorite",
            "favorite",
            "revision",
            crate::db::sync_queue::ContentSyncOptions {
                manual_share: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        set_state_value(&pool, CLOUD_CURSOR_KEY, "42")
            .await
            .unwrap();

        clear_cloud_retention_snapshot(&pool).await.unwrap();
        assert!(cloud_retention_snapshot(&pool).await.unwrap().is_none());
        assert_eq!(
            protected_sync_keys(&pool).await.unwrap(),
            ["favorite", "pinned", "remote-protected"]
        );
        assert_eq!(cloud_cursor(&pool).await.unwrap(), 42);
        let pending = crate::db::sync_queue::next_ready_content(&pool, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.operation_order, operation);
        assert!(pending.manual_share);

        set_cloud_retention_snapshot(&pool, "refreshed")
            .await
            .unwrap();
        clear_group_state(&pool).await.unwrap();
        assert!(cloud_retention_snapshot(&pool).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn origin_index_preserves_per_target_outbox_and_cursor_order() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run(&pool).await.unwrap();
        sqlx::query("DROP INDEX idx_sync_events_origin_cursor")
            .execute(&pool)
            .await
            .unwrap();
        for (id, sequence, origin) in [
            ("first", 8, "local"),
            ("remote", 4, "other"),
            ("last", 2, "local"),
        ] {
            let mut event = test_event(id, sequence);
            event.origin_device_id = origin.to_owned();
            insert_event(&pool, &event, true, &[]).await.unwrap();
        }
        mark_delivered(&pool, "cloud", &["first".to_owned()])
            .await
            .unwrap();
        let ids = |events: Vec<StoredSyncEvent>| {
            events
                .into_iter()
                .map(|stored| stored.event.event_id)
                .collect::<Vec<_>>()
        };
        let before = ids(
            pending_origin_events_for_target(&pool, "cloud", "local", 10)
                .await
                .unwrap(),
        );
        sqlx::query(
            "CREATE INDEX idx_sync_events_origin_cursor \
             ON sync_events(origin_device_id, cursor)",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            ids(
                pending_origin_events_for_target(&pool, "cloud", "local", 10)
                    .await
                    .unwrap()
            ),
            before
        );
        assert_eq!(before, ["last"]);
        assert_eq!(
            ids(
                pending_origin_events_for_target(&pool, "another", "local", 10)
                    .await
                    .unwrap()
            ),
            ["first", "last"]
        );
        assert_eq!(
            ids(
                pending_origin_events_for_target(&pool, "another", "local", 1)
                    .await
                    .unwrap()
            ),
            ["first"]
        );
    }

    #[tokio::test]
    async fn unapplied_index_preserves_order_limits_and_applied_state() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run(&pool).await.unwrap();
        sqlx::query("DROP INDEX idx_sync_events_unapplied_cursor")
            .execute(&pool)
            .await
            .unwrap();
        for (id, sequence, applied) in [
            ("already-applied", 1, true),
            ("pending-z", 4, false),
            ("pending-a", 2, false),
            ("pending-m", 3, false),
        ] {
            insert_event(&pool, &test_event(id, sequence), applied, &[])
                .await
                .unwrap();
        }
        let before = unapplied_events(&pool, 10).await.unwrap();
        assert_eq!(
            before
                .iter()
                .map(|stored| stored.event.event_id.as_str())
                .collect::<Vec<_>>(),
            ["pending-z", "pending-a", "pending-m"]
        );

        sqlx::query(
            "CREATE INDEX idx_sync_events_unapplied_cursor \
             ON sync_events(cursor) WHERE is_applied = 0",
        )
        .execute(&pool)
        .await
        .unwrap();

        for limit in [0, 1, 2, 10] {
            let after = unapplied_events(&pool, limit).await.unwrap();
            let expected_len = usize::from(limit).min(before.len());
            assert_eq!(after.len(), expected_len);
            for (actual, expected) in after.iter().zip(&before[..expected_len]) {
                assert_eq!(actual.cursor, expected.cursor);
                assert_eq!(actual.event, expected.event);
            }
        }
        let plan = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT cursor, event_id, origin_device_id, origin_sequence,
                   event_created_at_ms, nonce, ciphertext
            FROM sync_events
            WHERE is_applied = 0
            ORDER BY cursor ASC
            LIMIT ?
            "#,
        )
        .bind(10_i64)
        .fetch_all(&pool)
        .await
        .unwrap();
        let details: Vec<String> = plan.iter().map(|row| row.get("detail")).collect();
        assert!(details
            .iter()
            .any(|detail| detail.contains("idx_sync_events_unapplied_cursor")));
        assert!(!details.iter().any(|detail| detail.contains("TEMP B-TREE")));

        mark_applied(&pool, "pending-z").await.unwrap();
        insert_event(&pool, &test_event("pending-new", 5), false, &[])
            .await
            .unwrap();
        let pending = unapplied_events(&pool, 10).await.unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|stored| stored.event.event_id.as_str())
                .collect::<Vec<_>>(),
            ["pending-a", "pending-m", "pending-new"]
        );

        for stored in pending {
            mark_applied(&pool, &stored.event.event_id).await.unwrap();
        }
        assert!(unapplied_events(&pool, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn state_events_are_not_hidden_behind_a_full_content_batch() {
        let pool = memory_pool().await;
        for sequence in 1..=MAX_EVENTS_PER_BATCH {
            let event_id = format!("content-{sequence}");
            insert_event(
                &pool,
                &test_event(&event_id, u64::from(sequence)),
                false,
                &[],
            )
            .await
            .unwrap();
        }
        let mut state = test_event("state", u64::from(MAX_EVENTS_PER_BATCH) + 1);
        state.kind = SyncEventKind::ItemState;
        insert_event(&pool, &state, false, &[]).await.unwrap();

        assert_eq!(
            unapplied_events(&pool, MAX_EVENTS_PER_BATCH)
                .await
                .unwrap()
                .len(),
            usize::from(MAX_EVENTS_PER_BATCH)
        );
        let states = unapplied_events_by_kind(&pool, SyncEventKind::ItemState, 1)
            .await
            .unwrap();
        assert_eq!(states[0].event.event_id, "state");
    }

    #[tokio::test]
    async fn item_content_lookup_ignores_newer_state_events() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "item").await;
        let content = test_event("content", 1);
        insert_event(&pool, &content, true, &[]).await.unwrap();
        link_event_to_item(&pool, "item", "content", "local")
            .await
            .unwrap();
        let mut state = test_event("state", 2);
        state.kind = SyncEventKind::ItemState;
        insert_event(&pool, &state, true, &[]).await.unwrap();
        link_event_to_item(&pool, "item", "state", "local")
            .await
            .unwrap();

        assert_eq!(
            event_for_item(&pool, "item").await.unwrap().as_deref(),
            Some("content")
        );
        assert_eq!(
            latest_local_event_for_item(&pool, "item")
                .await
                .unwrap()
                .unwrap()
                .event
                .event_id,
            "content"
        );
    }

    #[tokio::test]
    async fn any_successful_route_clears_the_pending_event_count() {
        let pool = memory_pool().await;
        insert_test_event(&pool, "event-1").await;

        assert_eq!(status_counts(&pool).await.unwrap().0, 1);

        mark_delivered(&pool, "peer:android", &["event-1".into()])
            .await
            .unwrap();

        assert_eq!(status_counts(&pool).await.unwrap().0, 0);
    }

    #[tokio::test]
    async fn lan_delivery_keeps_the_event_pending_for_cloud() {
        let pool = memory_pool().await;
        insert_test_event(&pool, "event-1").await;

        mark_delivered(&pool, "peer:android", &["event-1".into()])
            .await
            .unwrap();

        assert!(pending_events_for_target(&pool, "peer:android", 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            pending_events_for_target(&pool, "cloud", 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn explicit_delivery_route_excludes_the_other_transport() {
        let pool = memory_pool().await;
        insert_test_event(&pool, "event-1").await;
        insert_test_event(&pool, "event-2").await;
        sqlx::query("UPDATE sync_events SET delivery_route = 'lan' WHERE event_id = 'event-1'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE sync_events SET delivery_route = 'cloud' WHERE event_id = 'event-2'")
            .execute(&pool)
            .await
            .unwrap();

        let lan = pending_events_for_target(&pool, "peer:android", 10)
            .await
            .unwrap();
        let cloud = pending_origin_events_for_target(&pool, "cloud", "mac", 10)
            .await
            .unwrap();

        assert_eq!(
            lan.into_iter()
                .map(|event| event.event.event_id)
                .collect::<Vec<_>>(),
            vec!["event-1"]
        );
        assert_eq!(
            cloud
                .into_iter()
                .map(|event| event.event.event_id)
                .collect::<Vec<_>>(),
            vec!["event-2"]
        );
    }

    #[tokio::test]
    async fn canceled_delivery_waits_for_explicit_item_retry() {
        let pool = memory_pool().await;
        insert_test_event(&pool, "event-1").await;
        insert_test_item(&pool, "item").await;
        link_event_to_item(&pool, "item", "event-1", "local")
            .await
            .unwrap();
        mark_delivery_queued(&pool, "cloud", &["event-1".into()])
            .await
            .unwrap();

        cancel_delivery(&pool, "cloud", "event-1").await.unwrap();
        assert_eq!(
            canceled_delivery_ids(&pool, "cloud", &["event-1".into(), "not-canceled".into()])
                .await
                .unwrap(),
            HashSet::from(["event-1".to_owned()])
        );
        let status = item_statuses(&pool, &["item".into()])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(status.cloud.state, SyncItemState::Error);
        assert_eq!(
            status.cloud.last_error.as_deref(),
            Some("transfer canceled")
        );
        assert!(pending_origin_events_for_target(&pool, "cloud", "mac", 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            pending_events_for_target(&pool, "peer:phone", 10)
                .await
                .unwrap()
                .len(),
            1
        );

        resume_item_delivery(&pool, "item", true).await.unwrap();
        assert_eq!(
            pending_origin_events_for_target(&pool, "cloud", "mac", 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn retry_preserves_one_route_and_selecting_the_other_expands_it() {
        let pool = memory_pool().await;
        insert_test_event(&pool, "event-1").await;
        insert_test_item(&pool, "item").await;
        link_event_to_item(&pool, "item", "event-1", "local")
            .await
            .unwrap();
        sqlx::query("UPDATE sync_events SET delivery_route = 'cloud' WHERE event_id = 'event-1'")
            .execute(&pool)
            .await
            .unwrap();

        resume_item_delivery(&pool, "item", true).await.unwrap();
        let route: Option<String> =
            sqlx::query_scalar("SELECT delivery_route FROM sync_events WHERE event_id = 'event-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(route.as_deref(), Some("cloud"));

        resume_item_delivery(&pool, "item", false).await.unwrap();
        let route: Option<String> =
            sqlx::query_scalar("SELECT delivery_route FROM sync_events WHERE event_id = 'event-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(route, None);
    }

    #[tokio::test]
    async fn source_icon_uploads_remain_queued_until_confirmed() {
        let pool = memory_pool().await;
        let initial = StoredBlob {
            blob_id: "icon-blob".into(),
            encrypted_path: "old.bin".into(),
            size: 12,
            source_path: Some("old-icon.png".into()),
            source_is_directory: false,
            stream_nonce: Some(vec![1; 24]),
        };
        let refreshed = StoredBlob {
            blob_id: initial.blob_id.clone(),
            encrypted_path: "new.bin".into(),
            size: 24,
            source_path: Some("new-icon.png".into()),
            source_is_directory: true,
            stream_nonce: Some(vec![2; 24]),
        };

        enqueue_source_icon_uploads(&pool, "group-a", &[initial])
            .await
            .unwrap();
        enqueue_source_icon_uploads(&pool, "group-a", std::slice::from_ref(&refreshed))
            .await
            .unwrap();

        let pending = pending_source_icon_uploads(&pool, "group-a", 10)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].encrypted_path, refreshed.encrypted_path);
        assert_eq!(pending[0].size, refreshed.size);
        assert_eq!(pending[0].source_path, refreshed.source_path);
        assert_eq!(
            pending[0].source_is_directory,
            refreshed.source_is_directory
        );
        assert_eq!(pending[0].stream_nonce, refreshed.stream_nonce);

        complete_source_icon_uploads(&pool, "group-a", &[refreshed.blob_id])
            .await
            .unwrap();
        assert!(pending_source_icon_uploads(&pool, "group-a", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn history_backfill_marker_is_scoped_to_the_sync_space() {
        let pool = memory_pool().await;

        assert!(!history_backfill_completed(&pool, "group-a").await.unwrap());
        set_state_value(&pool, HISTORY_BACKFILL_GROUP_KEY, "group-a")
            .await
            .unwrap();
        assert!(!history_backfill_completed(&pool, "group-a").await.unwrap());
        mark_history_backfill_completed(&pool, "group-a")
            .await
            .unwrap();
        assert!(history_backfill_completed(&pool, "group-a").await.unwrap());
        assert!(!history_backfill_completed(&pool, "group-b").await.unwrap());
    }

    #[tokio::test]
    async fn cloud_bootstrap_tracks_pending_and_completed_phases() {
        let pool = memory_pool().await;

        assert!(!cloud_bootstrap_pending(&pool, "group-a").await.unwrap());
        start_cloud_bootstrap(&pool, "group-a", 42, 84)
            .await
            .unwrap();
        assert_eq!(cloud_cursor(&pool).await.unwrap(), 42);
        assert!(cloud_bootstrap_pending(&pool, "group-a").await.unwrap());
        assert_eq!(
            cloud_bootstrap_target(&pool, "group-a").await.unwrap(),
            Some(84)
        );
        assert!(cloud_bootstrap_in_progress(&pool).await.unwrap());
        assert!(!cloud_bootstrap_completed(&pool, "group-a").await.unwrap());

        mark_cloud_bootstrap_completed(&pool, "group-a")
            .await
            .unwrap();
        assert!(!cloud_bootstrap_pending(&pool, "group-a").await.unwrap());
        assert_eq!(
            cloud_bootstrap_target(&pool, "group-a").await.unwrap(),
            None
        );
        assert!(!cloud_bootstrap_in_progress(&pool).await.unwrap());
        assert!(cloud_bootstrap_completed(&pool, "group-a").await.unwrap());
        assert!(!cloud_bootstrap_completed(&pool, "group-b").await.unwrap());
    }

    #[tokio::test]
    async fn released_content_event_is_pruned_only_after_its_item_is_gone() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "item-1").await;
        insert_test_event(&pool, "event-1").await;
        link_event_to_item(&pool, "item-1", "event-1", "local")
            .await
            .unwrap();
        assert_eq!(prune_released_content_events(&pool, 10).await.unwrap(), 0);
        sqlx::query("DELETE FROM clipboard_items WHERE id = 'item-1'")
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(prune_released_content_events(&pool, 10).await.unwrap(), 1);
        assert!(events_after_cursor(&pool, 0, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn group_state_clear_preserves_sequence_and_drops_item_state() {
        let pool = memory_pool().await;
        let first = next_origin_sequence(&pool).await.unwrap();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO sync_item_states (sync_key, created_at, updated_at) VALUES ('hash', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
        insert_test_item(&pool, "item").await;
        let (sync_key, source_revision): (String, String) = sqlx::query_as(
            "SELECT sync_key, source_revision FROM clipboard_items WHERE id = 'item'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        crate::db::sync_queue::stage_existing_content(
            &pool,
            "item",
            &sync_key,
            &source_revision,
            crate::db::sync_queue::ContentSyncOptions::default(),
        )
        .await
        .unwrap();

        clear_group_state(&pool).await.unwrap();
        let second = next_origin_sequence(&pool).await.unwrap();
        let state_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_item_states")
            .fetch_one(&pool)
            .await
            .unwrap();
        let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_content_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert!(second > first);
        assert_eq!(state_count, 0);
        assert_eq!(outbox_count, 0);
    }

    #[tokio::test]
    async fn imported_group_state_drops_remote_cards_and_keeps_local_history() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "local-item").await;
        insert_test_item(&pool, "remote-item").await;
        insert_test_event(&pool, "remote-event").await;
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO sync_remote_file_items (
                item_id, event_id, origin_device_id, state, created_at, updated_at
            ) VALUES ('remote-item', 'remote-event', 'mac', 'available', ?, ?)
            "#,
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sync_active_transfers (task_id, item_id, created_at, updated_at) \
             VALUES ('task', 'local-item', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
        set_state_value(&pool, "origin_sequence", "999")
            .await
            .unwrap();

        let removed = clear_imported_group_state(&pool, Some(123)).await.unwrap();

        let local_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM clipboard_items WHERE id = 'local-item'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let remote_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM clipboard_items WHERE id = 'remote-item'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        let active_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_active_transfers")
            .fetch_one(&pool)
            .await
            .unwrap();
        let restored_sequence = current_origin_sequence(&pool).await.unwrap();

        assert_eq!(removed, 1);
        assert_eq!(local_count, 1);
        assert_eq!(remote_count, 0);
        assert_eq!(event_count, 0);
        assert_eq!(active_count, 0);
        assert_eq!(restored_sequence, Some(123));
    }

    #[tokio::test]
    async fn group_state_clear_expires_remote_file_card_without_deleting_it() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "remote-item").await;
        insert_test_event(&pool, "remote-event").await;
        sqlx::query(
            r#"
            INSERT INTO sync_remote_file_items (
                item_id, event_id, origin_device_id, state, created_at, updated_at
            ) VALUES ('remote-item', 'remote-event', 'mac', 'available', ?, ?)
            "#,
        )
        .bind(Utc::now())
        .bind(Utc::now())
        .execute(&pool)
        .await
        .unwrap();

        clear_group_state(&pool).await.unwrap();

        let marker: (Option<String>, String) = sqlx::query_as(
            "SELECT event_id, state FROM sync_remote_file_items WHERE item_id = 'remote-item'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let item_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM clipboard_items WHERE id = 'remote-item'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(marker, (None, "expired".to_owned()));
        assert_eq!(item_count, 1);
    }

    #[tokio::test]
    async fn unavailable_remote_file_expires_and_releases_its_event_link() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "remote-item").await;
        insert_test_event(&pool, "remote-event").await;
        link_event_to_item(&pool, "remote-item", "remote-event", "remote")
            .await
            .unwrap();
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO sync_remote_file_items (
                item_id, event_id, origin_device_id, state, created_at, updated_at
            ) VALUES ('remote-item', 'remote-event', 'mac', 'available', ?, ?)
            "#,
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            !expire_remote_file_item(&pool, "remote-item", "replaced-event")
                .await
                .unwrap()
        );
        assert!(
            expire_remote_file_item(&pool, "remote-item", "remote-event")
                .await
                .unwrap()
        );

        let marker: (Option<String>, String) = sqlx::query_as(
            "SELECT event_id, state FROM sync_remote_file_items WHERE item_id = 'remote-item'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let link_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sync_item_events WHERE item_id = 'remote-item'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(marker, (None, "expired".to_owned()));
        assert_eq!(link_count, 0);
        assert!(
            !expire_remote_file_item(&pool, "remote-item", "remote-event")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn origin_sequence_allocation_is_atomic_across_wal_connections() {
        let directory =
            tempfile::tempdir_in(std::env::current_dir().unwrap().join("target")).unwrap();
        let database_path = directory.path().join("sequence.db");
        let setup_options = SqliteConnectOptions::new()
            .filename(&database_path)
            .create_if_missing(true)
            .foreign_keys(true);
        let setup_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(setup_options)
            .await
            .unwrap();
        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode = WAL")
            .fetch_one(&setup_pool)
            .await
            .unwrap();
        assert_eq!(journal_mode, "wal");
        sqlx::migrate!("./migrations")
            .run(&setup_pool)
            .await
            .unwrap();
        setup_pool.close().await;
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(database_path)
                    .foreign_keys(true),
            )
            .await
            .unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let pool = pool.clone();
            tasks.spawn(async move { next_origin_sequence(&pool).await.unwrap() });
        }
        let mut sequences = Vec::new();
        while let Some(result) = tasks.join_next().await {
            sequences.push(result.unwrap());
        }
        sequences.sort_unstable();

        assert_eq!(sequences.len(), 64);
        assert!(sequences.windows(2).all(|values| values[0] < values[1]));
    }

    #[tokio::test]
    async fn insert_event_reports_reused_origin_sequences_without_storing_them() {
        let pool = memory_pool().await;
        let first = insert_event(&pool, &test_event("event-1", 1), false, &[])
            .await
            .unwrap();
        let duplicate = insert_event(&pool, &test_event("event-1", 1), false, &[])
            .await
            .unwrap();
        let conflict = insert_event(&pool, &test_event("event-2", 1), false, &[])
            .await
            .unwrap();

        assert_eq!(
            first,
            InsertEventResult {
                inserted: true,
                stored: true
            }
        );
        assert_eq!(
            duplicate,
            InsertEventResult {
                inserted: false,
                stored: true
            }
        );
        assert_eq!(
            conflict,
            InsertEventResult {
                inserted: false,
                stored: false
            }
        );
    }

    #[tokio::test]
    async fn content_event_and_presence_version_are_stored_together() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "item").await;
        sqlx::query("UPDATE clipboard_items SET sync_key = 'stable-key' WHERE id = 'item'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO sync_item_states (
                sync_key, is_present, presence_origin_sequence,
                presence_origin_device_id, dirty_mask, local_revision,
                created_at, updated_at
            ) VALUES ('stable-key', 0, 0, 'mac', 8, 1, '2026-01-01', '2026-01-01')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let result =
            insert_content_event(&pool, &test_event("event-1", 1), &[], "stable-key", false)
                .await
                .unwrap();

        assert!(result.stored);
        let state: (Option<i64>, Option<i64>, Option<String>, i64) = sqlx::query_as(
            r#"
            SELECT is_present, presence_origin_sequence,
                   presence_origin_device_id, dirty_mask
            FROM sync_item_states
            WHERE sync_key = 'stable-key'
            "#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(state, (Some(1), Some(1), Some("mac".into()), 0));
    }

    #[tokio::test]
    async fn content_event_commit_removes_its_outbox_entry_atomically() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "item").await;
        let (sync_key, source_revision): (String, String) = sqlx::query_as(
            "SELECT sync_key, source_revision FROM clipboard_items WHERE id = 'item'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let operation_order = crate::db::sync_queue::stage_existing_content(
            &pool,
            "item",
            &sync_key,
            &source_revision,
            crate::db::sync_queue::ContentSyncOptions::default(),
        )
        .await
        .unwrap();

        commit_content_outbox_event(
            &pool,
            ContentOutboxCommit {
                operation_order,
                item_id: "item",
                event: &test_event("event-1", 1),
                blobs: &[],
                sync_key: &sync_key,
                is_sensitive: false,
                delivery_route: None,
            },
        )
        .await
        .unwrap();

        let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_content_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sync_item_events WHERE item_id = 'item' AND event_id = 'event-1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(outbox_count, 0);
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn transient_apply_failure_is_retried_while_invalid_events_are_quarantined() {
        let pool = memory_pool().await;
        insert_event(&pool, &test_event("event-1", 1), false, &[])
            .await
            .unwrap();

        let delay = mark_apply_failed(&pool, "event-1", "database busy")
            .await
            .unwrap();
        assert_eq!(delay, chrono::Duration::seconds(2));
        assert!(unapplied_events_by_kind(&pool, SyncEventKind::Content, 10)
            .await
            .unwrap()
            .is_empty());
        sqlx::query("UPDATE sync_events SET next_apply_at = ? WHERE event_id = 'event-1'")
            .bind(Utc::now() - chrono::Duration::seconds(1))
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            unapplied_events_by_kind(&pool, SyncEventKind::Content, 10)
                .await
                .unwrap()
                .len(),
            1
        );

        mark_rejected(&pool, "event-1", "invalid payload")
            .await
            .unwrap();
        let state: (bool, String, Option<String>) = sqlx::query_as(
            "SELECT is_applied, apply_state, last_apply_error FROM sync_events WHERE event_id = 'event-1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            state,
            (true, "rejected".into(), Some("invalid payload".into()))
        );
    }

    #[tokio::test]
    async fn peer_batch_commits_events_deliveries_and_raw_device_cursor_together() {
        let pool = memory_pool().await;
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "Pixel".into(),
                platform: "android".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: vec!["192.168.1.2:35555".into()],
                relay_urls: Vec::new(),
                last_seen_ms: 100,
            },
        )
        .await
        .unwrap();
        let mut event = test_event("remote-event", 1);
        event.origin_device_id = "other".into();

        let result = store_remote_batch(
            &pool,
            RemoteBatchSource::Peer {
                device_id: "android",
            },
            Some(17),
            &[event],
            "local",
        )
        .await
        .unwrap();

        assert!(result.received_new_events);
        assert!(result.conflicts.is_empty());
        assert_eq!(list_peers(&pool).await.unwrap()[0].pull_cursor, 17);
        let delivery_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sync_deliveries WHERE event_id = 'remote-event' AND target_id IN ('peer:android', 'cloud')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(delivery_count, 1);
        assert_eq!(sqlx::query_scalar::<_, String>(
            "SELECT phase FROM sync_delivery_states WHERE event_id = 'remote-event' AND target_id = 'cloud'",
        ).fetch_one(&pool).await.unwrap(), "queued");
    }

    #[tokio::test]
    async fn cloud_peer_identity_preserves_current_lan_routes() {
        let pool = memory_pool().await;
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "Old name".into(),
                platform: "android".into(),
                endpoint_id: "endpoint-1".into(),
                direct_addresses: vec!["192.168.1.2:35555".into()],
                relay_urls: Vec::new(),
                last_seen_ms: 100,
            },
        )
        .await
        .unwrap();
        mark_peer_online(&pool, "android", Some("192.168.1.2:35555"), Some("direct"))
            .await
            .unwrap();

        upsert_cloud_peer_identity(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "New name".into(),
                platform: "android".into(),
                endpoint_id: "endpoint-1".into(),
                direct_addresses: vec!["172.19.0.1:35555".into()],
                relay_urls: vec!["https://relay.example".into()],
                last_seen_ms: 200,
            },
        )
        .await
        .unwrap();

        let peer = list_peers(&pool).await.unwrap().remove(0).announcement;
        assert_eq!(peer.device_name, "New name");
        assert_eq!(peer.direct_addresses, ["192.168.1.2:35555"]);
        assert!(peer.relay_urls.is_empty());
        assert_eq!(peer.last_seen_ms, 100);
        assert_eq!(
            preferred_peer_address(&pool, "android").await.unwrap(),
            Some("192.168.1.2:35555".into())
        );

        upsert_cloud_peer_identity(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "New identity".into(),
                platform: "android".into(),
                endpoint_id: "endpoint-2".into(),
                direct_addresses: vec!["10.45.180.213:35555".into()],
                relay_urls: Vec::new(),
                last_seen_ms: 300,
            },
        )
        .await
        .unwrap();

        let peer = list_peers(&pool).await.unwrap().remove(0).announcement;
        assert_eq!(peer.endpoint_id, "endpoint-2");
        assert!(peer.direct_addresses.is_empty());
        assert_eq!(peer.last_seen_ms, 300);
        assert_eq!(
            preferred_peer_address(&pool, "android").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn rejected_event_identity_conflict_does_not_exhaust_local_sequence() {
        let pool = memory_pool().await;
        insert_event(&pool, &test_event("event-1", 1), false, &[])
            .await
            .unwrap();

        let conflict = insert_event(&pool, &test_event("event-1", i64::MAX as u64), false, &[])
            .await
            .unwrap();

        assert!(!conflict.stored);
        assert!(next_origin_sequence(&pool).await.is_ok());
    }

    #[tokio::test]
    async fn item_status_tracks_each_route_independently() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "item-1").await;
        insert_test_event(&pool, "event-1").await;
        link_event_to_item(&pool, "item-1", "event-1", "local")
            .await
            .unwrap();
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "Pixel".into(),
                platform: "android".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: vec!["127.0.0.1:35555".into()],
                relay_urls: Vec::new(),
                last_seen_ms: 100,
            },
        )
        .await
        .unwrap();
        mark_delivery_error(&pool, "cloud", &["event-1".into()], "offline")
            .await
            .unwrap();
        mark_delivered(&pool, "peer:android", &["event-1".into()])
            .await
            .unwrap();

        let status = item_statuses(&pool, &["item-1".into()])
            .await
            .unwrap()
            .remove(0);

        assert_eq!(status.lan.state, SyncItemState::Success);
        assert_eq!(status.lan.delivered_targets, 1);
        assert_eq!(status.lan.total_targets, 1);
        assert_eq!(status.cloud.state, SyncItemState::Error);
        assert_eq!(status.cloud.last_error.as_deref(), Some("offline"));
    }

    #[tokio::test]
    async fn removed_peer_stays_blocked_until_explicitly_restored() {
        let pool = memory_pool().await;
        let peer = PeerAnnouncement {
            device_id: "android".into(),
            device_name: "Pixel".into(),
            platform: "android".into(),
            endpoint_id: "endpoint".into(),
            direct_addresses: vec!["127.0.0.1:35555".into()],
            relay_urls: Vec::new(),
            last_seen_ms: 100,
        };
        upsert_peer(&pool, &peer).await.unwrap();
        insert_test_event(&pool, "event-1").await;
        mark_delivered(&pool, "peer:android", &["event-1".into()])
            .await
            .unwrap();

        assert!(remove_peer(&pool, "android").await.unwrap().is_some());
        assert!(is_peer_removed(&pool, "android", "endpoint").await.unwrap());
        assert!(list_peers(&pool).await.unwrap().is_empty());
        let deliveries: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sync_deliveries WHERE target_id = 'peer:android'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(deliveries, 0);

        upsert_peer(&pool, &peer).await.unwrap();
        assert!(list_peers(&pool).await.unwrap().is_empty());

        restore_and_upsert_peer(&pool, &peer).await.unwrap();
        assert!(!is_peer_removed(&pool, "android", "endpoint").await.unwrap());
        assert!(removed_devices(&pool).await.unwrap()[0]
            .restored_at_ms
            .is_some());
        assert_eq!(list_peers(&pool).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn approved_peer_restoration_rolls_back_when_route_storage_fails() {
        let pool = memory_pool().await;
        let peer = PeerAnnouncement {
            device_id: "android".into(),
            device_name: "Pixel".into(),
            platform: "android".into(),
            endpoint_id: "endpoint".into(),
            direct_addresses: vec!["127.0.0.1:35555".into()],
            relay_urls: Vec::new(),
            last_seen_ms: 100,
        };
        upsert_peer(&pool, &peer).await.unwrap();
        remove_peer(&pool, "android").await.unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER reject_android_peer
            BEFORE INSERT ON sync_peers
            WHEN NEW.device_id = 'android'
            BEGIN
                SELECT RAISE(ABORT, 'blocked for test');
            END
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        assert!(restore_and_upsert_peer(&pool, &peer).await.is_err());
        assert!(is_peer_removed(&pool, "android", "endpoint").await.unwrap());
        assert!(list_peers(&pool).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn discovered_routes_update_known_peer_and_clear_legacy_relays() {
        let pool = memory_pool().await;
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "Pixel".into(),
                platform: "android".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: vec!["10.0.0.2:35555".into()],
                relay_urls: vec!["https://relay.example".into()],
                last_seen_ms: 100,
            },
        )
        .await
        .unwrap();

        assert!(
            update_peer_routes(&pool, "endpoint", &["10.0.0.3:35555".into()], true)
                .await
                .unwrap()
        );
        let peer = list_peers(&pool).await.unwrap().remove(0).announcement;
        assert_eq!(peer.direct_addresses, ["10.0.0.3:35555"]);
        assert!(peer.relay_urls.is_empty());
        assert!(
            !update_peer_routes(&pool, "unknown", &["10.0.0.4:35555".into()], true)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cloud_status_tracks_queue_upload_confirmation_and_never_regresses_after_ack() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "item").await;
        insert_test_event(&pool, "event-1").await;
        link_event_to_item(&pool, "item", "event-1", "local")
            .await
            .unwrap();
        let ids = ["event-1".to_owned()];
        let item_ids = ["item".to_owned()];
        mark_delivery_queued(&pool, "cloud", &ids).await.unwrap();
        assert_eq!(
            item_statuses(&pool, &item_ids).await.unwrap()[0]
                .cloud
                .state,
            SyncItemState::Queued
        );
        mark_delivery_syncing(&pool, "cloud", &ids).await.unwrap();
        assert_eq!(
            item_statuses(&pool, &item_ids).await.unwrap()[0]
                .cloud
                .state,
            SyncItemState::Syncing
        );
        mark_delivery_confirming(&pool, "cloud", &ids)
            .await
            .unwrap();
        assert_eq!(
            item_statuses(&pool, &item_ids).await.unwrap()[0]
                .cloud
                .state,
            SyncItemState::Confirming
        );
        mark_delivered(&pool, "cloud", &ids).await.unwrap();
        mark_delivery_error(&pool, "cloud", &ids, "another record failed")
            .await
            .unwrap();
        mark_delivery_queued(&pool, "cloud", &ids).await.unwrap();
        let status = item_statuses(&pool, &item_ids).await.unwrap().remove(0);
        assert_eq!(status.cloud.state, SyncItemState::Success);
        assert_eq!(status.cloud.last_error, None);
    }

    #[tokio::test]
    async fn peer_receipt_waits_for_the_same_event_to_be_confirmed_by_the_hub() {
        let pool = memory_pool().await;
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "peer".into(),
                device_name: "Phone".into(),
                platform: "android".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: Vec::new(),
                relay_urls: Vec::new(),
                last_seen_ms: 1,
            },
        )
        .await
        .unwrap();
        let event = test_event("remote", 1);
        store_remote_batch(
            &pool,
            RemoteBatchSource::Peer { device_id: "peer" },
            Some(17),
            std::slice::from_ref(&event),
            "local",
        )
        .await
        .unwrap();
        insert_test_item(&pool, "item").await;
        link_event_to_item(&pool, "item", &event.event_id, "remote")
            .await
            .unwrap();
        let ids = ["item".to_owned()];
        let status = item_statuses(&pool, &ids).await.unwrap().remove(0);
        assert_eq!(status.lan.state, SyncItemState::Success);
        assert_eq!(status.cloud.state, SyncItemState::Queued);
        assert!(
            pending_origin_events_for_target(&pool, "cloud", "local", 10)
                .await
                .unwrap()
                .is_empty()
        );
        store_remote_batch(&pool, RemoteBatchSource::Cloud, Some(42), &[event], "local")
            .await
            .unwrap();
        assert_eq!(
            item_statuses(&pool, &ids).await.unwrap()[0].cloud.state,
            SyncItemState::Success
        );
        assert_eq!(cloud_cursor(&pool).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn inbound_peer_push_confirms_only_lan_and_preserves_pull_progress() {
        let pool = memory_pool().await;
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "peer".into(),
                device_name: "Phone".into(),
                platform: "android".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: Vec::new(),
                relay_urls: Vec::new(),
                last_seen_ms: 1,
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE sync_peers SET pull_cursor = 17 WHERE device_id = 'peer'")
            .execute(&pool)
            .await
            .unwrap();
        let event = test_event("push", 1);
        let source = RemoteBatchSource::Peer { device_id: "peer" };
        let stored = store_remote_batch(&pool, source, None, std::slice::from_ref(&event), "local")
            .await
            .unwrap();
        assert_eq!(stored.inserted_event_ids, ["push"]);
        insert_test_item(&pool, "item").await;
        link_event_to_item(&pool, "item", &event.event_id, "remote")
            .await
            .unwrap();
        let ids = ["item".to_owned()];
        let status = item_statuses(&pool, &ids).await.unwrap().remove(0);
        assert_eq!(status.lan.state, SyncItemState::Success);
        assert_eq!(status.cloud.state, SyncItemState::Queued);
        assert_eq!(status.cloud.delivered_targets, 0);
        assert_eq!(list_peers(&pool).await.unwrap()[0].pull_cursor, 17);

        let duplicate =
            store_remote_batch(&pool, source, None, std::slice::from_ref(&event), "local")
                .await
                .unwrap();
        assert!(duplicate.inserted_event_ids.is_empty());
        assert_eq!(
            item_statuses(&pool, &ids).await.unwrap()[0].cloud.state,
            SyncItemState::Queued
        );

        store_remote_batch(
            &pool,
            RemoteBatchSource::Cloud,
            Some(42),
            std::slice::from_ref(&event),
            "local",
        )
        .await
        .unwrap();
        store_remote_batch(&pool, source, None, &[event], "local")
            .await
            .unwrap();
        let status = item_statuses(&pool, &ids).await.unwrap().remove(0);
        assert_eq!(status.cloud.state, SyncItemState::Success);
        assert_eq!(status.cloud.delivered_targets, 1);
        assert_eq!(list_peers(&pool).await.unwrap()[0].pull_cursor, 17);
        assert_eq!(cloud_cursor(&pool).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn inbound_peer_push_rolls_back_events_when_route_acknowledgement_fails() {
        let pool = memory_pool().await;
        sqlx::raw_sql(
            "CREATE TRIGGER reject_peer_receipt BEFORE INSERT ON sync_deliveries \
             WHEN NEW.target_id = 'peer:phone' BEGIN SELECT RAISE(ABORT, 'receipt failure'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let result = store_remote_batch(
            &pool,
            RemoteBatchSource::Peer { device_id: "phone" },
            None,
            &[test_event("push", 1)],
            "local",
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sync_events")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn legacy_migration_rechecks_remote_claims_without_resending_or_advancing_cloud_cursor() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let migrations = sqlx::migrate!("./migrations");
        for migration in migrations
            .iter()
            .filter(|migration| migration.version <= 16)
        {
            sqlx::raw_sql(migration.sql.clone())
                .execute(&pool)
                .await
                .unwrap();
        }
        let own = test_event("own", 1);
        let mut remote = test_event("real-remote", 2);
        remote.origin_device_id = "phone".into();
        let mut absent = test_event("false-remote", 3);
        absent.origin_device_id = "phone".into();
        let mut remote_state = test_event("remote-state", 4);
        remote_state.origin_device_id = "phone".into();
        remote_state.kind = SyncEventKind::ItemState;
        for event in [&own, &remote, &absent, &remote_state] {
            insert_event(&pool, event, true, &[]).await.unwrap();
            insert_test_item(&pool, &event.event_id).await;
            link_event_to_item(&pool, &event.event_id, &event.event_id, "remote")
                .await
                .unwrap();
            sqlx::query("INSERT INTO sync_deliveries(event_id,target_id,created_at,updated_at) VALUES (?,'cloud',?,?)")
                .bind(&event.event_id).bind(Utc::now()).bind(Utc::now()).execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO sync_delivery_states(event_id,target_id,state,created_at,updated_at) VALUES (?,'cloud','success',?,?)")
                .bind(&event.event_id).bind(Utc::now()).bind(Utc::now()).execute(&pool).await.unwrap();
        }
        set_state_value(&pool, CLOUD_CURSOR_KEY, "456")
            .await
            .unwrap();
        for version in [17, 18] {
            let migration = migrations
                .iter()
                .find(|migration| migration.version == version)
                .unwrap();
            sqlx::raw_sql(migration.sql.clone())
                .execute(&pool)
                .await
                .unwrap();
        }
        assert_eq!(
            cloud_confirmation_repair_cursor(&pool, "mac")
                .await
                .unwrap(),
            Some(None)
        );
        assert_eq!(
            item_statuses(&pool, &["own".into()]).await.unwrap()[0]
                .cloud
                .state,
            SyncItemState::Success
        );
        assert_eq!(
            item_statuses(&pool, &["false-remote".into()])
                .await
                .unwrap()[0]
                .cloud
                .state,
            SyncItemState::Confirming
        );
        let mut forged = absent.clone();
        forged.ciphertext[0] ^= 1;
        repair_cloud_confirmations(&pool, &[remote, remote_state, forged], Some(123))
            .await
            .unwrap();
        assert_eq!(
            cloud_confirmation_repair_cursor(&pool, "mac")
                .await
                .unwrap(),
            Some(Some(123))
        );
        assert_eq!(
            item_statuses(&pool, &["real-remote".into()]).await.unwrap()[0]
                .cloud
                .state,
            SyncItemState::Success
        );
        assert_eq!(
            item_statuses(&pool, &["false-remote".into()])
                .await
                .unwrap()[0]
                .cloud
                .state,
            SyncItemState::Confirming
        );
        repair_cloud_confirmations(&pool, &[], None).await.unwrap();
        assert_eq!(
            cloud_confirmation_repair_cursor(&pool, "mac")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            item_statuses(&pool, &["false-remote".into()])
                .await
                .unwrap()[0]
                .cloud
                .state,
            SyncItemState::Queued
        );
        assert_eq!(cloud_cursor(&pool).await.unwrap(), 456);
        assert_eq!(
            item_statuses(&pool, &["remote-state".into()])
                .await
                .unwrap()[0]
                .cloud
                .state,
            SyncItemState::Success
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM clipboard_items")
                .fetch_one(&pool)
                .await
                .unwrap(),
            4
        );
        assert!(pending_origin_events_for_target(&pool, "cloud", "mac", 10)
            .await
            .unwrap()
            .is_empty());
    }

    async fn insert_test_item(pool: &SqlitePool, item_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO clipboard_items (
                id, kind, content, content_hash, platform, created_at, updated_at
            ) VALUES (?, 'text', 'test', ?, 'test', '2026-01-01', '2026-01-01')
            "#,
        )
        .bind(item_id)
        .bind(format!("hash-{item_id}"))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_test_event(pool: &SqlitePool, event_id: &str) {
        let sequence = if event_id == "event-1" { 1 } else { 2 };
        insert_event(pool, &test_event(event_id, sequence), true, &[])
            .await
            .unwrap();
    }

    fn test_event(event_id: &str, origin_sequence: u64) -> EncryptedEvent {
        EncryptedEvent {
            event_id: event_id.into(),
            origin_device_id: "mac".into(),
            origin_sequence,
            created_at_ms: 100,
            nonce: vec![1; 24],
            ciphertext: vec![2; 32],
            kind: SyncEventKind::Content,
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
            retention_key: None,
        }
    }
}
