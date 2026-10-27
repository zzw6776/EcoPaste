use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
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

const CLOUD_CURSOR_KEY: &str = "cloud_cursor";
const CLOUD_BOOTSTRAP_GROUP_KEY: &str = "cloud_bootstrap_group_id";
const CLOUD_BOOTSTRAP_TARGET_KEY: &str = "cloud_bootstrap_target_cursor";
const CLOUD_TARGET: &str = "cloud";
const HISTORY_BACKFILL_GROUP_KEY: &str = "history_backfill_group_id";
const HISTORY_BACKFILL_VERSION: u8 = 2;
const HISTORY_TIMESTAMP_REPAIR_GROUP_KEY: &str = "history_timestamp_repair_group_id";
const LAST_SUCCESS_KEY: &str = "last_success_at";

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

/// Stores an encrypted event in the local append-only log. Duplicate events are idempotent.
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
        sqlx::query(
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
        .bind(origin_sequence.to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *connection)
        .await
        .context("observe synchronized event sequence")?;
    }
    if inserted {
        for blob in blobs {
            sqlx::query(
                r#"
                INSERT INTO sync_event_blobs (
                    event_id, blob_id, encrypted_path, size, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&event.event_id)
            .bind(&blob.blob_id)
            .bind(&blob.encrypted_path)
            .bind(i64::try_from(blob.size).context("encrypted sync blob is too large")?)
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

/// Atomically publishes one materialized content event and removes its durable outbox entry.
pub async fn commit_content_outbox_event(
    pool: &SqlitePool,
    operation_order: i64,
    item_id: &str,
    event: &EncryptedEvent,
    blobs: &[StoredBlob],
    sync_key: &str,
    is_sensitive: bool,
) -> Result<InsertEventResult> {
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

/// Returns the newest event originated locally for this clipboard item.
pub async fn latest_local_event_for_item(
    pool: &SqlitePool,
    item_id: &str,
) -> Result<Option<StoredSyncEvent>> {
    let row = sqlx::query(
        r#"
        SELECT e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext, e.event_kind
        FROM sync_item_events i
        JOIN sync_events e ON e.event_id = i.event_id
        WHERE i.item_id = ? AND i.direction = 'local' AND e.event_kind = ?
        ORDER BY e.cursor DESC
        LIMIT 1
        "#,
    )
    .bind(item_id)
    .bind(SyncEventKind::Content.as_i64())
    .fetch_optional(pool)
    .await
    .context("find latest local sync event for clipboard item")?;

    row.map(event_from_row).transpose()
}

/// Lists encrypted events linked to one clipboard item for timestamp reconciliation.
pub async fn linked_events_for_item(
    pool: &SqlitePool,
    item_id: &str,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext, e.event_kind
        FROM sync_item_events i
        JOIN sync_events e ON e.event_id = i.event_id
        WHERE i.item_id = ?
        ORDER BY e.cursor ASC
        "#,
    )
    .bind(item_id)
    .fetch_all(pool)
    .await
    .context("list sync events linked to clipboard item")?;

    rows.into_iter().map(event_from_row).collect()
}

/// Lists all item/event links so an upgraded client can repair retained history timestamps.
pub async fn linked_item_events(pool: &SqlitePool) -> Result<Vec<LinkedSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT i.item_id, e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext, e.event_kind
        FROM sync_item_events i
        JOIN sync_events e ON e.event_id = i.event_id
        ORDER BY e.cursor ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list clipboard item sync events")?;

    rows.into_iter()
        .map(|row| {
            let item_id = row.try_get("item_id")?;
            let stored = event_from_row(row)?;
            Ok(LinkedSyncEvent { item_id, stored })
        })
        .collect()
}

pub async fn mark_applied(pool: &SqlitePool, event_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sync_events
        SET is_applied = 1,
            apply_state = 'applied',
            apply_attempt_count = 0,
            next_apply_at = NULL,
            last_apply_error = NULL,
            updated_at = ?
        WHERE event_id = ?
        "#,
    )
    .bind(Utc::now())
    .bind(event_id)
    .execute(pool)
    .await
    .context("mark sync event applied")?;
    Ok(())
}

/// Permanently quarantines an authenticated event whose payload cannot be decoded or validated.
pub async fn mark_rejected(pool: &SqlitePool, event_id: &str, error: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sync_events
        SET is_applied = 1,
            apply_state = 'rejected',
            next_apply_at = NULL,
            last_apply_error = ?,
            updated_at = ?
        WHERE event_id = ?
        "#,
    )
    .bind(error)
    .bind(Utc::now())
    .bind(event_id)
    .execute(pool)
    .await
    .context("reject invalid sync event")?;
    Ok(())
}

/// Defers a transient application failure without acknowledging the event as applied.
pub async fn mark_apply_failed(
    pool: &SqlitePool,
    event_id: &str,
    error: &str,
) -> Result<chrono::Duration> {
    let attempts: i64 =
        sqlx::query_scalar("SELECT apply_attempt_count FROM sync_events WHERE event_id = ?")
            .bind(event_id)
            .fetch_optional(pool)
            .await
            .context("read sync event apply attempts")?
            .unwrap_or(0_i64)
            .saturating_add(1);
    let seconds = match attempts {
        i64::MIN..=1 => 2,
        2 => 5,
        3 => 15,
        4 => 30,
        5 => 60,
        _ => 300,
    };
    sqlx::query(
        r#"
        UPDATE sync_events
        SET is_applied = 0,
            apply_state = 'pending',
            apply_attempt_count = ?,
            next_apply_at = ?,
            last_apply_error = ?,
            updated_at = ?
        WHERE event_id = ?
        "#,
    )
    .bind(attempts)
    .bind(Utc::now() + chrono::Duration::seconds(seconds))
    .bind(error)
    .bind(Utc::now())
    .bind(event_id)
    .execute(pool)
    .await
    .context("defer failed sync event application")?;
    Ok(chrono::Duration::seconds(seconds))
}

pub async fn attach_event_blobs(
    pool: &SqlitePool,
    event_id: &str,
    blobs: &[StoredBlob],
) -> Result<()> {
    let now = Utc::now();
    for blob in blobs {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO sync_event_blobs (
                event_id, blob_id, encrypted_path, size, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(event_id)
        .bind(&blob.blob_id)
        .bind(&blob.encrypted_path)
        .bind(i64::try_from(blob.size).context("encrypted sync blob is too large")?)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .context("attach blob to sync event")?;
    }
    Ok(())
}

#[cfg(test)]
pub async fn unapplied_events(pool: &SqlitePool, limit: u16) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT cursor, event_id, origin_device_id, origin_sequence,
               event_created_at_ms, nonce, ciphertext, event_kind
        FROM sync_events
        WHERE is_applied = 0
          AND apply_state = 'pending'
          AND (next_apply_at IS NULL OR next_apply_at <= ?)
        ORDER BY cursor ASC
        LIMIT ?
        "#,
    )
    .bind(Utc::now())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list unapplied sync events")?;
    rows.into_iter().map(event_from_row).collect()
}

/// Lists unapplied events of one semantic kind without letting earlier content hide later state.
pub async fn unapplied_events_by_kind(
    pool: &SqlitePool,
    kind: SyncEventKind,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT cursor, event_id, origin_device_id, origin_sequence,
               event_created_at_ms, nonce, ciphertext, event_kind
        FROM sync_events
        WHERE is_applied = 0
          AND apply_state = 'pending'
          AND event_kind = ?
          AND (next_apply_at IS NULL OR next_apply_at <= ?)
        ORDER BY cursor ASC
        LIMIT ?
        "#,
    )
    .bind(kind.as_i64())
    .bind(Utc::now())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list unapplied sync events by kind")?;
    rows.into_iter().map(event_from_row).collect()
}

pub async fn has_unapplied_events_by_kind(pool: &SqlitePool, kind: SyncEventKind) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM sync_events
            WHERE is_applied = 0
              AND apply_state = 'pending'
              AND event_kind = ?
              AND (next_apply_at IS NULL OR next_apply_at <= ?)
        )
        "#,
    )
    .bind(kind.as_i64())
    .bind(Utc::now())
    .fetch_one(pool)
    .await
    .context("check unapplied sync events by kind")?
        != 0)
}

pub async fn pending_events_for_target(
    pool: &SqlitePool,
    target_id: &str,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext, e.event_kind
        FROM sync_events e
        LEFT JOIN sync_deliveries d
          ON d.event_id = e.event_id AND d.target_id = ?
        WHERE d.event_id IS NULL
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list pending sync events")?;
    rows.into_iter().map(event_from_row).collect()
}

/// Lists only events originated by this device that still need delivery to the Hub.
/// Remote events already retain their original owner and must not be re-uploaded by every peer.
pub async fn pending_origin_events_for_target(
    pool: &SqlitePool,
    target_id: &str,
    origin_device_id: &str,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext, e.event_kind
        FROM sync_events e
        LEFT JOIN sync_deliveries d
          ON d.event_id = e.event_id AND d.target_id = ?
        WHERE d.event_id IS NULL AND e.origin_device_id = ?
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(origin_device_id)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list pending origin sync events")?;
    rows.into_iter().map(event_from_row).collect()
}

pub async fn events_after_cursor(
    pool: &SqlitePool,
    after_cursor: u64,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT cursor, event_id, origin_device_id, origin_sequence,
               event_created_at_ms, nonce, ciphertext, event_kind
        FROM sync_events
        WHERE cursor > ?
        ORDER BY cursor ASC
        LIMIT ?
        "#,
    )
    .bind(i64::try_from(after_cursor).unwrap_or(i64::MAX))
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list sync events after cursor")?;
    rows.into_iter().map(event_from_row).collect()
}

pub async fn blobs_for_events(pool: &SqlitePool, event_ids: &[String]) -> Result<Vec<StoredBlob>> {
    if event_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT DISTINCT blob_id, encrypted_path, size FROM sync_event_blobs WHERE event_id IN (",
    );
    let mut separated = query.separated(", ");
    for event_id in event_ids {
        separated.push_bind(event_id);
    }
    query.push(")");
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("list sync event blobs")?;
    rows.into_iter()
        .map(|row| {
            let size: i64 = row.try_get("size")?;
            Ok(StoredBlob {
                blob_id: row.try_get("blob_id")?,
                encrypted_path: row.try_get("encrypted_path")?,
                size: u64::try_from(size).context("negative encrypted blob size")?,
            })
        })
        .collect()
}

/// Adds transport-only blob references to a batch without duplicating them in the event table.
pub async fn attach_event_blob_ids(
    pool: &SqlitePool,
    events: &mut [StoredSyncEvent],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let event_indexes = events
        .iter()
        .enumerate()
        .map(|(index, event)| (event.event.event_id.clone(), index))
        .collect::<HashMap<_, _>>();
    let mut query = sqlx::QueryBuilder::new(
        "SELECT event_id, blob_id FROM sync_event_blobs WHERE event_id IN (",
    );
    let mut separated = query.separated(", ");
    for event in events.iter() {
        separated.push_bind(&event.event.event_id);
    }
    query.push(") ORDER BY event_id, blob_id");
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("list event blob references")?;
    for row in rows {
        let event_id: String = row.try_get("event_id")?;
        let blob_id: String = row.try_get("blob_id")?;
        if let Some(index) = event_indexes.get(&event_id) {
            events[*index].event.blob_ids.push(blob_id);
        }
    }
    Ok(())
}

/// Persists optional source icons until the Hub has acknowledged their storage.
pub async fn enqueue_source_icon_uploads(
    pool: &SqlitePool,
    group_id: &str,
    blobs: &[StoredBlob],
) -> Result<()> {
    if blobs.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    let mut transaction = pool
        .begin()
        .await
        .context("begin source icon upload transaction")?;
    for blob in blobs {
        sqlx::query(
            r#"
            INSERT INTO sync_source_icon_uploads (
                group_id, blob_id, encrypted_path, size, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, blob_id) DO UPDATE SET
                encrypted_path = excluded.encrypted_path,
                size = excluded.size,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(group_id)
        .bind(&blob.blob_id)
        .bind(&blob.encrypted_path)
        .bind(i64::try_from(blob.size).context("encrypted source icon is too large")?)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("queue source icon upload")?;
    }
    transaction
        .commit()
        .await
        .context("commit source icon uploads")?;
    Ok(())
}

/// Lists source icons whose storage has not yet been acknowledged by the Hub.
pub async fn pending_source_icon_uploads(
    pool: &SqlitePool,
    group_id: &str,
    limit: u16,
) -> Result<Vec<StoredBlob>> {
    let rows = sqlx::query(
        r#"
        SELECT blob_id, encrypted_path, size
        FROM sync_source_icon_uploads
        WHERE group_id = ?
        ORDER BY created_at ASC
        LIMIT ?
        "#,
    )
    .bind(group_id)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list pending source icon uploads")?;

    rows.into_iter()
        .map(|row| {
            let size: i64 = row.try_get("size")?;
            Ok(StoredBlob {
                blob_id: row.try_get("blob_id")?,
                encrypted_path: row.try_get("encrypted_path")?,
                size: u64::try_from(size).context("negative source icon size")?,
            })
        })
        .collect()
}

/// Removes source icon uploads only after the Hub confirms them.
pub async fn complete_source_icon_uploads(
    pool: &SqlitePool,
    group_id: &str,
    blob_ids: &[String],
) -> Result<()> {
    if blob_ids.is_empty() {
        return Ok(());
    }

    let mut query =
        sqlx::QueryBuilder::new("DELETE FROM sync_source_icon_uploads WHERE group_id = ");
    query.push_bind(group_id).push(" AND blob_id IN (");
    let mut separated = query.separated(", ");
    for blob_id in blob_ids {
        separated.push_bind(blob_id);
    }
    query.push(")");
    query
        .build()
        .execute(pool)
        .await
        .context("complete source icon uploads")?;
    Ok(())
}

pub async fn mark_delivered(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin sync delivery transaction")?;
    mark_delivered_on(&mut transaction, target_id, event_ids).await?;
    transaction
        .commit()
        .await
        .context("commit sync deliveries")?;
    Ok(())
}

async fn mark_delivered_on(
    connection: &mut SqliteConnection,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    let now = Utc::now();
    for event_id in event_ids {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO sync_deliveries (event_id, target_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(now)
        .bind(now)
        .execute(&mut *connection)
        .await
        .context("mark sync event delivered")?;
        sqlx::query(
            r#"
            INSERT INTO sync_delivery_states (
                event_id, target_id, state, last_error, last_attempt_at,
                last_success_at, created_at, updated_at
            ) VALUES (?, ?, 'success', NULL, ?, ?, ?, ?)
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                state = 'success',
                last_error = NULL,
                last_attempt_at = excluded.last_attempt_at,
                last_success_at = excluded.last_success_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *connection)
        .await
        .context("record successful sync delivery")?;
    }
    Ok(())
}

/// Stores a pulled batch, delivery acknowledgements, and its pull cursor in one transaction.
/// A crash therefore either replays the whole batch or observes every event on the next cycle.
pub async fn store_remote_batch(
    pool: &SqlitePool,
    source: RemoteBatchSource<'_>,
    latest_cursor: u64,
    events: &[EncryptedEvent],
    local_device_id: &str,
) -> Result<StoreRemoteBatchResult> {
    let mut transaction = pool.begin().await.context("begin remote sync batch")?;
    let target_id = source.delivery_target();
    let mut result = StoreRemoteBatchResult::default();
    let mut received_event_ids = Vec::new();
    let mut remote_event_ids = Vec::new();

    for event in events {
        let inserted = insert_event_on(
            &mut transaction,
            event,
            event.origin_device_id == local_device_id,
            &[],
            None,
        )
        .await?;
        if !inserted.stored {
            result
                .conflicts
                .push((event.event_id.clone(), event.origin_device_id.clone()));
            continue;
        }
        received_event_ids.push(event.event_id.clone());
        if event.origin_device_id != local_device_id {
            remote_event_ids.push(event.event_id.clone());
            result.received_new_events |= inserted.inserted;
        }
    }

    mark_delivered_on(&mut transaction, &target_id, &received_event_ids).await?;
    match source {
        RemoteBatchSource::Cloud => {
            set_state_value_on(
                &mut transaction,
                CLOUD_CURSOR_KEY,
                &latest_cursor.to_string(),
            )
            .await?;
        }
        RemoteBatchSource::Peer { device_id } => {
            mark_delivered_on(&mut transaction, CLOUD_TARGET, &remote_event_ids).await?;
            sqlx::query(
                "UPDATE sync_peers SET pull_cursor = ?, updated_at = ? WHERE device_id = ?",
            )
            .bind(i64::try_from(latest_cursor).unwrap_or(i64::MAX))
            .bind(Utc::now())
            .bind(device_id)
            .execute(&mut *transaction)
            .await
            .context("update peer cursor with remote sync batch")?;
        }
    }

    transaction
        .commit()
        .await
        .context("commit remote sync batch")?;
    Ok(result)
}

pub async fn mark_delivery_syncing(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    set_delivery_state(pool, target_id, event_ids, "syncing", None).await
}

pub async fn mark_delivery_error(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    error: &str,
) -> Result<()> {
    set_delivery_state(pool, target_id, event_ids, "error", Some(error)).await
}

async fn set_delivery_state(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    state: &str,
    error: Option<&str>,
) -> Result<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    let mut transaction = pool.begin().await.context("begin sync state transaction")?;
    for event_id in event_ids {
        sqlx::query(
            r#"
            INSERT INTO sync_delivery_states (
                event_id, target_id, state, last_error, last_attempt_at,
                last_success_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                state = excluded.state,
                last_error = excluded.last_error,
                last_attempt_at = excluded.last_attempt_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(state)
        .bind(error)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("record sync delivery state")?;
    }
    transaction
        .commit()
        .await
        .context("commit sync delivery states")?;
    Ok(())
}

pub async fn upsert_peer(pool: &SqlitePool, peer: &PeerAnnouncement) -> Result<()> {
    let mut connection = pool
        .acquire()
        .await
        .context("acquire sync peer connection")?;
    upsert_peer_on(&mut connection, peer).await
}

/// Updates cloud-visible peer identity without importing Hub-cached network routes.
pub async fn upsert_cloud_peer_identity(pool: &SqlitePool, peer: &PeerAnnouncement) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin cloud sync peer identity transaction")?;
    let previous_endpoint: Option<String> =
        sqlx::query_scalar("SELECT endpoint_id FROM sync_peers WHERE device_id = ?")
            .bind(&peer.device_id)
            .fetch_optional(&mut *transaction)
            .await
            .context("read existing cloud sync peer identity")?;
    let endpoint_changed = previous_endpoint
        .as_deref()
        .is_some_and(|endpoint_id| endpoint_id != peer.endpoint_id);
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_peers (
            device_id, device_name, platform, endpoint_id, direct_addresses,
            relay_urls, pull_cursor, last_seen_ms, created_at, updated_at
        )
        SELECT ?, ?, ?, ?, '[]', '[]', 0, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM sync_removed_peers
            WHERE (device_id = ? OR endpoint_id = ?)
              AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
        )
        ON CONFLICT(device_id) DO UPDATE SET
            device_name = excluded.device_name,
            platform = excluded.platform,
            endpoint_id = excluded.endpoint_id,
            direct_addresses = CASE
                WHEN sync_peers.endpoint_id = excluded.endpoint_id
                THEN sync_peers.direct_addresses
                ELSE excluded.direct_addresses
            END,
            relay_urls = CASE
                WHEN sync_peers.endpoint_id = excluded.endpoint_id
                THEN sync_peers.relay_urls
                ELSE excluded.relay_urls
            END,
            last_seen_ms = CASE
                WHEN sync_peers.endpoint_id = excluded.endpoint_id
                THEN sync_peers.last_seen_ms
                ELSE excluded.last_seen_ms
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&peer.device_id)
    .bind(&peer.device_name)
    .bind(&peer.platform)
    .bind(&peer.endpoint_id)
    .bind(peer.last_seen_ms)
    .bind(now)
    .bind(now)
    .bind(&peer.device_id)
    .bind(&peer.endpoint_id)
    .execute(&mut *transaction)
    .await
    .context("upsert cloud sync peer identity")?;

    if endpoint_changed {
        sqlx::query(
            r#"
            UPDATE sync_peer_connections
            SET state = 'offline', connected_address = NULL, transport = NULL,
                last_error = NULL, updated_at = ?
            WHERE device_id = ?
            "#,
        )
        .bind(now)
        .bind(&peer.device_id)
        .execute(&mut *transaction)
        .await
        .context("invalidate routes for changed cloud peer identity")?;
    }

    transaction
        .commit()
        .await
        .context("commit cloud sync peer identity")?;
    Ok(())
}

async fn upsert_peer_on(connection: &mut SqliteConnection, peer: &PeerAnnouncement) -> Result<()> {
    let now = Utc::now();
    let direct_addresses = lan_direct_addresses(&peer.direct_addresses);
    sqlx::query(
        r#"
        INSERT INTO sync_peers (
            device_id, device_name, platform, endpoint_id, direct_addresses,
            relay_urls, pull_cursor, last_seen_ms, created_at, updated_at
        )
        SELECT ?, ?, ?, ?, ?, ?, 0, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM sync_removed_peers
            WHERE (device_id = ? OR endpoint_id = ?)
              AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
        )
        ON CONFLICT(device_id) DO UPDATE SET
            device_name = excluded.device_name,
            platform = excluded.platform,
            endpoint_id = excluded.endpoint_id,
            direct_addresses = excluded.direct_addresses,
            relay_urls = excluded.relay_urls,
            last_seen_ms = excluded.last_seen_ms,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&peer.device_id)
    .bind(&peer.device_name)
    .bind(&peer.platform)
    .bind(&peer.endpoint_id)
    .bind(serde_json::to_string(&direct_addresses)?)
    .bind("[]")
    .bind(peer.last_seen_ms)
    .bind(now)
    .bind(now)
    .bind(&peer.device_id)
    .bind(&peer.endpoint_id)
    .execute(connection)
    .await
    .context("upsert sync peer")?;
    Ok(())
}

async fn restore_peer_on(
    connection: &mut SqliteConnection,
    device_id: &str,
    endpoint_id: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query("DELETE FROM sync_removed_peers WHERE endpoint_id = ? AND device_id <> ?")
        .bind(endpoint_id)
        .bind(device_id)
        .execute(&mut *connection)
        .await
        .context("remove stale endpoint membership")?;
    sqlx::query(
        r#"
        INSERT INTO sync_removed_peers (
            device_id, endpoint_id, removed_at_ms, restored_at_ms, created_at, updated_at
        ) VALUES (?, ?, 0, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            endpoint_id = excluded.endpoint_id,
            restored_at_ms = MAX(COALESCE(sync_removed_peers.restored_at_ms, 0), excluded.restored_at_ms),
            updated_at = excluded.updated_at
        "#,
    )
    .bind(device_id)
    .bind(endpoint_id)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(now)
    .execute(connection)
    .await
    .context("restore removed sync peer")?;
    Ok(())
}

/// Restores a removed member and stores its current route as one membership transaction.
pub async fn restore_and_upsert_peer(pool: &SqlitePool, peer: &PeerAnnouncement) -> Result<()> {
    let mut transaction = pool.begin().await.context("begin approving sync peer")?;
    restore_peer_on(&mut transaction, &peer.device_id, &peer.endpoint_id).await?;
    upsert_peer_on(&mut transaction, peer).await?;
    transaction
        .commit()
        .await
        .context("commit approved sync peer")?;
    Ok(())
}

pub async fn is_peer_removed(
    pool: &SqlitePool,
    device_id: &str,
    endpoint_id: &str,
) -> Result<bool> {
    let removed: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM sync_removed_peers
            WHERE (device_id = ? OR endpoint_id = ?)
              AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
        )
        "#,
    )
    .bind(device_id)
    .bind(endpoint_id)
    .fetch_one(pool)
    .await
    .context("check removed sync peer")?;
    Ok(removed != 0)
}

pub async fn removed_peer_ids(pool: &SqlitePool) -> Result<HashSet<String>> {
    let rows = sqlx::query_scalar(
        r#"
        SELECT device_id FROM sync_removed_peers
        WHERE restored_at_ms IS NULL OR removed_at_ms > restored_at_ms
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list removed sync peers")?;
    Ok(rows.into_iter().collect())
}

pub async fn removed_devices(pool: &SqlitePool) -> Result<Vec<RemovedDevice>> {
    let rows = sqlx::query(
        r#"
        SELECT device_id, endpoint_id, removed_at_ms, restored_at_ms
        FROM sync_removed_peers
        WHERE endpoint_id NOT LIKE 'legacy:%'
        ORDER BY removed_at_ms ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list removed sync devices")?;
    rows.into_iter()
        .map(|row| {
            Ok(RemovedDevice {
                device_id: row.try_get("device_id")?,
                endpoint_id: row.try_get("endpoint_id")?,
                removed_at_ms: row.try_get("removed_at_ms")?,
                restored_at_ms: row.try_get("restored_at_ms")?,
            })
        })
        .collect()
}

pub async fn latest_removed_at_ms(pool: &SqlitePool) -> Result<i64> {
    let value: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT MAX(MAX(removed_at_ms, COALESCE(restored_at_ms, 0)))
        FROM sync_removed_peers WHERE endpoint_id NOT LIKE 'legacy:%'
        "#,
    )
    .fetch_one(pool)
    .await
    .context("read latest removed device timestamp")?;
    Ok(value.unwrap_or(0))
}

/// 合并其他设备或 Hub 传来的删除标记，并清掉对应路由和投递状态。
pub async fn merge_removed_devices(pool: &SqlitePool, devices: &[RemovedDevice]) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin merging removed sync devices")?;
    for device in devices {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO sync_removed_peers (
                device_id, endpoint_id, removed_at_ms, restored_at_ms, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                endpoint_id = excluded.endpoint_id,
                removed_at_ms = MAX(sync_removed_peers.removed_at_ms, excluded.removed_at_ms),
                restored_at_ms = CASE
                    WHEN excluded.restored_at_ms IS NULL THEN sync_removed_peers.restored_at_ms
                    ELSE MAX(COALESCE(sync_removed_peers.restored_at_ms, 0), excluded.restored_at_ms)
                END,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&device.device_id)
        .bind(&device.endpoint_id)
        .bind(device.removed_at_ms)
        .bind(device.restored_at_ms)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("merge removed sync device")?;

        let (stored_removed_at_ms, stored_restored_at_ms): (i64, Option<i64>) = sqlx::query_as(
            "SELECT removed_at_ms, restored_at_ms FROM sync_removed_peers WHERE device_id = ?",
        )
        .bind(&device.device_id)
        .fetch_one(&mut *transaction)
        .await
        .context("read merged sync device membership")?;
        let effectively_removed = stored_restored_at_ms
            .is_none_or(|restored_at_ms| stored_removed_at_ms > restored_at_ms);
        if effectively_removed {
            let target_id = format!("peer:{}", device.device_id);
            for statement in [
                "DELETE FROM sync_delivery_states WHERE target_id = ?",
                "DELETE FROM sync_deliveries WHERE target_id = ?",
            ] {
                sqlx::query(statement)
                    .bind(&target_id)
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| format!("run {statement}"))?;
            }
            sqlx::query("DELETE FROM sync_peers WHERE device_id = ? OR endpoint_id = ?")
                .bind(&device.device_id)
                .bind(&device.endpoint_id)
                .execute(&mut *transaction)
                .await
                .context("purge removed sync peer route")?;
        }
    }
    transaction
        .commit()
        .await
        .context("commit removed sync devices")?;
    Ok(())
}

/// 忘记单个对端及其投递状态，并阻止发现与 Hub 设备列表将它自动加回。
pub async fn remove_peer(pool: &SqlitePool, device_id: &str) -> Result<Option<RemovedDevice>> {
    let mut transaction = pool.begin().await.context("begin removing sync peer")?;
    let endpoint_id: Option<String> =
        sqlx::query_scalar("SELECT endpoint_id FROM sync_peers WHERE device_id = ?")
            .bind(device_id)
            .fetch_optional(&mut *transaction)
            .await
            .context("find sync peer to remove")?;
    let Some(endpoint_id) = endpoint_id else {
        transaction
            .rollback()
            .await
            .context("rollback missing sync peer removal")?;
        return Ok(None);
    };

    let now = Utc::now();
    let removed = RemovedDevice {
        device_id: device_id.to_owned(),
        endpoint_id,
        removed_at_ms: now.timestamp_millis(),
        restored_at_ms: None,
    };
    sqlx::query(
        r#"
        INSERT INTO sync_removed_peers (
            device_id, endpoint_id, removed_at_ms, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            endpoint_id = excluded.endpoint_id,
            removed_at_ms = excluded.removed_at_ms,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&removed.device_id)
    .bind(&removed.endpoint_id)
    .bind(removed.removed_at_ms)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("remember removed sync peer")?;

    let target_id = format!("peer:{device_id}");
    for statement in [
        "DELETE FROM sync_delivery_states WHERE target_id = ?",
        "DELETE FROM sync_deliveries WHERE target_id = ?",
    ] {
        sqlx::query(statement)
            .bind(&target_id)
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("run {statement}"))?;
    }
    sqlx::query("DELETE FROM sync_peers WHERE device_id = ?")
        .bind(device_id)
        .execute(&mut *transaction)
        .await
        .context("delete sync peer")?;
    transaction
        .commit()
        .await
        .context("commit removing sync peer")?;
    Ok(Some(removed))
}

/// Refreshes the volatile routes of a paired endpoint discovered on the local network.
pub async fn update_peer_routes(
    pool: &SqlitePool,
    endpoint_id: &str,
    direct_addresses: &[String],
    refresh_presence: bool,
) -> Result<bool> {
    let now = Utc::now();
    let direct_addresses = serde_json::to_string(&lan_direct_addresses(direct_addresses))?;
    let result = sqlx::query(
        r#"
        UPDATE sync_peers
        SET direct_addresses = CASE WHEN ? = '[]' THEN direct_addresses ELSE ? END,
            relay_urls = '[]',
            last_seen_ms = ?, updated_at = ?
        WHERE endpoint_id = ?
          AND ((? != '[]' AND direct_addresses != ?)
            OR relay_urls != '[]')
        "#,
    )
    .bind(&direct_addresses)
    .bind(&direct_addresses)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(endpoint_id)
    .bind(&direct_addresses)
    .bind(&direct_addresses)
    .execute(pool)
    .await
    .context("refresh discovered peer routes")?;
    let routes_changed = result.rows_affected() > 0;
    if !routes_changed && refresh_presence {
        sqlx::query("UPDATE sync_peers SET last_seen_ms = ?, updated_at = ? WHERE endpoint_id = ?")
            .bind(now.timestamp_millis())
            .bind(now)
            .bind(endpoint_id)
            .execute(pool)
            .await
            .context("refresh discovered peer presence")?;
    }
    Ok(routes_changed)
}

/// Resolves a paired device from its stable Iroh endpoint identity.
pub async fn peer_device_id_by_endpoint(
    pool: &SqlitePool,
    endpoint_id: &str,
) -> Result<Option<String>> {
    sqlx::query_scalar("SELECT device_id FROM sync_peers WHERE endpoint_id = ?")
        .bind(endpoint_id)
        .fetch_optional(pool)
        .await
        .context("find peer by endpoint")
}

fn lan_direct_addresses(addresses: &[String]) -> Vec<String> {
    addresses
        .iter()
        .filter_map(|value| value.parse::<SocketAddr>().ok())
        .filter(|address| is_lan_ip(address.ip()))
        .map(|address| address.to_string())
        .collect()
}

pub async fn list_peers(pool: &SqlitePool) -> Result<Vec<SyncPeer>> {
    let rows = sqlx::query(
        r#"
        SELECT device_id, device_name, platform, endpoint_id, direct_addresses,
               relay_urls, pull_cursor, last_seen_ms
        FROM sync_peers
        ORDER BY last_seen_ms DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list sync peers")?;
    rows.into_iter()
        .map(|row| {
            let direct: String = row.try_get("direct_addresses")?;
            let relays: String = row.try_get("relay_urls")?;
            let cursor: i64 = row.try_get("pull_cursor")?;
            Ok(SyncPeer {
                announcement: PeerAnnouncement {
                    device_id: row.try_get("device_id")?,
                    device_name: row.try_get("device_name")?,
                    platform: row.try_get("platform")?,
                    endpoint_id: row.try_get("endpoint_id")?,
                    direct_addresses: serde_json::from_str(&direct)
                        .context("invalid peer direct addresses")?,
                    relay_urls: serde_json::from_str(&relays).context("invalid peer relay URLs")?,
                    last_seen_ms: row.try_get("last_seen_ms")?,
                },
                pull_cursor: u64::try_from(cursor).context("negative peer cursor")?,
            })
        })
        .collect()
}

pub async fn mark_peer_connecting(pool: &SqlitePool, device_id: &str) -> Result<()> {
    set_peer_connection(pool, device_id, "connecting", None, None, None, false).await
}

pub async fn mark_peer_offline(
    pool: &SqlitePool,
    device_id: &str,
    error: Option<&str>,
) -> Result<()> {
    set_peer_connection(pool, device_id, "offline", None, None, error, false).await
}

pub async fn mark_peer_error(pool: &SqlitePool, device_id: &str, error: &str) -> Result<()> {
    set_peer_connection(pool, device_id, "error", None, None, Some(error), false).await
}

pub async fn mark_peer_online(
    pool: &SqlitePool,
    device_id: &str,
    connected_address: Option<&str>,
    transport: Option<&str>,
) -> Result<()> {
    set_peer_connection(
        pool,
        device_id,
        "online",
        connected_address,
        transport,
        None,
        true,
    )
    .await
}

/// Returns the last direct route that completed a synchronization with this peer.
pub async fn preferred_peer_address(pool: &SqlitePool, device_id: &str) -> Result<Option<String>> {
    sqlx::query_scalar(
        r#"
        SELECT connected_address
        FROM sync_peer_connections
        WHERE device_id = ? AND transport = 'direct' AND connected_address IS NOT NULL
        "#,
    )
    .bind(device_id)
    .fetch_optional(pool)
    .await
    .context("load preferred sync peer address")
}

async fn set_peer_connection(
    pool: &SqlitePool,
    device_id: &str,
    state: &str,
    connected_address: Option<&str>,
    transport: Option<&str>,
    error: Option<&str>,
    succeeded: bool,
) -> Result<()> {
    let now = Utc::now();
    let success = succeeded.then_some(now);
    sqlx::query(
        r#"
        INSERT INTO sync_peer_connections (
            device_id, state, connected_address, transport, last_attempt_at,
            last_success_at, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            state = excluded.state,
            connected_address = COALESCE(excluded.connected_address, sync_peer_connections.connected_address),
            transport = COALESCE(excluded.transport, sync_peer_connections.transport),
            last_attempt_at = excluded.last_attempt_at,
            last_success_at = COALESCE(excluded.last_success_at, sync_peer_connections.last_success_at),
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(device_id)
    .bind(state)
    .bind(connected_address)
    .bind(transport)
    .bind(now)
    .bind(success)
    .bind(error)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("update sync peer connection")?;
    Ok(())
}

pub async fn list_peer_statuses(pool: &SqlitePool) -> Result<Vec<SyncPeerStatus>> {
    let rows = sqlx::query(
        r#"
        SELECT p.device_id, p.device_name, p.platform, p.endpoint_id,
               p.direct_addresses, p.relay_urls, p.last_seen_ms,
               c.state, c.connected_address, c.transport, c.last_attempt_at,
               c.last_success_at, c.last_error
        FROM sync_peers p
        LEFT JOIN sync_peer_connections c ON c.device_id = p.device_id
        ORDER BY p.last_seen_ms DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list sync peer statuses")?;
    rows.into_iter()
        .map(|row| {
            let direct: String = row.try_get("direct_addresses")?;
            let relays: String = row.try_get("relay_urls")?;
            let state: Option<String> = row.try_get("state")?;
            let last_seen_ms: i64 = row.try_get("last_seen_ms")?;
            Ok(SyncPeerStatus {
                device_id: row.try_get("device_id")?,
                device_name: row.try_get("device_name")?,
                platform: row.try_get("platform")?,
                endpoint_id: row.try_get("endpoint_id")?,
                direct_addresses: serde_json::from_str(&direct)
                    .context("invalid peer direct addresses")?,
                relay_urls: serde_json::from_str(&relays).context("invalid peer relay URLs")?,
                state: channel_state(state.as_deref()),
                connected_address: row.try_get("connected_address")?,
                transport: row.try_get("transport")?,
                last_seen_at: Utc
                    .timestamp_millis_opt(last_seen_ms)
                    .single()
                    .map(|value| value.to_rfc3339()),
                last_attempt_at: row.try_get("last_attempt_at")?,
                last_success_at: row.try_get("last_success_at")?,
                last_error: row.try_get("last_error")?,
            })
        })
        .collect()
}

fn channel_state(value: Option<&str>) -> SyncChannelState {
    match value {
        Some("connecting") => SyncChannelState::Connecting,
        Some("online") => SyncChannelState::Online,
        Some("error") => SyncChannelState::Error,
        _ => SyncChannelState::Idle,
    }
}

pub async fn cloud_cursor(pool: &SqlitePool) -> Result<u64> {
    state_value(pool, CLOUD_CURSOR_KEY)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()
        .context("invalid cloud sync cursor")
}

pub async fn cloud_bootstrap_completed(pool: &SqlitePool, group_id: &str) -> Result<bool> {
    let value = state_value(pool, CLOUD_BOOTSTRAP_GROUP_KEY).await?;
    Ok(value.as_deref() == Some(group_id) || value == Some(format!("complete:{group_id}")))
}

pub async fn cloud_bootstrap_pending(pool: &SqlitePool, group_id: &str) -> Result<bool> {
    Ok(state_value(pool, CLOUD_BOOTSTRAP_GROUP_KEY).await? == Some(format!("pending:{group_id}")))
}

pub async fn cloud_bootstrap_target(pool: &SqlitePool, group_id: &str) -> Result<Option<u64>> {
    if !cloud_bootstrap_pending(pool, group_id).await? {
        return Ok(None);
    }
    state_value(pool, CLOUD_BOOTSTRAP_TARGET_KEY)
        .await?
        .map(|value| {
            value
                .parse()
                .context("invalid cloud bootstrap target cursor")
        })
        .transpose()
}

pub async fn cloud_bootstrap_in_progress(pool: &SqlitePool) -> Result<bool> {
    Ok(state_value(pool, CLOUD_BOOTSTRAP_GROUP_KEY)
        .await?
        .is_some_and(|value| value.starts_with("pending:")))
}

/// Atomically persists the selected Hub floor and marks initial bootstrap in progress.
pub async fn start_cloud_bootstrap(
    pool: &SqlitePool,
    group_id: &str,
    start_cursor: u64,
    target_cursor: u64,
) -> Result<()> {
    let mut transaction = pool.begin().await.context("begin cloud bootstrap")?;
    let now = Utc::now();
    for (key, value) in [
        (CLOUD_CURSOR_KEY, start_cursor.to_string()),
        (CLOUD_BOOTSTRAP_GROUP_KEY, format!("pending:{group_id}")),
        (CLOUD_BOOTSTRAP_TARGET_KEY, target_cursor.to_string()),
    ] {
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
        .execute(&mut *transaction)
        .await
        .context("persist cloud bootstrap state")?;
    }
    transaction
        .commit()
        .await
        .context("commit cloud bootstrap")?;
    Ok(())
}

pub async fn mark_cloud_bootstrap_completed(pool: &SqlitePool, group_id: &str) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin completing cloud bootstrap")?;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        "#,
    )
    .bind(CLOUD_BOOTSTRAP_GROUP_KEY)
    .bind(format!("complete:{group_id}"))
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("mark cloud bootstrap complete")?;
    sqlx::query("DELETE FROM sync_state WHERE key = ?")
        .bind(CLOUD_BOOTSTRAP_TARGET_KEY)
        .execute(&mut *transaction)
        .await
        .context("clear cloud bootstrap target")?;
    transaction
        .commit()
        .await
        .context("commit completed cloud bootstrap")?;
    Ok(())
}

/// Drops applied content events whose local clipboard record is gone. Item-state events stay
/// available because they carry metadata and deletion ordering independently from content.
pub async fn prune_released_content_events(pool: &SqlitePool, limit: u32) -> Result<u64> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin local sync event cleanup")?;
    let event_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT e.event_id
        FROM sync_events e
        WHERE e.event_kind = ?
          AND e.is_applied = 1
          AND NOT EXISTS (
              SELECT 1 FROM sync_item_events i WHERE i.event_id = e.event_id
          )
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(SyncEventKind::Content.as_i64())
    .bind(i64::from(limit.max(1)))
    .fetch_all(&mut *transaction)
    .await
    .context("list released local sync events")?;
    if event_ids.is_empty() {
        transaction
            .commit()
            .await
            .context("commit empty local sync event cleanup")?;
        return Ok(0);
    }

    let mut query = sqlx::QueryBuilder::new("DELETE FROM sync_events WHERE event_id IN (");
    let mut separated = query.separated(", ");
    for event_id in &event_ids {
        separated.push_bind(event_id);
    }
    query.push(")");
    query
        .build()
        .execute(&mut *transaction)
        .await
        .context("delete released local sync events")?;
    transaction
        .commit()
        .await
        .context("commit local sync event cleanup")?;
    Ok(event_ids.len() as u64)
}

pub async fn live_sync_blob_ids(pool: &SqlitePool) -> Result<HashSet<String>> {
    let ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT blob_id FROM sync_event_blobs
        UNION
        SELECT blob_id FROM sync_source_icon_uploads
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list live local sync blobs")?;
    Ok(ids.into_iter().collect())
}

/// Returns whether this sync space has completed its one-time local history backfill.
pub async fn history_backfill_completed(pool: &SqlitePool, group_id: &str) -> Result<bool> {
    let expected = format!("{HISTORY_BACKFILL_VERSION}:{group_id}");
    Ok(state_value(pool, HISTORY_BACKFILL_GROUP_KEY)
        .await?
        .as_deref()
        == Some(expected.as_str()))
}

/// Marks the one-time local history backfill complete for the current sync space.
pub async fn mark_history_backfill_completed(pool: &SqlitePool, group_id: &str) -> Result<()> {
    set_state_value(
        pool,
        HISTORY_BACKFILL_GROUP_KEY,
        &format!("{HISTORY_BACKFILL_VERSION}:{group_id}"),
    )
    .await
}

pub async fn history_timestamp_repair_completed(pool: &SqlitePool, group_id: &str) -> Result<bool> {
    Ok(state_value(pool, HISTORY_TIMESTAMP_REPAIR_GROUP_KEY)
        .await?
        .as_deref()
        == Some(group_id))
}

pub async fn mark_history_timestamp_repair_completed(
    pool: &SqlitePool,
    group_id: &str,
) -> Result<()> {
    set_state_value(pool, HISTORY_TIMESTAMP_REPAIR_GROUP_KEY, group_id).await
}

pub async fn mark_success(pool: &SqlitePool) -> Result<()> {
    set_state_value(pool, LAST_SUCCESS_KEY, &Utc::now().to_rfc3339()).await
}

pub async fn last_success(pool: &SqlitePool) -> Result<Option<String>> {
    state_value(pool, LAST_SUCCESS_KEY).await
}

pub async fn mark_pending_item(pool: &SqlitePool, item_id: &str, reason: &str) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_pending_items (item_id, reason, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET reason = excluded.reason, updated_at = excluded.updated_at
        "#,
    )
    .bind(item_id)
    .bind(reason)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("mark manual sync item")?;
    Ok(())
}

pub async fn clear_pending_item(pool: &SqlitePool, item_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM sync_pending_items WHERE item_id = ?")
        .bind(item_id)
        .execute(pool)
        .await
        .context("clear manual sync item")?;
    Ok(())
}

pub async fn status_counts(pool: &SqlitePool) -> Result<(u64, u64, u64)> {
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sync_events e WHERE NOT EXISTS (SELECT 1 FROM sync_deliveries d WHERE d.event_id = e.event_id)",
    )
    .fetch_one(pool)
    .await?;
    let manual: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_pending_items")
        .fetch_one(pool)
        .await?;
    let peers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_peers")
        .fetch_one(pool)
        .await?;
    Ok((
        pending.max(0) as u64,
        manual.max(0) as u64,
        peers.max(0) as u64,
    ))
}

pub async fn item_statuses(pool: &SqlitePool, item_ids: &[String]) -> Result<Vec<SyncItemStatus>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    let peer_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_peers")
        .fetch_one(pool)
        .await?;
    let mut statuses = item_ids
        .iter()
        .map(|id| {
            let mut status = SyncItemStatus::idle(id.clone());
            status.lan.total_targets = peer_count.max(0) as u64;
            status.cloud.total_targets = 1;
            (id.clone(), status)
        })
        .collect::<HashMap<_, _>>();

    let mut manual_query =
        sqlx::QueryBuilder::new("SELECT item_id FROM sync_pending_items WHERE item_id IN (");
    let mut manual_ids = manual_query.separated(", ");
    for item_id in item_ids {
        manual_ids.push_bind(item_id);
    }
    manual_query.push(")");
    for row in manual_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        if let Some(status) = statuses.get_mut(&item_id) {
            status.lan.state = SyncItemState::Manual;
            status.cloud.state = SyncItemState::Manual;
        }
    }

    let mut event_query = sqlx::QueryBuilder::new(
        r#"
        WITH latest AS (
            SELECT item_id, event_id,
                   ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY created_at DESC) AS rank
            FROM sync_item_events
            WHERE item_id IN (
        "#,
    );
    let mut event_ids = event_query.separated(", ");
    for item_id in item_ids {
        event_ids.push_bind(item_id);
    }
    event_query.push(
        r#")
        )
        SELECT latest.item_id, targets.target_id, targets.state, targets.last_error
        FROM latest
        JOIN (
            SELECT d.event_id, d.target_id, 'success' AS state, NULL AS last_error
            FROM sync_deliveries d
            UNION ALL
            SELECT s.event_id, s.target_id, s.state, s.last_error
            FROM sync_delivery_states s
            WHERE NOT EXISTS (
                SELECT 1 FROM sync_deliveries d
                WHERE d.event_id = s.event_id AND d.target_id = s.target_id
            )
        ) targets ON targets.event_id = latest.event_id
        WHERE latest.rank = 1
        "#,
    );
    for row in event_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        let target_id: String = row.try_get("target_id")?;
        let state: String = row.try_get("state")?;
        let last_error: Option<String> = row.try_get("last_error")?;
        let Some(status) = statuses.get_mut(&item_id) else {
            continue;
        };
        let channel = if target_id == "cloud" {
            &mut status.cloud
        } else if target_id.starts_with("peer:") {
            &mut status.lan
        } else {
            continue;
        };
        match state.as_str() {
            "success" => {
                channel.state = SyncItemState::Success;
                channel.delivered_targets = channel.delivered_targets.saturating_add(1);
                channel.last_error = None;
            }
            "syncing" if channel.state != SyncItemState::Success => {
                channel.state = SyncItemState::Syncing;
            }
            "error" if channel.state != SyncItemState::Success => {
                channel.state = SyncItemState::Error;
                channel.last_error = last_error;
            }
            _ => {}
        }
    }
    Ok(item_ids
        .iter()
        .filter_map(|id| statuses.remove(id))
        .collect())
}

pub async fn clear_group_state(pool: &SqlitePool) -> Result<()> {
    let mut transaction = pool.begin().await.context("begin clearing sync state")?;
    for statement in [
        "DELETE FROM source_app_sync_aliases",
        "DELETE FROM sync_source_icon_uploads",
        "DELETE FROM sync_peer_connections",
        "DELETE FROM sync_delivery_states",
        "DELETE FROM sync_item_events",
        "DELETE FROM sync_deliveries",
        "DELETE FROM sync_event_blobs",
        "DELETE FROM sync_events",
        "DELETE FROM sync_content_outbox",
        "DELETE FROM sync_item_states",
        "DELETE FROM sync_peers",
        "DELETE FROM sync_removed_peers",
        "DELETE FROM sync_state WHERE key <> 'origin_sequence'",
        "DELETE FROM sync_pending_items",
    ] {
        sqlx::query(statement)
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("run {statement}"))?;
    }
    transaction
        .commit()
        .await
        .context("commit clearing sync state")?;
    Ok(())
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
    async fn source_icon_uploads_remain_queued_until_confirmed() {
        let pool = memory_pool().await;
        let initial = StoredBlob {
            blob_id: "icon-blob".into(),
            encrypted_path: "old.bin".into(),
            size: 12,
        };
        let refreshed = StoredBlob {
            blob_id: initial.blob_id.clone(),
            encrypted_path: "new.bin".into(),
            size: 24,
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
            false,
            false,
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
            false,
            false,
        )
        .await
        .unwrap();

        commit_content_outbox_event(
            &pool,
            operation_order,
            "item",
            &test_event("event-1", 1),
            &[],
            &sync_key,
            false,
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
            17,
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
        assert_eq!(delivery_count, 2);
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
        }
    }
}
