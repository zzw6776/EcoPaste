use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use sqlx::{Row, SqliteConnection, SqlitePool};

use crate::core::Result;
use crate::db::models::{ClipboardItem, ClipboardKind};

const MAX_PREP_RETRY_SECONDS: i64 = 300;
const LOCAL_SEQ_KEY: &str = "local_seq";
const LAMPORT_SEQ_KEY: &str = "origin_sequence";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentDeliveryRoute {
    Lan,
    Cloud,
}

impl ContentDeliveryRoute {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lan => "lan",
            Self::Cloud => "cloud",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "lan" => Some(Self::Lan),
            "cloud" => Some(Self::Cloud),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentSyncOptions {
    pub history_backfill: bool,
    pub manual_share: bool,
    pub force_files: bool,
    pub delivery_route: Option<ContentDeliveryRoute>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxContentLane {
    Text,
    Image,
    Files,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingContentPrep {
    pub sync_key: String,
    pub item_id: String,
    pub source_revision: String,
    pub kind: ClipboardKind,
    pub local_seq: i64,
    pub history_backfill: bool,
    pub manual_share: bool,
    pub force_files: bool,
    pub delivery_route: Option<ContentDeliveryRoute>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingStatePrep {
    pub sync_key: String,
    pub item_id: Option<String>,
    pub is_sensitive: bool,
    pub deleted: bool,
    pub is_present: bool,
    pub is_favorite: bool,
    pub is_pinned: bool,
    pub note: Option<String>,
    pub local_seq: i64,
}

/// Allocates the next strictly monotonic local sync sequence inside an open transaction.
pub async fn next_local_seq_on(connection: &mut SqliteConnection) -> Result<i64> {
    allocate_counter_on(connection, LOCAL_SEQ_KEY).await
}

/// Allocates the next Lamport clock value used for per-field CRDT versions.
pub async fn next_lamport_seq_on(connection: &mut SqliteConnection) -> Result<u64> {
    let value = allocate_counter_on(connection, LAMPORT_SEQ_KEY).await?;
    Ok(u64::try_from(value).unwrap_or(1))
}

/// Advances the local Lamport clock past a remotely observed field version.
pub async fn observe_lamport_seq_on(
    connection: &mut SqliteConnection,
    observed: u64,
) -> Result<()> {
    if observed == 0 {
        return Ok(());
    }
    let current = read_counter_on(connection, LAMPORT_SEQ_KEY).await?;
    let observed_i64 = i64::try_from(observed).unwrap_or(i64::MAX);
    if observed_i64 <= current {
        return Ok(());
    }
    write_counter_on(connection, LAMPORT_SEQ_KEY, observed_i64).await
}

async fn allocate_counter_on(connection: &mut SqliteConnection, key: &str) -> Result<i64> {
    let next = read_counter_on(connection, key).await?.saturating_add(1);
    write_counter_on(connection, key, next).await?;
    Ok(next)
}

async fn read_counter_on(connection: &mut SqliteConnection, key: &str) -> Result<i64> {
    let current: Option<String> = sqlx::query_scalar("SELECT value FROM sync_state WHERE key = ?")
        .bind(key)
        .fetch_optional(&mut *connection)
        .await
        .with_context(|| format!("read sync_state counter {key}"))?;
    Ok(current
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0))
}

async fn write_counter_on(connection: &mut SqliteConnection, key: &str, value: i64) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(key)
    .bind(value.to_string())
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .with_context(|| format!("write sync_state counter {key}"))?;
    Ok(())
}

/// Stages a locally captured or reused clipboard item into `sync_items` atomically.
pub async fn stage_content_on(
    connection: &mut SqliteConnection,
    item: &ClipboardItem,
    item_id: &str,
    sync_key: &str,
    source_revision: &str,
    options: ContentSyncOptions,
) -> Result<i64> {
    let local_seq = next_local_seq_on(connection).await?;
    let lamport = next_lamport_seq_on(connection).await? as i64;
    let now = Utc::now();
    let kind = match item.kind {
        ClipboardKind::Text => "text",
        ClipboardKind::Image => "image",
        ClipboardKind::Files => "files",
    };
    let protected = item.is_favorite || item.is_pinned;
    let payload_updated_at_ms = item.updated_at.timestamp_millis();
    let route = options.delivery_route.map(ContentDeliveryRoute::as_str);

    sqlx::query(
        r#"
        INSERT INTO sync_items (
            sync_key, item_id, origin, source_revision, kind, is_sensitive,
            deleted, protected,
            content_seq, content_device_id,
            presence_seq, presence_device_id,
            favorite_seq, favorite_device_id,
            pinned_seq, pinned_device_id,
            note_seq, note_device_id,
            is_present, is_favorite, is_pinned, note,
            local_seq, payload_updated_at_ms,
            prep_state, prep_error, prep_attempts, next_prep_at,
            history_backfill, manual_share, delivery_route,
            local_state, remote_file_state, needs_apply,
            created_at, updated_at
        ) VALUES (
            ?, ?, 'local', ?, ?, ?,
            0, ?,
            ?, '',
            ?, '',
            CASE WHEN ? <> 0 THEN ? ELSE 0 END, '',
            CASE WHEN ? <> 0 THEN ? ELSE 0 END, '',
            CASE WHEN ? IS NOT NULL THEN ? ELSE 0 END, '',
            1, ?, ?, ?,
            ?, ?,
            'pending_content', NULL, 0, NULL,
            ?, ?, ?,
            'managed', NULL, 0,
            ?, ?
        )
        ON CONFLICT(sync_key) DO UPDATE SET
            item_id = excluded.item_id,
            origin = 'local',
            source_revision = excluded.source_revision,
            kind = excluded.kind,
            is_sensitive = excluded.is_sensitive,
            deleted = 0,
            is_present = 1,
            protected = MAX(sync_items.is_favorite, excluded.is_favorite)
                     OR MAX(sync_items.is_pinned, excluded.is_pinned),
            content_seq = excluded.content_seq,
            content_device_id = '',
            presence_seq = CASE
                WHEN sync_items.deleted <> 0 OR sync_items.is_present = 0 OR sync_items.presence_seq = 0
                THEN excluded.presence_seq
                ELSE sync_items.presence_seq
            END,
            presence_device_id = CASE
                WHEN sync_items.deleted <> 0 OR sync_items.is_present = 0 OR sync_items.presence_seq = 0
                THEN ''
                ELSE sync_items.presence_device_id
            END,
            local_seq = excluded.local_seq,
            payload_updated_at_ms = MAX(sync_items.payload_updated_at_ms, excluded.payload_updated_at_ms),
            prep_state = 'pending_content',
            prep_error = NULL,
            prep_attempts = 0,
            next_prep_at = NULL,
            history_backfill = MIN(sync_items.history_backfill, excluded.history_backfill),
            manual_share = MAX(sync_items.manual_share, excluded.manual_share),
            delivery_route = COALESCE(excluded.delivery_route, sync_items.delivery_route),
            local_state = 'managed',
            remote_file_state = NULL,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(item_id)
    .bind(source_revision)
    .bind(kind)
    .bind(item.is_sensitive)
    .bind(protected)
    .bind(lamport)
    .bind(lamport)
    .bind(i64::from(item.is_favorite))
    .bind(lamport)
    .bind(i64::from(item.is_pinned))
    .bind(lamport)
    .bind(item.note.as_deref())
    .bind(lamport)
    .bind(item.is_favorite)
    .bind(item.is_pinned)
    .bind(item.note.as_deref())
    .bind(local_seq)
    .bind(payload_updated_at_ms)
    .bind(options.history_backfill)
    .bind(options.manual_share || options.force_files)
    .bind(route)
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .context("stage clipboard content in sync_items")?;

    Ok(local_seq)
}

/// Stages an already persisted local item for synchronization (used by Android file share & backfill).
pub async fn stage_existing_content(
    pool: &SqlitePool,
    item: &ClipboardItem,
    options: ContentSyncOptions,
) -> Result<i64> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin staging existing sync item")?;
    let local_seq = stage_content_on(
        &mut transaction,
        item,
        &item.id,
        &item.sync_key,
        &item.source_revision,
        options,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("commit staging existing sync item")?;
    Ok(local_seq)
}

/// Cancels a staged content preparation matching `local_seq` without disturbing newer captures.
pub async fn cancel_content_prep(pool: &SqlitePool, local_seq: i64) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        UPDATE sync_items
        SET prep_state = 'ready',
            manual_share = 0,
            prep_error = NULL,
            next_prep_at = NULL,
            updated_at = ?
        WHERE local_seq = ? AND prep_state = 'pending_content'
        "#,
    )
    .bind(now)
    .bind(local_seq)
    .execute(pool)
    .await
    .context("cancel pending sync item preparation")?;
    Ok(())
}

/// Records a local favorite toggle atomically alongside `clipboard_items`.
pub async fn mark_local_favorite_on(
    connection: &mut SqliteConnection,
    sync_key: &str,
    item_id: &str,
    is_sensitive: bool,
    is_favorite: bool,
    is_pinned: bool,
) -> Result<()> {
    let local_seq = next_local_seq_on(connection).await?;
    let lamport = next_lamport_seq_on(connection).await? as i64;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_items (
            sync_key, item_id, origin, is_sensitive, deleted, protected,
            favorite_seq, favorite_device_id, is_favorite, is_pinned,
            local_seq, payload_updated_at_ms, prep_state, created_at, updated_at
        ) VALUES (?, ?, 'local', ?, 0, ?, ?, '', ?, ?, ?, ?, 'pending_state', ?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            item_id = COALESCE(sync_items.item_id, excluded.item_id),
            is_sensitive = excluded.is_sensitive,
            is_favorite = excluded.is_favorite,
            protected = CASE WHEN sync_items.deleted = 0 THEN (excluded.is_favorite OR sync_items.is_pinned) ELSE 0 END,
            favorite_seq = excluded.favorite_seq,
            favorite_device_id = '',
            local_seq = excluded.local_seq,
            prep_state = CASE
                WHEN sync_items.prep_state = 'pending_content' THEN 'pending_content'
                ELSE 'pending_state'
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(item_id)
    .bind(is_sensitive)
    .bind(is_favorite || is_pinned)
    .bind(lamport)
    .bind(is_favorite)
    .bind(is_pinned)
    .bind(local_seq)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .context("stage local favorite mutation")?;
    Ok(())
}

/// Records a local pin toggle atomically alongside `clipboard_items`.
pub async fn mark_local_pinned_on(
    connection: &mut SqliteConnection,
    sync_key: &str,
    item_id: &str,
    is_sensitive: bool,
    is_favorite: bool,
    is_pinned: bool,
) -> Result<()> {
    let local_seq = next_local_seq_on(connection).await?;
    let lamport = next_lamport_seq_on(connection).await? as i64;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_items (
            sync_key, item_id, origin, is_sensitive, deleted, protected,
            pinned_seq, pinned_device_id, is_favorite, is_pinned,
            local_seq, payload_updated_at_ms, prep_state, created_at, updated_at
        ) VALUES (?, ?, 'local', ?, 0, ?, ?, '', ?, ?, ?, ?, 'pending_state', ?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            item_id = COALESCE(sync_items.item_id, excluded.item_id),
            is_sensitive = excluded.is_sensitive,
            is_pinned = excluded.is_pinned,
            protected = CASE WHEN sync_items.deleted = 0 THEN (sync_items.is_favorite OR excluded.is_pinned) ELSE 0 END,
            pinned_seq = excluded.pinned_seq,
            pinned_device_id = '',
            local_seq = excluded.local_seq,
            prep_state = CASE
                WHEN sync_items.prep_state = 'pending_content' THEN 'pending_content'
                ELSE 'pending_state'
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(item_id)
    .bind(is_sensitive)
    .bind(is_favorite || is_pinned)
    .bind(lamport)
    .bind(is_favorite)
    .bind(is_pinned)
    .bind(local_seq)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .context("stage local pin mutation")?;
    Ok(())
}

/// Records a local note update atomically alongside `clipboard_items`.
pub async fn mark_local_note_on(
    connection: &mut SqliteConnection,
    sync_key: &str,
    item_id: &str,
    is_sensitive: bool,
    note: Option<&str>,
) -> Result<()> {
    let local_seq = next_local_seq_on(connection).await?;
    let lamport = next_lamport_seq_on(connection).await? as i64;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_items (
            sync_key, item_id, origin, is_sensitive, deleted,
            note_seq, note_device_id, note,
            local_seq, payload_updated_at_ms, prep_state, created_at, updated_at
        ) VALUES (?, ?, 'local', ?, 0, ?, '', ?, ?, ?, 'pending_state', ?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            item_id = COALESCE(sync_items.item_id, excluded.item_id),
            is_sensitive = excluded.is_sensitive,
            note = excluded.note,
            note_seq = excluded.note_seq,
            note_device_id = '',
            local_seq = excluded.local_seq,
            prep_state = CASE
                WHEN sync_items.prep_state = 'pending_content' THEN 'pending_content'
                ELSE 'pending_state'
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(item_id)
    .bind(is_sensitive)
    .bind(lamport)
    .bind(note)
    .bind(local_seq)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .context("stage local note mutation")?;
    Ok(())
}

/// Records a local deletion tombstone atomically alongside `clipboard_items` deletion.
pub async fn mark_local_deleted_on(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
) -> Result<()> {
    let local_seq = next_local_seq_on(connection).await?;
    let lamport = next_lamport_seq_on(connection).await? as i64;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_items (
            sync_key, item_id, origin, is_sensitive, deleted, protected,
            presence_seq, presence_device_id, is_present,
            local_seq, payload_updated_at_ms, prep_state,
            manual_share, remote_file_state,
            created_at, updated_at
        ) VALUES (?, NULL, 'local', ?, 1, 0, ?, '', 0, ?, ?, 'pending_state', 0, NULL, ?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            item_id = NULL,
            is_sensitive = excluded.is_sensitive,
            deleted = 1,
            protected = 0,
            is_present = 0,
            presence_seq = excluded.presence_seq,
            presence_device_id = '',
            local_seq = excluded.local_seq,
            payload_updated_at_ms = excluded.payload_updated_at_ms,
            prep_state = 'pending_state',
            prep_error = NULL,
            prep_attempts = 0,
            next_prep_at = NULL,
            history_backfill = 0,
            manual_share = 0,
            remote_file_state = NULL,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(is_sensitive)
    .bind(lamport)
    .bind(local_seq)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(now)
    .execute(&mut *connection)
    .await
    .context("stage local deletion tombstone")?;
    sqlx::query("DELETE FROM sync_item_blobs WHERE sync_key = ?")
        .bind(sync_key)
        .execute(&mut *connection)
        .await
        .context("clear blobs for deleted sync item")?;
    Ok(())
}

/// Returns the highest-priority pending content item ready for encryption/blob preparation.
///
/// Always prioritizes realtime/manual items (`history_backfill = 0`) and newest timestamps first
/// so historical catch-up never blocks freshly copied text, images, or files.
pub async fn next_pending_content(
    pool: &SqlitePool,
    include_sensitive: bool,
    lane: Option<OutboxContentLane>,
) -> Result<Option<PendingContentPrep>> {
    let now = Utc::now();
    let lane_kind = lane.map(|lane| match lane {
        OutboxContentLane::Text => "text",
        OutboxContentLane::Image => "image",
        OutboxContentLane::Files => "files",
    });
    let row = sqlx::query(
        r#"
        SELECT s.sync_key, s.item_id, s.source_revision, s.kind, s.local_seq,
               s.history_backfill, s.manual_share, s.delivery_route, s.prep_error
        FROM sync_items s
        JOIN clipboard_items c ON c.id = s.item_id
        WHERE s.prep_state = 'pending_content'
          AND s.deleted = 0
          AND (s.next_prep_at IS NULL OR s.next_prep_at <= ?)
          AND (? OR s.is_sensitive = 0 OR s.manual_share = 1)
          AND (? IS NULL OR s.kind = ?)
          AND (c.kind <> 'files' OR c.size IS NOT NULL)
        ORDER BY s.manual_share DESC,
                 s.history_backfill ASC,
                 s.payload_updated_at_ms DESC,
                 s.local_seq DESC
        LIMIT 1
        "#,
    )
    .bind(now)
    .bind(include_sensitive)
    .bind(lane_kind)
    .bind(lane_kind)
    .fetch_optional(pool)
    .await
    .context("query next pending content sync item")?;

    let Some(row) = row else {
        return Ok(None);
    };
    let parsed: anyhow::Result<PendingContentPrep> = (|| {
        let kind = match row.try_get::<String, _>("kind")?.as_str() {
            "image" => ClipboardKind::Image,
            "files" => ClipboardKind::Files,
            _ => ClipboardKind::Text,
        };
        let manual_share = row.try_get::<bool, _>("manual_share")?;
        let route_raw: Option<String> = row.try_get("delivery_route")?;
        Ok(PendingContentPrep {
            sync_key: row.try_get("sync_key")?,
            item_id: row.try_get("item_id")?,
            source_revision: row
                .try_get::<Option<String>, _>("source_revision")?
                .unwrap_or_default(),
            kind,
            local_seq: row.try_get("local_seq")?,
            history_backfill: row.try_get("history_backfill")?,
            manual_share,
            force_files: manual_share,
            delivery_route: route_raw.as_deref().and_then(ContentDeliveryRoute::parse),
            last_error: row.try_get("prep_error")?,
        })
    })();
    Ok(Some(parsed?))
}

/// Returns the earliest retry timestamp among deferred content items in `lane`.
pub async fn next_content_retry_at(
    pool: &SqlitePool,
    include_sensitive: bool,
    lane: OutboxContentLane,
) -> Result<Option<DateTime<Utc>>> {
    let lane_kind = match lane {
        OutboxContentLane::Text => "text",
        OutboxContentLane::Image => "image",
        OutboxContentLane::Files => "files",
    };
    let value: Option<DateTime<Utc>> = sqlx::query_scalar(
        r#"
        SELECT MIN(s.next_prep_at)
        FROM sync_items s
        JOIN clipboard_items c ON c.id = s.item_id
        WHERE s.prep_state = 'pending_content'
          AND s.deleted = 0
          AND s.next_prep_at IS NOT NULL
          AND (? OR s.is_sensitive = 0 OR s.manual_share = 1)
          AND s.kind = ?
          AND (c.kind <> 'files' OR c.size IS NOT NULL)
        "#,
    )
    .bind(include_sensitive)
    .bind(lane_kind)
    .fetch_one(pool)
    .await
    .context("query next content prep retry time")?;
    Ok(value)
}

/// Defers a failed content preparation with bounded exponential backoff.
pub async fn defer_content_prep(pool: &SqlitePool, sync_key: &str, error: &str) -> Result<()> {
    let attempts: i64 =
        sqlx::query_scalar("SELECT prep_attempts FROM sync_items WHERE sync_key = ?")
            .bind(sync_key)
            .fetch_optional(pool)
            .await
            .context("read sync item prep attempts")?
            .unwrap_or(0);
    let next_attempts = attempts.saturating_add(1);
    let delay_secs = (2_i64.saturating_pow(u32::try_from(next_attempts.min(8)).unwrap_or(8)))
        .min(MAX_PREP_RETRY_SECONDS);
    let now = Utc::now();
    let next_at = now + Duration::seconds(delay_secs);
    sqlx::query(
        r#"
        UPDATE sync_items
        SET prep_attempts = ?, prep_error = ?, next_prep_at = ?, updated_at = ?
        WHERE sync_key = ? AND prep_state = 'pending_content'
        "#,
    )
    .bind(next_attempts)
    .bind(error)
    .bind(next_at)
    .bind(now)
    .bind(sync_key)
    .execute(pool)
    .await
    .context("defer sync item content preparation")?;
    Ok(())
}

/// Returns the next item with pending metadata/tombstone state awaiting snapshot re-encryption.
pub async fn next_pending_state(
    pool: &SqlitePool,
    include_sensitive: bool,
) -> Result<Option<PendingStatePrep>> {
    let row = sqlx::query(
        r#"
        SELECT sync_key, item_id, is_sensitive, deleted, is_present,
               is_favorite, is_pinned, note, local_seq
        FROM sync_items
        WHERE prep_state = 'pending_state'
          AND (? OR is_sensitive = 0)
        ORDER BY payload_updated_at_ms DESC, local_seq DESC
        LIMIT 1
        "#,
    )
    .bind(include_sensitive)
    .fetch_optional(pool)
    .await
    .context("query next pending sync state item")?;

    let Some(row) = row else {
        return Ok(None);
    };
    let parsed: anyhow::Result<PendingStatePrep> = (|| {
        Ok(PendingStatePrep {
            sync_key: row.try_get("sync_key")?,
            item_id: row.try_get("item_id")?,
            is_sensitive: row.try_get("is_sensitive")?,
            deleted: row.try_get("deleted")?,
            is_present: row.try_get("is_present")?,
            is_favorite: row.try_get("is_favorite")?,
            is_pinned: row.try_get("is_pinned")?,
            note: row.try_get("note")?,
            local_seq: row.try_get("local_seq")?,
        })
    })();
    Ok(Some(parsed?))
}

/// Resets `next_prep_at` so deferred preparations retry immediately on wake or manual sync.
pub async fn reset_prep_backoff(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "UPDATE sync_items SET next_prep_at = NULL WHERE prep_state <> 'ready' AND next_prep_at IS NOT NULL",
    )
    .execute(pool)
    .await
    .context("reset sync item prep backoff")?;
    Ok(())
}
