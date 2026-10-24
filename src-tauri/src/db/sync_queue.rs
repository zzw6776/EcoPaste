use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use sqlx::{FromRow, SqliteConnection, SqlitePool};

const LOCAL_OPERATION_SEQUENCE_KEY: &str = "local_operation_sequence";

#[derive(Debug, Clone, FromRow, PartialEq, Eq)]
pub struct ContentOutboxOperation {
    pub operation_order: i64,
    pub item_id: String,
    pub sync_key: String,
    pub source_revision: String,
    pub force_files: bool,
    pub history_backfill: bool,
}

/// Allocates the durable order shared by local content and item-state operations.
pub async fn next_operation_order(connection: &mut SqliteConnection) -> Result<i64> {
    let floor = Utc::now().timestamp_millis().max(1);
    let now = Utc::now();
    let next: Option<String> = sqlx::query_scalar(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = MAX(CAST(sync_state.value AS INTEGER) + 1, CAST(excluded.value AS INTEGER)),
            updated_at = excluded.updated_at
        WHERE sync_state.value <> ''
          AND sync_state.value NOT GLOB '*[^0-9]*'
          AND CAST(sync_state.value AS INTEGER) < 9223372036854775807
        RETURNING value
        "#,
    )
    .bind(LOCAL_OPERATION_SEQUENCE_KEY)
    .bind(floor.to_string())
    .bind(now)
    .bind(now)
    .fetch_optional(connection)
    .await
    .context("allocate local sync operation order")?;

    next.context("invalid or exhausted local sync operation order")?
        .parse::<i64>()
        .context("invalid local sync operation order")
}

/// Stages one immutable item revision for background event materialization.
pub async fn stage_content(
    connection: &mut SqliteConnection,
    item_id: &str,
    sync_key: &str,
    source_revision: &str,
    force_files: bool,
    history_backfill: bool,
) -> Result<i64> {
    let operation_order = next_operation_order(connection).await?;
    let now = Utc::now();
    let stored_order: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO sync_content_outbox (
            operation_order, item_id, sync_key, source_revision,
            force_files, history_backfill, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_id, source_revision, force_files, history_backfill) DO UPDATE SET
            attempt_count = 0,
            next_attempt_at = NULL,
            last_error = NULL,
            updated_at = excluded.updated_at
        RETURNING operation_order
        "#,
    )
    .bind(operation_order)
    .bind(item_id)
    .bind(sync_key)
    .bind(source_revision)
    .bind(force_files)
    .bind(history_backfill)
    .bind(now)
    .bind(now)
    .fetch_one(connection)
    .await
    .context("stage clipboard content sync operation")?;
    Ok(stored_order)
}

/// Stages an existing item revision as a standalone transaction.
pub async fn stage_existing_content(
    pool: &SqlitePool,
    item_id: &str,
    sync_key: &str,
    source_revision: &str,
    force_files: bool,
    history_backfill: bool,
) -> Result<i64> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin clipboard content sync staging")?;
    let operation_order = stage_content(
        &mut transaction,
        item_id,
        sync_key,
        source_revision,
        force_files,
        history_backfill,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("commit clipboard content sync staging")?;
    Ok(operation_order)
}

/// Returns the earliest due content operation whose key has no earlier pending content operation.
pub async fn next_ready_content(
    pool: &SqlitePool,
    sync_sensitive: bool,
) -> Result<Option<ContentOutboxOperation>> {
    let now = Utc::now();
    sqlx::query_as::<_, ContentOutboxOperation>(
        r#"
        SELECT current.operation_order, current.item_id, current.sync_key,
               current.source_revision, current.force_files, current.history_backfill
        FROM sync_content_outbox current
        LEFT JOIN clipboard_items item ON item.id = current.item_id
        WHERE (
              next_attempt_at IS NULL
              OR next_attempt_at <= ?
              OR item.id IS NULL
              OR item.source_revision <> current.source_revision
              OR item.sync_key <> current.sync_key
          )
          AND (? OR item.is_sensitive = 0 OR item.id IS NULL)
          AND NOT EXISTS (
              SELECT 1 FROM sync_content_outbox earlier
              WHERE earlier.sync_key = current.sync_key
                AND earlier.operation_order < current.operation_order
          )
          AND NOT EXISTS (
              SELECT 1 FROM sync_item_states state
              WHERE state.sync_key = current.sync_key
                AND state.dirty_mask <> 0
                AND state.pending_order < current.operation_order
          )
        ORDER BY operation_order ASC
        LIMIT 1
        "#,
    )
    .bind(now)
    .bind(sync_sensitive)
    .fetch_optional(pool)
    .await
    .context("load next clipboard content sync operation")
}

pub async fn content_operation_exists(pool: &SqlitePool, operation_order: i64) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(SELECT 1 FROM sync_content_outbox WHERE operation_order = ?)",
    )
    .bind(operation_order)
    .fetch_one(pool)
    .await
    .context("check clipboard content sync operation")?
        != 0)
}

pub async fn complete_content(pool: &SqlitePool, operation_order: i64) -> Result<()> {
    sqlx::query("DELETE FROM sync_content_outbox WHERE operation_order = ?")
        .bind(operation_order)
        .execute(pool)
        .await
        .context("complete clipboard content sync operation")?;
    Ok(())
}

pub async fn fail_content(
    pool: &SqlitePool,
    operation_order: i64,
    error: &str,
) -> Result<Duration> {
    let attempt_count: i64 = sqlx::query_scalar(
        "SELECT attempt_count FROM sync_content_outbox WHERE operation_order = ?",
    )
    .bind(operation_order)
    .fetch_optional(pool)
    .await
    .context("read clipboard content sync attempt count")?
    .unwrap_or(0_i64)
    .saturating_add(1);
    let delay = retry_delay(attempt_count);
    let next_attempt_at = Utc::now() + delay;
    sqlx::query(
        r#"
        UPDATE sync_content_outbox
        SET attempt_count = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
        WHERE operation_order = ?
        "#,
    )
    .bind(attempt_count)
    .bind(next_attempt_at)
    .bind(error)
    .bind(Utc::now())
    .bind(operation_order)
    .execute(pool)
    .await
    .context("defer clipboard content sync operation")?;
    Ok(delay)
}

pub async fn next_content_retry_at(
    pool: &SqlitePool,
    sync_sensitive: bool,
) -> Result<Option<chrono::DateTime<Utc>>> {
    sqlx::query_scalar(
        r#"
        SELECT MIN(current.next_attempt_at)
        FROM sync_content_outbox current
        LEFT JOIN clipboard_items item ON item.id = current.item_id
        WHERE current.next_attempt_at IS NOT NULL
          AND (? OR item.is_sensitive = 0 OR item.id IS NULL)
        "#,
    )
    .bind(sync_sensitive)
    .fetch_one(pool)
    .await
    .context("read next clipboard content sync retry")
}

fn retry_delay(attempt_count: i64) -> Duration {
    let seconds = match attempt_count {
        i64::MIN..=1 => 2,
        2 => 5,
        3 => 15,
        4 => 30,
        5 => 60,
        _ => 300,
    };
    Duration::seconds(seconds)
}

#[cfg(test)]
mod tests {
    use crate::db::test_support::memory_pool;

    use super::*;

    #[tokio::test]
    async fn duplicate_stage_preserves_the_original_causal_order() {
        let pool = memory_pool().await;
        let mut transaction = pool.begin().await.unwrap();
        let first = stage_content(
            &mut transaction,
            "item",
            "stable-key",
            "revision",
            false,
            false,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = pool.begin().await.unwrap();
        let duplicate = stage_content(
            &mut transaction,
            "item",
            "stable-key",
            "revision",
            false,
            false,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(duplicate, first);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_content_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
}
