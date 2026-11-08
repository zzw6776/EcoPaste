use super::*;

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
        WHERE source_path IS NULL OR stream_nonce IS NULL
        UNION
        SELECT blob_id FROM sync_source_icon_uploads
        WHERE source_path IS NULL OR stream_nonce IS NULL
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list live local sync staging references")?;
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
        sqlx::QueryBuilder::new("SELECT item.id AS item_id, EXISTS(SELECT 1 FROM sync_pending_items p WHERE p.item_id = item.id) AS manual, EXISTS(SELECT 1 FROM sync_content_outbox q WHERE q.item_id = item.id) AS queued FROM clipboard_items item WHERE item.id IN (");
    let mut manual_ids = manual_query.separated(", ");
    for item_id in item_ids {
        manual_ids.push_bind(item_id);
    }
    manual_query.push(")");
    for row in manual_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        if let Some(status) = statuses.get_mut(&item_id) {
            if row.try_get::<bool, _>("queued")? {
                status.cloud.state = SyncItemState::Queued;
                if peer_count > 0 {
                    status.lan.state = SyncItemState::Queued;
                }
            } else if row.try_get::<bool, _>("manual")? {
                status.lan.state = SyncItemState::Manual;
                status.cloud.state = SyncItemState::Manual;
            }
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
            SELECT d.event_id, d.target_id,
                   CASE WHEN d.target_id = 'cloud' AND d.confirmed = 0 THEN 'confirming' ELSE 'success' END AS state,
                   NULL AS last_error
            FROM sync_deliveries d
            UNION ALL
            SELECT s.event_id, s.target_id,
                   CASE WHEN s.state = 'syncing' THEN s.phase ELSE s.state END AS state,
                   s.last_error
            FROM sync_delivery_states s
            WHERE NOT EXISTS (
                SELECT 1 FROM sync_deliveries d
                WHERE d.event_id = s.event_id AND d.target_id = s.target_id
            )
        ) targets ON targets.event_id = latest.event_id
        WHERE latest.rank = 1
          AND NOT EXISTS (SELECT 1 FROM sync_content_outbox queued WHERE queued.item_id = latest.item_id)
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
            "uploading" if channel.state != SyncItemState::Success => {
                channel.state = SyncItemState::Syncing;
            }
            "queued" if channel.state == SyncItemState::Idle => {
                channel.state = SyncItemState::Queued;
            }
            "confirming" if channel.state != SyncItemState::Success => {
                channel.state = SyncItemState::Confirming;
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
    sqlx::query(
        r#"
        UPDATE sync_remote_file_items
        SET event_id = NULL, state = 'expired', updated_at = ?
        WHERE state = 'available'
        "#,
    )
    .bind(Utc::now())
    .execute(&mut *transaction)
    .await
    .context("expire remote file metadata before clearing sync state")?;
    clear_group_runtime_on(&mut transaction).await?;
    transaction
        .commit()
        .await
        .context("commit clearing sync state")?;
    Ok(())
}

/// Drops transport state restored from a history backup while retaining materialized local data.
pub async fn clear_imported_group_state(
    pool: &SqlitePool,
    current_origin_sequence: Option<u64>,
) -> Result<u64> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin clearing imported sync state")?;
    let removed_remote_items = sqlx::query(
        "DELETE FROM clipboard_items WHERE id IN (SELECT item_id FROM sync_remote_file_items)",
    )
    .execute(&mut *transaction)
    .await
    .context("remove imported remote file metadata")?
    .rows_affected();
    clear_group_runtime_on(&mut transaction).await?;
    if let Some(sequence) = current_origin_sequence {
        set_state_value_on(&mut transaction, "origin_sequence", &sequence.to_string()).await?;
    } else {
        sqlx::query("DELETE FROM sync_state WHERE key = 'origin_sequence'")
            .execute(&mut *transaction)
            .await
            .context("clear imported sync origin sequence")?;
    }
    transaction
        .commit()
        .await
        .context("commit clearing imported sync state")?;
    Ok(removed_remote_items)
}

async fn clear_group_runtime_on(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<()> {
    for statement in [
        "DELETE FROM source_app_sync_aliases",
        "DELETE FROM sync_source_icon_uploads",
        "DELETE FROM sync_active_transfers",
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
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("run {statement}"))?;
    }
    Ok(())
}
