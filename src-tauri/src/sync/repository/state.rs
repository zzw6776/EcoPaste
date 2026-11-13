use super::*;

pub async fn cloud_cursor(pool: &SqlitePool) -> Result<u64> {
    state_value(pool, CLOUD_CURSOR_KEY)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()
        .context("invalid cloud sync cursor")
}

pub async fn cloud_availability_cursor(pool: &SqlitePool) -> Result<u64> {
    state_value(pool, CLOUD_AVAILABILITY_CURSOR_KEY)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()
        .context("invalid cloud availability cursor")
}

pub async fn cloud_pruned_availability_cursor(pool: &SqlitePool) -> Result<u64> {
    state_value(pool, CLOUD_PRUNED_AVAILABILITY_CURSOR_KEY)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()
        .context("invalid cloud pruning cursor")
}

pub async fn mark_cloud_pruning_reconciled(pool: &SqlitePool, cursor: u64) -> Result<()> {
    set_state_value(pool, CLOUD_PRUNED_AVAILABILITY_CURSOR_KEY, &cursor.to_string()).await
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
        (CLOUD_AVAILABILITY_CURSOR_KEY, "0".to_owned()),
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

pub async fn status_counts(pool: &SqlitePool) -> Result<(u64, u64, u64)> {
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sync_events e WHERE NOT EXISTS (SELECT 1 FROM sync_delivery_targets d WHERE d.event_id = e.event_id AND d.status = 'confirmed')",
    )
    .fetch_one(pool)
    .await?;
    let manual: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(DISTINCT links.item_id)
        FROM sync_item_events links
        JOIN sync_delivery_targets delivery ON delivery.event_id = links.event_id
        WHERE links.direction = 'local'
          AND delivery.target_id = 'cloud'
          AND delivery.confirmed_content = 'metadata'
          AND EXISTS (
              SELECT 1 FROM sync_event_blobs blobs WHERE blobs.event_id = links.event_id
          )
        "#,
    )
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

/// Loads the latest device-local external destination set for each requested item.
pub async fn external_download_sessions(
    pool: &SqlitePool,
    item_ids: &[String],
) -> Result<Vec<ExternalDownloadSession>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        r#"
        WITH latest AS (
            SELECT links.item_id, links.event_id, links.content_identity,
                   ROW_NUMBER() OVER (
                       PARTITION BY links.item_id ORDER BY links.created_at DESC
                   ) AS rank
            FROM sync_item_events links
            JOIN sync_events event ON event.event_id = links.event_id
            WHERE event.event_kind = 0 AND links.item_id IN (
        "#,
    );
    let mut ids = query.separated(", ");
    for item_id in item_ids {
        ids.push_bind(item_id);
    }
    query.push(
        r#")
        )
        SELECT latest.item_id, local.destinations_json, local.updated_at
        FROM latest
        JOIN sync_item_events matching
          ON matching.item_id = latest.item_id
         AND (
             matching.event_id = latest.event_id
             OR (
                 latest.content_identity IS NOT NULL
                 AND matching.content_identity = latest.content_identity
             )
         )
        JOIN sync_local_content local ON local.event_id = matching.event_id
        WHERE latest.rank = 1
          AND local.state IN ('external', 'external_missing')
        ORDER BY latest.item_id, local.updated_at DESC
        "#,
    );

    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("list external download receipts")?;
    let mut sessions = Vec::<ExternalDownloadSession>::new();
    let mut seen_items = HashSet::new();
    for row in rows {
        let item_id: String = row.try_get("item_id")?;
        if !seen_items.insert(item_id.clone()) {
            continue;
        }
        let destinations_json: Option<String> = row.try_get("destinations_json")?;
        let targets = destinations_json
            .as_deref()
            .map(serde_json::from_str::<Vec<ExternalDownloadTarget>>)
            .transpose()
            .context("decode external download destinations")?
            .unwrap_or_default();
        sessions.push(ExternalDownloadSession { item_id, targets });
    }
    Ok(sessions)
}

pub async fn item_statuses(pool: &SqlitePool, item_ids: &[String]) -> Result<Vec<SyncItemStatus>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    let lan_available = has_online_peer(pool).await?;
    let mut facts = item_ids
        .iter()
        .map(|id| {
            (
                id.clone(),
                ItemStatusFacts {
                    item_id: id.clone(),
                    origin: SyncItemOrigin::Local,
                    local_state: SyncLocalContentState::Managed,
                    queued_lan: false,
                    queued_cloud: false,
                    preparation_error: None,
                    lan_available,
                    remote_file_available: false,
                    deliveries: Vec::new(),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    let mut event_query = sqlx::QueryBuilder::new(
        r#"
        WITH latest AS (
            SELECT links.item_id, links.event_id, links.content_identity,
                   links.direction,
                   ROW_NUMBER() OVER (
                       PARTITION BY links.item_id ORDER BY links.created_at DESC
                   ) AS rank
            FROM sync_item_events links
            JOIN sync_events event ON event.event_id = links.event_id
            WHERE event.event_kind = 0 AND links.item_id IN (
        "#,
    );
    let mut event_item_ids = event_query.separated(", ");
    for item_id in item_ids {
        event_item_ids.push_bind(item_id);
    }
    event_query.push(
        r#")
        )
        SELECT latest.item_id,
               EXISTS(
                   SELECT 1 FROM sync_item_events local_link
                   JOIN sync_events local_event ON local_event.event_id = local_link.event_id
                   WHERE local_link.item_id = latest.item_id
                     AND local_link.direction = 'local'
                     AND local_event.event_kind = 0
               ) AS has_local_origin,
               COALESCE((
                   SELECT matching_replica.state
                   FROM sync_item_events matching
                   JOIN sync_local_content matching_replica
                     ON matching_replica.event_id = matching.event_id
                   WHERE matching.item_id = latest.item_id
                     AND (
                         matching.event_id = latest.event_id
                         OR (
                             latest.content_identity IS NOT NULL
                             AND matching.content_identity = latest.content_identity
                         )
                     )
                   ORDER BY CASE matching_replica.state
                       WHEN 'managed' THEN 4
                       WHEN 'external' THEN 3
                       WHEN 'external_missing' THEN 2
                       ELSE 1
                   END DESC,
                   matching_replica.updated_at DESC
                   LIMIT 1
               ), 'metadata') AS replica_state,
               latest.direction AS latest_direction,
               EXISTS(
                   SELECT 1 FROM sync_delivery_targets target
                   WHERE target.event_id = latest.event_id
                     AND target.target_id = 'cloud'
               ) AS has_cloud_target,
               EXISTS(
                   SELECT 1 FROM sync_peers peer
                   JOIN sync_peer_connections connection
                     ON connection.device_id = peer.device_id
                    AND connection.state = 'online'
                   WHERE NOT EXISTS (
                       SELECT 1 FROM sync_delivery_targets target
                       WHERE target.event_id = latest.event_id
                         AND target.target_id = 'peer:' || peer.device_id
                   )
               ) AS missing_lan_target,
               EXISTS(
                   SELECT 1 FROM sync_remote_file_items remote
                   WHERE remote.item_id = latest.item_id AND remote.state = 'available'
               ) AS remote_file_available
        FROM latest
        WHERE latest.rank = 1
        "#,
    );
    for row in event_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        let has_local_origin: bool = row.try_get("has_local_origin")?;
        let replica_state: String = row.try_get("replica_state")?;
        let Some(item_facts) = facts.get_mut(&item_id) else {
            continue;
        };
        item_facts.origin = if has_local_origin {
            SyncItemOrigin::Local
        } else {
            SyncItemOrigin::Remote
        };
        item_facts.local_state = if has_local_origin || replica_state == "managed" {
            SyncLocalContentState::Managed
        } else {
            match replica_state.as_str() {
                "external" => SyncLocalContentState::External,
                "external_missing" => SyncLocalContentState::ExternalMissing,
                _ => SyncLocalContentState::Missing,
            }
        };
        if row.try_get::<String, _>("latest_direction")? == "local" {
            let has_cloud_target: bool = row.try_get("has_cloud_target")?;
            let missing_lan_target: bool = row.try_get("missing_lan_target")?;
            item_facts.queued_cloud = !has_cloud_target;
            item_facts.queued_lan = lan_available && missing_lan_target;
        }
        item_facts.remote_file_available = row.try_get("remote_file_available")?;
    }

    let mut queued_query = sqlx::QueryBuilder::new(
        "SELECT queued.item_id, queued.last_error FROM sync_content_outbox queued \
         JOIN clipboard_items item ON item.id = queued.item_id \
         AND item.source_revision = queued.source_revision WHERE queued.item_id IN (",
    );
    let mut queued_ids = queued_query.separated(", ");
    for item_id in item_ids {
        queued_ids.push_bind(item_id);
    }
    queued_query.push(") ORDER BY queued.operation_order DESC");
    let mut queued_items = HashSet::new();
    for row in queued_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        if !queued_items.insert(item_id.clone()) {
            continue;
        }
        if let Some(item_facts) = facts.get_mut(&item_id) {
            item_facts.queued_cloud = true;
            item_facts.queued_lan = lan_available;
            item_facts.preparation_error = row.try_get("last_error")?;
        }
    }

    let mut delivery_query = sqlx::QueryBuilder::new(
        r#"
        WITH latest AS (
            SELECT links.item_id, links.event_id, links.content_identity,
                   ROW_NUMBER() OVER (
                       PARTITION BY links.item_id ORDER BY links.created_at DESC
                   ) AS rank
            FROM sync_item_events links
            JOIN sync_events linked_event ON linked_event.event_id = links.event_id
            WHERE links.item_id IN (
        "#,
    );
    let mut delivery_item_ids = delivery_query.separated(", ");
    for item_id in item_ids {
        delivery_item_ids.push_bind(item_id);
    }
    delivery_query.push(") AND linked_event.event_kind = ");
    delivery_query.push_bind(SyncEventKind::Content.as_i64());
    delivery_query.push(
        r#"),
        cloud_fallback AS (
            SELECT latest.item_id, target.target_id, target.desired_content,
                   target.confirmed_content, target.status, target.last_error,
                   ROW_NUMBER() OVER (
                       PARTITION BY latest.item_id
                       ORDER BY CASE target.confirmed_content
                           WHEN 'complete' THEN 2
                           WHEN 'metadata' THEN 1
                           ELSE 0
                       END DESC,
                       target.updated_at DESC
                   ) AS rank
            FROM latest
            JOIN sync_item_events equivalent
              ON equivalent.item_id = latest.item_id
             AND latest.content_identity IS NOT NULL
             AND equivalent.content_identity = latest.content_identity
             AND equivalent.event_id <> latest.event_id
            JOIN sync_delivery_targets target
              ON target.event_id = equivalent.event_id
             AND target.target_id = 'cloud'
            WHERE latest.rank = 1
              AND target.confirmed_content <> 'none'
              AND NOT EXISTS (
                  SELECT 1 FROM sync_delivery_targets current_target
                  WHERE current_target.event_id = latest.event_id
                    AND current_target.target_id = 'cloud'
              )
        )
        SELECT latest.item_id, target.target_id, target.desired_content,
               target.confirmed_content, target.status, target.last_error
        FROM latest
        JOIN sync_delivery_targets target ON target.event_id = latest.event_id
        WHERE latest.rank = 1
        UNION ALL
        SELECT item_id, target_id, desired_content,
               confirmed_content, status, last_error
        FROM cloud_fallback
        WHERE rank = 1
        "#,
    );
    for row in delivery_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        let target_id: String = row.try_get("target_id")?;
        let desired = ContentLevel::parse(&row.try_get::<String, _>("desired_content")?);
        let confirmed = ContentLevel::parse(&row.try_get::<String, _>("confirmed_content")?);
        let delivery_status = DeliveryStatus::parse(&row.try_get::<String, _>("status")?);
        let last_error: Option<String> = row.try_get("last_error")?;
        if let Some(item_facts) = facts.get_mut(&item_id) {
            item_facts.deliveries.push(DeliveryFact {
                target_id,
                desired,
                confirmed,
                status: delivery_status,
                last_error,
            });
        }
    }

    Ok(item_ids
        .iter()
        .filter_map(|id| facts.remove(id))
        .map(super::super::item_status::derive_item_status)
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
        "DELETE FROM sync_delivery_targets",
        "DELETE FROM sync_local_content",
        "DELETE FROM sync_item_events",
        "DELETE FROM sync_event_blobs",
        "DELETE FROM sync_content_objects",
        "DELETE FROM sync_events",
        "DELETE FROM sync_content_outbox",
        "DELETE FROM sync_item_states",
        "DELETE FROM sync_peers",
        "DELETE FROM sync_removed_peers",
        "DELETE FROM sync_state WHERE key <> 'origin_sequence'",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("run {statement}"))?;
    }
    Ok(())
}
