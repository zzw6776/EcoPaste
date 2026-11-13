use super::*;

pub async fn lock_transfer_item(pool: &SqlitePool, task_id: &str, item_id: &str) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        "INSERT OR REPLACE INTO sync_active_transfers (task_id, item_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(task_id)
    .bind(item_id)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("protect active transfer item")?;
    Ok(())
}

pub async fn unlock_transfer_item(pool: &SqlitePool, task_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM sync_active_transfers WHERE task_id = ?")
        .bind(task_id)
        .execute(pool)
        .await
        .context("release active transfer item")?;
    Ok(())
}

pub async fn clear_stale_transfer_locks(pool: &SqlitePool) -> Result<()> {
    sqlx::query("DELETE FROM sync_active_transfers")
        .execute(pool)
        .await
        .context("clear stale transfer item protections")?;
    Ok(())
}

/// Prevents a canceled upload route from being restarted by the background worker.
pub async fn cancel_delivery(pool: &SqlitePool, target_id: &str, event_id: &str) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_delivery_targets (
            event_id, target_id, desired_content, confirmed_content,
            status, last_error, created_at, updated_at
        )
        SELECT ?, ?,
               CASE WHEN EXISTS (
                   SELECT 1 FROM sync_event_blobs WHERE event_id = ?
               ) THEN 'complete' ELSE 'metadata' END,
               'none', 'canceled', 'transfer canceled', ?, ?
        WHERE EXISTS (SELECT 1 FROM sync_events WHERE event_id = ?)
        ON CONFLICT(event_id, target_id) DO UPDATE SET
            status = CASE
                WHEN sync_delivery_targets.confirmed_content = 'complete'
                    OR (
                        sync_delivery_targets.desired_content = 'metadata'
                        AND sync_delivery_targets.confirmed_content = 'metadata'
                    )
                    THEN 'confirmed'
                ELSE 'canceled'
            END,
            last_error = CASE
                WHEN sync_delivery_targets.confirmed_content = 'complete'
                    OR (
                        sync_delivery_targets.desired_content = 'metadata'
                        AND sync_delivery_targets.confirmed_content = 'metadata'
                    )
                    THEN NULL
                ELSE 'transfer canceled'
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(event_id)
    .bind(target_id)
    .bind(event_id)
    .bind(now)
    .bind(now)
    .bind(event_id)
    .execute(pool)
    .await
    .context("cancel sync delivery")?;
    Ok(())
}

/// Records an event-specific terminal failure without blocking unrelated background deliveries.
pub async fn block_delivery(
    pool: &SqlitePool,
    target_id: &str,
    event_id: &str,
    error: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_delivery_targets (
            event_id, target_id, desired_content, confirmed_content,
            status, last_error, created_at, updated_at
        )
        SELECT ?, ?,
               CASE WHEN EXISTS (
                   SELECT 1 FROM sync_event_blobs WHERE event_id = ?
               ) THEN 'complete' ELSE 'metadata' END,
               'none', 'blocked', ?, ?, ?
        WHERE EXISTS (SELECT 1 FROM sync_events WHERE event_id = ?)
        ON CONFLICT(event_id, target_id) DO UPDATE SET
            status = CASE
                WHEN sync_delivery_targets.confirmed_content = 'complete'
                    OR (
                        sync_delivery_targets.desired_content = 'metadata'
                        AND sync_delivery_targets.confirmed_content = 'metadata'
                    )
                    THEN 'confirmed'
                ELSE 'blocked'
            END,
            last_error = CASE
                WHEN sync_delivery_targets.confirmed_content = 'complete'
                    OR (
                        sync_delivery_targets.desired_content = 'metadata'
                        AND sync_delivery_targets.confirmed_content = 'metadata'
                    )
                    THEN NULL
                ELSE excluded.last_error
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(event_id)
    .bind(target_id)
    .bind(event_id)
    .bind(error)
    .bind(now)
    .bind(now)
    .bind(event_id)
    .execute(pool)
    .await
    .context("block sync delivery")?;
    Ok(())
}

/// Reads durable cancellation markers for events already selected into an in-memory batch.
pub async fn canceled_delivery_ids(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<HashSet<String>> {
    if event_ids.is_empty() {
        return Ok(HashSet::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT event_id FROM sync_delivery_targets WHERE status IN ('blocked', 'canceled') AND target_id = ",
    );
    query.push_bind(target_id).push(" AND event_id IN (");
    let mut separated = query.separated(", ");
    for event_id in event_ids {
        separated.push_bind(event_id);
    }
    query.push(")");
    Ok(query
        .build_query_scalar::<String>()
        .fetch_all(pool)
        .await
        .context("list canceled sync deliveries")?
        .into_iter()
        .collect())
}

/// Requests one explicit delivery route for the latest event linked to this item.
///
/// Returns whether an existing event was selected. Callers only materialize a new event when this
/// returns `false`.
pub async fn request_item_delivery(pool: &SqlitePool, item_id: &str, cloud: bool) -> Result<bool> {
    let mut transaction = pool.begin().await.context("begin explicit sync delivery")?;
    let event_id: Option<String> = sqlx::query_scalar(
        r#"
        SELECT links.event_id
        FROM sync_item_events links
        JOIN sync_events event ON event.event_id = links.event_id
        WHERE links.item_id = ? AND event.event_kind = ?
        ORDER BY links.created_at DESC
        LIMIT 1
        "#,
    )
    .bind(item_id)
    .bind(SyncEventKind::Content.as_i64())
    .fetch_optional(&mut *transaction)
    .await
    .context("find selected sync delivery event")?;
    let event_found = event_id.is_some();
    if let Some(event_id) = event_id {
        let now = Utc::now();
        if cloud {
            sqlx::query(
                r#"
                INSERT INTO sync_delivery_targets (
                    event_id, target_id, desired_content, confirmed_content,
                    status, content_requested, created_at, updated_at
                ) VALUES (?, 'cloud', 'complete', 'none', 'pending', 1, ?, ?)
                ON CONFLICT(event_id, target_id) DO UPDATE SET
                    desired_content = 'complete',
                    content_requested = 1,
                    status = CASE
                        WHEN sync_delivery_targets.confirmed_content = 'complete'
                            THEN 'confirmed'
                        ELSE 'pending'
                    END,
                    last_error = NULL,
                    retry_at = NULL,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(&event_id)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .context("request explicit cloud content delivery")?;
        } else {
            let peer_ids = sqlx::query_scalar::<_, String>(
                r#"
                SELECT peer.device_id
                FROM sync_peers peer
                JOIN sync_peer_connections connection
                  ON connection.device_id = peer.device_id
                 AND connection.state = 'online'
                "#,
            )
            .fetch_all(&mut *transaction)
            .await
            .context("list online LAN delivery targets")?;
            for peer_id in peer_ids {
                sqlx::query(
                    r#"
                    INSERT INTO sync_delivery_targets (
                        event_id, target_id, desired_content, confirmed_content,
                        status, content_requested, created_at, updated_at
                    ) VALUES (?, ?, 'complete', 'none', 'pending', 1, ?, ?)
                    ON CONFLICT(event_id, target_id) DO UPDATE SET
                        desired_content = 'complete',
                        content_requested = 1,
                        status = CASE
                            WHEN sync_delivery_targets.confirmed_content = 'complete'
                                THEN 'confirmed'
                            ELSE 'pending'
                        END,
                        last_error = NULL,
                        retry_at = NULL,
                        updated_at = excluded.updated_at
                    "#,
                )
                .bind(&event_id)
                .bind(format!("peer:{peer_id}"))
                .bind(now)
                .bind(now)
                .execute(&mut *transaction)
                .await
                .context("request explicit LAN content delivery")?;
            }
        }
    }
    transaction
        .commit()
        .await
        .context("commit explicit sync delivery")?;
    Ok(event_found)
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

/// Persists only the plaintext source and nonce required to recreate transport staging on demand.
pub async fn register_event_blob_sources(
    pool: &SqlitePool,
    event_id: &str,
    blobs: &[StoredBlob],
) -> Result<()> {
    let now = Utc::now();
    let mut transaction = pool
        .begin()
        .await
        .context("begin sync blob source registration")?;
    for blob in blobs {
        if blob.source_path.is_none() || blob.stream_nonce.is_none() {
            bail!("sync blob source registration is incomplete");
        }
        sqlx::query(
            r#"
            INSERT INTO sync_event_blobs (
                event_id, blob_id, encrypted_path, size, source_path,
                source_is_directory, stream_nonce, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, blob_id) DO UPDATE SET
                encrypted_path = excluded.encrypted_path,
                size = excluded.size,
                source_path = excluded.source_path,
                source_is_directory = excluded.source_is_directory,
                stream_nonce = excluded.stream_nonce,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(&blob.blob_id)
        .bind(&blob.encrypted_path)
        .bind(i64::try_from(blob.size).context("encrypted sync blob is too large")?)
        .bind(&blob.source_path)
        .bind(blob.source_is_directory)
        .bind(&blob.stream_nonce)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("register sync blob source")?;
    }
    transaction
        .commit()
        .await
        .context("commit sync blob source registration")?;
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

/// Lists pending content that the active source actually advertised to this device.
pub async fn unapplied_content_events_for_target(
    pool: &SqlitePool,
    target_id: &str,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT event.cursor, event.event_id, event.origin_device_id, event.origin_sequence,
               event.event_created_at_ms, event.nonce, event.ciphertext, event.event_kind
        FROM sync_events event
        JOIN sync_delivery_targets target ON target.event_id = event.event_id
        WHERE event.is_applied = 0
          AND event.apply_state = 'pending'
          AND event.event_kind = ?
          AND target.target_id = ?
          AND (event.next_apply_at IS NULL OR event.next_apply_at <= ?)
        ORDER BY event.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(SyncEventKind::Content.as_i64())
    .bind(target_id)
    .bind(Utc::now())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list unapplied sync content for source")?;
    rows.into_iter().map(event_from_row).collect()
}

pub async fn has_unapplied_content_for_target(pool: &SqlitePool, target_id: &str) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM sync_events event
            JOIN sync_delivery_targets target ON target.event_id = event.event_id
            WHERE event.is_applied = 0
              AND event.apply_state = 'pending'
              AND event.event_kind = ?
              AND target.target_id = ?
              AND (event.next_apply_at IS NULL OR event.next_apply_at <= ?)
        )
        "#,
    )
    .bind(SyncEventKind::Content.as_i64())
    .bind(target_id)
    .bind(Utc::now())
    .fetch_one(pool)
    .await
    .context("check unapplied sync content for source")?
        != 0)
}

/// Lists records for one LAN peer without echoing events that originated from that peer.
///
/// Content routing is applied later when blobs are selected; it must never hide the record itself.
pub async fn pending_peer_events_for_target(
    pool: &SqlitePool,
    target_id: &str,
    peer_device_id: &str,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext, e.event_kind
        FROM sync_events e
        LEFT JOIN sync_delivery_targets d
          ON d.event_id = e.event_id AND d.target_id = ?
        WHERE (
              d.event_id IS NULL
              OR (
                  d.status = 'pending'
                  AND (
                      d.confirmed_content = 'none'
                      OR (d.desired_content = 'complete' AND d.confirmed_content <> 'complete')
                  )
              )
          )
          AND e.origin_device_id <> ?
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(peer_device_id)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list pending sync events")?;
    rows.into_iter().map(event_from_row).collect()
}

/// Lists locally originated events plus remote content explicitly selected for Hub upload.
pub async fn pending_cloud_events_for_target(
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
        LEFT JOIN sync_delivery_targets d
          ON d.event_id = e.event_id AND d.target_id = ?
        WHERE (
              d.event_id IS NULL
              OR (
                  d.status = 'pending'
                  AND (
                      d.confirmed_content = 'none'
                      OR (d.desired_content = 'complete' AND d.confirmed_content <> 'complete')
                  )
              )
          )
          AND (
              e.origin_device_id = ?
              OR (
                  d.desired_content = 'complete'
                  AND d.confirmed_content <> 'complete'
              )
          )
          AND substr(e.ciphertext, 1, 8) = ?
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(origin_device_id)
    .bind(crate::sync::crypto::EVENT_AAD_MAGIC.as_slice())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list pending origin sync events")?;
    rows.into_iter().map(event_from_row).collect()
}

#[cfg(test)]
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

/// Reads the peer-visible LAN record stream.
pub async fn peer_events_after_cursor(
    pool: &SqlitePool,
    peer_device_id: &str,
    after_cursor: u64,
    limit: u16,
) -> Result<(Vec<StoredSyncEvent>, u64)> {
    let after_cursor = i64::try_from(after_cursor).unwrap_or(i64::MAX);
    let rows = sqlx::query(
        r#"
        SELECT cursor, event_id, origin_device_id, origin_sequence,
               event_created_at_ms, nonce, ciphertext, event_kind
        FROM sync_events
        WHERE cursor > ?
          AND origin_device_id <> ?
        ORDER BY cursor ASC
        LIMIT ?
        "#,
    )
    .bind(after_cursor)
    .bind(peer_device_id)
    .bind(i64::from(limit.max(1)))
    .fetch_all(pool)
    .await
    .context("list peer-visible sync events after cursor")?;
    let events = rows
        .into_iter()
        .map(event_from_row)
        .collect::<Result<Vec<_>>>()?;
    let latest_cursor = if let Some(event) = events.last() {
        event.cursor
    } else {
        let latest: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(cursor), ?) FROM sync_events WHERE cursor > ?")
                .bind(after_cursor)
                .bind(after_cursor)
                .fetch_one(pool)
                .await
                .context("advance peer cursor across filtered sync events")?;
        u64::try_from(latest).context("negative sync event cursor")?
    };
    Ok((events, latest_cursor))
}

pub async fn blobs_for_events(pool: &SqlitePool, event_ids: &[String]) -> Result<Vec<StoredBlob>> {
    if event_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT DISTINCT blob_id, encrypted_path, size, source_path, source_is_directory, stream_nonce FROM sync_event_blobs WHERE event_id IN (",
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
                source_path: row.try_get("source_path")?,
                source_is_directory: row.try_get("source_is_directory")?,
                stream_nonce: row.try_get("stream_nonce")?,
            })
        })
        .collect()
}

/// Reads explicit target requests and exclusive share routes without changing another route.
pub async fn event_ids_with_requested_content(
    pool: &SqlitePool,
    events: &[StoredSyncEvent],
    target_id: &str,
    route: &str,
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT event.event_id FROM sync_events event \
         LEFT JOIN sync_delivery_targets target ON target.event_id = event.event_id \
         AND target.target_id = ",
    );
    query
        .push_bind(target_id)
        .push(" WHERE (target.content_requested = 1 OR event.delivery_route = ")
        .push_bind(route)
        .push(") AND event.event_id IN (");
    let mut separated = query.separated(", ");
    for event in events {
        separated.push_bind(&event.event.event_id);
    }
    query.push(")");
    Ok(query
        .build_query_scalar::<String>()
        .fetch_all(pool)
        .await
        .context("list explicit sync content requests")?
        .into_iter()
        .collect())
}

/// Reads record acknowledgements separately from the outstanding content request.
pub async fn event_ids_without_record(
    pool: &SqlitePool,
    events: &[StoredSyncEvent],
    target_id: &str,
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT event_id FROM sync_delivery_targets WHERE confirmed_content = 'none' AND target_id = ",
    );
    query.push_bind(target_id).push(" AND event_id IN (");
    let mut ids = query.separated(", ");
    for event in events {
        ids.push_bind(&event.event.event_id);
    }
    query.push(")");
    Ok(query
        .build_query_scalar::<String>()
        .fetch_all(pool)
        .await
        .context("read sync record acknowledgements")?
        .into_iter()
        .collect())
}

/// Returns events whose complete payload is assigned to another route.
pub async fn event_ids_routed_away_from(
    pool: &SqlitePool,
    events: &[StoredSyncEvent],
    route: &str,
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT event_id FROM sync_events WHERE delivery_route IS NOT NULL AND delivery_route <> ",
    );
    query.push_bind(route).push(" AND event_id IN (");
    let mut separated = query.separated(", ");
    for event in events {
        separated.push_bind(&event.event.event_id);
    }
    query.push(")");
    Ok(query
        .build_query_scalar::<String>()
        .fetch_all(pool)
        .await
        .context("list metadata-only route events")?
        .into_iter()
        .collect())
}

/// Returns file events whose current source can provide the complete body.
///
/// Hub availability is authoritative. A LAN peer records a complete offer before the receiver
/// starts downloading, then confirms it after the bytes have been materialized.
pub async fn event_ids_with_complete_content_source(
    pool: &SqlitePool,
    events: &[StoredSyncEvent],
    target_id: &str,
    cloud: bool,
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }
    let mut query =
        sqlx::QueryBuilder::new("SELECT event_id FROM sync_delivery_targets WHERE target_id = ");
    query.push_bind(target_id).push(" AND ");
    if cloud {
        query.push("confirmed_content = 'complete'");
    } else {
        query.push("desired_content = 'complete'");
    }
    query.push(" AND event_id IN (");
    let mut separated = query.separated(", ");
    for event in events {
        separated.push_bind(&event.event.event_id);
    }
    query.push(")");
    Ok(query
        .build_query_scalar::<String>()
        .fetch_all(pool)
        .await
        .context("list complete content sources")?
        .into_iter()
        .collect())
}

/// Resolves a local blob so a LAN pull can use staging or recreate it from its source.
pub async fn outgoing_blob(pool: &SqlitePool, blob_id: &str) -> Result<Option<StoredBlob>> {
    let row = sqlx::query(
        r#"
        SELECT blob_id, encrypted_path, size, source_path, source_is_directory, stream_nonce
        FROM sync_event_blobs
        WHERE blob_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        "#,
    )
    .bind(blob_id)
    .fetch_optional(pool)
    .await
    .context("find outgoing sync staging source")?;
    row.map(|row| {
        let size: i64 = row.try_get("size")?;
        Ok(StoredBlob {
            blob_id: row.try_get("blob_id")?,
            encrypted_path: row.try_get("encrypted_path")?,
            size: u64::try_from(size).context("negative encrypted blob size")?,
            source_path: row.try_get("source_path")?,
            source_is_directory: row.try_get("source_is_directory")?,
            stream_nonce: row.try_get("stream_nonce")?,
        })
    })
    .transpose()
}

/// Resolves a pulled file blob back to the local clipboard operation that owns it.
pub async fn outgoing_file_event_for_blob(
    pool: &SqlitePool,
    blob_id: &str,
) -> Result<Option<(String, StoredSyncEvent)>> {
    let row = sqlx::query(
        r#"
        SELECT links.item_id,
               event.cursor, event.event_id, event.origin_device_id,
               event.origin_sequence, event.event_created_at_ms,
               event.nonce, event.ciphertext, event.event_kind
        FROM sync_event_blobs blob
        JOIN sync_item_events links
          ON links.event_id = blob.event_id
        JOIN sync_events event ON event.event_id = links.event_id
        JOIN clipboard_items item ON item.id = links.item_id
        WHERE blob.blob_id = ?
          AND event.event_kind = ?
          AND item.kind = 'files'
        ORDER BY event.event_created_at_ms DESC, event.cursor DESC
        LIMIT 1
        "#,
    )
    .bind(blob_id)
    .bind(SyncEventKind::Content.as_i64())
    .fetch_optional(pool)
    .await
    .context("resolve pulled file blob operation")?;
    row.map(|row| {
        let item_id = row.try_get("item_id")?;
        let event = event_from_row(row)?;
        Ok((item_id, event))
    })
    .transpose()
}

/// Returns reproducible staged files whose currently known deliveries are all complete.
pub async fn releasable_staging_for_events(
    pool: &SqlitePool,
    event_ids: &[String],
) -> Result<Vec<(String, PathBuf)>> {
    if event_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        r#"
        SELECT DISTINCT candidate.blob_id, candidate.encrypted_path
        FROM sync_event_blobs candidate
        WHERE candidate.source_path IS NOT NULL
          AND candidate.stream_nonce IS NOT NULL
          AND candidate.encrypted_path <> ''
          AND candidate.event_id IN (
        "#,
    );
    let mut separated = query.separated(", ");
    for event_id in event_ids {
        separated.push_bind(event_id);
    }
    query.push(
        r#")
          AND NOT EXISTS (
              SELECT 1
              FROM sync_event_blobs reference
              JOIN sync_delivery_targets delivery ON delivery.event_id = reference.event_id
              WHERE reference.blob_id = candidate.blob_id
                AND delivery.status IN ('pending', 'awaiting_receipt')
                AND (
                    delivery.confirmed_content = 'none'
                    OR (
                        delivery.desired_content = 'complete'
                        AND delivery.confirmed_content <> 'complete'
                    )
                )
          )
          AND NOT EXISTS (
              SELECT 1 FROM sync_source_icon_uploads icon
              WHERE icon.blob_id = candidate.blob_id
          )
        "#,
    );
    let paths = query
        .build_query_as::<(String, String)>()
        .fetch_all(pool)
        .await
        .context("list releasable sync staging files")?;
    Ok(paths
        .into_iter()
        .map(|(blob_id, path)| (blob_id, PathBuf::from(path)))
        .collect())
}

/// Resolves one outgoing event to the logical clipboard item shown in transfer progress.
pub async fn transfer_item_id_for_event(
    pool: &SqlitePool,
    event_id: &str,
) -> Result<Option<String>> {
    sqlx::query_scalar(
        r#"
        SELECT links.item_id
        FROM sync_item_events links
        WHERE links.event_id = ?
        ORDER BY links.created_at DESC
        LIMIT 1
        "#,
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await
    .context("resolve outgoing event transfer item")
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
                group_id, blob_id, encrypted_path, size, source_path,
                source_is_directory, stream_nonce, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, blob_id) DO UPDATE SET
                encrypted_path = excluded.encrypted_path,
                size = excluded.size,
                source_path = excluded.source_path,
                source_is_directory = excluded.source_is_directory,
                stream_nonce = excluded.stream_nonce,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(group_id)
        .bind(&blob.blob_id)
        .bind(&blob.encrypted_path)
        .bind(i64::try_from(blob.size).context("encrypted source icon is too large")?)
        .bind(&blob.source_path)
        .bind(blob.source_is_directory)
        .bind(&blob.stream_nonce)
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
        SELECT blob_id, encrypted_path, size, source_path, source_is_directory, stream_nonce
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
                source_path: row.try_get("source_path")?,
                source_is_directory: row.try_get("source_is_directory")?,
                stream_nonce: row.try_get("stream_nonce")?,
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

#[cfg(test)]
pub async fn mark_delivered(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    mark_delivered_with_metadata(pool, target_id, event_ids, &HashSet::new()).await
}

/// Persists whether each confirmed route received file bytes or authenticated metadata only.
#[cfg(test)]
pub async fn mark_delivered_with_metadata(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    metadata_only_event_ids: &HashSet<String>,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin sync delivery transaction")?;
    mark_delivered_on(
        &mut transaction,
        target_id,
        event_ids,
        metadata_only_event_ids,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("commit sync deliveries")?;
    Ok(())
}

/// A LAN record acknowledgement confirms metadata; only its receiver receipt confirms content.
pub async fn complete_outbound_delivery(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    metadata_only_event_ids: &HashSet<String>,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin completed outbound delivery")?;
    for event_id in event_ids {
        let content = if target_id != CLOUD_TARGET || metadata_only_event_ids.contains(event_id) {
            ContentLevel::Metadata
        } else {
            ContentLevel::Complete
        };
        confirm_delivery_on(&mut transaction, target_id, event_id, content, false).await?;
        if target_id != CLOUD_TARGET && !metadata_only_event_ids.contains(event_id) {
            sqlx::query(
                r#"
                UPDATE sync_delivery_targets
                SET status = 'awaiting_receipt'
                WHERE event_id = ? AND target_id = ?
                  AND desired_content = 'complete' AND confirmed_content <> 'complete'
                  AND status = 'pending'
                "#,
            )
            .bind(event_id)
            .bind(target_id)
            .execute(&mut *transaction)
            .await
            .context("await LAN content receipt")?;
        }
    }
    transaction
        .commit()
        .await
        .context("commit completed outbound delivery")?;
    Ok(())
}

/// Confirms durable LAN receipts using the route's two availability levels.
pub async fn confirm_lan_receipts(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    complete_event_ids: &HashSet<String>,
) -> Result<()> {
    let metadata_only_event_ids = event_ids
        .iter()
        .filter(|event_id| !complete_event_ids.contains(*event_id))
        .cloned()
        .collect::<HashSet<_>>();
    let mut transaction = pool.begin().await.context("begin confirmed LAN receipts")?;
    for event_id in event_ids {
        let content = if metadata_only_event_ids.contains(event_id) {
            ContentLevel::Metadata
        } else {
            ContentLevel::Complete
        };
        confirm_delivery_on(&mut transaction, target_id, event_id, content, false).await?;
    }
    transaction
        .commit()
        .await
        .context("commit confirmed LAN receipts")?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct PendingPeerReceipt {
    pub event_id: String,
    pub content_complete: bool,
}

/// Reads durable LAN receipts that still need to be returned to one peer.
pub async fn pending_peer_receipts(
    pool: &SqlitePool,
    peer_device_id: &str,
    limit: u16,
) -> Result<Vec<PendingPeerReceipt>> {
    let target_id = format!("peer:{peer_device_id}");
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT delivery.event_id, delivery.confirmed_content
        FROM sync_delivery_targets delivery
        WHERE delivery.target_id = ?
          AND delivery.receipt_pending = 1
        ORDER BY delivery.created_at, delivery.event_id
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(i64::from(limit.max(1)))
    .fetch_all(pool)
    .await
    .context("list pending peer receipts")?;
    Ok(rows
        .into_iter()
        .map(|(event_id, confirmed_content)| PendingPeerReceipt {
            event_id,
            content_complete: confirmed_content == "complete",
        })
        .collect())
}

/// Records that one route delivered complete content without claiming an app-managed replica.
pub async fn complete_remote_file_download(
    pool: &SqlitePool,
    origin_device_id: &str,
    event_id: &str,
    lan_only: bool,
) -> Result<()> {
    let source = if lan_only {
        RemoteBatchSource::Peer {
            device_id: origin_device_id,
        }
    } else {
        RemoteBatchSource::Cloud
    };
    record_inbound_outcome(pool, source, event_id, InboundContentOutcome::External).await?;
    Ok(())
}

/// Replaces the user-owned destination set attached to this local content replica.
pub async fn record_external_download_session(
    pool: &SqlitePool,
    event_id: &str,
    targets: &[ExternalDownloadTarget],
) -> Result<()> {
    if targets.is_empty() {
        bail!("external download has no destinations");
    }
    let destinations_json =
        serde_json::to_string(targets).context("encode external download destinations")?;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_local_content (
            event_id, state, destinations_json, created_at, updated_at
        ) VALUES (?, 'external', ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            state = CASE
                WHEN sync_local_content.state = 'managed' THEN 'managed'
                ELSE 'external'
            END,
            destinations_json = excluded.destinations_json,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(event_id)
    .bind(destinations_json)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("record external download destinations")?;
    Ok(())
}

/// Removes accepted receipts without deleting a concurrently upgraded content receipt.
pub async fn complete_peer_receipts(
    pool: &SqlitePool,
    peer_device_id: &str,
    receipts: &[PendingPeerReceipt],
) -> Result<()> {
    if receipts.is_empty() {
        return Ok(());
    }
    let target_id = format!("peer:{peer_device_id}");
    let mut transaction = pool
        .begin()
        .await
        .context("begin completing peer receipts")?;
    for receipt in receipts {
        sqlx::query(
            r#"
            UPDATE sync_delivery_targets
            SET receipt_pending = 0, updated_at = ?
            WHERE target_id = ? AND event_id = ? AND confirmed_content = ?
            "#,
        )
        .bind(Utc::now())
        .bind(&target_id)
        .bind(&receipt.event_id)
        .bind(if receipt.content_complete {
            "complete"
        } else {
            "metadata"
        })
        .execute(&mut *transaction)
        .await
        .context("complete peer receipt")?;
    }
    transaction
        .commit()
        .await
        .context("commit completed peer receipts")?;
    Ok(())
}

#[cfg(test)]
async fn mark_delivered_on(
    connection: &mut SqliteConnection,
    target_id: &str,
    event_ids: &[String],
    metadata_only_event_ids: &HashSet<String>,
) -> Result<()> {
    for event_id in event_ids {
        let content = if metadata_only_event_ids.contains(event_id) {
            ContentLevel::Metadata
        } else {
            ContentLevel::Complete
        };
        confirm_delivery_on(connection, target_id, event_id, content, false).await?;
    }
    Ok(())
}

/// Records route transport and local replica state without deriving either fact from the other.
pub async fn record_inbound_outcome(
    pool: &SqlitePool,
    source: RemoteBatchSource<'_>,
    event_id: &str,
    outcome: InboundContentOutcome,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin inbound content outcome")?;
    record_inbound_outcome_on(&mut transaction, source, event_id, outcome).await?;
    transaction
        .commit()
        .await
        .context("commit inbound content outcome")?;
    Ok(())
}

/// Applies one Hub availability page and its continuation cursor atomically.
pub async fn record_cloud_availability_batch(
    pool: &SqlitePool,
    updates: &[(String, RouteContentAvailability)],
    latest_cursor: u64,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin cloud availability batch")?;
    for (event_id, state) in updates {
        let content = match state {
            RouteContentAvailability::Metadata => ContentLevel::Metadata,
            RouteContentAvailability::Complete => ContentLevel::Complete,
        };
        set_cloud_availability_on(&mut transaction, event_id, content).await?;
    }
    set_state_value_on(
        &mut transaction,
        CLOUD_AVAILABILITY_CURSOR_KEY,
        &latest_cursor.to_string(),
    )
    .await?;
    transaction
        .commit()
        .await
        .context("commit cloud availability batch")?;
    Ok(())
}

async fn record_inbound_outcome_on(
    connection: &mut SqliteConnection,
    source: RemoteBatchSource<'_>,
    event_id: &str,
    outcome: InboundContentOutcome,
) -> Result<()> {
    let now = Utc::now();
    let local_state = match outcome {
        InboundContentOutcome::Received | InboundContentOutcome::Deduplicated => "managed",
        InboundContentOutcome::External => "external",
        InboundContentOutcome::Metadata => "metadata",
    };
    sqlx::query(
        r#"
        INSERT INTO sync_local_content (
            event_id, state, destinations_json, created_at, updated_at
        )
        SELECT ?,
               CASE WHEN EXISTS (
                   SELECT 1 FROM sync_item_events
                   WHERE event_id = ? AND direction = 'local'
               ) THEN 'managed' ELSE ? END,
               NULL, ?, ?
        WHERE EXISTS (
            SELECT 1 FROM sync_events WHERE event_id = ? AND event_kind = 0
        )
        ON CONFLICT(event_id) DO UPDATE SET
            state = CASE
                WHEN sync_local_content.state = 'managed' THEN 'managed'
                WHEN excluded.state = 'managed' THEN 'managed'
                WHEN sync_local_content.state = 'external' THEN 'external'
                WHEN excluded.state = 'external' THEN 'external'
                ELSE excluded.state
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(event_id)
    .bind(event_id)
    .bind(local_state)
    .bind(now)
    .bind(now)
    .bind(event_id)
    .execute(&mut *connection)
    .await
    .context("record local content replica")?;

    let (target_id, content, receipt_pending) = match source {
        RemoteBatchSource::Cloud => (CLOUD_TARGET.to_owned(), ContentLevel::Metadata, false),
        RemoteBatchSource::Peer { device_id } => (
            format!("peer:{device_id}"),
            if outcome.received_bytes() {
                ContentLevel::Complete
            } else {
                ContentLevel::Metadata
            },
            true,
        ),
    };
    confirm_delivery_on(connection, &target_id, event_id, content, receipt_pending).await?;
    Ok(())
}

/// Replaces the cloud fact with the Hub's authoritative blob availability.
async fn set_cloud_availability_on(
    connection: &mut SqliteConnection,
    event_id: &str,
    content: ContentLevel,
) -> Result<()> {
    let now = Utc::now();
    let content_became_available = content == ContentLevel::Complete
        && !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM sync_delivery_targets \
             WHERE event_id = ? AND target_id = 'cloud' AND confirmed_content = 'complete')",
        )
        .bind(event_id)
        .fetch_one(&mut *connection)
        .await?;
    sqlx::query(
        r#"
        INSERT INTO sync_delivery_targets (
            event_id, target_id, desired_content, confirmed_content,
            status, receipt_pending, last_error, created_at, updated_at
        )
        SELECT ?, 'cloud', ?, ?, 'confirmed', 0, NULL, ?, ?
        WHERE EXISTS (SELECT 1 FROM sync_events WHERE event_id = ?)
        ON CONFLICT(event_id, target_id) DO UPDATE SET
            desired_content = CASE
                WHEN sync_delivery_targets.desired_content = 'complete'
                    AND sync_delivery_targets.confirmed_content <> 'complete'
                    THEN 'complete'
                ELSE excluded.confirmed_content
            END,
            confirmed_content = excluded.confirmed_content,
            status = CASE
                WHEN excluded.confirmed_content = 'complete' THEN 'confirmed'
                WHEN sync_delivery_targets.status IN ('blocked', 'canceled')
                    THEN sync_delivery_targets.status
                WHEN sync_delivery_targets.desired_content = 'complete'
                    AND sync_delivery_targets.confirmed_content <> 'complete'
                    AND excluded.confirmed_content <> 'complete'
                    THEN 'pending'
                ELSE 'confirmed'
            END,
            receipt_pending = 0,
            last_error = CASE
                WHEN excluded.confirmed_content <> 'complete'
                    AND sync_delivery_targets.status IN ('blocked', 'canceled')
                    THEN sync_delivery_targets.last_error
                ELSE NULL
            END,
            retry_at = NULL,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(event_id)
    .bind(content.as_str())
    .bind(content.as_str())
    .bind(now)
    .bind(now)
    .bind(event_id)
    .execute(&mut *connection)
    .await
    .context("record authoritative cloud availability")?;
    if content_became_available {
        requeue_metadata_content_on(connection, event_id).await?;
    }
    Ok(())
}

/// Updates the sole durable route fact after an authenticated acknowledgement.
async fn confirm_delivery_on(
    connection: &mut SqliteConnection,
    target_id: &str,
    event_id: &str,
    content: ContentLevel,
    receipt_pending: bool,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_delivery_targets (
            event_id, target_id, desired_content, confirmed_content,
            status, receipt_pending, last_error, created_at, updated_at
        )
        SELECT ?, ?, ?, ?, 'confirmed', ?, NULL, ?, ?
        WHERE EXISTS (SELECT 1 FROM sync_events WHERE event_id = ?)
        ON CONFLICT(event_id, target_id) DO UPDATE SET
            desired_content = CASE
                WHEN sync_delivery_targets.desired_content = 'complete'
                    OR excluded.confirmed_content = 'complete'
                    THEN 'complete'
                ELSE 'metadata'
            END,
            confirmed_content = CASE
                WHEN excluded.confirmed_content = 'complete' THEN 'complete'
                WHEN sync_delivery_targets.confirmed_content = 'complete' THEN 'complete'
                ELSE excluded.confirmed_content
            END,
            status = CASE
                WHEN excluded.confirmed_content = 'complete'
                    OR sync_delivery_targets.confirmed_content = 'complete'
                    THEN 'confirmed'
                WHEN sync_delivery_targets.status IN ('blocked', 'canceled')
                    THEN sync_delivery_targets.status
                WHEN sync_delivery_targets.desired_content = 'complete'
                    THEN CASE
                        WHEN sync_delivery_targets.status = 'awaiting_receipt'
                            THEN 'awaiting_receipt'
                        ELSE 'pending'
                    END
                ELSE 'confirmed'
            END,
            receipt_pending = MAX(
                sync_delivery_targets.receipt_pending,
                excluded.receipt_pending
            ),
            last_error = CASE
                WHEN excluded.confirmed_content <> 'complete'
                    AND sync_delivery_targets.confirmed_content <> 'complete'
                    AND sync_delivery_targets.status IN ('blocked', 'canceled')
                    THEN sync_delivery_targets.last_error
                ELSE NULL
            END,
            retry_at = NULL,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(event_id)
    .bind(target_id)
    .bind(content.as_str())
    .bind(content.as_str())
    .bind(receipt_pending)
    .bind(now)
    .bind(now)
    .bind(event_id)
    .execute(&mut *connection)
    .await
    .context("confirm sync delivery")?;
    Ok(())
}

/// Records that a peer offered the complete body while the local replica still has metadata only.
async fn expect_complete_delivery_on(
    connection: &mut SqliteConnection,
    target_id: &str,
    event_id: &str,
) -> Result<()> {
    let now = Utc::now();
    let already_offered = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM sync_delivery_targets \
         WHERE event_id = ? AND target_id = ? AND desired_content = 'complete')",
    )
    .bind(event_id)
    .bind(target_id)
    .fetch_one(&mut *connection)
    .await?;
    sqlx::query(
        r#"
        UPDATE sync_delivery_targets
        SET desired_content = 'complete',
            status = CASE
                WHEN confirmed_content = 'complete' THEN 'confirmed'
                ELSE 'pending'
            END,
            updated_at = ?
        WHERE event_id = ? AND target_id = ?
        "#,
    )
    .bind(now)
    .bind(event_id)
    .bind(target_id)
    .execute(&mut *connection)
    .await
    .context("record complete inbound content offer")?;
    if !already_offered {
        requeue_metadata_content_on(connection, event_id).await?;
    }
    Ok(())
}

/// Replays metadata-only replicas once their body becomes available, keeping saved files intact.
async fn requeue_metadata_content_on(
    connection: &mut SqliteConnection,
    event_id: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sync_events
        SET is_applied = 0, apply_state = 'pending', next_apply_at = NULL,
            last_apply_error = NULL, updated_at = ?
        WHERE event_id = ? AND is_applied = 1
          AND EXISTS (
              SELECT 1 FROM sync_local_content content
              WHERE content.event_id = sync_events.event_id AND content.state = 'metadata'
          )
        "#,
    )
    .bind(Utc::now())
    .bind(event_id)
    .execute(connection)
    .await
    .context("resume synchronized content after metadata")?;
    Ok(())
}

/// Stores received events, route acknowledgements, and an optional pull cursor atomically.
/// Inbound pushes omit the cursor so they cannot skip events in a later peer pull.
/// A crash therefore either replays the whole batch or observes every event on the next cycle.
#[cfg(test)]
pub async fn store_remote_batch(
    pool: &SqlitePool,
    source: RemoteBatchSource<'_>,
    latest_cursor: Option<u64>,
    events: &[EncryptedEvent],
    local_device_id: &str,
) -> Result<StoreRemoteBatchResult> {
    store_remote_batch_with_availability(pool, source, latest_cursor, events, local_device_id, &[])
        .await
}

/// Stores a remote batch, its route facts, and its continuation cursor in one transaction.
#[cfg(test)]
pub async fn store_remote_batch_with_availability(
    pool: &SqlitePool,
    source: RemoteBatchSource<'_>,
    latest_cursor: Option<u64>,
    events: &[EncryptedEvent],
    local_device_id: &str,
    availability: &[(String, RouteContentAvailability)],
) -> Result<StoreRemoteBatchResult> {
    store_remote_batch_with_content_policy(
        pool,
        source,
        latest_cursor,
        events,
        local_device_id,
        availability,
        &HashSet::new(),
    )
    .await
}

/// Stores a remote batch while keeping record delivery independent from file-body delivery.
pub async fn store_remote_batch_with_content_policy(
    pool: &SqlitePool,
    source: RemoteBatchSource<'_>,
    latest_cursor: Option<u64>,
    events: &[EncryptedEvent],
    local_device_id: &str,
    availability: &[(String, RouteContentAvailability)],
    metadata_only_event_ids: &HashSet<String>,
) -> Result<StoreRemoteBatchResult> {
    let mut transaction = pool.begin().await.context("begin remote sync batch")?;
    let mut result = StoreRemoteBatchResult::default();
    let mut received_event_ids = Vec::new();
    let complete_content_events = events
        .iter()
        .filter(|event| {
            event.kind == SyncEventKind::Content
                && !metadata_only_event_ids.contains(&event.event_id)
        })
        .map(|event| event.event_id.clone())
        .collect::<HashSet<_>>();

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
        if inserted.inserted {
            result.inserted_event_ids.push(event.event_id.clone());
        }
        if event.origin_device_id != local_device_id {
            result.received_new_events |= inserted.inserted;
        }
    }

    let source_id = source.delivery_target();
    for event_id in &received_event_ids {
        let already_managed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sync_local_content WHERE event_id = ? AND state = 'managed') \
             OR EXISTS(SELECT 1 FROM sync_item_events WHERE event_id = ? AND direction = 'local')",
        )
        .bind(event_id)
        .bind(event_id)
        .fetch_one(&mut *transaction)
        .await
        .context("read existing local content replica")?;
        let outcome = match source {
            RemoteBatchSource::Cloud => InboundContentOutcome::Metadata,
            RemoteBatchSource::Peer { .. } if already_managed => {
                InboundContentOutcome::Deduplicated
            }
            RemoteBatchSource::Peer { .. } => InboundContentOutcome::Metadata,
        };
        record_inbound_outcome_on(&mut transaction, source, event_id, outcome).await?;
        if matches!(source, RemoteBatchSource::Peer { .. })
            && !already_managed
            && complete_content_events.contains(event_id)
        {
            expect_complete_delivery_on(&mut transaction, &source_id, event_id).await?;
        }
    }
    for (event_id, state) in availability {
        let content = match state {
            RouteContentAvailability::Metadata => ContentLevel::Metadata,
            RouteContentAvailability::Complete => ContentLevel::Complete,
        };
        match source {
            RemoteBatchSource::Cloud => {
                set_cloud_availability_on(&mut transaction, event_id, content).await?;
            }
            RemoteBatchSource::Peer { .. } => {
                confirm_delivery_on(&mut transaction, &source_id, event_id, content, false).await?;
            }
        }
    }
    match source {
        RemoteBatchSource::Cloud => {
            if let Some(latest_cursor) = latest_cursor {
                set_state_value_on(
                    &mut transaction,
                    CLOUD_CURSOR_KEY,
                    &latest_cursor.to_string(),
                )
                .await?;
            }
        }
        RemoteBatchSource::Peer { device_id } => {
            if let Some(latest_cursor) = latest_cursor {
                let now = Utc::now();
                sqlx::query(
                    "UPDATE sync_peers SET pull_cursor = ?, updated_at = ? WHERE device_id = ?",
                )
                .bind(i64::try_from(latest_cursor).unwrap_or(i64::MAX))
                .bind(now)
                .bind(device_id)
                .execute(&mut *transaction)
                .await
                .context("update peer cursor with remote sync batch")?;
            }
        }
    }

    transaction
        .commit()
        .await
        .context("commit remote sync batch")?;
    Ok(result)
}

/// Counts a real transport attempt while leaving its live phase in the transfer registry.
pub async fn record_delivery_attempt(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    set_delivery_pending(pool, target_id, event_ids, None, true).await
}

pub async fn mark_delivery_error(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    error: &str,
) -> Result<()> {
    set_delivery_pending(pool, target_id, event_ids, Some(error), false).await
}

/// Persists the scheduler's desired content level before live transfer phases begin.
pub async fn plan_deliveries(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    metadata_only_event_ids: &HashSet<String>,
) -> Result<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    let mut transaction = pool.begin().await.context("begin delivery planning")?;
    let now = Utc::now();
    for event_id in event_ids {
        let desired = if metadata_only_event_ids.contains(event_id) {
            "metadata"
        } else {
            "complete"
        };
        sqlx::query(
            r#"
            INSERT INTO sync_delivery_targets (
                event_id, target_id, desired_content, confirmed_content,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, 'none', 'pending', ?, ?)
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                desired_content = excluded.desired_content,
                status = CASE
                    WHEN sync_delivery_targets.confirmed_content = 'complete'
                        OR (
                            excluded.desired_content = 'metadata'
                            AND sync_delivery_targets.confirmed_content = 'metadata'
                        )
                        THEN 'confirmed'
                    WHEN sync_delivery_targets.status = 'awaiting_receipt'
                        THEN 'awaiting_receipt'
                    ELSE 'pending'
                END,
                last_error = NULL,
                retry_at = NULL,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(desired)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("persist sync delivery plan")?;
    }
    transaction
        .commit()
        .await
        .context("commit sync delivery plan")?;
    Ok(())
}

async fn set_delivery_pending(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    error: Option<&str>,
    increment_attempt: bool,
) -> Result<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    let mut transaction = pool.begin().await.context("begin sync state transaction")?;
    set_delivery_pending_on(
        &mut transaction,
        target_id,
        event_ids,
        error,
        increment_attempt,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("commit sync delivery states")?;
    Ok(())
}

/// Persists retry intent and errors without duplicating live transfer phases in SQLite.
async fn set_delivery_pending_on(
    connection: &mut SqliteConnection,
    target_id: &str,
    event_ids: &[String],
    error: Option<&str>,
    increment_attempt: bool,
) -> Result<()> {
    let now = Utc::now();
    for event_id in event_ids {
        sqlx::query(
            r#"
            INSERT INTO sync_delivery_targets (
                event_id, target_id, desired_content, confirmed_content,
                status, last_error, attempt_count, created_at, updated_at
            )
            SELECT ?, ?,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM sync_event_blobs WHERE event_id = ?
                   ) THEN 'complete' ELSE 'metadata' END,
                   'none', 'pending', ?, ?, ?, ?
            WHERE EXISTS (SELECT 1 FROM sync_events WHERE event_id = ?)
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                status = CASE
                    WHEN sync_delivery_targets.confirmed_content = 'complete'
                        OR (
                            sync_delivery_targets.desired_content = 'metadata'
                            AND sync_delivery_targets.confirmed_content = 'metadata'
                        )
                        THEN 'confirmed'
                    WHEN sync_delivery_targets.status = 'awaiting_receipt'
                        THEN 'awaiting_receipt'
                    ELSE 'pending'
                END,
                last_error = CASE
                    WHEN sync_delivery_targets.status = 'awaiting_receipt' THEN NULL
                    ELSE excluded.last_error
                END,
                attempt_count = sync_delivery_targets.attempt_count + excluded.attempt_count,
                retry_at = NULL,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(event_id)
        .bind(error)
        .bind(if increment_attempt { 1_i64 } else { 0_i64 })
        .bind(now)
        .bind(now)
        .bind(event_id)
        .execute(&mut *connection)
        .await
        .context("record sync delivery state")?;
    }
    Ok(())
}
