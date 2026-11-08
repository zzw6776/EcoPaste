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
    let mut transaction = pool.begin().await.context("begin cancel sync delivery")?;
    sqlx::query(
        r#"
        INSERT INTO sync_canceled_deliveries (event_id, target_id, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(event_id, target_id) DO UPDATE SET updated_at = excluded.updated_at
        "#,
    )
    .bind(event_id)
    .bind(target_id)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("cancel sync delivery")?;
    set_delivery_state_on(
        &mut transaction,
        target_id,
        &[event_id.to_owned()],
        "error",
        "uploading",
        Some("transfer canceled"),
    )
    .await?;
    transaction
        .commit()
        .await
        .context("commit canceled sync delivery")?;
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
    let mut query =
        sqlx::QueryBuilder::new("SELECT event_id FROM sync_canceled_deliveries WHERE target_id = ");
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

/// Re-enables an explicitly selected route and expands to both routes only when needed.
pub async fn resume_item_delivery(pool: &SqlitePool, item_id: &str, cloud: bool) -> Result<()> {
    let target_filter = if cloud { "cloud" } else { "peer:%" };
    let delivery_route = if cloud { "cloud" } else { "lan" };
    let mut transaction = pool.begin().await.context("begin sync delivery retry")?;
    let delete = if cloud {
        "DELETE FROM sync_canceled_deliveries WHERE target_id = ? AND event_id IN (SELECT event_id FROM sync_item_events WHERE item_id = ?)"
    } else {
        "DELETE FROM sync_canceled_deliveries WHERE target_id LIKE ? AND event_id IN (SELECT event_id FROM sync_item_events WHERE item_id = ?)"
    };
    sqlx::query(delete)
        .bind(target_filter)
        .bind(item_id)
        .execute(&mut *transaction)
        .await
        .context("resume canceled sync delivery")?;
    sqlx::query(
        r#"
        UPDATE sync_events
        SET delivery_route = CASE
                WHEN delivery_route IS NULL OR delivery_route = ? THEN delivery_route
                ELSE NULL
            END,
            updated_at = ?
        WHERE event_id IN (SELECT event_id FROM sync_item_events WHERE item_id = ?)
        "#,
    )
    .bind(delivery_route)
    .bind(Utc::now())
    .bind(item_id)
    .execute(&mut *transaction)
    .await
    .context("allow manually selected sync route")?;
    transaction
        .commit()
        .await
        .context("commit sync delivery retry")?;
    Ok(())
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
          AND NOT EXISTS (
              SELECT 1 FROM sync_canceled_deliveries canceled
              WHERE canceled.event_id = e.event_id AND canceled.target_id = ?
          )
          AND (
              e.delivery_route IS NULL
              OR e.delivery_route = CASE WHEN ? = 'cloud' THEN 'cloud' ELSE 'lan' END
          )
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(target_id)
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
          AND NOT EXISTS (
              SELECT 1 FROM sync_canceled_deliveries canceled
              WHERE canceled.event_id = e.event_id AND canceled.target_id = ?
          )
          AND (
              e.delivery_route IS NULL
              OR e.delivery_route = CASE WHEN ? = 'cloud' THEN 'cloud' ELSE 'lan' END
          )
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(origin_device_id)
    .bind(target_id)
    .bind(target_id)
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
              JOIN sync_deliveries delivery ON delivery.event_id = reference.event_id
              WHERE reference.blob_id = candidate.blob_id AND delivery.confirmed = 0
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

/// Resolves one outgoing event to the logical clipboard operation shown in transfer progress.
pub async fn transfer_item_for_event(
    pool: &SqlitePool,
    event_id: &str,
) -> Result<Option<(String, u64)>> {
    let row: Option<(String, i64)> = sqlx::query_as(
        r#"
        SELECT links.item_id,
               (SELECT COALESCE(SUM(event_blob.size), 0)
                FROM sync_event_blobs event_blob
                WHERE event_blob.event_id = links.event_id) AS operation_total_bytes
        FROM sync_item_events links
        WHERE links.event_id = ? AND links.direction = 'local'
        ORDER BY links.created_at DESC
        LIMIT 1
        "#,
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await
    .context("resolve outgoing event transfer item")?;
    row.map(|(item_id, total_bytes)| {
        Ok((
            item_id,
            u64::try_from(total_bytes).context("negative transfer operation size")?,
        ))
    })
    .transpose()
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
            INSERT INTO sync_deliveries (event_id, target_id, confirmed, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                confirmed = 1,
                updated_at = excluded.updated_at
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
        sqlx::query("DELETE FROM sync_canceled_deliveries WHERE event_id = ? AND target_id = ?")
            .bind(event_id)
            .bind(target_id)
            .execute(&mut *connection)
            .await
            .context("clear canceled sync delivery after success")?;
    }
    Ok(())
}

/// Stores received events, route acknowledgements, and an optional pull cursor atomically.
/// Inbound pushes omit the cursor so they cannot skip events in a later peer pull.
/// A crash therefore either replays the whole batch or observes every event on the next cycle.
pub async fn store_remote_batch(
    pool: &SqlitePool,
    source: RemoteBatchSource<'_>,
    latest_cursor: Option<u64>,
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
        if inserted.inserted {
            result.inserted_event_ids.push(event.event_id.clone());
        }
        if event.origin_device_id != local_device_id {
            remote_event_ids.push(event.event_id.clone());
            result.received_new_events |= inserted.inserted;
        }
    }

    mark_delivered_on(&mut transaction, &target_id, &received_event_ids).await?;
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
            // Receipt from a peer confirms only that peer route, never Hub delivery.
            set_delivery_state_on(
                &mut transaction,
                CLOUD_TARGET,
                &remote_event_ids,
                "syncing",
                "queued",
                None,
            )
            .await?;
            if let Some(latest_cursor) = latest_cursor {
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
    }

    transaction
        .commit()
        .await
        .context("commit remote sync batch")?;
    Ok(result)
}

/// Reads the one-time legacy repair cursor without moving the normal cloud receive cursor.
pub async fn cloud_confirmation_repair_cursor(
    pool: &SqlitePool,
    local_device_id: &str,
) -> Result<Option<Option<u64>>> {
    let cursor: Option<String> = sqlx::query_scalar("SELECT value FROM sync_state WHERE key = ?")
        .bind(CLOUD_CONFIRMATION_REPAIR_KEY)
        .fetch_optional(pool)
        .await?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    // The old peer-receipt shortcut never marked this device's own events as cloud-delivered.
    sqlx::query("UPDATE sync_deliveries SET confirmed = 1 WHERE target_id = 'cloud' AND confirmed = 0 AND event_id IN (SELECT event_id FROM sync_events WHERE origin_device_id = ?)")
        .bind(local_device_id).execute(pool).await?;
    let remaining: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sync_deliveries WHERE target_id = 'cloud' AND confirmed = 0)",
    )
    .fetch_one(pool)
    .await?;
    if !remaining {
        sqlx::query("DELETE FROM sync_state WHERE key = ?")
            .bind(CLOUD_CONFIRMATION_REPAIR_KEY)
            .execute(pool)
            .await?;
        return Ok(None);
    }
    Ok(Some(if cursor == "start" {
        None
    } else {
        Some(
            cursor
                .parse()
                .context("invalid legacy confirmation cursor")?,
        )
    }))
}

/// Reconciles only authenticated, byte-identical local events returned by the Hub.
pub async fn repair_cloud_confirmations(
    pool: &SqlitePool,
    events: &[EncryptedEvent],
    next_before_cursor: Option<u64>,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin cloud delivery confirmation repair")?;
    let mut confirmed = Vec::new();
    for event in events {
        let matches: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sync_events e JOIN sync_deliveries d USING(event_id) \
             WHERE d.target_id = 'cloud' AND d.confirmed = 0 AND e.event_id = ? \
             AND e.origin_device_id = ? AND e.origin_sequence = ? AND e.nonce = ? AND e.ciphertext = ? AND e.event_kind = ?)",
        )
        .bind(&event.event_id).bind(&event.origin_device_id).bind(i64::try_from(event.origin_sequence).context("invalid event origin sequence")?)
        .bind(&event.nonce).bind(&event.ciphertext).bind(event.kind.as_i64())
        .fetch_one(&mut *transaction).await?;
        if matches {
            confirmed.push(event.event_id.clone());
        }
    }
    mark_delivered_on(&mut transaction, CLOUD_TARGET, &confirmed).await?;
    if let Some(cursor) = next_before_cursor {
        sqlx::query("UPDATE sync_state SET value = ?, updated_at = ? WHERE key = ?")
            .bind(cursor.to_string())
            .bind(Utc::now())
            .bind(CLOUD_CONFIRMATION_REPAIR_KEY)
            .execute(&mut *transaction)
            .await?;
    } else {
        sqlx::query("UPDATE sync_delivery_states SET state = 'syncing', phase = 'queued', last_error = NULL, updated_at = ? WHERE target_id = 'cloud' AND event_id IN (SELECT event_id FROM sync_deliveries WHERE target_id = 'cloud' AND confirmed = 0)")
            .bind(Utc::now()).execute(&mut *transaction).await?;
        sqlx::query("DELETE FROM sync_deliveries WHERE target_id = 'cloud' AND confirmed = 0")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM sync_state WHERE key = ?")
            .bind(CLOUD_CONFIRMATION_REPAIR_KEY)
            .execute(&mut *transaction)
            .await?;
    }
    transaction
        .commit()
        .await
        .context("commit cloud delivery confirmation repair")?;
    Ok(())
}

pub async fn mark_delivery_syncing(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    set_delivery_state(pool, target_id, event_ids, "syncing", "uploading", None).await
}

pub async fn mark_delivery_queued(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    set_delivery_state(pool, target_id, event_ids, "syncing", "queued", None).await
}

pub async fn mark_delivery_confirming(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    set_delivery_state(pool, target_id, event_ids, "syncing", "confirming", None).await
}

pub async fn mark_delivery_error(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    error: &str,
) -> Result<()> {
    set_delivery_state(
        pool,
        target_id,
        event_ids,
        "error",
        "uploading",
        Some(error),
    )
    .await
}

async fn set_delivery_state(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    state: &str,
    phase: &str,
    error: Option<&str>,
) -> Result<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    let mut transaction = pool.begin().await.context("begin sync state transaction")?;
    set_delivery_state_on(&mut transaction, target_id, event_ids, state, phase, error).await?;
    transaction
        .commit()
        .await
        .context("commit sync delivery states")?;
    Ok(())
}

/// Keeps route progress and incoming-event storage inside their caller's transaction.
async fn set_delivery_state_on(
    connection: &mut SqliteConnection,
    target_id: &str,
    event_ids: &[String],
    state: &str,
    phase: &str,
    error: Option<&str>,
) -> Result<()> {
    let now = Utc::now();
    for event_id in event_ids {
        sqlx::query(
            r#"
            INSERT INTO sync_delivery_states (
                event_id, target_id, state, last_error, last_attempt_at,
                last_success_at, phase, created_at, updated_at
            ) SELECT ?, ?, ?, ?, ?, NULL, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM sync_deliveries
                WHERE event_id = ? AND target_id = ? AND confirmed = 1
            )
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                state = excluded.state,
                last_error = excluded.last_error,
                last_attempt_at = excluded.last_attempt_at,
                phase = excluded.phase,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(state)
        .bind(error)
        .bind(now)
        .bind(phase)
        .bind(now)
        .bind(now)
        .bind(event_id)
        .bind(target_id)
        .execute(&mut *connection)
        .await
        .context("record sync delivery state")?;
    }
    Ok(())
}
