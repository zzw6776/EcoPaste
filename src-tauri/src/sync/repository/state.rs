use super::*;

pub async fn cloud_window_cursor(pool: &SqlitePool) -> Result<SyncWindowCursor> {
    let synced_seq = state_value(pool, CLOUD_CURSOR_KEY)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()
        .context("invalid cloud synced_seq")?;
    let catchup_before = state_value(pool, CLOUD_CATCHUP_BEFORE_KEY)
        .await?
        .filter(|value| !value.is_empty())
        .map(|value| value.parse().context("invalid cloud catchup_before"))
        .transpose()?;
    let window_high: u64 = state_value(pool, CLOUD_WINDOW_HIGH_KEY)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()
        .context("invalid cloud window_high")?;
    Ok(SyncWindowCursor {
        synced_seq,
        catchup_before,
        window_high: window_high.max(synced_seq),
    })
}

pub async fn update_cloud_window_cursor(pool: &SqlitePool, window: SyncWindowCursor) -> Result<()> {
    let mut transaction = pool.begin().await.context("begin updating cloud window")?;
    set_state_value_on(
        &mut transaction,
        CLOUD_CURSOR_KEY,
        &window.synced_seq.to_string(),
    )
    .await?;
    set_state_value_on(
        &mut transaction,
        CLOUD_WINDOW_HIGH_KEY,
        &window.window_high.to_string(),
    )
    .await?;
    match window.catchup_before {
        Some(before) => {
            set_state_value_on(
                &mut transaction,
                CLOUD_CATCHUP_BEFORE_KEY,
                &before.to_string(),
            )
            .await?;
        }
        None => {
            sqlx::query("DELETE FROM sync_state WHERE key = ?")
                .bind(CLOUD_CATCHUP_BEFORE_KEY)
                .execute(&mut *transaction)
                .await
                .context("clear cloud catchup_before")?;
        }
    }
    transaction
        .commit()
        .await
        .context("commit cloud window update")?;
    Ok(())
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

pub async fn mark_success(pool: &SqlitePool) -> Result<()> {
    set_state_value(pool, LAST_SUCCESS_KEY, &Utc::now().to_rfc3339()).await
}

pub async fn last_success(pool: &SqlitePool) -> Result<Option<String>> {
    state_value(pool, LAST_SUCCESS_KEY).await
}

pub async fn live_sync_blob_ids(pool: &SqlitePool) -> Result<HashSet<String>> {
    let ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT blob_id FROM sync_item_blobs
        WHERE encrypted_path <> '' AND (source_path IS NULL OR stream_nonce IS NULL)
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

/// Drops non-deleted sync metadata rows whose local clipboard record was evicted and that are
/// no longer needed for outbound delivery. Tombstones stay so deletion propagates across devices.
pub async fn prune_released_items(pool: &SqlitePool, limit: u32) -> Result<u64> {
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        SELECT s.sync_key
        FROM sync_items s
        WHERE s.item_id IS NULL
          AND s.deleted = 0
          AND s.is_present = 1
          AND s.prep_state = 'ready'
          AND s.needs_apply = 0
          AND s.needs_blob_pull = 0
        ORDER BY s.updated_at ASC
        LIMIT ?
        "#,
    )
    .bind(i64::from(limit.max(1)))
    .fetch_all(pool)
    .await
    .context("list released sync items")?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut query = sqlx::QueryBuilder::new("DELETE FROM sync_items WHERE sync_key IN (");
    let mut separated = query.separated(", ");
    for sync_key in &rows {
        separated.push_bind(sync_key);
    }
    query.push(")");
    let result = query
        .build()
        .execute(pool)
        .await
        .context("delete released sync items")?;
    Ok(result.rows_affected())
}

pub async fn status_counts(
    pool: &SqlitePool,
    include_sensitive: bool,
    cloud_enabled: bool,
    online_peer_ids: &[String],
) -> Result<(u64, u64, u64)> {
    let mut pending_query = sqlx::QueryBuilder::new(
        r#"
        SELECT COUNT(*) FROM sync_items s
        WHERE (s.prep_state <> 'ready' AND (
        "#,
    );
    pending_query
        .push_bind(include_sensitive)
        .push(" OR s.is_sensitive = 0 OR s.manual_share = 1))");

    if cloud_enabled {
        pending_query.push(
            r#"
            OR (
                s.prep_state = 'ready'
                AND s.nonce IS NOT NULL
                AND (
                    (
                        s.local_seq > 0
                        AND NOT EXISTS (
                            SELECT 1 FROM sync_item_targets t
                            WHERE t.sync_key = s.sync_key
                              AND t.target_id = 'cloud'
                              AND t.pushed_local_seq >= s.local_seq
                        )
                    )
                    OR (
                        s.deleted = 0
                        AND (
                            (s.origin = 'local' AND s.cloud_blob_policy IN ('auto', 'requested'))
                            OR s.cloud_blob_policy = 'requested'
                        )
                        AND EXISTS (
                            SELECT 1 FROM sync_item_blobs b
                            WHERE b.sync_key = s.sync_key
                              AND b.optional = 0
                              AND b.cloud_available = 0
                              AND b.local_available = 1
                        )
                    )
                )
            )
            "#,
        );
    }

    if !online_peer_ids.is_empty() {
        pending_query.push(
            r#"
            OR (
                s.prep_state = 'ready'
                AND s.nonce IS NOT NULL
                AND s.local_seq > 0
                AND EXISTS (
                    SELECT 1 FROM sync_peers p
                    LEFT JOIN sync_item_targets t
                      ON t.sync_key = s.sync_key AND t.target_id = ('peer:' || p.device_id)
                    WHERE p.device_id IN (
            "#,
        );
        let mut separated = pending_query.separated(", ");
        for peer_id in online_peer_ids {
            separated.push_bind(peer_id);
        }
        pending_query.push(
            r#"
                    )
                      AND (t.target_id IS NULL OR t.pushed_local_seq < s.local_seq)
                )
            )
            "#,
        );
    }

    let pending: i64 = pending_query
        .build_query_scalar()
        .fetch_one(pool)
        .await
        .context("count pending sync items")?;

    let manual: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(DISTINCT s.item_id)
        FROM sync_items s
        JOIN sync_item_targets t ON t.sync_key = s.sync_key
        WHERE s.origin = 'local'
          AND s.item_id IS NOT NULL
          AND s.cloud_blob_policy = 'defer'
          AND t.target_id = 'cloud'
          AND t.confirmed_content = 'metadata'
          AND EXISTS (
              SELECT 1 FROM sync_item_blobs b
              WHERE b.sync_key = s.sync_key AND b.optional = 0 AND b.cloud_available = 0
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

/// Computes aggregate automatic history backfill progress without exposing per-item details.
/// Returns `None` once all staged historical backfill items have converged on the active routes.
pub async fn backfill_progress(
    pool: &SqlitePool,
    include_sensitive: bool,
    cloud_enabled: bool,
    online_peer_ids: &[String],
) -> Result<Option<crate::sync::model::SyncBackfillProgress>> {
    if !cloud_enabled && online_peer_ids.is_empty() {
        return Ok(None);
    }
    let total: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM sync_items s
        WHERE s.history_backfill = 1
          AND s.manual_share = 0
          AND s.deleted = 0
          AND (? OR s.is_sensitive = 0)
        "#,
    )
    .bind(include_sensitive)
    .fetch_one(pool)
    .await
    .context("count automatic history backfill items")?;
    if total <= 0 {
        return Ok(None);
    }

    let remaining: i64 = if cloud_enabled {
        sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM sync_items s
            LEFT JOIN sync_item_targets t
              ON t.sync_key = s.sync_key AND t.target_id = 'cloud'
            WHERE s.history_backfill = 1
              AND s.manual_share = 0
              AND s.deleted = 0
              AND (? OR s.is_sensitive = 0)
              AND (
                  s.prep_state <> 'ready'
                  OR (
                      s.nonce IS NOT NULL
                      AND s.local_seq > 0
                      AND (t.target_id IS NULL OR t.pushed_local_seq < s.local_seq)
                  )
              )
            "#,
        )
        .bind(include_sensitive)
        .fetch_one(pool)
        .await
        .context("count remaining cloud history backfill items")?
    } else {
        let mut query = sqlx::QueryBuilder::new(
            r#"
            SELECT COUNT(*)
            FROM sync_items s
            WHERE s.history_backfill = 1
              AND s.manual_share = 0
              AND s.deleted = 0
              AND (
            "#,
        );
        query.push_bind(include_sensitive).push(
            r#"
                 OR s.is_sensitive = 0)
              AND (
                  s.prep_state <> 'ready'
                  OR (
                      s.nonce IS NOT NULL
                      AND s.local_seq > 0
                      AND EXISTS (
                          SELECT 1
                          FROM sync_peers p
                          LEFT JOIN sync_item_targets t
                            ON t.sync_key = s.sync_key AND t.target_id = ('peer:' || p.device_id)
                          WHERE p.device_id IN (
            "#,
        );
        let mut separated = query.separated(", ");
        for peer_id in online_peer_ids {
            separated.push_bind(peer_id);
        }
        query.push(
            r#"
                          )
                            AND (t.target_id IS NULL OR t.pushed_local_seq < s.local_seq)
                      )
                  )
              )
            "#,
        );
        query
            .build_query_scalar()
            .fetch_one(pool)
            .await
            .context("count remaining LAN history backfill items")?
    };

    if remaining <= 0 {
        return Ok(None);
    }
    let total_u64 = total as u64;
    let remaining_u64 = (remaining as u64).min(total_u64);
    Ok(Some(crate::sync::model::SyncBackfillProgress {
        completed: total_u64.saturating_sub(remaining_u64),
        total: total_u64,
    }))
}

/// Loads device-local external download destinations for the requested items.
pub async fn external_download_sessions(
    pool: &SqlitePool,
    item_ids: &[String],
) -> Result<Vec<ExternalDownloadSession>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT item_id, external_destinations_json FROM sync_items \
         WHERE local_state IN ('external', 'external_missing') \
           AND external_destinations_json IS NOT NULL \
           AND item_id IN (",
    );
    let mut ids = query.separated(", ");
    for item_id in item_ids {
        ids.push_bind(item_id);
    }
    query.push(")");
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("list external download receipts")?;
    let mut sessions = Vec::with_capacity(rows.len());
    for row in rows {
        let item_id: String = row.try_get("item_id")?;
        let json: String = row.try_get("external_destinations_json")?;
        let targets = serde_json::from_str::<Vec<ExternalDownloadTarget>>(&json)
            .context("decode external download destinations")?;
        sessions.push(ExternalDownloadSession { item_id, targets });
    }
    Ok(sessions)
}

/// Projects durable per-item sync state into the frontend card status contract in O(N) batch queries.
pub async fn item_statuses(
    pool: &SqlitePool,
    item_ids: &[String],
    cloud_enabled: bool,
    online_peer_ids: &[String],
    external_states: &HashMap<String, SyncLocalContentState>,
) -> Result<Vec<SyncItemStatus>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut facts = item_ids
        .iter()
        .map(|id| {
            (
                id.clone(),
                ItemStatusFacts {
                    item_id: id.clone(),
                    origin: SyncItemOrigin::Local,
                    local_state: external_states
                        .get(id)
                        .copied()
                        .unwrap_or(SyncLocalContentState::Managed),
                    local_seq: 0,
                    prep_pending: false,
                    prep_error: None,
                    cloud_blob_policy: crate::db::sync_items::CloudBlobPolicy::Auto,
                    has_pending_cloud_blobs: false,
                    cloud_enabled,
                    online_peer_ids: online_peer_ids.to_vec(),
                    remote_file_available: false,
                    deliveries: Vec::new(),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    let mut item_query = sqlx::QueryBuilder::new(
        "SELECT sync_key, item_id, origin, local_state, local_seq, prep_state, prep_error, \
                cloud_blob_policy, remote_file_state, deleted, (nonce IS NOT NULL) AS has_snapshot \
         FROM sync_items WHERE item_id IN (",
    );
    let mut separated = item_query.separated(", ");
    for item_id in item_ids {
        separated.push_bind(item_id);
    }
    item_query.push(")");

    let mut sync_key_to_item_id = HashMap::new();
    let mut active_sync_keys = Vec::new();
    for row in item_query.build().fetch_all(pool).await? {
        let sync_key: String = row.try_get("sync_key")?;
        let item_id: String = row.try_get("item_id")?;
        let origin: String = row.try_get("origin")?;
        let local_state: String = row.try_get("local_state")?;
        let local_seq: i64 = row.try_get("local_seq")?;
        let prep_state: String = row.try_get("prep_state")?;
        let prep_error: Option<String> = row.try_get("prep_error")?;
        let cloud_blob_policy: String = row.try_get("cloud_blob_policy")?;
        let remote_file_state: Option<String> = row.try_get("remote_file_state")?;
        let deleted: bool = row.try_get("deleted")?;
        let has_snapshot: bool = row.try_get("has_snapshot")?;

        sync_key_to_item_id.insert(sync_key.clone(), item_id.clone());
        if !deleted {
            active_sync_keys.push(sync_key.clone());
        }
        let Some(item_facts) = facts.get_mut(&item_id) else {
            continue;
        };
        let is_local = origin == "local";
        item_facts.origin = if is_local {
            SyncItemOrigin::Local
        } else {
            SyncItemOrigin::Remote
        };
        item_facts.local_state =
            external_states
                .get(&item_id)
                .copied()
                .unwrap_or(match local_state.as_str() {
                    "external" => SyncLocalContentState::External,
                    "external_missing" => SyncLocalContentState::ExternalMissing,
                    "managed" => SyncLocalContentState::Managed,
                    _ if is_local => SyncLocalContentState::Managed,
                    _ => SyncLocalContentState::Missing,
                });
        item_facts.cloud_blob_policy =
            crate::db::sync_items::CloudBlobPolicy::parse(&cloud_blob_policy);
        item_facts.remote_file_available = remote_file_state.as_deref() == Some("available");
        if prep_state != "ready" {
            item_facts.prep_pending = true;
            item_facts.prep_error = prep_error;
            item_facts.local_seq = local_seq;
        } else {
            item_facts.local_seq = if has_snapshot { local_seq } else { 0 };
        }
    }

    if !active_sync_keys.is_empty() {
        let mut blob_query = sqlx::QueryBuilder::new(
            "SELECT DISTINCT sync_key FROM sync_item_blobs \
             WHERE optional = 0 AND cloud_available = 0 AND local_available = 1 AND sync_key IN (",
        );
        let mut separated = blob_query.separated(", ");
        for sync_key in &active_sync_keys {
            separated.push_bind(sync_key);
        }
        blob_query.push(")");
        let pending_blob_keys: Vec<String> = blob_query
            .build_query_scalar()
            .fetch_all(pool)
            .await
            .context("query items with pending cloud blobs")?;
        for sync_key in pending_blob_keys {
            if let Some(item_id) = sync_key_to_item_id.get(&sync_key) {
                if let Some(item_facts) = facts.get_mut(item_id) {
                    item_facts.has_pending_cloud_blobs = true;
                }
            }
        }
    }

    if !sync_key_to_item_id.is_empty() {
        let mut target_query = sqlx::QueryBuilder::new(
            "SELECT sync_key, target_id, confirmed_content, pushed_local_seq, last_error \
             FROM sync_item_targets WHERE sync_key IN (",
        );
        let mut separated = target_query.separated(", ");
        for sync_key in sync_key_to_item_id.keys() {
            separated.push_bind(sync_key);
        }
        target_query.push(")");

        for row in target_query.build().fetch_all(pool).await? {
            let sync_key: String = row.try_get("sync_key")?;
            let target_id: String = row.try_get("target_id")?;
            let confirmed = ContentLevel::parse(&row.try_get::<String, _>("confirmed_content")?);
            let pushed_local_seq: i64 = row.try_get("pushed_local_seq")?;
            let last_error: Option<String> = row.try_get("last_error")?;
            if let Some(item_id) = sync_key_to_item_id.get(&sync_key) {
                if let Some(item_facts) = facts.get_mut(item_id) {
                    item_facts.deliveries.push(TargetFact {
                        target_id,
                        confirmed,
                        pushed_local_seq,
                        last_error,
                    });
                }
            }
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
    // Remove un-downloaded remote file placeholders before dropping sync_items.
    sqlx::query(
        "DELETE FROM clipboard_items \
         WHERE id IN (SELECT item_id FROM sync_items WHERE remote_file_state IS NOT NULL)",
    )
    .execute(&mut *transaction)
    .await
    .context("remove remote file placeholders before clearing sync state")?;
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
        "DELETE FROM clipboard_items \
         WHERE id IN (SELECT item_id FROM sync_items WHERE remote_file_state IS NOT NULL)",
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
        "DELETE FROM sync_item_targets",
        "DELETE FROM sync_item_blobs",
        "DELETE FROM sync_items",
        "DELETE FROM sync_content_objects",
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

fn rewrite_resource_path(
    raw: &str,
    old_resources: &std::path::Path,
    new_resources: &std::path::Path,
) -> Option<String> {
    let relative = std::path::Path::new(raw).strip_prefix(old_resources).ok()?;
    Some(new_resources.join(relative).to_string_lossy().into_owned())
}

/// Rewrites queued sync blob and source-path prefixes after moving the local storage directory.
pub async fn rebase_sync_resource_paths(
    pool: &SqlitePool,
    old_resources: &std::path::Path,
    new_resources: &std::path::Path,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin rebasing sync resource paths")?;
    let now = Utc::now();
    for (select_sql, update_sql) in [
        (
            "SELECT rowid, encrypted_path, source_path FROM sync_item_blobs",
            "UPDATE sync_item_blobs SET encrypted_path = ?, source_path = ?, updated_at = ? WHERE rowid = ?",
        ),
        (
            "SELECT rowid, encrypted_path, source_path FROM sync_source_icon_uploads",
            "UPDATE sync_source_icon_uploads SET encrypted_path = ?, source_path = ?, updated_at = ? WHERE rowid = ?",
        ),
    ] {
        let rows = sqlx::query_as::<_, (i64, String, Option<String>)>(select_sql)
            .fetch_all(&mut *transaction)
            .await
            .context("list queued sync resource paths")?;
        for (rowid, encrypted_path, source_path) in rows {
            let next_encrypted =
                rewrite_resource_path(&encrypted_path, old_resources, new_resources);
            let next_source = source_path
                .as_deref()
                .and_then(|value| rewrite_resource_path(value, old_resources, new_resources));
            if next_encrypted.is_none() && next_source.is_none() {
                continue;
            }
            sqlx::query(update_sql)
                .bind(next_encrypted.as_deref().unwrap_or(&encrypted_path))
                .bind(next_source.as_deref().or(source_path.as_deref()))
                .bind(now)
                .bind(rowid)
                .execute(&mut *transaction)
                .await
                .context("rebase queued sync resource paths")?;
        }
    }
    transaction
        .commit()
        .await
        .context("commit rebasing sync resource paths")?;
    Ok(())
}
