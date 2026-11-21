use super::*;
use crate::sync::model::{FieldVersion, SyncedItemSnapshot, SNAPSHOT_VERSION};
use ecopaste_sync_protocol::{
    AcceptedItem, LanContentLevel, LanItemAck, SyncItemRecord, SyncItemWrite,
};

#[derive(Debug, Clone)]
pub struct SyncItemBlobRow {
    pub blob_id: String,
    pub role: String,
    pub optional: bool,
    pub size: u64,
    pub original_size: u64,
    pub encrypted_path: String,
    pub source_path: Option<String>,
    pub source_is_directory: bool,
    pub stream_nonce: Option<Vec<u8>>,
    pub stream_commitment: bool,
    pub cloud_available: bool,
    pub local_available: bool,
}

impl SyncItemBlobRow {
    pub fn to_stored_blob(&self) -> StoredBlob {
        let encrypted_path = if !self.stream_commitment && self.encrypted_path.is_empty() {
            super::super::manager::SYNC_STAGING_DIR.to_owned()
        } else {
            self.encrypted_path.clone()
        };
        StoredBlob {
            blob_id: self.blob_id.clone(),
            encrypted_path,
            size: self.size,
            source_path: self.source_path.clone(),
            source_is_directory: self.source_is_directory,
            stream_nonce: self.stream_nonce.clone(),
        }
    }
}

const BACKFILL_CONTROL_BATCH_LIMIT: u16 = 64;

#[derive(Debug, Clone)]
pub struct PendingCloudBlobUpload {
    pub sync_key: String,
    pub item_id: Option<String>,
    pub kind: String,
    pub history_backfill: bool,
    pub manual_share: bool,
    pub payload_updated_at_ms: i64,
    pub blobs: Vec<SyncItemBlobRow>,
}

#[derive(Debug, Clone)]
pub struct PendingOutboundBatch {
    pub writes: Vec<SyncItemWrite>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct StoredSyncItemMeta {
    pub sync_key: String,
    pub item_key: Option<String>,
    pub item_id: Option<String>,
    pub origin_device_id: Option<String>,
    pub content_identity: Option<String>,
    pub deleted: bool,
    pub protected: bool,
    pub content_version: FieldVersion,
    pub presence_version: FieldVersion,
    pub favorite_version: FieldVersion,
    pub pinned_version: FieldVersion,
    pub note_version: FieldVersion,
    pub is_present: bool,
    pub is_favorite: bool,
    pub is_pinned: bool,
    pub note: Option<String>,
    pub payload_updated_at_ms: i64,
    pub prep_state: String,
    pub nonce: Option<Vec<u8>>,
    pub ciphertext: Option<Vec<u8>>,
    pub local_state: String,
    pub remote_file_state: Option<String>,
    pub needs_blob_pull: bool,
}

pub async fn lock_transfer_item(pool: &SqlitePool, task_id: &str, item_id: &str) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_active_transfers (task_id, item_id, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(task_id) DO UPDATE SET item_id = excluded.item_id, updated_at = excluded.updated_at
        "#,
    )
    .bind(task_id)
    .bind(item_id)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("lock active sync transfer item")?;
    Ok(())
}

pub async fn unlock_transfer_item(pool: &SqlitePool, task_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM sync_active_transfers WHERE task_id = ?")
        .bind(task_id)
        .execute(pool)
        .await
        .context("unlock active sync transfer item")?;
    Ok(())
}

pub async fn clear_stale_transfer_locks(pool: &SqlitePool) -> Result<()> {
    sqlx::query("DELETE FROM sync_active_transfers")
        .execute(pool)
        .await
        .context("clear stale sync transfer locks")?;
    Ok(())
}

pub async fn current_origin_sequence(pool: &SqlitePool) -> Result<Option<u64>> {
    state_value(pool, "origin_sequence")
        .await?
        .map(|value| value.parse().context("invalid sync origin sequence"))
        .transpose()
}

pub async fn reusable_content_object(
    pool: &SqlitePool,
    group_id: &str,
    plaintext_hash: &str,
) -> Result<Option<StoredContentObject>> {
    let row = sqlx::query(
        r#"
        SELECT plaintext_hash, blob_id, stream_nonce, original_size, encrypted_size
        FROM sync_content_objects
        WHERE group_id = ? AND plaintext_hash = ?
        "#,
    )
    .bind(group_id)
    .bind(plaintext_hash)
    .fetch_optional(pool)
    .await
    .context("read reusable sync content object")?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(StoredContentObject {
        plaintext_hash: row.try_get("plaintext_hash")?,
        blob_id: row.try_get("blob_id")?,
        stream_nonce: row.try_get("stream_nonce")?,
        original_size: u64::try_from(row.try_get::<i64, _>("original_size")?)
            .context("negative content object original size")?,
        encrypted_size: u64::try_from(row.try_get::<i64, _>("encrypted_size")?)
            .context("negative content object encrypted size")?,
    }))
}

/// Loads one `sync_items` row with full CRDT version vectors and optional encrypted payload.
pub async fn load_sync_item_meta(
    pool: &SqlitePool,
    sync_key: &str,
) -> Result<Option<StoredSyncItemMeta>> {
    let row = sqlx::query(
        r#"
        SELECT sync_key, item_key, item_id, origin, origin_device_id, source_revision,
               content_identity, kind, is_sensitive, deleted, protected,
               content_seq, content_device_id, presence_seq, presence_device_id,
               favorite_seq, favorite_device_id, pinned_seq, pinned_device_id,
               note_seq, note_device_id, is_present, is_favorite, is_pinned, note,
               local_seq, hub_rev, payload_updated_at_ms, prep_state, history_backfill,
               manual_share, nonce, ciphertext, local_state, remote_file_state,
               needs_blob_pull
        FROM sync_items
        WHERE sync_key = ?
        "#,
    )
    .bind(sync_key)
    .fetch_optional(pool)
    .await
    .context("load sync_items metadata")?;
    row.map(meta_from_row).transpose()
}

pub async fn load_sync_item_meta_by_item_key(
    pool: &SqlitePool,
    item_key: &str,
) -> Result<Option<StoredSyncItemMeta>> {
    let row = sqlx::query(
        r#"
        SELECT sync_key, item_key, item_id, origin, origin_device_id, source_revision,
               content_identity, kind, is_sensitive, deleted, protected,
               content_seq, content_device_id, presence_seq, presence_device_id,
               favorite_seq, favorite_device_id, pinned_seq, pinned_device_id,
               note_seq, note_device_id, is_present, is_favorite, is_pinned, note,
               local_seq, hub_rev, payload_updated_at_ms, prep_state, history_backfill,
               manual_share, nonce, ciphertext, local_state, remote_file_state,
               needs_blob_pull
        FROM sync_items
        WHERE item_key = ?
        "#,
    )
    .bind(item_key)
    .fetch_optional(pool)
    .await
    .context("load sync_items metadata by item_key")?;
    row.map(meta_from_row).transpose()
}

pub async fn load_sync_item_meta_by_item_id(
    pool: &SqlitePool,
    item_id: &str,
) -> Result<Option<StoredSyncItemMeta>> {
    let row = sqlx::query(
        r#"
        SELECT sync_key, item_key, item_id, origin, origin_device_id, source_revision,
               content_identity, kind, is_sensitive, deleted, protected,
               content_seq, content_device_id, presence_seq, presence_device_id,
               favorite_seq, favorite_device_id, pinned_seq, pinned_device_id,
               note_seq, note_device_id, is_present, is_favorite, is_pinned, note,
               local_seq, hub_rev, payload_updated_at_ms, prep_state, history_backfill,
               manual_share, nonce, ciphertext, local_state, remote_file_state,
               needs_blob_pull
        FROM sync_items
        WHERE item_id = ?
        "#,
    )
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .context("load sync_items metadata by item_id")?;
    row.map(meta_from_row).transpose()
}

fn meta_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredSyncItemMeta> {
    Ok(StoredSyncItemMeta {
        sync_key: row.try_get("sync_key")?,
        item_key: row.try_get("item_key")?,
        item_id: row.try_get("item_id")?,
        origin_device_id: row.try_get("origin_device_id")?,
        content_identity: row.try_get("content_identity")?,
        deleted: row.try_get("deleted")?,
        protected: row.try_get("protected")?,
        content_version: FieldVersion::new(
            u64::try_from(row.try_get::<i64, _>("content_seq")?).unwrap_or(0),
            row.try_get::<String, _>("content_device_id")?,
        ),
        presence_version: FieldVersion::new(
            u64::try_from(row.try_get::<i64, _>("presence_seq")?).unwrap_or(0),
            row.try_get::<String, _>("presence_device_id")?,
        ),
        favorite_version: FieldVersion::new(
            u64::try_from(row.try_get::<i64, _>("favorite_seq")?).unwrap_or(0),
            row.try_get::<String, _>("favorite_device_id")?,
        ),
        pinned_version: FieldVersion::new(
            u64::try_from(row.try_get::<i64, _>("pinned_seq")?).unwrap_or(0),
            row.try_get::<String, _>("pinned_device_id")?,
        ),
        note_version: FieldVersion::new(
            u64::try_from(row.try_get::<i64, _>("note_seq")?).unwrap_or(0),
            row.try_get::<String, _>("note_device_id")?,
        ),
        is_present: row.try_get("is_present")?,
        is_favorite: row.try_get("is_favorite")?,
        is_pinned: row.try_get("is_pinned")?,
        note: row.try_get("note")?,
        payload_updated_at_ms: row.try_get("payload_updated_at_ms")?,
        prep_state: row.try_get("prep_state")?,
        nonce: row.try_get("nonce")?,
        ciphertext: row.try_get("ciphertext")?,
        local_state: row.try_get("local_state")?,
        remote_file_state: row.try_get("remote_file_state")?,
        needs_blob_pull: row.try_get("needs_blob_pull")?,
    })
}

/// Parameters for atomically committing one encrypted per-item snapshot to `sync_items`.
pub struct PreparedItemCommit<'a> {
    pub sync_key: &'a str,
    pub item_key: &'a str,
    pub item_id: Option<&'a str>,
    pub expected_local_seq: i64,
    pub origin_device_id: &'a str,
    pub content_identity: Option<&'a str>,
    pub group_id: &'a str,
    pub snapshot: &'a SyncedItemSnapshot,
    pub nonce: &'a [u8],
    pub ciphertext: &'a [u8],
    pub blobs: &'a [SyncItemBlobRow],
    pub content_objects: &'a [StoredContentObject],
    /// `Some(policy)` sets/updates the cloud attachment upload policy; `None` preserves existing
    /// policy (used by metadata-only edits on deferred large files).
    pub cloud_blob_policy: Option<crate::db::sync_items::CloudBlobPolicy>,
}

/// Commits an encrypted local snapshot if `local_seq` still matches, returning `true` when committed.
pub async fn commit_prepared_item(
    pool: &SqlitePool,
    commit: PreparedItemCommit<'_>,
) -> Result<bool> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin committing prepared sync item")?;
    let current_seq: Option<i64> =
        sqlx::query_scalar("SELECT local_seq FROM sync_items WHERE sync_key = ?")
            .bind(commit.sync_key)
            .fetch_optional(&mut *transaction)
            .await
            .context("check sync item local_seq before commit")?;
    let Some(current_seq) = current_seq else {
        transaction.rollback().await.ok();
        return Ok(false);
    };
    if current_seq != commit.expected_local_seq {
        transaction.rollback().await.ok();
        return Ok(false);
    }

    let now = Utc::now();
    let snapshot = commit.snapshot;
    let deleted = snapshot.deleted();
    let protected = snapshot.protected();
    let snapshot_kind = snapshot.item.as_ref().map(|item| item.kind.as_str());
    let payload_updated_at_ms = snapshot.updated_at_ms();
    let requested_policy = commit
        .cloud_blob_policy
        .map(crate::db::sync_items::CloudBlobPolicy::as_str);
    sqlx::query(
        r#"
        UPDATE sync_items
        SET item_key = ?,
            item_id = COALESCE(?, item_id),
            origin_device_id = ?,
            content_identity = COALESCE(?, content_identity),
            kind = COALESCE(?, kind),
            deleted = ?,
            protected = ?,
            content_seq = ?, content_device_id = ?,
            presence_seq = ?, presence_device_id = ?,
            favorite_seq = ?, favorite_device_id = ?,
            pinned_seq = ?, pinned_device_id = ?,
            note_seq = ?, note_device_id = ?,
            is_present = ?, is_favorite = ?, is_pinned = ?, note = ?,
            payload_updated_at_ms = CASE WHEN ? > 0 THEN MAX(payload_updated_at_ms, ?) ELSE payload_updated_at_ms END,
            cloud_blob_policy = COALESCE(?, cloud_blob_policy),
            prep_state = 'ready',
            prep_error = NULL,
            prep_attempts = 0,
            next_prep_at = NULL,
            nonce = ?,
            ciphertext = ?,
            needs_blob_pull = CASE WHEN ? = 1 THEN 0 ELSE needs_blob_pull END,
            updated_at = ?
        WHERE sync_key = ?
        "#,
    )
    .bind(commit.item_key)
    .bind(commit.item_id)
    .bind(commit.origin_device_id)
    .bind(commit.content_identity)
    .bind(snapshot_kind)
    .bind(deleted)
    .bind(protected)
    .bind(i64::try_from(snapshot.content_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.content_version.device_id)
    .bind(i64::try_from(snapshot.presence_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.presence_version.device_id)
    .bind(i64::try_from(snapshot.favorite_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.favorite_version.device_id)
    .bind(i64::try_from(snapshot.pinned_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.pinned_version.device_id)
    .bind(i64::try_from(snapshot.note_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.note_version.device_id)
    .bind(snapshot.is_present)
    .bind(snapshot.is_favorite)
    .bind(snapshot.is_pinned)
    .bind(snapshot.note.as_deref())
    .bind(payload_updated_at_ms)
    .bind(payload_updated_at_ms)
    .bind(requested_policy)
    .bind(commit.nonce)
    .bind(commit.ciphertext)
    .bind(deleted)
    .bind(now)
    .bind(commit.sync_key)
    .execute(&mut *transaction)
    .await
    .context("commit encrypted sync item snapshot")?;

    // Preserve any existing `cloud_available` flags for unchanged blob_ids.
    let existing_cloud_blobs: HashSet<String> = sqlx::query_scalar(
        "SELECT blob_id FROM sync_item_blobs WHERE sync_key = ? AND cloud_available = 1",
    )
    .bind(commit.sync_key)
    .fetch_all(&mut *transaction)
    .await
    .context("read existing cloud blob availability")?
    .into_iter()
    .collect();

    sqlx::query("DELETE FROM sync_item_blobs WHERE sync_key = ?")
        .bind(commit.sync_key)
        .execute(&mut *transaction)
        .await
        .context("replace sync item blobs")?;
    for blob in commit.blobs {
        let cloud_available = blob.cloud_available || existing_cloud_blobs.contains(&blob.blob_id);
        sqlx::query(
            r#"
            INSERT INTO sync_item_blobs (
                sync_key, blob_id, role, optional, size, original_size,
                encrypted_path, source_path, source_is_directory, stream_nonce,
                stream_commitment, cloud_available, local_available, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(commit.sync_key)
        .bind(&blob.blob_id)
        .bind(&blob.role)
        .bind(blob.optional)
        .bind(i64::try_from(blob.size).unwrap_or(i64::MAX))
        .bind(i64::try_from(blob.original_size).unwrap_or(i64::MAX))
        .bind(&blob.encrypted_path)
        .bind(blob.source_path.as_deref())
        .bind(blob.source_is_directory)
        .bind(blob.stream_nonce.as_deref())
        .bind(blob.stream_commitment)
        .bind(cloud_available)
        .bind(blob.local_available)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("insert sync item blob row")?;
    }

    for object in commit.content_objects {
        sqlx::query(
            r#"
            INSERT INTO sync_content_objects (
                group_id, plaintext_hash, blob_id, stream_nonce,
                original_size, encrypted_size, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, plaintext_hash) DO UPDATE SET
                blob_id = excluded.blob_id,
                stream_nonce = excluded.stream_nonce,
                original_size = excluded.original_size,
                encrypted_size = excluded.encrypted_size,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(commit.group_id)
        .bind(&object.plaintext_hash)
        .bind(&object.blob_id)
        .bind(&object.stream_nonce)
        .bind(i64::try_from(object.original_size).unwrap_or(i64::MAX))
        .bind(i64::try_from(object.encrypted_size).unwrap_or(i64::MAX))
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("upsert sync content object")?;
    }

    // Do not pre-insert synthetic `cloud` or `peer:*` rows: pending outbound push is derived
    // mathematically from `s.local_seq > COALESCE(t.pushed_local_seq, 0)`. Only clear stale
    // errors on existing target rows when a fresh capture or explicit retry lands.
    if commit.cloud_blob_policy == Some(crate::db::sync_items::CloudBlobPolicy::Requested) {
        sqlx::query(
            "UPDATE sync_item_targets SET last_error = NULL, updated_at = ? \
             WHERE sync_key = ? AND target_id = 'cloud' AND last_error IS NOT NULL",
        )
        .bind(now)
        .bind(commit.sync_key)
        .execute(&mut *transaction)
        .await
        .context("clear stale cloud error for explicitly requested sync item")?;
    }

    sqlx::query(
        "UPDATE sync_item_targets SET last_error = NULL, updated_at = ? \
         WHERE sync_key = ? AND target_id LIKE 'peer:%' AND last_error IS NOT NULL",
    )
    .bind(now)
    .bind(commit.sync_key)
    .execute(&mut *transaction)
    .await
    .context("clear stale peer errors for updated sync item")?;

    transaction
        .commit()
        .await
        .context("commit prepared sync item")?;
    Ok(true)
}

/// Selects the highest-priority pending outbound item writes for `target_id` (Cloud or a LAN peer).
///
/// Realtime/manual items (`history_backfill = 0 || manual_share = 1`) are always dispatched in
/// their own dedicated batch without piggybacking historical backfill so fresh copies finish in
/// one fast round-trip; backfill items run in bounded 64-item slices for fast preemption.
pub async fn pending_outbound_writes(
    pool: &SqlitePool,
    target_id: &str,
    limit: u16,
) -> Result<PendingOutboundBatch> {
    let is_cloud = target_id == CLOUD_TARGET;
    let effective_limit = limit.max(1);
    let realtime_fetch_limit = i64::from(effective_limit) + 1;

    let mut rows = sqlx::query(
        r#"
        SELECT s.sync_key, s.item_key, s.hub_rev, s.local_seq,
               COALESCE(s.origin_device_id, '') AS origin_device_id,
               s.payload_updated_at_ms, s.deleted, s.protected,
               s.nonce, s.ciphertext
        FROM sync_items s
        LEFT JOIN sync_item_targets t
          ON t.sync_key = s.sync_key AND t.target_id = ?
        WHERE s.prep_state = 'ready'
          AND s.local_seq > 0
          AND s.item_key IS NOT NULL
          AND s.nonce IS NOT NULL
          AND s.ciphertext IS NOT NULL
          AND (s.history_backfill = 0 OR s.manual_share = 1)
          AND (t.target_id IS NULL OR t.pushed_local_seq < s.local_seq)
        ORDER BY s.manual_share DESC,
                 s.payload_updated_at_ms DESC,
                 s.local_seq DESC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(realtime_fetch_limit)
    .fetch_all(pool)
    .await
    .context("query pending realtime outbound sync item writes")?;

    let has_more = if !rows.is_empty() {
        let truncated = rows.len() > usize::from(effective_limit);
        rows.truncate(usize::from(effective_limit));
        if truncated {
            true
        } else {
            sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM sync_items s
                    LEFT JOIN sync_item_targets t
                      ON t.sync_key = s.sync_key AND t.target_id = ?
                    WHERE s.prep_state = 'ready'
                      AND s.local_seq > 0
                      AND s.item_key IS NOT NULL
                      AND s.nonce IS NOT NULL
                      AND s.ciphertext IS NOT NULL
                      AND s.history_backfill = 1
                      AND s.manual_share = 0
                      AND (t.target_id IS NULL OR t.pushed_local_seq < s.local_seq)
                )
                "#,
            )
            .bind(target_id)
            .fetch_one(pool)
            .await
            .context("check remaining backfill outbound writes")?
        }
    } else {
        let backfill_limit = effective_limit.min(BACKFILL_CONTROL_BATCH_LIMIT);
        let backfill_fetch_limit = i64::from(backfill_limit) + 1;
        rows = sqlx::query(
            r#"
            SELECT s.sync_key, s.item_key, s.hub_rev, s.local_seq,
                   COALESCE(s.origin_device_id, '') AS origin_device_id,
                   s.payload_updated_at_ms, s.deleted, s.protected,
                   s.nonce, s.ciphertext
            FROM sync_items s
            LEFT JOIN sync_item_targets t
              ON t.sync_key = s.sync_key AND t.target_id = ?
            WHERE s.prep_state = 'ready'
              AND s.local_seq > 0
              AND s.item_key IS NOT NULL
              AND s.nonce IS NOT NULL
              AND s.ciphertext IS NOT NULL
              AND s.history_backfill = 1
              AND s.manual_share = 0
              AND (t.target_id IS NULL OR t.pushed_local_seq < s.local_seq)
            ORDER BY s.payload_updated_at_ms DESC,
                     s.local_seq DESC
            LIMIT ?
            "#,
        )
        .bind(target_id)
        .bind(backfill_fetch_limit)
        .fetch_all(pool)
        .await
        .context("query pending backfill outbound sync item writes")?;
        let truncated = rows.len() > usize::from(backfill_limit);
        rows.truncate(usize::from(backfill_limit));
        truncated
    };

    if rows.is_empty() {
        return Ok(PendingOutboundBatch {
            writes: Vec::new(),
            has_more: false,
        });
    }

    let sync_keys = rows
        .iter()
        .map(|row| row.try_get::<String, _>("sync_key"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let blobs_map = if is_cloud {
        load_blob_ids_map(pool, &sync_keys).await?
    } else {
        load_lan_available_blobs_map(pool, &sync_keys)
            .await?
            .into_iter()
            .map(
                |(sync_key, (content_blobs, optional_blobs, available_blobs))| {
                    let available_set: HashSet<&str> =
                        available_blobs.iter().map(String::as_str).collect();
                    let local_content = content_blobs
                        .into_iter()
                        .filter(|blob_id| available_set.contains(blob_id.as_str()))
                        .collect::<Vec<_>>();
                    (sync_key, (local_content, optional_blobs))
                },
            )
            .collect()
    };

    let mut writes = Vec::with_capacity(rows.len());
    for row in rows {
        let sync_key: String = row.try_get("sync_key")?;
        let deleted: bool = row.try_get("deleted")?;
        let (blob_ids, optional_blob_ids) = if deleted {
            (Vec::new(), Vec::new())
        } else {
            blobs_map.get(&sync_key).cloned().unwrap_or_default()
        };
        writes.push(SyncItemWrite {
            item_key: row.try_get("item_key")?,
            expected_hub_rev: if is_cloud {
                u64::try_from(row.try_get::<i64, _>("hub_rev")?).unwrap_or(0)
            } else {
                0
            },
            local_seq: u64::try_from(row.try_get::<i64, _>("local_seq")?).unwrap_or(0),
            origin_device_id: row.try_get("origin_device_id")?,
            updated_at_ms: row.try_get("payload_updated_at_ms")?,
            deleted,
            protected: row.try_get("protected")?,
            nonce: row.try_get("nonce")?,
            ciphertext: row.try_get("ciphertext")?,
            blob_ids,
            optional_blob_ids,
        });
    }
    Ok(PendingOutboundBatch { writes, has_more })
}

/// Counts ready historical backfill items still awaiting outbound control push or cloud blob
/// upload so local backfill preparation can apply simple watermark backpressure.
pub async fn count_ready_backfill_backlog(
    pool: &SqlitePool,
    cloud_enabled: bool,
    online_peer_ids: &[String],
) -> Result<(i64, i64)> {
    if cloud_enabled {
        let unpushed_items: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM sync_items s
            LEFT JOIN sync_item_targets t
              ON t.sync_key = s.sync_key AND t.target_id = 'cloud'
            WHERE s.prep_state = 'ready'
              AND s.nonce IS NOT NULL
              AND s.local_seq > 0
              AND s.history_backfill = 1
              AND s.manual_share = 0
              AND (t.target_id IS NULL OR t.pushed_local_seq < s.local_seq)
            "#,
        )
        .fetch_one(pool)
        .await
        .context("count ready unpushed cloud backfill items")?;

        let pending_blobs: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM sync_items s
            WHERE s.prep_state = 'ready'
              AND s.deleted = 0
              AND s.history_backfill = 1
              AND s.manual_share = 0
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
            "#,
        )
        .fetch_one(pool)
        .await
        .context("count ready pending cloud backfill blobs")?;

        return Ok((unpushed_items, pending_blobs));
    }

    if !online_peer_ids.is_empty() {
        let mut query = sqlx::QueryBuilder::new(
            r#"
            SELECT COUNT(*)
            FROM sync_items s
            WHERE s.prep_state = 'ready'
              AND s.nonce IS NOT NULL
              AND s.local_seq > 0
              AND s.history_backfill = 1
              AND s.manual_share = 0
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
            "#,
        );
        let unpushed_items: i64 = query
            .build_query_scalar()
            .fetch_one(pool)
            .await
            .context("count ready unpushed LAN backfill items")?;

        return Ok((unpushed_items, 0));
    }

    Ok((0, 0))
}

/// Serves one newest-first window of local ready items to a requesting LAN peer.
pub async fn list_items_window_for_peer(
    pool: &SqlitePool,
    after_seq: u64,
    before_seq: Option<u64>,
    limit: u16,
) -> Result<(Vec<SyncItemRecord>, u64, bool)> {
    let limit = limit.clamp(1, ecopaste_sync_protocol::MAX_ITEMS_PER_BATCH);
    let latest_seq_raw: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(local_seq) FROM sync_items \
         WHERE prep_state = 'ready' AND item_key IS NOT NULL AND nonce IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .context("read latest local sync item seq")?;
    let latest_seq = latest_seq_raw
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0);

    let fetch_limit = i64::from(limit) + 1;
    let rows = sqlx::query(
        r#"
        SELECT sync_key, item_key, local_seq,
               COALESCE(origin_device_id, '') AS origin_device_id,
               payload_updated_at_ms, deleted, protected, nonce, ciphertext
        FROM sync_items
        WHERE prep_state = 'ready'
          AND item_key IS NOT NULL
          AND nonce IS NOT NULL
          AND ciphertext IS NOT NULL
          AND local_seq > ?
          AND (? IS NULL OR local_seq < ?)
        ORDER BY local_seq DESC
        LIMIT ?
        "#,
    )
    .bind(i64::try_from(after_seq).unwrap_or(i64::MAX))
    .bind(before_seq.map(|value| i64::try_from(value).unwrap_or(i64::MAX)))
    .bind(before_seq.map(|value| i64::try_from(value).unwrap_or(i64::MAX)))
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .context("list LAN sync item window")?;

    let had_more = rows.len() > usize::from(limit);
    let rows = rows
        .into_iter()
        .take(usize::from(limit))
        .collect::<Vec<_>>();
    let sync_keys = rows
        .iter()
        .map(|row| row.try_get::<String, _>("sync_key"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let blobs_detail = load_lan_available_blobs_map(pool, &sync_keys).await?;

    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let sync_key: String = row.try_get("sync_key")?;
        let deleted: bool = row.try_get("deleted")?;
        let (blob_ids, optional_blob_ids, available_blob_ids) = if deleted {
            (Vec::new(), Vec::new(), Vec::new())
        } else {
            blobs_detail.get(&sync_key).cloned().unwrap_or_default()
        };
        records.push(SyncItemRecord {
            seq: u64::try_from(row.try_get::<i64, _>("local_seq")?).unwrap_or(0),
            hub_rev: 0,
            item_key: row.try_get("item_key")?,
            origin_device_id: row.try_get("origin_device_id")?,
            updated_at_ms: row.try_get("payload_updated_at_ms")?,
            deleted,
            protected: row.try_get("protected")?,
            nonce: row.try_get("nonce")?,
            ciphertext: row.try_get("ciphertext")?,
            blob_ids,
            optional_blob_ids,
            available_blob_ids,
        });
    }
    Ok((records, latest_seq, had_more))
}

/// Counts remaining ready local items for an active LAN peer catch-up window (`after_seq < local_seq < before_seq`).
pub async fn count_items_remaining_for_peer(
    pool: &SqlitePool,
    after_seq: u64,
    before_seq: u64,
) -> Result<u64> {
    if before_seq <= after_seq {
        return Ok(0);
    }
    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM sync_items
        WHERE prep_state = 'ready'
          AND item_key IS NOT NULL
          AND nonce IS NOT NULL
          AND ciphertext IS NOT NULL
          AND local_seq > ?
          AND local_seq < ?
        "#,
    )
    .bind(i64::try_from(after_seq).unwrap_or(i64::MAX))
    .bind(i64::try_from(before_seq).unwrap_or(i64::MAX))
    .fetch_one(pool)
    .await
    .context("count remaining LAN sync items for peer")?;
    Ok(u64::try_from(count).unwrap_or(0))
}

/// Counts items pulled from `target_id` within the active catch-up range (`remote_seq >= min_remote_seq`).
pub async fn count_pulled_items_in_window(
    pool: &SqlitePool,
    target_id: &str,
    min_remote_seq: u64,
) -> Result<u64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sync_item_targets WHERE target_id = ? AND remote_seq >= ?",
    )
    .bind(target_id)
    .bind(i64::try_from(min_remote_seq).unwrap_or(i64::MAX))
    .fetch_one(pool)
    .await
    .context("count pulled items in active catchup window")?;
    Ok(u64::try_from(count).unwrap_or(0))
}

/// Applies accepted Hub CAS writes, advancing `hub_rev` and immediately confirming inline items.
pub async fn record_cloud_accepted(
    pool: &SqlitePool,
    accepted: &[AcceptedItem],
    pushed_local_seqs: &HashMap<String, u64>,
) -> Result<()> {
    if accepted.is_empty() {
        return Ok(());
    }
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin recording cloud accepted items")?;
    let now = Utc::now();
    for item in accepted {
        let pushed_seq = pushed_local_seqs.get(&item.item_key).copied().unwrap_or(0);
        let row: Option<(String, bool)> =
            sqlx::query_as("SELECT sync_key, deleted FROM sync_items WHERE item_key = ?")
                .bind(&item.item_key)
                .fetch_optional(&mut *transaction)
                .await
                .context("lookup accepted sync item")?;
        let Some((sync_key, deleted)) = row else {
            continue;
        };
        sqlx::query("UPDATE sync_items SET hub_rev = ?, updated_at = ? WHERE sync_key = ?")
            .bind(i64::try_from(item.hub_rev).unwrap_or(i64::MAX))
            .bind(now)
            .bind(&sync_key)
            .execute(&mut *transaction)
            .await
            .context("update item hub_rev")?;

        if !deleted && !item.available_blob_ids.is_empty() {
            let mut mark = sqlx::QueryBuilder::new(
                "UPDATE sync_item_blobs SET cloud_available = 1, updated_at = ",
            );
            mark.push_bind(now)
                .push(" WHERE sync_key = ")
                .push_bind(&sync_key)
                .push(" AND blob_id IN (");
            let mut separated = mark.separated(", ");
            for blob_id in &item.available_blob_ids {
                separated.push_bind(blob_id);
            }
            mark.push(")");
            mark.build()
                .execute(&mut *transaction)
                .await
                .context("mark already-stored cloud blobs available on acceptance")?;
        }

        let missing_cloud_blobs: i64 = if deleted {
            0
        } else {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM sync_item_blobs WHERE sync_key = ? AND optional = 0 AND cloud_available = 0",
            )
            .bind(&sync_key)
            .fetch_one(&mut *transaction)
            .await
            .context("count unuploaded cloud blobs")?
        };
        let confirmed_content = if missing_cloud_blobs == 0 {
            "complete"
        } else {
            "metadata"
        };
        let pushed_i64 = i64::try_from(pushed_seq).unwrap_or(i64::MAX);
        sqlx::query(
            r#"
            INSERT INTO sync_item_targets (
                sync_key, target_id, confirmed_content,
                pushed_local_seq, last_error, created_at, updated_at
            ) VALUES (?, 'cloud', ?, ?, NULL, ?, ?)
            ON CONFLICT(sync_key, target_id) DO UPDATE SET
                confirmed_content = CASE
                    WHEN excluded.confirmed_content = 'complete' THEN 'complete'
                    WHEN sync_item_targets.confirmed_content = 'complete' AND excluded.confirmed_content = 'metadata'
                    THEN 'metadata'
                    ELSE excluded.confirmed_content
                END,
                pushed_local_seq = MAX(sync_item_targets.pushed_local_seq, excluded.pushed_local_seq),
                last_error = NULL,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&sync_key)
        .bind(confirmed_content)
        .bind(pushed_i64)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("update cloud target after accepted write")?;
    }
    transaction
        .commit()
        .await
        .context("commit cloud accepted items")?;
    Ok(())
}

/// Applies LAN peer acknowledgements (inline from `LanSynced` or async from `AckLanItems`).
pub async fn record_lan_acks(
    pool: &SqlitePool,
    target_id: &str,
    acks: &[LanItemAck],
) -> Result<()> {
    if acks.is_empty() {
        return Ok(());
    }
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin recording LAN acks")?;
    let now = Utc::now();
    for ack in acks {
        let sync_key: Option<String> =
            sqlx::query_scalar("SELECT sync_key FROM sync_items WHERE item_key = ?")
                .bind(&ack.item_key)
                .fetch_optional(&mut *transaction)
                .await
                .context("lookup acked LAN sync item")?;
        let Some(sync_key) = sync_key else {
            continue;
        };
        let ack_seq = i64::try_from(ack.local_seq).unwrap_or(i64::MAX);
        let confirmed_content = match ack.level {
            LanContentLevel::Complete => "complete",
            LanContentLevel::Metadata => "metadata",
        };
        sqlx::query(
            r#"
            INSERT INTO sync_item_targets (
                sync_key, target_id, confirmed_content,
                pushed_local_seq, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(sync_key, target_id) DO UPDATE SET
                confirmed_content = CASE
                    WHEN excluded.confirmed_content = 'complete' THEN 'complete'
                    WHEN sync_item_targets.confirmed_content = 'complete'
                         AND sync_item_targets.pushed_local_seq >= excluded.pushed_local_seq
                    THEN 'complete'
                    ELSE excluded.confirmed_content
                END,
                pushed_local_seq = MAX(sync_item_targets.pushed_local_seq, excluded.pushed_local_seq),
                last_error = NULL,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&sync_key)
        .bind(target_id)
        .bind(confirmed_content)
        .bind(ack_seq)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("update LAN peer target ack")?;
    }
    transaction.commit().await.context("commit LAN acks")?;
    Ok(())
}

/// One unuploaded content blob belonging to a LAN-routed or metadata-only Cloud item that can
/// be verified against the Hub without re-uploading attachment bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCloudBlobCheck {
    pub sync_key: String,
    pub blob_id: String,
    pub size: u64,
}

/// Summary of local item promotions after verifying blobs already stored on the Cloud Hub.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CloudBlobVerifyPromotion {
    pub promoted_items: usize,
    pub needs_control_push: bool,
}

/// Lists non-optional unuploaded blobs for deferred-blob or metadata-only Cloud items so
/// existing Hub storage can be confirmed via `CheckCloudBlobs`.
pub async fn pending_cloud_blob_verifications(
    pool: &SqlitePool,
    include_sensitive: bool,
    limit: u16,
) -> Result<Vec<PendingCloudBlobCheck>> {
    let effective_limit = i64::from(limit.max(1));
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT s.sync_key, b.blob_id, b.size, s.payload_updated_at_ms, s.local_seq
        FROM sync_items s
        JOIN sync_item_blobs b
          ON b.sync_key = s.sync_key
         AND b.optional = 0
         AND b.cloud_available = 0
         AND b.size > 0
        LEFT JOIN sync_item_targets t
          ON t.sync_key = s.sync_key AND t.target_id = 'cloud'
        WHERE s.prep_state = 'ready'
          AND s.nonce IS NOT NULL
          AND s.deleted = 0
          AND (? OR s.is_sensitive = 0 OR s.manual_share = 1)
          AND s.cloud_blob_policy NOT IN ('blocked', 'canceled')
          AND (
              (t.target_id = 'cloud' AND t.confirmed_content = 'metadata')
              OR (
                  s.origin = 'local'
                  AND (
                      t.target_id IS NULL
                      OR (t.confirmed_content = 'none' AND s.cloud_blob_policy = 'defer')
                  )
              )
          )
        ORDER BY s.payload_updated_at_ms DESC, s.local_seq DESC, b.blob_id ASC
        LIMIT ?
        "#,
    )
    .bind(include_sensitive)
    .bind(effective_limit)
    .fetch_all(pool)
    .await
    .context("query sync blobs needing cloud availability verification")?;

    let mut checks = Vec::with_capacity(rows.len());
    for row in rows {
        let size = u64::try_from(row.try_get::<i64, _>("size")?).unwrap_or(0);
        if size == 0 {
            continue;
        }
        checks.push(PendingCloudBlobCheck {
            sync_key: row.try_get("sync_key")?,
            blob_id: row.try_get("blob_id")?,
            size,
        });
    }
    Ok(checks)
}

/// Marks Hub-confirmed blobs available and promotes complete metadata-only items to confirmed
/// Cloud completion (or queues their metadata push when never pushed to Cloud).
pub async fn promote_cloud_blobs_available(
    pool: &SqlitePool,
    available_blob_ids: &[String],
) -> Result<CloudBlobVerifyPromotion> {
    if available_blob_ids.is_empty() {
        return Ok(CloudBlobVerifyPromotion::default());
    }

    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin promoting existing cloud blobs")?;
    let now = Utc::now();

    let mut affected_query = sqlx::QueryBuilder::new(
        "SELECT DISTINCT sync_key FROM sync_item_blobs WHERE optional = 0 AND cloud_available = 0 AND blob_id IN (",
    );
    let mut separated = affected_query.separated(", ");
    for blob_id in available_blob_ids {
        separated.push_bind(blob_id);
    }
    affected_query.push(")");
    let affected_sync_keys: Vec<String> = affected_query
        .build_query_scalar()
        .fetch_all(&mut *transaction)
        .await
        .context("list items affected by verified cloud blobs")?;

    let mut mark =
        sqlx::QueryBuilder::new("UPDATE sync_item_blobs SET cloud_available = 1, updated_at = ");
    mark.push_bind(now)
        .push(" WHERE cloud_available = 0 AND blob_id IN (");
    let mut separated = mark.separated(", ");
    for blob_id in available_blob_ids {
        separated.push_bind(blob_id);
    }
    mark.push(")");
    mark.build()
        .execute(&mut *transaction)
        .await
        .context("mark verified cloud blobs available")?;

    let mut promotion = CloudBlobVerifyPromotion::default();
    for sync_key in affected_sync_keys {
        let missing_blobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sync_item_blobs \
             WHERE sync_key = ? AND optional = 0 AND cloud_available = 0",
        )
        .bind(&sync_key)
        .fetch_one(&mut *transaction)
        .await
        .context("count remaining unuploaded blobs after cloud verification")?;
        if missing_blobs > 0 {
            continue;
        }

        let Some((origin, current_local_seq, cloud_blob_policy)): Option<(String, i64, String)> =
            sqlx::query_as(
                "SELECT origin, local_seq, cloud_blob_policy \
                 FROM sync_items WHERE sync_key = ? AND prep_state = 'ready' AND deleted = 0",
            )
            .bind(&sync_key)
            .fetch_optional(&mut *transaction)
            .await
            .context("load sync item for cloud verification promotion")?
        else {
            continue;
        };

        if matches!(cloud_blob_policy.as_str(), "blocked" | "canceled") {
            continue;
        }

        let existing_target: Option<(String, i64)> = sqlx::query_as(
            "SELECT confirmed_content, pushed_local_seq \
             FROM sync_item_targets WHERE sync_key = ? AND target_id = 'cloud'",
        )
        .bind(&sync_key)
        .fetch_optional(&mut *transaction)
        .await
        .context("load cloud target for cloud verification promotion")?;

        if let Some((confirmed_content, pushed_local_seq)) = existing_target {
            if confirmed_content == "metadata" {
                let still_current = pushed_local_seq >= current_local_seq;
                sqlx::query(
                    r#"
                    UPDATE sync_item_targets
                    SET confirmed_content = 'complete',
                        last_error = NULL,
                        updated_at = ?
                    WHERE sync_key = ? AND target_id = 'cloud'
                    "#,
                )
                .bind(now)
                .bind(&sync_key)
                .execute(&mut *transaction)
                .await
                .context("promote metadata-only cloud target to complete")?;

                promotion.promoted_items += 1;
                if !still_current {
                    promotion.needs_control_push = true;
                }
                continue;
            }
            if confirmed_content == "complete" {
                continue;
            }
        }

        if origin == "local" {
            if current_local_seq <= 0 {
                let local_seq = crate::db::sync_items::next_local_seq_on(&mut transaction).await?;
                sqlx::query(
                    "UPDATE sync_items SET local_seq = ?, updated_at = ? WHERE sync_key = ?",
                )
                .bind(local_seq)
                .bind(now)
                .bind(&sync_key)
                .execute(&mut *transaction)
                .await
                .context("assign local_seq for cloud-present blobs")?;
            }
            promotion.promoted_items += 1;
            promotion.needs_control_push = true;
        }
    }

    transaction
        .commit()
        .await
        .context("commit existing cloud blob verification promotions")?;
    Ok(promotion)
}

/// Lists local items that still have un-uploaded content blobs desired on the Cloud Hub.
/// Ordered realtime-first (`history_backfill ASC, payload_updated_at_ms DESC`). When
/// `realtime_only` is `true`, only freshly captured or manually shared items are returned for the
/// dedicated VIP upload worker.
pub async fn pending_cloud_blob_uploads(
    pool: &SqlitePool,
    realtime_only: bool,
    limit: u16,
) -> Result<Vec<PendingCloudBlobUpload>> {
    let rows = sqlx::query(
        r#"
        SELECT s.sync_key, s.item_id, s.kind, s.history_backfill, s.manual_share,
               s.payload_updated_at_ms
        FROM sync_items s
        WHERE s.prep_state = 'ready'
          AND s.deleted = 0
          AND (
              (s.origin = 'local' AND s.cloud_blob_policy IN ('auto', 'requested'))
              OR s.cloud_blob_policy = 'requested'
          )
          AND (? = 0 OR s.history_backfill = 0 OR s.manual_share = 1)
          AND EXISTS (
              SELECT 1 FROM sync_item_blobs b
              WHERE b.sync_key = s.sync_key
                AND b.optional = 0
                AND b.cloud_available = 0
                AND b.local_available = 1
          )
        ORDER BY s.manual_share DESC,
                 s.history_backfill ASC,
                 s.payload_updated_at_ms DESC,
                 s.local_seq DESC
        LIMIT ?
        "#,
    )
    .bind(i64::from(realtime_only))
    .bind(i64::from(limit.max(1)))
    .fetch_all(pool)
    .await
    .context("list pending cloud blob uploads")?;

    let mut uploads = Vec::with_capacity(rows.len());
    for row in rows {
        let sync_key: String = row.try_get("sync_key")?;
        let blobs = load_item_blobs(pool, &sync_key)
            .await?
            .into_iter()
            .filter(|blob| !blob.optional && !blob.cloud_available && blob.local_available)
            .collect::<Vec<_>>();
        if blobs.is_empty() {
            continue;
        }
        uploads.push(PendingCloudBlobUpload {
            sync_key,
            item_id: row.try_get("item_id")?,
            kind: row.try_get("kind")?,
            history_backfill: row.try_get("history_backfill")?,
            manual_share: row.try_get("manual_share")?,
            payload_updated_at_ms: row.try_get("payload_updated_at_ms")?,
            blobs,
        });
    }
    Ok(uploads)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloudBlobAvailableOutcome {
    pub completed: bool,
    pub needs_control_push: bool,
}

/// Marks one content blob uploaded to Cloud and promotes the cloud target to `Complete` once all blobs land.
pub async fn mark_blob_cloud_available(
    pool: &SqlitePool,
    sync_key: &str,
    blob_id: &str,
) -> Result<CloudBlobAvailableOutcome> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin marking cloud blob available")?;
    let now = Utc::now();
    sqlx::query(
        "UPDATE sync_item_blobs SET cloud_available = 1, updated_at = ? \
         WHERE sync_key = ? AND blob_id = ?",
    )
    .bind(now)
    .bind(sync_key)
    .bind(blob_id)
    .execute(&mut *transaction)
    .await
    .context("mark sync item blob cloud_available")?;

    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sync_item_blobs \
         WHERE sync_key = ? AND optional = 0 AND cloud_available = 0",
    )
    .bind(sync_key)
    .fetch_one(&mut *transaction)
    .await
    .context("count remaining cloud blobs for item")?;

    let (completed, needs_control_push) = if remaining == 0 {
        let current_seq: i64 =
            sqlx::query_scalar("SELECT local_seq FROM sync_items WHERE sync_key = ?")
                .bind(sync_key)
                .fetch_optional(&mut *transaction)
                .await?
                .unwrap_or(0);
        let existing_target: Option<(String, i64)> = sqlx::query_as(
            "SELECT confirmed_content, pushed_local_seq FROM sync_item_targets \
             WHERE sync_key = ? AND target_id = 'cloud'",
        )
        .bind(sync_key)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some((confirmed_content, pushed_local_seq)) = existing_target {
            if pushed_local_seq > 0 || confirmed_content != "none" {
                sqlx::query(
                    r#"
                    UPDATE sync_item_targets
                    SET confirmed_content = 'complete',
                        last_error = NULL,
                        updated_at = ?
                    WHERE sync_key = ? AND target_id = 'cloud'
                    "#,
                )
                .bind(now)
                .bind(sync_key)
                .execute(&mut *transaction)
                .await
                .context("promote cloud target to complete")?;
                (true, pushed_local_seq < current_seq)
            } else {
                // Metadata has not been accepted by the Hub yet; let the control plane push it.
                (false, true)
            }
        } else {
            // No Hub metadata record exists yet; let the control plane push it first.
            (false, true)
        }
    } else {
        (false, false)
    };
    transaction
        .commit()
        .await
        .context("commit cloud blob availability")?;
    Ok(CloudBlobAvailableOutcome {
        completed,
        needs_control_push,
    })
}

/// Persists a merged remote snapshot received from Cloud or a LAN peer.
///
/// If `re_push_required` is `true` (CRDT merge combined local-only edits with remote edits),
/// `local_seq` is incremented so outbound sync pushes the converged state back.
pub struct StoreMergedRemoteItem<'a> {
    pub sync_key: &'a str,
    pub item_key: &'a str,
    pub origin_device_id: &'a str,
    pub hub_rev: Option<u64>,
    pub remote_seq: u64,
    pub source_target_id: &'a str,
    pub snapshot: &'a SyncedItemSnapshot,
    pub nonce: &'a [u8],
    pub ciphertext: &'a [u8],
    pub available_blob_ids: &'a [String],
    pub needs_apply: bool,
    pub re_push_required: bool,
}

pub async fn store_merged_remote_item(
    pool: &SqlitePool,
    input: StoreMergedRemoteItem<'_>,
) -> Result<()> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin storing merged remote sync item")?;
    let now = Utc::now();
    let snapshot = input.snapshot;
    let deleted = snapshot.deleted();
    let protected = snapshot.protected();
    let kind = snapshot
        .item
        .as_ref()
        .map(|item| item.kind.as_str())
        .unwrap_or("text");
    let is_sensitive = snapshot.item.as_ref().is_some_and(|item| item.is_sensitive);
    let source_revision = snapshot
        .item
        .as_ref()
        .and_then(|item| item.source_revision.as_deref());
    let payload_updated_at_ms = snapshot.updated_at_ms();
    let local_seq = if input.re_push_required {
        crate::db::sync_items::next_local_seq_on(&mut transaction).await?
    } else {
        sqlx::query_scalar::<_, i64>("SELECT local_seq FROM sync_items WHERE sync_key = ?")
            .bind(input.sync_key)
            .fetch_optional(&mut *transaction)
            .await?
            .unwrap_or(0)
    };
    crate::db::sync_items::observe_lamport_seq_on(&mut transaction, snapshot.max_lamport_seq())
        .await?;

    let hub_rev = input
        .hub_rev
        .map(|value| i64::try_from(value).unwrap_or(i64::MAX));
    sqlx::query(
        r#"
        INSERT INTO sync_items (
            sync_key, item_key, origin, origin_device_id, source_revision,
            content_identity, kind, is_sensitive, deleted, protected,
            content_seq, content_device_id, presence_seq, presence_device_id,
            favorite_seq, favorite_device_id, pinned_seq, pinned_device_id,
            note_seq, note_device_id, is_present, is_favorite, is_pinned, note,
            local_seq, hub_rev, payload_updated_at_ms, prep_state, cloud_blob_policy,
            nonce, ciphertext, local_state, needs_apply, created_at, updated_at
        ) VALUES (
            ?, ?, 'remote', ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, COALESCE(?, 0), ?, 'ready', 'defer',
            ?, ?, CASE WHEN ? = 1 THEN 'managed' ELSE 'metadata' END, ?, ?, ?
        )
        ON CONFLICT(sync_key) DO UPDATE SET
            item_key = excluded.item_key,
            origin_device_id = COALESCE(excluded.origin_device_id, sync_items.origin_device_id),
            source_revision = COALESCE(excluded.source_revision, sync_items.source_revision),
            content_identity = COALESCE(excluded.content_identity, sync_items.content_identity),
            kind = CASE WHEN excluded.deleted = 0 THEN excluded.kind ELSE sync_items.kind END,
            is_sensitive = CASE WHEN excluded.deleted = 0 THEN excluded.is_sensitive ELSE sync_items.is_sensitive END,
            deleted = excluded.deleted,
            protected = excluded.protected,
            content_seq = excluded.content_seq,
            content_device_id = excluded.content_device_id,
            presence_seq = excluded.presence_seq,
            presence_device_id = excluded.presence_device_id,
            favorite_seq = excluded.favorite_seq,
            favorite_device_id = excluded.favorite_device_id,
            pinned_seq = excluded.pinned_seq,
            pinned_device_id = excluded.pinned_device_id,
            note_seq = excluded.note_seq,
            note_device_id = excluded.note_device_id,
            is_present = excluded.is_present,
            is_favorite = excluded.is_favorite,
            is_pinned = excluded.is_pinned,
            note = excluded.note,
            local_seq = MAX(sync_items.local_seq, excluded.local_seq),
            hub_rev = COALESCE(?, sync_items.hub_rev),
            payload_updated_at_ms = CASE
                WHEN excluded.payload_updated_at_ms > 0
                THEN MAX(sync_items.payload_updated_at_ms, excluded.payload_updated_at_ms)
                ELSE sync_items.payload_updated_at_ms
            END,
            prep_state = 'ready',
            cloud_blob_policy = CASE
                WHEN sync_items.origin = 'remote' AND sync_items.deleted = 1 AND excluded.deleted = 0
                THEN 'defer'
                ELSE sync_items.cloud_blob_policy
            END,
            nonce = excluded.nonce,
            ciphertext = excluded.ciphertext,
            needs_apply = MAX(sync_items.needs_apply, excluded.needs_apply),
            needs_blob_pull = CASE WHEN excluded.deleted = 1 THEN 0 ELSE sync_items.needs_blob_pull END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(input.sync_key)
    .bind(input.item_key)
    .bind(input.origin_device_id)
    .bind(source_revision)
    .bind(snapshot.content_identity.as_deref())
    .bind(kind)
    .bind(is_sensitive)
    .bind(deleted)
    .bind(protected)
    .bind(i64::try_from(snapshot.content_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.content_version.device_id)
    .bind(i64::try_from(snapshot.presence_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.presence_version.device_id)
    .bind(i64::try_from(snapshot.favorite_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.favorite_version.device_id)
    .bind(i64::try_from(snapshot.pinned_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.pinned_version.device_id)
    .bind(i64::try_from(snapshot.note_version.seq).unwrap_or(i64::MAX))
    .bind(&snapshot.note_version.device_id)
    .bind(snapshot.is_present)
    .bind(snapshot.is_favorite)
    .bind(snapshot.is_pinned)
    .bind(snapshot.note.as_deref())
    .bind(local_seq)
    .bind(hub_rev)
    .bind(payload_updated_at_ms)
    .bind(input.nonce)
    .bind(input.ciphertext)
    .bind(i64::from(deleted || snapshot.blobs.is_empty()))
    .bind(input.needs_apply)
    .bind(now)
    .bind(now)
    .bind(hub_rev)
    .execute(&mut *transaction)
    .await
    .context("upsert merged remote sync item")?;

    let available_set: HashSet<&str> = input
        .available_blob_ids
        .iter()
        .map(String::as_str)
        .collect();
    let is_cloud = input.source_target_id == CLOUD_TARGET;
    struct ExistingBlobState {
        encrypted_path: String,
        source_path: Option<String>,
        stream_nonce: Option<Vec<u8>>,
        cloud_available: bool,
        local_available: bool,
    }
    let existing_blobs: HashMap<String, ExistingBlobState> = sqlx::query(
        "SELECT blob_id, encrypted_path, source_path, stream_nonce, cloud_available, local_available \
         FROM sync_item_blobs WHERE sync_key = ?",
    )
    .bind(input.sync_key)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| {
        Ok((
            row.try_get::<String, _>("blob_id")?,
            ExistingBlobState {
                encrypted_path: row.try_get("encrypted_path")?,
                source_path: row.try_get("source_path")?,
                stream_nonce: row.try_get("stream_nonce")?,
                cloud_available: row.try_get("cloud_available")?,
                local_available: row.try_get("local_available")?,
            },
        ))
    })
    .collect::<Result<_>>()?;

    sqlx::query("DELETE FROM sync_item_blobs WHERE sync_key = ?")
        .bind(input.sync_key)
        .execute(&mut *transaction)
        .await
        .context("refresh remote sync item blobs")?;
    if !deleted {
        for blob in &snapshot.blobs {
            let prev = existing_blobs.get(&blob.blob_id);
            let cloud_avail = (is_cloud && available_set.contains(blob.blob_id.as_str()))
                || prev.is_some_and(|p| p.cloud_available);
            let local_avail = prev.is_some_and(|p| p.local_available);
            let enc_path = prev.map(|p| p.encrypted_path.as_str()).unwrap_or("");
            let src_path = prev.and_then(|p| p.source_path.as_deref());
            let stream_nonce = prev.and_then(|p| p.stream_nonce.as_deref());
            let role = match blob.role {
                super::super::model::BlobRole::Image => "image",
                super::super::model::BlobRole::File => "file",
            };
            sqlx::query(
                r#"
                INSERT INTO sync_item_blobs (
                    sync_key, blob_id, role, optional, size, original_size,
                    encrypted_path, source_path, source_is_directory, stream_nonce,
                    stream_commitment, cloud_available, local_available, created_at, updated_at
                ) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(input.sync_key)
            .bind(&blob.blob_id)
            .bind(role)
            .bind(i64::try_from(blob.encrypted_size).unwrap_or(i64::MAX))
            .bind(i64::try_from(blob.original_size).unwrap_or(i64::MAX))
            .bind(enc_path)
            .bind(src_path)
            .bind(blob.is_directory_archive)
            .bind(stream_nonce)
            .bind(blob.stream_commitment)
            .bind(cloud_avail)
            .bind(local_avail)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .context("insert remote sync item blob")?;
        }
        if let Some(icon) = snapshot
            .source_app
            .as_ref()
            .and_then(|source| source.icon.as_ref())
        {
            let prev = existing_blobs.get(&icon.blob_id);
            let cloud_avail = (is_cloud && available_set.contains(icon.blob_id.as_str()))
                || prev.is_some_and(|p| p.cloud_available);
            let local_avail = prev.is_some_and(|p| p.local_available);
            sqlx::query(
                r#"
                INSERT OR IGNORE INTO sync_item_blobs (
                    sync_key, blob_id, role, optional, size, original_size,
                    encrypted_path, source_path, source_is_directory, stream_nonce,
                    stream_commitment, cloud_available, local_available, created_at, updated_at
                ) VALUES (?, ?, 'source_icon', 1, ?, ?, '', NULL, 0, NULL, 0, ?, ?, ?, ?)
                "#,
            )
            .bind(input.sync_key)
            .bind(&icon.blob_id)
            .bind(i64::try_from(icon.encrypted_size).unwrap_or(i64::MAX))
            .bind(i64::try_from(icon.original_size).unwrap_or(i64::MAX))
            .bind(cloud_avail)
            .bind(local_avail)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .context("insert remote source icon blob")?;
        }
    }

    // Record source route availability for card status & remote file download routing.
    let all_blobs_available_on_source = deleted
        || snapshot.blobs.is_empty()
        || snapshot
            .blobs
            .iter()
            .all(|blob| available_set.contains(blob.blob_id.as_str()));
    let source_level = if all_blobs_available_on_source {
        "complete"
    } else {
        "metadata"
    };
    let pushed_seq = if input.re_push_required { 0 } else { local_seq };
    let remote_seq = i64::try_from(input.remote_seq).unwrap_or(i64::MAX);
    sqlx::query(
        r#"
        INSERT INTO sync_item_targets (
            sync_key, target_id, confirmed_content,
            pushed_local_seq, remote_seq, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
        ON CONFLICT(sync_key, target_id) DO UPDATE SET
            confirmed_content = CASE
                WHEN excluded.confirmed_content = 'complete' THEN 'complete'
                WHEN excluded.confirmed_content = 'metadata'
                     AND sync_item_targets.confirmed_content = 'none'
                THEN 'metadata'
                ELSE sync_item_targets.confirmed_content
            END,
            pushed_local_seq = CASE
                WHEN ? <> 0 THEN sync_item_targets.pushed_local_seq
                ELSE MAX(sync_item_targets.pushed_local_seq, excluded.pushed_local_seq)
            END,
            remote_seq = MAX(sync_item_targets.remote_seq, excluded.remote_seq),
            updated_at = excluded.updated_at
        "#,
    )
    .bind(input.sync_key)
    .bind(input.source_target_id)
    .bind(source_level)
    .bind(pushed_seq)
    .bind(remote_seq)
    .bind(now)
    .bind(now)
    .bind(i64::from(input.re_push_required))
    .execute(&mut *transaction)
    .await
    .context("record source route target availability")?;

    transaction
        .commit()
        .await
        .context("commit merged remote sync item")?;
    Ok(())
}

/// Lists items needing local DB/clipboard application, newest-first so latest copies apply first.
pub async fn list_items_needing_apply(
    pool: &SqlitePool,
    limit: u16,
) -> Result<Vec<StoredSyncItemMeta>> {
    let rows = sqlx::query(
        r#"
        SELECT sync_key, item_key, item_id, origin, origin_device_id, source_revision,
               content_identity, kind, is_sensitive, deleted, protected,
               content_seq, content_device_id, presence_seq, presence_device_id,
               favorite_seq, favorite_device_id, pinned_seq, pinned_device_id,
               note_seq, note_device_id, is_present, is_favorite, is_pinned, note,
               local_seq, hub_rev, payload_updated_at_ms, prep_state, history_backfill,
               manual_share, nonce, ciphertext, local_state, remote_file_state,
               needs_blob_pull
        FROM sync_items
        WHERE needs_apply = 1
          AND item_key IS NOT NULL
          AND nonce IS NOT NULL
          AND ciphertext IS NOT NULL
        ORDER BY payload_updated_at_ms DESC, local_seq DESC
        LIMIT ?
        "#,
    )
    .bind(i64::from(limit.max(1)))
    .fetch_all(pool)
    .await
    .context("list sync items needing local apply")?;
    rows.into_iter().map(meta_from_row).collect()
}

/// Lists items awaiting asynchronous Data-Plane blob downloads that are confirmed complete on
/// `target_id` (`'cloud'` or `'peer:<device_id>'`). When `min_updated_at_ms` is set, only fresh
/// realtime items are returned for the dedicated VIP download worker.
pub async fn list_blob_pull_items_for_source(
    pool: &SqlitePool,
    target_id: &str,
    min_updated_at_ms: Option<i64>,
    limit: u16,
) -> Result<Vec<StoredSyncItemMeta>> {
    let rows = sqlx::query(
        r#"
        SELECT s.sync_key, s.item_key, s.item_id, s.origin, s.origin_device_id, s.source_revision,
               s.content_identity, s.kind, s.is_sensitive, s.deleted, s.protected,
               s.content_seq, s.content_device_id, s.presence_seq, s.presence_device_id,
               s.favorite_seq, s.favorite_device_id, s.pinned_seq, s.pinned_device_id,
               s.note_seq, s.note_device_id, s.is_present, s.is_favorite, s.is_pinned, s.note,
               s.local_seq, s.hub_rev, s.payload_updated_at_ms, s.prep_state, s.history_backfill,
               s.manual_share, s.nonce, s.ciphertext, s.local_state,
               s.remote_file_state, s.needs_blob_pull
        FROM sync_items s
        WHERE s.needs_blob_pull = 1
          AND s.deleted = 0
          AND s.item_key IS NOT NULL
          AND s.nonce IS NOT NULL
          AND s.ciphertext IS NOT NULL
          AND (? IS NULL OR s.payload_updated_at_ms >= ?)
          AND EXISTS (
              SELECT 1 FROM sync_item_targets t
              WHERE t.sync_key = s.sync_key
                AND t.target_id = ?
                AND t.confirmed_content = 'complete'
          )
        ORDER BY s.payload_updated_at_ms DESC, s.local_seq DESC
        LIMIT ?
        "#,
    )
    .bind(min_updated_at_ms)
    .bind(min_updated_at_ms)
    .bind(target_id)
    .bind(i64::from(limit.max(1)))
    .fetch_all(pool)
    .await
    .context("list sync items awaiting remote blob pull")?;
    rows.into_iter().map(meta_from_row).collect()
}

pub async fn mark_item_applied(
    pool: &SqlitePool,
    sync_key: &str,
    item_id: Option<&str>,
    local_state: &str,
    remote_file_state: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sync_items
        SET needs_apply = 0,
            needs_blob_pull = 0,
            item_id = COALESCE(?, item_id),
            local_state = ?,
            remote_file_state = ?,
            updated_at = ?
        WHERE sync_key = ?
        "#,
    )
    .bind(item_id)
    .bind(local_state)
    .bind(remote_file_state)
    .bind(Utc::now())
    .bind(sync_key)
    .execute(pool)
    .await
    .context("mark sync item applied")?;
    Ok(())
}

/// Completes inline application and hands the item off to the Data-Plane blob download workers.
pub async fn mark_item_needs_blob_pull(
    pool: &SqlitePool,
    sync_key: &str,
    item_id: Option<&str>,
    local_state: &str,
    remote_file_state: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sync_items
        SET needs_apply = 0,
            needs_blob_pull = 1,
            item_id = COALESCE(?, item_id),
            local_state = ?,
            remote_file_state = ?,
            updated_at = ?
        WHERE sync_key = ?
        "#,
    )
    .bind(item_id)
    .bind(local_state)
    .bind(remote_file_state)
    .bind(Utc::now())
    .bind(sync_key)
    .execute(pool)
    .await
    .context("mark sync item awaiting remote blob pull")?;
    Ok(())
}

pub async fn clear_item_needs_apply(pool: &SqlitePool, sync_key: &str) -> Result<()> {
    sqlx::query(
        "UPDATE sync_items SET needs_apply = 0, needs_blob_pull = 0, updated_at = ? WHERE sync_key = ?",
    )
    .bind(Utc::now())
    .bind(sync_key)
    .execute(pool)
    .await
    .context("clear sync item needs_apply")?;
    Ok(())
}

/// Downgrades `target_id`'s availability to `'metadata'` after an explicit `NotFound` blob error
/// and clears `needs_blob_pull` when no other complete source remains, preventing poison-pill loops.
pub async fn downgrade_source_availability_on_missing_blob(
    pool: &SqlitePool,
    sync_key: &str,
    target_id: &str,
    missing_blob_id: &str,
) -> Result<bool> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin downgrading missing remote blob source")?;
    let now = Utc::now();
    sqlx::query(
        r#"
        UPDATE sync_item_targets
        SET confirmed_content = CASE
                WHEN confirmed_content = 'complete' THEN 'metadata'
                ELSE confirmed_content
            END,
            updated_at = ?
        WHERE sync_key = ? AND target_id = ?
        "#,
    )
    .bind(now)
    .bind(sync_key)
    .bind(target_id)
    .execute(&mut *transaction)
    .await
    .context("downgrade target availability on missing blob")?;
    if target_id == CLOUD_TARGET {
        sqlx::query(
            "UPDATE sync_item_blobs SET cloud_available = 0, updated_at = ? \
             WHERE sync_key = ? AND blob_id = ?",
        )
        .bind(now)
        .bind(sync_key)
        .bind(missing_blob_id)
        .execute(&mut *transaction)
        .await
        .context("clear cloud_available for missing Hub blob")?;
    }
    let has_other_complete: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sync_item_targets \
         WHERE sync_key = ? AND confirmed_content = 'complete')",
    )
    .bind(sync_key)
    .fetch_one(&mut *transaction)
    .await
    .context("check remaining complete blob sources")?;
    if !has_other_complete {
        sqlx::query("UPDATE sync_items SET needs_blob_pull = 0, updated_at = ? WHERE sync_key = ?")
            .bind(now)
            .bind(sync_key)
            .execute(&mut *transaction)
            .await
            .context("clear needs_blob_pull when no complete source remains")?;
    }
    transaction
        .commit()
        .await
        .context("commit missing blob source downgrade")?;
    Ok(!has_other_complete)
}

/// Returns `(peer_device_id, remote_seq)` pairs for LAN peers that sent or served `sync_key`.
pub async fn lan_remote_seqs_for_item(
    pool: &SqlitePool,
    sync_key: &str,
) -> Result<Vec<(String, u64)>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT target_id, remote_seq FROM sync_item_targets \
         WHERE sync_key = ? AND target_id LIKE 'peer:%' AND remote_seq > 0",
    )
    .bind(sync_key)
    .fetch_all(pool)
    .await
    .context("list LAN remote sequences for sync item")?;
    Ok(rows
        .into_iter()
        .filter_map(|(target_id, remote_seq)| {
            let device_id = target_id.strip_prefix("peer:")?.to_owned();
            let seq = u64::try_from(remote_seq).ok().filter(|&v| v > 0)?;
            Some((device_id, seq))
        })
        .collect())
}

pub async fn load_item_blobs(pool: &SqlitePool, sync_key: &str) -> Result<Vec<SyncItemBlobRow>> {
    let rows = sqlx::query(
        r#"
        SELECT sync_key, blob_id, role, optional, size, original_size,
               encrypted_path, source_path, source_is_directory, stream_nonce,
               stream_commitment, cloud_available, local_available
        FROM sync_item_blobs
        WHERE sync_key = ?
        ORDER BY optional ASC, blob_id ASC
        "#,
    )
    .bind(sync_key)
    .fetch_all(pool)
    .await
    .context("load sync item blobs")?;
    rows.into_iter()
        .map(|row| {
            Ok(SyncItemBlobRow {
                blob_id: row.try_get("blob_id")?,
                role: row.try_get("role")?,
                optional: row.try_get("optional")?,
                size: u64::try_from(row.try_get::<i64, _>("size")?).unwrap_or(0),
                original_size: u64::try_from(row.try_get::<i64, _>("original_size")?).unwrap_or(0),
                encrypted_path: row.try_get("encrypted_path")?,
                source_path: row.try_get("source_path")?,
                source_is_directory: row.try_get("source_is_directory")?,
                stream_nonce: row.try_get("stream_nonce")?,
                stream_commitment: row.try_get("stream_commitment")?,
                cloud_available: row.try_get("cloud_available")?,
                local_available: row.try_get("local_available")?,
            })
        })
        .collect()
}

pub async fn outgoing_blob(pool: &SqlitePool, blob_id: &str) -> Result<Option<StoredBlob>> {
    let row = sqlx::query(
        r#"
        SELECT b.blob_id, b.encrypted_path, b.size, b.source_path, b.source_is_directory,
               b.stream_nonce, b.stream_commitment
        FROM sync_item_blobs b
        JOIN sync_items s ON s.sync_key = b.sync_key
        WHERE b.blob_id = ? AND b.local_available = 1
        ORDER BY s.deleted ASC,
                 CASE WHEN b.encrypted_path <> '' THEN 0 ELSE 1 END ASC,
                 s.payload_updated_at_ms DESC,
                 s.history_backfill ASC,
                 s.updated_at DESC,
                 s.local_seq DESC
        LIMIT 1
        "#,
    )
    .bind(blob_id)
    .fetch_optional(pool)
    .await
    .context("lookup outgoing sync item blob")?;
    if let Some(row) = row {
        let raw_encrypted_path: String = row.try_get("encrypted_path")?;
        let stream_commitment: bool = row.try_get("stream_commitment")?;
        let encrypted_path = if !stream_commitment && raw_encrypted_path.is_empty() {
            super::super::manager::SYNC_STAGING_DIR.to_owned()
        } else {
            raw_encrypted_path
        };
        return Ok(Some(StoredBlob {
            blob_id: row.try_get("blob_id")?,
            encrypted_path,
            size: u64::try_from(row.try_get::<i64, _>("size")?).unwrap_or(0),
            source_path: row.try_get("source_path")?,
            source_is_directory: row.try_get("source_is_directory")?,
            stream_nonce: row.try_get("stream_nonce")?,
        }));
    }
    let icon_row = sqlx::query(
        r#"
        SELECT blob_id, encrypted_path, size, source_path, stream_nonce
        FROM sync_source_icon_uploads
        WHERE blob_id = ?
        LIMIT 1
        "#,
    )
    .bind(blob_id)
    .fetch_optional(pool)
    .await
    .context("lookup outgoing source icon blob")?;
    let Some(row) = icon_row else {
        return Ok(None);
    };
    let raw_encrypted_path: String = row.try_get("encrypted_path")?;
    let encrypted_path = if raw_encrypted_path.is_empty() {
        super::super::manager::SYNC_STAGING_DIR.to_owned()
    } else {
        raw_encrypted_path
    };
    Ok(Some(StoredBlob {
        blob_id: row.try_get("blob_id")?,
        encrypted_path,
        size: u64::try_from(row.try_get::<i64, _>("size")?).unwrap_or(0),
        source_path: row.try_get("source_path")?,
        source_is_directory: false,
        stream_nonce: row.try_get("stream_nonce")?,
    }))
}

pub async fn mark_blob_local_unavailable(
    pool: &SqlitePool,
    sync_key: &str,
    blob_id: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE sync_item_blobs SET local_available = 0, updated_at = ? \
         WHERE sync_key = ? AND blob_id = ?",
    )
    .bind(Utc::now())
    .bind(sync_key)
    .bind(blob_id)
    .execute(pool)
    .await
    .context("mark sync item blob local_available = 0")?;
    Ok(())
}

pub async fn item_id_for_blob(
    pool: &SqlitePool,
    blob_id: &str,
    item_sync_key: Option<&str>,
) -> Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        r#"
        SELECT s.sync_key, s.item_id
        FROM sync_item_blobs b
        JOIN sync_items s ON s.sync_key = b.sync_key
        WHERE b.blob_id = ? AND b.optional = 0 AND s.item_id IS NOT NULL AND s.deleted = 0
        ORDER BY CASE WHEN ? IS NOT NULL AND s.sync_key = ? THEN 0 ELSE 1 END ASC,
                 s.payload_updated_at_ms DESC, s.history_backfill ASC, s.updated_at DESC, s.local_seq DESC
        LIMIT 1
        "#,
    )
    .bind(blob_id)
    .bind(item_sync_key)
    .bind(item_sync_key)
    .fetch_optional(pool)
    .await
    .context("find item_id for outgoing blob")?;
    Ok(row)
}

pub async fn update_blob_local_source(
    pool: &SqlitePool,
    sync_key: &str,
    blob_id: &str,
    encrypted_path: &str,
    source_path: Option<&str>,
    source_is_directory: bool,
    stream_nonce: Option<&[u8]>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sync_item_blobs
        SET encrypted_path = ?, source_path = ?, source_is_directory = ?,
            stream_nonce = COALESCE(?, stream_nonce), local_available = 1, updated_at = ?
        WHERE sync_key = ? AND blob_id = ?
        "#,
    )
    .bind(encrypted_path)
    .bind(source_path)
    .bind(source_is_directory)
    .bind(stream_nonce)
    .bind(Utc::now())
    .bind(sync_key)
    .bind(blob_id)
    .execute(pool)
    .await
    .context("update local sync item blob source")?;
    Ok(())
}

/// Promotes an item for manual delivery, sets `cloud_blob_policy = 'requested'` for Cloud
/// requests, ensures a positive `local_seq` when Control-Plane metadata still needs pushing,
/// and clears target errors.
pub async fn request_item_delivery(pool: &SqlitePool, item_id: &str, cloud: bool) -> Result<bool> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin manual item delivery request")?;
    let Some((sync_key, current_local_seq)): Option<(String, i64)> =
        sqlx::query_as("SELECT sync_key, local_seq FROM sync_items WHERE item_id = ?")
            .bind(item_id)
            .fetch_optional(&mut *transaction)
            .await
            .context("find sync_key for manual delivery")?
    else {
        transaction.rollback().await.ok();
        return Ok(false);
    };
    let now = Utc::now();

    if cloud {
        let existing_cloud: Option<(String, i64)> = sqlx::query_as(
            "SELECT confirmed_content, pushed_local_seq FROM sync_item_targets \
             WHERE sync_key = ? AND target_id = 'cloud'",
        )
        .bind(&sync_key)
        .fetch_optional(&mut *transaction)
        .await
        .context("check existing cloud target for manual delivery")?;

        let cloud_metadata_current = existing_cloud
            .as_ref()
            .is_some_and(|(confirmed, pushed)| confirmed != "none" && *pushed >= current_local_seq);
        let local_seq = if cloud_metadata_current || current_local_seq > 0 {
            current_local_seq
        } else {
            crate::db::sync_items::next_local_seq_on(&mut transaction).await?
        };
        sqlx::query(
            "UPDATE sync_items SET local_seq = ?, history_backfill = 0, manual_share = 1, \
             cloud_blob_policy = 'requested', updated_at = ? WHERE sync_key = ?",
        )
        .bind(local_seq)
        .bind(now)
        .bind(&sync_key)
        .execute(&mut *transaction)
        .await
        .context("promote item priority for manual cloud delivery")?;

        if cloud_metadata_current {
            sqlx::query(
                "UPDATE sync_item_targets SET last_error = NULL, updated_at = ? \
                 WHERE sync_key = ? AND target_id = 'cloud'",
            )
            .bind(now)
            .bind(&sync_key)
            .execute(&mut *transaction)
            .await
            .context("clear cloud error for metadata-confirmed manual delivery")?;
        } else {
            sqlx::query(
                r#"
                UPDATE sync_item_targets
                SET pushed_local_seq = CASE
                        WHEN confirmed_content = 'complete' THEN pushed_local_seq
                        ELSE 0
                    END,
                    last_error = NULL,
                    updated_at = ?
                WHERE sync_key = ? AND target_id = 'cloud'
                "#,
            )
            .bind(now)
            .bind(&sync_key)
            .execute(&mut *transaction)
            .await
            .context("reset cloud target push watermark for manual delivery")?;
        }
    } else {
        let local_seq = if current_local_seq > 0 {
            current_local_seq
        } else {
            crate::db::sync_items::next_local_seq_on(&mut transaction).await?
        };
        sqlx::query(
            "UPDATE sync_items SET local_seq = ?, history_backfill = 0, manual_share = 1, \
             updated_at = ? WHERE sync_key = ?",
        )
        .bind(local_seq)
        .bind(now)
        .bind(&sync_key)
        .execute(&mut *transaction)
        .await
        .context("promote item priority for manual LAN delivery")?;

        sqlx::query(
            r#"
            UPDATE sync_item_targets
            SET pushed_local_seq = 0,
                last_error = NULL,
                updated_at = ?
            WHERE sync_key = ?
              AND target_id LIKE 'peer:%'
              AND confirmed_content <> 'complete'
            "#,
        )
        .bind(now)
        .bind(&sync_key)
        .execute(&mut *transaction)
        .await
        .context("requeue incomplete LAN peers for manual delivery")?;
    }

    transaction
        .commit()
        .await
        .context("commit manual item delivery request")?;
    Ok(true)
}

pub async fn cancel_delivery(pool: &SqlitePool, target_id: &str, sync_key: &str) -> Result<()> {
    set_terminal_delivery_policy(pool, target_id, sync_key, "canceled", "transfer canceled").await
}

pub async fn block_delivery(
    pool: &SqlitePool,
    target_id: &str,
    sync_key: &str,
    error: &str,
) -> Result<()> {
    set_terminal_delivery_policy(pool, target_id, sync_key, "blocked", error).await
}

async fn set_terminal_delivery_policy(
    pool: &SqlitePool,
    target_id: &str,
    sync_key: &str,
    policy: &str,
    error: &str,
) -> Result<()> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin updating terminal sync delivery policy")?;
    let now = Utc::now();
    let existing_confirmed: Option<String> = sqlx::query_scalar(
        "SELECT confirmed_content FROM sync_item_targets WHERE sync_key = ? AND target_id = ?",
    )
    .bind(sync_key)
    .bind(target_id)
    .fetch_optional(&mut *transaction)
    .await
    .context("read confirmed content before updating terminal delivery policy")?;
    if existing_confirmed.as_deref() == Some("complete") {
        sqlx::query(
            "UPDATE sync_item_targets SET last_error = NULL, updated_at = ? \
             WHERE sync_key = ? AND target_id = ?",
        )
        .bind(now)
        .bind(sync_key)
        .bind(target_id)
        .execute(&mut *transaction)
        .await
        .context("clear error on already-complete sync target")?;
        transaction
            .commit()
            .await
            .context("commit already-complete sync target cleanup")?;
        return Ok(());
    }

    if target_id == CLOUD_TARGET {
        sqlx::query(
            "UPDATE sync_items SET cloud_blob_policy = ?, manual_share = 0, updated_at = ? \
             WHERE sync_key = ?",
        )
        .bind(policy)
        .bind(now)
        .bind(sync_key)
        .execute(&mut *transaction)
        .await
        .context("update sync item cloud_blob_policy")?;
    }

    sqlx::query(
        r#"
        INSERT INTO sync_item_targets (
            sync_key, target_id, confirmed_content,
            last_error, created_at, updated_at
        ) VALUES (?, ?, 'none', ?, ?, ?)
        ON CONFLICT(sync_key, target_id) DO UPDATE SET
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(target_id)
    .bind(error)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("record terminal sync delivery error")?;
    transaction
        .commit()
        .await
        .context("commit terminal sync delivery policy")?;
    Ok(())
}

pub async fn mark_delivery_error(
    pool: &SqlitePool,
    target_id: &str,
    sync_keys: &[String],
    error: &str,
) -> Result<()> {
    if sync_keys.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    for sync_key in sync_keys {
        sqlx::query(
            r#"
            INSERT INTO sync_item_targets (
                sync_key, target_id, confirmed_content,
                last_error, attempts, last_attempt_at, created_at, updated_at
            ) VALUES (?, ?, 'none', ?, 1, ?, ?, ?)
            ON CONFLICT(sync_key, target_id) DO UPDATE SET
                last_error = excluded.last_error,
                attempts = sync_item_targets.attempts + 1,
                last_attempt_at = excluded.last_attempt_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(sync_key)
        .bind(target_id)
        .bind(error)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .context("record sync delivery error")?;
    }
    Ok(())
}

pub async fn is_delivery_canceled(
    pool: &SqlitePool,
    target_id: &str,
    sync_key: &str,
) -> Result<bool> {
    if target_id != CLOUD_TARGET {
        return Ok(false);
    }
    let canceled: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sync_items WHERE sync_key = ? AND cloud_blob_policy IN ('canceled', 'blocked'))",
    )
    .bind(sync_key)
    .fetch_one(pool)
    .await
    .context("check canceled sync delivery")?;
    Ok(canceled)
}

pub struct DownloadedBlobBinding {
    pub blob_id: String,
    pub plaintext_hash: String,
    pub stream_nonce: Vec<u8>,
    pub original_size: u64,
    pub encrypted_size: u64,
    pub source_path: String,
}

pub async fn record_external_download_session(
    pool: &SqlitePool,
    sync_key: &str,
    targets: &[ExternalDownloadTarget],
) -> Result<()> {
    let json = serde_json::to_string(targets).context("encode external download targets")?;
    sqlx::query(
        r#"
        UPDATE sync_items
        SET local_state = 'external',
            external_destinations_json = ?,
            updated_at = ?
        WHERE sync_key = ?
        "#,
    )
    .bind(json)
    .bind(Utc::now())
    .bind(sync_key)
    .execute(pool)
    .await
    .context("record external download session")?;
    Ok(())
}

/// Preserves original blob identity and stream nonce for downloaded external files so a later
/// Cloud share reuses the existing `blob_id` (allowing instant Hub deduplication) instead of
/// minting a new random blob and re-uploading the full file.
pub async fn record_downloaded_external_blobs(
    pool: &SqlitePool,
    group_id: &str,
    sync_key: &str,
    bindings: &[DownloadedBlobBinding],
) -> Result<()> {
    if bindings.is_empty() {
        return Ok(());
    }
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin recording downloaded external blobs")?;
    let now = Utc::now();
    for binding in bindings {
        sqlx::query(
            r#"
            INSERT INTO sync_content_objects (
                group_id, plaintext_hash, blob_id, stream_nonce,
                original_size, encrypted_size, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, plaintext_hash) DO NOTHING
            "#,
        )
        .bind(group_id)
        .bind(&binding.plaintext_hash)
        .bind(&binding.blob_id)
        .bind(&binding.stream_nonce)
        .bind(i64::try_from(binding.original_size).unwrap_or(i64::MAX))
        .bind(i64::try_from(binding.encrypted_size).unwrap_or(i64::MAX))
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("record downloaded sync content object")?;

        sqlx::query(
            r#"
            UPDATE sync_item_blobs
            SET local_available = 1,
                source_path = ?,
                stream_nonce = COALESCE(stream_nonce, ?),
                updated_at = ?
            WHERE sync_key = ? AND blob_id = ?
            "#,
        )
        .bind(&binding.source_path)
        .bind(&binding.stream_nonce)
        .bind(now)
        .bind(sync_key)
        .bind(&binding.blob_id)
        .execute(&mut *transaction)
        .await
        .context("bind downloaded external source to sync_item_blobs")?;
    }
    transaction
        .commit()
        .await
        .context("commit downloaded external blob bindings")?;
    Ok(())
}

pub async fn is_latest_clipboard_item(pool: &SqlitePool, item_id: &str) -> Result<bool> {
    let latest_id: Option<String> = sqlx::query_scalar(
        "SELECT id FROM clipboard_items ORDER BY updated_at DESC, created_at DESC, id ASC LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .context("find latest clipboard item")?;
    Ok(latest_id.as_deref() == Some(item_id))
}

pub async fn enqueue_source_icon_uploads(
    pool: &SqlitePool,
    group_id: &str,
    blobs: &[StoredBlob],
) -> Result<()> {
    if blobs.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    for blob in blobs {
        sqlx::query(
            r#"
            INSERT INTO sync_source_icon_uploads (
                group_id, blob_id, encrypted_path, size, source_path, stream_nonce,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, blob_id) DO UPDATE SET
                encrypted_path = excluded.encrypted_path,
                size = excluded.size,
                source_path = COALESCE(excluded.source_path, sync_source_icon_uploads.source_path),
                stream_nonce = COALESCE(excluded.stream_nonce, sync_source_icon_uploads.stream_nonce),
                updated_at = excluded.updated_at
            "#,
        )
        .bind(group_id)
        .bind(&blob.blob_id)
        .bind(&blob.encrypted_path)
        .bind(i64::try_from(blob.size).unwrap_or(i64::MAX))
        .bind(blob.source_path.as_deref())
        .bind(blob.stream_nonce.as_deref())
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .context("enqueue source icon upload")?;
    }
    Ok(())
}

pub async fn pending_source_icon_uploads(
    pool: &SqlitePool,
    group_id: &str,
    limit: u16,
) -> Result<Vec<StoredBlob>> {
    let rows = sqlx::query(
        r#"
        SELECT blob_id, encrypted_path, size, source_path, stream_nonce
        FROM sync_source_icon_uploads
        WHERE group_id = ?
        ORDER BY created_at ASC
        LIMIT ?
        "#,
    )
    .bind(group_id)
    .bind(i64::from(limit.max(1)))
    .fetch_all(pool)
    .await
    .context("list pending source icon uploads")?;
    rows.into_iter()
        .map(|row| {
            Ok(StoredBlob {
                blob_id: row.try_get("blob_id")?,
                encrypted_path: row.try_get("encrypted_path")?,
                size: u64::try_from(row.try_get::<i64, _>("size")?).unwrap_or(0),
                source_path: row.try_get("source_path")?,
                source_is_directory: false,
                stream_nonce: row.try_get("stream_nonce")?,
            })
        })
        .collect()
}

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

async fn load_blob_ids_map(
    pool: &SqlitePool,
    sync_keys: &[String],
) -> Result<HashMap<String, (Vec<String>, Vec<String>)>> {
    if sync_keys.is_empty() {
        return Ok(HashMap::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT sync_key, blob_id, optional FROM sync_item_blobs WHERE sync_key IN (",
    );
    let mut separated = query.separated(", ");
    for sync_key in sync_keys {
        separated.push_bind(sync_key);
    }
    query.push(") ORDER BY sync_key, blob_id");
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("load sync item blob ids")?;
    let mut map: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
    for row in rows {
        let sync_key: String = row.try_get("sync_key")?;
        let blob_id: String = row.try_get("blob_id")?;
        let optional: bool = row.try_get("optional")?;
        let entry = map.entry(sync_key).or_default();
        if optional {
            entry.1.push(blob_id);
        } else {
            entry.0.push(blob_id);
        }
    }
    Ok(map)
}

type LanBlobLists = (Vec<String>, Vec<String>, Vec<String>);

async fn load_lan_available_blobs_map(
    pool: &SqlitePool,
    sync_keys: &[String],
) -> Result<HashMap<String, LanBlobLists>> {
    if sync_keys.is_empty() {
        return Ok(HashMap::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT sync_key, blob_id, optional, local_available FROM sync_item_blobs WHERE sync_key IN (",
    );
    let mut separated = query.separated(", ");
    for sync_key in sync_keys {
        separated.push_bind(sync_key);
    }
    query.push(") ORDER BY sync_key, blob_id");
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("load LAN available sync item blob ids")?;
    let mut map: HashMap<String, LanBlobLists> = HashMap::new();
    for row in rows {
        let sync_key: String = row.try_get("sync_key")?;
        let blob_id: String = row.try_get("blob_id")?;
        let optional: bool = row.try_get("optional")?;
        let local_available: bool = row.try_get("local_available")?;
        let entry = map.entry(sync_key).or_default();
        if optional {
            entry.1.push(blob_id.clone());
        } else {
            entry.0.push(blob_id.clone());
        }
        if local_available {
            entry.2.push(blob_id);
        }
    }
    Ok(map)
}

/// Metadata needed to track passive LAN blob transfers (`GetBlob`) in the transfer center.
#[derive(Debug, Clone)]
pub struct BlobServeInfo {
    pub sync_key: String,
    pub item_id: String,
    pub kind: String,
    pub history_backfill: bool,
    pub manual_share: bool,
    pub payload_updated_at_ms: i64,
    pub single_file_name: Option<String>,
}

/// Resolves card identity and transfer tracking attributes for an outgoing `GetBlob`.
pub async fn blob_serve_info(
    pool: &SqlitePool,
    blob_id: &str,
    item_sync_key: Option<&str>,
) -> Result<Option<BlobServeInfo>> {
    let row = sqlx::query(
        r#"
        SELECT s.sync_key, s.item_id, s.kind, s.history_backfill, s.manual_share,
               s.payload_updated_at_ms, c.content
        FROM sync_item_blobs b
        JOIN sync_items s ON s.sync_key = b.sync_key
        LEFT JOIN clipboard_items c ON c.id = s.item_id
        WHERE b.blob_id = ? AND b.optional = 0 AND s.item_id IS NOT NULL AND s.deleted = 0
        ORDER BY CASE WHEN ? IS NOT NULL AND s.sync_key = ? THEN 0 ELSE 1 END ASC,
                 s.payload_updated_at_ms DESC, s.history_backfill ASC, s.updated_at DESC, s.local_seq DESC
        LIMIT 1
        "#,
    )
    .bind(blob_id)
    .bind(item_sync_key)
    .bind(item_sync_key)
    .fetch_optional(pool)
    .await
    .context("find clipboard item info for outgoing blob")?;
    let Some(row) = row else {
        return Ok(None);
    };
    let sync_key: String = row.try_get("sync_key")?;
    let item_id: String = row.try_get("item_id")?;
    let kind: String = row.try_get("kind")?;
    let history_backfill: bool = row.try_get("history_backfill")?;
    let manual_share: bool = row.try_get("manual_share")?;
    let payload_updated_at_ms: i64 = row.try_get("payload_updated_at_ms")?;
    let content: Option<String> = row.try_get("content")?;
    let single_file_name = if kind == "files" {
        let lines = content
            .as_deref()
            .unwrap_or("")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();
        if lines.len() == 1 {
            std::path::Path::new(lines[0])
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        } else {
            None
        }
    } else {
        None
    };
    Ok(Some(BlobServeInfo {
        sync_key,
        item_id,
        kind,
        history_backfill,
        manual_share,
        payload_updated_at_ms,
        single_file_name,
    }))
}

/// Counts remote items awaiting automatic attachment pull from currently reachable sources.
pub async fn count_reachable_pending_blob_pulls(
    pool: &SqlitePool,
    cloud_enabled: bool,
    online_peer_ids: &[String],
) -> Result<u64> {
    if !cloud_enabled && online_peer_ids.is_empty() {
        return Ok(0);
    }
    let mut query = sqlx::QueryBuilder::new(
        r#"
        SELECT COUNT(*)
        FROM sync_items s
        WHERE s.needs_blob_pull = 1
          AND s.deleted = 0
          AND s.item_key IS NOT NULL
          AND s.nonce IS NOT NULL
          AND s.ciphertext IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM sync_item_targets t
              WHERE t.sync_key = s.sync_key
                AND t.confirmed_content = 'complete'
                AND (
        "#,
    );
    let mut has_clause = false;
    if cloud_enabled {
        query.push("t.target_id = 'cloud'");
        has_clause = true;
    }
    if !online_peer_ids.is_empty() {
        if has_clause {
            query.push(" OR ");
        }
        query.push("t.target_id IN (");
        let mut separated = query.separated(", ");
        for peer_id in online_peer_ids {
            separated.push_bind(format!("peer:{peer_id}"));
        }
        query.push(")");
    }
    query.push("))");
    let count: i64 = query
        .build_query_scalar()
        .fetch_one(pool)
        .await
        .context("count reachable pending remote blob pulls")?;
    Ok(u64::try_from(count).unwrap_or(0))
}

/// Summarizes reachable pending automatic attachment pulls alongside the active source routes
/// that can satisfy them (`attachment_worker` prefers online LAN peers before Cloud).
pub async fn reachable_pending_blob_pull_summary(
    pool: &SqlitePool,
    cloud_enabled: bool,
    online_peer_ids: &[String],
) -> Result<(u64, crate::sync::model::SyncInboundCatchupSources)> {
    let remaining =
        count_reachable_pending_blob_pulls(pool, cloud_enabled, online_peer_ids).await?;
    if remaining == 0 {
        return Ok((0, crate::sync::model::SyncInboundCatchupSources::default()));
    }

    let mut sources = crate::sync::model::SyncInboundCatchupSources::default();
    if !online_peer_ids.is_empty() {
        let mut query = sqlx::QueryBuilder::new(
            r#"
            SELECT DISTINCT p.device_name
            FROM sync_peers p
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
              AND EXISTS (
                  SELECT 1
                  FROM sync_items s
                  JOIN sync_item_targets t ON t.sync_key = s.sync_key
                  WHERE s.needs_blob_pull = 1
                    AND s.deleted = 0
                    AND s.item_key IS NOT NULL
                    AND s.nonce IS NOT NULL
                    AND s.ciphertext IS NOT NULL
                    AND t.target_id = ('peer:' || p.device_id)
                    AND t.confirmed_content = 'complete'
              )
            ORDER BY p.device_name ASC, p.device_id ASC
            "#,
        );
        let rows: Vec<String> = query
            .build_query_scalar()
            .fetch_all(pool)
            .await
            .context("list LAN sources for pending blob pulls")?;
        for name in rows {
            let trimmed = name.trim();
            if !trimmed.is_empty() && !sources.peers.iter().any(|existing| existing == trimmed) {
                sources.peers.push(trimmed.to_owned());
            }
        }
    }

    if cloud_enabled {
        let mut query = sqlx::QueryBuilder::new(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM sync_items s
                JOIN sync_item_targets t ON t.sync_key = s.sync_key
                WHERE s.needs_blob_pull = 1
                  AND s.deleted = 0
                  AND s.item_key IS NOT NULL
                  AND s.nonce IS NOT NULL
                  AND s.ciphertext IS NOT NULL
                  AND t.target_id = 'cloud'
                  AND t.confirmed_content = 'complete'
            "#,
        );
        if !online_peer_ids.is_empty() {
            query.push(
                r#"
                  AND NOT EXISTS (
                      SELECT 1
                      FROM sync_item_targets tp
                      WHERE tp.sync_key = s.sync_key
                        AND tp.confirmed_content = 'complete'
                        AND tp.target_id IN (
                "#,
            );
            let mut separated = query.separated(", ");
            for peer_id in online_peer_ids {
                separated.push_bind(format!("peer:{peer_id}"));
            }
            query.push("))");
        }
        query.push(")");
        let needs_cloud: i64 = query
            .build_query_scalar()
            .fetch_one(pool)
            .await
            .context("check Cloud source for pending blob pulls")?;
        sources.cloud = needs_cloud != 0;
    }

    Ok((remaining, sources))
}

/// Builds a compact `SyncedItemSnapshot` from a `StoredSyncItemMeta` row (for tombstones or fallback).
pub fn snapshot_from_meta(meta: &StoredSyncItemMeta, local_device_id: &str) -> SyncedItemSnapshot {
    let resolve = |version: &FieldVersion| {
        if version.seq > 0 && version.device_id.is_empty() {
            FieldVersion::new(version.seq, local_device_id)
        } else {
            version.clone()
        }
    };
    SyncedItemSnapshot {
        version: SNAPSHOT_VERSION,
        sync_key: meta.sync_key.clone(),
        content_version: resolve(&meta.content_version),
        presence_version: resolve(&meta.presence_version),
        favorite_version: resolve(&meta.favorite_version),
        pinned_version: resolve(&meta.pinned_version),
        note_version: resolve(&meta.note_version),
        is_present: meta.is_present,
        is_favorite: meta.is_favorite,
        is_pinned: meta.is_pinned,
        note: meta.note.clone(),
        item: None,
        blobs: Vec::new(),
        source_app: None,
        content_identity: meta.content_identity.clone(),
    }
}
