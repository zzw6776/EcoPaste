use anyhow::Context;
use chrono::Utc;
use sqlx::{AssertSqlSafe, FromRow, SqliteConnection, SqlitePool};

use crate::{core::Result, db::models::ClipboardKind, sync::SyncedItemStatePatch};

pub const FAVORITE_DIRTY: i64 = 1;
pub const PINNED_DIRTY: i64 = 1 << 1;
pub const NOTE_DIRTY: i64 = 1 << 2;
pub const PRESENCE_DIRTY: i64 = 1 << 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncEventVersion {
    pub origin_sequence: u64,
    pub origin_device_id: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct DirtyItemState {
    pub sync_key: String,
    pub is_favorite: Option<bool>,
    pub is_pinned: Option<bool>,
    pub note: Option<String>,
    pub is_present: Option<bool>,
    pub dirty_mask: i64,
    pub local_revision: i64,
    pub pending_order: i64,
}

impl DirtyItemState {
    pub fn patch(&self) -> SyncedItemStatePatch {
        SyncedItemStatePatch {
            sync_key: self.sync_key.clone(),
            is_favorite: (self.dirty_mask & FAVORITE_DIRTY != 0)
                .then_some(self.is_favorite)
                .flatten(),
            is_pinned: (self.dirty_mask & PINNED_DIRTY != 0)
                .then_some(self.is_pinned)
                .flatten(),
            note_changed: self.dirty_mask & NOTE_DIRTY != 0,
            note: self.note.clone(),
            is_present: (self.dirty_mask & PRESENCE_DIRTY != 0)
                .then_some(self.is_present)
                .flatten(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ApplyStateOutcome {
    pub item_id: Option<String>,
    pub image_files: Vec<String>,
    pub presence_accepted: bool,
    pub canonical_presence: Option<bool>,
}

/// Records a local favorite mutation as durable synchronization work.
pub async fn mark_local_favorite(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
    value: bool,
) -> Result<()> {
    mark_local_bool(
        connection,
        sync_key,
        is_sensitive,
        "is_favorite",
        value,
        FAVORITE_DIRTY,
    )
    .await
}

/// Records a local pin mutation as durable synchronization work.
pub async fn mark_local_pinned(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
    value: bool,
) -> Result<()> {
    mark_local_bool(
        connection,
        sync_key,
        is_sensitive,
        "is_pinned",
        value,
        PINNED_DIRTY,
    )
    .await
}

async fn mark_local_bool(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
    column: &str,
    value: bool,
    dirty_bit: i64,
) -> Result<()> {
    let pending_order = crate::db::sync_queue::next_operation_order(connection).await?;
    let now = Utc::now();
    let query = format!(
        r#"
        INSERT INTO sync_item_states (
            sync_key, is_sensitive, {column}, dirty_mask, local_revision,
            pending_order, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            is_sensitive = excluded.is_sensitive,
            {column} = excluded.{column},
            dirty_mask = sync_item_states.dirty_mask | excluded.dirty_mask,
            local_revision = sync_item_states.local_revision + 1,
            pending_order = excluded.pending_order,
            updated_at = excluded.updated_at
        "#,
    );
    sqlx::query(AssertSqlSafe(query))
        .bind(sync_key)
        .bind(is_sensitive)
        .bind(value)
        .bind(dirty_bit)
        .bind(pending_order)
        .bind(now)
        .bind(now)
        .execute(connection)
        .await
        .context("record local synchronized boolean state")?;
    Ok(())
}

/// Records a local note mutation; `None` is an explicit clear operation.
pub async fn mark_local_note(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
    note: Option<&str>,
) -> Result<()> {
    let pending_order = crate::db::sync_queue::next_operation_order(connection).await?;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_item_states (
            sync_key, is_sensitive, note, dirty_mask, local_revision,
            pending_order, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            is_sensitive = excluded.is_sensitive,
            note = excluded.note,
            dirty_mask = sync_item_states.dirty_mask | excluded.dirty_mask,
            local_revision = sync_item_states.local_revision + 1,
            pending_order = excluded.pending_order,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(is_sensitive)
    .bind(note)
    .bind(NOTE_DIRTY)
    .bind(pending_order)
    .bind(now)
    .bind(now)
    .execute(connection)
    .await
    .context("record local synchronized note state")?;
    Ok(())
}

/// Records a local delete as durable synchronization work.
pub async fn mark_local_deleted(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
) -> Result<()> {
    let pending_order = crate::db::sync_queue::next_operation_order(connection).await?;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_item_states (
            sync_key, is_sensitive, is_present, dirty_mask, local_revision,
            pending_order, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            is_sensitive = excluded.is_sensitive,
            is_present = excluded.is_present,
            dirty_mask = sync_item_states.dirty_mask | excluded.dirty_mask,
            local_revision = sync_item_states.local_revision + 1,
            pending_order = excluded.pending_order,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(sync_key)
    .bind(is_sensitive)
    .bind(false)
    .bind(PRESENCE_DIRTY)
    .bind(pending_order)
    .bind(now)
    .bind(now)
    .execute(connection)
    .await
    .context("record local synchronized presence state")?;
    Ok(())
}

pub async fn dirty_states(
    pool: &SqlitePool,
    limit: u16,
    sync_sensitive: bool,
) -> Result<Vec<DirtyItemState>> {
    Ok(sqlx::query_as::<_, DirtyItemState>(
        r#"
        SELECT sync_key, is_favorite, is_pinned, note, is_present,
               CASE
                   WHEN ? OR is_sensitive = 0 THEN dirty_mask
                   ELSE dirty_mask & ?
               END AS dirty_mask,
               local_revision, pending_order
        FROM sync_item_states state
        WHERE dirty_mask <> 0
          AND pending_order IS NOT NULL
          AND (
              ? OR is_sensitive = 0
              OR ((dirty_mask & ?) <> 0 AND presence_origin_sequence IS NOT NULL)
          )
          AND NOT EXISTS (
              SELECT 1 FROM sync_content_outbox content
              WHERE content.sync_key = state.sync_key
                AND content.operation_order < state.pending_order
          )
        ORDER BY pending_order ASC
        LIMIT ?
        "#,
    )
    .bind(sync_sensitive)
    .bind(PRESENCE_DIRTY)
    .bind(sync_sensitive)
    .bind(PRESENCE_DIRTY)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("load dirty synchronized item states")?)
}

/// Records the version assigned to a durable local state event and clears only the exact revision
/// that was serialized. A concurrent local mutation leaves the row dirty for the next pass.
#[cfg(test)]
pub async fn mark_state_sent(
    pool: &SqlitePool,
    state: &DirtyItemState,
    version: &SyncEventVersion,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin synchronized state send")?;
    mark_state_sent_on(&mut transaction, state, version).await?;

    transaction
        .commit()
        .await
        .context("commit synchronized state send")?;
    Ok(())
}

/// Finalizes a local state revision inside the caller's event-log transaction.
pub async fn mark_state_sent_on(
    connection: &mut SqliteConnection,
    state: &DirtyItemState,
    version: &SyncEventVersion,
) -> Result<()> {
    let sequence =
        i64::try_from(version.origin_sequence).context("sync state version too large")?;
    let now = Utc::now();

    if state.dirty_mask & FAVORITE_DIRTY != 0 {
        set_version(
            connection,
            &state.sync_key,
            "favorite",
            sequence,
            &version.origin_device_id,
            state.local_revision,
        )
        .await?;
    }
    if state.dirty_mask & PINNED_DIRTY != 0 {
        set_version(
            connection,
            &state.sync_key,
            "pinned",
            sequence,
            &version.origin_device_id,
            state.local_revision,
        )
        .await?;
    }
    if state.dirty_mask & NOTE_DIRTY != 0 {
        set_version(
            connection,
            &state.sync_key,
            "note",
            sequence,
            &version.origin_device_id,
            state.local_revision,
        )
        .await?;
    }
    if state.dirty_mask & PRESENCE_DIRTY != 0 {
        set_version(
            connection,
            &state.sync_key,
            "presence",
            sequence,
            &version.origin_device_id,
            state.local_revision,
        )
        .await?;
    }

    sqlx::query(
        r#"
        UPDATE sync_item_states
        SET dirty_mask = dirty_mask & ~?,
            pending_order = CASE
                WHEN (dirty_mask & ~?) = 0 THEN NULL
                ELSE pending_order
            END,
            updated_at = ?
        WHERE sync_key = ? AND local_revision = ?
        "#,
    )
    .bind(state.dirty_mask)
    .bind(state.dirty_mask)
    .bind(now)
    .bind(&state.sync_key)
    .bind(state.local_revision)
    .execute(connection)
    .await
    .context("clear sent synchronized item state")?;

    Ok(())
}

async fn set_version(
    connection: &mut SqliteConnection,
    sync_key: &str,
    field: &str,
    sequence: i64,
    device_id: &str,
    local_revision: i64,
) -> Result<()> {
    let query = format!(
        "UPDATE sync_item_states SET {field}_origin_sequence = ?, {field}_origin_device_id = ? WHERE sync_key = ? AND local_revision = ?",
    );
    sqlx::query(AssertSqlSafe(query))
        .bind(sequence)
        .bind(device_id)
        .bind(sync_key)
        .bind(local_revision)
        .execute(connection)
        .await
        .context("record synchronized item state version")?;
    Ok(())
}

/// Merges an incoming state patch per field. The operation is idempotent and may run before the
/// corresponding clipboard content exists.
pub async fn apply_remote_patch(
    pool: &SqlitePool,
    patch: &SyncedItemStatePatch,
    version: &SyncEventVersion,
) -> Result<ApplyStateOutcome> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin synchronized state merge")?;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO sync_item_states (sync_key, created_at, updated_at)
        VALUES (?, ?, ?)
        "#,
    )
    .bind(&patch.sync_key)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("ensure synchronized item state")?;

    if let Some(value) = patch.is_favorite {
        merge_bool_field(
            &mut transaction,
            &patch.sync_key,
            "is_favorite",
            "favorite",
            FAVORITE_DIRTY,
            value,
            version,
        )
        .await?;
    }
    if let Some(value) = patch.is_pinned {
        merge_bool_field(
            &mut transaction,
            &patch.sync_key,
            "is_pinned",
            "pinned",
            PINNED_DIRTY,
            value,
            version,
        )
        .await?;
    }
    if patch.note_changed {
        merge_note_field(&mut transaction, patch, version).await?;
    }

    let presence_accepted = if let Some(value) = patch.is_present {
        merge_bool_field(
            &mut transaction,
            &patch.sync_key,
            "is_present",
            "presence",
            PRESENCE_DIRTY,
            value,
            version,
        )
        .await?
    } else {
        false
    };

    let canonical_presence: Option<bool> = sqlx::query_scalar(
        "SELECT is_present FROM sync_item_states WHERE sync_key = ? AND presence_origin_sequence IS NOT NULL",
    )
    .bind(&patch.sync_key)
    .fetch_optional(&mut *transaction)
    .await
    .context("read merged synchronized item presence")?
    .flatten();

    let mut image_files = Vec::new();
    if canonical_presence == Some(false) {
        let rows = sqlx::query_as::<_, (ClipboardKind, String)>(
            "DELETE FROM clipboard_items WHERE sync_key = ? RETURNING kind, content",
        )
        .bind(&patch.sync_key)
        .fetch_all(&mut *transaction)
        .await
        .context("delete clipboard item from synchronized state")?;
        image_files.extend(
            rows.into_iter()
                .filter_map(|(kind, content)| (kind == ClipboardKind::Image).then_some(content)),
        );
    } else {
        materialize_state(&mut transaction, &patch.sync_key).await?;
    }

    let item_id = sqlx::query_scalar(
        "SELECT id FROM clipboard_items WHERE sync_key = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(&patch.sync_key)
    .fetch_optional(&mut *transaction)
    .await
    .context("find synchronized item state target")?;

    transaction
        .commit()
        .await
        .context("commit synchronized state merge")?;
    Ok(ApplyStateOutcome {
        item_id,
        image_files,
        presence_accepted,
        canonical_presence,
    })
}

async fn merge_bool_field(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    sync_key: &str,
    value_column: &str,
    version_prefix: &str,
    dirty_bit: i64,
    value: bool,
    version: &SyncEventVersion,
) -> Result<bool> {
    let sequence =
        i64::try_from(version.origin_sequence).context("sync state version too large")?;
    let query = format!(
        r#"
        UPDATE sync_item_states
        SET {value_column} = ?,
            {version_prefix}_origin_sequence = ?,
            {version_prefix}_origin_device_id = ?,
            updated_at = ?
        WHERE sync_key = ?
          AND (dirty_mask & ?) = 0
          AND (
              {version_prefix}_origin_sequence IS NULL
              OR {version_prefix}_origin_sequence < ?
              OR (
                  {version_prefix}_origin_sequence = ?
                  AND COALESCE({version_prefix}_origin_device_id, '') < ?
              )
          )
        "#,
    );
    let result = sqlx::query(AssertSqlSafe(query))
        .bind(value)
        .bind(sequence)
        .bind(&version.origin_device_id)
        .bind(Utc::now())
        .bind(sync_key)
        .bind(dirty_bit)
        .bind(sequence)
        .bind(sequence)
        .bind(&version.origin_device_id)
        .execute(&mut **transaction)
        .await
        .context("merge synchronized boolean field")?;
    Ok(result.rows_affected() == 1)
}

async fn merge_note_field(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    patch: &SyncedItemStatePatch,
    version: &SyncEventVersion,
) -> Result<bool> {
    let sequence =
        i64::try_from(version.origin_sequence).context("sync state version too large")?;
    let result = sqlx::query(
        r#"
        UPDATE sync_item_states
        SET note = ?, note_origin_sequence = ?, note_origin_device_id = ?, updated_at = ?
        WHERE sync_key = ?
          AND (dirty_mask & ?) = 0
          AND (
              note_origin_sequence IS NULL
              OR note_origin_sequence < ?
              OR (
                  note_origin_sequence = ?
                  AND COALESCE(note_origin_device_id, '') < ?
              )
          )
        "#,
    )
    .bind(patch.note.as_deref())
    .bind(sequence)
    .bind(&version.origin_device_id)
    .bind(Utc::now())
    .bind(&patch.sync_key)
    .bind(NOTE_DIRTY)
    .bind(sequence)
    .bind(sequence)
    .bind(&version.origin_device_id)
    .execute(&mut **transaction)
    .await
    .context("merge synchronized note field")?;
    Ok(result.rows_affected() == 1)
}

async fn materialize_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    sync_key: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE clipboard_items
        SET is_favorite = COALESCE(
                (SELECT is_favorite FROM sync_item_states WHERE sync_key = ?),
                is_favorite
            ),
            is_pinned = COALESCE(
                (SELECT is_pinned FROM sync_item_states WHERE sync_key = ?),
                is_pinned
            ),
            note = CASE
                WHEN (SELECT note_origin_sequence FROM sync_item_states WHERE sync_key = ?) IS NOT NULL
                THEN (SELECT note FROM sync_item_states WHERE sync_key = ?)
                ELSE note
            END
        WHERE sync_key = ?
        "#,
    )
    .bind(sync_key)
    .bind(sync_key)
    .bind(sync_key)
    .bind(sync_key)
    .bind(sync_key)
    .execute(&mut **transaction)
    .await
    .context("materialize synchronized item state")?;
    Ok(())
}

/// Applies retained state after clipboard content has been inserted.
pub async fn materialize_for_content(pool: &SqlitePool, sync_key: &str) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin item state materialization")?;
    materialize_state(&mut transaction, sync_key).await?;
    transaction
        .commit()
        .await
        .context("commit item state materialization")?;
    Ok(())
}

#[cfg(test)]
async fn is_present(pool: &SqlitePool, sync_key: &str) -> Result<Option<bool>> {
    Ok(sqlx::query_scalar(
        "SELECT is_present FROM sync_item_states WHERE sync_key = ? AND presence_origin_sequence IS NOT NULL",
    )
    .bind(sync_key)
    .fetch_optional(pool)
    .await
    .context("read synchronized item presence")?
    .flatten())
}

/// Records the versioned presence implied by a locally originated content event.
#[cfg(test)]
pub async fn record_content_presence(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
    version: &SyncEventVersion,
) -> Result<()> {
    record_content_presence_at_order(connection, sync_key, is_sensitive, version, None).await
}

/// Records content presence without superseding a later local delete staged during materialization.
pub async fn record_content_presence_at_order(
    connection: &mut SqliteConnection,
    sync_key: &str,
    is_sensitive: bool,
    version: &SyncEventVersion,
    operation_order: Option<i64>,
) -> Result<()> {
    if let Some(operation_order) = operation_order {
        let has_later_delete = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM sync_item_states
                WHERE sync_key = ?
                  AND (dirty_mask & ?) <> 0
                  AND pending_order > ?
            )
            "#,
        )
        .bind(sync_key)
        .bind(PRESENCE_DIRTY)
        .bind(operation_order)
        .fetch_one(&mut *connection)
        .await
        .context("check later synchronized presence mutation")?
            != 0;
        if has_later_delete {
            return Ok(());
        }
    }

    let sequence =
        i64::try_from(version.origin_sequence).context("sync state version too large")?;
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_item_states (
            sync_key, is_sensitive, is_present, presence_origin_sequence,
            presence_origin_device_id, dirty_mask, local_revision, created_at, updated_at
        )
        SELECT ?, ?, 1, ?, ?, 0, 1, ?, ?
        WHERE EXISTS (SELECT 1 FROM clipboard_items WHERE sync_key = ?)
        ON CONFLICT(sync_key) DO UPDATE SET
            is_sensitive = excluded.is_sensitive,
            is_present = 1,
            presence_origin_sequence = excluded.presence_origin_sequence,
            presence_origin_device_id = excluded.presence_origin_device_id,
            dirty_mask = sync_item_states.dirty_mask & ~?,
            pending_order = CASE
                WHEN (sync_item_states.dirty_mask & ~?) = 0 THEN NULL
                ELSE sync_item_states.pending_order
            END,
            local_revision = sync_item_states.local_revision + 1,
            updated_at = excluded.updated_at
        WHERE EXISTS (SELECT 1 FROM clipboard_items WHERE sync_key = excluded.sync_key)
        "#,
    )
    .bind(sync_key)
    .bind(is_sensitive)
    .bind(sequence)
    .bind(&version.origin_device_id)
    .bind(now)
    .bind(now)
    .bind(sync_key)
    .bind(PRESENCE_DIRTY)
    .bind(PRESENCE_DIRTY)
    .execute(connection)
    .await
    .context("record local synchronized content presence")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;
    use crate::db::{
        items::{content_hash, find_item_by_id, insert_item},
        models::{ClipboardItem, Platform},
        test_support::memory_pool,
    };

    fn sample_item(id: &str, sync_key: &str) -> ClipboardItem {
        let content = format!("content-{id}");
        let now = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        ClipboardItem {
            id: id.to_owned(),
            kind: ClipboardKind::Text,
            sub_kind: None,
            group_id: None,
            source_app_id: None,
            source_revision: id.to_owned(),
            content_hash: content_hash(ClipboardKind::Text, &content),
            sync_key: sync_key.to_owned(),
            content,
            search_text: None,
            summary: None,
            text_char_count: None,
            file_types: None,
            size: None,
            width: None,
            height: None,
            use_count: 1,
            is_favorite: false,
            is_pinned: false,
            is_sensitive: false,
            platform: Platform::Macos,
            note: None,
            created_at: now,
            updated_at: now,
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            source_app_accent_start: None,
            source_app_accent_end: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        }
    }

    fn version(sequence: u64, device_id: &str) -> SyncEventVersion {
        SyncEventVersion {
            origin_sequence: sequence,
            origin_device_id: device_id.to_owned(),
        }
    }

    #[tokio::test]
    async fn missing_content_state_is_materialized_when_content_arrives() {
        let pool = memory_pool().await;
        let patch = SyncedItemStatePatch {
            sync_key: "stable-key".into(),
            is_favorite: Some(true),
            is_pinned: None,
            note_changed: true,
            note: Some("remote note".into()),
            is_present: None,
        };

        let outcome = apply_remote_patch(&pool, &patch, &version(2, "device-b"))
            .await
            .unwrap();
        assert_eq!(outcome.item_id, None);

        let item = sample_item("late-content", "stable-key");
        insert_item(&pool, &item).await.unwrap();
        materialize_for_content(&pool, "stable-key").await.unwrap();

        let materialized = find_item_by_id(&pool, &item.id).await.unwrap().unwrap();
        assert!(materialized.is_favorite);
        assert_eq!(materialized.note.as_deref(), Some("remote note"));
    }

    #[tokio::test]
    async fn independent_fields_converge_in_any_arrival_order() {
        let first_pool = memory_pool().await;
        let second_pool = memory_pool().await;
        for pool in [&first_pool, &second_pool] {
            insert_item(pool, &sample_item("item", "stable-key"))
                .await
                .unwrap();
        }
        let favorite = SyncedItemStatePatch {
            sync_key: "stable-key".into(),
            is_favorite: Some(true),
            is_pinned: None,
            note_changed: false,
            note: None,
            is_present: None,
        };
        let note = SyncedItemStatePatch {
            sync_key: "stable-key".into(),
            is_favorite: None,
            is_pinned: None,
            note_changed: true,
            note: Some("note".into()),
            is_present: None,
        };

        apply_remote_patch(&first_pool, &favorite, &version(1, "device-a"))
            .await
            .unwrap();
        apply_remote_patch(&first_pool, &note, &version(2, "device-b"))
            .await
            .unwrap();
        apply_remote_patch(&second_pool, &note, &version(2, "device-b"))
            .await
            .unwrap();
        apply_remote_patch(&second_pool, &favorite, &version(1, "device-a"))
            .await
            .unwrap();

        for pool in [&first_pool, &second_pool] {
            let item = find_item_by_id(pool, "item").await.unwrap().unwrap();
            assert!(item.is_favorite);
            assert_eq!(item.note.as_deref(), Some("note"));
        }
    }

    #[tokio::test]
    async fn newer_content_presence_does_not_overwrite_metadata_fields() {
        let pool = memory_pool().await;
        let item = sample_item("item", "stable-key");
        insert_item(&pool, &item).await.unwrap();
        let metadata = SyncedItemStatePatch {
            sync_key: "stable-key".into(),
            is_favorite: Some(true),
            is_pinned: None,
            note_changed: true,
            note: Some("keep me".into()),
            is_present: None,
        };
        apply_remote_patch(&pool, &metadata, &version(1, "device-a"))
            .await
            .unwrap();

        let mut connection = pool.acquire().await.unwrap();
        record_content_presence(
            &mut connection,
            "stable-key",
            false,
            &version(99, "device-b"),
        )
        .await
        .unwrap();
        drop(connection);

        let materialized = find_item_by_id(&pool, "item").await.unwrap().unwrap();
        assert!(materialized.is_favorite);
        assert_eq!(materialized.note.as_deref(), Some("keep me"));
    }

    #[tokio::test]
    async fn local_content_event_supersedes_an_unsent_delete() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("item", "stable-key"))
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        mark_local_deleted(&mut connection, "stable-key", false)
            .await
            .unwrap();
        drop(connection);

        let mut connection = pool.acquire().await.unwrap();
        record_content_presence(
            &mut connection,
            "stable-key",
            false,
            &version(2, "device-a"),
        )
        .await
        .unwrap();
        drop(connection);

        assert_eq!(is_present(&pool, "stable-key").await.unwrap(), Some(true));
        assert!(dirty_states(&pool, 10, false).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn earlier_content_materialization_preserves_a_later_local_delete() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("item", "stable-key"))
            .await
            .unwrap();

        let mut transaction = pool.begin().await.unwrap();
        let content_order = crate::db::sync_queue::next_operation_order(&mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let mut connection = pool.acquire().await.unwrap();
        mark_local_deleted(&mut connection, "stable-key", false)
            .await
            .unwrap();
        record_content_presence_at_order(
            &mut connection,
            "stable-key",
            false,
            &version(2, "device-a"),
            Some(content_order),
        )
        .await
        .unwrap();
        drop(connection);

        let state = dirty_states(&pool, 10, false).await.unwrap().pop().unwrap();
        assert_eq!(state.patch().is_present, Some(false));
        let stored_presence: Option<bool> =
            sqlx::query_scalar("SELECT is_present FROM sync_item_states WHERE sync_key = ?")
                .bind("stable-key")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored_presence, Some(false));
    }

    #[tokio::test]
    async fn local_queue_orders_content_before_a_later_delete_for_the_same_item() {
        let pool = memory_pool().await;
        let item = sample_item("item", "stable-key");
        insert_item(&pool, &item).await.unwrap();
        let mut transaction = pool.begin().await.unwrap();
        let content_order = crate::db::sync_queue::stage_content(
            &mut transaction,
            &item.id,
            &item.sync_key,
            &item.source_revision,
            false,
            false,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        mark_local_deleted(&mut connection, &item.sync_key, false)
            .await
            .unwrap();
        drop(connection);

        let content = crate::db::sync_queue::next_ready_content(&pool, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(content.operation_order, content_order);
        assert!(dirty_states(&pool, 1, false).await.unwrap().is_empty());

        crate::db::sync_queue::complete_content(&pool, content_order)
            .await
            .unwrap();
        assert_eq!(
            dirty_states(&pool, 1, false)
                .await
                .unwrap()
                .pop()
                .unwrap()
                .patch()
                .is_present,
            Some(false)
        );
    }

    #[tokio::test]
    async fn local_queue_orders_delete_before_later_content_for_the_same_item() {
        let pool = memory_pool().await;
        let item = sample_item("item", "stable-key");
        insert_item(&pool, &item).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        mark_local_deleted(&mut connection, &item.sync_key, false)
            .await
            .unwrap();
        drop(connection);
        let mut transaction = pool.begin().await.unwrap();
        crate::db::sync_queue::stage_content(
            &mut transaction,
            &item.id,
            &item.sync_key,
            &item.source_revision,
            false,
            false,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        assert!(crate::db::sync_queue::next_ready_content(&pool, false)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            dirty_states(&pool, 1, false)
                .await
                .unwrap()
                .pop()
                .unwrap()
                .patch()
                .is_present,
            Some(false)
        );
    }

    #[tokio::test]
    async fn newer_presence_resurrects_but_older_content_cannot() {
        let pool = memory_pool().await;
        let item = sample_item("item", "stable-key");
        insert_item(&pool, &item).await.unwrap();
        let delete = SyncedItemStatePatch {
            sync_key: "stable-key".into(),
            is_favorite: None,
            is_pinned: None,
            note_changed: false,
            note: None,
            is_present: Some(false),
        };
        apply_remote_patch(&pool, &delete, &version(2, "device-a"))
            .await
            .unwrap();
        assert!(find_item_by_id(&pool, "item").await.unwrap().is_none());

        let stale_present = SyncedItemStatePatch {
            is_present: Some(true),
            ..delete.clone()
        };
        insert_item(&pool, &sample_item("stale-item", "stable-key"))
            .await
            .unwrap();
        let outcome = apply_remote_patch(&pool, &stale_present, &version(1, "device-b"))
            .await
            .unwrap();
        assert_eq!(outcome.canonical_presence, Some(false));
        assert_eq!(is_present(&pool, "stable-key").await.unwrap(), Some(false));
        assert!(find_item_by_id(&pool, "stale-item")
            .await
            .unwrap()
            .is_none());

        apply_remote_patch(&pool, &stale_present, &version(3, "device-b"))
            .await
            .unwrap();
        assert_eq!(is_present(&pool, "stable-key").await.unwrap(), Some(true));
    }

    #[tokio::test]
    async fn local_state_remains_dirty_until_its_revision_is_stored() {
        let pool = memory_pool().await;
        let mut transaction = pool.begin().await.unwrap();
        mark_local_note(&mut transaction, "stable-key", false, Some("note"))
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let state = dirty_states(&pool, 10, false).await.unwrap().pop().unwrap();
        assert_eq!(state.patch().note.as_deref(), Some("note"));
        mark_state_sent(&pool, &state, &version(1, "device-a"))
            .await
            .unwrap();
        assert!(dirty_states(&pool, 10, false).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sensitive_metadata_waits_until_sensitive_sync_is_enabled() {
        let pool = memory_pool().await;
        let mut transaction = pool.begin().await.unwrap();
        mark_local_note(&mut transaction, "sensitive-key", true, Some("private"))
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        assert!(dirty_states(&pool, 10, false).await.unwrap().is_empty());
        let state = dirty_states(&pool, 10, true).await.unwrap().pop().unwrap();
        assert_eq!(state.patch().note.as_deref(), Some("private"));
        assert!(state.patch().note_changed);
    }

    #[tokio::test]
    async fn sensitive_delete_still_propagates_after_content_was_published() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("item", "sensitive-key"))
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        record_content_presence(
            &mut connection,
            "sensitive-key",
            true,
            &version(1, "device-a"),
        )
        .await
        .unwrap();
        mark_local_note(&mut connection, "sensitive-key", true, Some("private"))
            .await
            .unwrap();
        mark_local_deleted(&mut connection, "sensitive-key", true)
            .await
            .unwrap();
        drop(connection);

        let state = dirty_states(&pool, 10, false).await.unwrap().pop().unwrap();
        let patch = state.patch();
        assert_eq!(patch.is_present, Some(false));
        assert!(!patch.note_changed);
        mark_state_sent(&pool, &state, &version(2, "device-a"))
            .await
            .unwrap();

        assert!(dirty_states(&pool, 10, false).await.unwrap().is_empty());
        assert!(
            dirty_states(&pool, 10, true)
                .await
                .unwrap()
                .pop()
                .unwrap()
                .patch()
                .note_changed
        );
    }
}
