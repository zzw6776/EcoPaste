use anyhow::Context;
use blake3::Hasher;
use chrono::Utc;
use sqlx::{QueryBuilder, Sqlite, SqliteConnection, SqlitePool};

use crate::core::Result;
use crate::db::models::{
    ClipboardGroupFilter, ClipboardItem, ClipboardItemQuery, ClipboardItemSort, ClipboardKind,
};

const SELECT_ITEM: &str = "SELECT clipboard_items.id, clipboard_items.kind, clipboard_items.sub_kind, \
     clipboard_items.group_id, clipboard_items.source_app_id, clipboard_items.source_revision, clipboard_items.source_updated_at, \
     clipboard_items.source_device_id, clipboard_items.source_device_name, clipboard_items.content, \
     clipboard_items.content_hash, clipboard_items.sync_key, clipboard_items.search_text, clipboard_items.summary, \
     clipboard_items.file_types, clipboard_items.size, clipboard_items.width, clipboard_items.height, \
     clipboard_items.use_count, clipboard_items.is_favorite, clipboard_items.is_pinned, \
     clipboard_items.is_sensitive, clipboard_items.platform, clipboard_items.note, \
     clipboard_items.created_at, clipboard_items.updated_at, \
     clipboard_apps.name AS source_app_name, \
     clipboard_apps.icon_file AS source_app_icon_file, \
     clipboard_apps.accent_start AS source_app_accent_start, \
     clipboard_apps.accent_end AS source_app_accent_end, \
     sync_remote_file_items.state AS remote_file_state \
     FROM clipboard_items \
     LEFT JOIN clipboard_apps ON clipboard_apps.id = clipboard_items.source_app_id \
     LEFT JOIN sync_remote_file_items ON sync_remote_file_items.item_id = clipboard_items.id";
const HISTORY_CLEANUP_BATCH_SIZE: i64 = 256;
/// The same protection predicate is compiled into the age, count and capacity queries.
macro_rules! evictable_history {
    () => {
        "is_pinned = 0 AND is_favorite = 0 AND NOT EXISTS (
            SELECT 1 FROM sync_content_outbox manual
            WHERE manual.item_id = clipboard_items.id
              AND manual.manual_share = 1
              AND julianday(manual.updated_at) > julianday('now', '-24 hours')
        ) AND NOT EXISTS (
            SELECT 1 FROM sync_active_transfers active
            WHERE active.item_id = clipboard_items.id
        ) AND NOT EXISTS (
            SELECT 1
            FROM sync_item_events links
            JOIN sync_delivery_targets receiver
              ON receiver.event_id = links.event_id
            WHERE links.item_id = clipboard_items.id
              AND links.direction = 'local'
              AND receiver.target_id LIKE 'peer:%'
              AND receiver.confirmed_content = 'metadata'
              AND julianday(receiver.updated_at) > julianday('now', '-24 hours')
              AND NOT EXISTS (
                  SELECT 1 FROM sync_delivery_targets cloud
                  WHERE cloud.event_id = links.event_id
                    AND cloud.target_id = 'cloud'
                    AND cloud.confirmed_content = 'complete'
              )
        ) AND NOT EXISTS (
            SELECT 1 FROM sync_item_events links
            JOIN sync_delivery_targets delivery ON delivery.event_id = links.event_id
            WHERE links.item_id = clipboard_items.id
              AND delivery.status IN ('pending', 'awaiting_receipt')
              AND julianday(delivery.updated_at) > julianday('now', '-24 hours')
        ) AND NOT EXISTS (
            SELECT 1
            FROM sync_item_events links
            JOIN sync_events event ON event.event_id = links.event_id
            WHERE links.item_id = clipboard_items.id
              AND links.direction = 'local'
              AND event.delivery_route IS NOT NULL
              AND julianday(event.updated_at) > julianday('now', '-24 hours')
              AND NOT EXISTS (
                  SELECT 1 FROM sync_delivery_targets canceled
                  WHERE canceled.event_id = event.event_id
                    AND canceled.status IN ('blocked', 'canceled')
                    AND (
                        (event.delivery_route = 'cloud' AND canceled.target_id = 'cloud')
                        OR (event.delivery_route = 'lan' AND canceled.target_id LIKE 'peer:%')
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM sync_delivery_targets delivery
                  WHERE delivery.event_id = event.event_id
                    AND delivery.status = 'confirmed'
                    AND (
                        (
                            event.delivery_route = 'cloud'
                            AND delivery.target_id = 'cloud'
                            AND delivery.confirmed_content = 'complete'
                        )
                        OR (
                            event.delivery_route = 'lan'
                            AND delivery.target_id LIKE 'peer:%'
                        )
                    )
              )
        )"
    };
}

/// 列表/单条刷新场景的精简 SELECT：普通 text 由前端用 `summary` 渲染；URL 保留完整
/// `content`，避免长地址被摘要截断。HTML/RTF/长纯文本可能很大（用户复制整段文档），
/// 不应整段过 IPC + 进 DOM；image/files 的 content 是文件名 / 路径列表，同样保留。
/// 预览/写回走 [`find_item_by_id`] 拿完整 content。
///
/// LEFT JOIN `clipboard_apps` 顺带把来源应用名 / 图标文件名带回，前端直接渲染，
/// 不再额外发 list_clipboard_apps + get_clipboard_app_icon_path 请求。
const LIST_SELECT_ITEM: &str = "SELECT clipboard_items.id, clipboard_items.kind, \
     clipboard_items.sub_kind, clipboard_items.group_id, clipboard_items.source_app_id, clipboard_items.source_revision, clipboard_items.source_updated_at, \
     clipboard_items.source_device_id, clipboard_items.source_device_name, \
     CASE WHEN clipboard_items.sub_kind = 'url' THEN clipboard_items.content \
          WHEN clipboard_items.kind = 'text' THEN '' ELSE clipboard_items.content END AS content, \
     clipboard_items.content_hash, clipboard_items.sync_key, \
     CASE WHEN clipboard_items.kind = 'text' THEN NULL ELSE clipboard_items.search_text END AS search_text, \
     clipboard_items.summary, \
     CASE WHEN clipboard_items.kind = 'text' \
          THEN length(COALESCE(clipboard_items.search_text, clipboard_items.content)) \
          ELSE NULL END AS text_char_count, \
     clipboard_items.file_types, clipboard_items.size, \
     clipboard_items.width, clipboard_items.height, clipboard_items.use_count, \
     clipboard_items.is_favorite, clipboard_items.is_pinned, \
     clipboard_items.is_sensitive, \
     clipboard_items.platform, clipboard_items.note, \
     clipboard_items.created_at, clipboard_items.updated_at, \
     clipboard_apps.name AS source_app_name, \
     clipboard_apps.icon_file AS source_app_icon_file, \
     clipboard_apps.accent_start AS source_app_accent_start, \
     clipboard_apps.accent_end AS source_app_accent_end, \
     sync_remote_file_items.state AS remote_file_state \
     FROM clipboard_items \
     LEFT JOIN clipboard_apps ON clipboard_apps.id = clipboard_items.source_app_id \
     LEFT JOIN sync_remote_file_items ON sync_remote_file_items.item_id = clipboard_items.id";

fn kind_tag(kind: ClipboardKind) -> &'static str {
    match kind {
        ClipboardKind::Text => "text",
        ClipboardKind::Image => "image",
        ClipboardKind::Files => "files",
    }
}

/// 入库去重的结果：`id` 为生效行的主键（命中时是已有行，未命中时是新插入行），
/// `deduplicated` 表示是否命中了已有内容（命中则只 `use_count + 1` 未插入新行）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertResult {
    pub id: String,
    pub deduplicated: bool,
}

/// 计算去重指纹：`blake3("<kind>:<content>")`。
/// 加 `kind` 前缀，避免 text 与 files 恰好同串内容被误判为重复。
/// text 直接哈希内容串即可；image/files 的 `content` 是落盘引用/路径，
/// 调用方持有原始字节时可改为对原始内容字节哈希后写入 `content_hash`。
pub fn content_hash(kind: ClipboardKind, content: &str) -> String {
    let mut hasher = Hasher::new();
    hasher.update(kind_tag(kind).as_bytes());
    hasher.update(b":");
    hasher.update(content.as_bytes());
    hasher.finalize().to_hex().to_string()
}

/// 入库主入口：按 `item.content_hash` 去重。
/// 命中已有记录 → 复用 [`increment_item_use_count`] 累加并刷新 `updated_at`，不插入新行；
/// 未命中 → 调用 [`insert_item`] 插入。返回生效行 id 与是否去重。
#[cfg(test)]
pub async fn upsert_item(pool: &SqlitePool, item: &ClipboardItem) -> Result<UpsertResult> {
    upsert_item_with_sync_outbox(pool, item, None)
        .await
        .map(|(result, _)| result)
}

/// Persists one local capture and its synchronization intent in the same SQLite transaction.
/// Returns its history identity and whether sync was staged, including carried manual-share intent.
pub async fn upsert_item_with_sync_outbox(
    pool: &SqlitePool,
    item: &ClipboardItem,
    sync_options: Option<crate::db::sync_queue::ContentSyncOptions>,
) -> Result<(UpsertResult, bool)> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin local clipboard upsert")?;
    let existing_id: Option<String> = sqlx::query_scalar(
        "SELECT id FROM clipboard_items WHERE content_hash = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(&item.content_hash)
    .fetch_optional(&mut *transaction)
    .await
    .context("find local clipboard item by content hash")?;

    let (id, deduplicated) = if let Some(existing_id) = existing_id {
        merge_local_reuse_on(&mut transaction, &existing_id, item).await?;
        (existing_id, true)
    } else {
        insert_item_on(&mut transaction, item).await?;
        (item.id.clone(), false)
    };

    // Reusing identical content can change its source revision before a queued share is sent.
    // Carry explicit share intent forward even when automatic synchronization is disabled.
    let pending_manual_share: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sync_content_outbox WHERE item_id = ? AND manual_share = 1)",
    )
    .bind(&id)
    .fetch_one(&mut *transaction)
    .await
    .context("read pending manual share intent")?;
    let sync_options = if pending_manual_share {
        Some(crate::db::sync_queue::ContentSyncOptions {
            manual_share: true,
            ..sync_options.unwrap_or_default()
        })
    } else {
        sync_options
    };

    if let Some(options) = sync_options {
        let (source_revision, sync_key): (String, String) =
            sqlx::query_as("SELECT source_revision, sync_key FROM clipboard_items WHERE id = ?")
                .bind(&id)
                .fetch_one(&mut *transaction)
                .await
                .context("load staged clipboard item revision")?;
        crate::db::sync_queue::stage_content(
            &mut transaction,
            &id,
            &sync_key,
            &source_revision,
            options,
        )
        .await?;
    }

    transaction
        .commit()
        .await
        .context("commit local clipboard upsert")?;
    Ok((UpsertResult { id, deduplicated }, sync_options.is_some()))
}

/// Merges a synchronized record without turning network receipt into local reuse time.
#[cfg(test)]
pub async fn upsert_synced_item(pool: &SqlitePool, item: &ClipboardItem) -> Result<UpsertResult> {
    if let Some(existing) = find_item_by_sync_key(pool, &item.sync_key).await? {
        let mut connection = pool
            .acquire()
            .await
            .context("acquire synchronized clipboard connection")?;
        merge_synced_item_on(&mut connection, &existing.id, item).await?;
        return Ok(UpsertResult {
            id: existing.id,
            deduplicated: true,
        });
    }

    insert_item(pool, item).await?;
    Ok(UpsertResult {
        id: item.id.clone(),
        deduplicated: false,
    })
}

/// Atomically creates or refreshes a metadata-only remote file card and its backing event.
pub async fn upsert_remote_file_item(
    pool: &SqlitePool,
    item: &ClipboardItem,
    event_id: &str,
    origin_device_id: &str,
) -> Result<UpsertResult> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin remote file metadata upsert")?;
    let existing: Option<(String, bool)> = sqlx::query_as(
        r#"
        SELECT item.id,
               EXISTS(SELECT 1 FROM sync_remote_file_items remote WHERE remote.item_id = item.id)
        FROM clipboard_items item
        WHERE item.sync_key = ?
        ORDER BY item.created_at DESC
        LIMIT 1
        "#,
    )
    .bind(&item.sync_key)
    .fetch_optional(&mut *transaction)
    .await
    .context("find remote file metadata item")?;

    let (id, deduplicated) = match existing {
        Some((id, true)) => {
            replace_remote_content_on(&mut transaction, &id, item).await?;
            (id, true)
        }
        Some((id, false)) => {
            replace_remote_content_on(&mut transaction, &id, item).await?;
            (id, true)
        }
        None => {
            insert_item_on(&mut transaction, item).await?;
            (item.id.clone(), false)
        }
    };
    let now = Utc::now();
    sqlx::query(
        r#"
            INSERT INTO sync_remote_file_items (
                item_id, event_id, origin_device_id, state, created_at, updated_at
            ) VALUES (?, ?, ?, 'available', ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                event_id = excluded.event_id,
                origin_device_id = excluded.origin_device_id,
                state = 'available',
                updated_at = excluded.updated_at
            "#,
    )
    .bind(&id)
    .bind(event_id)
    .bind(origin_device_id)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("register remote file metadata")?;
    crate::db::sync_item_state::materialize_state_on(&mut transaction, &item.sync_key).await?;
    transaction
        .commit()
        .await
        .context("commit remote file metadata upsert")?;
    Ok(UpsertResult { id, deduplicated })
}

/// Promotes a metadata-only card to materialized local content without leaving a remote marker.
pub async fn upsert_materialized_synced_item(
    pool: &SqlitePool,
    item: &ClipboardItem,
) -> Result<UpsertResult> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin materialized synchronized item upsert")?;
    let existing: Option<(String, bool)> = sqlx::query_as(
        r#"
        SELECT item.id,
               EXISTS(SELECT 1 FROM sync_remote_file_items remote WHERE remote.item_id = item.id)
        FROM clipboard_items item
        WHERE item.sync_key = ?
        ORDER BY item.created_at DESC
        LIMIT 1
        "#,
    )
    .bind(&item.sync_key)
    .fetch_optional(&mut *transaction)
    .await
    .context("find materialized synchronized item")?;

    let (id, deduplicated) = match existing {
        Some((id, true)) => {
            replace_remote_content_on(&mut transaction, &id, item).await?;
            sqlx::query("DELETE FROM sync_remote_file_items WHERE item_id = ?")
                .bind(&id)
                .execute(&mut *transaction)
                .await
                .context("clear remote file metadata after materialization")?;
            (id, true)
        }
        Some((id, false)) => {
            if item.kind == ClipboardKind::Files {
                replace_materialized_file_paths_on(&mut transaction, &id, item).await?;
            }
            merge_synced_item_on(&mut transaction, &id, item).await?;
            (id, true)
        }
        None => {
            insert_item_on(&mut transaction, item).await?;
            (item.id.clone(), false)
        }
    };
    crate::db::sync_item_state::materialize_state_on(&mut transaction, &item.sync_key).await?;
    transaction
        .commit()
        .await
        .context("commit materialized synchronized item upsert")?;
    Ok(UpsertResult { id, deduplicated })
}

async fn merge_synced_item_on(
    connection: &mut SqliteConnection,
    id: &str,
    item: &ClipboardItem,
) -> Result<()> {
    let source_updated_at = item.source_updated_at.unwrap_or(item.updated_at);
    sqlx::query(
        r#"
        UPDATE clipboard_items
        SET created_at = CASE WHEN created_at > ? THEN ? ELSE created_at END,
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END,
            source_app_id = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_app_id END,
            source_device_id = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_device_id END,
            source_device_name = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_device_name END,
            source_revision = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_revision END,
            source_updated_at = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_updated_at END
        WHERE id = ?
        "#,
    )
    .bind(item.created_at)
    .bind(item.created_at)
    .bind(item.updated_at)
    .bind(item.updated_at)
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(item.source_app_id.as_deref())
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(item.source_device_id.as_deref())
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(item.source_device_name.as_deref())
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(&item.source_revision)
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(source_updated_at)
    .bind(id)
    .execute(connection)
    .await
    .context("merge synchronized clipboard timestamps")?;
    Ok(())
}

async fn replace_remote_content_on(
    connection: &mut SqliteConnection,
    id: &str,
    item: &ClipboardItem,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE clipboard_items
        SET kind = ?, sub_kind = ?, content = ?, content_hash = ?, search_text = ?,
            summary = ?, file_types = ?, size = ?, width = ?, height = ?,
            is_sensitive = ?, platform = ?
        WHERE id = ?
        "#,
    )
    .bind(item.kind)
    .bind(item.sub_kind)
    .bind(&item.content)
    .bind(&item.content_hash)
    .bind(item.search_text.as_deref())
    .bind(item.summary.as_deref())
    .bind(item.file_types.as_deref())
    .bind(item.size)
    .bind(item.width)
    .bind(item.height)
    .bind(item.is_sensitive)
    .bind(item.platform)
    .bind(id)
    .execute(&mut *connection)
    .await
    .context("promote remote metadata to local content")?;
    merge_synced_item_on(connection, id, item).await?;
    Ok(())
}

/// Repairs only the physical paths of an existing local file record while preserving its metadata.
async fn replace_materialized_file_paths_on(
    connection: &mut SqliteConnection,
    id: &str,
    item: &ClipboardItem,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE clipboard_items
        SET content = ?, content_hash = ?, search_text = ?, summary = ?,
            file_types = ?, size = ?, width = ?, height = ?, is_sensitive = ?
        WHERE id = ?
        "#,
    )
    .bind(&item.content)
    .bind(&item.content_hash)
    .bind(item.search_text.as_deref())
    .bind(item.summary.as_deref())
    .bind(item.file_types.as_deref())
    .bind(item.size)
    .bind(item.width)
    .bind(item.height)
    .bind(item.is_sensitive)
    .bind(id)
    .execute(connection)
    .await
    .context("replace synchronized file paths")?;
    Ok(())
}

/// Replaces timestamps with values reconciled from all versioned sync events for an item.
pub async fn set_synced_item_timestamps(
    pool: &SqlitePool,
    item_id: &str,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
) -> Result<()> {
    sqlx::query("UPDATE clipboard_items SET created_at = ?, updated_at = ? WHERE id = ?")
        .bind(created_at)
        .bind(updated_at)
        .bind(item_id)
        .execute(pool)
        .await
        .context("reconcile synchronized clipboard timestamps")?;
    Ok(())
}

/// Finds the newest local row associated with a stable cross-device item key.
pub async fn find_item_by_sync_key(
    pool: &SqlitePool,
    sync_key: &str,
) -> Result<Option<ClipboardItem>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_ITEM);
    qb.push(" WHERE clipboard_items.sync_key = ")
        .push_bind(sync_key.to_owned())
        .push(" ORDER BY clipboard_items.created_at DESC LIMIT 1");

    Ok(qb
        .build_query_as::<ClipboardItem>()
        .fetch_optional(pool)
        .await
        .context("failed to find clipboard item by sync key")?)
}

/// 插入一条剪贴板记录（不做去重；本地捕获去重请走 [`upsert_item_with_sync_outbox`]）。
#[cfg(test)]
pub async fn insert_item(pool: &SqlitePool, item: &ClipboardItem) -> Result<()> {
    let mut connection = pool
        .acquire()
        .await
        .context("acquire clipboard item connection")?;
    insert_item_on(&mut connection, item).await
}

/// 合并一次本机重新使用，并用捕获时刻与稳定 revision 决定最新来源应用。
async fn merge_local_reuse_on(
    connection: &mut SqliteConnection,
    id: &str,
    item: &ClipboardItem,
) -> Result<()> {
    let now = Utc::now();
    let source_updated_at = item.source_updated_at.unwrap_or(item.updated_at);
    sqlx::query(
        r#"
        UPDATE clipboard_items
        SET use_count = use_count + 1,
            updated_at = ?,
            size = ?,
            source_app_id = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_app_id END,
            source_device_id = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_device_id END,
            source_device_name = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_device_name END,
            source_revision = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_revision END,
            source_updated_at = CASE
                WHEN source_updated_at < ? OR (source_updated_at = ? AND source_revision < ?)
                THEN ? ELSE source_updated_at END
        WHERE id = ?
        "#,
    )
    .bind(now)
    .bind(item.size)
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(item.source_app_id.as_deref())
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(item.source_device_id.as_deref())
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(item.source_device_name.as_deref())
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(&item.source_revision)
    .bind(source_updated_at)
    .bind(source_updated_at)
    .bind(&item.source_revision)
    .bind(source_updated_at)
    .bind(id)
    .execute(connection)
    .await
    .context("merge local clipboard reuse")?;
    Ok(())
}

async fn insert_item_on(connection: &mut SqliteConnection, item: &ClipboardItem) -> Result<()> {
    sqlx::query(
        "INSERT INTO clipboard_items \
         (id, kind, sub_kind, group_id, source_app_id, source_device_id, source_device_name, source_revision, source_updated_at, content, content_hash, sync_key, search_text, \
          summary, file_types, size, width, height, use_count, is_favorite, is_pinned, is_sensitive, platform, note, \
          created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(item.id.as_str())
    .bind(item.kind)
    .bind(item.sub_kind)
    .bind(item.group_id.as_deref())
    .bind(item.source_app_id.as_deref())
    .bind(item.source_device_id.as_deref())
    .bind(item.source_device_name.as_deref())
    .bind(&item.source_revision)
    .bind(item.source_updated_at.unwrap_or(item.updated_at))
    .bind(item.content.as_str())
    .bind(item.content_hash.as_str())
    .bind(item.sync_key.as_str())
    .bind(item.search_text.as_deref())
    .bind(item.summary.as_deref())
    .bind(item.file_types.as_deref())
    .bind(item.size)
    .bind(item.width)
    .bind(item.height)
    .bind(item.use_count)
    .bind(item.is_favorite)
    .bind(item.is_pinned)
    .bind(item.is_sensitive)
    .bind(item.platform)
    .bind(item.note.as_deref())
    .bind(item.created_at)
    .bind(item.updated_at)
    .execute(connection)
    .await
    .context("failed to insert clipboard item")?;
    Ok(())
}

/// 后台目录统计只填充仍为空且 revision 未变化的大小，避免覆盖更新后的同路径记录。
pub async fn fill_file_item_size(
    pool: &SqlitePool,
    item_id: &str,
    source_revision: &str,
    size: i64,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE clipboard_items SET size = ? \
         WHERE id = ? AND kind = ? AND source_revision = ? AND size IS NULL",
    )
    .bind(size)
    .bind(item_id)
    .bind(ClipboardKind::Files)
    .bind(source_revision)
    .execute(pool)
    .await
    .context("fill clipboard file size")?;
    Ok(result.rows_affected() > 0)
}

/// 文件真正打包时用复核后的总大小更新记录，但不改变记录排序时间。
pub async fn update_file_item_size(
    pool: &SqlitePool,
    item_id: &str,
    source_revision: &str,
    size: i64,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE clipboard_items SET size = ? \
         WHERE id = ? AND kind = ? AND source_revision = ?",
    )
    .bind(size)
    .bind(item_id)
    .bind(ClipboardKind::Files)
    .bind(source_revision)
    .execute(pool)
    .await
    .context("update clipboard file size")?;
    Ok(result.rows_affected() > 0)
}

/// 仅返回项的轻量查询：生产路径走 [`query_items_page`]（顺带返回 total），
/// 本函数留给单元测试做断言。
#[cfg(test)]
pub async fn query_items(pool: &SqlitePool, q: &ClipboardItemQuery) -> Result<Vec<ClipboardItem>> {
    fetch_items(pool, q, KeywordFilter::from_keyword(q.keyword.as_deref())).await
}

/// 列表 + 总数一次返回，供命令层组装 [`ClipboardItemPage`]：
/// 一次 IPC 拿到「本页项 / 当前过滤下的总数 / 是否还有下一页」。
/// `keyword` 按字符长度分流：≥3 走 FTS5（trigram 分词），1–2 走 `LIKE '%kw%'`
/// （兜底短词；trigram 索引最短 3 字符，对 1–2 字符词永远 0 命中）。
pub async fn query_items_page(
    pool: &SqlitePool,
    q: &ClipboardItemQuery,
) -> Result<(Vec<ClipboardItem>, i64)> {
    let keyword = KeywordFilter::from_keyword(q.keyword.as_deref());
    let items = fetch_items(pool, q, keyword.clone()).await?;
    let total = fetch_items_count(pool, q, keyword).await?;
    Ok((items, total))
}

/// 返回指定记录在当前筛选与排序下的零基索引；记录不属于该视图时返回 `None`。
pub async fn find_item_index(
    pool: &SqlitePool,
    id: &str,
    q: &ClipboardItemQuery,
) -> Result<Option<i64>> {
    let keyword = KeywordFilter::from_keyword(q.keyword.as_deref());
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT item_index FROM (SELECT clipboard_items.id, ROW_NUMBER() OVER (ORDER BY ",
    );
    push_sort_clause(&mut qb, q.sort);
    qb.push(") - 1 AS item_index FROM clipboard_items WHERE 1 = 1");
    push_filter_clauses(&mut qb, q, &keyword);
    qb.push(") ranked WHERE id = ").push_bind(id.to_owned());

    let row = qb
        .build_query_as::<(i64,)>()
        .fetch_optional(pool)
        .await
        .context("find clipboard item index")?;
    Ok(row.map(|(index,)| index))
}

/// 按 `id` 查找单条记录，不存在时返回 `None`。
pub async fn find_item_by_id(pool: &SqlitePool, id: &str) -> Result<Option<ClipboardItem>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_ITEM);
    qb.push(" WHERE clipboard_items.id = ")
        .push_bind(id.to_owned());

    let item = qb
        .build_query_as::<ClipboardItem>()
        .fetch_optional(pool)
        .await
        .context("failed to find clipboard item by id")?;
    Ok(item)
}

/// Reads only the ids of the most recent local-origin records for one-time sync backfill.
/// Remote-only records are already owned by another endpoint and must not be re-originated here.
/// Callers load the full records one at a time so large histories do not coexist in memory.
pub async fn recent_item_ids_for_sync(pool: &SqlitePool, limit: u16) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut qb: QueryBuilder<Sqlite> =
        QueryBuilder::new("SELECT clipboard_items.id FROM clipboard_items");
    qb.push(
        r#"
        WHERE NOT EXISTS (
            SELECT 1 FROM sync_item_events remote
            WHERE remote.item_id = clipboard_items.id AND remote.direction = 'remote'
        ) OR EXISTS (
            SELECT 1 FROM sync_item_events local
            WHERE local.item_id = clipboard_items.id AND local.direction = 'local'
        )
        ORDER BY clipboard_items.updated_at DESC, clipboard_items.created_at DESC LIMIT
        "#,
    )
    .push_bind(i64::from(limit));

    Ok(qb
        .build_query_scalar::<String>()
        .fetch_all(pool)
        .await
        .context("failed to query recent clipboard item ids for sync")?)
}

/// 返回尚未记录总大小的文件卡片，供升级后的后台回填使用。
pub async fn file_items_missing_size(pool: &SqlitePool) -> Result<Vec<ClipboardItem>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_ITEM);
    qb.push(" WHERE clipboard_items.kind = ")
        .push_bind(ClipboardKind::Files)
        .push(" AND clipboard_items.size IS NULL ORDER BY clipboard_items.updated_at DESC");

    Ok(qb
        .build_query_as::<ClipboardItem>()
        .fetch_all(pool)
        .await
        .context("query clipboard file items missing size")?)
}

/// 按 `id` 查找单条记录的「列表视图」副本——与 [`fetch_items`] 走同款 [`LIST_SELECT_ITEM`] 裁剪：
/// URL 保留完整 `content`，其它 text 的 `content` 与全部 text 的 `search_text` 置空，
/// 由前端用 `summary` 渲染。
/// 供前端响应 `clipboard://updated` 事件时按 id 拉取使用，避免事件驱动刷新整页 refetch
/// 时回传整段 HTML/RTF。需要完整 `content` 的写回 / 预览路径请走 [`find_item_by_id`]。
pub async fn find_item_for_list_by_id(
    pool: &SqlitePool,
    id: &str,
) -> Result<Option<ClipboardItem>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(LIST_SELECT_ITEM);
    qb.push(" WHERE clipboard_items.id = ")
        .push_bind(id.to_owned());

    let item = qb
        .build_query_as::<ClipboardItem>()
        .fetch_optional(pool)
        .await
        .context("failed to find clipboard item (list view) by id")?;
    Ok(item)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToggleFavoriteOutcome {
    pub is_favorite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TogglePinnedOutcome {
    pub is_pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteItemOutcome {
    pub image_file_name: Option<String>,
}

/// 翻转 `is_favorite`（收藏 / 取消收藏），原子记录待同步状态并返回新值。
pub async fn toggle_item_favorite(pool: &SqlitePool, id: &str) -> Result<ToggleFavoriteOutcome> {
    let mut transaction = pool.begin().await.context("begin favorite update")?;
    let row: (bool, String, bool) = sqlx::query_as(
        "UPDATE clipboard_items SET is_favorite = NOT is_favorite WHERE id = ? RETURNING is_favorite, sync_key, is_sensitive",
    )
    .bind(id)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to toggle clipboard item favorite")?;
    crate::db::sync_item_state::mark_local_favorite(&mut transaction, &row.1, row.2, row.0).await?;
    transaction
        .commit()
        .await
        .context("commit favorite update")?;
    Ok(ToggleFavoriteOutcome { is_favorite: row.0 })
}

/// 幂等地将 `is_favorite` 置为 true（已收藏的无变化）。auto-favorite 场景用。
pub async fn mark_item_favorite(
    pool: &SqlitePool,
    id: &str,
) -> Result<Option<ToggleFavoriteOutcome>> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin automatic favorite update")?;
    let row: Option<(bool, String, bool)> = sqlx::query_as(
        "UPDATE clipboard_items SET is_favorite = 1 WHERE id = ? AND is_favorite = 0 RETURNING is_favorite, sync_key, is_sensitive",
    )
    .bind(id)
    .fetch_optional(&mut *transaction)
    .await
    .context("failed to mark clipboard item favorite")?;
    if let Some((is_favorite, sync_key, is_sensitive)) = row.as_ref() {
        crate::db::sync_item_state::mark_local_favorite(
            &mut transaction,
            sync_key,
            *is_sensitive,
            *is_favorite,
        )
        .await?;
    }
    transaction
        .commit()
        .await
        .context("commit automatic favorite update")?;
    Ok(row.map(|(is_favorite, _, _)| ToggleFavoriteOutcome { is_favorite }))
}

/// 翻转 `is_pinned`（置顶 / 取消置顶），原子记录待同步状态并返回新值。
pub async fn toggle_item_pinned(pool: &SqlitePool, id: &str) -> Result<TogglePinnedOutcome> {
    let mut transaction = pool.begin().await.context("begin pinned update")?;
    let row: (bool, String, bool) = sqlx::query_as(
        "UPDATE clipboard_items SET is_pinned = NOT is_pinned WHERE id = ? RETURNING is_pinned, sync_key, is_sensitive",
    )
    .bind(id)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to toggle clipboard item pinned")?;
    crate::db::sync_item_state::mark_local_pinned(&mut transaction, &row.1, row.2, row.0).await?;
    transaction.commit().await.context("commit pinned update")?;
    Ok(TogglePinnedOutcome { is_pinned: row.0 })
}

/// 更新备注，传 `None` 清空备注，并在同一事务中记录待同步状态。
pub async fn update_item_note(pool: &SqlitePool, id: &str, note: Option<&str>) -> Result<()> {
    let mut transaction = pool.begin().await.context("begin note update")?;
    let row: (String, bool) = sqlx::query_as(
        "UPDATE clipboard_items SET note = ? WHERE id = ? RETURNING sync_key, is_sensitive",
    )
    .bind(note)
    .bind(id)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to update clipboard item note")?;
    crate::db::sync_item_state::mark_local_note(&mut transaction, &row.0, row.1, note).await?;
    transaction.commit().await.context("commit note update")?;
    Ok(())
}

/// 更新条目所属分组；不刷新 `updated_at`，避免污染最近使用排序。
pub async fn update_item_group(pool: &SqlitePool, id: &str, group_id: Option<&str>) -> Result<()> {
    sqlx::query("UPDATE clipboard_items SET group_id = ? WHERE id = ?")
        .bind(group_id)
        .bind(id)
        .execute(pool)
        .await
        .context("failed to update clipboard item group")?;
    Ok(())
}

/// `use_count + 1` 并刷新 `updated_at`（命中去重时复用）。
pub async fn increment_item_use_count(pool: &SqlitePool, id: &str) -> Result<()> {
    sqlx::query(
        "UPDATE clipboard_items SET use_count = use_count + 1, updated_at = ? WHERE id = ?",
    )
    .bind(Utc::now())
    .bind(id)
    .execute(pool)
    .await
    .context("failed to increment clipboard item use_count")?;
    Ok(())
}

/// 删除单条记录并记录墓碑。
pub async fn delete_item(pool: &SqlitePool, id: &str) -> Result<Option<DeleteItemOutcome>> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin clipboard item deletion")?;
    let row = sqlx::query_as::<_, (ClipboardKind, String, String, bool)>(
        "DELETE FROM clipboard_items WHERE id = ? RETURNING kind, content, sync_key, is_sensitive",
    )
    .bind(id)
    .fetch_optional(&mut *transaction)
    .await
    .context("failed to delete clipboard item")?;

    let Some((kind, content, sync_key, is_sensitive)) = row else {
        transaction
            .commit()
            .await
            .context("commit empty clipboard item deletion")?;
        return Ok(None);
    };

    crate::db::sync_item_state::mark_local_deleted(&mut transaction, &sync_key, is_sensitive)
        .await?;
    transaction
        .commit()
        .await
        .context("commit clipboard item deletion")?;
    let image_file_name = image_file_name(kind, content);
    Ok(Some(DeleteItemOutcome { image_file_name }))
}

/// 取被删行里需要连带删除的图片文件名：kind 为 image 时 `content` 即文件名，否则 `None`。
fn image_file_name(kind: ClipboardKind, content: String) -> Option<String> {
    (kind == ClipboardKind::Image).then_some(content)
}

/// 批量删除，返回实际删除行数；`ids` 为空时不发查询。
#[allow(dead_code)]
pub async fn delete_items(pool: &SqlitePool, ids: &[String]) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }

    let mut qb: QueryBuilder<Sqlite> =
        QueryBuilder::new("DELETE FROM clipboard_items WHERE id IN (");
    let mut separated = qb.separated(", ");
    for id in ids {
        separated.push_bind(id);
    }
    qb.push(")");

    let result = qb
        .build()
        .execute(pool)
        .await
        .context("failed to delete clipboard items")?;
    Ok(result.rows_affected())
}

/// 历史清理的结果：删除行数 + 其中图片记录的落盘文件名（供调用方删图）。
#[derive(Debug, Default)]
pub struct CleanupOutcome {
    pub removed: u64,
    pub image_files: Vec<String>,
}

/// 历史清理：按时间和条数裁剪，保留置顶、收藏与保护期内的手动分享。
/// 返回删除行数与被删图片的文件名。`older_than = None` 跳过时长清理；`max_count = None` 或 `Some(0)` 跳过条数清理。
pub async fn cleanup_history(
    pool: &SqlitePool,
    older_than: Option<chrono::DateTime<chrono::Utc>>,
    max_count: Option<u32>,
) -> Result<CleanupOutcome> {
    let max_count = max_count.filter(|count| *count > 0);
    if older_than.is_none() && max_count.is_none() {
        return Ok(CleanupOutcome::default());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin clipboard history cleanup")?;
    let mut outcome = CleanupOutcome::default();

    if let Some(cutoff) = older_than {
        loop {
            let rows = sqlx::query_scalar::<_, Option<String>>(concat!(
                "DELETE FROM clipboard_items WHERE id IN ( \
                     SELECT id FROM clipboard_items \
                     WHERE ",
                evictable_history!(),
                " AND created_at < ? \
                     LIMIT ? \
                 ) \
                 RETURNING CASE WHEN kind = ? THEN content ELSE NULL END"
            ))
            .bind(cutoff)
            .bind(HISTORY_CLEANUP_BATCH_SIZE)
            .bind(ClipboardKind::Image)
            .fetch_all(&mut *transaction)
            .await
            .context("failed to cleanup clipboard items by retention")?;
            if absorb_cleanup_rows(&mut outcome, rows) < HISTORY_CLEANUP_BATCH_SIZE as usize {
                break;
            }
        }
    }

    if let Some(max) = max_count {
        // 仅在非置顶 / 非收藏集合内按 created_at DESC 保留前 max 条，多余的删除。
        loop {
            let rows = sqlx::query_scalar::<_, Option<String>>(concat!(
                "DELETE FROM clipboard_items WHERE id IN ( \
                     SELECT id FROM clipboard_items \
                     WHERE ",
                evictable_history!(),
                " \
                     ORDER BY created_at DESC \
                     LIMIT ? OFFSET ? \
                 ) \
                 RETURNING CASE WHEN kind = ? THEN content ELSE NULL END"
            ))
            .bind(HISTORY_CLEANUP_BATCH_SIZE)
            .bind(max as i64)
            .bind(ClipboardKind::Image)
            .fetch_all(&mut *transaction)
            .await
            .context("failed to cleanup clipboard items by max_count")?;
            if absorb_cleanup_rows(&mut outcome, rows) < HISTORY_CLEANUP_BATCH_SIZE as usize {
                break;
            }
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit clipboard history cleanup")?;
    Ok(outcome)
}

/// Deletes an oldest-first prefix atomically. Managed resources and images with unknown sizes form
/// a remeasurement boundary; external file cards use their database footprint and stay batched.
pub(crate) async fn evict_oldest_history(
    pool: &SqlitePool,
    excess_bytes: u64,
    managed_reference_prefix: &str,
) -> Result<CleanupOutcome> {
    let rows = sqlx::query_scalar::<_, Option<String>>(concat!(
        "WITH oldest AS MATERIALIZED (
            SELECT id, updated_at, created_at,
                CASE WHEN kind = 'text' THEN MAX(1, length(CAST(content AS BLOB))
                    + COALESCE(length(CAST(search_text AS BLOB)), 0)
                    + COALESCE(length(CAST(note AS BLOB)), 0))
                    WHEN kind = 'image' AND size > 0 THEN MIN(size, ?1)
                    WHEN kind = 'files' AND instr(content, ?2) = 0 THEN
                        MAX(1, length(CAST(content AS BLOB))
                            + COALESCE(length(CAST(search_text AS BLOB)), 0)
                            + COALESCE(length(CAST(note AS BLOB)), 0))
                    ELSE ?1 END AS bytes
            FROM clipboard_items WHERE ", evictable_history!(), "
            ORDER BY updated_at, created_at, id LIMIT ?3
        ), ranked AS (
            SELECT id, SUM(bytes) OVER (ORDER BY updated_at, created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_bytes
            FROM oldest
        )
        DELETE FROM clipboard_items WHERE id IN (
            SELECT id FROM ranked WHERE COALESCE(previous_bytes, 0) < ?1
        ) RETURNING CASE WHEN kind = 'image' THEN content ELSE NULL END"
    ))
    .bind(i64::try_from(excess_bytes).unwrap_or(i64::MAX))
    .bind(managed_reference_prefix)
    .bind(HISTORY_CLEANUP_BATCH_SIZE)
    .fetch_all(pool).await.context("evict oldest local clipboard records")?;
    let mut outcome = CleanupOutcome::default();
    absorb_cleanup_rows(&mut outcome, rows);
    Ok(outcome)
}

/// 只保留批量删除返回的图片文件名；文本行仅以 `NULL` 计数，不把正文载入内存。
fn absorb_cleanup_rows(outcome: &mut CleanupOutcome, rows: Vec<Option<String>>) -> usize {
    let removed = rows.len();
    outcome.removed += removed as u64;
    outcome.image_files.extend(rows.into_iter().flatten());
    removed
}

/// 清空记录，返回删除行数与被删图片文件名；未显式删除的收藏 / 置顶项会保留。
pub async fn clear_items(
    pool: &SqlitePool,
    delete_favorites: bool,
    delete_pinned: bool,
) -> Result<CleanupOutcome> {
    let mut transaction = pool.begin().await.context("begin clipboard item clear")?;
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("DELETE FROM clipboard_items");

    let mut has_condition = false;
    if !delete_favorites {
        qb.push(" WHERE is_favorite = 0");
        has_condition = true;
    }
    if !delete_pinned {
        qb.push(if has_condition { " AND " } else { " WHERE " });
        qb.push("is_pinned = 0");
    }
    qb.push(" RETURNING kind, content, sync_key, is_sensitive");

    let rows = qb
        .build_query_as::<(ClipboardKind, String, String, bool)>()
        .fetch_all(&mut *transaction)
        .await
        .context("failed to clear clipboard items")?;

    let mut outcome = CleanupOutcome::default();
    for (kind, content, sync_key, is_sensitive) in rows {
        outcome.removed += 1;
        if let Some(file_name) = image_file_name(kind, content) {
            outcome.image_files.push(file_name);
        }
        crate::db::sync_item_state::mark_local_deleted(&mut transaction, &sync_key, is_sensitive)
            .await?;
    }
    transaction
        .commit()
        .await
        .context("commit clipboard item clear")?;
    Ok(outcome)
}

/// 关键词过滤分流：≥3 字符走 FTS5（trigram），1–2 字符走 LIKE 兜底，空 / 仅空白不过滤。
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeywordFilter {
    None,
    /// `clipboard_items_fts MATCH ?` 的表达式，已含前缀通配。
    Fts(String),
    /// 已转义 `% _ \` 的关键词；下游统一拼成 `%<kw>%` 多列模糊匹配。
    Like(String),
}

impl KeywordFilter {
    /// 按字符数（非字节）判定走 FTS 还是 LIKE。CJK 一个字符也算 1，
    /// 与 trigram 的 3 字符门槛保持一致。
    ///
    /// 注意：门槛按**每个空白分词**判定，而非整串字符数。trigram 索引对 <3 字符的
    /// token 永远 0 命中，而 FTS5 表达式里多 token 之间默认是 AND —— 只要有一个短词，
    /// 整条表达式就被拖成 0 结果（例如 `a b` 会生成 `"a"* "b"*`，二者皆 <3 字符）。
    /// 因此仅当所有分词均 ≥3 字符时才走 FTS，否则降级到 LIKE 兜底。
    fn from_keyword(keyword: Option<&str>) -> Self {
        let Some(trimmed) = keyword.map(str::trim).filter(|s| !s.is_empty()) else {
            return Self::None;
        };

        let fts_viable = trimmed
            .split_whitespace()
            .all(|token| token.chars().count() >= 3);

        if fts_viable {
            if let Some(expr) = build_fts_expr(trimmed) {
                return Self::Fts(expr);
            }
        }

        Self::Like(escape_like(trimmed))
    }
}

/// 把用户关键词拆成 FTS5 前缀匹配表达式（如 `foo bar` -> `"foo"* "bar"*`）。
/// 双引号包裹 + 转义，避免关键词中的 FTS5 语法字符被当作运算符。空白关键词返回 `None`。
fn build_fts_expr(keyword: &str) -> Option<String> {
    let expr = keyword
        .split_whitespace()
        .map(|token| format!("\"{}\"*", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" ");

    (!expr.is_empty()).then_some(expr)
}

/// 转义 LIKE 的特殊字符（`\ % _`），配合 SQL 端 `ESCAPE '\\'`。
fn escape_like(keyword: &str) -> String {
    let mut out = String::with_capacity(keyword.len());
    for ch in keyword.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// 拼装查询：过滤（含可选关键词匹配） + 排序（置顶恒前置） + 分页。
/// 所有 bind 均传入拥有所有权/Copy 的值，避免 `QueryBuilder` 借用 `q` 引发的生命周期问题。
async fn fetch_items(
    pool: &SqlitePool,
    q: &ClipboardItemQuery,
    keyword: KeywordFilter,
) -> Result<Vec<ClipboardItem>> {
    let build_query = |select_clause: &str| {
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(select_clause);
        qb.push(" WHERE 1 = 1");
        push_filter_clauses(&mut qb, q, &keyword);

        qb.push(" ORDER BY ");
        push_sort_clause(&mut qb, q.sort);

        qb.push(" LIMIT ").push_bind(q.limit);
        qb.push(" OFFSET ").push_bind(q.offset);
        qb
    };

    let items = build_query(LIST_SELECT_ITEM)
        .build_query_as::<ClipboardItem>()
        .fetch_all(pool)
        .await
        .context("failed to query clipboard items")?;
    Ok(items)
}

/// 统计满足同样过滤条件的总条数（不参与排序 / 分页），与 [`fetch_items`] 共用 [`push_filter_clauses`]。
async fn fetch_items_count(
    pool: &SqlitePool,
    q: &ClipboardItemQuery,
    keyword: KeywordFilter,
) -> Result<i64> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("SELECT COUNT(*) FROM clipboard_items");
    qb.push(" WHERE 1 = 1");
    push_filter_clauses(&mut qb, q, &keyword);

    let row: (i64,) = qb
        .build_query_as()
        .fetch_one(pool)
        .await
        .context("failed to count filtered clipboard items")?;
    Ok(row.0)
}

/// 追加与列表查询完全一致的排序，供分页查询和记录定位共同使用。
fn push_sort_clause(qb: &mut QueryBuilder<Sqlite>, sort: ClipboardItemSort) {
    qb.push("clipboard_items.is_pinned DESC, ");
    match sort {
        ClipboardItemSort::CreatedAt => {
            qb.push("clipboard_items.created_at DESC");
        }
        ClipboardItemSort::UpdatedAt => {
            qb.push("clipboard_items.updated_at DESC, clipboard_items.created_at DESC");
        }
        ClipboardItemSort::UseCount => {
            qb.push("clipboard_items.use_count DESC, clipboard_items.created_at DESC");
        }
    }
    qb.push(", clipboard_items.id ASC");
}

/// 把当前查询的过滤条件追加到 `qb`（不含 ORDER BY / LIMIT / OFFSET），供列表查询与计数共用。
fn push_filter_clauses(
    qb: &mut QueryBuilder<Sqlite>,
    q: &ClipboardItemQuery,
    keyword: &KeywordFilter,
) {
    match keyword {
        KeywordFilter::None => {}
        KeywordFilter::Fts(expr) => {
            qb.push(
                " AND clipboard_items.rowid IN (SELECT rowid FROM clipboard_items_fts WHERE clipboard_items_fts MATCH ",
            )
            .push_bind(expr.clone())
            .push(")");
        }
        KeywordFilter::Like(kw) => {
            // FTS 索引覆盖 search_text / note 两列，LIKE 兜底也跟齐，
            // 让 1–2 字符短词的命中范围与长词一致。
            let pattern = format!("%{kw}%");
            qb.push(" AND (clipboard_items.search_text LIKE ")
                .push_bind(pattern.clone())
                .push(" ESCAPE '\\' OR clipboard_items.note LIKE ")
                .push_bind(pattern)
                .push(" ESCAPE '\\')");
        }
    }
    // group（UI Tab）覆盖显式 kind / favorite；为 None 时回退到显式字段（单测使用）。
    let (effective_kind, effective_favorite) = match q.group {
        Some(ClipboardGroupFilter::All) => (None, None),
        Some(ClipboardGroupFilter::Text) => (Some(ClipboardKind::Text), None),
        Some(ClipboardGroupFilter::Image) => (Some(ClipboardKind::Image), None),
        Some(ClipboardGroupFilter::Files) => (Some(ClipboardKind::Files), None),
        Some(ClipboardGroupFilter::Favorite) => (None, Some(true)),
        None => (q.kind, q.favorite),
    };
    if let Some(kind) = effective_kind {
        qb.push(" AND clipboard_items.kind = ").push_bind(kind);
    }
    if let Some(group_id) = &q.group_id {
        qb.push(" AND clipboard_items.group_id = ")
            .push_bind(group_id.clone());
    }
    if let Some(favorite) = effective_favorite {
        qb.push(" AND clipboard_items.is_favorite = ")
            .push_bind(favorite);
    }
    if let Some(pinned) = q.pinned {
        qb.push(" AND clipboard_items.is_pinned = ")
            .push_bind(pinned);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::apps::upsert_app;
    use crate::db::groups::insert_group;
    use crate::db::models::{
        ClipboardApp, ClipboardGroup, ClipboardKind, ClipboardSubKind, Platform,
    };
    use crate::db::sync_queue::{self, ContentSyncOptions};
    use crate::db::test_support::memory_pool;
    use chrono::DateTime;

    fn sample_item(id: &str) -> ClipboardItem {
        let ts = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let content = format!("content-{id}");
        ClipboardItem {
            id: id.to_owned(),
            kind: ClipboardKind::Text,
            sub_kind: None,
            group_id: None,
            source_app_id: None,
            source_revision: id.to_owned(),
            source_device_id: None,
            source_device_name: None,
            source_updated_at: None,
            source_device_is_local: false,
            content_hash: content_hash(ClipboardKind::Text, &content),
            sync_key: content_hash(ClipboardKind::Text, &content),
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
            created_at: ts,
            updated_at: ts,
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            source_app_accent_start: None,
            source_app_accent_end: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            remote_file_state: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        }
    }

    fn ids(items: &[ClipboardItem]) -> Vec<&str> {
        items.iter().map(|item| item.id.as_str()).collect()
    }

    #[tokio::test]
    async fn manual_share_is_atomic_and_survives_local_reuse_without_auto_sync() {
        let pool = memory_pool().await;
        let mut item = sample_item("share");
        item.is_sensitive = true;
        let (saved, staged) = upsert_item_with_sync_outbox(
            &pool,
            &item,
            Some(ContentSyncOptions {
                manual_share: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert!(staged);
        let first = sync_queue::next_ready_content(&pool, false)
            .await
            .unwrap()
            .unwrap();
        assert!(first.manual_share && first.force_files);
        assert_eq!(first.item_id, saved.id);

        item.source_revision = "recaptured".to_owned();
        item.updated_at += chrono::Duration::seconds(1);
        let (reused, staged) = upsert_item_with_sync_outbox(&pool, &item, None)
            .await
            .unwrap();
        assert!(reused.deduplicated && staged);
        assert_eq!(reused.id, saved.id);
        sync_queue::complete_content(&pool, first.operation_order)
            .await
            .unwrap();
        let current = sync_queue::next_ready_content(&pool, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.source_revision, "recaptured");
        assert!(current.manual_share && current.force_files);

        sync_queue::complete_content(&pool, current.operation_order)
            .await
            .unwrap();
        let (_, staged) = upsert_item_with_sync_outbox(&pool, &item, None)
            .await
            .unwrap();
        assert!(!staged);
        assert!(sync_queue::next_ready_content(&pool, false)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn manual_share_queue_failure_rolls_back_history() {
        let pool = memory_pool().await;
        sqlx::query(
            "CREATE TRIGGER reject_share BEFORE INSERT ON sync_content_outbox \
             WHEN NEW.manual_share = 1 BEGIN SELECT RAISE(ABORT, 'queue unavailable'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let item = sample_item("share");
        assert!(upsert_item_with_sync_outbox(
            &pool,
            &item,
            Some(ContentSyncOptions {
                manual_share: true,
                ..Default::default()
            }),
        )
        .await
        .is_err());
        assert!(find_item_by_id(&pool, &item.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn history_cleanup_preserves_manual_shares_until_content_is_materialized() {
        let pool = memory_pool().await;
        let item = sample_item("share");
        upsert_item_with_sync_outbox(
            &pool,
            &item,
            Some(ContentSyncOptions {
                manual_share: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        for index in 1..=2 {
            let mut recent = sample_item(&format!("ordinary-{index}"));
            recent.created_at += chrono::Duration::seconds(index);
            insert_item(&pool, &recent).await.unwrap();
        }
        assert_eq!(
            cleanup_history(&pool, None, Some(1)).await.unwrap().removed,
            1
        );
        assert!(find_item_by_id(&pool, &item.id).await.unwrap().is_some());
        assert_eq!(
            cleanup_history(&pool, Some(Utc::now()), None)
                .await
                .unwrap()
                .removed,
            1
        );
        assert!(find_item_by_id(&pool, &item.id).await.unwrap().is_some());
        let pending = sync_queue::next_ready_content(&pool, false)
            .await
            .unwrap()
            .unwrap();
        sync_queue::complete_content(&pool, pending.operation_order)
            .await
            .unwrap();
        assert_eq!(
            cleanup_history(&pool, Some(Utc::now()), None)
                .await
                .unwrap()
                .removed,
            1
        );
    }

    #[tokio::test]
    async fn history_cleanup_releases_expired_manual_share_protection() {
        let pool = memory_pool().await;
        let item = sample_item("expired-share");
        upsert_item_with_sync_outbox(
            &pool,
            &item,
            Some(ContentSyncOptions {
                manual_share: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE sync_content_outbox SET updated_at = datetime('now', '-25 hours') WHERE item_id = ?",
        )
        .bind(&item.id)
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            cleanup_history(&pool, Some(Utc::now()), None)
                .await
                .unwrap()
                .removed,
            1
        );
        assert!(find_item_by_id(&pool, &item.id).await.unwrap().is_none());
    }

    async fn insert_source_app(pool: &SqlitePool, id: &str) {
        let ts = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        upsert_app(
            pool,
            &ClipboardApp {
                id: id.into(),
                name: id.into(),
                icon_file: None,
                icon_hash: None,
                accent_start: None,
                accent_end: None,
                platform: Platform::Macos,
                created_at: ts,
                updated_at: ts,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn insert_and_find_by_id_roundtrip() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("a")).await.unwrap();

        let found = find_item_by_id(&pool, "a")
            .await
            .unwrap()
            .expect("item should exist");
        assert_eq!(found.id, "a");
        assert_eq!(found.content, "content-a");
        assert_eq!(found.kind, ClipboardKind::Text);
        assert_eq!(found.use_count, 1);

        assert!(find_item_by_id(&pool, "missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_view_keeps_full_url_but_omits_other_text_content() {
        let pool = memory_pool().await;
        let mut url = sample_item("url");
        url.sub_kind = Some(ClipboardSubKind::Url);
        url.content = "https://example.com/a/complete/path?query=value".to_owned();
        url.summary = Some("https://example.com/a/complete/path".to_owned());
        insert_item(&pool, &url).await.unwrap();
        insert_item(&pool, &sample_item("plain")).await.unwrap();

        let listed_url = find_item_for_list_by_id(&pool, "url")
            .await
            .unwrap()
            .unwrap();
        let listed_plain = find_item_for_list_by_id(&pool, "plain")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(listed_url.content, url.content);
        assert!(listed_plain.content.is_empty());
    }

    #[tokio::test]
    async fn list_view_reports_full_plain_text_char_count() {
        let pool = memory_pool().await;
        let plain_text = "你😀".repeat(150);
        let mut rich_text = sample_item("rich-text");
        rich_text.sub_kind = Some(ClipboardSubKind::Html);
        rich_text.content = "<p>HTML source is intentionally a different length</p>".to_owned();
        rich_text.search_text = Some(plain_text.clone());
        rich_text.summary = Some(plain_text.chars().take(256).collect());
        insert_item(&pool, &rich_text).await.unwrap();

        let listed = find_item_for_list_by_id(&pool, "rich-text")
            .await
            .unwrap()
            .unwrap();

        assert!(listed.content.is_empty());
        assert_eq!(listed.summary.as_deref().unwrap().chars().count(), 256);
        assert_eq!(listed.text_char_count, Some(300));
    }

    #[tokio::test]
    async fn recent_item_ids_for_sync_uses_updated_order() {
        let pool = memory_pool().await;
        for (id, updated_at) in [
            ("old", 1_700_000_001),
            ("new", 1_700_000_003),
            ("mid", 1_700_000_002),
        ] {
            let mut item = sample_item(id);
            item.updated_at = DateTime::from_timestamp(updated_at, 0).unwrap();
            insert_item(&pool, &item).await.unwrap();
        }

        let item_ids = recent_item_ids_for_sync(&pool, 2).await.unwrap();

        assert_eq!(item_ids, ["new", "mid"]);
    }

    #[tokio::test]
    async fn recent_item_ids_for_sync_excludes_remote_only_records_before_limit() {
        let pool = memory_pool().await;
        for id in ["remote", "local", "unlinked"] {
            insert_item(&pool, &sample_item(id)).await.unwrap();
        }
        for (event_id, item_id, direction, sequence) in [
            ("remote-event", "remote", "remote", 1),
            ("local-event", "local", "local", 2),
        ] {
            let event = ecopaste_sync_protocol::EncryptedEvent {
                event_id: event_id.into(),
                origin_device_id: "device".into(),
                origin_sequence: sequence,
                created_at_ms: sequence as i64,
                nonce: vec![0; 24],
                ciphertext: vec![1],
                kind: ecopaste_sync_protocol::SyncEventKind::Content,
                blob_ids: Vec::new(),
                optional_blob_ids: Vec::new(),
                retention_key: None,
            };
            crate::sync::repository::insert_event(&pool, &event, true, &[])
                .await
                .unwrap();
            crate::sync::repository::link_event_to_item(&pool, item_id, event_id, direction)
                .await
                .unwrap();
        }

        let ids = recent_item_ids_for_sync(&pool, 10).await.unwrap();

        assert!(ids.iter().any(|id| id == "local"));
        assert!(ids.iter().any(|id| id == "unlinked"));
        assert!(!ids.iter().any(|id| id == "remote"));
    }

    #[tokio::test]
    async fn upsert_inserts_when_content_is_new() {
        let pool = memory_pool().await;
        let result = upsert_item(&pool, &sample_item("a")).await.unwrap();
        assert_eq!(
            result,
            UpsertResult {
                id: "a".to_owned(),
                deduplicated: false,
            }
        );
        assert_eq!(
            find_item_by_id(&pool, "a")
                .await
                .unwrap()
                .unwrap()
                .use_count,
            1
        );
    }

    #[tokio::test]
    async fn upsert_dedups_same_content_bumping_use_count() {
        let pool = memory_pool().await;
        let first = sample_item("first");
        upsert_item(&pool, &first).await.unwrap();

        // 不同 id，但内容相同 → content_hash 相同 → 命中去重，不插入新行。
        let mut dup = sample_item("second");
        dup.content = first.content.clone();
        dup.content_hash = first.content_hash.clone();
        let result = upsert_item(&pool, &dup).await.unwrap();

        assert_eq!(
            result,
            UpsertResult {
                id: "first".to_owned(),
                deduplicated: true,
            }
        );
        // 仍只有一行，且命中行 use_count 累加到 2。
        let all = query_items(&pool, &ClipboardItemQuery::default())
            .await
            .unwrap();
        assert_eq!(ids(&all), ["first"]);
        assert_eq!(all[0].use_count, 2);
        assert!(find_item_by_id(&pool, "second").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn synced_upsert_merges_timestamps_without_counting_network_receipt_as_reuse() {
        let pool = memory_pool().await;
        let mut existing = sample_item("existing");
        existing.created_at = DateTime::from_timestamp(1_700_000_010, 0).unwrap();
        existing.updated_at = DateTime::from_timestamp(1_700_000_020, 0).unwrap();
        insert_item(&pool, &existing).await.unwrap();

        let mut incoming = sample_item("incoming");
        incoming.content = existing.content.clone();
        incoming.content_hash = existing.content_hash.clone();
        incoming.sync_key = existing.sync_key.clone();
        incoming.created_at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        incoming.updated_at = DateTime::from_timestamp(1_700_000_030, 0).unwrap();

        let result = upsert_synced_item(&pool, &incoming).await.unwrap();
        let merged = find_item_by_id(&pool, "existing").await.unwrap().unwrap();

        assert_eq!(result.id, "existing");
        assert!(result.deduplicated);
        assert_eq!(merged.created_at, incoming.created_at);
        assert_eq!(merged.updated_at, incoming.updated_at);
        assert_eq!(merged.use_count, 1);
    }

    #[tokio::test]
    async fn remote_file_metadata_is_atomic_and_promotes_in_place() {
        let pool = memory_pool().await;
        let event = ecopaste_sync_protocol::EncryptedEvent {
            event_id: "remote-event".into(),
            origin_device_id: "origin".into(),
            origin_sequence: 1,
            created_at_ms: 1,
            nonce: vec![1; 24],
            ciphertext: vec![2; 32],
            kind: ecopaste_sync_protocol::SyncEventKind::Content,
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
            retention_key: None,
        };
        crate::sync::repository::insert_event(&pool, &event, false, &[])
            .await
            .unwrap();
        let mut remote = sample_item("remote");
        remote.kind = ClipboardKind::Files;
        remote.content = "large.bin".into();
        remote.content_hash = remote.sync_key.clone();
        remote.size = Some(10);

        let inserted = upsert_remote_file_item(&pool, &remote, &event.event_id, "origin")
            .await
            .unwrap();
        assert_eq!(inserted.id, "remote");
        assert_eq!(
            find_item_by_id(&pool, "remote")
                .await
                .unwrap()
                .unwrap()
                .remote_file_state
                .as_deref(),
            Some("available")
        );

        let mut materialized = remote.clone();
        materialized.id = "materialized".into();
        materialized.content = "/managed/large.bin".into();
        materialized.content_hash = content_hash(ClipboardKind::Files, &materialized.content);
        materialized.source_revision = "materialized".into();
        let promoted = upsert_materialized_synced_item(&pool, &materialized)
            .await
            .unwrap();
        assert_eq!(promoted.id, "remote");
        let stored = find_item_by_id(&pool, "remote").await.unwrap().unwrap();
        assert_eq!(stored.content, materialized.content);
        assert!(stored.remote_file_state.is_none());
        let marker_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_remote_file_items WHERE item_id = ?")
                .bind("remote")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(marker_count, 0);
    }

    #[tokio::test]
    async fn unavailable_local_file_can_become_a_remote_metadata_card() {
        let pool = memory_pool().await;
        let event = ecopaste_sync_protocol::EncryptedEvent {
            event_id: "remote-event".into(),
            origin_device_id: "origin".into(),
            origin_sequence: 1,
            created_at_ms: 1,
            nonce: vec![1; 24],
            ciphertext: vec![2; 32],
            kind: ecopaste_sync_protocol::SyncEventKind::Content,
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
            retention_key: None,
        };
        crate::sync::repository::insert_event(&pool, &event, false, &[])
            .await
            .unwrap();
        let mut local = sample_item("local");
        local.kind = ClipboardKind::Files;
        local.content = "/missing/large.bin".into();
        local.content_hash = content_hash(local.kind, &local.content);
        local.is_favorite = true;
        insert_item(&pool, &local).await.unwrap();

        let mut remote = local.clone();
        remote.id = "remote".into();
        remote.content = "large.bin".into();
        remote.content_hash = content_hash(remote.kind, &remote.content);
        let result = upsert_remote_file_item(&pool, &remote, &event.event_id, "origin")
            .await
            .unwrap();
        let stored = find_item_by_id(&pool, &local.id).await.unwrap().unwrap();

        assert_eq!(result.id, local.id);
        assert_eq!(stored.content, "large.bin");
        assert!(stored.is_favorite);
        assert_eq!(stored.remote_file_state.as_deref(), Some("available"));
    }

    #[tokio::test]
    async fn materialized_file_repairs_paths_of_a_same_key_local_record() {
        let pool = memory_pool().await;
        let mut local = sample_item("local");
        local.kind = ClipboardKind::Files;
        local.content = "/missing/large.bin".into();
        local.content_hash = content_hash(local.kind, &local.content);
        local.size = Some(10);
        local.file_types = Some("f".into());
        local.is_pinned = true;
        insert_item(&pool, &local).await.unwrap();

        let mut materialized = local.clone();
        materialized.id = "materialized".into();
        materialized.content = "ecopaste-resource://sync-files/event/000/large.bin".into();
        materialized.content_hash = content_hash(materialized.kind, &materialized.content);
        materialized.size = Some(20);
        materialized.file_types = Some("d".into());
        let result = upsert_materialized_synced_item(&pool, &materialized)
            .await
            .unwrap();
        let stored = find_item_by_id(&pool, &local.id).await.unwrap().unwrap();

        assert_eq!(result.id, local.id);
        assert_eq!(stored.content, materialized.content);
        assert_eq!(stored.size, materialized.size);
        assert_eq!(stored.file_types, materialized.file_types);
        assert!(stored.is_pinned);
    }

    #[tokio::test]
    async fn repeated_remote_file_event_refreshes_content_metadata() {
        let pool = memory_pool().await;
        let event = ecopaste_sync_protocol::EncryptedEvent {
            event_id: "remote-event-1".into(),
            origin_device_id: "origin".into(),
            origin_sequence: 1,
            created_at_ms: 1,
            nonce: vec![1; 24],
            ciphertext: vec![2; 32],
            kind: ecopaste_sync_protocol::SyncEventKind::Content,
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
            retention_key: None,
        };
        crate::sync::repository::insert_event(&pool, &event, false, &[])
            .await
            .unwrap();
        let mut remote = sample_item("remote");
        remote.kind = ClipboardKind::Files;
        remote.content = "large.bin".into();
        remote.content_hash = remote.sync_key.clone();
        remote.size = Some(10);
        remote.file_types = Some("f".into());
        upsert_remote_file_item(&pool, &remote, &event.event_id, "origin")
            .await
            .unwrap();

        let mut next_event = event.clone();
        next_event.event_id = "remote-event-2".into();
        next_event.origin_sequence = 2;
        crate::sync::repository::insert_event(&pool, &next_event, false, &[])
            .await
            .unwrap();
        let mut refreshed = remote.clone();
        refreshed.source_revision = "new-revision".into();
        refreshed.size = Some(20);
        refreshed.file_types = Some("d".into());
        upsert_remote_file_item(&pool, &refreshed, &next_event.event_id, "origin")
            .await
            .unwrap();

        let stored = find_item_by_id(&pool, &remote.id).await.unwrap().unwrap();
        assert_eq!(stored.size, refreshed.size);
        assert_eq!(stored.file_types, refreshed.file_types);
        assert_eq!(stored.source_revision, refreshed.source_revision);
    }

    #[tokio::test]
    async fn remote_file_metadata_rolls_back_when_event_registration_fails() {
        let pool = memory_pool().await;
        let mut remote = sample_item("remote");
        remote.kind = ClipboardKind::Files;
        remote.content = "large.bin".into();

        assert!(
            upsert_remote_file_item(&pool, &remote, "missing-event", "origin")
                .await
                .is_err()
        );
        assert!(find_item_by_id(&pool, "remote").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn synced_upsert_uses_latest_source_and_revision_as_tie_breaker() {
        let pool = memory_pool().await;
        insert_source_app(&pool, "old-app").await;
        insert_source_app(&pool, "new-app").await;

        let mut existing = sample_item("existing");
        existing.source_app_id = Some("old-app".into());
        existing.source_revision = "revision-b".into();
        insert_item(&pool, &existing).await.unwrap();

        let mut older_revision = sample_item("incoming-old");
        older_revision.content = existing.content.clone();
        older_revision.content_hash = existing.content_hash.clone();
        older_revision.sync_key = existing.sync_key.clone();
        older_revision.source_app_id = Some("new-app".into());
        older_revision.source_revision = "revision-a".into();
        upsert_synced_item(&pool, &older_revision).await.unwrap();
        assert_eq!(
            find_item_by_id(&pool, "existing")
                .await
                .unwrap()
                .unwrap()
                .source_app_id
                .as_deref(),
            Some("old-app")
        );

        let mut newer_revision = older_revision;
        newer_revision.source_revision = "revision-c".into();
        upsert_synced_item(&pool, &newer_revision).await.unwrap();
        let merged = find_item_by_id(&pool, "existing").await.unwrap().unwrap();
        assert_eq!(merged.source_app_id.as_deref(), Some("new-app"));
        assert_eq!(merged.source_revision, "revision-c");
    }

    #[tokio::test]
    async fn local_reuse_replaces_source_with_new_capture() {
        let pool = memory_pool().await;
        insert_source_app(&pool, "old-app").await;
        insert_source_app(&pool, "new-app").await;

        let mut existing = sample_item("existing");
        existing.source_app_id = Some("old-app".into());
        insert_item(&pool, &existing).await.unwrap();

        let mut captured = sample_item("captured");
        captured.content = existing.content.clone();
        captured.content_hash = existing.content_hash.clone();
        captured.source_app_id = Some("new-app".into());
        captured.updated_at = existing.updated_at + chrono::Duration::seconds(1);
        upsert_item(&pool, &captured).await.unwrap();

        let merged = find_item_by_id(&pool, "existing").await.unwrap().unwrap();
        assert_eq!(merged.source_app_id.as_deref(), Some("new-app"));
        assert_eq!(merged.source_revision, "captured");
        assert_eq!(merged.use_count, 2);
    }

    #[tokio::test]
    async fn local_file_reuse_invalidates_then_refills_recursive_size() {
        let pool = memory_pool().await;
        let mut existing = sample_item("existing");
        existing.kind = ClipboardKind::Files;
        existing.content = "/tmp/folder".into();
        existing.content_hash = content_hash(existing.kind, &existing.content);
        existing.size = Some(10);
        insert_item(&pool, &existing).await.unwrap();

        let mut captured = existing.clone();
        captured.id = "captured".into();
        captured.source_revision = "captured".into();
        captured.updated_at = existing.updated_at + chrono::Duration::seconds(1);
        captured.size = None;
        let result = upsert_item(&pool, &captured).await.unwrap();

        assert!(result.deduplicated);
        assert_eq!(
            find_item_by_id(&pool, &existing.id)
                .await
                .unwrap()
                .unwrap()
                .size,
            None
        );
        assert!(
            !fill_file_item_size(&pool, &existing.id, "stale-revision", 20)
                .await
                .unwrap()
        );
        assert!(
            fill_file_item_size(&pool, &existing.id, &captured.source_revision, 20)
                .await
                .unwrap()
        );
        assert_eq!(
            find_item_by_id(&pool, &existing.id)
                .await
                .unwrap()
                .unwrap()
                .size,
            Some(20)
        );
    }

    #[tokio::test]
    async fn file_size_backfill_query_only_returns_missing_file_sizes() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("text")).await.unwrap();
        let mut missing = sample_item("missing");
        missing.kind = ClipboardKind::Files;
        missing.content_hash = content_hash(missing.kind, &missing.content);
        insert_item(&pool, &missing).await.unwrap();
        let mut complete = sample_item("complete");
        complete.kind = ClipboardKind::Files;
        complete.content_hash = content_hash(complete.kind, &complete.content);
        complete.size = Some(10);
        insert_item(&pool, &complete).await.unwrap();

        let items = file_items_missing_size(&pool).await.unwrap();

        assert_eq!(ids(&items), ["missing"]);
    }

    #[tokio::test]
    async fn upsert_keeps_distinct_content_separate() {
        let pool = memory_pool().await;
        upsert_item(&pool, &sample_item("a")).await.unwrap();
        upsert_item(&pool, &sample_item("b")).await.unwrap();

        let all = query_items(&pool, &ClipboardItemQuery::default())
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn query_default_sorts_by_updated_at() {
        assert_eq!(
            ClipboardItemQuery::default().sort,
            ClipboardItemSort::UpdatedAt
        );
    }

    #[test]
    fn content_hash_is_stable_and_kind_scoped() {
        // 同 kind 同内容 → 同哈希（稳定、可去重）。
        assert_eq!(
            content_hash(ClipboardKind::Text, "hello"),
            content_hash(ClipboardKind::Text, "hello")
        );
        // 内容不同 → 哈希不同。
        assert_ne!(
            content_hash(ClipboardKind::Text, "hello"),
            content_hash(ClipboardKind::Text, "world")
        );
        // 内容相同但 kind 不同 → 哈希不同（kind 前缀隔离）。
        assert_ne!(
            content_hash(ClipboardKind::Text, "same"),
            content_hash(ClipboardKind::Files, "same")
        );
    }

    #[tokio::test]
    async fn query_filters_by_kind() {
        let pool = memory_pool().await;
        let mut text = sample_item("text");
        text.kind = ClipboardKind::Text;
        let mut image = sample_item("image");
        image.kind = ClipboardKind::Image;
        insert_item(&pool, &text).await.unwrap();
        insert_item(&pool, &image).await.unwrap();

        let q = ClipboardItemQuery {
            kind: Some(ClipboardKind::Image),
            ..Default::default()
        };
        assert_eq!(ids(&query_items(&pool, &q).await.unwrap()), ["image"]);
    }

    #[tokio::test]
    async fn query_filters_by_favorite_and_pinned() {
        let pool = memory_pool().await;
        let plain = sample_item("plain");
        let mut fav = sample_item("fav");
        fav.is_favorite = true;
        let mut pin = sample_item("pin");
        pin.is_pinned = true;
        for item in [&plain, &fav, &pin] {
            insert_item(&pool, item).await.unwrap();
        }

        let favs = query_items(
            &pool,
            &ClipboardItemQuery {
                favorite: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(ids(&favs), ["fav"]);

        let pins = query_items(
            &pool,
            &ClipboardItemQuery {
                pinned: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(ids(&pins), ["pin"]);
    }

    #[tokio::test]
    async fn query_filters_by_group() {
        let pool = memory_pool().await;
        let group = ClipboardGroup {
            id: "g1".to_owned(),
            name: "G1".to_owned(),
            icon: "i-lets-icons:folder".to_owned(),
            is_hidden: false,
            sort_order: 0,
            created_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            updated_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        };
        insert_group(&pool, &group).await.unwrap();

        let mut grouped = sample_item("grouped");
        grouped.group_id = Some("g1".to_owned());
        insert_item(&pool, &grouped).await.unwrap();
        insert_item(&pool, &sample_item("ungrouped")).await.unwrap();

        let q = ClipboardItemQuery {
            group_id: Some("g1".to_owned()),
            ..Default::default()
        };
        assert_eq!(ids(&query_items(&pool, &q).await.unwrap()), ["grouped"]);
    }

    #[tokio::test]
    async fn update_item_group_does_not_refresh_updated_at() {
        let pool = memory_pool().await;
        let group = ClipboardGroup {
            id: "g1".to_owned(),
            name: "G1".to_owned(),
            icon: "i-lets-icons:folder".to_owned(),
            is_hidden: false,
            sort_order: 0,
            created_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            updated_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        };
        insert_group(&pool, &group).await.unwrap();

        let item = sample_item("item");
        let original_updated_at = item.updated_at;
        insert_item(&pool, &item).await.unwrap();

        update_item_group(&pool, "item", Some("g1")).await.unwrap();

        let moved = find_item_by_id(&pool, "item").await.unwrap().unwrap();
        assert_eq!(moved.group_id, Some("g1".to_owned()));
        assert_eq!(moved.updated_at, original_updated_at);
    }

    #[tokio::test]
    async fn history_index_preserves_filters_sort_keys_and_pagination() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run(&pool).await.unwrap();
        sqlx::query("DROP INDEX idx_clipboard_items_updated_sort")
            .execute(&pool)
            .await
            .unwrap();
        for n in 0..12 {
            let mut item = sample_item(&format!("item-{n}"));
            item.created_at = DateTime::from_timestamp(1_700_000_000 + n / 2, 0).unwrap();
            item.updated_at = DateTime::from_timestamp(1_700_000_020 - n, 0).unwrap();
            item.use_count = n % 3;
            item.is_pinned = n % 4 == 0;
            item.is_favorite = n % 2 == 0;
            item.search_text = Some(format!("searchable-{n}"));
            insert_item(&pool, &item).await.unwrap();
        }

        let mut snapshots = Vec::new();
        for sort in [
            ClipboardItemSort::CreatedAt,
            ClipboardItemSort::UpdatedAt,
            ClipboardItemSort::UseCount,
        ] {
            for filter in [
                ClipboardItemQuery::default(),
                ClipboardItemQuery {
                    favorite: Some(true),
                    ..Default::default()
                },
                ClipboardItemQuery {
                    pinned: Some(false),
                    ..Default::default()
                },
                ClipboardItemQuery {
                    kind: Some(ClipboardKind::Image),
                    ..Default::default()
                },
                ClipboardItemQuery {
                    keyword: Some("searchable-1".into()),
                    ..Default::default()
                },
                ClipboardItemQuery {
                    keyword: Some("-1".into()),
                    ..Default::default()
                },
            ] {
                let query = ClipboardItemQuery { sort, ..filter };
                let result = query_items_page(&pool, &query).await.unwrap();
                snapshots.push((query, result));
            }
        }

        sqlx::query(
            "CREATE INDEX idx_clipboard_items_updated_sort \
             ON clipboard_items(is_pinned DESC, updated_at DESC, created_at DESC)",
        )
        .execute(&pool)
        .await
        .unwrap();
        for (query, (before, before_total)) in snapshots {
            let (after, after_total) = query_items_page(&pool, &query).await.unwrap();
            assert_eq!(before_total, after_total);
            let mut before_ids = ids(&before);
            let mut after_ids = ids(&after);
            before_ids.sort_unstable();
            after_ids.sort_unstable();
            assert_eq!(before_ids, after_ids);

            let sort_key = |item: &ClipboardItem| {
                let primary = match query.sort {
                    ClipboardItemSort::CreatedAt => item.created_at.timestamp(),
                    ClipboardItemSort::UpdatedAt => item.updated_at.timestamp(),
                    ClipboardItemSort::UseCount => item.use_count,
                };
                (item.is_pinned, primary, item.created_at.timestamp())
            };
            assert_eq!(
                before.iter().map(sort_key).collect::<Vec<_>>(),
                after.iter().map(sort_key).collect::<Vec<_>>()
            );

            let mut paged = Vec::new();
            for offset in (0..=after_total).step_by(2) {
                let page_query = ClipboardItemQuery {
                    limit: 2,
                    offset,
                    ..query.clone()
                };
                let (page, total) = query_items_page(&pool, &page_query).await.unwrap();
                assert_eq!(total, after_total);
                paged.extend(page);
            }
            assert_eq!(ids(&paged), ids(&after));
        }

        let mut plan_query: QueryBuilder<Sqlite> = QueryBuilder::new("EXPLAIN QUERY PLAN ");
        plan_query.push(LIST_SELECT_ITEM).push(
            " WHERE 1 = 1 ORDER BY clipboard_items.is_pinned DESC, \
             clipboard_items.updated_at DESC, clipboard_items.created_at DESC LIMIT ",
        );
        plan_query
            .push_bind(20_i64)
            .push(" OFFSET ")
            .push_bind(0_i64);
        let plan: Vec<(i64, i64, i64, String)> =
            plan_query.build_query_as().fetch_all(&pool).await.unwrap();
        assert!(plan
            .iter()
            .any(|row| row.3.contains("idx_clipboard_items_updated_sort")));
        assert!(!plan.iter().any(|row| row.3.contains("TEMP B-TREE")));
    }

    #[tokio::test]
    async fn query_orders_pinned_first_then_by_sort() {
        let pool = memory_pool().await;
        let mut a = sample_item("a");
        a.created_at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        a.updated_at = DateTime::from_timestamp(1_700_000_010, 0).unwrap();
        a.use_count = 9;
        let mut b = sample_item("b");
        b.created_at = DateTime::from_timestamp(1_700_000_010, 0).unwrap();
        b.updated_at = DateTime::from_timestamp(1_700_000_020, 0).unwrap();
        b.is_pinned = true;
        b.use_count = 1;
        let mut c = sample_item("c");
        c.created_at = DateTime::from_timestamp(1_700_000_020, 0).unwrap();
        c.updated_at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        c.use_count = 5;
        for item in [&a, &b, &c] {
            insert_item(&pool, item).await.unwrap();
        }

        // 置顶项 b 恒前置；其余按时间倒序 c(20) > a(0)。
        let by_time = query_items(
            &pool,
            &ClipboardItemQuery {
                sort: ClipboardItemSort::CreatedAt,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(ids(&by_time), ["b", "c", "a"]);

        // 置顶项 b 恒前置；其余按更新时间倒序 a(10) > c(0)。
        let by_updated = query_items(
            &pool,
            &ClipboardItemQuery {
                sort: ClipboardItemSort::UpdatedAt,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(ids(&by_updated), ["b", "a", "c"]);

        // 置顶项 b 恒前置；其余按使用次数倒序 a(9) > c(5)。
        let by_use = query_items(
            &pool,
            &ClipboardItemQuery {
                sort: ClipboardItemSort::UseCount,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(ids(&by_use), ["b", "a", "c"]);
    }

    #[tokio::test]
    async fn query_paginates_with_limit_and_offset() {
        let pool = memory_pool().await;
        for n in 0..5i64 {
            let mut item = sample_item(&format!("id{n}"));
            item.created_at = DateTime::from_timestamp(1_700_000_000 + n, 0).unwrap();
            insert_item(&pool, &item).await.unwrap();
        }

        let page = |limit, offset| ClipboardItemQuery {
            limit,
            offset,
            ..Default::default()
        };
        // created_at 倒序：id4, id3, id2, id1, id0
        assert_eq!(
            ids(&query_items(&pool, &page(2, 0)).await.unwrap()),
            ["id4", "id3"]
        );
        assert_eq!(
            ids(&query_items(&pool, &page(2, 2)).await.unwrap()),
            ["id2", "id1"]
        );
        assert_eq!(
            ids(&query_items(&pool, &page(2, 4)).await.unwrap()),
            ["id0"]
        );
    }

    #[tokio::test]
    async fn search_fts_matches_prefix_across_columns() {
        let pool = memory_pool().await;
        let mut a = sample_item("a");
        a.content = "hello rustacean".to_owned();
        a.search_text = Some("hello rustacean".to_owned());
        let mut b = sample_item("b");
        b.content = "goodbye world".to_owned();
        b.search_text = Some("searchable token".to_owned());
        let mut c = sample_item("c");
        c.content = "plain".to_owned();
        c.note = Some("annotated".to_owned());
        for item in [&a, &b, &c] {
            insert_item(&pool, item).await.unwrap();
        }

        let search = |kw: &str| ClipboardItemQuery {
            keyword: Some(kw.to_owned()),
            ..Default::default()
        };
        assert_eq!(
            ids(&query_items(&pool, &search("rust")).await.unwrap()),
            ["a"]
        );
        assert_eq!(
            ids(&query_items(&pool, &search("token")).await.unwrap()),
            ["b"]
        );
        assert_eq!(
            ids(&query_items(&pool, &search("annot")).await.unwrap()),
            ["c"]
        );
        assert!(query_items(&pool, &search("zzz")).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn search_fts_still_applies_filters() {
        let pool = memory_pool().await;
        let mut x = sample_item("x");
        x.content = "shared text".to_owned();
        x.search_text = Some("shared text".to_owned());
        x.is_favorite = true;
        let mut y = sample_item("y");
        y.content = "shared note".to_owned();
        y.search_text = Some("shared note".to_owned());
        insert_item(&pool, &x).await.unwrap();
        insert_item(&pool, &y).await.unwrap();

        let q = ClipboardItemQuery {
            keyword: Some("shared".to_owned()),
            favorite: Some(true),
            ..Default::default()
        };
        assert_eq!(ids(&query_items(&pool, &q).await.unwrap()), ["x"]);
    }

    #[tokio::test]
    async fn toggle_favorite_and_pinned_flip_flags() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("a")).await.unwrap();

        toggle_item_favorite(&pool, "a").await.unwrap();
        assert!(
            find_item_by_id(&pool, "a")
                .await
                .unwrap()
                .unwrap()
                .is_favorite
        );
        toggle_item_favorite(&pool, "a").await.unwrap();
        assert!(
            !find_item_by_id(&pool, "a")
                .await
                .unwrap()
                .unwrap()
                .is_favorite
        );

        toggle_item_pinned(&pool, "a").await.unwrap();
        assert!(
            find_item_by_id(&pool, "a")
                .await
                .unwrap()
                .unwrap()
                .is_pinned
        );
        toggle_item_pinned(&pool, "a").await.unwrap();
        assert!(
            !find_item_by_id(&pool, "a")
                .await
                .unwrap()
                .unwrap()
                .is_pinned
        );
    }

    #[tokio::test]
    async fn update_note_sets_and_clears() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("a")).await.unwrap();

        update_item_note(&pool, "a", Some("my note")).await.unwrap();
        assert_eq!(
            find_item_by_id(&pool, "a")
                .await
                .unwrap()
                .unwrap()
                .note
                .as_deref(),
            Some("my note")
        );

        update_item_note(&pool, "a", None).await.unwrap();
        assert_eq!(
            find_item_by_id(&pool, "a").await.unwrap().unwrap().note,
            None
        );
    }

    #[tokio::test]
    async fn increment_use_count_bumps_count_and_updated_at() {
        let pool = memory_pool().await;
        let item = sample_item("a");
        let original_updated_at = item.updated_at;
        insert_item(&pool, &item).await.unwrap();

        increment_item_use_count(&pool, "a").await.unwrap();

        let after = find_item_by_id(&pool, "a").await.unwrap().unwrap();
        assert_eq!(after.use_count, 2);
        assert!(after.updated_at > original_updated_at);
    }

    #[tokio::test]
    async fn delete_item_removes_row() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("a")).await.unwrap();

        // 文本行删除：返回删除结果且无图片文件名。
        let outcome = delete_item(&pool, "a")
            .await
            .unwrap()
            .expect("item was deleted");
        assert_eq!(outcome.image_file_name, None);
        assert!(find_item_by_id(&pool, "a").await.unwrap().is_none());

        // 记录不存在：同样返回 None，不报错。
        assert_eq!(delete_item(&pool, "missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_image_item_returns_file_name() {
        let pool = memory_pool().await;
        let mut img = sample_item("img");
        img.kind = ClipboardKind::Image;
        img.content = "deadbeef.png".to_owned();
        img.content_hash = content_hash(ClipboardKind::Image, "deadbeef.png");
        insert_item(&pool, &img).await.unwrap();

        // 图片行删除：返回落盘文件名供调用方删图。
        assert_eq!(
            delete_item(&pool, "img")
                .await
                .unwrap()
                .and_then(|o| o.image_file_name),
            Some("deadbeef.png".to_owned())
        );
        assert!(find_item_by_id(&pool, "img").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_items_returns_count_and_ignores_empty() {
        let pool = memory_pool().await;
        for id in ["a", "b", "c"] {
            insert_item(&pool, &sample_item(id)).await.unwrap();
        }

        assert_eq!(delete_items(&pool, &[]).await.unwrap(), 0);

        let removed = delete_items(
            &pool,
            &["a".to_owned(), "b".to_owned(), "missing".to_owned()],
        )
        .await
        .unwrap();
        assert_eq!(removed, 2);
        assert_eq!(
            ids(&query_items(&pool, &ClipboardItemQuery::default())
                .await
                .unwrap()),
            ["c"]
        );
    }

    #[tokio::test]
    async fn clear_items_optionally_keeps_favorites_and_pinned() {
        let pool = memory_pool().await;
        let mut fav = sample_item("fav");
        fav.is_favorite = true;
        fav.created_at = DateTime::from_timestamp(1_700_000_001, 0).unwrap();
        insert_item(&pool, &fav).await.unwrap();
        let mut pin = sample_item("pin");
        pin.is_pinned = true;
        pin.created_at = DateTime::from_timestamp(1_700_000_002, 0).unwrap();
        insert_item(&pool, &pin).await.unwrap();
        let mut both = sample_item("both");
        both.is_favorite = true;
        both.is_pinned = true;
        both.created_at = DateTime::from_timestamp(1_700_000_003, 0).unwrap();
        insert_item(&pool, &both).await.unwrap();
        let mut plain = sample_item("plain");
        plain.created_at = DateTime::from_timestamp(1_700_000_004, 0).unwrap();
        insert_item(&pool, &plain).await.unwrap();

        assert_eq!(clear_items(&pool, false, false).await.unwrap().removed, 1);
        assert_eq!(
            ids(&query_items(&pool, &ClipboardItemQuery::default())
                .await
                .unwrap()),
            ["both", "pin", "fav"]
        );

        assert_eq!(clear_items(&pool, true, false).await.unwrap().removed, 1);
        assert_eq!(
            ids(&query_items(&pool, &ClipboardItemQuery::default())
                .await
                .unwrap()),
            ["both", "pin"]
        );

        assert_eq!(clear_items(&pool, true, true).await.unwrap().removed, 2);
        assert!(query_items(&pool, &ClipboardItemQuery::default())
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn cleanup_history_drops_old_items_keeping_pinned_and_favorite() {
        let pool = memory_pool().await;
        let mk = |id: &str, ts: i64, fav: bool, pin: bool| {
            let mut it = sample_item(id);
            it.created_at = DateTime::from_timestamp(ts, 0).unwrap();
            it.is_favorite = fav;
            it.is_pinned = pin;
            it
        };
        let old_plain = mk("old", 1_000, false, false);
        let old_fav = mk("old-fav", 1_000, true, false);
        let old_pin = mk("old-pin", 1_000, false, true);
        let recent = mk("recent", 9_000, false, false);
        for item in [&old_plain, &old_fav, &old_pin, &recent] {
            insert_item(&pool, item).await.unwrap();
        }

        let cutoff = DateTime::from_timestamp(5_000, 0).unwrap();
        let outcome = cleanup_history(&pool, Some(cutoff), None).await.unwrap();
        assert_eq!(outcome.removed, 1);
        assert!(outcome.image_files.is_empty());

        let all = query_items(&pool, &ClipboardItemQuery::default())
            .await
            .unwrap();
        // 置顶项恒前置；保留集合：old-pin、old-fav、recent。
        assert_eq!(ids(&all), ["old-pin", "recent", "old-fav"]);
    }

    #[tokio::test]
    async fn cleanup_history_enforces_max_count_keeping_pinned_and_favorite() {
        let pool = memory_pool().await;
        // 五条普通项 + 一条收藏 + 一条置顶；max_count = 2 时仅保留两条最新的普通项。
        for n in 0..5i64 {
            let mut it = sample_item(&format!("p{n}"));
            it.created_at = DateTime::from_timestamp(1_000 + n, 0).unwrap();
            insert_item(&pool, &it).await.unwrap();
        }
        let mut fav = sample_item("fav");
        fav.is_favorite = true;
        fav.created_at = DateTime::from_timestamp(500, 0).unwrap();
        insert_item(&pool, &fav).await.unwrap();
        let mut pin = sample_item("pin");
        pin.is_pinned = true;
        pin.created_at = DateTime::from_timestamp(400, 0).unwrap();
        insert_item(&pool, &pin).await.unwrap();

        let outcome = cleanup_history(&pool, None, Some(2)).await.unwrap();
        assert_eq!(outcome.removed, 3); // p0, p1, p2 被删
        assert!(outcome.image_files.is_empty());

        let all = query_items(&pool, &ClipboardItemQuery::default())
            .await
            .unwrap();
        assert_eq!(ids(&all), ["pin", "p4", "p3", "fav"]);
    }

    #[tokio::test]
    async fn quota_cleanup_removes_only_the_oldest_eligible_prefix() {
        let pool = memory_pool().await;
        let protected = sample_item("pending");
        upsert_item_with_sync_outbox(
            &pool,
            &protected,
            Some(ContentSyncOptions {
                manual_share: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        for (index, id) in [
            "favorite",
            "pinned",
            "old",
            "external-files-a",
            "external-files-b",
            "shared-files",
            "new",
        ]
        .into_iter()
        .enumerate()
        {
            let mut item = sample_item(id);
            item.is_favorite = id == "favorite";
            item.is_pinned = id == "pinned";
            item.created_at += chrono::Duration::seconds(index as i64 + 1);
            if matches!(id, "external-files-a" | "external-files-b" | "shared-files") {
                item.kind = ClipboardKind::Files;
            }
            if id == "shared-files" {
                item.content = format!(
                    "{}shared-files/{}/report.pdf",
                    crate::clipboard::shared_files::REFERENCE_PREFIX,
                    "a".repeat(64)
                );
                item.content_hash = content_hash(ClipboardKind::Files, &item.content);
                item.sync_key.clone_from(&item.content_hash);
            }
            insert_item(&pool, &item).await.unwrap();
        }
        assert_eq!(
            evict_oldest_history(&pool, 1, crate::clipboard::shared_files::REFERENCE_PREFIX)
                .await
                .unwrap()
                .removed,
            1
        );
        assert!(find_item_by_id(&pool, "old").await.unwrap().is_none());
        assert_eq!(
            evict_oldest_history(
                &pool,
                1024,
                crate::clipboard::shared_files::REFERENCE_PREFIX,
            )
            .await
            .unwrap()
            .removed,
            3
        );
        for id in ["external-files-a", "external-files-b", "shared-files"] {
            assert!(find_item_by_id(&pool, id).await.unwrap().is_none());
        }
        assert!(find_item_by_id(&pool, "new").await.unwrap().is_some());
        assert_eq!(
            evict_oldest_history(
                &pool,
                1024,
                crate::clipboard::shared_files::REFERENCE_PREFIX,
            )
            .await
            .unwrap()
            .removed,
            1
        );
        assert_eq!(
            evict_oldest_history(
                &pool,
                1024,
                crate::clipboard::shared_files::REFERENCE_PREFIX,
            )
            .await
            .unwrap()
            .removed,
            0
        );
        for id in ["pending", "pinned", "favorite"] {
            assert!(find_item_by_id(&pool, id).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn quota_cleanup_uses_last_reuse_time() {
        let pool = memory_pool().await;
        let mut recently_reused = sample_item("recently-reused");
        recently_reused.updated_at += chrono::Duration::seconds(20);
        insert_item(&pool, &recently_reused).await.unwrap();

        let mut stale = sample_item("stale");
        stale.created_at += chrono::Duration::seconds(10);
        stale.updated_at += chrono::Duration::seconds(10);
        insert_item(&pool, &stale).await.unwrap();

        let outcome =
            evict_oldest_history(&pool, 1, crate::clipboard::shared_files::REFERENCE_PREFIX)
                .await
                .unwrap();

        assert_eq!(outcome.removed, 1);
        assert!(find_item_by_id(&pool, "stale").await.unwrap().is_none());
        assert!(find_item_by_id(&pool, "recently-reused")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn quota_cleanup_keeps_explicit_delivery_until_success() {
        use ecopaste_sync_protocol::{EncryptedEvent, SyncEventKind};

        let pool = memory_pool().await;
        let item = sample_item("manual-cloud");
        insert_item(&pool, &item).await.unwrap();
        let event_id = "manual-cloud-event";
        crate::sync::repository::insert_event(
            &pool,
            &EncryptedEvent {
                event_id: event_id.into(),
                origin_device_id: "local-device".into(),
                origin_sequence: 1,
                created_at_ms: 1,
                nonce: vec![1; 24],
                ciphertext: vec![2; 32],
                kind: SyncEventKind::Content,
                blob_ids: Vec::new(),
                optional_blob_ids: Vec::new(),
                retention_key: None,
            },
            true,
            &[],
        )
        .await
        .unwrap();
        sqlx::query("UPDATE sync_events SET delivery_route = 'cloud' WHERE event_id = ?")
            .bind(event_id)
            .execute(&pool)
            .await
            .unwrap();
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO sync_item_events (item_id, event_id, direction, created_at, updated_at) \
             VALUES (?, ?, 'local', ?, ?)",
        )
        .bind(&item.id)
        .bind(event_id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            evict_oldest_history(&pool, 1, crate::clipboard::shared_files::REFERENCE_PREFIX)
                .await
                .unwrap()
                .removed,
            0
        );

        crate::sync::repository::mark_delivery_error(
            &pool,
            "cloud",
            &[event_id.to_owned()],
            "offline",
        )
        .await
        .unwrap();
        assert_eq!(
            evict_oldest_history(&pool, 1, crate::clipboard::shared_files::REFERENCE_PREFIX)
                .await
                .unwrap()
                .removed,
            0
        );

        crate::sync::repository::mark_delivered(&pool, "cloud", &[event_id.to_owned()])
            .await
            .unwrap();
        assert_eq!(
            evict_oldest_history(&pool, 1, crate::clipboard::shared_files::REFERENCE_PREFIX)
                .await
                .unwrap()
                .removed,
            1
        );
    }

    #[tokio::test]
    async fn cleanup_history_no_op_when_both_disabled() {
        let pool = memory_pool().await;
        insert_item(&pool, &sample_item("a")).await.unwrap();
        assert_eq!(cleanup_history(&pool, None, None).await.unwrap().removed, 0);
        assert_eq!(
            cleanup_history(&pool, None, Some(0)).await.unwrap().removed,
            0
        );
        let all = query_items(&pool, &ClipboardItemQuery::default())
            .await
            .unwrap();
        assert_eq!(ids(&all), ["a"]);
    }

    #[tokio::test]
    async fn cleanup_history_collects_deleted_image_file_names() {
        let pool = memory_pool().await;
        // 一条旧图片 + 一条旧文本，都早于 cutoff；只有图片应进入 image_files。
        let mut img = sample_item("img");
        img.kind = ClipboardKind::Image;
        img.content = "cafe1234.png".to_owned();
        img.content_hash = content_hash(ClipboardKind::Image, "cafe1234.png");
        img.created_at = DateTime::from_timestamp(1_000, 0).unwrap();
        let mut txt = sample_item("txt");
        txt.created_at = DateTime::from_timestamp(1_000, 0).unwrap();
        insert_item(&pool, &img).await.unwrap();
        insert_item(&pool, &txt).await.unwrap();

        let cutoff = DateTime::from_timestamp(5_000, 0).unwrap();
        let outcome = cleanup_history(&pool, Some(cutoff), None).await.unwrap();
        assert_eq!(outcome.removed, 2);
        assert_eq!(outcome.image_files, vec!["cafe1234.png".to_owned()]);
    }

    #[test]
    fn keyword_filter_classifies_by_char_length() {
        assert_eq!(KeywordFilter::from_keyword(None), KeywordFilter::None);
        assert_eq!(
            KeywordFilter::from_keyword(Some("   ")),
            KeywordFilter::None
        );

        // 1–2 字符走 LIKE（含 CJK）。
        assert_eq!(
            KeywordFilter::from_keyword(Some("a")),
            KeywordFilter::Like("a".to_owned())
        );
        assert_eq!(
            KeywordFilter::from_keyword(Some("中文")),
            KeywordFilter::Like("中文".to_owned())
        );

        // ≥3 字符走 FTS。
        assert_eq!(
            KeywordFilter::from_keyword(Some("foo")),
            KeywordFilter::Fts("\"foo\"*".to_owned())
        );
        assert_eq!(
            KeywordFilter::from_keyword(Some("foo bar")),
            KeywordFilter::Fts("\"foo\"* \"bar\"*".to_owned())
        );
        assert_eq!(
            KeywordFilter::from_keyword(Some("a\"b")),
            KeywordFilter::Fts("\"a\"\"b\"*".to_owned())
        );
    }

    #[test]
    fn keyword_filter_drops_multi_token_short_words_to_like() {
        // 整串 ≥3 字符，但每个空白分词都 <3 字符：trigram 对 <3 字符 token 永远 0 命中，
        // 且 FTS5 多 token 默认 AND，整条表达式会被任一短词拖成 0 结果 → 必须降级 LIKE。
        assert_eq!(
            KeywordFilter::from_keyword(Some("a b")),
            KeywordFilter::Like("a b".to_owned())
        );
        assert_eq!(
            KeywordFilter::from_keyword(Some("ab cd")),
            KeywordFilter::Like("ab cd".to_owned())
        );
        // 混合长度：只要有一个分词 <3 字符，整条 FTS 表达式就会被拖成 0 命中 → LIKE。
        assert_eq!(
            KeywordFilter::from_keyword(Some("foo b")),
            KeywordFilter::Like("foo b".to_owned())
        );
        // 所有分词均 ≥3 字符仍走 FTS（回归保障，语义不变）。
        assert_eq!(
            KeywordFilter::from_keyword(Some("foo bar baz")),
            KeywordFilter::Fts("\"foo\"* \"bar\"* \"baz\"*".to_owned())
        );
    }

    #[tokio::test]
    async fn multi_token_short_keyword_falls_back_to_like() {
        let pool = memory_pool().await;
        let mut item = sample_item("short");
        item.content = "ab cd ef".to_owned();
        item.content_hash = content_hash(ClipboardKind::Text, &item.content);
        item.search_text = Some("ab cd ef".to_owned());
        insert_item(&pool, &item).await.unwrap();

        let q = ClipboardItemQuery {
            keyword: Some("ab cd".to_owned()),
            ..Default::default()
        };

        // 旧逻辑按整串字符数（5 ≥ 3）判走 FTS：`"ab"* "cd"*`，两个 token 均 <3 字符，
        // trigram 返回 0 行 → 搜不到刚插入的记录。现按分词长度降级 LIKE `%ab cd%`，命中。
        let found = query_items(&pool, &q).await.unwrap();
        assert_eq!(ids(&found), ["short"]);
    }

    #[test]
    fn escape_like_escapes_special_chars() {
        assert_eq!(escape_like("100%"), "100\\%");
        assert_eq!(escape_like("a_b"), "a\\_b");
        assert_eq!(escape_like("c:\\path"), "c:\\\\path");
    }
}
