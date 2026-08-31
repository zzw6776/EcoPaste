//! 来源应用仓储：以 macOS bundle id / Windows exe 路径作主键去重，
//! 同 id 二次入库走「保留 created_at、刷新 name/icon_file/updated_at」语义——
//! 应用改名或换图标时无需重建条目，引用方（`clipboard_items.source_app_id`）天然跟着更新。

use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

use crate::core::Result;
use crate::db::models::ClipboardApp;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SourceAppAsset {
    pub app_id: String,
    pub icon_hash: String,
    pub blob_id: String,
    pub original_size: u64,
    pub encrypted_size: u64,
    pub is_attached: bool,
}

const SELECT_APP: &str = "SELECT id, name, icon_file, icon_hash, accent_start, accent_end, platform, created_at, updated_at \
     FROM clipboard_apps";

/// 按 id 查单条记录。
#[allow(dead_code)]
pub async fn find_app_by_id(pool: &SqlitePool, id: &str) -> Result<Option<ClipboardApp>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_APP);
    qb.push(" WHERE id = ").push_bind(id.to_owned());
    let row = qb
        .build_query_as::<ClipboardApp>()
        .fetch_optional(pool)
        .await
        .context("failed to find clipboard app by id")?;
    Ok(row)
}

/// 按 id 列表批量取——给前端渲染卡片时一次性补齐 icon/name 用。
/// 空列表直接返回空结果，避免拼出 `IN ()` 这种非法 SQL。
pub async fn list_apps_by_ids(pool: &SqlitePool, ids: &[String]) -> Result<Vec<ClipboardApp>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_APP);
    qb.push(" WHERE id IN (");
    let mut sep = qb.separated(", ");
    for id in ids {
        sep.push_bind(id.clone());
    }
    qb.push(")");
    let rows = qb
        .build_query_as::<ClipboardApp>()
        .fetch_all(pool)
        .await
        .context("failed to list clipboard apps by ids")?;
    Ok(rows)
}

/// 列出全部已知应用（监听过程中捕获、手动添加或默认忽略物化的应用）。
/// 名称按大小写不敏感升序，给前端过滤选择 UI 一个稳定顺序。
pub async fn list_all_apps(pool: &SqlitePool) -> Result<Vec<ClipboardApp>> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(SELECT_APP);
    qb.push(" ORDER BY name COLLATE NOCASE ASC, id ASC");
    let rows = qb
        .build_query_as::<ClipboardApp>()
        .fetch_all(pool)
        .await
        .context("failed to list all clipboard apps")?;
    Ok(rows)
}

/// 删除未被历史记录引用的来源应用，返回实际删除的应用 id。
pub async fn delete_unreferenced_apps(pool: &SqlitePool, ids: &[String]) -> Result<Vec<String>> {
    let mut deleted = Vec::new();

    for id in ids {
        let result = sqlx::query(
            "DELETE FROM clipboard_apps \
             WHERE id = ? \
               AND NOT EXISTS (SELECT 1 FROM clipboard_items WHERE source_app_id = ? LIMIT 1)",
        )
        .bind(id)
        .bind(id)
        .execute(pool)
        .await
        .context("failed to delete unreferenced clipboard app")?;

        if result.rows_affected() > 0 {
            deleted.push(id.clone());
        }
    }

    Ok(deleted)
}

/// upsert：id 已存在则只刷新 name / icon_file / updated_at；不存在则全量插入。
/// 显式分两路而非依赖 `INSERT OR REPLACE`：后者会重置 created_at 与（潜在的）外键级联。
pub async fn upsert_app(pool: &SqlitePool, app: &ClipboardApp) -> Result<()> {
    let now = Utc::now();
    let updated = sqlx::query(
        "UPDATE clipboard_apps SET name = ?, icon_file = COALESCE(?, icon_file), icon_hash = COALESCE(?, icon_hash), accent_start = COALESCE(?, accent_start), accent_end = COALESCE(?, accent_end), updated_at = ? WHERE id = ?",
    )
    .bind(app.name.as_str())
    .bind(app.icon_file.as_deref())
    .bind(app.icon_hash.as_deref())
    .bind(app.accent_start.as_deref())
    .bind(app.accent_end.as_deref())
    .bind(now)
    .bind(app.id.as_str())
    .execute(pool)
    .await
    .context("failed to update clipboard app")?;

    if updated.rows_affected() > 0 {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO clipboard_apps (id, name, icon_file, icon_hash, accent_start, accent_end, platform, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(app.id.as_str())
    .bind(app.name.as_str())
    .bind(app.icon_file.as_deref())
    .bind(app.icon_hash.as_deref())
    .bind(app.accent_start.as_deref())
    .bind(app.accent_end.as_deref())
    .bind(app.platform)
    .bind(app.created_at)
    .bind(app.updated_at)
    .execute(pool)
    .await
    .context("failed to insert clipboard app")?;
    Ok(())
}

/// Resolves a space-scoped anonymous source key to one local application row.
pub async fn resolve_synced_app(
    pool: &SqlitePool,
    group_id: &str,
    source_key: &str,
    mut app: ClipboardApp,
    icon: Option<(&str, &str, u64, u64)>,
    source_updated_at: DateTime<Utc>,
    source_revision: &str,
) -> Result<ClipboardApp> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin synchronized source app resolution")?;
    let existing: Option<(String, DateTime<Utc>, String)> = sqlx::query_as(
        "SELECT app_id, source_updated_at, source_revision FROM source_app_sync_aliases WHERE group_id = ? AND source_key = ?",
    )
    .bind(group_id)
    .bind(source_key)
    .fetch_optional(&mut *transaction)
    .await
    .context("find synchronized source app alias")?;
    let now = Utc::now();
    let (icon_hash, blob_id, original_size, encrypted_size) = icon
        .map(|(hash, blob, original, encrypted)| {
            (Some(hash), Some(blob), Some(original), Some(encrypted))
        })
        .unwrap_or((None, None, None, None));
    let original_size = original_size
        .map(i64::try_from)
        .transpose()
        .context("source app icon is too large")?;
    let encrypted_size = encrypted_size
        .map(i64::try_from)
        .transpose()
        .context("encrypted source app icon is too large")?;

    match existing {
        Some((app_id, current_updated_at, current_revision)) => {
            app.id = app_id;
            let incoming_is_newer = source_updated_at > current_updated_at
                || (source_updated_at == current_updated_at
                    && source_revision > current_revision.as_str());
            if incoming_is_newer {
                sqlx::query(
                    "UPDATE clipboard_apps SET name = ?, icon_file = ?, icon_hash = ?, accent_start = ?, accent_end = ?, platform = ?, updated_at = ? WHERE id = ?",
                )
                .bind(app.name.as_str())
                .bind(app.icon_file.as_deref())
                .bind(app.icon_hash.as_deref())
                .bind(app.accent_start.as_deref())
                .bind(app.accent_end.as_deref())
                .bind(app.platform)
                .bind(now)
                .bind(app.id.as_str())
                .execute(&mut *transaction)
                .await
                .context("update synchronized source app")?;
                sqlx::query(
                    r#"
                    UPDATE source_app_sync_aliases
                    SET icon_hash = ?, blob_id = ?, icon_original_size = ?,
                        icon_encrypted_size = ?, source_updated_at = ?,
                        source_revision = ?, updated_at = ?
                    WHERE group_id = ? AND source_key = ?
                    "#,
                )
                .bind(icon_hash)
                .bind(blob_id)
                .bind(original_size)
                .bind(encrypted_size)
                .bind(source_updated_at)
                .bind(source_revision)
                .bind(now)
                .bind(group_id)
                .bind(source_key)
                .execute(&mut *transaction)
                .await
                .context("update synchronized source app alias")?;
            }
        }
        None => {
            app.id = format!("sync-{}", uuid::Uuid::new_v4().simple());
            sqlx::query(
                "INSERT INTO clipboard_apps (id, name, icon_file, icon_hash, accent_start, accent_end, platform, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(app.id.as_str())
            .bind(app.name.as_str())
            .bind(app.icon_file.as_deref())
            .bind(app.icon_hash.as_deref())
            .bind(app.accent_start.as_deref())
            .bind(app.accent_end.as_deref())
            .bind(app.platform)
            .bind(app.created_at)
            .bind(app.updated_at)
            .execute(&mut *transaction)
            .await
            .context("insert synchronized source app")?;
            sqlx::query(
                r#"
                INSERT INTO source_app_sync_aliases (
                    group_id, source_key, app_id, icon_hash, blob_id,
                    icon_original_size, icon_encrypted_size, source_updated_at,
                    source_revision, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(group_id)
            .bind(source_key)
            .bind(&app.id)
            .bind(icon_hash)
            .bind(blob_id)
            .bind(original_size)
            .bind(encrypted_size)
            .bind(source_updated_at)
            .bind(source_revision)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .context("insert synchronized source app alias")?;
        }
    }
    transaction
        .commit()
        .await
        .context("commit synchronized source app resolution")?;

    Ok(find_app_by_id(pool, &app.id)
        .await?
        .context("resolved synchronized source app is missing")?)
}

/// Lists all source icon assets and whether the application row already references them.
pub async fn source_app_assets(pool: &SqlitePool, group_id: &str) -> Result<Vec<SourceAppAsset>> {
    let rows = sqlx::query(
        r#"
        SELECT aliases.app_id, aliases.icon_hash, aliases.blob_id,
               aliases.icon_original_size, aliases.icon_encrypted_size,
               CASE WHEN apps.icon_hash = aliases.icon_hash
                          AND apps.icon_file = aliases.icon_hash || '.png'
                          AND apps.accent_start IS NOT NULL
                          AND apps.accent_end IS NOT NULL
                    THEN 1 ELSE 0 END AS is_attached
        FROM source_app_sync_aliases aliases
        LEFT JOIN clipboard_apps apps ON apps.id = aliases.app_id
        WHERE aliases.group_id = ?
          AND aliases.icon_hash IS NOT NULL
          AND aliases.blob_id IS NOT NULL
          AND aliases.icon_original_size IS NOT NULL
          AND aliases.icon_encrypted_size IS NOT NULL
        "#,
    )
    .bind(group_id)
    .fetch_all(pool)
    .await
    .context("list pending synchronized source app assets")?;

    rows.into_iter()
        .map(|row| {
            use sqlx::Row;
            let original_size: i64 = row
                .try_get("icon_original_size")
                .context("read source app icon size")?;
            let encrypted_size: i64 = row
                .try_get("icon_encrypted_size")
                .context("read encrypted source app icon size")?;
            Ok(SourceAppAsset {
                app_id: row.try_get("app_id").context("read source app id")?,
                icon_hash: row
                    .try_get("icon_hash")
                    .context("read source app icon hash")?,
                blob_id: row
                    .try_get("blob_id")
                    .context("read source app icon blob id")?,
                original_size: u64::try_from(original_size)
                    .context("negative source app icon size")?,
                encrypted_size: u64::try_from(encrypted_size)
                    .context("negative encrypted source app icon size")?,
                is_attached: row
                    .try_get::<i64, _>("is_attached")
                    .context("read source app attachment state")?
                    != 0,
            })
        })
        .collect()
}

/// Attaches one verified normalized icon to its local source application.
pub async fn update_synced_app_icon(
    pool: &SqlitePool,
    app_id: &str,
    icon_file: &str,
    icon_hash: &str,
    accent_start: &str,
    accent_end: &str,
) -> Result<bool> {
    let updated = sqlx::query(
        r#"
        UPDATE clipboard_apps
        SET icon_file = ?, icon_hash = ?, accent_start = ?, accent_end = ?, updated_at = ?
        WHERE id = ?
          AND EXISTS (
              SELECT 1 FROM source_app_sync_aliases
              WHERE app_id = ? AND icon_hash = ?
          )
        "#,
    )
    .bind(icon_file)
    .bind(icon_hash)
    .bind(accent_start)
    .bind(accent_end)
    .bind(Utc::now())
    .bind(app_id)
    .bind(app_id)
    .bind(icon_hash)
    .execute(pool)
    .await
    .context("attach synchronized source app icon")?;
    Ok(updated.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::Platform;
    use crate::db::test_support::memory_pool;
    use chrono::DateTime;

    fn sample_app(id: &str) -> ClipboardApp {
        let ts = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        ClipboardApp {
            id: id.to_owned(),
            name: format!("name-{id}"),
            icon_file: Some(format!("{id}.png")),
            icon_hash: None,
            accent_start: None,
            accent_end: None,
            platform: Platform::Macos,
            created_at: ts,
            updated_at: ts,
        }
    }

    #[tokio::test]
    async fn insert_then_find_roundtrip() {
        let pool = memory_pool().await;
        let app = sample_app("com.example.foo");
        upsert_app(&pool, &app).await.unwrap();

        let got = find_app_by_id(&pool, &app.id).await.unwrap().unwrap();
        assert_eq!(got.id, app.id);
        assert_eq!(got.name, "name-com.example.foo");
        assert_eq!(got.icon_file.as_deref(), Some("com.example.foo.png"));
    }

    #[tokio::test]
    async fn upsert_refreshes_name_and_icon_but_keeps_created_at() {
        let pool = memory_pool().await;
        let mut app = sample_app("com.example.bar");
        upsert_app(&pool, &app).await.unwrap();
        let first = find_app_by_id(&pool, &app.id).await.unwrap().unwrap();

        // 同 id 再写：改名、换图标。
        app.name = "renamed".to_owned();
        app.icon_file = Some("other.png".to_owned());
        upsert_app(&pool, &app).await.unwrap();

        let after = find_app_by_id(&pool, &app.id).await.unwrap().unwrap();
        assert_eq!(after.name, "renamed");
        assert_eq!(after.icon_file.as_deref(), Some("other.png"));
        // created_at 不变；updated_at 刷新（>= 原值，避免时间精度抖动）。
        assert_eq!(after.created_at, first.created_at);
        assert!(after.updated_at >= first.updated_at);
    }

    #[tokio::test]
    async fn find_missing_returns_none() {
        let pool = memory_pool().await;
        assert!(find_app_by_id(&pool, "nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn synchronized_alias_is_stable_and_tracks_pending_icon() {
        let pool = memory_pool().await;
        let mut app = sample_app("");
        app.icon_file = None;
        app.icon_hash = None;
        let icon_hash = "1".repeat(64);
        let blob_id = "2".repeat(64);

        let first = resolve_synced_app(
            &pool,
            "group",
            &"3".repeat(64),
            app.clone(),
            Some((&icon_hash, &blob_id, 128, 192)),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            "revision-a",
        )
        .await
        .unwrap();
        app.name = "renamed".into();
        let repeated = resolve_synced_app(
            &pool,
            "group",
            &"3".repeat(64),
            app,
            Some((&icon_hash, &blob_id, 128, 192)),
            DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
            "revision-b",
        )
        .await
        .unwrap();

        assert_eq!(first.id, repeated.id);
        assert_eq!(repeated.name, "renamed");

        let mut tie_winner = sample_app("");
        tie_winner.name = "tie-winner".into();
        let tie_result = resolve_synced_app(
            &pool,
            "group",
            &"3".repeat(64),
            tie_winner,
            Some((&icon_hash, &blob_id, 128, 192)),
            DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
            "revision-c",
        )
        .await
        .unwrap();
        assert_eq!(tie_result.name, "tie-winner");

        let mut stale = sample_app("");
        stale.name = "stale".into();
        let stale_result = resolve_synced_app(
            &pool,
            "group",
            &"3".repeat(64),
            stale,
            Some((&"4".repeat(64), &"5".repeat(64), 128, 192)),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            "revision-z",
        )
        .await
        .unwrap();
        assert_eq!(stale_result.name, "tie-winner");

        let assets = source_app_assets(&pool, "group").await.unwrap();
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].app_id, first.id);
        assert_eq!(assets[0].icon_hash, icon_hash);
        assert!(!assets[0].is_attached);

        assert!(!update_synced_app_icon(
            &pool,
            &first.id,
            "stale.png",
            &"4".repeat(64),
            "#000000",
            "#000000",
        )
        .await
        .unwrap());
        assert!(update_synced_app_icon(
            &pool,
            &first.id,
            &format!("{icon_hash}.png"),
            &icon_hash,
            "#112233",
            "#445566",
        )
        .await
        .unwrap());
        assert!(source_app_assets(&pool, "group").await.unwrap()[0].is_attached);

        let mut without_icon = sample_app("");
        without_icon.name = "without-icon".into();
        without_icon.icon_file = None;
        without_icon.icon_hash = None;
        let cleared = resolve_synced_app(
            &pool,
            "group",
            &"3".repeat(64),
            without_icon,
            None,
            DateTime::from_timestamp(1_700_000_002, 0).unwrap(),
            "revision-d",
        )
        .await
        .unwrap();
        assert_eq!(cleared.name, "without-icon");
        assert_eq!(cleared.icon_file, None);
        assert_eq!(cleared.icon_hash, None);
    }
}
