//! Local history retention and quota checks, serialized with sync and storage replacement.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use ecopaste_sync_protocol::quota_low_watermark;
use serde_json::json;
use sqlx::SqlitePool;
use tauri::{AppHandle, Emitter, Listener, Manager};
use tokio::sync::Notify;
use tokio::time::Instant;

use super::shared_files;
use super::storage::ImageStore;
use super::watcher::CLIPBOARD_UPDATED_EVENT;
use crate::core::paths;
use crate::db::items::{cleanup_history, evict_oldest_history, CleanupOutcome};
use crate::settings::{Retention, RetentionUnit, SettingsStore};
use crate::sync::SyncManager;

const STORAGE_USAGE_UPDATED_EVENT: &str = "storage://usage-updated";
const CAPACITY_RECHECK: Duration = Duration::from_secs(300);
const WRITE_COALESCE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupTrigger {
    Scheduled,
    Capacity,
    CloudBootstrap,
}

/// Coalesces clipboard writes and checks quiet growth (e.g. sync blobs) only with an active limit.
/// Capacity checks run independently of the time/count cleanup interval.
pub fn spawn(app: AppHandle) {
    let writes = Arc::new(Notify::new());
    let wake = writes.clone();
    app.listen(CLIPBOARD_UPDATED_EVENT, move |_| wake.notify_one());

    tauri::async_runtime::spawn(async move {
        let store = app.state::<SettingsStore>();
        let manager = app.state::<Arc<SyncManager>>();
        let mut changes = store.subscribe_changes();
        let mut limit = store.snapshot().clipboard.history.max_storage_bytes;
        manager
            .run_local_retention_cleanup(CleanupTrigger::Scheduled)
            .await;
        let mut last_scheduled = Instant::now();
        let mut last_check = last_scheduled;
        loop {
            let history = store.snapshot().clipboard.history;
            let interval = Duration::from_secs(u64::from(history.cleanup_interval_hours) * 3600);
            let limit_changed = history.max_storage_bytes != limit;
            limit = history.max_storage_bytes;

            if !(limit_changed && limit > 0) {
                tokio::select! {
                    changed = changes.changed() => {
                        if changed.is_err() { return; }
                        continue;
                    }
                    _ = tokio::time::sleep_until(last_scheduled + interval), if !interval.is_zero() => {
                        manager.run_local_retention_cleanup(CleanupTrigger::Scheduled).await;
                        last_scheduled = Instant::now();
                        last_check = last_scheduled;
                        continue;
                    }
                    _ = writes.notified(), if limit > 0 => {
                        tokio::time::sleep(WRITE_COALESCE).await;
                    }
                    _ = tokio::time::sleep_until(last_check + CAPACITY_RECHECK), if limit > 0 => {}
                }
            }

            match storage_bytes(&app).await {
                Ok(bytes) if bytes > limit => {
                    manager
                        .run_local_retention_cleanup(CleanupTrigger::Capacity)
                        .await;
                }
                Ok(bytes) => {
                    publish_usage(&app, bytes);
                }
                Err(error) => log::warn!("measure local data directory failed: {error:#}"),
            }
            last_check = Instant::now();
        }
    });
}

async fn storage_bytes(app: &AppHandle) -> Result<u64> {
    let root = paths::app_data_dir(app)?;
    Ok(tauri::async_runtime::spawn_blocking(move || paths::dir_size(&root)).await??)
}

fn publish_usage(app: &AppHandle, bytes: u64) {
    if let Err(error) = app.emit(STORAGE_USAGE_UPDATED_EVENT, bytes) {
        log::warn!("publish local storage usage failed: {error}");
    }
}

/// Runs under the transfer and file locks so an import or a pending cloud protection cannot
/// race eviction. Age/count policies retain their own schedule.
pub(crate) async fn apply_retention(
    app: &AppHandle,
    manager: &SyncManager,
    pool: &SqlitePool,
    trigger: CleanupTrigger,
) -> Result<()> {
    if trigger != CleanupTrigger::CloudBootstrap
        && crate::sync::repository::cloud_bootstrap_in_progress(pool).await?
    {
        return Ok(());
    }
    let settings = app.state::<SettingsStore>();
    let history = settings.snapshot().clipboard.history;
    let limit = history.max_storage_bytes;
    let resources = paths::resources_dir(app)?;
    let quota_triggered =
        limit > 0 && (trigger == CleanupTrigger::Capacity || storage_bytes(app).await? > limit);
    if trigger != CleanupTrigger::Capacity {
        let cutoff = retention_cutoff(&history.retention, Utc::now());
        let max_count = (history.max_count > 0).then_some(history.max_count);
        if cutoff.is_some() || max_count.is_some() {
            let outcome = cleanup_history(pool, cutoff, max_count).await?;
            finish_deletion(app, pool, outcome).await?;
        }
    }
    // Reclaim orphans on every run so a transient file lock is retried at startup or the next
    // scheduled pass even when all retention and capacity policies are disabled.
    manager.reclaim_released_sync_content(pool).await?;
    shared_files::cleanup_resources_locked(pool, resources.clone()).await?;
    super::cache::reclaim_unreferenced_images(pool, &resources).await?;
    let measured = storage_bytes(app).await?;
    if limit == 0 {
        publish_usage(app, measured);
        return Ok(());
    }

    let target = quota_low_watermark(limit);
    if !should_evict_for_quota(quota_triggered, measured, target) {
        publish_usage(app, measured);
        return Ok(());
    }
    let root = paths::app_data_dir(app)?;
    let mut used = compacted_usage(pool, &root, target, false).await?;
    while used > target && settings.snapshot().clipboard.history.max_storage_bytes == limit {
        let outcome =
            evict_oldest_history(pool, used - target, shared_files::REFERENCE_PREFIX).await?;
        let exhausted = outcome.removed == 0;
        if !exhausted {
            finish_deletion(app, pool, outcome).await?;
            shared_files::cleanup_resources_locked(pool, resources.clone()).await?;
            manager.reclaim_released_sync_content(pool).await?;
        }
        used = compacted_usage(pool, &root, target, exhausted).await?;
        if exhausted {
            break;
        }
    }
    publish_usage(app, storage_bytes(app).await?);
    Ok(())
}

fn should_evict_for_quota(quota_triggered: bool, measured: u64, target: u64) -> bool {
    quota_triggered && measured > target
}

/// Checkpoints the WAL and discounts SQLite pages freed by earlier batches. Rewrite the file
/// once those pages suffice (or no more rows are eligible), then verify actual directory bytes.
async fn compacted_usage(
    pool: &SqlitePool,
    root: &Path,
    target: u64,
    exhausted: bool,
) -> Result<u64> {
    checkpoint(pool).await?;
    let measure = || {
        let root = root.to_owned();
        tauri::async_runtime::spawn_blocking(move || paths::dir_size(&root))
    };
    let physical = measure().await??;
    if physical <= target {
        return Ok(physical);
    }
    let free: i64 = sqlx::query_scalar(
        "SELECT page_size * freelist_count FROM pragma_page_size(), pragma_freelist_count()",
    )
    .fetch_one(pool)
    .await?;
    let used = physical.saturating_sub(u64::try_from(free)?);
    if free == 0 || (used > target && !exhausted) {
        return Ok(used);
    }
    sqlx::query("VACUUM")
        .execute(pool)
        .await
        .context("compact clipboard database")?;
    checkpoint(pool).await?;
    Ok(measure().await??)
}

/// A busy checkpoint leaves transient WAL bytes on disk; defer eviction instead of counting them.
async fn checkpoint(pool: &SqlitePool) -> Result<()> {
    let (busy, _, _): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
        .fetch_one(pool)
        .await?;
    anyhow::ensure!(busy == 0, "clipboard database checkpoint busy");
    Ok(())
}

/// Emits committed deletions and reclaims images that no surviving row references.
async fn finish_deletion(
    app: &AppHandle,
    pool: &SqlitePool,
    outcome: CleanupOutcome,
) -> Result<()> {
    if outcome.removed == 0 {
        return Ok(());
    }
    if let Err(error) = app.emit(
        CLIPBOARD_UPDATED_EVENT,
        json!({ "cleanup": outcome.removed }),
    ) {
        log::warn!("emit history cleanup failed: {error}");
    }
    let images = app.state::<ImageStore>();
    super::cache::reclaim_deleted_images(pool, images.inner(), &outcome.image_files).await?;
    Ok(())
}

/// `Retention` → 绝对截止时间。`Forever` 或 `value == 0` 表示禁用。
/// 月份近似按 30 天处理（与前端展示口径一致，不引日历库）。
fn retention_cutoff(r: &Retention, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if r.value == 0 {
        return None;
    }
    let dur = match r.unit {
        RetentionUnit::Forever => return None,
        RetentionUnit::Hours => ChronoDuration::hours(r.value as i64),
        RetentionUnit::Days => ChronoDuration::days(r.value as i64),
        RetentionUnit::Weeks => ChronoDuration::weeks(r.value as i64),
        RetentionUnit::Months => ChronoDuration::days((r.value as i64) * 30),
    };
    Some(now - dur)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[tokio::test]
    async fn quota_cleanup_recovers_wal_and_free_pages_before_evicting_live_data() {
        use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
        let dir = tempfile::tempdir().unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(dir.path().join("clipboard.db"))
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal),
            )
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        checkpoint(&pool).await.unwrap();
        let target = paths::dir_size(dir.path()).unwrap() + 128 * 1024;
        sqlx::query("INSERT INTO clipboard_items (id, kind, content, content_hash, platform, created_at, updated_at, search_text)
            VALUES ('old','text',zeroblob(1000000),'old','macos','0','0',NULL),
                   ('keep','text','needle','keep','macos','1','1','needle')")
            .execute(&pool).await.unwrap();
        checkpoint(&pool).await.unwrap();
        sqlx::query("DELETE FROM clipboard_items WHERE id = 'old'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(dir.path().join("clipboard.db").metadata().unwrap().len() > target);
        let used = compacted_usage(&pool, dir.path(), target, false)
            .await
            .unwrap();
        assert!(used <= target);
        assert_eq!(paths::dir_size(dir.path()).unwrap(), used);
        let id: String = sqlx::query_scalar(
            "SELECT id FROM clipboard_items WHERE rowid IN (
            SELECT rowid FROM clipboard_items_fts WHERE clipboard_items_fts MATCH 'needle')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(id, "keep");
    }

    #[test]
    fn retention_cutoff_returns_none_when_disabled() {
        assert!(retention_cutoff(
            &Retention {
                value: 0,
                unit: RetentionUnit::Days
            },
            now()
        )
        .is_none());
        assert!(retention_cutoff(
            &Retention {
                value: 7,
                unit: RetentionUnit::Forever
            },
            now()
        )
        .is_none());
    }

    #[test]
    fn retention_cutoff_subtracts_by_unit() {
        let n = now();
        assert_eq!(
            retention_cutoff(
                &Retention {
                    value: 2,
                    unit: RetentionUnit::Hours
                },
                n
            ),
            Some(n - ChronoDuration::hours(2))
        );
        assert_eq!(
            retention_cutoff(
                &Retention {
                    value: 3,
                    unit: RetentionUnit::Days
                },
                n
            ),
            Some(n - ChronoDuration::days(3))
        );
        assert_eq!(
            retention_cutoff(
                &Retention {
                    value: 1,
                    unit: RetentionUnit::Weeks
                },
                n
            ),
            Some(n - ChronoDuration::weeks(1))
        );
        assert_eq!(
            retention_cutoff(
                &Retention {
                    value: 1,
                    unit: RetentionUnit::Months
                },
                n
            ),
            Some(n - ChronoDuration::days(30))
        );
    }

    #[test]
    fn over_limit_episode_continues_below_limit_until_low_watermark() {
        let limit = (0.1_f64 * 1024_f64.powi(3)).round() as u64;
        let measured_after_orphan_cleanup = 102 * 1024 * 1024;
        let target = quota_low_watermark(limit);

        assert!(measured_after_orphan_cleanup < limit);
        assert!(measured_after_orphan_cleanup > target);
        assert!(should_evict_for_quota(
            true,
            measured_after_orphan_cleanup,
            target
        ));
        assert!(!should_evict_for_quota(
            false,
            measured_after_orphan_cleanup,
            target
        ));
    }
}
