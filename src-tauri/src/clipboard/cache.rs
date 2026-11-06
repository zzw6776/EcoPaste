use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::Result;
use sqlx::SqlitePool;
use walkdir::WalkDir;

use super::shared_files::CleanupStats;
use super::storage::ImageStore;

/// Removes only deleted image payloads with no surviving row while the caller holds the file lock.
pub(crate) async fn reclaim_deleted_images(
    pool: &SqlitePool,
    store: &ImageStore,
    file_names: &[String],
) -> Result<()> {
    for file_name in file_names {
        let referenced: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM clipboard_items WHERE kind = 'image' AND content = ?)",
        )
        .bind(file_name)
        .fetch_one(pool)
        .await?;
        if referenced {
            continue;
        }
        if let Err(error) = store.remove(file_name) {
            log::warn!("remove unreferenced image {file_name} failed: {error}");
        }
    }
    Ok(())
}

/// The caller holds the file lock so an in-flight capture cannot lose its original before commit.
pub(crate) async fn reclaim_unreferenced_images(
    pool: &SqlitePool,
    resources: &Path,
) -> Result<CleanupStats> {
    let images = resources.join("clipboard-images");
    let exists = match images.try_exists() {
        Ok(exists) => exists,
        Err(error) => {
            log::warn!("inspect clipboard image cache {images:?} failed: {error}");
            return Ok(CleanupStats::default());
        }
    };
    if !exists {
        return Ok(CleanupStats::default());
    }
    let names: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT content FROM clipboard_items WHERE kind = 'image'")
            .fetch_all(pool)
            .await?;
    let references = names.into_iter().collect();
    tauri::async_runtime::spawn_blocking(move || {
        let mut stats = CleanupStats::default();
        for dir in ["origin", "thumbnails"] {
            clean_unreferenced_files(&images.join(dir), &references, &mut stats)?;
        }
        Ok(stats)
    })
    .await?
}

/// Visits owned files only, then removes directories that became empty.
pub(crate) fn clean_unreferenced_files(
    root: &Path,
    references: &HashSet<String>,
    stats: &mut CleanupStats,
) -> Result<()> {
    let exists = match root.try_exists() {
        Ok(exists) => exists,
        Err(error) => {
            log::warn!("inspect resource cache {root:?} failed: {error}");
            return Ok(());
        }
    };
    if !exists {
        return Ok(());
    }
    for entry in WalkDir::new(root)
        .contents_first(true)
        .follow_root_links(false)
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                log::warn!("scan resource cache for reclamation failed: {error}");
                continue;
            }
        };
        let path = entry.path();
        if entry.file_type().is_dir() {
            let _ = fs::remove_dir(path);
        } else if entry.file_type().is_file()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| !references.contains(name))
        {
            let bytes = match entry.metadata() {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    log::warn!("measure unreferenced cache file {path:?} failed: {error}");
                    continue;
                }
            };
            match fs::remove_file(path) {
                Ok(()) => {
                    stats.files += 1;
                    stats.bytes += bytes;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    log::warn!("remove unreferenced cache file {path:?} failed: {error}");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_support::memory_pool;

    #[tokio::test]
    async fn deleted_image_is_kept_until_its_last_database_reference_is_gone() {
        let temp = tempfile::tempdir().unwrap();
        let store = ImageStore::for_test(temp.path().join("images"));
        let file_name = format!("{}.png", "a".repeat(64));
        let origin = store.origin_path(&file_name);
        fs::create_dir_all(origin.parent().unwrap()).unwrap();
        fs::write(&origin, b"image").unwrap();
        let pool = memory_pool().await;
        sqlx::query(
            "INSERT INTO clipboard_items \
             (id, kind, content, content_hash, sync_key, platform, created_at, updated_at) \
             VALUES ('survivor', 'image', ?, 'hash', 'sync', 'macos', '0', '0')",
        )
        .bind(&file_name)
        .execute(&pool)
        .await
        .unwrap();

        reclaim_deleted_images(&pool, &store, std::slice::from_ref(&file_name))
            .await
            .unwrap();
        assert!(origin.is_file());

        sqlx::query("DELETE FROM clipboard_items WHERE id = 'survivor'")
            .execute(&pool)
            .await
            .unwrap();
        reclaim_deleted_images(&pool, &store, &[file_name])
            .await
            .unwrap();
        assert!(!origin.exists());
    }
}
