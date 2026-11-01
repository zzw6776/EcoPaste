//! Ownership, reference-based reclamation and relocation of imported share attachments.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tauri::{AppHandle, Manager};
use tokio::sync::{Mutex, MutexGuard};
use walkdir::WalkDir;

use crate::db::items::content_hash;
use crate::db::models::ClipboardKind;

pub(crate) const DIRECTORY: &str = "shared-files";

/// Serializes attachment creation through database commit with reclamation and storage replacement.
pub(crate) async fn lock() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::const_new(());
    LOCK.lock().await
}

#[derive(Default)]
pub(crate) struct CleanupStats {
    pub files: u64,
    pub bytes: u64,
}

/// Reclaims only app-owned attachments; original files outside this directory are never visited.
pub(crate) async fn cleanup(app: &AppHandle) -> Result<CleanupStats> {
    let _guard = lock().await;
    let root = crate::core::paths::resources_dir(app)?.join(DIRECTORY);
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    cleanup_locked(&pool, root).await
}

/// A successful history deletion remains successful even when file reclamation needs a later retry.
pub(crate) async fn cleanup_after_delete(app: &AppHandle) {
    if let Err(error) = cleanup(app).await {
        log::warn!("reclaim shared attachments failed: {error:#}");
    }
}

/// The caller holds `lock()` until both file creation and record persistence have finished.
pub(crate) async fn cleanup_locked(pool: &SqlitePool, root: PathBuf) -> Result<CleanupStats> {
    if !root.exists() {
        return Ok(CleanupStats::default());
    }
    let contents =
        sqlx::query_scalar::<_, String>("SELECT content FROM clipboard_items WHERE kind = 'files'")
            .fetch_all(pool)
            .await
            .context("read live shared attachment references")?;
    let references: HashSet<PathBuf> = contents
        .iter()
        .flat_map(|content| content.lines().map(PathBuf::from))
        .collect();
    tauri::async_runtime::spawn_blocking(move || clean_paths(&root, &references)).await?
}

/// Walks children before parents without following symlinks, retaining files referenced by any row.
fn clean_paths(root: &Path, references: &HashSet<PathBuf>) -> Result<CleanupStats> {
    let mut stats = CleanupStats::default();
    if !root.exists() {
        return Ok(stats);
    }
    for entry in WalkDir::new(root).contents_first(true).follow_links(false) {
        let entry = entry.context("walk shared attachments")?;
        let path = entry.path();
        if entry.file_type().is_dir() {
            match fs::remove_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                Err(error) => {
                    return Err(error).context("remove empty shared attachment directory")
                }
            }
        } else if entry.file_type().is_file() && !references.contains(path) {
            let bytes = entry.metadata()?.len();
            fs::remove_file(path).context("remove unreferenced shared attachment")?;
            stats.files += 1;
            stats.bytes += bytes;
        }
    }
    Ok(stats)
}

/// Maps one known source attachment root to the current root across Windows and Android/macOS paths.
pub(crate) struct Relocation {
    source: String,
    target: PathBuf,
}

impl Relocation {
    pub(crate) fn new(source: String, target: PathBuf) -> Self {
        Self {
            source: source.trim_end_matches(['/', '\\']).to_owned(),
            target,
        }
    }

    /// Rewrites only content-addressed files under the declared root; external file paths stay intact.
    pub(crate) fn rewrite(&self, content: &str) -> Option<String> {
        if self.source.is_empty() {
            return None;
        }
        let mut changed = false;
        let paths = content
            .lines()
            .map(|path| {
                let Some(suffix) = path.strip_prefix(&self.source) else {
                    return path.to_owned();
                };
                let Some(suffix) = suffix.strip_prefix(['/', '\\']) else {
                    return path.to_owned();
                };
                let parts = suffix.split(['/', '\\']).collect::<Vec<_>>();
                if parts.len() != 2
                    || parts[0].len() != 64
                    || !parts[0].bytes().all(|value| value.is_ascii_hexdigit())
                    || parts[1].is_empty()
                    || matches!(parts[1], "." | "..")
                    || parts[1].contains(':')
                {
                    return path.to_owned();
                }
                let target = self.target.join(parts[0]).join(parts[1]);
                let target = target.to_string_lossy().into_owned();
                changed |= target != path;
                target
            })
            .collect::<Vec<_>>();
        changed.then(|| paths.join("\n"))
    }

    /// Changes only local filesystem references and their hash, preserving timestamps and sync identity.
    pub(crate) async fn apply(&self, pool: &SqlitePool) -> Result<()> {
        let mut tx = pool
            .begin()
            .await
            .context("begin shared attachment relocation")?;
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT id, content FROM clipboard_items WHERE kind = 'files'",
        )
        .fetch_all(&mut *tx)
        .await?;
        for (id, content) in rows {
            let Some(content) = self.rewrite(&content) else {
                continue;
            };
            sqlx::query("UPDATE clipboard_items SET content = ?, content_hash = ? WHERE id = ?")
                .bind(&content)
                .bind(content_hash(ClipboardKind::Files, &content))
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit()
            .await
            .context("commit shared attachment relocation")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clipboard::{build_item, ClipboardPayload, ImageStore};
    use crate::db::items::{cleanup_history, clear_items, delete_item, insert_item};
    use crate::db::test_support::memory_pool;

    fn attachment(root: &Path, digest: char, name: &str, bytes: &[u8]) -> PathBuf {
        let directory = root.join(digest.to_string().repeat(64));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }

    fn files_item(root: &Path, paths: &[&Path]) -> crate::db::models::ClipboardItem {
        let store = ImageStore::for_test(root.join("images"));
        let paths = paths
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        build_item(&store, &ClipboardPayload::Files(paths))
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn deletion_and_clear_reclaim_only_unreferenced_owned_attachments() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(DIRECTORY);
        let shared = attachment(&root, 'a', "report.pdf", b"shared");
        let second = attachment(&root, 'b', "report.pdf", b"second");
        let orphan = attachment(&root, 'c', "orphan.mp4", b"orphan");
        let original = temp.path().join("original.txt");
        fs::write(&original, b"original").unwrap();
        let pool = memory_pool().await;
        let first = files_item(temp.path(), &[&shared]);
        let mut retained = files_item(temp.path(), &[&shared, &second, &original]);
        retained.is_favorite = true;
        insert_item(&pool, &first).await.unwrap();
        insert_item(&pool, &retained).await.unwrap();

        delete_item(&pool, &first.id).await.unwrap();
        let stats = cleanup_locked(&pool, root.clone()).await.unwrap();
        assert_eq!((stats.files, stats.bytes), (1, 6));
        assert!(!orphan.exists());
        assert!(shared.exists());
        assert!(second.exists());

        clear_items(&pool, false, false).await.unwrap();
        assert_eq!(cleanup_locked(&pool, root.clone()).await.unwrap().files, 0);
        assert!(shared.exists());
        clear_items(&pool, true, true).await.unwrap();
        let stats = cleanup_locked(&pool, root.clone()).await.unwrap();
        assert_eq!((stats.files, stats.bytes), (2, 12));
        assert!(!root.exists());
        assert_eq!(fs::read(original).unwrap(), b"original");
    }

    #[tokio::test]
    async fn retention_keeps_attachments_used_by_pinned_records() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(DIRECTORY);
        let retained = attachment(&root, 'a', "retained.pdf", b"keep");
        let expired = attachment(&root, 'b', "expired.pdf", b"delete");
        let pool = memory_pool().await;
        let mut pinned = files_item(temp.path(), &[&retained]);
        pinned.is_pinned = true;
        let old = files_item(temp.path(), &[&retained, &expired]);
        insert_item(&pool, &pinned).await.unwrap();
        insert_item(&pool, &old).await.unwrap();
        let outcome = cleanup_history(&pool, Some(chrono::Utc::now()), None)
            .await
            .unwrap();
        assert_eq!(outcome.removed, 1);
        assert_eq!(cleanup_locked(&pool, root).await.unwrap().files, 1);
        assert!(retained.exists());
        assert!(!expired.exists());
    }

    #[test]
    fn relocation_handles_both_source_platforms_and_leaves_external_paths_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join(DIRECTORY);
        let digest = "a".repeat(64);
        let android = "/data/user/0/app/files/resources/shared-files";
        let windows = r"C:\Users\User\EcoPaste\resources\shared-files";
        for (source, separator) in [(android, "/"), (windows, "\\")] {
            let relocation = Relocation::new(source.to_owned(), target.clone());
            let managed = format!("{source}{separator}{digest}{separator}report.pdf");
            let external = "/storage/emulated/0/Download/report.pdf";
            let content = format!("{managed}\n{external}");
            assert_eq!(
                relocation.rewrite(&content).unwrap(),
                format!(
                    "{}\n{external}",
                    target.join(&digest).join("report.pdf").display()
                )
            );
            assert!(relocation
                .rewrite(&format!("{source}-other/{digest}/report.pdf"))
                .is_none());
            assert!(relocation
                .rewrite(&format!("{source}/{digest}/../private"))
                .is_none());
            assert!(relocation
                .rewrite(&format!("{source}/{digest}/C:private"))
                .is_none());
        }
    }

    #[tokio::test]
    async fn overwrite_and_storage_move_preserve_sync_identity_and_record_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let old = temp.path().join("old").join(DIRECTORY);
        let new = temp.path().join("new").join(DIRECTORY);
        let path = attachment(&old, 'a', "report.pdf", b"restore");
        attachment(&new, 'a', "report.pdf", b"restore");
        let pool = memory_pool().await;
        let mut item = files_item(temp.path(), &[&path]);
        item.is_favorite = true;
        insert_item(&pool, &item).await.unwrap();
        let relocation = Relocation::new(old.to_string_lossy().into_owned(), new.clone());
        relocation.apply(&pool).await.unwrap();
        let restored = crate::db::items::find_item_by_id(&pool, &item.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            restored.content,
            new.join("a".repeat(64))
                .join("report.pdf")
                .to_string_lossy()
        );
        assert_eq!(fs::read(&restored.content).unwrap(), b"restore");
        assert_eq!(
            restored.content_hash,
            content_hash(ClipboardKind::Files, &restored.content)
        );
        assert_eq!(restored.sync_key, item.sync_key);
        assert_eq!(restored.source_revision, item.source_revision);
        assert_eq!(restored.created_at, item.created_at);
        assert_eq!(restored.updated_at, item.updated_at);
        assert!(restored.is_favorite);
        assert_eq!(cleanup_locked(&pool, new).await.unwrap().files, 0);
    }
}
