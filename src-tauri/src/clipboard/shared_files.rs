//! Stable references, reclamation, and legacy migration for app-owned file resources.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, MutexGuard};
use walkdir::WalkDir;

use crate::db::items::content_hash;
use crate::db::models::{ClipboardItem, ClipboardKind};

pub(crate) const DIRECTORY: &str = "shared-files";
pub(crate) const SYNC_DIRECTORY: &str = "sync-files";
pub(crate) const REFERENCE_PREFIX: &str = "ecopaste-resource://";

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
        .flat_map(|content| content.lines())
        .map(|path| resolve_path(root.parent().unwrap_or(&root), path))
        .collect::<Result<_>>()?;
    tauri::async_runtime::spawn_blocking(move || clean_paths(&root, &references)).await?
}

/// Reclaims both locally imported attachments and materialized synchronized files.
/// The caller holds [`lock()`] until database references and owned files are stable.
pub(crate) async fn cleanup_resources_locked(
    pool: &SqlitePool,
    resources: PathBuf,
) -> Result<CleanupStats> {
    let mut stats = cleanup_locked(pool, resources.join(DIRECTORY)).await?;
    let sync_stats = cleanup_sync_files_locked(pool, resources.join(SYNC_DIRECTORY)).await?;
    stats.files += sync_stats.files;
    stats.bytes += sync_stats.bytes;
    Ok(stats)
}

/// Keeps a synchronized event directory while any live file record references a path below it.
async fn cleanup_sync_files_locked(pool: &SqlitePool, root: PathBuf) -> Result<CleanupStats> {
    if !root.exists() {
        return Ok(CleanupStats::default());
    }
    let contents =
        sqlx::query_scalar::<_, String>("SELECT content FROM clipboard_items WHERE kind = 'files'")
            .fetch_all(pool)
            .await
            .context("read live synchronized file references")?;
    let references: HashSet<PathBuf> = contents
        .iter()
        .flat_map(|content| content.lines())
        .map(|path| resolve_path(root.parent().unwrap_or(&root), path))
        .collect::<Result<_>>()?;
    tauri::async_runtime::spawn_blocking(move || clean_sync_files(&root, &references)).await?
}

/// Walks children before parents without following symlinks, retaining files referenced by any row.
fn clean_paths(root: &Path, references: &HashSet<PathBuf>) -> Result<CleanupStats> {
    let mut stats = CleanupStats::default();
    if !root.exists() {
        return Ok(stats);
    }
    for entry in WalkDir::new(root).contents_first(true).follow_links(false) {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                log::warn!("scan shared attachment for reclamation failed: {error}");
                continue;
            }
        };
        let path = entry.path();
        if entry.file_type().is_dir() {
            match fs::remove_dir(path) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
                    ) => {}
                Err(error) => {
                    log::warn!("remove empty shared attachment directory {path:?} failed: {error}");
                }
            }
        } else if entry.file_type().is_file() && !references.contains(path) {
            let bytes = match entry.metadata() {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    log::warn!("measure unreferenced shared attachment {path:?} failed: {error}");
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
                    log::warn!("remove unreferenced shared attachment {path:?} failed: {error}");
                }
            }
        }
    }
    Ok(stats)
}

/// Synchronized files are grouped by event ID. Retain the whole event directory for directory
/// payloads because the database stores only the selected root path, not every descendant.
fn clean_sync_files(root: &Path, references: &HashSet<PathBuf>) -> Result<CleanupStats> {
    let mut stats = CleanupStats::default();
    if !root.exists() {
        return Ok(stats);
    }
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) => {
            log::warn!("read synchronized file cache {root:?} failed: {error}");
            return Ok(stats);
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                log::warn!("read synchronized file cache entry failed: {error}");
                continue;
            }
        };
        let path = entry.path();
        if references
            .iter()
            .any(|reference| reference == &path || reference.starts_with(&path))
        {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                log::warn!("read synchronized file type {path:?} failed: {error}");
                continue;
            }
        };
        if file_type.is_dir() {
            let mut files = 0_u64;
            let mut bytes = 0_u64;
            let mut measured = true;
            for child in WalkDir::new(&path).follow_links(false) {
                let child = match child {
                    Ok(child) => child,
                    Err(error) => {
                        log::warn!("measure synchronized file cache {path:?} failed: {error}");
                        measured = false;
                        break;
                    }
                };
                if child.file_type().is_file() {
                    match child.metadata() {
                        Ok(metadata) => {
                            files += 1;
                            bytes += metadata.len();
                        }
                        Err(error) => {
                            log::warn!(
                                "measure synchronized file {} failed: {error}",
                                child.path().display()
                            );
                            measured = false;
                            break;
                        }
                    }
                }
            }
            if !measured {
                continue;
            }
            match fs::remove_dir_all(&path) {
                Ok(()) => {
                    stats.files += files;
                    stats.bytes += bytes;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => log::warn!(
                    "remove unreferenced synchronized file directory {path:?} failed: {error}"
                ),
            }
        } else {
            let bytes = match entry.metadata() {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    log::warn!("measure unreferenced synchronized file {path:?} failed: {error}");
                    continue;
                }
            };
            match fs::remove_file(&path) {
                Ok(()) => {
                    stats.files += 1;
                    stats.bytes += bytes;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    log::warn!("remove unreferenced synchronized file {path:?} failed: {error}");
                }
            }
        }
    }
    let _ = fs::remove_dir(root);
    Ok(stats)
}

#[derive(Clone, Copy)]
enum ResourceLayout {
    SharedFiles,
    SyncFiles,
}

impl ResourceLayout {
    const fn directory(self) -> &'static str {
        match self {
            Self::SharedFiles => DIRECTORY,
            Self::SyncFiles => SYNC_DIRECTORY,
        }
    }
}

struct MigrationRule {
    source: String,
    layout: ResourceLayout,
}

/// Converts legacy absolute app-owned paths into location-independent resource references.
pub(crate) struct ReferenceMigration {
    resources: PathBuf,
    rules: Vec<MigrationRule>,
    infer_shared_files: bool,
    infer_sync_files: bool,
}

impl ReferenceMigration {
    /// Migrates current roots and validates stale roots left by older storage moves.
    pub(crate) fn current(resources: PathBuf) -> Self {
        let shared = resources.join(DIRECTORY).to_string_lossy().into_owned();
        let sync = resources
            .join(SYNC_DIRECTORY)
            .to_string_lossy()
            .into_owned();
        Self {
            resources,
            rules: vec![
                MigrationRule {
                    source: shared,
                    layout: ResourceLayout::SharedFiles,
                },
                MigrationRule {
                    source: sync,
                    layout: ResourceLayout::SyncFiles,
                },
            ],
            infer_shared_files: true,
            infer_sync_files: true,
        }
    }

    /// Migrates a copied database from one known data directory into its new resource directory.
    pub(crate) fn storage_move(source_resources: &Path, target_resources: PathBuf) -> Self {
        Self {
            resources: target_resources,
            rules: vec![
                MigrationRule {
                    source: source_resources
                        .join(DIRECTORY)
                        .to_string_lossy()
                        .into_owned(),
                    layout: ResourceLayout::SharedFiles,
                },
                MigrationRule {
                    source: source_resources
                        .join(SYNC_DIRECTORY)
                        .to_string_lossy()
                        .into_owned(),
                    layout: ResourceLayout::SyncFiles,
                },
            ],
            infer_shared_files: true,
            infer_sync_files: true,
        }
    }

    /// Imports published backups. Missing root metadata falls back to validated archive paths.
    pub(crate) fn backup(
        resources: PathBuf,
        shared_files_root: Option<String>,
        sync_files_root: Option<String>,
    ) -> Self {
        let mut rules = Vec::new();
        if let Some(source) = shared_files_root.as_deref() {
            rules.push(MigrationRule {
                source: source.trim_end_matches(['/', '\\']).to_owned(),
                layout: ResourceLayout::SharedFiles,
            });
        }
        if let Some(source) = sync_files_root.as_deref() {
            rules.push(MigrationRule {
                source: source.trim_end_matches(['/', '\\']).to_owned(),
                layout: ResourceLayout::SyncFiles,
            });
        }
        Self {
            resources,
            rules,
            infer_shared_files: shared_files_root.is_none(),
            infer_sync_files: sync_files_root.is_none(),
        }
    }

    /// Rewrites only validated resources present in the destination; external paths stay intact.
    pub(crate) fn rewrite(&self, content: &str) -> Result<Option<String>> {
        let mut changed = false;
        let paths = content
            .lines()
            .map(|path| {
                if parse_reference(path)?.is_some() {
                    return Ok(path.to_owned());
                }
                for rule in &self.rules {
                    let Some(reference) = self.rewrite_known_path(path, rule)? else {
                        continue;
                    };
                    changed = true;
                    return Ok(reference);
                }
                for layout in [ResourceLayout::SharedFiles, ResourceLayout::SyncFiles] {
                    let infer = match layout {
                        ResourceLayout::SharedFiles => self.infer_shared_files,
                        ResourceLayout::SyncFiles => self.infer_sync_files,
                    };
                    if !infer {
                        continue;
                    }
                    let Some(reference) = self.infer_archived_path(path, layout)? else {
                        continue;
                    };
                    changed = true;
                    return Ok(reference);
                }
                Ok(path.to_owned())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(changed.then(|| paths.join("\n")))
    }

    /// Changes only local references and their hash, preserving timestamps and sync identity.
    pub(crate) async fn apply(&self, pool: &SqlitePool) -> Result<()> {
        let mut tx = pool
            .begin()
            .await
            .context("begin managed resource reference migration")?;
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT id, content FROM clipboard_items WHERE kind = 'files'",
        )
        .fetch_all(&mut *tx)
        .await?;
        for (id, content) in rows {
            let Some(content) = self.rewrite(&content)? else {
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
            .context("commit managed resource reference migration")?;
        Ok(())
    }

    fn rewrite_known_path(&self, path: &str, rule: &MigrationRule) -> Result<Option<String>> {
        if rule.source.is_empty() {
            return Ok(None);
        }
        let Some(suffix) = path.strip_prefix(&rule.source) else {
            return Ok(None);
        };
        let Some(suffix) = suffix.strip_prefix(['/', '\\']) else {
            return Ok(None);
        };
        self.reference_if_present(rule.layout, suffix.split(['/', '\\']).collect())
    }

    fn infer_archived_path(&self, path: &str, layout: ResourceLayout) -> Result<Option<String>> {
        let parts = path.split(['/', '\\']).collect::<Vec<_>>();
        for index in 1..parts.len() {
            if parts[index - 1] != "resources" || parts[index] != layout.directory() {
                continue;
            }
            if let Some(reference) =
                self.reference_if_present(layout, parts[index + 1..].to_vec())?
            {
                return Ok(Some(reference));
            }
        }
        Ok(None)
    }

    fn reference_if_present(
        &self,
        layout: ResourceLayout,
        parts: Vec<&str>,
    ) -> Result<Option<String>> {
        if !valid_layout_parts(layout, &parts) {
            return Ok(None);
        }
        let mut target = self.resources.join(layout.directory());
        for part in &parts {
            target.push(part);
        }
        if !target.starts_with(&self.resources) || !target.try_exists()? {
            return Ok(None);
        }
        Ok(Some(format_reference(layout, &parts)))
    }
}

/// Converts a newly captured app-owned file item before it enters persistence and sync queues.
pub(crate) fn normalize_new_item(resources: &Path, item: &mut ClipboardItem) -> Result<()> {
    if item.kind != ClipboardKind::Files {
        return Ok(());
    }
    let Some(content) = normalize_owned_content(resources, &item.content)? else {
        return Ok(());
    };
    let hash = content_hash(ClipboardKind::Files, &content);
    item.content = content;
    item.content_hash.clone_from(&hash);
    item.sync_key = hash;
    Ok(())
}

/// Converts only paths below the active managed roots; similarly named external paths stay intact.
pub(crate) fn normalize_owned_content(resources: &Path, content: &str) -> Result<Option<String>> {
    let mut migration = ReferenceMigration::current(resources.to_owned());
    migration.infer_shared_files = false;
    migration.infer_sync_files = false;
    migration.rewrite(content)
}

/// Resolves one managed reference against the active resource root; external paths pass through.
pub(crate) fn resolve_path(resources: &Path, value: &str) -> Result<PathBuf> {
    let Some((layout, parts)) = parse_reference(value)? else {
        return Ok(PathBuf::from(value));
    };
    let mut path = resources.join(layout.directory());
    for part in parts {
        path.push(part);
    }
    anyhow::ensure!(
        path.starts_with(resources),
        "managed resource resolved outside its root"
    );
    Ok(path)
}

/// Resolves every path in a files item for OS-facing commands while keeping the database logical.
pub(crate) fn resolve_item_content(resources: &Path, item: &mut ClipboardItem) -> Result<()> {
    if item.kind != ClipboardKind::Files {
        return Ok(());
    }
    item.content = item
        .content
        .lines()
        .map(|value| {
            Ok(resolve_path(resources, value)?
                .to_string_lossy()
                .into_owned())
        })
        .collect::<Result<Vec<_>>>()?
        .join("\n");
    Ok(())
}

fn parse_reference(value: &str) -> Result<Option<(ResourceLayout, Vec<String>)>> {
    let Some(suffix) = value.strip_prefix(REFERENCE_PREFIX) else {
        return Ok(None);
    };
    let mut parts = suffix.split('/');
    let layout = match parts.next() {
        Some(DIRECTORY) => ResourceLayout::SharedFiles,
        Some(SYNC_DIRECTORY) => ResourceLayout::SyncFiles,
        _ => anyhow::bail!("unknown managed resource directory"),
    };
    let parts = parts.map(decode_component).collect::<Result<Vec<_>>>()?;
    anyhow::ensure!(
        valid_layout_parts(layout, &parts),
        "invalid managed resource reference"
    );
    Ok(Some((layout, parts)))
}

fn format_reference(layout: ResourceLayout, parts: &[&str]) -> String {
    format!(
        "{REFERENCE_PREFIX}{}/{}",
        layout.directory(),
        parts
            .iter()
            .map(|part| encode_component(part))
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn valid_layout_parts<T: AsRef<str>>(layout: ResourceLayout, parts: &[T]) -> bool {
    let shape_valid = match layout {
        ResourceLayout::SharedFiles => {
            parts.len() == 2
                && parts[0].as_ref().len() == 64
                && parts[0]
                    .as_ref()
                    .bytes()
                    .all(|value| value.is_ascii_hexdigit())
        }
        ResourceLayout::SyncFiles => parts.len() >= 3,
    };
    shape_valid
        && parts
            .iter()
            .all(|part| valid_managed_component(part.as_ref()))
}

fn valid_managed_component(component: &str) -> bool {
    !component.is_empty()
        && !matches!(component, "." | "..")
        && !component.contains(['/', '\\', '\0'])
}

fn encode_component(component: &str) -> String {
    let mut encoded = String::with_capacity(component.len());
    for byte in component.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write;
            write!(encoded, "%{byte:02X}").expect("writing to a string cannot fail");
        }
    }
    encoded
}

fn decode_component(component: &str) -> Result<String> {
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            anyhow::ensure!(index + 2 < bytes.len(), "invalid managed resource escape");
            let high = decode_hex(bytes[index + 1])?;
            let low = decode_hex(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
            continue;
        }
        anyhow::ensure!(
            bytes[index].is_ascii_alphanumeric()
                || matches!(bytes[index], b'-' | b'.' | b'_' | b'~'),
            "unescaped managed resource component"
        );
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(decoded).context("managed resource component is not utf-8")
}

fn decode_hex(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => anyhow::bail!("invalid managed resource escape"),
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

    #[tokio::test]
    async fn synchronized_file_cleanup_keeps_only_referenced_event_directories() {
        let temp = tempfile::tempdir().unwrap();
        let resources = temp.path().join("resources");
        let sync_root = resources.join(SYNC_DIRECTORY);
        let live_directory = sync_root.join("live-event").join("000").join("folder");
        let orphan_directory = sync_root.join("orphan-event").join("000");
        fs::create_dir_all(&live_directory).unwrap();
        fs::create_dir_all(&orphan_directory).unwrap();
        fs::write(live_directory.join("live.txt"), b"live").unwrap();
        fs::write(orphan_directory.join("orphan.txt"), b"orphan").unwrap();
        let pool = memory_pool().await;
        let live = files_item(temp.path(), &[&live_directory]);
        insert_item(&pool, &live).await.unwrap();

        let stats = cleanup_resources_locked(&pool, resources.clone())
            .await
            .unwrap();

        assert_eq!((stats.files, stats.bytes), (1, 6));
        assert!(live_directory.join("live.txt").is_file());
        assert!(!sync_root.join("orphan-event").exists());

        delete_item(&pool, &live.id).await.unwrap();
        let stats = cleanup_resources_locked(&pool, resources).await.unwrap();
        assert_eq!((stats.files, stats.bytes), (1, 4));
        assert!(!sync_root.exists());
    }

    #[test]
    fn migration_handles_both_source_platforms_and_leaves_external_paths_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let resources = temp.path().join("resources");
        let digest = "a".repeat(64);
        attachment(&resources.join(DIRECTORY), 'a', "report.pdf", b"restored");
        let android = "/data/user/0/app/files/resources/shared-files";
        let windows = r"C:\Users\User\EcoPaste\resources\shared-files";
        for (source, separator) in [(android, "/"), (windows, "\\")] {
            let migration =
                ReferenceMigration::backup(resources.clone(), Some(source.to_owned()), None);
            let managed = format!("{source}{separator}{digest}{separator}report.pdf");
            let external = "/storage/emulated/0/Download/report.pdf";
            let content = format!("{managed}\n{external}");
            assert_eq!(
                migration.rewrite(&content).unwrap(),
                Some(format!(
                    "{REFERENCE_PREFIX}{DIRECTORY}/{digest}/report.pdf\n{external}"
                ))
            );
            assert!(migration
                .rewrite(&format!("{source}-other/{digest}/report.pdf"))
                .unwrap()
                .is_none());
            assert!(migration
                .rewrite(&format!("{source}/{digest}/../private"))
                .unwrap()
                .is_none());
            assert!(migration
                .rewrite(&format!("{source}/{digest}/C:private"))
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn managed_references_escape_file_names_and_resolve_them_losslessly() {
        let temp = tempfile::tempdir().unwrap();
        let resources = temp.path().join("resources");
        let file = attachment(
            &resources.join(DIRECTORY),
            'a',
            "报告 100%.pdf",
            b"restored",
        );
        let migration = ReferenceMigration::current(resources.clone());

        let reference = migration.rewrite(&file.to_string_lossy()).unwrap().unwrap();

        assert!(reference.ends_with("/%E6%8A%A5%E5%91%8A%20100%25.pdf"));
        assert_eq!(resolve_path(&resources, &reference).unwrap(), file);
    }

    #[test]
    fn migration_rewrites_synchronized_files_with_nested_directory_paths() {
        let temp = tempfile::tempdir().unwrap();
        let resources = temp.path().join("resources");
        let target = resources
            .join(SYNC_DIRECTORY)
            .join("event-1/000/folder/sync-files");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("report.pdf"), b"restored").unwrap();
        let migration = ReferenceMigration::backup(
            resources.clone(),
            None,
            Some("/old/resources/sync-files".to_owned()),
        );
        let managed = "/old/resources/sync-files/event-1/000/folder/sync-files/report.pdf";
        let external = "/old/resources/sync-files-other/event-1/000/report.pdf";

        assert_eq!(
            migration
                .rewrite(&format!("{managed}\n{external}"))
                .unwrap(),
            Some(format!(
                "{REFERENCE_PREFIX}{SYNC_DIRECTORY}/event-1/000/folder/sync-files/report.pdf\n{external}"
            ))
        );
        assert!(migration
            .rewrite("/old/resources/sync-files/event-1/../private")
            .unwrap()
            .is_none());

        let stale = ReferenceMigration::current(resources);
        assert_eq!(
            stale.rewrite(managed).unwrap(),
            Some(format!(
                "{REFERENCE_PREFIX}{SYNC_DIRECTORY}/event-1/000/folder/sync-files/report.pdf"
            ))
        );
    }

    #[tokio::test]
    async fn synchronized_file_migration_preserves_favorite_record_and_resource() {
        let temp = tempfile::tempdir().unwrap();
        let old_resources = temp.path().join("old/resources");
        let new_resources = temp.path().join("new/resources");
        let old_file = old_resources
            .join(SYNC_DIRECTORY)
            .join("event-1/000/report.pdf");
        let new_file = new_resources
            .join(SYNC_DIRECTORY)
            .join("event-1/000/report.pdf");
        fs::create_dir_all(old_file.parent().unwrap()).unwrap();
        fs::create_dir_all(new_file.parent().unwrap()).unwrap();
        fs::write(&old_file, b"old").unwrap();
        fs::write(&new_file, b"restored").unwrap();
        let pool = memory_pool().await;
        let mut item = files_item(temp.path(), &[&old_file]);
        item.is_favorite = true;
        insert_item(&pool, &item).await.unwrap();
        let migration = ReferenceMigration::storage_move(&old_resources, new_resources.clone());

        migration.apply(&pool).await.unwrap();
        let restored = crate::db::items::find_item_by_id(&pool, &item.id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            restored.content,
            format!("{REFERENCE_PREFIX}{SYNC_DIRECTORY}/event-1/000/report.pdf")
        );
        assert_eq!(
            fs::read(resolve_path(&new_resources, &restored.content).unwrap()).unwrap(),
            b"restored"
        );
        assert!(restored.is_favorite);
        assert_eq!(
            cleanup_sync_files_locked(&pool, new_resources.join(SYNC_DIRECTORY))
                .await
                .unwrap()
                .files,
            0
        );
    }

    #[tokio::test]
    async fn overwrite_and_storage_move_preserve_sync_identity_and_record_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let old_resources = temp.path().join("old/resources");
        let new_resources = temp.path().join("new/resources");
        let path = attachment(
            &old_resources.join(DIRECTORY),
            'a',
            "report.pdf",
            b"restore",
        );
        attachment(
            &new_resources.join(DIRECTORY),
            'a',
            "report.pdf",
            b"restore",
        );
        let pool = memory_pool().await;
        let mut item = files_item(temp.path(), &[&path]);
        item.is_favorite = true;
        insert_item(&pool, &item).await.unwrap();
        let migration = ReferenceMigration::storage_move(&old_resources, new_resources.clone());
        migration.apply(&pool).await.unwrap();
        let restored = crate::db::items::find_item_by_id(&pool, &item.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            restored.content,
            format!(
                "{REFERENCE_PREFIX}{DIRECTORY}/{}/report.pdf",
                "a".repeat(64)
            )
        );
        assert_eq!(
            fs::read(resolve_path(&new_resources, &restored.content).unwrap()).unwrap(),
            b"restore"
        );
        assert_eq!(
            restored.content_hash,
            content_hash(ClipboardKind::Files, &restored.content)
        );
        assert_eq!(restored.sync_key, item.sync_key);
        assert_eq!(restored.source_revision, item.source_revision);
        assert_eq!(restored.created_at, item.created_at);
        assert_eq!(restored.updated_at, item.updated_at);
        assert!(restored.is_favorite);
        assert_eq!(
            cleanup_locked(&pool, new_resources.join(DIRECTORY))
                .await
                .unwrap()
                .files,
            0
        );
    }
}
