//! Android system shares enter the same history, refresh and sync pipeline as clipboard captures.

use std::fs;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
#[cfg(target_os = "android")]
use std::{collections::HashSet, io::Write, mem::ManuallyDrop, os::fd::FromRawFd, sync::Arc};

use anyhow::{Context, Result};
use serde::Deserialize;

use super::{build_item_with_settings, ClipboardPayload, ImagePayload, ImageStore, TextPayload};
use crate::db::models::ClipboardItem;
use crate::settings::{Capture, Sensitive};

const MAX_FILE_NAME_BYTES: usize = 200;

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct ShareDelivery {
    #[cfg_attr(not(target_os = "android"), allow(dead_code))]
    source_app: Option<ShareSourceApp>,
    texts: Vec<String>,
    html: Option<String>,
    files: Vec<SharedFile>,
    failed: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
struct ShareSourceApp {
    package_name: String,
    app_name: String,
    icon_png: Vec<u8>,
}

#[derive(Deserialize)]
struct SharedFile {
    #[serde(default)]
    path: PathBuf,
    #[serde(default)]
    #[cfg_attr(not(target_os = "android"), allow(dead_code))]
    fd: Option<i32>,
    name: String,
    mime: String,
    #[serde(default)]
    #[cfg_attr(not(target_os = "android"), allow(dead_code))]
    size: Option<u64>,
}

#[derive(Default)]
pub(crate) struct ShareOutcome {
    pub saved: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Explicit shares preserve every supported representation without automatic capture limits.
fn share_capture() -> Capture {
    Capture {
        text: true,
        html: true,
        rtf: true,
        image: true,
        files: true,
        max_text_mb: 0,
        max_image_mb: 0,
        ..Default::default()
    }
}

/// Keeps the basename portable when the imported attachment later synchronizes to a desktop.
fn safe_file_name(name: &str) -> String {
    let name: String = name
        .chars()
        .map(|value| {
            if value.is_control() || "/\\:*?\"<>|".contains(value) {
                '_'
            } else {
                value
            }
        })
        .collect();
    let name = name.trim().trim_end_matches('.');
    if name.is_empty() {
        return "attachment".to_owned();
    }
    if name.len() <= MAX_FILE_NAME_BYTES {
        return name.to_owned();
    }
    let suffix = Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| extension.len() + 1 < MAX_FILE_NAME_BYTES)
        .map(|extension| format!(".{extension}"))
        .unwrap_or_default();
    let mut stem = name[..name.len() - suffix.len()].to_owned();
    while stem.len() > MAX_FILE_NAME_BYTES - suffix.len() {
        stem.pop();
    }
    if stem.is_empty() {
        stem.push('_');
    }
    stem.push_str(&suffix);
    stem
}

/// Content addressing preserves files beyond the sender's temporary grant and deduplicates re-shares.
fn store_attachment(source: &Path, root: &Path, name: &str) -> Result<PathBuf> {
    let mut reader = fs::File::open(source)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let directory = root.join(hasher.finalize().to_hex().as_str());
    fs::create_dir_all(&directory)?;
    let destination = directory.join(safe_file_name(name));
    if !destination.is_file() {
        let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
        std::io::copy(&mut fs::File::open(source)?, &mut temporary)?;
        temporary
            .persist_noclobber(&destination)
            .context("persist shared attachment")?;
    }
    Ok(destination)
}

/// Decode supported static images in Rust; other formats retain their original bytes as files.
fn attachment_payload(
    file: &SharedFile,
    staging_root: &Path,
    owned_root: &Path,
) -> Result<ClipboardPayload> {
    let source = file.path.canonicalize()?;
    anyhow::ensure!(
        source.starts_with(staging_root.canonicalize()?) && source.is_file(),
        "shared attachment is outside the private staging directory"
    );
    if file.mime.starts_with("image/") {
        let decoded = (|| -> image::ImageResult<Option<image::DynamicImage>> {
            use image::ImageDecoder;

            let mut reader = image::ImageReader::open(&source)?.with_guessed_format()?;
            let mut limits = image::Limits::default();
            // Large phone photos remain intact as files rather than exhausting Android memory.
            limits.max_alloc = Some(128 * 1024 * 1024);
            reader.limits(limits.clone());
            let mut decoder: Box<dyn ImageDecoder> =
                if reader.format() == Some(image::ImageFormat::Png) {
                    let png = image::codecs::png::PngDecoder::with_limits(
                        reader.into_inner(),
                        limits.clone(),
                    )?;
                    if png.is_apng()? {
                        return Ok(None);
                    }
                    Box::new(png)
                } else {
                    Box::new(reader.into_decoder()?)
                };
            limits.reserve(decoder.total_bytes())?;
            decoder.set_limits(limits)?;
            let orientation = decoder.orientation()?;
            let mut image = image::DynamicImage::from_decoder(decoder)?;
            image.apply_orientation(orientation);
            Ok(Some(image))
        })();
        if let Ok(Some(image)) = decoded {
            let width = image.width();
            let height = image.height();
            let mut png = Cursor::new(Vec::new());
            image.write_to(&mut png, image::ImageFormat::Png)?;
            let bytes = png.into_inner();
            return Ok(ClipboardPayload::Image(ImagePayload {
                bytes,
                width,
                height,
            }));
        }
    }
    let destination = store_attachment(&source, owned_root, &file.name)?;
    Ok(ClipboardPayload::Files(vec![destination
        .to_string_lossy()
        .into_owned()]))
}

/// Materializes image bytes immediately so a multi-image share holds only one image in memory.
fn append_item(
    store: &ImageStore,
    payload: ClipboardPayload,
    capture: &Capture,
    items: &mut Vec<ClipboardItem>,
    outcome: &mut ShareOutcome,
) {
    match build_item_with_settings(
        store,
        &payload,
        capture,
        &Sensitive {
            collect_secrets: true,
            ..Default::default()
        },
        false,
    ) {
        Ok(Some(item)) => items.push(item),
        Ok(None) => outcome.skipped += 1,
        Err(error) => {
            outcome.failed += 1;
            log::warn!("prepare Android shared record failed: {error}");
        }
    }
}

/// Performs image decoding and file ownership transfer on a blocking worker, never the UI thread.
fn prepare_items(
    delivery: ShareDelivery,
    store: &ImageStore,
    staging_root: &Path,
    owned_root: &Path,
) -> (Vec<ClipboardItem>, ShareOutcome) {
    let capture = share_capture();
    let mut outcome = ShareOutcome {
        failed: delivery.failed,
        ..Default::default()
    };
    let mut items = Vec::new();
    let single_text = delivery.texts.len() == 1;
    for text in delivery.texts {
        if text.trim().is_empty() {
            continue;
        }
        append_item(
            store,
            ClipboardPayload::Text(TextPayload {
                text,
                html: single_text.then(|| delivery.html.clone()).flatten(),
                rtf: None,
            }),
            &capture,
            &mut items,
            &mut outcome,
        );
    }
    let mut files = Vec::new();
    for file in delivery.files {
        match attachment_payload(&file, staging_root, owned_root) {
            Ok(ClipboardPayload::Files(paths)) => files.extend(paths),
            Ok(payload) => append_item(store, payload, &capture, &mut items, &mut outcome),
            Err(error) => {
                outcome.failed += 1;
                log::warn!("prepare Android shared attachment failed: {error:#}");
            }
        }
    }
    if !files.is_empty() {
        append_item(
            store,
            ClipboardPayload::Files(files),
            &capture,
            &mut items,
            &mut outcome,
        );
    }
    (items, outcome)
}

/// Applies the shared delivery policy and maps its exclusive route into the durable outbox model.
#[cfg(target_os = "android")]
fn shared_delivery_route(
    item: &ClipboardItem,
    auto_upload_max_mb: u32,
    lan_connected: bool,
) -> Option<crate::db::sync_queue::ContentDeliveryRoute> {
    let target = crate::sync::android_share_delivery_target(
        item.kind == crate::db::models::ClipboardKind::Files,
        item.size.and_then(|size| u64::try_from(size).ok()),
        auto_upload_max_mb,
        lan_connected,
    )?;
    Some(match target {
        crate::sync::SyncTarget::Lan => crate::db::sync_queue::ContentDeliveryRoute::Lan,
        crate::sync::SyncTarget::Cloud => crate::db::sync_queue::ContentDeliveryRoute::Cloud,
    })
}

#[cfg(target_os = "android")]
fn clone_shared_descriptor(file: &SharedFile) -> Result<fs::File> {
    let fd = file.fd.context("shared attachment descriptor is missing")?;
    anyhow::ensure!(fd >= 0, "shared attachment descriptor is invalid");
    // Kotlin retains the provider descriptor until this JNI call returns. Clone it so Rust owns
    // exactly one close operation without racing Android's final close.
    let borrowed = ManuallyDrop::new(unsafe { fs::File::from_raw_fd(fd) });
    borrowed
        .try_clone()
        .context("clone shared attachment descriptor")
}

#[cfg(target_os = "android")]
fn importable_as_image(file: &SharedFile) -> bool {
    const MAX_DECODE_SOURCE_BYTES: u64 = 128 * 1024 * 1024;
    file.mime.starts_with("image/") && file.size.is_none_or(|size| size <= MAX_DECODE_SOURCE_BYTES)
}

#[cfg(target_os = "android")]
fn prepare_shared_images(
    files: Vec<SharedFile>,
    store: &ImageStore,
    staging_root: &Path,
    owned_root: &Path,
) -> (Vec<ClipboardItem>, ShareOutcome) {
    let capture = share_capture();
    let mut outcome = ShareOutcome::default();
    let mut items = Vec::new();
    if let Err(error) = fs::create_dir_all(staging_root) {
        outcome.failed = files.len();
        log::warn!("create Android image share staging directory failed: {error}");
        return (items, outcome);
    }
    for file in files {
        let result = (|| -> Result<ClipboardPayload> {
            let mut temporary = tempfile::NamedTempFile::new_in(staging_root)?;
            std::io::copy(&mut clone_shared_descriptor(&file)?, &mut temporary)?;
            let staged = SharedFile {
                path: temporary.path().to_owned(),
                fd: None,
                name: file.name,
                mime: file.mime,
                size: file.size,
            };
            attachment_payload(&staged, staging_root, owned_root)
        })();
        match result {
            Ok(payload) => append_item(store, payload, &capture, &mut items, &mut outcome),
            Err(error) => {
                outcome.failed += 1;
                log::warn!("prepare Android shared image failed: {error:#}");
            }
        }
    }
    (items, outcome)
}

#[cfg(target_os = "android")]
fn unique_import_name(name: &str, used: &mut HashSet<String>) -> String {
    let safe = safe_file_name(name);
    if used.insert(safe.clone()) {
        return safe;
    }
    let path = Path::new(&safe);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("attachment");
    let extension = path.extension().and_then(|value| value.to_str());
    for suffix in 2_u32.. {
        let candidate = match extension {
            Some(extension) => format!("{stem} ({suffix}).{extension}"),
            None => format!("{stem} ({suffix})"),
        };
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!()
}

#[cfg(target_os = "android")]
struct PendingFileImport {
    item: ClipboardItem,
    files: Vec<(SharedFile, PathBuf)>,
    directory: PathBuf,
    declared_bytes: u64,
}

#[cfg(target_os = "android")]
fn plan_file_import(
    files: Vec<SharedFile>,
    store: &ImageStore,
    owned_root: &Path,
) -> Result<PendingFileImport> {
    let directory = owned_root.join(
        blake3::hash(uuid::Uuid::new_v4().as_bytes())
            .to_hex()
            .as_str(),
    );
    fs::create_dir_all(&directory)?;
    let mut used = HashSet::new();
    let mut planned = Vec::with_capacity(files.len());
    let mut paths = Vec::with_capacity(files.len());
    let mut declared_bytes = 0_u64;
    let all_sizes_known = files.iter().all(|file| file.size.is_some());
    for file in files {
        let destination = directory.join(unique_import_name(&file.name, &mut used));
        fs::File::create(&destination)?;
        declared_bytes = declared_bytes.saturating_add(file.size.unwrap_or(0));
        paths.push(destination.to_string_lossy().into_owned());
        planned.push((file, destination));
    }
    let mut item = build_item_with_settings(
        store,
        &ClipboardPayload::Files(paths),
        &share_capture(),
        &Sensitive {
            collect_secrets: true,
            ..Default::default()
        },
        false,
    )?
    .context("shared file payload was empty")?;
    item.size = all_sizes_known.then(|| i64::try_from(declared_bytes).unwrap_or(i64::MAX));
    Ok(PendingFileImport {
        item,
        files: planned,
        directory,
        declared_bytes,
    })
}

#[cfg(target_os = "android")]
fn materialize_file_import(
    files: Vec<(SharedFile, PathBuf)>,
    manager: Option<&Arc<crate::sync::SyncManager>>,
    task_id: Option<&str>,
) -> Result<u64> {
    let mut transferred = 0_u64;
    let mut buffer = [0_u8; 256 * 1024];
    for (file, destination) in files {
        let mut source = clone_shared_descriptor(&file)?;
        let directory = destination
            .parent()
            .context("shared attachment destination has no directory")?;
        let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
        loop {
            if manager
                .zip(task_id)
                .is_some_and(|(manager, task_id)| manager.shared_file_import_canceled(task_id))
            {
                anyhow::bail!("shared file import canceled");
            }
            let count = source.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            temporary.write_all(&buffer[..count])?;
            transferred = transferred.saturating_add(count as u64);
            if let Some((manager, task_id)) = manager.zip(task_id) {
                manager.update_shared_file_import(task_id, transferred);
            }
        }
        temporary.flush()?;
        temporary
            .persist(&destination)
            .map_err(|error| error.error)
            .context("persist shared attachment")?;
    }
    Ok(transferred)
}

#[cfg(target_os = "android")]
async fn remove_import_placeholder(
    app: &tauri::AppHandle,
    pool: &sqlx::SqlitePool,
    item_id: &str,
    directory: &Path,
) {
    if let Err(error) = sqlx::query("DELETE FROM clipboard_items WHERE id = ?")
        .bind(item_id)
        .execute(pool)
        .await
    {
        log::warn!("remove failed Android share placeholder failed: {error:#}");
    }
    if let Err(error) = fs::remove_dir_all(directory) {
        if error.kind() != std::io::ErrorKind::NotFound {
            log::warn!("remove failed Android share files failed: {error}");
        }
    }
    super::watcher::notify_clipboard_updated(
        app,
        item_id,
        crate::db::models::ClipboardKind::Files,
        false,
    );
}

/// Commits each record independently so an unreadable attachment cannot discard the other content.
#[cfg(target_os = "android")]
pub(crate) async fn receive(
    app: &tauri::AppHandle,
    delivery: ShareDelivery,
) -> Result<ShareOutcome> {
    use tauri::Manager;

    let _guard = super::shared_files::lock().await;
    let store = app.state::<ImageStore>().inner().clone();
    let staging_root = app.path().app_cache_dir()?.join("ecopaste-share");
    let owned_root = crate::core::paths::resources_dir(app)?.join(super::shared_files::DIRECTORY);
    let ShareDelivery {
        source_app,
        texts,
        html,
        files,
        failed,
    } = delivery;
    let source_app = source_app
        .filter(|source| !source.package_name.is_empty())
        .map(|source| {
            super::watcher::materialize_source(
                &app.state::<super::AppIconStore>(),
                Some(&app.state::<super::AppsRegistry>()),
                super::source::FrontmostApp {
                    id: source.package_name,
                    name: source.app_name,
                    platform: crate::db::models::Platform::Android,
                    icon_png: (!source.icon_png.is_empty()).then_some(source.icon_png),
                },
            )
        });
    let (image_files, direct_files): (Vec<_>, Vec<_>) =
        files.into_iter().partition(importable_as_image);
    let owned_root_for_task = owned_root.clone();
    let staging_root_for_task = staging_root.clone();
    let store_for_task = store.clone();
    let (mut items, mut outcome) = tauri::async_runtime::spawn_blocking(move || {
        let (mut items, mut outcome) = prepare_items(
            ShareDelivery {
                source_app: None,
                texts,
                html,
                files: Vec::new(),
                failed,
            },
            &store_for_task,
            &staging_root_for_task,
            &owned_root_for_task,
        );
        let (image_items, image_outcome) = prepare_shared_images(
            image_files,
            &store_for_task,
            &staging_root_for_task,
            &owned_root_for_task,
        );
        items.extend(image_items);
        outcome.skipped += image_outcome.skipped;
        outcome.failed += image_outcome.failed;
        (items, outcome)
    })
    .await?;
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let sync_settings = app
        .state::<crate::settings::SettingsStore>()
        .snapshot()
        .sync;
    let lan_connected = app
        .try_state::<std::sync::Arc<crate::sync::SyncManager>>()
        .is_some_and(|manager| manager.has_connected_lan_peer());
    for mut item in items.drain(..) {
        item.source_app_id = source_app.as_ref().map(|source| source.id.clone());
        let initial_delivery_route =
            shared_delivery_route(&item, sync_settings.auto_upload_max_mb, lan_connected);
        match super::watcher::persist_and_notify_with_sync(
            app,
            &pool,
            &item,
            source_app.as_ref(),
            None,
            Some(crate::db::sync_queue::ContentSyncOptions {
                manual_share: true,
                delivery_route: initial_delivery_route,
                ..Default::default()
            }),
        )
        .await
        {
            Ok(_) => outcome.saved += 1,
            Err(error) => {
                outcome.failed += 1;
                log::error!("persist Android shared record failed: {error}");
            }
        }
    }
    if !direct_files.is_empty() {
        let pending = plan_file_import(direct_files, &store, &owned_root)?;
        let mut item = pending.item;
        item.source_app_id = source_app.as_ref().map(|source| source.id.clone());
        let initial_delivery_route =
            shared_delivery_route(&item, sync_settings.auto_upload_max_mb, lan_connected);
        let manager = app
            .try_state::<Arc<crate::sync::SyncManager>>()
            .map(|manager| manager.inner().clone());
        let initial_route = match initial_delivery_route.unwrap_or({
            if lan_connected {
                crate::db::sync_queue::ContentDeliveryRoute::Lan
            } else {
                crate::db::sync_queue::ContentDeliveryRoute::Cloud
            }
        }) {
            crate::db::sync_queue::ContentDeliveryRoute::Cloud => crate::sync::TransferRoute::Cloud,
            crate::db::sync_queue::ContentDeliveryRoute::Lan => crate::sync::TransferRoute::Lan,
        };
        let task_id = manager.as_ref().map(|manager| {
            manager.start_shared_file_import(&item.id, pending.declared_bytes, initial_route)
        });
        match super::watcher::persist_and_notify_with_sync(
            app, &pool, &item, source_app.as_ref(), None, None,
        )
        .await
        {
            Ok(result) => {
                item = crate::db::items::find_item_by_id(&pool, &result.id)
                    .await?
                    .context("imported shared file record is missing")?;
                let files = pending.files;
                let manager_for_task = manager.clone();
                let task_id_for_task = task_id.clone();
                let import_result = tauri::async_runtime::spawn_blocking(move || {
                    materialize_file_import(
                        files,
                        manager_for_task.as_ref(),
                        task_id_for_task.as_deref(),
                    )
                })
                .await?;
                match import_result {
                    Ok(total_bytes) => {
                        item.size = Some(i64::try_from(total_bytes).unwrap_or(i64::MAX));
                        sqlx::query("UPDATE clipboard_items SET size = ? WHERE id = ?")
                            .bind(item.size)
                            .bind(&item.id)
                            .execute(&pool)
                            .await
                            .context("update imported shared file size")?;
                        let delivery_route = shared_delivery_route(
                            &item,
                            app.state::<crate::settings::SettingsStore>()
                                .snapshot()
                                .sync
                                .auto_upload_max_mb,
                            manager
                                .as_ref()
                                .is_some_and(|manager| manager.has_connected_lan_peer()),
                        );
                        let canceled = manager.as_ref().zip(task_id.as_deref()).is_some_and(
                            |(manager, task_id)| manager.shared_file_import_canceled(task_id),
                        );
                        if canceled {
                            remove_import_placeholder(app, &pool, &item.id, &pending.directory)
                                .await;
                            outcome.skipped += 1;
                        } else {
                            if let Some((manager, task_id)) =
                                manager.as_ref().zip(task_id.as_deref())
                            {
                                manager.finish_shared_file_import(task_id, total_bytes);
                            }
                            match crate::db::sync_queue::stage_existing_content(
                                &pool,
                                &item.id,
                                &item.sync_key,
                                &item.source_revision,
                                crate::db::sync_queue::ContentSyncOptions {
                                    manual_share: true,
                                    delivery_route,
                                    ..Default::default()
                                },
                            )
                            .await
                            {
                                Ok(operation_order) => {
                                    if let Some((manager, task_id)) =
                                        manager.as_ref().zip(task_id.as_deref())
                                    {
                                        manager.bind_shared_file_import(
                                            task_id,
                                            operation_order,
                                            item.source_revision.clone(),
                                        );
                                    }
                                    let canceled = manager
                                        .as_ref()
                                        .zip(task_id.as_deref())
                                        .is_some_and(|(manager, task_id)| {
                                            manager.shared_file_import_canceled(task_id)
                                        });
                                    if canceled {
                                        crate::db::sync_queue::complete_content(
                                            &pool,
                                            operation_order,
                                        )
                                        .await?;
                                        outcome.saved += 1;
                                    } else {
                                        crate::sync::notify_local_item_ready(app);
                                        outcome.saved += 1;
                                    }
                                }
                                Err(error) => {
                                    if let Some((manager, task_id)) =
                                        manager.as_ref().zip(task_id.as_deref())
                                    {
                                        manager.fail_shared_file_import(
                                            task_id,
                                            total_bytes,
                                            error.to_string(),
                                        );
                                    }
                                    remove_import_placeholder(
                                        app,
                                        &pool,
                                        &item.id,
                                        &pending.directory,
                                    )
                                    .await;
                                    outcome.failed += 1;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let canceled = manager.as_ref().zip(task_id.as_deref()).is_some_and(
                            |(manager, task_id)| manager.shared_file_import_canceled(task_id),
                        );
                        if !canceled {
                            if let Some((manager, task_id)) =
                                manager.as_ref().zip(task_id.as_deref())
                            {
                                manager.fail_shared_file_import(task_id, 0, error.to_string());
                            }
                            outcome.failed += 1;
                            log::warn!("import Android shared files failed: {error:#}");
                        } else {
                            outcome.skipped += 1;
                        }
                        remove_import_placeholder(app, &pool, &item.id, &pending.directory).await;
                    }
                }
            }
            Err(error) => {
                if let Some((manager, task_id)) = manager.as_ref().zip(task_id.as_deref()) {
                    manager.fail_shared_file_import(task_id, 0, error.to_string());
                }
                outcome.failed += 1;
                let _ = fs::remove_dir_all(&pending.directory);
                log::error!("persist Android shared file placeholder failed: {error:#}");
            }
        }
    }
    if outcome.failed > 0 {
        if let Err(error) = super::shared_files::cleanup_locked(&pool, owned_root).await {
            log::warn!("reclaim failed share attachments failed: {error:#}");
        }
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::{ClipboardKind, ClipboardSubKind};

    #[test]
    fn imports_mixed_share_and_preserves_owned_files_after_staging_is_removed() {
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("cache");
        fs::create_dir(&staging).unwrap();
        let document = staging.join("document");
        fs::write(&document, b"%PDF-test").unwrap();
        let image = staging.join("image");
        image::DynamicImage::new_rgb8(2, 3)
            .save_with_format(&image, image::ImageFormat::Png)
            .unwrap();
        let store = ImageStore::for_test(temp.path().join("images"));
        let delivery = ShareDelivery {
            texts: vec!["https://example.com".to_owned()],
            files: vec![
                SharedFile {
                    path: image,
                    fd: None,
                    name: "photo.png".to_owned(),
                    mime: "image/png".to_owned(),
                    size: None,
                },
                SharedFile {
                    path: document,
                    fd: None,
                    name: "report.pdf".to_owned(),
                    mime: "application/pdf".to_owned(),
                    size: None,
                },
                SharedFile {
                    path: staging.join("missing"),
                    fd: None,
                    name: "missing".to_owned(),
                    mime: "video/mp4".to_owned(),
                    size: None,
                },
            ],
            ..Default::default()
        };
        let (items, outcome) =
            prepare_items(delivery, &store, &staging, &temp.path().join("owned"));
        assert_eq!(outcome.saved, 0);
        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.skipped, 0);
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].sub_kind, Some(ClipboardSubKind::Url));
        assert_eq!(items[1].kind, ClipboardKind::Image);
        assert_eq!((items[1].width, items[1].height), (Some(2), Some(3)));
        assert_eq!(items[2].kind, ClipboardKind::Files);
        fs::remove_dir_all(staging).unwrap();
        assert_eq!(fs::read(&items[2].content).unwrap(), b"%PDF-test");
        assert_eq!(
            Path::new(&items[2].content).file_name().unwrap(),
            "report.pdf"
        );
    }

    #[test]
    fn retains_unsupported_image_bytes_and_deduplicates_repeated_attachments() {
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("cache");
        fs::create_dir(&staging).unwrap();
        let path = staging.join("image");
        fs::write(&path, b"GIF89a-original-animation").unwrap();
        let file = SharedFile {
            path,
            fd: None,
            name: "animation.gif".to_owned(),
            mime: "image/gif".to_owned(),
            size: None,
        };
        let owned = temp.path().join("owned");
        let first = attachment_payload(&file, &staging, &owned).unwrap();
        assert_eq!(first, attachment_payload(&file, &staging, &owned).unwrap());
        let ClipboardPayload::Files(paths) = first else {
            panic!("animation must remain a file")
        };
        assert_eq!(fs::read(&paths[0]).unwrap(), b"GIF89a-original-animation");
    }

    #[test]
    fn preserves_apng_animation_as_an_owned_original_file() {
        use image::AnimationDecoder;

        let bytes = include_bytes!("fixtures/share-animation.png");
        let png = image::codecs::png::PngDecoder::new(Cursor::new(bytes)).unwrap();
        assert!(png.is_apng().unwrap());
        let frames = png.apng().unwrap().into_frames().collect_frames().unwrap();
        assert_eq!(frames.len(), 2);
        assert_ne!(frames[0].buffer(), frames[1].buffer());

        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("cache");
        fs::create_dir(&staging).unwrap();
        let path = staging.join("0");
        fs::write(&path, bytes).unwrap();
        let owned = temp.path().join("owned");
        for mime in ["image/png", "image/apng"] {
            let file = SharedFile {
                path: path.clone(),
                fd: None,
                name: "animation.png".to_owned(),
                mime: mime.to_owned(),
                size: None,
            };
            let payload = attachment_payload(&file, &staging, &owned).unwrap();
            let ClipboardPayload::Files(paths) = payload else {
                panic!("APNG must retain all animation frames")
            };
            assert_eq!(fs::read(&paths[0]).unwrap(), bytes);
            assert_eq!(Path::new(&paths[0]).extension().unwrap(), "png");
        }
    }

    #[test]
    fn long_unicode_attachment_names_keep_their_extension() {
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("cache");
        fs::create_dir(&staging).unwrap();
        let path = staging.join("0");
        fs::write(&path, b"%PDF-original").unwrap();
        let file = SharedFile {
            path,
            fd: None,
            name: format!("{}.pdf", "文".repeat(68)),
            mime: "application/pdf".to_owned(),
            size: None,
        };
        let payload = attachment_payload(&file, &staging, &temp.path().join("owned")).unwrap();
        let ClipboardPayload::Files(paths) = payload else {
            panic!("document must remain a file")
        };
        let name = Path::new(&paths[0]).file_name().unwrap().to_str().unwrap();
        assert!(name.len() <= MAX_FILE_NAME_BYTES);
        assert!(name.ends_with(".pdf"));
        assert!(name.starts_with('文'));
        assert_eq!(fs::read(&paths[0]).unwrap(), b"%PDF-original");
    }

    #[test]
    fn unusual_long_names_stay_within_the_byte_limit() {
        for name in [
            "文".repeat(100),
            format!(".{}", "文".repeat(100)),
            format!("文.{}", "x".repeat(198)),
            format!("文.{}", "x".repeat(240)),
        ] {
            let safe = safe_file_name(&name);
            assert!(!safe.is_empty());
            assert!(safe.len() <= MAX_FILE_NAME_BYTES);
        }
        assert!(safe_file_name(&format!("文.{}", "x".repeat(198)))
            .ends_with(&format!(".{}", "x".repeat(198))));
    }

    #[test]
    fn rejects_paths_outside_the_private_staging_directory() {
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("cache");
        fs::create_dir(&staging).unwrap();
        let path = temp.path().join("private");
        fs::write(&path, b"private data").unwrap();
        let file = SharedFile {
            path,
            fd: None,
            name: "../../escape\n.txt".to_owned(),
            mime: "application/octet-stream".to_owned(),
            size: None,
        };
        assert!(attachment_payload(&file, &staging, &temp.path().join("owned")).is_err());
        assert_eq!(safe_file_name("../../escape\n.txt"), ".._.._escape_.txt");
        assert_eq!(safe_file_name(".env"), ".env");
    }

    #[test]
    fn groups_files_without_overwriting_identical_names() {
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("cache");
        fs::create_dir(&staging).unwrap();
        let files = [b"first".as_slice(), b"second".as_slice()]
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                let path = staging.join(index.to_string());
                fs::write(&path, bytes).unwrap();
                SharedFile {
                    path,
                    fd: None,
                    name: "document.txt".to_owned(),
                    mime: "text/plain".to_owned(),
                    size: None,
                }
            })
            .collect();
        let delivery = ShareDelivery {
            files,
            ..Default::default()
        };
        let store = ImageStore::for_test(temp.path().join("images"));
        let (items, outcome) =
            prepare_items(delivery, &store, &staging, &temp.path().join("owned"));
        assert_eq!(outcome.failed, 0);
        assert_eq!(items.len(), 1);
        let paths = items[0].content.lines().collect::<Vec<_>>();
        assert_eq!(paths.len(), 2);
        assert_ne!(paths[0], paths[1]);
        assert_eq!(fs::read(paths[0]).unwrap(), b"first");
        assert_eq!(fs::read(paths[1]).unwrap(), b"second");
        assert_eq!(items[0].size, Some(11));
    }

    #[test]
    fn accepts_secrets_and_large_text_without_automatic_capture_settings() {
        let temp = tempfile::tempdir().unwrap();
        let store = ImageStore::for_test(temp.path().join("images"));
        let delivery = ShareDelivery {
            texts: vec![
                "ordinary text".to_owned(),
                "-----BEGIN PRIVATE KEY-----".to_owned(),
                "x".repeat(4 * 1024 * 1024 + 1),
            ],
            failed: 1,
            ..Default::default()
        };
        let (items, outcome) =
            prepare_items(delivery, &store, temp.path(), &temp.path().join("owned"));
        assert_eq!(items.len(), 3);
        assert!(items[1].is_sensitive);
        assert_eq!(items[2].content.len(), 4 * 1024 * 1024 + 1);
        assert_eq!(outcome.skipped, 0);
        assert_eq!(outcome.failed, 1);
        assert_eq!(
            crate::i18n::commands::share_result(
                crate::settings::Language::ZhCN,
                items.len(),
                outcome.skipped,
                outcome.failed
            ),
            "已收录并加入同步队列 3 条记录，跳过 0 条，失败 1 项"
        );
    }

    #[test]
    fn preserves_html_representation_for_explicit_shares() {
        let temp = tempfile::tempdir().unwrap();
        let store = ImageStore::for_test(temp.path().join("images"));
        let html = "<p><strong>Shared content</strong></p>";
        let delivery = ShareDelivery {
            texts: vec!["Shared content".to_owned()],
            html: Some(html.to_owned()),
            ..Default::default()
        };
        let (items, outcome) = prepare_items(delivery, &store, temp.path(), temp.path());
        assert_eq!(outcome.skipped + outcome.failed, 0);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].sub_kind, Some(ClipboardSubKind::Html));
        assert_eq!(items[0].content, html);
    }
}
