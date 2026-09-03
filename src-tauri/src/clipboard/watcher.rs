//! OS 级剪贴板监听：把 [`clipboard_rs`] 的 watcher 接到「读取 → 去重入库 → emit」闭环。
//!
//! [`clipboard_rs`] 内部已实现 macOS（`NSPasteboard.changeCount` 轮询）/ Windows
//! （`AddClipboardFormatListener` → `WM_CLIPBOARDUPDATE`）的平台监听，这里不重复造。
//!
//! 线程模型：`ClipboardWatcherContext::start_watch()` 是阻塞调用，故整个监听跑在独立
//! `std::thread` 上。`ClipboardContext` 等平台句柄**在该线程内构造**，不跨线程移动，
//! 从而绕开其 `Send` 约束；只有 `Send` 的数据（`AppHandle`、`item`）会被
//! 投递进 Tauri 异步运行时做 sqlx 入库与事件 emit。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(not(target_os = "android"))]
use std::time::Duration;

use chrono::Utc;
#[cfg(not(target_os = "android"))]
use clipboard_rs::{ClipboardHandler, ClipboardWatcher, ClipboardWatcherContext};
use serde_json::json;
use sqlx::SqlitePool;
use tauri::{AppHandle, Emitter, Manager};

use super::app_store::AppIconStore;
use super::apps_registry::AppsRegistry;
use super::fingerprint::{ClipboardFingerprint, ClipboardFingerprintState, ClipboardObservation};
use super::guard::WritebackGuard;
use super::ingest::{build_item_with_settings, calculate_files_total_size};
#[cfg(not(target_os = "android"))]
use super::read::ClipboardReader;
use super::sound;
#[cfg(not(target_os = "android"))]
use super::source;
use super::source::FrontmostApp;
use super::storage::ImageStore;
use crate::db::apps::upsert_app;
use crate::db::items::{file_items_missing_size, fill_file_item_size, upsert_item, UpsertResult};
use crate::db::models::{ClipboardApp, ClipboardItem, ClipboardKind};
use crate::settings::SettingsStore;

/// 剪贴板更新事件名。前端监听此事件后增量刷新 / 重新拉取列表。
pub const CLIPBOARD_UPDATED_EVENT: &str = "clipboard://updated";

/// macOS 轮询 `changeCount` 的间隔。上游 clipboard-rs 默认 500ms，对复制响应（尤其图片）
/// 偏慢；我们 fork 出 `new_with_interval` 后调到 120ms，跟手且 CPU 开销可忽略。
/// Windows 走事件驱动（`WM_CLIPBOARDUPDATE`），此值被忽略。
#[cfg(not(target_os = "android"))]
const CLIPBOARD_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(120);

/// Another clipboard listener can briefly hold the Windows clipboard open. Retry those read
/// failures within a bounded window before dropping the update.
#[cfg(not(target_os = "android"))]
const CLIPBOARD_READ_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(15),
    Duration::from_millis(35),
    Duration::from_millis(75),
];

#[cfg(not(target_os = "android"))]
fn read_with_retry<T, E>(
    retry_delays: &[Duration],
    mut read: impl FnMut() -> Result<Option<T>, E>,
) -> Result<Option<T>, E> {
    let mut result = read();
    for delay in retry_delays {
        if result.is_ok() {
            return result;
        }
        std::thread::sleep(*delay);
        result = read();
    }
    result
}

/// 监听暂停开关。托盘菜单「停止监听」翻转，handler 早返回跳过整条入库链路。
/// 用 `Arc<AtomicBool>` 跨线程共享；不停 watcher 线程本身，避免反复重建平台句柄。
#[derive(Debug, Default, Clone)]
pub struct WatcherPause(Arc<AtomicBool>);

impl WatcherPause {
    pub fn is_paused(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn set_paused(&self, paused: bool) {
        self.0.store(paused, Ordering::Relaxed);
    }
}

/// 把同步抓到的 [`FrontmostApp`] 落 icon 字节 + 拼成可入库的 [`ClipboardApp`]。
/// icon 落盘失败不阻断（仍保留应用名），仅 warn。
///
/// `registry` 命中完整缓存时优先复用；旧缓存缺图标时仍用新采集的 PNG 补齐，
/// 并把结果回写缓存，让首次见到的应用后续直接命中。
pub fn materialize_source(
    store: &AppIconStore,
    registry: Option<&AppsRegistry>,
    src: FrontmostApp,
) -> ClipboardApp {
    let cached = registry.and_then(|value| value.get(&src.id));
    if let Some(mut cached) = cached.clone() {
        let mut icon_is_current = cached
            .icon_file
            .as_deref()
            .is_some_and(|file_name| store.is_current_format(file_name));
        let metadata_missing = cached.icon_hash.is_none()
            || cached.accent_start.is_none()
            || cached.accent_end.is_none();
        if metadata_missing && (icon_is_current || src.icon_png.is_none()) {
            if let Some(icon_file) = cached.icon_file.as_deref() {
                match store.refresh_metadata(icon_file) {
                    Ok(metadata) => {
                        cached.icon_file = Some(metadata.file_name);
                        cached.icon_hash = Some(metadata.icon_hash);
                        cached.accent_start = Some(metadata.accent_start);
                        cached.accent_end = Some(metadata.accent_end);
                        icon_is_current = true;
                    }
                    Err(error) => {
                        log::warn!("refresh cached app icon for {} failed: {error}", src.id);
                    }
                }
            }
        }
        let metadata_complete = cached.icon_file.is_some()
            && cached.icon_hash.is_some()
            && cached.accent_start.is_some()
            && cached.accent_end.is_some()
            && icon_is_current;
        if metadata_complete || src.icon_png.is_none() {
            if cached.name != src.name {
                cached.name = src.name;
                cached.updated_at = Utc::now();
            }
            if let Some(registry) = registry {
                registry.insert_into_cache(cached.clone());
            }
            return cached;
        }
    }

    let icon = src
        .icon_png
        .as_deref()
        .and_then(|bytes| match store.store_with_metadata(bytes) {
            Ok(icon) => Some(icon),
            Err(err) => {
                log::warn!("app icon store failed for {}: {err}", src.id);
                None
            }
        });
    let now = Utc::now();
    let app = ClipboardApp {
        id: src.id,
        name: src.name,
        icon_file: icon.as_ref().map(|value| value.file_name.clone()),
        icon_hash: icon.as_ref().map(|value| value.icon_hash.clone()),
        accent_start: icon.as_ref().map(|value| value.accent_start.clone()),
        accent_end: icon.as_ref().map(|value| value.accent_end.clone()),
        platform: src.platform,
        created_at: cached.as_ref().map_or(now, |value| value.created_at),
        updated_at: now,
    };
    if let Some(reg) = registry {
        reg.insert_into_cache(app.clone());
    }
    app
}

/// 去重入库 + emit「剪贴板更新」事件。监听回调与 `read_clipboard` 命令共用，
/// 保证两条路径的入库语义与事件契约一致。失败仅记日志（监听场景无人接收 Result）。
///
/// `source_app` 为 `Some` 时先 upsert apps 表再写 item，满足 FK 约束。
/// 应用 upsert 失败不阻断条目入库——清掉 source_app_id 后继续，避免单次系统调用抽风丢内容。
pub async fn persist_and_notify(
    app: &AppHandle,
    pool: &SqlitePool,
    item: &ClipboardItem,
    source_app: Option<&ClipboardApp>,
    observation: Option<ClipboardObservation>,
) -> crate::core::Result<UpsertResult> {
    let mut item_to_write = item.clone();
    if let Some(src) = source_app {
        match upsert_app(pool, src).await {
            Ok(()) => {}
            Err(err) => {
                log::warn!("clipboard source app upsert failed ({}): {err}", src.id);
                item_to_write.source_app_id = None;
            }
        }
    }
    let result = upsert_item(pool, &item_to_write).await?;
    sound::maybe_play_copy(app);
    notify_clipboard_updated(app, &result.id, item_to_write.kind, result.deduplicated);
    item_to_write.id = result.id.clone();
    if item_to_write.kind == ClipboardKind::Files && item_to_write.size.is_none() {
        resolve_file_size_and_enqueue(app, pool, item_to_write, observation);
    } else {
        crate::sync::enqueue_local_item(app, item_to_write, observation);
    }
    Ok(result)
}

/// 发出统一的记录刷新通知，并同步唤醒 Android 原生上滑面板。
fn notify_clipboard_updated(
    app: &AppHandle,
    item_id: &str,
    kind: ClipboardKind,
    deduplicated: bool,
) {
    if let Err(err) = app.emit(
        CLIPBOARD_UPDATED_EVENT,
        json!({
            "id": item_id,
            "kind": kind,
            "deduplicated": deduplicated,
        }),
    ) {
        log::warn!("emit {CLIPBOARD_UPDATED_EVENT} failed: {err}");
    }
    #[cfg(target_os = "android")]
    crate::commands::android::notify_overlay_clipboard_changed();
}

/// 目录递归统计在阻塞线程池执行；完成前记录已可见，但不会用未知大小启动自动同步。
fn resolve_file_size_and_enqueue(
    app: &AppHandle,
    pool: &SqlitePool,
    mut item: ClipboardItem,
    observation: Option<ClipboardObservation>,
) {
    let app = app.clone();
    let pool = pool.clone();
    let content = item.content.clone();
    tauri::async_runtime::spawn(async move {
        let size = match tauri::async_runtime::spawn_blocking(move || {
            calculate_files_total_size(&content)
        })
        .await
        {
            Ok(Ok(size)) => size,
            Ok(Err(error)) => {
                log::warn!("calculate clipboard file size failed: {error:#}");
                crate::sync::enqueue_local_item(&app, item, observation);
                return;
            }
            Err(error) => {
                log::warn!("clipboard file size task failed: {error}");
                crate::sync::enqueue_local_item(&app, item, observation);
                return;
            }
        };
        item.size = Some(size);
        match fill_file_item_size(&pool, &item.id, &item.source_revision, size).await {
            Ok(true) => {
                notify_clipboard_updated(&app, &item.id, item.kind, true);
                crate::sync::enqueue_local_item(&app, item, observation);
            }
            Ok(false) => {}
            Err(error) => {
                log::warn!("persist clipboard file size failed: {error:#}");
                crate::sync::enqueue_local_item(&app, item, observation);
            }
        }
    });
}

/// 顺序回填升级前的文件记录；只更新展示元数据，不产生新的同步事件。
fn spawn_file_size_backfill(app: AppHandle) {
    tauri::async_runtime::spawn(async move {
        let pool = app.state::<crate::db::DatabaseState>().pool().await;
        let items = match file_items_missing_size(&pool).await {
            Ok(items) => items,
            Err(error) => {
                log::warn!("load clipboard file size backfill failed: {error:#}");
                return;
            }
        };
        let candidate_count = items.len();
        let mut updated_count = 0_usize;
        for item in items {
            let content = item.content.clone();
            let size = match tauri::async_runtime::spawn_blocking(move || {
                calculate_files_total_size(&content)
            })
            .await
            {
                Ok(Ok(size)) => size,
                Ok(Err(error)) => {
                    log::debug!(
                        "skip clipboard file size backfill for {}: {error:#}",
                        item.id
                    );
                    continue;
                }
                Err(error) => {
                    log::warn!("clipboard file size backfill task failed: {error}");
                    continue;
                }
            };
            match fill_file_item_size(&pool, &item.id, &item.source_revision, size).await {
                Ok(true) => updated_count += 1,
                Ok(false) => {}
                Err(error) => {
                    log::warn!("persist clipboard file size backfill failed: {error:#}");
                }
            }
        }
        if updated_count > 0 {
            if let Err(error) = app.emit(
                CLIPBOARD_UPDATED_EVENT,
                json!({
                    "reconciled": true,
                }),
            ) {
                log::warn!("emit clipboard file size backfill update failed: {error}");
            }
            #[cfg(target_os = "android")]
            crate::commands::android::notify_overlay_clipboard_changed();
        }
        log::debug!(
            "clipboard file size backfill completed: candidates={candidate_count} updated={updated_count}"
        );
    });
}

/// 启动监听：注册 [`WritebackGuard`] / [`ImageStore`] / [`AppIconStore`] 到 Tauri `State`
/// （供写回时打标记 / 取图 / 取来源应用图标），并在独立线程上跑 OS 级监听。
/// 应在 `setup` 中、连接池就绪后调用一次。store 创建失败属致命配置错误，直接返回错误。
pub fn init(app: &AppHandle) -> crate::core::Result<()> {
    let guard = Arc::new(WritebackGuard::new());
    app.manage(guard.clone());

    let fingerprints = Arc::new(ClipboardFingerprintState::new());
    app.manage(fingerprints.clone());

    let store = ImageStore::new(app)?;
    app.manage(store.clone());

    let app_icon_store = AppIconStore::new(app)?;
    app.manage(app_icon_store.clone());

    let file_icon_store = super::FileIconStore::new(app)?;
    app.manage(file_icon_store);

    let registry = AppsRegistry::new(app.clone(), app_icon_store.clone());
    app.manage(registry.clone());

    let pause = WatcherPause::default();
    app.manage(pause.clone());

    // 启动期只把已落库的应用读进缓存；运行中应用由偏好页打开/刷新时补齐。
    {
        let registry = registry.clone();
        let excluded_app_ids = app
            .try_state::<SettingsStore>()
            .map(|store| store.snapshot().clipboard.filters.excluded_app_ids)
            .unwrap_or_default();
        tauri::async_runtime::spawn(async move {
            if let Err(err) = registry.load_from_db().await {
                log::warn!("apps registry: initial DB load failed: {err}");
            }
            if let Err(err) =
                super::apps_registry::add_apps_from_ids(registry.clone(), excluded_app_ids).await
            {
                log::warn!("apps registry: initial excluded app materialization failed: {err}");
            }
        });
    }

    spawn_file_size_backfill(app.clone());
    super::cleanup::spawn(app.clone());
    #[cfg(not(target_os = "android"))]
    spawn_watch_thread(
        app.clone(),
        guard,
        fingerprints,
        store,
        app_icon_store,
        registry,
        pause,
    );
    Ok(())
}

/// Receives one deduplicated native Android clipboard-change event.
#[cfg(target_os = "android")]
pub fn capture_android_text(app: &AppHandle, text: String, source: Option<FrontmostApp>) {
    let app = app.clone();
    let guard = app.state::<Arc<WritebackGuard>>().inner().clone();
    let store = app.state::<ImageStore>().inner().clone();
    let pause = app.state::<WatcherPause>().inner().clone();
    tauri::async_runtime::spawn(async move {
        persist_android_text(&app, &guard, &store, &pause, text, source).await;
    });
}

#[cfg(target_os = "android")]
async fn persist_android_text(
    app: &AppHandle,
    guard: &WritebackGuard,
    store: &ImageStore,
    pause: &WatcherPause,
    text: String,
    source: Option<FrontmostApp>,
) {
    use super::payload::{ClipboardPayload, TextPayload};

    let fingerprints = app.state::<Arc<ClipboardFingerprintState>>();
    let observation = fingerprints.begin_observation();
    if pause.is_paused() || text.trim().is_empty() {
        return;
    }
    let settings = app
        .try_state::<SettingsStore>()
        .map(|store| store.snapshot())
        .unwrap_or_default();
    let payload = ClipboardPayload::Text(TextPayload {
        text,
        html: None,
        rtf: None,
    });
    let mut item = match build_item_with_settings(
        store,
        &payload,
        &settings.clipboard.capture,
        &settings.clipboard.sensitive,
        settings.clipboard.content.copy_plain,
    ) {
        Ok(Some(item)) => item,
        _ => return,
    };
    if guard.should_skip(&item.content_hash) {
        if let Some(fingerprint) = ClipboardFingerprint::from_text_item(&item) {
            fingerprints.commit_observation(&observation, fingerprint);
        }
        return;
    }
    if let Some(fingerprint) = ClipboardFingerprint::from_text_item(&item) {
        fingerprints.commit_observation(&observation, fingerprint);
    }
    let source_app = source.map(|source| {
        materialize_source(
            &app.state::<AppIconStore>(),
            Some(&app.state::<AppsRegistry>()),
            source,
        )
    });
    if let Some(source) = source_app.as_ref() {
        item.source_app_id = Some(source.id.clone());
    }
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    if let Err(error) = persist_and_notify(app, &pool, &item, source_app.as_ref(), None).await {
        log::error!("Android clipboard capture persist failed: {error}");
    }
}

#[cfg(not(target_os = "android"))]
fn spawn_watch_thread(
    app: AppHandle,
    guard: Arc<WritebackGuard>,
    fingerprints: Arc<ClipboardFingerprintState>,
    store: ImageStore,
    app_icon_store: AppIconStore,
    registry: AppsRegistry,
    pause: WatcherPause,
) {
    std::thread::Builder::new()
        .name("clipboard-watcher".to_owned())
        .spawn(move || {
            // 平台剪贴板句柄在本线程内构造，不跨线程移动。
            let reader = match ClipboardReader::new() {
                Ok(reader) => reader,
                Err(err) => {
                    log::error!("clipboard watcher: failed to create reader: {err}");
                    return;
                }
            };

            let mut watcher =
                match ClipboardWatcherContext::new_with_interval(CLIPBOARD_POLL_INTERVAL) {
                    Ok(watcher) => watcher,
                    Err(err) => {
                        log::error!("clipboard watcher: failed to create watcher: {err}");
                        return;
                    }
                };

            watcher.add_handler(ClipboardChangeHandler {
                reader,
                app,
                guard,
                fingerprints,
                store,
                app_icon_store,
                registry,
                pause,
            });

            log::info!("clipboard watcher started");
            // 阻塞直至进程退出。
            watcher.start_watch();
        })
        .expect("failed to spawn clipboard watcher thread");
}

#[cfg(not(target_os = "android"))]
struct ClipboardChangeHandler {
    reader: ClipboardReader,
    app: AppHandle,
    guard: Arc<WritebackGuard>,
    fingerprints: Arc<ClipboardFingerprintState>,
    store: ImageStore,
    app_icon_store: AppIconStore,
    registry: AppsRegistry,
    pause: WatcherPause,
}

#[cfg(not(target_os = "android"))]
impl ClipboardHandler for ClipboardChangeHandler {
    fn on_clipboard_change(&mut self) {
        let observation = self.fingerprints.begin_observation();
        // 用户从托盘关掉「监听」时直接早退，不读取、不入库、不 emit。
        if self.pause.is_paused() {
            return;
        }

        // **先**抓前台应用：等异步入库再问，前台早就切回我们自己了。
        // 自身写回的事件会在下方 guard 处被丢弃，但 detect 仍会无害地返回我们自己的 bundle id——
        // 顺序换不得：guard 判定依赖 content_hash，必须先把 payload 读出来才能判，
        // 而 read_all 期间用户可能已经切走前台。
        let source = source::detect_frontmost();

        // 用户在偏好里勾选了「过滤此应用」时，本次复制整条直接丢弃——不读取、不入库、不 emit。
        // 提前到读 payload 前判定，省掉无效的 OS 调用 + 图片解码开销。
        if let Some(src) = &source {
            let excluded = self
                .app
                .try_state::<SettingsStore>()
                .map(|s| {
                    s.snapshot()
                        .clipboard
                        .filters
                        .excluded_app_ids
                        .iter()
                        .any(|id| id == &src.id)
                })
                .unwrap_or(false);
            if excluded {
                return;
            }
        }

        let settings = self
            .app
            .try_state::<SettingsStore>()
            .map(|s| s.snapshot())
            .unwrap_or_default();

        // 同步读取 + 转换（含图片落盘）：拿到 content_hash 才能判定是否为自身写回。
        let payload = match read_with_retry(&CLIPBOARD_READ_RETRY_DELAYS, || {
            self.reader.read_with_capture(&settings.clipboard.capture)
        }) {
            Ok(Some(payload)) => payload,
            Ok(None) => return,
            Err(err) => {
                log::warn!("clipboard watcher: read failed: {err}");
                return;
            }
        };
        let image_fingerprint = match &payload {
            super::payload::ClipboardPayload::Image(image) => {
                ClipboardFingerprint::from_image_bytes(&image.bytes)
            }
            _ => None,
        };

        let mut item = match build_item_with_settings(
            &self.store,
            &payload,
            &settings.clipboard.capture,
            &settings.clipboard.sensitive,
            settings.clipboard.content.copy_plain,
        ) {
            Ok(Some(item)) => item,
            Ok(None) => return,
            Err(err) => {
                log::warn!("clipboard watcher: build item failed: {err}");
                return;
            }
        };

        // 自身写回触发的变更：跳过入库，避免回环。
        let suppression = image_fingerprint
            .as_ref()
            .map(ClipboardFingerprint::as_str)
            .unwrap_or(&item.content_hash);
        if self.guard.should_skip(suppression) {
            let fingerprint =
                image_fingerprint.or_else(|| ClipboardFingerprint::from_text_item(&item));
            if let Some(fingerprint) = fingerprint {
                self.fingerprints
                    .commit_observation(&observation, fingerprint);
            } else {
                self.fingerprints.restore_observation(observation);
            }
            return;
        }

        let sync_observation = if item.kind == ClipboardKind::Files {
            Some(observation)
        } else {
            let fingerprint =
                image_fingerprint.or_else(|| ClipboardFingerprint::from_text_item(&item));
            if let Some(fingerprint) = fingerprint {
                self.fingerprints
                    .commit_observation(&observation, fingerprint);
            }
            None
        };

        let source_app =
            source.map(|src| materialize_source(&self.app_icon_store, Some(&self.registry), src));
        if let Some(src) = &source_app {
            item.source_app_id = Some(src.id.clone());
        }

        // 入库与 emit 交给异步运行时；只移动 Send 数据，不碰平台句柄。
        let app = self.app.clone();
        tauri::async_runtime::spawn(async move {
            let pool = app.state::<crate::db::DatabaseState>().pool().await;
            if let Err(err) =
                persist_and_notify(&app, &pool, &item, source_app.as_ref(), sync_observation).await
            {
                log::error!("clipboard watcher: persist failed: {err}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use clipboard_rs::{Clipboard, ClipboardContext};

    use super::*;
    use crate::clipboard::{build_item, ImageStore, WritebackGuard};
    use crate::db::items::find_item_by_id;
    use crate::db::test_support::memory_pool;

    const ZERO_DELAY_RETRIES: [Duration; 3] = [Duration::ZERO; 3];

    #[test]
    fn clipboard_read_retry_returns_immediate_success() {
        let attempts = Cell::new(0);

        let result = read_with_retry(&ZERO_DELAY_RETRIES, || {
            attempts.set(attempts.get() + 1);
            Ok::<_, &'static str>(Some("captured"))
        });

        assert_eq!(result, Ok(Some("captured")));
        assert_eq!(attempts.get(), 1);
    }

    #[test]
    fn clipboard_read_retry_recovers_after_transient_error() {
        let attempts = Cell::new(0);

        let result = read_with_retry(&ZERO_DELAY_RETRIES, || {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                Err("clipboard busy")
            } else {
                Ok(Some("captured"))
            }
        });

        assert_eq!(result, Ok(Some("captured")));
        assert_eq!(attempts.get(), 2);
    }

    #[test]
    fn clipboard_read_retry_does_not_retry_empty_content() {
        let attempts = Cell::new(0);

        let result = read_with_retry(&ZERO_DELAY_RETRIES, || {
            attempts.set(attempts.get() + 1);
            Ok::<Option<&'static str>, &'static str>(None)
        });

        assert_eq!(result, Ok(None));
        assert_eq!(attempts.get(), 1);
    }

    #[test]
    fn clipboard_read_retry_returns_final_error_after_exhaustion() {
        let attempts = Cell::new(0);

        let result = read_with_retry(&ZERO_DELAY_RETRIES, || {
            attempts.set(attempts.get() + 1);
            Err::<Option<&'static str>, _>(attempts.get())
        });

        assert_eq!(result, Err(4));
        assert_eq!(attempts.get(), 4);
    }

    fn temp_image_store() -> (TempDir, ImageStore) {
        let dir = TempDir::new();
        let store = ImageStore::for_test(dir.path().join("resources").join("clipboard-images"));
        (dir, store)
    }

    // 复刻 on_clipboard_change 的同步部分（读取 → 转换 → 去重判定）+ async 入库，
    // 但绕开 Tauri AppHandle / emit（无法在单测里构造），验证整条数据链路。
    // 触碰真实系统剪贴板，默认 ignore；本机用 `cargo test -- --ignored` 验证。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "touches the real system clipboard; run with --ignored on a desktop session"]
    async fn end_to_end_text_ingests_once_then_dedups() {
        let pool = memory_pool().await;
        let guard = WritebackGuard::new();
        let (_dir, store) = temp_image_store();

        // 串行锁只覆盖触碰真实剪贴板的同步段，await DB 前即释放（不跨 await 持锁）。
        let item = {
            let _serial = crate::clipboard::test_lock::serial();
            let ctx = ClipboardContext::new().unwrap();
            ctx.set_text("e2e ecopaste watcher".to_owned()).unwrap();

            let reader = ClipboardReader::new().unwrap();
            let payload = reader
                .read_with_capture(&crate::settings::Capture::default())
                .unwrap()
                .expect("should read text");
            build_item(&store, &payload)
                .unwrap()
                .expect("should map to item")
        };
        assert!(!guard.should_skip(&item.content_hash));

        // 首次入库：新行。
        let first = upsert_item(&pool, &item).await.unwrap();
        assert!(!first.deduplicated);
        assert_eq!(
            find_item_by_id(&pool, &first.id)
                .await
                .unwrap()
                .unwrap()
                .content,
            "e2e ecopaste watcher"
        );

        // 同内容再来一次：命中去重，use_count 累加，不新增行。
        let second = upsert_item(&pool, &item).await.unwrap();
        assert!(second.deduplicated);
        assert_eq!(first.id, second.id);
        assert_eq!(
            find_item_by_id(&pool, &first.id)
                .await
                .unwrap()
                .unwrap()
                .use_count,
            2
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "touches the real system clipboard; run with --ignored on a desktop session"]
    async fn writeback_guard_suppresses_self_copy() {
        let (_dir, store) = temp_image_store();
        let _serial = crate::clipboard::test_lock::serial();
        let ctx = ClipboardContext::new().unwrap();
        ctx.set_text("self writeback content".to_owned()).unwrap();

        let reader = ClipboardReader::new().unwrap();
        let payload = reader
            .read_with_capture(&crate::settings::Capture::default())
            .unwrap()
            .unwrap();
        let item = build_item(&store, &payload).unwrap().unwrap();

        // 模拟写回前登记 → 监听读到同内容 → 被抑制。
        let guard = WritebackGuard::new();
        guard.suppress(item.content_hash.clone());
        assert!(guard.should_skip(&item.content_hash));
    }

    // 验证真实剪贴板图片链路：set_image（OS 原生 TIFF）→ read_with_capture 解码为 PNG →
    // build_item 落盘原图/缩略图 → upsert 入库。覆盖合成 PNG 测不到的 OS 解码段。
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "touches the real system clipboard; run with --ignored on a desktop session"]
    async fn end_to_end_image_stores_and_ingests() {
        use clipboard_rs::common::RustImage;

        let pool = memory_pool().await;
        let (_dir, store) = temp_image_store();

        let item = {
            let _serial = crate::clipboard::test_lock::serial();
            let png = {
                use std::io::Cursor;
                let buf = image::RgbaImage::from_pixel(40, 24, image::Rgba([7, 8, 9, 255]));
                let mut out = Cursor::new(Vec::new());
                image::DynamicImage::ImageRgba8(buf)
                    .write_to(&mut out, image::ImageFormat::Png)
                    .unwrap();
                out.into_inner()
            };
            let ctx = ClipboardContext::new().unwrap();
            ctx.set_image(clipboard_rs::RustImageData::from_bytes(&png).unwrap())
                .unwrap();

            let reader = ClipboardReader::new().unwrap();
            let payload = reader
                .read_with_capture(&crate::settings::Capture::default())
                .unwrap()
                .expect("should read image");
            build_item(&store, &payload)
                .unwrap()
                .expect("image should map to item")
        };

        assert_eq!(item.kind, crate::db::models::ClipboardKind::Image);
        assert!(item.content.ends_with(".png"));
        assert!(item.width.unwrap() > 0 && item.height.unwrap() > 0);
        // 复制热路径只落原图；缩略图懒生成，此刻尚未存在。
        assert!(store.origin_path(&item.content).exists());
        assert!(!store.thumbnail_path(&item.content).exists());
        // 模拟前端首次取图：按需生成缩略图。
        assert!(store.ensure_thumbnail(&item.content).unwrap().exists());

        let result = upsert_item(&pool, &item).await.unwrap();
        assert!(!result.deduplicated);
        assert_eq!(
            find_item_by_id(&pool, &result.id)
                .await
                .unwrap()
                .unwrap()
                .kind,
            crate::db::models::ClipboardKind::Image
        );
    }

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("ecopaste-watcher-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }
}
