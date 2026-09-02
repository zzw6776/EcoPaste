//! Android 平台原生能力命令：权限检测、系统设置跳转、悬浮手势启停、最小化与自动粘贴。

use serde::{Deserialize, Serialize};

use crate::core::Result;

#[cfg(target_os = "android")]
use tauri::{Emitter, Manager};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AndroidPermissionsStatus {
    pub overlay_granted: bool,
    pub accessibility_granted: bool,
    pub notification_granted: bool,
    pub battery_ignored: bool,
    pub root_available: bool,
    pub root_clipboard_granted: bool,
    pub overlay_service_running: bool,
    pub engine_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AndroidEngineResult {
    pub success: bool,
    pub mode: String,
    pub root_clipboard_granted: bool,
    pub message: String,
}

/// Result returned by the Android native file bridge without user-facing text.
#[derive(Debug, Clone, Deserialize)]
pub struct AndroidFileActionResult {
    pub status: String,
    pub message: String,
}

#[cfg(target_os = "android")]
static GLOBAL_VM: std::sync::OnceLock<jni::JavaVM> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
static GLOBAL_CONTEXT: std::sync::OnceLock<jni::objects::GlobalRef> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
static BRIDGE_CLASS: std::sync::OnceLock<jni::objects::GlobalRef> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
static IROH_ANDROID_CONTEXT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
static APP_HANDLE: std::sync::OnceLock<tauri::AppHandle> = std::sync::OnceLock::new();
#[cfg(any(target_os = "android", test))]
#[derive(Default)]
struct PendingAutomaticDeviceName {
    value: std::sync::Mutex<Option<String>>,
}

#[cfg(any(target_os = "android", test))]
impl PendingAutomaticDeviceName {
    fn replace(&self, name: String) {
        *self.value.lock().expect("pending device name poisoned") = Some(name);
    }

    fn take(&self) -> Option<String> {
        self.value
            .lock()
            .expect("pending device name poisoned")
            .take()
    }

    fn restore_if_empty(&self, name: String) {
        let mut pending = self.value.lock().expect("pending device name poisoned");
        if pending.is_none() {
            *pending = Some(name);
        }
    }
}

#[cfg(target_os = "android")]
static PENDING_AUTOMATIC_DEVICE_NAME: std::sync::LazyLock<PendingAutomaticDeviceName> =
    std::sync::LazyLock::new(PendingAutomaticDeviceName::default);

#[cfg(target_os = "android")]
pub fn set_app_handle(app_handle: tauri::AppHandle) {
    let _ = APP_HANDLE.set(app_handle);
    refresh_pending_automatic_device_name();
}

/// 暂存 Android 系统名称，并在 Rust 同步运行时就绪后可靠交付。
#[cfg(target_os = "android")]
fn enqueue_automatic_device_name(name: String) {
    PENDING_AUTOMATIC_DEVICE_NAME.replace(name);
    refresh_pending_automatic_device_name();
}

/// 消费已取得的系统名称；运行时未就绪或持久化失败时保留待下次重试。
#[cfg(target_os = "android")]
fn refresh_pending_automatic_device_name() {
    let Some(app) = APP_HANDLE.get() else {
        return;
    };
    let Some(manager) = app.try_state::<std::sync::Arc<crate::sync::SyncManager>>() else {
        return;
    };
    let Some(name) = PENDING_AUTOMATIC_DEVICE_NAME.take() else {
        return;
    };

    if let Err(error) = manager.refresh_system_device_name(name.clone()) {
        PENDING_AUTOMATIC_DEVICE_NAME.restore_if_empty(name);
        log::warn!("refresh Android automatic device name failed: {error}");
    }
}

#[cfg(target_os = "android")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AndroidOverlayClipboardItem {
    id: String,
    kind: &'static str,
    tag: &'static str,
    preview: String,
    detail: String,
    image_path: Option<String>,
    source_app_name: String,
    source_app_icon_path: Option<String>,
    source_app_accent_start: Option<String>,
    source_app_accent_end: Option<String>,
    display_created_at: String,
    is_favorite: bool,
    is_pinned: bool,
    sync: crate::sync::SyncItemStatus,
}

#[cfg(target_os = "android")]
fn load_overlay_items_json(keyword: Option<String>, limit: i64) -> Result<String> {
    use crate::db::models::{ClipboardItemQuery, ClipboardKind};

    let app = APP_HANDLE
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Android app runtime is not ready"))?;

    tauri::async_runtime::block_on(async move {
        let db = app.state::<crate::db::DatabaseState>();
        let pool = db.pool().await;
        let query = ClipboardItemQuery {
            keyword: keyword.filter(|value| !value.trim().is_empty()),
            limit: limit.clamp(1, 50),
            ..Default::default()
        };
        let (items, _) = crate::db::items::query_items_page(&pool, &query).await?;
        let item_ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
        let mut sync_statuses =
            if let Some(manager) = app.try_state::<std::sync::Arc<crate::sync::SyncManager>>() {
                manager
                    .item_statuses(&item_ids)
                    .await?
                    .into_iter()
                    .map(|status| (status.item_id.clone(), status))
                    .collect::<std::collections::HashMap<_, _>>()
            } else {
                std::collections::HashMap::new()
            };
        let redact_sensitive = app
            .state::<crate::settings::SettingsStore>()
            .snapshot()
            .clipboard
            .sensitive
            .redact_secrets;
        let image_store = app.state::<crate::clipboard::ImageStore>();
        let app_icon_store = app.state::<crate::clipboard::AppIconStore>();
        let items = items
            .into_iter()
            .map(|item| {
                let sync = sync_statuses
                    .remove(&item.id)
                    .unwrap_or_else(|| crate::sync::SyncItemStatus::idle(item.id.clone()));
                let kind = match item.kind {
                    ClipboardKind::Text => "text",
                    ClipboardKind::Image => "image",
                    ClipboardKind::Files => "files",
                };
                let tag = match (item.kind, item.sub_kind) {
                    (ClipboardKind::Image, _) => "图片",
                    (ClipboardKind::Files, _) => "文件",
                    (ClipboardKind::Text, Some(crate::db::models::ClipboardSubKind::Url)) => "链接",
                    (ClipboardKind::Text, Some(crate::db::models::ClipboardSubKind::Email)) => {
                        "邮件"
                    }
                    (ClipboardKind::Text, Some(crate::db::models::ClipboardSubKind::Color)) => {
                        "颜色"
                    }
                    (ClipboardKind::Text, Some(crate::db::models::ClipboardSubKind::Path)) => {
                        "路径"
                    }
                    (ClipboardKind::Text, Some(crate::db::models::ClipboardSubKind::Html)) => {
                        "HTML"
                    }
                    (ClipboardKind::Text, Some(crate::db::models::ClipboardSubKind::Rtf)) => "RTF",
                    (ClipboardKind::Text, None) => "文本",
                };
                let preview = if redact_sensitive && item.is_sensitive {
                    "敏感内容已隐藏".to_owned()
                } else {
                    match item.kind {
                        ClipboardKind::Text => item
                            .note
                            .clone()
                            .or(item.summary.clone())
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or_else(|| "空文本".to_owned()),
                        ClipboardKind::Image => match (item.width, item.height) {
                            (Some(width), Some(height)) => format!("图片 · {width} × {height}"),
                            _ => "图片".to_owned(),
                        },
                        ClipboardKind::Files => {
                            let count =
                                item.content.lines().filter(|line| !line.is_empty()).count();
                            if count > 1 {
                                format!("{count} 个文件")
                            } else {
                                item.content
                                    .lines()
                                    .find(|line| !line.is_empty())
                                    .and_then(|path| std::path::Path::new(path).file_name())
                                    .and_then(|name| name.to_str())
                                    .unwrap_or("文件")
                                    .to_owned()
                            }
                        }
                    }
                };
                let display_created_at = item
                    .created_at
                    .with_timezone(&chrono::Local)
                    .format("%m-%d %H:%M")
                    .to_string();
                let source_app_name = item
                    .source_app_name
                    .filter(|name| !name.trim().is_empty())
                    .unwrap_or_else(|| "EcoPaste".to_owned());
                let source_app_icon_path = item
                    .source_app_icon_file
                    .as_deref()
                    .and_then(|name| app_icon_store.icon_path(name).to_str().map(str::to_owned));
                let detail = match item.kind {
                    ClipboardKind::Image => match (item.width, item.height) {
                        (Some(width), Some(height)) => format!("{width} × {height}"),
                        _ => "图片".to_owned(),
                    },
                    ClipboardKind::Files => {
                        let count = item.content.lines().filter(|line| !line.is_empty()).count();
                        format!("{count} 个文件")
                    }
                    ClipboardKind::Text => format!("{} 个字符", preview.chars().count()),
                };
                let image_path = if item.kind == ClipboardKind::Image
                    && !(redact_sensitive && item.is_sensitive)
                {
                    overlay_image_path(&image_store, &item.content)
                } else {
                    None
                };

                AndroidOverlayClipboardItem {
                    id: item.id,
                    kind,
                    tag,
                    preview,
                    detail,
                    image_path,
                    source_app_name,
                    source_app_icon_path,
                    source_app_accent_start: item.source_app_accent_start,
                    source_app_accent_end: item.source_app_accent_end,
                    display_created_at,
                    is_favorite: item.is_favorite,
                    is_pinned: item.is_pinned,
                    sync,
                }
            })
            .collect::<Vec<_>>();

        serde_json::to_string(&items).map_err(|error| anyhow::anyhow!(error).into())
    })
}

/// 返回上滑图片卡片可安全读取的本地原图路径；异常文件名和缺失文件降级为无预览。
#[cfg(target_os = "android")]
fn overlay_image_path(store: &crate::clipboard::ImageStore, file_name: &str) -> Option<String> {
    let invalid = file_name.is_empty()
        || file_name.contains('/')
        || file_name.contains('\\')
        || file_name.contains("..")
        || !file_name.ends_with(".png");
    if invalid {
        return None;
    }

    let path = store.origin_path(file_name);
    path.is_file()
        .then(|| path.to_str().map(str::to_owned))
        .flatten()
}

#[cfg(target_os = "android")]
fn load_overlay_cloud_records_json(before_cursor: Option<u64>, limit: u16) -> Result<String> {
    let app = APP_HANDLE
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Android app runtime is not ready"))?;
    let manager = app
        .try_state::<std::sync::Arc<crate::sync::SyncManager>>()
        .ok_or_else(|| anyhow::anyhow!("sync manager is not ready"))?
        .inner()
        .clone();
    Ok(tauri::async_runtime::block_on(async move {
        let page = manager
            .cloud_records(before_cursor, limit.clamp(1, 30))
            .await?;
        serde_json::to_string(&page).map_err(anyhow::Error::from)
    })?)
}

#[cfg(target_os = "android")]
fn load_overlay_sync_status_json() -> Result<String> {
    let app = APP_HANDLE
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Android app runtime is not ready"))?;
    tauri::async_runtime::block_on(async move {
        let manager = app.state::<std::sync::Arc<crate::sync::SyncManager>>();
        serde_json::to_string(&manager.status().await?)
            .map_err(|error| anyhow::anyhow!(error).into())
    })
}

#[cfg(target_os = "android")]
fn sync_overlay_item_json(id: String, target: crate::sync::SyncTarget) -> Result<String> {
    let app = APP_HANDLE
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Android app runtime is not ready"))?;
    tauri::async_runtime::block_on(async move {
        let pool = app.state::<crate::db::DatabaseState>().pool().await;
        let item = crate::db::items::find_item_by_id(&pool, &id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("剪贴板记录不存在"))?;
        let manager = app.state::<std::sync::Arc<crate::sync::SyncManager>>();
        let status = manager.inner().clone().sync_item_now(item, target).await?;
        serde_json::to_string(&status).map_err(|error| anyhow::anyhow!(error).into())
    })
}

#[cfg(target_os = "android")]
fn reconnect_overlay_peer(device_id: Option<String>) -> Result<()> {
    let app = APP_HANDLE
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Android app runtime is not ready"))?;
    tauri::async_runtime::block_on(async move {
        let manager = app.state::<std::sync::Arc<crate::sync::SyncManager>>();
        manager.inner().clone().reconnect_peer(device_id).await?;
        Ok(())
    })
}

#[cfg(target_os = "android")]
fn paste_overlay_item(id: String) -> Result<()> {
    use std::sync::Arc;

    use crate::clipboard::WritebackGuard;
    use crate::core::AppError;
    use crate::db::items::{find_item_by_id, increment_item_use_count};

    let app = APP_HANDLE
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Android app runtime is not ready"))?;

    tauri::async_runtime::block_on(async {
        let db = app.state::<crate::db::DatabaseState>();
        let pool = db.pool().await;
        let item = find_item_by_id(&pool, &id)
            .await?
            .ok_or_else(|| AppError::Clipboard(format!("clipboard item not found: {id}")))?;
        let guard = app.state::<Arc<WritebackGuard>>();
        crate::clipboard::write_to_clipboard_app(&app, guard.inner().as_ref(), &item)?;

        let settings = app.state::<crate::settings::SettingsStore>().snapshot();
        if settings.clipboard.content.update_on_reuse {
            increment_item_use_count(&pool, &id).await?;
            if let Err(error) = app.emit(
                "clipboard://updated",
                serde_json::json!({
                    "id": id,
                    "kind": item.kind,
                    "deduplicated": true,
                }),
            ) {
                log::warn!("emit clipboard update after Android overlay paste failed: {error}");
            }
        }

        Ok::<_, crate::core::AppError>(())
    })?;

    crate::keystroke::simulate_paste()
}

#[cfg(target_os = "android")]
fn persist_overlay_panel_height_percent(height_percent: i32) -> Result<()> {
    if !(30..=90).contains(&height_percent) {
        return Err(
            anyhow::anyhow!("Android gesture popup height must be between 30% and 90%").into(),
        );
    }

    let app = APP_HANDLE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Android app runtime is not ready"))?;
    let next = app
        .state::<crate::settings::SettingsStore>()
        .update(serde_json::json!({
            "android": {
                "gesture": {
                    "popupHeightPercent": height_percent,
                },
            },
        }))?;
    crate::commands::settings::emit_settings_updated(app, &next);
    Ok(())
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_initNdkContext(
    raw_env: *mut jni::sys::JNIEnv,
    class: jni::sys::jclass,
    context: jni::sys::jobject,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Ok(env) = jni::JNIEnv::from_raw(raw_env) {
            if GLOBAL_VM.get().is_none() {
                if let Ok(vm) = env.get_java_vm() {
                    let _ = GLOBAL_VM.set(vm);
                }
            }
            if GLOBAL_CONTEXT.get().is_none() && !context.is_null() {
                let obj = jni::objects::JObject::from_raw(context);
                if let Ok(global_ref) = env.new_global_ref(obj) {
                    let _ = GLOBAL_CONTEXT.set(global_ref);
                }
            }
            if BRIDGE_CLASS.get().is_none() && !class.is_null() {
                let cls_obj = jni::objects::JObject::from_raw(class);
                if let Ok(global_cls) = env.new_global_ref(cls_obj) {
                    let _ = BRIDGE_CLASS.set(global_cls);
                }
            }
            if let (Some(vm), Some(context)) = (GLOBAL_VM.get(), GLOBAL_CONTEXT.get()) {
                IROH_ANDROID_CONTEXT.get_or_init(|| unsafe {
                    iroh::dns::install_android_jni_context(
                        vm.get_java_vm_pointer().cast(),
                        context.as_obj().as_raw().cast(),
                    );
                });
            }
        }
    }));
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_MainActivity_initNdkContext(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    context: jni::sys::jobject,
) {
    if let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) {
        if let Ok(bridge_cls) = env.find_class("com/ayangweb/eco_paste/EcoPasteBridge") {
            Java_com_ayangweb_eco_1paste_EcoPasteBridge_initNdkContext(
                raw_env,
                bridge_cls.as_raw(),
                context,
            );
        }
    }
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_loadOverlayItemsJson(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    keyword: jni::sys::jstring,
    limit: jni::sys::jint,
) -> jni::sys::jstring {
    let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) else {
        return std::ptr::null_mut();
    };
    let keyword = if keyword.is_null() {
        None
    } else {
        let keyword = jni::objects::JString::from_raw(keyword);
        env.get_string(&keyword).map(String::from).ok()
    };
    let json = match std::panic::catch_unwind(|| load_overlay_items_json(keyword, i64::from(limit)))
    {
        Ok(Ok(json)) => json,
        Ok(Err(error)) => {
            log::error!("load Android overlay items failed: {error}");
            "[]".to_owned()
        }
        Err(_) => {
            log::error!("load Android overlay items panicked");
            "[]".to_owned()
        }
    };

    env.new_string(json)
        .map(jni::objects::JString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_loadOverlaySyncStatusJson(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
) -> jni::sys::jstring {
    let Ok(env) = jni::JNIEnv::from_raw(raw_env) else {
        return std::ptr::null_mut();
    };
    let json = load_overlay_sync_status_json().unwrap_or_else(|error| {
        log::error!("load Android overlay sync status failed: {error}");
        "{}".to_owned()
    });
    env.new_string(json)
        .map(jni::objects::JString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_loadOverlayCloudRecordsJson(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    before_cursor: jni::sys::jlong,
    limit: jni::sys::jint,
) -> jni::sys::jstring {
    let Ok(env) = jni::JNIEnv::from_raw(raw_env) else {
        return std::ptr::null_mut();
    };
    let before_cursor = (before_cursor >= 0).then_some(before_cursor as u64);
    let json = load_overlay_cloud_records_json(before_cursor, u16::try_from(limit).unwrap_or(30))
        .unwrap_or_else(|error| {
            log::error!("load Android overlay cloud records failed: {error}");
            serde_json::json!({ "error": error.to_string() }).to_string()
        });
    env.new_string(json)
        .map(jni::objects::JString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_syncOverlayItemJson(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    id: jni::sys::jstring,
    target: jni::sys::jstring,
) -> jni::sys::jstring {
    let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) else {
        return std::ptr::null_mut();
    };
    let id = jni::objects::JString::from_raw(id);
    let target = jni::objects::JString::from_raw(target);
    let result = env
        .get_string(&id)
        .map(String::from)
        .and_then(|id| {
            env.get_string(&target)
                .map(String::from)
                .map(|target| (id, target))
        })
        .map_err(|error| anyhow::anyhow!("read Android sync item request failed: {error}"))
        .and_then(|(id, target)| {
            let target = match target.as_str() {
                "lan" => crate::sync::SyncTarget::Lan,
                "cloud" => crate::sync::SyncTarget::Cloud,
                _ => return Err(anyhow::anyhow!("invalid sync target")),
            };
            sync_overlay_item_json(id, target)
                .map_err(|error| anyhow::anyhow!("sync Android overlay item failed: {error}"))
        });
    let json = result.unwrap_or_else(|error| {
        log::error!("sync Android overlay item failed: {error}");
        serde_json::json!({ "error": error.to_string() }).to_string()
    });
    env.new_string(json)
        .map(jni::objects::JString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_reconnectOverlayPeer(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    device_id: jni::sys::jstring,
) -> jni::sys::jboolean {
    let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) else {
        return 0;
    };
    let device_id = jni::objects::JString::from_raw(device_id);
    let device_id = env
        .get_string(&device_id)
        .map(String::from)
        .ok()
        .filter(|value| !value.trim().is_empty());
    match reconnect_overlay_peer(device_id) {
        Ok(()) => 1,
        Err(error) => {
            log::warn!("Android overlay reconnect failed: {error}");
            0
        }
    }
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_captureClipboardText(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    text: jni::sys::jstring,
    package_name: jni::sys::jstring,
    app_name: jni::sys::jstring,
    icon_png: jni::sys::jbyteArray,
) -> jni::sys::jboolean {
    let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) else {
        return 0;
    };
    let text = jni::objects::JString::from_raw(text);
    let Ok(text) = env.get_string(&text).map(String::from) else {
        return 0;
    };
    let package_name = jni::objects::JString::from_raw(package_name);
    let app_name = jni::objects::JString::from_raw(app_name);
    let package_name = env
        .get_string(&package_name)
        .map(String::from)
        .unwrap_or_default();
    let app_name = env
        .get_string(&app_name)
        .map(String::from)
        .unwrap_or_default();
    let icon_png = if icon_png.is_null() {
        None
    } else {
        let icon_png = jni::objects::JByteArray::from_raw(icon_png);
        env.convert_byte_array(&icon_png)
            .ok()
            .filter(|bytes| !bytes.is_empty())
    };
    let source = (!package_name.trim().is_empty()).then(|| crate::clipboard::FrontmostApp {
        id: package_name.clone(),
        name: if app_name.trim().is_empty() {
            package_name
        } else {
            app_name
        },
        icon_png,
        platform: crate::db::models::Platform::Android,
    });
    if let Some(app) = APP_HANDLE.get() {
        crate::clipboard::capture_android_text(app, text, source);
        return 1;
    }
    0
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_pasteOverlayItem(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    id: jni::sys::jstring,
) -> jni::sys::jboolean {
    let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) else {
        return 0;
    };
    let id = jni::objects::JString::from_raw(id);
    let result = env
        .get_string(&id)
        .map(String::from)
        .map_err(|error| anyhow::anyhow!("read Android overlay item id failed: {error}"))
        .and_then(|id| {
            paste_overlay_item(id)
                .map_err(|error| anyhow::anyhow!("paste Android overlay item failed: {error}"))
        });
    match result {
        Ok(()) => 1,
        Err(error) => {
            log::error!("paste Android overlay item failed: {error}");
            0
        }
    }
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_persistOverlayPanelHeightPercent(
    _raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    height_percent: jni::sys::jint,
) -> jni::sys::jboolean {
    match std::panic::catch_unwind(|| persist_overlay_panel_height_percent(height_percent)) {
        Ok(Ok(())) => 1,
        Ok(Err(error)) => {
            log::error!("persist Android overlay panel height failed: {error}");
            0
        }
        Err(_) => {
            log::error!("persist Android overlay panel height panicked");
            0
        }
    }
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_notifySyncDefaultNetworkChanged(
    _raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
) -> jni::sys::jboolean {
    let Some(app) = APP_HANDLE.get() else {
        return 0;
    };
    if let Some(manager) = app.try_state::<std::sync::Arc<crate::sync::SyncManager>>() {
        manager.notify_default_network_changed();
        return 1;
    }
    0
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_notifySyncDefaultNetworkLost(
    _raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
) -> jni::sys::jboolean {
    let Some(app) = APP_HANDLE.get() else {
        return 0;
    };
    if let Some(manager) = app.try_state::<std::sync::Arc<crate::sync::SyncManager>>() {
        manager.notify_default_network_lost();
        return 1;
    }
    0
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_notifySyncLanNetworkChanged(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    interface_addresses: jni::sys::jstring,
) -> jni::sys::jboolean {
    let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) else {
        return 0;
    };
    let interface_addresses = jni::objects::JString::from_raw(interface_addresses);
    let Ok(interface_addresses) = env.get_string(&interface_addresses).map(String::from) else {
        return 0;
    };
    let Some(app) = APP_HANDLE.get() else {
        return 0;
    };
    if let Some(manager) = app.try_state::<std::sync::Arc<crate::sync::SyncManager>>() {
        manager.notify_lan_network_changed(&interface_addresses);
        return 1;
    }
    0
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_notifySyncLanNetworkLost(
    _raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
) -> jni::sys::jboolean {
    let Some(app) = APP_HANDLE.get() else {
        return 0;
    };
    if let Some(manager) = app.try_state::<std::sync::Arc<crate::sync::SyncManager>>() {
        manager.notify_lan_network_lost();
        return 1;
    }
    0
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_notifySyncStatusRefresh(
    _raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
) {
    let Some(app) = APP_HANDLE.get() else {
        return;
    };
    if let Some(manager) = app.try_state::<std::sync::Arc<crate::sync::SyncManager>>() {
        manager.notify_status_refresh();
    }
}

#[cfg(target_os = "android")]
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_refreshAutomaticDeviceName(
    raw_env: *mut jni::sys::JNIEnv,
    _class: jni::sys::jclass,
    name: jni::sys::jstring,
) {
    let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) else {
        return;
    };
    let name = jni::objects::JString::from_raw(name);
    let Ok(name) = env.get_string(&name).map(String::from) else {
        return;
    };
    enqueue_automatic_device_name(name);
}

#[cfg(target_os = "android")]
mod jni_bridge {
    use anyhow::anyhow;
    use jni::objects::{JClass, JObject, JString, JValue};

    use super::*;

    fn with_jni_env<F, T>(f: F) -> Result<T>
    where
        F: FnOnce(&mut jni::JNIEnv, &JObject, &JClass) -> Result<T>,
    {
        let vm = GLOBAL_VM
            .get()
            .ok_or_else(|| anyhow!("JavaVM not initialized"))?;
        let context_ref = GLOBAL_CONTEXT
            .get()
            .ok_or_else(|| anyhow!("Context not initialized"))?;
        let bridge_class_ref = BRIDGE_CLASS
            .get()
            .ok_or_else(|| anyhow!("Bridge class not initialized"))?;
        let mut env = vm
            .attach_current_thread()
            .map_err(|e| anyhow!("failed to attach JNI thread: {e}"))?;
        let cls: &JClass = bridge_class_ref.as_obj().into();
        let res = f(&mut env, context_ref.as_obj(), cls);
        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_clear();
        }
        res
    }

    pub fn get_permissions_status() -> Result<AndroidPermissionsStatus> {
        with_jni_env(|env, context, bridge_class| {
            let result = env
                .call_static_method(
                    bridge_class,
                    "getPermissionsJson",
                    "(Landroid/content/Context;)Ljava/lang/String;",
                    &[JValue::Object(context)],
                )
                .map_err(|e| anyhow!("call getPermissionsJson failed: {e}"))?;

            let jstr: JString = result
                .l()
                .map_err(|e| anyhow!("extract JString failed: {e}"))?
                .into();

            let rust_str: String = env
                .get_string(&jstr)
                .map_err(|e| anyhow!("get_string failed: {e}"))?
                .into();

            serde_json::from_str(&rust_str)
                .map_err(|e| anyhow!("parse permissions JSON failed: {e}").into())
        })
    }

    fn read_device_string(method: &str, signature: &str, with_context: bool) -> Result<String> {
        with_jni_env(|env, context, bridge_class| {
            let arguments = if with_context {
                vec![JValue::Object(context)]
            } else {
                Vec::new()
            };
            let result = env
                .call_static_method(bridge_class, method, signature, &arguments)
                .map_err(|e| anyhow!("call {method} failed: {e}"))?;
            let jstr: JString = result
                .l()
                .map_err(|e| anyhow!("extract {method} result failed: {e}"))?
                .into();
            env.get_string(&jstr)
                .map(String::from)
                .map_err(|e| anyhow!("read {method} result failed: {e}").into())
        })
    }

    pub fn device_name() -> Result<String> {
        read_device_string(
            "getDeviceName",
            "(Landroid/content/Context;)Ljava/lang/String;",
            true,
        )
    }

    pub fn device_model() -> Result<String> {
        read_device_string("getDeviceModel", "()Ljava/lang/String;", false)
    }

    pub fn device_fallback_name() -> Result<String> {
        read_device_string("getDeviceFallbackName", "()Ljava/lang/String;", false)
    }

    pub fn request_permission(kind: &str) -> Result<()> {
        with_jni_env(|env, context, bridge_class| {
            let kind_jstr = env
                .new_string(kind)
                .map_err(|e| anyhow!("new_string failed: {e}"))?;

            env.call_static_method(
                bridge_class,
                "requestPermissionByName",
                "(Landroid/content/Context;Ljava/lang/String;)V",
                &[JValue::Object(context), JValue::Object(&kind_jstr)],
            )
            .map_err(|e| anyhow!("call requestPermissionByName failed: {e}"))?;

            Ok(())
        })
    }

    pub fn toggle_overlay_service(enabled: bool) -> Result<()> {
        with_jni_env(|env, context, bridge_class| {
            env.call_static_method(
                bridge_class,
                "setOverlayServiceEnabled",
                "(Landroid/content/Context;Z)V",
                &[JValue::Object(context), JValue::Bool(enabled as u8)],
            )
            .map_err(|e| anyhow!("call setOverlayServiceEnabled failed: {e}"))?;

            Ok(())
        })
    }

    pub fn minimize_app() -> Result<()> {
        with_jni_env(|env, context, bridge_class| {
            env.call_static_method(
                bridge_class,
                "minimizeCurrentApp",
                "(Landroid/content/Context;)V",
                &[JValue::Object(context)],
            )
            .map_err(|e| anyhow!("call minimizeCurrentApp failed: {e}"))?;

            Ok(())
        })
    }

    pub fn set_engine_mode(mode: &str) -> Result<AndroidEngineResult> {
        with_jni_env(|env, context, bridge_class| {
            let mode_jstr = env
                .new_string(mode)
                .map_err(|e| anyhow!("new_string failed: {e}"))?;

            let result = env
                .call_static_method(
                    bridge_class,
                    "setEngine",
                    "(Landroid/content/Context;Ljava/lang/String;)Ljava/lang/String;",
                    &[JValue::Object(context), JValue::Object(&mode_jstr)],
                )
                .map_err(|e| anyhow!("call setEngine failed: {e}"))?;

            let jstr: JString = result
                .l()
                .map_err(|e| anyhow!("extract engine result failed: {e}"))?
                .into();
            let json: String = env
                .get_string(&jstr)
                .map_err(|e| anyhow!("read engine result failed: {e}"))?
                .into();

            serde_json::from_str(&json)
                .map_err(|e| anyhow!("parse engine result failed: {e}").into())
        })
    }

    fn file_action(method: &str, path: &std::path::Path) -> Result<AndroidFileActionResult> {
        with_jni_env(|env, context, bridge_class| {
            let path = path
                .to_str()
                .ok_or_else(|| anyhow!("Android clipboard file path is not valid UTF-8"))?;
            let path_jstr = env
                .new_string(path)
                .map_err(|e| anyhow!("new_string failed: {e}"))?;
            let result = env
                .call_static_method(
                    bridge_class,
                    method,
                    "(Landroid/content/Context;Ljava/lang/String;)Ljava/lang/String;",
                    &[JValue::Object(context), JValue::Object(&path_jstr)],
                )
                .map_err(|e| anyhow!("call {method} failed: {e}"))?;
            let result: JString = result
                .l()
                .map_err(|e| anyhow!("extract {method} result failed: {e}"))?
                .into();
            let json: String = env
                .get_string(&result)
                .map_err(|e| anyhow!("read {method} result failed: {e}"))?
                .into();

            serde_json::from_str(&json)
                .map_err(|e| anyhow!("parse {method} result failed: {e}").into())
        })
    }

    pub fn open_clipboard_file(path: &std::path::Path) -> Result<AndroidFileActionResult> {
        file_action("openClipboardFile", path)
    }

    pub fn save_clipboard_file(path: &std::path::Path) -> Result<AndroidFileActionResult> {
        file_action("saveClipboardFile", path)
    }

    pub fn apply_gesture_settings(settings: &crate::settings::AndroidGesture) -> Result<()> {
        with_jni_env(|env, context, bridge_class| {
            env.call_static_method(
                bridge_class,
                "applyGestureConfig",
                "(Landroid/content/Context;ZZIIIII)V",
                &[
                    JValue::Object(context),
                    JValue::Bool(settings.enabled as u8),
                    JValue::Bool(settings.hide_overlay as u8),
                    JValue::Int(settings.popup_height_percent as i32),
                    JValue::Int(settings.left_width_dp as i32),
                    JValue::Int(settings.left_height_dp as i32),
                    JValue::Int(settings.right_width_dp as i32),
                    JValue::Int(settings.right_height_dp as i32),
                ],
            )
            .map_err(|e| anyhow!("call applyGestureConfig failed: {e}"))?;

            Ok(())
        })
    }

    pub fn perform_auto_paste() -> Result<bool> {
        with_jni_env(|env, _context, bridge_class| {
            let result = env
                .call_static_method(bridge_class, "triggerAutoPaste", "()Z", &[])
                .map_err(|e| anyhow!("call triggerAutoPaste failed: {e}"))?;

            let val = result
                .z()
                .map_err(|e| anyhow!("extract bool failed: {e}"))?;

            Ok(val)
        })
    }

    pub fn write_clipboard_text(text: &str) -> Result<()> {
        with_jni_env(|env, context, bridge_class| {
            let text = env
                .new_string(text)
                .map_err(|error| anyhow!("create Android clipboard text failed: {error}"))?;
            env.call_static_method(
                bridge_class,
                "writeClipboardText",
                "(Landroid/content/Context;Ljava/lang/String;)V",
                &[JValue::Object(context), JValue::Object(&text)],
            )
            .map_err(|error| anyhow!("write Android clipboard text failed: {error}"))?;
            Ok(())
        })
    }

    pub fn notify_overlay_sync_status_changed() -> Result<()> {
        with_jni_env(|env, _context, bridge_class| {
            env.call_static_method(bridge_class, "onSyncStatusChanged", "()V", &[])
                .map_err(|e| anyhow!("call onSyncStatusChanged failed: {e}"))?;
            Ok(())
        })
    }

    pub fn notify_overlay_clipboard_changed() -> Result<()> {
        with_jni_env(|env, _context, bridge_class| {
            env.call_static_method(bridge_class, "onClipboardDataChanged", "()V", &[])
                .map_err(|e| anyhow!("call onClipboardDataChanged failed: {e}"))?;
            Ok(())
        })
    }

    pub fn set_lan_discovery_enabled(enabled: bool) -> Result<()> {
        with_jni_env(|env, context, bridge_class| {
            env.call_static_method(
                bridge_class,
                "setLanDiscoveryEnabled",
                "(Landroid/content/Context;Z)V",
                &[JValue::Object(context), JValue::Bool(enabled as u8)],
            )
            .map_err(|e| anyhow!("call setLanDiscoveryEnabled failed: {e}"))?;
            Ok(())
        })
    }
}

/// Opens one Rust-validated file through Android's system app chooser.
pub fn open_android_clipboard_file_path(path: &std::path::Path) -> Result<AndroidFileActionResult> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::open_clipboard_file(path)
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = path;
        Ok(AndroidFileActionResult {
            status: "unavailable".to_owned(),
            message: String::new(),
        })
    }
}

/// Exports one Rust-validated file through Android's system document picker.
pub fn save_android_clipboard_file_path(path: &std::path::Path) -> Result<AndroidFileActionResult> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::save_clipboard_file(path)
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = path;
        Ok(AndroidFileActionResult {
            status: "unavailable".to_owned(),
            message: String::new(),
        })
    }
}

/// Writes one diagnostic stage to a stable Android logcat tag.
pub fn log_android_file_action(stage: &str, message: impl AsRef<str>) {
    #[cfg(target_os = "android")]
    {
        use std::ffi::{c_char, c_int, CString};

        #[link(name = "log")]
        extern "C" {
            fn __android_log_write(
                priority: c_int,
                tag: *const c_char,
                text: *const c_char,
            ) -> c_int;
        }

        let Ok(tag) = CString::new("EcoPasteFileAction") else {
            return;
        };
        let text = format!("{stage} | {}", message.as_ref()).replace('\0', "\\0");
        let Ok(text) = CString::new(text) else {
            return;
        };

        // Android's WARN priority remains visible on production builds and does not depend on JNI.
        unsafe {
            __android_log_write(5, tag.as_ptr(), text.as_ptr());
        }
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = (stage, message);
    }
}

#[cfg(target_os = "android")]
pub(crate) fn notify_overlay_sync_status_changed() {
    if let Err(error) = jni_bridge::notify_overlay_sync_status_changed() {
        log::debug!("notify Android overlay sync status failed: {error}");
    }
}

#[cfg(target_os = "android")]
pub(crate) fn notify_overlay_clipboard_changed() {
    if let Err(error) = jni_bridge::notify_overlay_clipboard_changed() {
        log::debug!("notify Android overlay clipboard change failed: {error}");
    }
}

#[cfg(target_os = "android")]
pub(crate) fn write_android_clipboard_text(text: &str) -> Result<()> {
    jni_bridge::write_clipboard_text(text)
}

#[cfg(target_os = "android")]
pub(crate) fn set_lan_discovery_enabled(enabled: bool) {
    if let Err(error) = jni_bridge::set_lan_discovery_enabled(enabled) {
        log::debug!("update Android LAN discovery lock failed: {error}");
    }
}

#[cfg(target_os = "android")]
pub(crate) fn android_device_name() -> Option<String> {
    jni_bridge::device_name()
        .inspect_err(|error| log::warn!("read Android device name failed: {error}"))
        .ok()
        .filter(|name| !name.trim().is_empty())
}

#[cfg(target_os = "android")]
pub(crate) fn android_device_model() -> Option<String> {
    jni_bridge::device_model()
        .inspect_err(|error| log::warn!("read Android device model failed: {error}"))
        .ok()
        .filter(|name| !name.trim().is_empty())
}

#[cfg(target_os = "android")]
pub(crate) fn android_device_fallback_name() -> Option<String> {
    jni_bridge::device_fallback_name()
        .inspect_err(|error| log::warn!("read Android fallback device name failed: {error}"))
        .ok()
        .filter(|name| !name.trim().is_empty())
}

/// 获取 Android 端各项权限与服务运行状态
#[tauri::command]
pub async fn get_android_permissions_status() -> Result<AndroidPermissionsStatus> {
    #[cfg(target_os = "android")]
    {
        tauri::async_runtime::spawn_blocking(jni_bridge::get_permissions_status)
            .await
            .map_err(|error| anyhow::anyhow!("Android permission check task failed: {error}"))?
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(AndroidPermissionsStatus {
            overlay_granted: true,
            accessibility_granted: true,
            notification_granted: true,
            battery_ignored: true,
            root_available: false,
            root_clipboard_granted: false,
            overlay_service_running: false,
            engine_mode: "accessibility".to_string(),
        })
    }
}

/// 请求跳转系统权限设置或授权
#[tauri::command]
pub async fn request_android_permission(kind: String) -> Result<()> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::request_permission(&kind)
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = kind;
        Ok(())
    }
}

/// 启停屏幕底角上滑手势悬浮服务
#[tauri::command]
pub async fn toggle_android_overlay_service(enabled: bool) -> Result<()> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::toggle_overlay_service(enabled)
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = enabled;
        Ok(())
    }
}

/// 最小化退回后台
#[tauri::command]
pub async fn minimize_android_app() -> Result<()> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::minimize_app()
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(())
    }
}

/// 切换剪贴板监听引擎模式
#[tauri::command]
pub async fn set_android_engine_mode(mode: String) -> Result<AndroidEngineResult> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::set_engine_mode(&mode)
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(AndroidEngineResult {
            success: true,
            mode,
            root_clipboard_granted: false,
            message: String::new(),
        })
    }
}

pub fn apply_android_gesture_settings(settings: &crate::settings::AndroidGesture) -> Result<()> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::apply_gesture_settings(settings)
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = settings;
        Ok(())
    }
}

/// 原生模拟自动粘贴
#[allow(dead_code)]
pub fn perform_android_auto_paste() -> Result<bool> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::perform_auto_paste()
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::PendingAutomaticDeviceName;

    #[test]
    fn pending_device_name_waits_until_consumed() {
        let pending = PendingAutomaticDeviceName::default();

        pending.replace("OnePlus 13".to_owned());

        assert_eq!(pending.take().as_deref(), Some("OnePlus 13"));
        assert_eq!(pending.take(), None);
    }

    #[test]
    fn failed_older_name_does_not_replace_newer_pending_name() {
        let pending = PendingAutomaticDeviceName::default();

        pending.replace("New system name".to_owned());
        pending.restore_if_empty("Old system name".to_owned());

        assert_eq!(pending.take().as_deref(), Some("New system name"));
    }

    #[test]
    fn failed_name_is_restored_when_no_newer_name_exists() {
        let pending = PendingAutomaticDeviceName::default();

        pending.restore_if_empty("OnePlus 13".to_owned());

        assert_eq!(pending.take().as_deref(), Some("OnePlus 13"));
    }
}
