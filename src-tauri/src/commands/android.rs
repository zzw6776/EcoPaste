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

#[cfg(target_os = "android")]
static GLOBAL_VM: std::sync::OnceLock<jni::JavaVM> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
static GLOBAL_CONTEXT: std::sync::OnceLock<jni::objects::GlobalRef> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
static BRIDGE_CLASS: std::sync::OnceLock<jni::objects::GlobalRef> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
static APP_HANDLE: std::sync::OnceLock<tauri::AppHandle> = std::sync::OnceLock::new();
#[cfg(target_os = "android")]
pub fn set_app_handle(app_handle: tauri::AppHandle) {
    let _ = APP_HANDLE.set(app_handle);
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
    source_app_name: String,
    display_created_at: String,
    is_favorite: bool,
    is_pinned: bool,
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
        let redact_sensitive = app
            .state::<crate::settings::SettingsStore>()
            .snapshot()
            .clipboard
            .sensitive
            .redact_secrets;
        let items = items
            .into_iter()
            .map(|item| {
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

                AndroidOverlayClipboardItem {
                    id: item.id,
                    kind,
                    tag,
                    preview,
                    detail,
                    source_app_name,
                    display_created_at,
                    is_favorite: item.is_favorite,
                    is_pinned: item.is_pinned,
                }
            })
            .collect::<Vec<_>>();

        serde_json::to_string(&items).map_err(|error| anyhow::anyhow!(error).into())
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
#[no_mangle]
pub unsafe extern "C" fn Java_com_ayangweb_eco_1paste_EcoPasteBridge_initNdkContext(
    raw_env: *mut jni::sys::JNIEnv,
    class: jni::sys::jclass,
    context: jni::sys::jobject,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Ok(mut env) = jni::JNIEnv::from_raw(raw_env) {
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
}

/// 获取 Android 端各项权限与服务运行状态
#[tauri::command]
pub async fn get_android_permissions_status() -> Result<AndroidPermissionsStatus> {
    #[cfg(target_os = "android")]
    {
        jni_bridge::get_permissions_status()
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
