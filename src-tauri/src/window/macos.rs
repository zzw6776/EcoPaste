//! macOS 窗口管理：剪贴板窗口转 NSPanel（show_and_make_key 拿键盘焦点但不激活 App），
//! 其它窗口走常规 show/hide。

#![allow(clippy::unused_unit)]

use std::ptr::NonNull;

use block2::RcBlock;
use objc2_app_kit::{
    NSAppKitVersionNumber, NSAutoresizingMaskOptions, NSGlassEffectView, NSGlassEffectViewStyle,
    NSView as AppKitView, NSVisualEffectBlendingMode, NSVisualEffectMaterial, NSVisualEffectState,
    NSVisualEffectView, NSWindow as AppKitWindow, NSWindowOrderingMode, NSWorkspace,
    NSWorkspaceScreensDidWakeNotification,
};
use objc2_foundation::{MainThreadMarker as ObjcMainThreadMarker, NSOperationQueue};
use objc2_web_kit::WKWebView;
use tauri::{AppHandle, Manager, WebviewWindow};
use tauri_nspanel::{
    tauri_panel, CollectionBehavior, ManagerExt, PanelLevel, StyleMask, WebviewWindowExt,
};

use super::{
    get_window, ClipboardShowRequest, CLIPBOARD_WINDOW_LABEL, ONBOARDING_WINDOW_LABEL,
    PREFERENCE_WINDOW_LABEL,
};
use crate::core::Result;
use crate::settings::SettingsStore;

const CLIPBOARD_CORNER_RADIUS: f64 = 26.0;
const MIN_APPKIT_VERSION_LIQUID_GLASS: f64 = 2685.0;
const SLOW_CLIPBOARD_SHOW_MS: u128 = 100;
const CLIPBOARD_PREWARM_SCRIPT: &str = "void document.documentElement.getBoundingClientRect();";

tauri_panel! {
    panel!(MainPanel {
        config: {
            is_floating_panel: true,
            can_become_key_window: true,
            can_become_main_window: false
        }
    })

    panel_event!(MainPanelEventHandler {
        window_did_resign_key(notification: &NSNotification) -> (),
    })
}

/// setup 最早阶段调用：plugin 必须在 to_panel 前注册。
pub fn register_plugin(app_handle: &AppHandle) {
    let _ = app_handle.plugin(tauri_nspanel::init());
}

/// setup 末尾调用：转 NSPanel + 绑事件 emit。
pub fn setup_clipboard_panel(app_handle: &AppHandle) -> Result<()> {
    configure_idle_activity();
    register_screen_wake_prewarm(app_handle);
    show_taskbar_icon(app_handle, false)?;

    let clipboard_window = get_window(app_handle, CLIPBOARD_WINDOW_LABEL)?;

    // 必须在 to_panel 改写 NSWindow 类之前重挂载 WKWebView，否则 WebKit 注销窗口 KVO 时会崩溃。
    schedule_clipboard_surface(&clipboard_window)?;

    let panel = clipboard_window
        .to_panel::<MainPanel>()
        .map_err(|e| anyhow::anyhow!("to_panel failed: {e:?}"))?;

    panel.set_level(PanelLevel::Dock.value());
    panel.set_style_mask(
        StyleMask::empty()
            .borderless()
            .resizable()
            .nonactivating_panel()
            .into(),
    );
    panel.set_transparent(true);
    panel.set_collection_behavior(
        CollectionBehavior::new()
            .stationary()
            .move_to_active_space()
            .full_screen_auxiliary()
            .into(),
    );

    panel.set_corner_radius(CLIPBOARD_CORNER_RADIUS);

    let handler = MainPanelEventHandler::new();

    let resign_handle = app_handle.clone();
    handler.window_did_resign_key(move |_| {
        if !super::should_auto_hide_clipboard_window() {
            return;
        }

        // 失焦即隐藏：Tauri 不主动隐藏 NSPanel，统一走 window::hide_window
        // 以触发 `window://visibility` 等下游副作用。
        if let Err(err) = super::hide_window(&resign_handle, CLIPBOARD_WINDOW_LABEL) {
            log::warn!("auto-hide clipboard window on resign-key failed: {err}");
        }
    });

    panel.set_event_handler(Some(handler.as_ref()));

    Ok(())
}

/// 在 Tauri 回调提供真实 WKWebView 后，将它交给对应的原生材质承载。
fn schedule_clipboard_surface(window: &WebviewWindow) -> Result<()> {
    window
        .with_webview(move |platform_webview| {
            let result = (|| -> Result<()> {
                let window_ptr = platform_webview.ns_window().cast::<AppKitWindow>();
                if window_ptr.is_null() {
                    return Err(anyhow::anyhow!("clipboard NSWindow pointer is null").into());
                }

                // Tauri 保证 with_webview 回调期间这些指针有效且运行在 WebView 所属主线程。
                let native_window = unsafe { &*window_ptr };
                let container = native_window
                    .contentView()
                    .ok_or_else(|| anyhow::anyhow!("clipboard NSWindow has no content view"))?;

                if supports_liquid_glass() {
                    let webview_ptr = platform_webview.inner().cast::<WKWebView>();
                    if webview_ptr.is_null() {
                        return Err(anyhow::anyhow!("clipboard WKWebView pointer is null").into());
                    }

                    let webview = unsafe { &*webview_ptr };
                    install_clipboard_liquid_glass(&container, webview)?;
                    log::info!("installed macOS liquid glass clipboard surface");
                } else {
                    install_clipboard_vibrancy(&container)?;
                    log::info!("installed legacy macOS vibrancy clipboard surface");
                }

                Ok(())
            })();

            if let Err(err) = result {
                log::error!("install clipboard native surface failed: {err:?}");
            }
        })
        .map_err(|e| anyhow::anyhow!("schedule clipboard native surface failed: {e}"))?;

    Ok(())
}

/// macOS 26+ 使用单个 NSGlassEffectView 承载真实 WKWebView。
fn install_clipboard_liquid_glass(container: &AppKitView, webview: &WKWebView) -> Result<()> {
    let main_thread = ObjcMainThreadMarker::new()
        .ok_or_else(|| anyhow::anyhow!("clipboard glass setup must run on the main thread"))?;
    let glass_view = NSGlassEffectView::initWithFrame(main_thread.alloc(), container.bounds());

    glass_view.setStyle(NSGlassEffectViewStyle::Regular);
    glass_view.setCornerRadius(CLIPBOARD_CORNER_RADIUS);
    glass_view.setTintColor(None);
    glass_view.setAutoresizingMask(
        NSAutoresizingMaskOptions::ViewWidthSizable | NSAutoresizingMaskOptions::ViewHeightSizable,
    );

    webview.removeFromSuperview();
    glass_view.setContentView(Some(webview));
    container.addSubview(&glass_view);

    Ok(())
}

/// 较旧 macOS 没有 Liquid Glass API，退回公开的 Popover vibrancy 材质。
fn install_clipboard_vibrancy(content_view: &AppKitView) -> Result<()> {
    let main_thread = ObjcMainThreadMarker::new()
        .ok_or_else(|| anyhow::anyhow!("clipboard vibrancy setup must run on the main thread"))?;
    let material_view = NSVisualEffectView::new(main_thread);

    material_view.setFrame(content_view.bounds());
    material_view.setAutoresizingMask(
        NSAutoresizingMaskOptions::ViewWidthSizable | NSAutoresizingMaskOptions::ViewHeightSizable,
    );
    material_view.setMaterial(NSVisualEffectMaterial::Popover);
    material_view.setBlendingMode(NSVisualEffectBlendingMode::BehindWindow);
    material_view.setState(NSVisualEffectState::Active);
    content_view.addSubview_positioned_relativeTo(
        &material_view,
        NSWindowOrderingMode::Below,
        None,
    );

    Ok(())
}

/// NSGlassEffectView 从 macOS 26 对应的 AppKit 版本开始可用。
fn supports_liquid_glass() -> bool {
    unsafe { NSAppKitVersionNumber >= MIN_APPKIT_VERSION_LIQUID_GLASS }
}

/// 保留后台交互响应，同时允许系统按用户设置自动休眠。
fn configure_idle_activity() {
    use objc2_foundation::{NSActivityOptions, NSProcessInfo, NSString};
    let process_info = NSProcessInfo::processInfo();
    let reason = NSString::from_str("Keep clipboard manager responsive for global hotkeys");
    let options = NSActivityOptions::UserInitiatedAllowingIdleSystemSleep
        | NSActivityOptions::LatencyCritical;
    let activity = process_info.beginActivityWithOptions_reason(options, &reason);
    std::mem::forget(activity);
}

/// 监听屏幕唤醒；关闭轻量模式时提前唤醒隐藏的 WebContent 并准备下一次绘制。
fn register_screen_wake_prewarm(app_handle: &AppHandle) {
    let workspace = NSWorkspace::sharedWorkspace();
    let center = workspace.notificationCenter();
    let queue = NSOperationQueue::mainQueue();
    let wake_handle = app_handle.clone();
    let block = RcBlock::new(
        move |_notification: NonNull<objc2_foundation::NSNotification>| {
            prewarm_clipboard_webview(&wake_handle);
        },
    );

    // observer 与应用同生命周期；NotificationCenter 持有 block，进程退出时统一释放。
    let observer = unsafe {
        center.addObserverForName_object_queue_usingBlock(
            Some(NSWorkspaceScreensDidWakeNotification),
            None,
            Some(&queue),
            &block,
        )
    };
    std::mem::forget(observer);
}

/// 通过一次同步布局读取唤醒 WebContent，并把原生视图标记为待重绘。
fn prewarm_clipboard_webview(app_handle: &AppHandle) {
    let Some(settings) = app_handle.try_state::<SettingsStore>() else {
        return;
    };
    if settings.snapshot().clipboard.window.lightweight_mode {
        return;
    }

    let Some(window) = app_handle.get_webview_window(CLIPBOARD_WINDOW_LABEL) else {
        return;
    };
    if window.is_visible().unwrap_or(false) {
        return;
    }

    let started = std::time::Instant::now();
    if let Err(err) = window.eval(CLIPBOARD_PREWARM_SCRIPT) {
        log::warn!("prewarm clipboard WebContent after screen wake failed: {err}");
        return;
    }
    if let Err(err) = window.with_webview(|platform_webview| {
        let webview_ptr = platform_webview.inner().cast::<WKWebView>();
        if !webview_ptr.is_null() {
            let webview = unsafe { &*webview_ptr };
            webview.setNeedsDisplay(true);
        }
    }) {
        log::warn!("prepare clipboard WebView redraw after screen wake failed: {err}");
        return;
    }

    log::info!(
        "prewarmed clipboard WebView after screen wake: elapsedMs={}",
        started.elapsed().as_millis()
    );
}

pub fn show_window(
    app_handle: &AppHandle,
    label: &str,
    clipboard_show_request: Option<ClipboardShowRequest>,
) -> Result<()> {
    if label == CLIPBOARD_WINDOW_LABEL {
        let request = clipboard_show_request
            .ok_or_else(|| anyhow::anyhow!("clipboard show request is missing"))?;

        show_clipboard_panel(app_handle, request)
    } else {
        let window = get_window(app_handle, label)?;
        window.show().map_err(|e| anyhow::anyhow!(e))?;
        window.unminimize().map_err(|e| anyhow::anyhow!(e))?;
        window.set_focus().map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}

pub fn hide_window(app_handle: &AppHandle, label: &str) -> Result<()> {
    if label == CLIPBOARD_WINDOW_LABEL {
        hide_clipboard_panel(app_handle)
    } else {
        get_window(app_handle, label)?
            .hide()
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}

pub fn show_taskbar_icon(app_handle: &AppHandle, visible: bool) -> Result<()> {
    app_handle
        .set_dock_visibility(visible)
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

/// 点击 dock 图标 reopen 时，无可见窗口则唤起偏好窗口。
pub fn handle_reopen(app_handle: &AppHandle, has_visible_windows: bool) {
    if has_visible_windows {
        return;
    }

    if let Some(settings_store) = app_handle.try_state::<SettingsStore>() {
        if !settings_store.snapshot().onboarding.completed {
            if let Err(err) = super::show_window(app_handle, ONBOARDING_WINDOW_LABEL) {
                log::error!("show onboarding window on reopen failed: {err:?}");
            }
            return;
        }
    }

    if let Err(err) = show_window(app_handle, PREFERENCE_WINDOW_LABEL, None) {
        log::error!("show preference window on reopen failed: {err:?}");
    }
}

/// 所有 panel 方法必须在主线程。
fn show_clipboard_panel(app_handle: &AppHandle, request: ClipboardShowRequest) -> Result<()> {
    let panel_handle = app_handle.clone();
    app_handle
        .run_on_main_thread(move || {
            let main_thread_started = std::time::Instant::now();
            if let Ok(panel) = panel_handle.get_webview_panel(CLIPBOARD_WINDOW_LABEL) {
                panel.show_and_make_key();
                // show 时切到 can_join_all_spaces：跟随用户当前 space 出现。
                panel.set_collection_behavior(
                    CollectionBehavior::new()
                        .stationary()
                        .can_join_all_spaces()
                        .full_screen_auxiliary()
                        .into(),
                );
                super::preview::resume_after_clipboard_show(&panel_handle);
                super::emit_clipboard_visibility_after_show(&panel_handle, request);
                super::lifecycle::on_shown(&panel_handle, CLIPBOARD_WINDOW_LABEL);

                let total_ms = request.requested_at.elapsed().as_millis();
                let main_thread_ms = main_thread_started.elapsed().as_millis();
                if total_ms >= SLOW_CLIPBOARD_SHOW_MS {
                    log::warn!(
                        "slow clipboard native show: requestId={} beforeMainMs={} mainThreadMs={} totalMs={}",
                        request.id,
                        total_ms.saturating_sub(main_thread_ms),
                        main_thread_ms,
                        total_ms
                    );
                } else {
                    log::debug!(
                        "clipboard native show: requestId={} beforeMainMs={} mainThreadMs={} totalMs={}",
                        request.id,
                        total_ms.saturating_sub(main_thread_ms),
                        main_thread_ms,
                        total_ms
                    );
                }
            }
        })
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}

fn hide_clipboard_panel(app_handle: &AppHandle) -> Result<()> {
    let handle = app_handle.clone();
    app_handle
        .run_on_main_thread(move || {
            if let Ok(panel) = handle.get_webview_panel(CLIPBOARD_WINDOW_LABEL) {
                panel.hide();
                // hide 后切回 move_to_active_space：下次 show 时按当前 space 重新落位。
                panel.set_collection_behavior(
                    CollectionBehavior::new()
                        .stationary()
                        .move_to_active_space()
                        .full_screen_auxiliary()
                        .into(),
                );
            }
        })
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

/// 让主 panel 放弃 key 状态，但保持可见——用于固定窗口下的粘贴：
/// panel 仍是 key window 时 CGEvent ⌘V 会被 panel 自身吞掉，resign 后键焦点回到前台 App 的窗口。
pub fn resign_clipboard_panel_key(app_handle: &AppHandle) -> Result<()> {
    let handle = app_handle.clone();
    app_handle
        .run_on_main_thread(move || {
            if let Ok(panel) = handle.get_webview_panel(CLIPBOARD_WINDOW_LABEL) {
                panel.resign_key_window();
            }
        })
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

/// 等待此前投递到 AppKit 主线程的 panel hide / resign 操作真正执行完毕。
/// `run_on_main_thread` 本身只保证入队；没有这个屏障时，模拟粘贴可能先于 panel 让出焦点。
pub async fn wait_for_clipboard_panel_focus_release(app_handle: &AppHandle) -> Result<()> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    app_handle
        .run_on_main_thread(move || {
            let _ = ready_tx.send(());
        })
        .map_err(|error| anyhow::anyhow!(error))?;

    tokio::time::timeout(std::time::Duration::from_secs(1), ready_rx)
        .await
        .map_err(|_| anyhow::anyhow!("wait for clipboard panel focus release timed out"))?
        .map_err(|_| anyhow::anyhow!("clipboard panel focus release was cancelled"))?;

    Ok(())
}

/// 粘贴完成后把 key 状态拿回来：固定窗口模式下用户还要继续用键盘 / 列表操作。
pub fn make_clipboard_panel_key(app_handle: &AppHandle) -> Result<()> {
    let handle = app_handle.clone();
    app_handle
        .run_on_main_thread(move || {
            if let Ok(panel) = handle.get_webview_panel(CLIPBOARD_WINDOW_LABEL) {
                panel.make_key_window();
            }
        })
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}
