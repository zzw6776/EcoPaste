//! macOS 窗口管理：剪贴板窗口转 NSPanel（show_and_make_key 拿键盘焦点但不激活 App），
//! 其它窗口走常规 show/hide。

#![allow(clippy::unused_unit)]

use std::cell::RefCell;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use objc2::rc::autoreleasepool;
use objc2::sel;
use objc2_app_kit::{
    NSAppKitVersionNumber, NSApplicationActivationOptions, NSAutoresizingMaskOptions,
    NSGlassEffectView, NSGlassEffectViewStyle, NSRunningApplication, NSView as AppKitView,
    NSVisualEffectBlendingMode, NSVisualEffectMaterial, NSVisualEffectState, NSVisualEffectView,
    NSWindow as AppKitWindow, NSWindowCollectionBehavior, NSWindowOrderingMode, NSWorkspace,
    NSWorkspaceApplicationKey, NSWorkspaceDidActivateApplicationNotification,
};
use objc2_foundation::MainThreadMarker as ObjcMainThreadMarker;
use objc2_web_kit::WKWebView;
use tauri::{AppHandle, Manager, WebviewWindow};
use tauri_nspanel::{
    tauri_panel, CollectionBehavior, ManagerExt, PanelLevel, StyleMask, WebviewWindowExt,
};

use super::{
    get_window, ClipboardNativeShowTiming, ClipboardShowRequest, CLIPBOARD_WINDOW_LABEL,
    ONBOARDING_WINDOW_LABEL, PREFERENCE_WINDOW_LABEL,
};
use crate::core::Result;
use crate::settings::SettingsStore;

const CLIPBOARD_CORNER_RADIUS: f64 = 26.0;
const MIN_APPKIT_VERSION_LIQUID_GLASS: f64 = 2685.0;
const MIN_MACOS_VERSION_JOIN_ALL_APPLICATIONS: isize = 26;
const MIN_MACOS_VERSION_PREVENTS_ACTIVATION_COMPAT: isize = 27;
const PASTE_TARGET_ACTIVATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const PASTE_TARGET_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
const SLOW_CLIPBOARD_SHOW_MS: u128 = 100;

static ACTIVE_CLIPBOARD_SHOW_TRACE: LazyLock<Mutex<Option<ClipboardShowRequest>>> =
    LazyLock::new(|| Mutex::new(None));
static CLIPBOARD_PASTE_TARGET_PID: LazyLock<Mutex<Option<i32>>> =
    LazyLock::new(|| Mutex::new(None));

thread_local! {
    static PASTE_TARGET_OBSERVER: RefCell<Option<Retained<PasteTargetObserver>>> = const {
        RefCell::new(None)
    };
}

define_class!(
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "EcoPastePasteTargetObserver"]
    #[ivars = AppHandle]
    struct PasteTargetObserver;

    unsafe impl NSObjectProtocol for PasteTargetObserver {}

    impl PasteTargetObserver {
        /// 固定面板保持可见时，跟随用户主动切换的外部应用更新粘贴目标。
        #[unsafe(method(applicationDidActivate:))]
        fn application_did_activate(&self, notification: &NSNotification) {
            let Some(info) = notification.userInfo() else {
                return;
            };
            let Some(application) = info.objectForKey(unsafe { NSWorkspaceApplicationKey }) else {
                return;
            };
            let Some(application) = application.downcast_ref::<NSRunningApplication>() else {
                return;
            };
            let pid = application.processIdentifier();
            if pid <= 0 || pid == NSRunningApplication::currentApplication().processIdentifier() {
                return;
            }

            let visible = self
                .ivars()
                .get_webview_panel(CLIPBOARD_WINDOW_LABEL)
                .is_ok_and(|panel| panel.is_visible());
            if !visible {
                return;
            }

            if let Ok(mut target) = CLIPBOARD_PASTE_TARGET_PID.lock() {
                *target = Some(pid);
            }
        }
    }
);

tauri_panel! {
    panel!(MainPanel {
        config: {
            is_floating_panel: true,
            can_become_key_window: true,
            can_become_main_window: false
        }
    })

    panel_event!(MainPanelEventHandler {
        window_did_become_key(notification: &NSNotification) -> (),
        window_did_change_occlusion_state(notification: &NSNotification) -> (),
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
    show_taskbar_icon(app_handle, false)?;

    let clipboard_window = get_window(app_handle, CLIPBOARD_WINDOW_LABEL)?;

    // 必须在 to_panel 改写 NSWindow 类之前重挂载 WKWebView，否则 WebKit 注销窗口 KVO 时会崩溃。
    schedule_clipboard_surface(&clipboard_window)?;

    let panel = clipboard_window
        .to_panel::<MainPanel>()
        .map_err(|e| anyhow::anyhow!("to_panel failed: {e:?}"))?;

    panel.set_style_mask(
        StyleMask::empty()
            .borderless()
            .resizable()
            .nonactivating_panel()
            .into(),
    );
    synchronize_panel_prevents_activation(panel.as_panel());
    panel.set_transparent(true);
    configure_clipboard_panel_spaces(panel.as_panel());

    panel.set_corner_radius(CLIPBOARD_CORNER_RADIUS);

    let handler = MainPanelEventHandler::new();

    handler.window_did_become_key(|_| {
        let request = ACTIVE_CLIPBOARD_SHOW_TRACE
            .lock()
            .ok()
            .and_then(|guard| *guard);
        if let Some(request) = request {
            log::info!(
                "clipboard show AppKit trace: requestId={} phase=becameKey totalUs={}",
                request.id,
                elapsed_us(request.requested_at)
            );
        }
    });

    let occlusion_handle = app_handle.clone();
    handler.window_did_change_occlusion_state(move |_| {
        let visible = occlusion_handle
            .get_webview_panel(CLIPBOARD_WINDOW_LABEL)
            .map(|panel| panel.is_visible())
            .unwrap_or(false);
        if !visible {
            return;
        }

        let request = ACTIVE_CLIPBOARD_SHOW_TRACE
            .lock()
            .ok()
            .and_then(|mut guard| guard.take());
        if let Some(request) = request {
            log::info!(
                "clipboard show AppKit trace: requestId={} phase=occlusionVisible totalUs={}",
                request.id,
                elapsed_us(request.requested_at)
            );
        }
    });

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
    register_paste_target_observer(app_handle)?;

    Ok(())
}

/// macOS 26 起允许非激活面板覆盖其他应用的全屏 Space，同时避免失活时自动隐藏。
fn configure_clipboard_panel_spaces(panel: &objc2_app_kit::NSPanel) {
    let major_version = objc2_foundation::NSProcessInfo::processInfo()
        .operatingSystemVersion()
        .majorVersion;
    let mut behavior = CollectionBehavior::new()
        .stationary()
        .can_join_all_spaces()
        .full_screen_auxiliary()
        .value();

    let level = if major_version >= MIN_MACOS_VERSION_JOIN_ALL_APPLICATIONS {
        behavior |= NSWindowCollectionBehavior::CanJoinAllApplications;
        panel.setHidesOnDeactivate(false);
        PanelLevel::ScreenSaver
    } else {
        PanelLevel::Dock
    };

    panel.setLevel(level.value() as isize);
    panel.setCollectionBehavior(behavior);
}

/// 修正 macOS 27 中运行时从 NSWindow 转换为 NSPanel 后未同步的禁止激活标记。
/// tauri-nspanel 无法走 NSPanel 初始化器，只能在转换后通过仍存在的 AppKit selector 同步标记；
/// selector 缺失时保留公共 API 的粘贴前焦点恢复兜底。
fn synchronize_panel_prevents_activation(panel: &objc2_app_kit::NSPanel) {
    let major_version = objc2_foundation::NSProcessInfo::processInfo()
        .operatingSystemVersion()
        .majorVersion;
    if major_version < MIN_MACOS_VERSION_PREVENTS_ACTIVATION_COMPAT {
        return;
    }

    let selector = objc2::sel!(_setPreventsActivation:);
    let responds: bool = unsafe { objc2::msg_send![panel, respondsToSelector: selector] };
    if !responds {
        log::warn!("NSPanel does not support _setPreventsActivation: compatibility selector");
        return;
    }

    unsafe {
        let _: () = objc2::msg_send![panel, _setPreventsActivation: true];
    }
    log::info!("synchronized NSPanel prevents-activation state for macOS {major_version}");
}

/// 在 AppKit 主线程保留一个工作区观察者，避免重建面板时重复注册。
fn register_paste_target_observer(app_handle: &AppHandle) -> Result<()> {
    let main_thread = ObjcMainThreadMarker::new()
        .ok_or_else(|| anyhow::anyhow!("clipboard target observer requires the main thread"))?;

    PASTE_TARGET_OBSERVER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return;
        }

        let observer = PasteTargetObserver::alloc(main_thread).set_ivars(app_handle.clone());
        // NSWorkspace 的应用激活通知在主线程交付；slot 持有观察者直到主线程退出。
        let observer: Retained<PasteTargetObserver> = unsafe { msg_send![super(observer), init] };
        unsafe {
            NSWorkspace::sharedWorkspace()
                .notificationCenter()
                .addObserver_selector_name_object(
                    &observer,
                    sel!(applicationDidActivate:),
                    Some(NSWorkspaceDidActivateApplicationNotification),
                    None,
                );
        }
        *slot = Some(observer);
    });

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
            let main_thread_started = Instant::now();
            let panel_lookup_started = Instant::now();
            if let Ok(panel) = panel_handle.get_webview_panel(CLIPBOARD_WINDOW_LABEL) {
                remember_clipboard_paste_target();
                let panel_lookup_us = elapsed_us(panel_lookup_started);
                if let Ok(mut trace) = ACTIVE_CLIPBOARD_SHOW_TRACE.lock() {
                    *trace = Some(request);
                }

                let content_view_started = Instant::now();
                let content_view = panel.content_view();
                let content_view_us = elapsed_us(content_view_started);

                let first_responder_started = Instant::now();
                let first_responder_accepted = panel.make_first_responder(Some(&content_view));
                let first_responder_us = elapsed_us(first_responder_started);

                let order_front_started = Instant::now();
                panel.order_front_regardless();
                let order_front_us = elapsed_us(order_front_started);

                let make_key_started = Instant::now();
                panel.make_key_window();
                let make_key_us = elapsed_us(make_key_started);

                let preview_resume_started = Instant::now();
                super::preview::resume_after_clipboard_show();
                let preview_resume_us = elapsed_us(preview_resume_started);

                let native_timing = ClipboardNativeShowTiming {
                    content_view_us,
                    first_responder_accepted,
                    first_responder_us,
                    layout_position_us: request.layout_position_us,
                    layout_restore_us: request.layout_restore_us,
                    main_queue_us: duration_us(request.layout_completed_at, main_thread_started),
                    make_key_us,
                    native_completed_ms: elapsed_ms(request.requested_at),
                    order_front_us,
                    panel_lookup_us,
                    preview_resume_us,
                };

                let emit_started = Instant::now();
                super::emit_clipboard_visibility_after_show(&panel_handle, request, native_timing);
                let emit_us = elapsed_us(emit_started);

                let lifecycle_started = Instant::now();
                super::lifecycle::on_shown(&panel_handle, CLIPBOARD_WINDOW_LABEL);
                let lifecycle_us = elapsed_us(lifecycle_started);

                let total_ms = request.requested_at.elapsed().as_millis();
                let message = format!(
                    "clipboard show native trace: requestId={} layoutRestoreUs={} layoutPositionUs={} mainQueueUs={} panelLookupUs={} contentViewUs={} firstResponderUs={} firstResponderAccepted={} orderFrontUs={} makeKeyUs={} previewResumeUs={} emitUs={} lifecycleUs={} totalMs={}",
                    request.id,
                    native_timing.layout_restore_us,
                    native_timing.layout_position_us,
                    native_timing.main_queue_us,
                    native_timing.panel_lookup_us,
                    native_timing.content_view_us,
                    native_timing.first_responder_us,
                    native_timing.first_responder_accepted,
                    native_timing.order_front_us,
                    native_timing.make_key_us,
                    native_timing.preview_resume_us,
                    emit_us,
                    lifecycle_us,
                    total_ms
                );
                if total_ms >= SLOW_CLIPBOARD_SHOW_MS {
                    log::warn!("{message}");
                } else {
                    log::info!("{message}");
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
            if let Ok(mut trace) = ACTIVE_CLIPBOARD_SHOW_TRACE.lock() {
                *trace = None;
            }
            if let Ok(panel) = handle.get_webview_panel(CLIPBOARD_WINDOW_LABEL) {
                panel.hide();
            }
        })
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

fn duration_us(started: Instant, completed: Instant) -> u64 {
    completed
        .saturating_duration_since(started)
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
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

/// 等待此前投递到 AppKit 主线程的 panel 操作真正执行完毕。
/// `run_on_main_thread` 本身只保证入队；hide / resign 后需要屏障再模拟粘贴，
/// 失败后重新 show 也需要屏障保证前端错误提示可见。
pub async fn wait_for_clipboard_panel_main_thread(app_handle: &AppHandle) -> Result<()> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    app_handle
        .run_on_main_thread(move || {
            let _ = ready_tx.send(());
        })
        .map_err(|error| anyhow::anyhow!(error))?;

    tokio::time::timeout(std::time::Duration::from_secs(1), ready_rx)
        .await
        .map_err(|_| anyhow::anyhow!("wait for clipboard panel main thread timed out"))?
        .map_err(|_| anyhow::anyhow!("clipboard panel main thread wait was cancelled"))?;

    Ok(())
}

/// 在粘贴开始时固定目标，避免数据库等待期间应用切换改变本次粘贴的接收方。
pub fn clipboard_paste_target_pid() -> Result<Option<i32>> {
    let target = CLIPBOARD_PASTE_TARGET_PID
        .lock()
        .map_err(|_| anyhow::anyhow!("clipboard paste target state poisoned"))?;

    Ok(*target)
}

/// 面板让出 key 状态后恢复原前台应用；应用活动状态不等同于输入控件焦点。
/// 此处兼容鼠标点击面板会激活 EcoPaste 的系统行为，仍需先执行 hide / resign 屏障。
pub async fn restore_clipboard_paste_target(
    app_handle: &AppHandle,
    target_pid: Option<i32>,
) -> Result<()> {
    let Some(target_pid) = target_pid else {
        return Err(anyhow::anyhow!("clipboard paste target was not captured").into());
    };

    if is_clipboard_paste_target_active(target_pid)? {
        return Ok(());
    }

    let (activation_tx, activation_rx) = tokio::sync::oneshot::channel();
    app_handle
        .run_on_main_thread(move || {
            let front_pid = NSWorkspace::sharedWorkspace()
                .frontmostApplication()
                .map(|application| application.processIdentifier());
            let own_pid = NSRunningApplication::currentApplication().processIdentifier();
            // 数据准备期间用户若切换到了第三个应用，不抢回焦点执行已过期的粘贴。
            if front_pid.is_some_and(|pid| pid != own_pid && pid != target_pid) {
                let _ = activation_tx.send(false);
                return;
            }

            log::info!(
                "clipboard paste focus restore: targetPid={target_pid} frontPid={front_pid:?}"
            );
            let accepted =
                NSRunningApplication::runningApplicationWithProcessIdentifier(target_pid)
                    .filter(|target| !target.isTerminated())
                    .is_some_and(|target| {
                        target.activateWithOptions(NSApplicationActivationOptions::empty())
                    });
            let _ = activation_tx.send(accepted);
        })
        .map_err(|error| anyhow::anyhow!(error))?;

    let accepted = tokio::time::timeout(PASTE_TARGET_ACTIVATION_TIMEOUT, activation_rx)
        .await
        .map_err(|_| anyhow::anyhow!("restore clipboard paste target request timed out"))?
        .map_err(|_| anyhow::anyhow!("restore clipboard paste target request was cancelled"))?;
    if !accepted {
        return Err(anyhow::anyhow!("clipboard paste target rejected activation").into());
    }

    let started = Instant::now();
    while started.elapsed() < PASTE_TARGET_ACTIVATION_TIMEOUT {
        if is_clipboard_paste_target_active(target_pid)? {
            return Ok(());
        }
        tokio::time::sleep(PASTE_TARGET_POLL_INTERVAL).await;
    }

    Err(anyhow::anyhow!("clipboard paste target did not become active").into())
}

/// 在面板取得 key 状态前记录真正接收键盘事件的外部应用。
fn remember_clipboard_paste_target() {
    let target_pid = autoreleasepool(|_| {
        let current_pid = NSRunningApplication::currentApplication().processIdentifier();
        NSWorkspace::sharedWorkspace()
            .frontmostApplication()
            .map(|application| application.processIdentifier())
            .filter(|pid| *pid > 0 && *pid != current_pid)
    });

    if let Ok(mut target) = CLIPBOARD_PASTE_TARGET_PID.lock() {
        *target = target_pid;
    }
}

fn is_clipboard_paste_target_active(target_pid: i32) -> Result<bool> {
    autoreleasepool(|_| {
        let target = NSRunningApplication::runningApplicationWithProcessIdentifier(target_pid)
            .ok_or_else(|| anyhow::anyhow!("clipboard paste target is no longer running"))?;
        if target.isTerminated() {
            return Err(anyhow::anyhow!("clipboard paste target has terminated").into());
        }

        Ok(target.isActive())
    })
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
