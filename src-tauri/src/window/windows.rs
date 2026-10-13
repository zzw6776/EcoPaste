//! Windows 窗口管理：剪贴板窗口默认不可聚焦，输入控件编辑期间临时恢复可聚焦。

use std::cell::RefCell;
use std::ptr::null_mut;
use std::sync::{Mutex, OnceLock};

use tauri::{AppHandle, Manager, WebviewWindow};
use winapi::shared::windef::{HWINEVENTHOOK, HWND as NativeHwnd};
use winapi::um::winuser::{
    GetWindowThreadProcessId, SetWinEventHook, UnhookWinEvent, EVENT_SYSTEM_FOREGROUND,
    WINEVENT_OUTOFCONTEXT,
};
use windows::Win32::Foundation::HWND;
use windows::Win32::UI::WindowsAndMessaging::{
    GetForegroundWindow, IsWindow, SetForegroundWindow, SetWindowPos, HWND_BOTTOM, HWND_TOPMOST,
    SWP_NOACTIVATE, SWP_NOMOVE, SWP_NOSIZE,
};

use super::{get_window, CLIPBOARD_WINDOW_LABEL, UPDATE_WINDOW_LABEL};
use crate::core::Result;
use crate::{keyboard, mouse};

static PRE_EDIT_FOREGROUND_HWND: Mutex<Option<isize>> = Mutex::new(None);
static UPDATE_FOREGROUND_APP: OnceLock<AppHandle> = OnceLock::new();

thread_local! {
    static UPDATE_FOREGROUND_HOOK: RefCell<Option<UpdateForegroundHook>> = const {
        RefCell::new(None)
    };
}

struct UpdateForegroundHook(HWINEVENTHOOK);

impl Drop for UpdateForegroundHook {
    fn drop(&mut self) {
        unsafe { UnhookWinEvent(self.0) };
    }
}

/// 更新窗口创建时在已有 UI 消息循环中监听前台应用变化，不新增轮询线程。
pub fn configure_update_window(window: &WebviewWindow) -> Result<()> {
    let _ = UPDATE_FOREGROUND_APP.set(window.app_handle().clone());
    window
        .with_webview(|_| {
            UPDATE_FOREGROUND_HOOK.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.is_some() {
                    return;
                }

                let hook = unsafe {
                    SetWinEventHook(
                        EVENT_SYSTEM_FOREGROUND,
                        EVENT_SYSTEM_FOREGROUND,
                        null_mut(),
                        Some(update_foreground_changed),
                        0,
                        0,
                        WINEVENT_OUTOFCONTEXT,
                    )
                };
                if hook.is_null() {
                    log::error!("register update foreground hook failed");
                    return;
                }
                *slot = Some(UpdateForegroundHook(hook));
            });
        })
        .map_err(|err| anyhow::anyhow!(err).into())
}

/// 读取当前前台窗口，避免异步通知中的旧 HWND 导致快速切换后仍然置顶。
unsafe extern "system" fn update_foreground_changed(
    _hook: HWINEVENTHOOK,
    _event: u32,
    _hwnd: NativeHwnd,
    _object: i32,
    _child: i32,
    _thread: u32,
    _time: u32,
) {
    let Some(app_handle) = UPDATE_FOREGROUND_APP.get() else {
        return;
    };
    if let Err(err) = set_update_window_foreground(app_handle, is_application_foreground()) {
        log::warn!("update window activation ordering failed: {err}");
    }
}

fn is_application_foreground() -> bool {
    let mut pid = 0;
    unsafe { GetWindowThreadProcessId(GetForegroundWindow().0 as NativeHwnd, &mut pid) };
    pid == std::process::id()
}

/// 唤出时位于 EcoPaste 上方，外部应用激活后取消置顶并退到其它普通窗口后方。
pub(super) fn set_update_window_foreground(app_handle: &AppHandle, foreground: bool) -> Result<()> {
    let Some(window) = app_handle.get_webview_window(UPDATE_WINDOW_LABEL) else {
        return Ok(());
    };
    if !window.is_visible().unwrap_or(false) {
        return Ok(());
    }

    let raw_hwnd = window.hwnd().map_err(|err| anyhow::anyhow!(err))?;
    let order = if foreground {
        HWND_TOPMOST
    } else {
        HWND_BOTTOM
    };
    unsafe {
        SetWindowPos(
            HWND(raw_hwnd.0 as isize),
            order,
            0,
            0,
            0,
            0,
            SWP_NOACTIVATE | SWP_NOMOVE | SWP_NOSIZE,
        )
        .map_err(|err| anyhow::anyhow!(err))?;
    }
    Ok(())
}

pub fn show_window(app_handle: &AppHandle, label: &str) -> Result<()> {
    let window = get_window(app_handle, label)?;
    if label == CLIPBOARD_WINDOW_LABEL {
        window
            .set_focusable(false)
            .map_err(|e| anyhow::anyhow!(e))?;
        if let Err(err) = window.set_shadow(false) {
            log::warn!("disable clipboard window shadow failed: {err}");
        }
        clear_pre_edit_foreground();
    }

    window.show().map_err(|e| anyhow::anyhow!(e))?;
    window.unminimize().map_err(|e| anyhow::anyhow!(e))?;

    if label == CLIPBOARD_WINDOW_LABEL {
        if let Err(err) = window_vibrancy::apply_blur(&window, None) {
            log::warn!("apply clipboard blur effect failed: {err}");
        }
        keyboard::enable_navigation_keys(app_handle);
        mouse::enable_outside_click_hide(app_handle);
    } else {
        window.set_focus().map_err(|e| anyhow::anyhow!(e))?;
        if label == UPDATE_WINDOW_LABEL {
            set_update_window_foreground(app_handle, true)?;
        }
    }

    Ok(())
}

pub fn set_clipboard_window_editing(app_handle: &AppHandle, editing: bool) -> Result<()> {
    let window = get_window(app_handle, CLIPBOARD_WINDOW_LABEL)?;
    let raw_hwnd = window.hwnd().map_err(|e| anyhow::anyhow!(e))?;
    let hwnd = HWND(raw_hwnd.0 as isize);

    if editing {
        if !keyboard::cancel_search_handoff_and_suspend(None) {
            keyboard::suspend_navigation_keys();
        }
        remember_pre_edit_foreground(hwnd);
        window.set_focusable(true).map_err(|e| anyhow::anyhow!(e))?;
        window.set_focus().map_err(|e| anyhow::anyhow!(e))?;

        return Ok(());
    }

    let should_restore_foreground = unsafe { GetForegroundWindow() == hwnd };
    window
        .set_focusable(false)
        .map_err(|e| anyhow::anyhow!(e))?;

    if window.is_visible().unwrap_or(false) {
        keyboard::enable_navigation_keys(app_handle);
        mouse::enable_outside_click_hide(app_handle);
    }

    if should_restore_foreground {
        restore_pre_edit_foreground(hwnd);
    } else {
        clear_pre_edit_foreground();
    }

    Ok(())
}

/// 搜索首字符交接的第一阶段：保留低级钩子，仅让窗口和输入框具备取得焦点的条件。
pub fn prepare_clipboard_search_handoff(app_handle: &AppHandle, session_id: u64) -> Result<bool> {
    if !keyboard::is_search_handoff_active(session_id) {
        return Ok(false);
    }

    let window = get_window(app_handle, CLIPBOARD_WINDOW_LABEL)?;
    if !window.is_visible().unwrap_or(false) {
        keyboard::cancel_search_handoff(Some(session_id));
        return Ok(false);
    }

    let raw_hwnd = window.hwnd().map_err(|e| anyhow::anyhow!(e))?;
    let hwnd = HWND(raw_hwnd.0 as isize);
    remember_pre_edit_foreground(hwnd);
    window.set_focusable(true).map_err(|e| anyhow::anyhow!(e))?;
    window.set_focus().map_err(|e| anyhow::anyhow!(e))?;

    Ok(true)
}

/// 搜索输入框确认真实焦点后的第二阶段：校验前台窗口并重放交接期间的按键。
pub fn confirm_clipboard_search_handoff(app_handle: &AppHandle, session_id: u64) -> Result<bool> {
    if !keyboard::is_search_handoff_active(session_id) {
        return Ok(false);
    }

    let window = get_window(app_handle, CLIPBOARD_WINDOW_LABEL)?;
    let raw_hwnd = window.hwnd().map_err(|e| anyhow::anyhow!(e))?;
    let hwnd = HWND(raw_hwnd.0 as isize);
    let valid_target =
        window.is_visible().unwrap_or(false) && unsafe { GetForegroundWindow() == hwnd };
    if !valid_target {
        cancel_clipboard_search_handoff(app_handle, Some(session_id))?;
        return Ok(false);
    }

    keyboard::confirm_search_handoff(session_id)
}

/// 取消当前或指定的焦点交接，并恢复剪贴板窗口的导航态与原前台窗口。
pub fn cancel_clipboard_search_handoff(
    app_handle: &AppHandle,
    session_id: Option<u64>,
) -> Result<bool> {
    if !keyboard::cancel_search_handoff_and_suspend(session_id) {
        return Ok(false);
    }

    let window = get_window(app_handle, CLIPBOARD_WINDOW_LABEL)?;
    let raw_hwnd = window.hwnd().map_err(|e| anyhow::anyhow!(e))?;
    let hwnd = HWND(raw_hwnd.0 as isize);
    let should_restore_foreground = unsafe { GetForegroundWindow() == hwnd };
    window
        .set_focusable(false)
        .map_err(|e| anyhow::anyhow!(e))?;

    if window.is_visible().unwrap_or(false) {
        keyboard::enable_navigation_keys(app_handle);
        mouse::enable_outside_click_hide(app_handle);
    }

    if should_restore_foreground {
        restore_pre_edit_foreground(hwnd);
    } else {
        clear_pre_edit_foreground();
    }

    Ok(true)
}

pub fn hide_window(app_handle: &AppHandle, label: &str) -> Result<()> {
    let window = get_window(app_handle, label)?;
    window.hide().map_err(|e| anyhow::anyhow!(e))?;
    if label == CLIPBOARD_WINDOW_LABEL {
        keyboard::cancel_search_handoff(None);
        if let Err(err) = window.set_focusable(false) {
            log::warn!("reset clipboard window focusable on hide failed: {err:?}");
        }
        clear_pre_edit_foreground();
        keyboard::disable_navigation_keys();
        mouse::disable_outside_click_hide();
        crate::menu::context_window::hide(app_handle);
        set_update_window_foreground(app_handle, is_application_foreground())?;
    }

    Ok(())
}

fn remember_pre_edit_foreground(clipboard_hwnd: HWND) {
    let mut guard = PRE_EDIT_FOREGROUND_HWND
        .lock()
        .expect("pre edit foreground hwnd poisoned");
    if guard.is_some() {
        return;
    }

    let foreground = unsafe { GetForegroundWindow() };
    if foreground.0 == 0 || foreground == clipboard_hwnd {
        return;
    }

    *guard = Some(foreground.0);
}

fn restore_pre_edit_foreground(clipboard_hwnd: HWND) {
    let previous = PRE_EDIT_FOREGROUND_HWND
        .lock()
        .expect("pre edit foreground hwnd poisoned")
        .take();
    let Some(previous) = previous else {
        return;
    };

    let previous_hwnd = HWND(previous);
    if previous_hwnd == clipboard_hwnd || !unsafe { IsWindow(previous_hwnd).as_bool() } {
        return;
    }

    if !unsafe { SetForegroundWindow(previous_hwnd).as_bool() } {
        log::debug!("restore pre-edit foreground window was rejected by Windows");
    }
}

fn clear_pre_edit_foreground() {
    PRE_EDIT_FOREGROUND_HWND
        .lock()
        .expect("pre edit foreground hwnd poisoned")
        .take();
}

pub fn show_taskbar_icon(app_handle: &AppHandle, visible: bool) -> Result<()> {
    let window = get_window(app_handle, CLIPBOARD_WINDOW_LABEL)?;
    window
        .set_skip_taskbar(!visible)
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}
