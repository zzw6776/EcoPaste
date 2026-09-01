//! Windows 窗口管理：剪贴板窗口默认不可聚焦，输入控件编辑期间临时恢复可聚焦。

use std::sync::Mutex;

use tauri::AppHandle;
use windows::Win32::Foundation::HWND;
use windows::Win32::UI::WindowsAndMessaging::{GetForegroundWindow, IsWindow, SetForegroundWindow};

use super::{get_window, CLIPBOARD_WINDOW_LABEL};
use crate::core::Result;
use crate::{keyboard, mouse};

static PRE_EDIT_FOREGROUND_HWND: Mutex<Option<isize>> = Mutex::new(None);

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
