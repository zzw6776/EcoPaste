use tauri::{PhysicalPosition, PhysicalSize, WebviewWindow};

use crate::core::Result;
use crate::settings::WindowPosition;

#[cfg(target_os = "android")]
pub fn position_window(_window: &WebviewWindow, _position: WindowPosition) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "android")]
pub fn set_clipboard_height(_window: &WebviewWindow, _height: f64) -> Result<()> {
    Ok(())
}

#[cfg(not(target_os = "android"))]
struct MonitorInfo {
    position: PhysicalPosition<i32>,
    size: PhysicalSize<u32>,
}

#[cfg(not(target_os = "android"))]
fn monitor_from_cursor(
    window: &WebviewWindow,
) -> Result<Option<(MonitorInfo, PhysicalPosition<f64>)>> {
    let cursor = window.cursor_position().map_err(|e| anyhow::anyhow!(e))?;
    let scale = window.scale_factor().map_err(|e| anyhow::anyhow!(e))?;

    let logical = cursor.to_logical::<f64>(scale);

    let monitor = window
        .monitor_from_point(logical.x, logical.y)
        .map_err(|e| anyhow::anyhow!(e))?;

    let Some(monitor) = monitor else {
        return Ok(None);
    };

    Ok(Some((
        MonitorInfo {
            position: *monitor.position(),
            size: *monitor.size(),
        },
        cursor,
    )))
}

#[cfg(not(target_os = "android"))]
pub fn position_window(window: &WebviewWindow, position: WindowPosition) -> Result<()> {
    let Some((monitor, _cursor)) = monitor_from_cursor(window)? else {
        return Ok(());
    };

    match position {
        WindowPosition::Remember => {}
        WindowPosition::FollowCursor => apply_bottom(window, &monitor)?,
        WindowPosition::Center => apply_bottom(window, &monitor)?,
    }

    Ok(())
}

#[cfg(target_os = "android")]
pub(super) fn center_on_cursor_monitor(_window: &WebviewWindow) -> Result<()> {
    Ok(())
}

#[cfg(not(target_os = "android"))]
#[allow(dead_code)]
fn apply_follow(
    window: &WebviewWindow,
    monitor: &MonitorInfo,
    cursor: &PhysicalPosition<f64>,
) -> Result<()> {
    let win_size = window.inner_size().map_err(|e| anyhow::anyhow!(e))?;
    let mon_x = monitor.position.x as f64;
    let mon_y = monitor.position.y as f64;
    let mon_w = monitor.size.width as f64;
    let mon_h = monitor.size.height as f64;

    let x = cursor.x.min(mon_x + mon_w - win_size.width as f64);
    let y = cursor.y.min(mon_y + mon_h - win_size.height as f64);

    window
        .set_position(PhysicalPosition::new(x.round() as i32, y.round() as i32))
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

#[cfg(not(target_os = "android"))]
/// 将窗口停靠到当前光标所在显示器底部居中。
pub(super) fn center_on_cursor_monitor(window: &WebviewWindow) -> Result<()> {
    let Some((monitor, _)) = monitor_from_cursor(window)? else {
        return Ok(());
    };
    apply_bottom(window, &monitor)
}

#[cfg(not(target_os = "android"))]
#[allow(dead_code)]
fn apply_center(window: &WebviewWindow, monitor: &MonitorInfo) -> Result<()> {
    apply_bottom(window, monitor)
}

use std::sync::atomic::{AtomicU32, Ordering};
use tauri::Manager;

static CLIPBOARD_HEIGHT: AtomicU32 = AtomicU32::new(340);

pub fn get_clipboard_height() -> f64 {
    CLIPBOARD_HEIGHT.load(Ordering::Relaxed) as f64
}

pub fn set_cached_clipboard_height(height_logical: f64) {
    let clamped = height_logical.clamp(200.0, 750.0);
    CLIPBOARD_HEIGHT.store(clamped.round() as u32, Ordering::Relaxed);
}

#[cfg(not(target_os = "android"))]
pub fn set_clipboard_height(window: &WebviewWindow, height_logical: f64) -> Result<()> {
    let scale = window.scale_factor().map_err(|e| anyhow::anyhow!(e))?;
    let height_clamped = height_logical.clamp(200.0, 750.0);
    CLIPBOARD_HEIGHT.store(height_clamped.round() as u32, Ordering::Relaxed);

    if let Some(store) = window
        .app_handle()
        .try_state::<crate::window::state::WindowStateStore>()
    {
        let pos = window.outer_position().unwrap_or_default();
        let width = window.inner_size().map(|s| s.width).unwrap_or(800);
        let _ = store.save(
            window.label(),
            crate::window::state::WindowState {
                x: pos.x,
                y: pos.y,
                width,
                height: (height_clamped * scale).round() as u32,
            },
        );
    }

    let Some((monitor, _)) = monitor_from_cursor(window)? else {
        return Ok(());
    };

    let mon_x = monitor.position.x as f64;
    let mon_y = monitor.position.y as f64;
    let mon_w = monitor.size.width as f64;
    let mon_h = monitor.size.height as f64;

    let target_width_logical = ((mon_w / scale) - 32.0).max(600.0);
    let target_size = PhysicalSize::new(
        (target_width_logical * scale).round() as u32,
        (height_clamped * scale).round() as u32,
    );
    let _ = window.set_size(target_size);

    let x = mon_x + (16.0 * scale);
    let y = mon_y + mon_h - (height_clamped * scale);

    window
        .set_position(PhysicalPosition::new(x.round() as i32, y.round() as i32))
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}

#[cfg(not(target_os = "android"))]
fn apply_bottom(window: &WebviewWindow, monitor: &MonitorInfo) -> Result<()> {
    let scale = window.scale_factor().map_err(|e| anyhow::anyhow!(e))?;
    let mon_x = monitor.position.x as f64;
    let mon_y = monitor.position.y as f64;
    let mon_w = monitor.size.width as f64;
    let mon_h = monitor.size.height as f64;

    let height_logical = if let Some(store) = window
        .app_handle()
        .try_state::<crate::window::state::WindowStateStore>()
    {
        if let Some(state) = store.get(window.label()) {
            if state.height > 0 {
                let h = (state.height as f64 / scale).clamp(200.0, 750.0);
                CLIPBOARD_HEIGHT.store(h.round() as u32, Ordering::Relaxed);
                h
            } else {
                get_clipboard_height()
            }
        } else {
            get_clipboard_height()
        }
    } else {
        get_clipboard_height()
    };

    // Paste 官方体验：自适应横跨整个屏幕底栏（左右各留 16px 呼吸边距）
    let target_width_logical = ((mon_w / scale) - 32.0).max(600.0);

    let target_size = PhysicalSize::new(
        (target_width_logical * scale).round() as u32,
        (height_logical * scale).round() as u32,
    );
    let _ = window.set_size(target_size);

    let x = mon_x + (16.0 * scale);
    let y = mon_y + mon_h - (height_logical * scale);

    window
        .set_position(PhysicalPosition::new(x.round() as i32, y.round() as i32))
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}
