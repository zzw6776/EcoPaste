//! Windows 系统托盘图标适配：按系统任务栏小图标尺寸生成像素对齐的单色线条图标，并监听浅/深色切换。

use std::f32::consts::{FRAC_PI_2, PI, TAU};
use std::os::windows::ffi::OsStrExt;

use tauri::image::Image;
use tauri::AppHandle;
use windows::core::PCWSTR;
use windows::Win32::System::Registry::{
    RegCloseKey, RegNotifyChangeKeyValue, RegOpenKeyExW, HKEY, HKEY_CURRENT_USER, KEY_NOTIFY,
    REG_NOTIFY_CHANGE_LAST_SET,
};
use windows::Win32::UI::WindowsAndMessaging::{GetSystemMetrics, SM_CXSMICON};
use windows_registry::CURRENT_USER;

const PERSONALIZE_KEY: &str = r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize";

struct RegistryKeyGuard(HKEY);

impl Drop for RegistryKeyGuard {
    fn drop(&mut self) {
        if !self.0.is_invalid() {
            unsafe {
                let _ = RegCloseKey(self.0);
            }
        }
    }
}

/// 读取当前 Windows 任务栏是否为浅色模式。
pub(super) fn is_taskbar_light() -> bool {
    let Ok(key) = CURRENT_USER.open(PERSONALIZE_KEY) else {
        return false;
    };

    if let Ok(value) = key.get_u32("SystemUsesLightTheme") {
        return value != 0;
    }

    if let Ok(value) = key.get_u32("AppsUseLightTheme") {
        return value != 0;
    }

    false
}

/// 生成像素网格对齐的 Windows 托盘单色线条图标（浅色任务栏黑线，深色任务栏白线）。
pub(super) fn build_tray_icon(light_taskbar: bool) -> Image<'static> {
    let size = unsafe { GetSystemMetrics(SM_CXSMICON) }.clamp(16, 128) as u32;
    let n = size as f32;

    // 比例与 macOS `tray-mac.ico` 保持一致（约 4.8% 线宽），横竖线对齐像素中心避免双像素发粗。
    let snap = |v: f32| -> f32 { ((v * n - 0.5).round() + 0.5) / n };

    let ol_x = snap(0.135);
    let ol_y = snap(0.131);
    let ol_x2 = snap(0.588);
    let ol_y2 = snap(0.584);
    let ol_r = (0.105_f32).max(1.5 / n);

    let ib_x1 = snap(0.273);
    let ib_y1 = snap(0.314);
    let ib_x2 = snap(0.836);
    let ib_y2 = snap(0.873);
    let ib_top_end = snap(0.610);
    let ib_right_start = snap(0.500);
    let ib_r = (0.105_f32).max(1.5 / n);

    let ar_x1 = snap(0.566);
    let ar_y1 = snap(0.545);
    let ar_x2 = snap(0.855);
    let ar_y2 = snap(0.256);
    let ar_hx = snap(0.715);
    let ar_vy = snap(0.396);

    let stroke_px = (n * 0.048).clamp(0.95, 2.2);
    let half_w = (stroke_px / n) * 0.5;
    let aa = (1.0 / n) * 0.9;

    let (r, g, b) = if light_taskbar {
        (0_u8, 0_u8, 0_u8)
    } else {
        (255_u8, 255_u8, 255_u8)
    };

    let mut rgba = vec![0_u8; (size * size * 4) as usize];
    for py_i in 0..size {
        let py = (py_i as f32 + 0.5) / n;
        for px_i in 0..size {
            let px = (px_i as f32 + 0.5) / n;
            let d = [
                dist_segment(px, py, ol_x, ol_y + ol_r, ol_x, ol_y2),
                dist_arc(px, py, ol_x + ol_r, ol_y + ol_r, ol_r, PI, PI + FRAC_PI_2),
                dist_segment(px, py, ol_x + ol_r, ol_y, ol_x2, ol_y),
                dist_segment(px, py, ib_x1 + ib_r, ib_y1, ib_top_end, ib_y1),
                dist_arc(px, py, ib_x1 + ib_r, ib_y1 + ib_r, ib_r, PI, PI + FRAC_PI_2),
                dist_segment(px, py, ib_x1, ib_y1 + ib_r, ib_x1, ib_y2 - ib_r),
                dist_arc(px, py, ib_x1 + ib_r, ib_y2 - ib_r, ib_r, FRAC_PI_2, PI),
                dist_segment(px, py, ib_x1 + ib_r, ib_y2, ib_x2 - ib_r, ib_y2),
                dist_arc(px, py, ib_x2 - ib_r, ib_y2 - ib_r, ib_r, 0.0, FRAC_PI_2),
                dist_segment(px, py, ib_x2, ib_y2 - ib_r, ib_x2, ib_right_start),
                dist_segment(px, py, ar_x1, ar_y1, ar_x2, ar_y2),
                dist_segment(px, py, ar_x1, ar_y1, ar_hx, ar_y1),
                dist_segment(px, py, ar_x1, ar_y1, ar_x1, ar_vy),
            ]
            .into_iter()
            .fold(f32::INFINITY, f32::min);

            let coverage = ((half_w + aa * 0.5 - d) / aa).clamp(0.0, 1.0);
            let idx = ((py_i * size + px_i) * 4) as usize;
            rgba[idx] = r;
            rgba[idx + 1] = g;
            rgba[idx + 2] = b;
            rgba[idx + 3] = (coverage * 255.0).round() as u8;
        }
    }

    Image::new_owned(rgba, size, size)
}

/// 计算点 `(px, py)` 到线段 `(ax, ay)-(bx, by)` 的最短欧氏距离。
fn dist_segment(px: f32, py: f32, ax: f32, ay: f32, bx: f32, by: f32) -> f32 {
    let dx = bx - ax;
    let dy = by - ay;
    let len_sq = dx * dx + dy * dy;
    if len_sq == 0.0 {
        return (px - ax).hypot(py - ay);
    }
    let t = (((px - ax) * dx + (py - ay) * dy) / len_sq).clamp(0.0, 1.0);
    (px - (ax + t * dx)).hypot(py - (ay + t * dy))
}

/// 计算点 `(px, py)` 到圆弧 `(cx, cy, r, a0..a1)` 的最短欧氏距离。
fn dist_arc(px: f32, py: f32, cx: f32, cy: f32, r: f32, a0: f32, a1: f32) -> f32 {
    let ang = (py - cy).atan2(px - cx);
    let span = (a1 - a0).rem_euclid(TAU);
    let rel = (ang - a0).rem_euclid(TAU);
    if rel <= span {
        return ((px - cx).hypot(py - cy) - r).abs();
    }
    let d0 = (px - (cx + r * a0.cos())).hypot(py - (cy + r * a0.sin()));
    let d1 = (px - (cx + r * a1.cos())).hypot(py - (cy + r * a1.sin()));
    d0.min(d1)
}

/// 后台阻塞监听任务栏主题注册表变化，切换浅/深色时实时刷新托盘图标。
pub(super) fn spawn_theme_watcher(app: AppHandle, tray_id: &'static str, initial_light: bool) {
    let result = std::thread::Builder::new()
        .name("tray-theme-watcher".into())
        .spawn(move || {
            let subkey: Vec<u16> = std::ffi::OsStr::new(PERSONALIZE_KEY)
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();

            let mut raw_hkey = HKEY::default();
            if let Err(err) = unsafe {
                RegOpenKeyExW(
                    HKEY_CURRENT_USER,
                    PCWSTR(subkey.as_ptr()),
                    0,
                    KEY_NOTIFY,
                    &mut raw_hkey,
                )
            } {
                log::warn!("tray theme watcher open registry failed: {err}");
                return;
            }
            let guard = RegistryKeyGuard(raw_hkey);
            let mut last_light = initial_light;

            loop {
                if let Err(err) = unsafe {
                    RegNotifyChangeKeyValue(guard.0, false, REG_NOTIFY_CHANGE_LAST_SET, None, false)
                } {
                    log::warn!("tray theme watcher notify failed: {err}");
                    break;
                }

                let next_light = is_taskbar_light();
                if next_light == last_light {
                    continue;
                }
                last_light = next_light;

                let Some(tray) = app.tray_by_id(tray_id) else {
                    break;
                };
                let next_icon = build_tray_icon(next_light);
                if let Err(err) = tray.set_icon(Some(next_icon)) {
                    log::warn!("tray theme update set_icon failed: {err}");
                }
            }
        });

    if let Err(err) = result {
        log::warn!("spawn tray theme watcher failed: {err}");
    }
}
