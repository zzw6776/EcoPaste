//! 自启动：直接用 `auto-launch` crate 实现，绕过 `tauri-plugin-autostart` 上游 bug
//! （tauri-apps/plugins-workspace#1922：macOS 下 `is_enabled` 误报、`enable` 路径写错）。
//!
//! 启动参数固定追加 `--auto-launch`，用于识别本次启动来源。

#[cfg(not(target_os = "android"))]
use std::env;

use tauri::AppHandle;
#[cfg(not(target_os = "android"))]
use tauri::Manager;

#[cfg(not(target_os = "android"))]
use crate::core::AppError;
use crate::core::Result;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "macos")]
use macos::PlatformAutostart;
#[cfg(target_os = "windows")]
use windows::PlatformAutostart;

#[cfg(not(target_os = "android"))]
pub(super) const AUTO_LAUNCH_ARG: &str = "--auto-launch";

#[cfg(not(target_os = "android"))]
pub struct AutostartManager {
    platform: PlatformAutostart,
}

#[cfg(not(target_os = "android"))]
pub fn init(app: &AppHandle) -> Result<()> {
    let exe = env::current_exe().map_err(|err| {
        log::error!("autostart init: current_exe failed: {err}");
        AppError::Other(anyhow::anyhow!("{err}"))
    })?;
    let exe_path = exe.to_string_lossy().to_string();

    let app_name = app.package_info().name.clone();

    let platform = PlatformAutostart::new(&app_name, &exe_path)?;

    app.manage(AutostartManager { platform });
    Ok(())
}

#[cfg(not(target_os = "android"))]
pub fn is_enabled(app: &AppHandle) -> Result<bool> {
    let manager = app.state::<AutostartManager>();
    manager.platform.is_enabled()
}

#[cfg(target_os = "android")]
pub fn is_enabled(_app: &AppHandle) -> Result<bool> {
    Ok(false)
}

#[cfg(not(target_os = "android"))]
pub fn set_enabled(app: &AppHandle, enabled: bool) -> Result<()> {
    let manager = app.state::<AutostartManager>();
    manager.platform.set_enabled(enabled)
}

#[cfg(target_os = "android")]
pub fn set_enabled(_app: &AppHandle, _enabled: bool) -> Result<()> {
    Ok(())
}

/// Align the OS autostart entry with the persisted setting during startup.
#[cfg(not(target_os = "android"))]
pub fn sync_enabled(app: &AppHandle, enabled: bool) -> Result<()> {
    set_enabled(app, enabled)
}

/// 判断进程参数是否来自 EcoPaste 注册的系统自启动项。
#[cfg(not(target_os = "android"))]
pub fn is_autostart_launch(args: &[String]) -> bool {
    args.iter().any(|arg| arg == AUTO_LAUNCH_ARG)
}

#[cfg(all(test, not(target_os = "android")))]
mod tests {
    use super::is_autostart_launch;

    #[test]
    fn detects_autostart_launch_argument() {
        let args = vec!["EcoPaste.exe".to_owned(), "--auto-launch".to_owned()];

        assert!(is_autostart_launch(&args));
    }

    #[test]
    fn rejects_regular_launch_arguments() {
        let args = vec!["EcoPaste.exe".to_owned()];

        assert!(!is_autostart_launch(&args));
    }
}
