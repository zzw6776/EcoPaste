#[cfg(not(target_os = "android"))]
pub mod clipboard_menu;
pub mod commands;
#[cfg(not(target_os = "android"))]
pub mod tray;
