#[cfg(not(target_os = "android"))]
pub mod clipboard_menu;
pub mod commands;
pub mod sync;
#[cfg(not(target_os = "android"))]
pub mod tray;
