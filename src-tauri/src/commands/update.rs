use tauri::AppHandle;

use crate::core::Result;
use crate::update::{self, AppUpdateStatus, UpdateMetadata};
use crate::window;

#[tauri::command]
pub async fn get_update_status(app: AppHandle) -> AppUpdateStatus {
    update::status(&app).await
}

#[tauri::command]
pub async fn check_for_updates(app: AppHandle) -> Result<AppUpdateStatus> {
    update::check(&app, update::CheckMode::Manual).await
}

#[tauri::command]
pub async fn download_update(app: AppHandle, version: String) -> Result<Option<UpdateMetadata>> {
    update::download(&app, version).await
}

#[tauri::command]
pub fn cancel_update_download(app: AppHandle) {
    update::cancel_download(&app);
}

#[tauri::command]
pub async fn install_update(app: AppHandle, version: String) -> Result<()> {
    update::install(&app, version)
}

#[tauri::command]
pub async fn skip_update_version(app: AppHandle, version: String) -> Result<AppUpdateStatus> {
    update::skip(&app, version)
}

#[tauri::command]
pub async fn open_update_window(app: AppHandle) -> Result<()> {
    window::show_window(&app, window::UPDATE_WINDOW_LABEL)
}
