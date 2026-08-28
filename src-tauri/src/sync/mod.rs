mod crypto;
mod identity;
mod manager;
mod model;
mod pairing;
pub(crate) mod repository;

pub use identity::PairingCode;
pub use manager::SyncManager;
pub use model::{
    CloudRecordPage, IncomingJoinRequest, NearbyJoinAttempt, NearbySyncSpace, SyncItemStatus,
    SyncPairingPreview, SyncStatus, SyncTarget,
};

use std::sync::Arc;

use tauri::{AppHandle, Manager};

use crate::{core::Result, db::models::ClipboardItem};

pub async fn init(app: &AppHandle) -> Result<()> {
    manager::init(app).await
}

/// Adds a locally captured clipboard item to the encrypted event log when policy permits it.
pub fn enqueue_local_item(app: &AppHandle, item: ClipboardItem) {
    let Some(manager) = app.try_state::<Arc<SyncManager>>() else {
        return;
    };
    let manager = manager.inner().clone();
    tauri::async_runtime::spawn(async move {
        if let Err(error) = manager.enqueue_item(item, false).await {
            log::warn!("enqueue clipboard sync event failed: {error}");
        }
    });
}

pub fn notify_settings_changed(app: &AppHandle) {
    wake_manager(app);
}

/// Reconnects promptly when the user brings any EcoPaste window to the foreground.
pub fn notify_foreground(app: &AppHandle) {
    wake_manager(app);
}

fn wake_manager(app: &AppHandle) {
    if let Some(manager) = app.try_state::<Arc<SyncManager>>() {
        manager.wake();
    }
}
