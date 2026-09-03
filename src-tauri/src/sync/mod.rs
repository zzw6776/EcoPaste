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

use crate::{clipboard::ClipboardObservation, core::Result, db::models::ClipboardItem};

pub async fn init(app: &AppHandle) -> Result<()> {
    manager::init(app).await
}

/// Adds a locally captured clipboard item to the encrypted event log when policy permits it.
pub fn enqueue_local_item(
    app: &AppHandle,
    item: ClipboardItem,
    observation: Option<ClipboardObservation>,
) {
    let Some(manager) = app.try_state::<Arc<SyncManager>>() else {
        return;
    };
    let manager = manager.inner().clone();
    tauri::async_runtime::spawn(async move {
        let result = match observation {
            Some(observation) => manager.enqueue_observed_item(item, observation).await,
            None => manager.enqueue_item(item, false).await,
        };
        if let Err(error) = result {
            log::warn!("enqueue clipboard sync event failed: {error:#}");
        }
    });
}

pub fn notify_settings_changed(app: &AppHandle) {
    wake_manager(app);
}

fn wake_manager(app: &AppHandle) {
    if let Some(manager) = app.try_state::<Arc<SyncManager>>() {
        manager.wake();
    }
}
