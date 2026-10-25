mod activity;
mod crypto;
mod identity;
mod manager;
mod model;
mod pairing;
pub(crate) mod repository;

pub use identity::PairingCode;
pub use manager::SyncManager;
pub use model::{
    CloudDeviceDirectory, CloudRecordPage, IncomingJoinRequest, NearbyJoinAttempt, NearbySyncSpace,
    SyncItemStatus, SyncPairingPreview, SyncStatus, SyncTarget, SyncedItemStatePatch,
};

use std::sync::Arc;

use tauri::{AppHandle, Manager};

use crate::{
    clipboard::ClipboardObservation, core::Result, db::models::ClipboardItem,
    settings::SettingsStore,
};

pub async fn init(app: &AppHandle) -> Result<()> {
    manager::init(app).await
}

/// Returns whether this local capture should create a durable automatic-sync operation.
pub fn should_stage_local_item(app: &AppHandle, item: &ClipboardItem) -> bool {
    app.try_state::<SettingsStore>().is_some_and(|store| {
        let settings = store.snapshot().sync;
        settings.enabled && (!item.is_sensitive || settings.sync_sensitive)
    })
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
    let ready = item.kind != crate::db::models::ClipboardKind::Files || item.size.is_some();
    manager.notify_content_staged(&item, observation, ready);
}

/// Wakes the durable content materializer after delayed file metadata becomes available.
pub fn notify_local_item_ready(app: &AppHandle) {
    if let Some(manager) = app.try_state::<Arc<SyncManager>>() {
        manager.wake_local_outbox();
    }
}

/// Wakes the synchronization worker after a durable local item-state mutation.
pub fn notify_local_state_changed(app: &AppHandle) {
    if let Some(manager) = app.try_state::<Arc<SyncManager>>() {
        manager.wake_local_outbox();
    }
}

pub fn notify_settings_changed(app: &AppHandle) {
    wake_manager(app);
}

fn wake_manager(app: &AppHandle) {
    if let Some(manager) = app.try_state::<Arc<SyncManager>>() {
        manager.wake();
    }
}
