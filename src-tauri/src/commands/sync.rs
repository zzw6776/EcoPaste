use std::sync::Arc;

use tauri::{AppHandle, Manager, State};

use crate::{
    core::Result,
    settings::SettingsStore,
    sync::{
        CloudRecordPage, IncomingJoinRequest, NearbyJoinAttempt, NearbySyncSpace, PairingCode,
        SyncItemStatus, SyncManager, SyncPairingPreview, SyncStatus, SyncTarget,
    },
};

#[tauri::command]
pub async fn get_sync_status(manager: State<'_, Arc<SyncManager>>) -> Result<SyncStatus> {
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn list_cloud_records(
    manager: State<'_, Arc<SyncManager>>,
    before_cursor: Option<u64>,
    limit: u16,
) -> Result<CloudRecordPage> {
    manager
        .inner()
        .clone()
        .cloud_records(before_cursor, limit)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn create_sync_group(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
) -> Result<String> {
    manager.create_group()?;
    let settings = app
        .state::<SettingsStore>()
        .update(serde_json::json!({ "sync": { "enabled": true } }))?;
    crate::commands::settings::emit_settings_updated(&app, &settings);
    manager.wake();
    manager.pairing_code().await.map_err(Into::into)
}

#[tauri::command]
pub async fn export_sync_pairing_code(manager: State<'_, Arc<SyncManager>>) -> Result<String> {
    manager.pairing_code().await.map_err(Into::into)
}

#[tauri::command]
pub async fn inspect_sync_pairing_code(
    manager: State<'_, Arc<SyncManager>>,
    pairing_code: String,
) -> Result<SyncPairingPreview> {
    let code = PairingCode::decode(&pairing_code)?;
    Ok(manager.pairing_preview(&code))
}

#[tauri::command]
pub async fn join_sync_group(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
    pairing_code: String,
    replace_existing: bool,
) -> Result<SyncStatus> {
    let code = PairingCode::decode(&pairing_code)?;
    let cloud_enabled = !code.server_endpoint_id.trim().is_empty();
    manager.join_group(&code, replace_existing).await?;
    let settings = app.state::<SettingsStore>().update(serde_json::json!({
        "sync": {
            "enabled": true,
            "cloudEnabled": cloud_enabled,
            "cloudRelayMode": code.cloud_relay_mode,
            "serverEndpointId": code.server_endpoint_id,
            "serverDirectAddresses": code.server_direct_addresses,
            "serverRelayUrls": code.server_relay_urls,
        }
    }))?;
    crate::commands::settings::emit_settings_updated(&app, &settings);
    manager.wake();
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn leave_sync_group(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
) -> Result<SyncStatus> {
    manager.leave_group().await?;
    let settings = app
        .state::<SettingsStore>()
        .update(serde_json::json!({ "sync": { "enabled": false } }))?;
    crate::commands::settings::emit_settings_updated(&app, &settings);
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn set_sync_device_name(
    manager: State<'_, Arc<SyncManager>>,
    name: String,
) -> Result<SyncStatus> {
    manager.set_device_name(name)?;
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn set_cloud_relay_auth_token(
    manager: State<'_, Arc<SyncManager>>,
    token: Option<String>,
) -> Result<SyncStatus> {
    manager.set_cloud_relay_auth_token(token)?;
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn sync_now(manager: State<'_, Arc<SyncManager>>) -> Result<SyncStatus> {
    manager.run_now().await?;
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn reconnect_sync_peer(
    manager: State<'_, Arc<SyncManager>>,
    device_id: Option<String>,
) -> Result<SyncStatus> {
    manager.reconnect_peer(device_id).await?;
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn reconnect_cloud(manager: State<'_, Arc<SyncManager>>) -> Result<SyncStatus> {
    manager.reconnect_cloud()?;
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn remove_sync_peer(
    manager: State<'_, Arc<SyncManager>>,
    device_id: String,
) -> Result<SyncStatus> {
    manager.remove_peer(&device_id).await?;
    Ok(manager.status().await?)
}

#[tauri::command]
pub async fn discover_nearby_sync_spaces(
    manager: State<'_, Arc<SyncManager>>,
) -> Result<Vec<NearbySyncSpace>> {
    manager
        .inner()
        .clone()
        .discover_nearby_spaces()
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn request_nearby_sync_join(
    manager: State<'_, Arc<SyncManager>>,
    endpoint_id: String,
) -> Result<NearbyJoinAttempt> {
    manager
        .inner()
        .clone()
        .request_nearby_join(endpoint_id.trim())
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn get_nearby_sync_join_attempt(
    manager: State<'_, Arc<SyncManager>>,
    request_id: String,
) -> Result<Option<NearbyJoinAttempt>> {
    Ok(manager.outgoing_join_attempt(request_id.trim()))
}

#[tauri::command]
pub async fn list_incoming_sync_join_requests(
    manager: State<'_, Arc<SyncManager>>,
) -> Result<Vec<IncomingJoinRequest>> {
    Ok(manager.incoming_join_requests().await)
}

#[tauri::command]
pub async fn respond_incoming_sync_join_request(
    manager: State<'_, Arc<SyncManager>>,
    request_id: String,
    approved: bool,
) -> Result<()> {
    manager
        .inner()
        .clone()
        .respond_nearby_join(request_id.trim(), approved)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn get_sync_item_statuses(
    manager: State<'_, Arc<SyncManager>>,
    item_ids: Vec<String>,
) -> Result<Vec<SyncItemStatus>> {
    manager.item_statuses(&item_ids).await.map_err(Into::into)
}

#[tauri::command]
pub async fn sync_item_now(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
    item_id: String,
    target: SyncTarget,
) -> Result<SyncItemStatus> {
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let item = crate::db::items::find_item_by_id(&pool, &item_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("剪贴板记录不存在"))?;
    manager
        .inner()
        .clone()
        .sync_item_now(item, target)
        .await
        .map_err(Into::into)
}

#[tauri::command]
pub async fn upload_sync_item(
    app: AppHandle,
    manager: State<'_, Arc<SyncManager>>,
    item_id: String,
) -> Result<SyncStatus> {
    let pool = app.state::<crate::db::DatabaseState>().pool().await;
    let item = crate::db::items::find_item_by_id(&pool, &item_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("剪贴板记录不存在"))?;
    manager
        .inner()
        .clone()
        .sync_item_now(item, SyncTarget::Cloud)
        .await?;
    Ok(manager.status().await?)
}
