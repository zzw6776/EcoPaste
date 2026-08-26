use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use ecopaste_sync_protocol::{EncryptedEvent, PeerAnnouncement};
use sqlx::{Row, SqlitePool};

use super::model::{
    StoredBlob, StoredSyncEvent, SyncChannelState, SyncItemState, SyncItemStatus, SyncPeer,
    SyncPeerStatus,
};

const CLOUD_CURSOR_KEY: &str = "cloud_cursor";
const LAST_SUCCESS_KEY: &str = "last_success_at";

/// Allocates a monotonically increasing sequence for this device.
pub async fn next_origin_sequence(pool: &SqlitePool) -> Result<u64> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin sync sequence transaction")?;
    let current: Option<String> =
        sqlx::query_scalar("SELECT value FROM sync_state WHERE key = 'origin_sequence'")
            .fetch_optional(&mut *transaction)
            .await
            .context("read sync origin sequence")?;
    let next = current
        .as_deref()
        .unwrap_or("0")
        .parse::<u64>()
        .context("invalid sync origin sequence")?
        .saturating_add(1);
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES ('origin_sequence', ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        "#,
    )
    .bind(next.to_string())
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("update sync origin sequence")?;
    transaction
        .commit()
        .await
        .context("commit sync origin sequence")?;
    Ok(next)
}

/// Stores an encrypted event in the local append-only log. Duplicate events are idempotent.
pub async fn insert_event(
    pool: &SqlitePool,
    event: &EncryptedEvent,
    applied: bool,
    blobs: &[StoredBlob],
) -> Result<bool> {
    let now = Utc::now();
    let mut transaction = pool.begin().await.context("begin sync event transaction")?;
    let result = sqlx::query(
        r#"
        INSERT OR IGNORE INTO sync_events (
            event_id, origin_device_id, origin_sequence, event_created_at_ms,
            nonce, ciphertext, is_applied, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&event.event_id)
    .bind(&event.origin_device_id)
    .bind(i64::try_from(event.origin_sequence).context("sync origin sequence too large")?)
    .bind(event.created_at_ms)
    .bind(&event.nonce)
    .bind(&event.ciphertext)
    .bind(applied)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("insert sync event")?;
    if result.rows_affected() == 1 {
        for blob in blobs {
            sqlx::query(
                r#"
                INSERT INTO sync_event_blobs (
                    event_id, blob_id, encrypted_path, size, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&event.event_id)
            .bind(&blob.blob_id)
            .bind(&blob.encrypted_path)
            .bind(i64::try_from(blob.size).context("encrypted sync blob is too large")?)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .context("insert sync event blob")?;
        }
    }
    transaction.commit().await.context("commit sync event")?;
    Ok(result.rows_affected() == 1)
}

/// Associates a clipboard record with the encrypted event that carries it.
pub async fn link_event_to_item(
    pool: &SqlitePool,
    item_id: &str,
    event_id: &str,
    direction: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO sync_item_events (
            item_id, event_id, direction, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        "#,
    )
    .bind(item_id)
    .bind(event_id)
    .bind(direction)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("link clipboard item to sync event")?;
    Ok(())
}

pub async fn event_for_item(pool: &SqlitePool, item_id: &str) -> Result<Option<String>> {
    sqlx::query_scalar(
        "SELECT event_id FROM sync_item_events WHERE item_id = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .context("find sync event for clipboard item")
}

pub async fn mark_applied(pool: &SqlitePool, event_id: &str) -> Result<()> {
    sqlx::query("UPDATE sync_events SET is_applied = 1, updated_at = ? WHERE event_id = ?")
        .bind(Utc::now())
        .bind(event_id)
        .execute(pool)
        .await
        .context("mark sync event applied")?;
    Ok(())
}

pub async fn attach_event_blobs(
    pool: &SqlitePool,
    event_id: &str,
    blobs: &[StoredBlob],
) -> Result<()> {
    let now = Utc::now();
    for blob in blobs {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO sync_event_blobs (
                event_id, blob_id, encrypted_path, size, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(event_id)
        .bind(&blob.blob_id)
        .bind(&blob.encrypted_path)
        .bind(i64::try_from(blob.size).context("encrypted sync blob is too large")?)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .context("attach blob to sync event")?;
    }
    Ok(())
}

pub async fn unapplied_events(pool: &SqlitePool, limit: u16) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT cursor, event_id, origin_device_id, origin_sequence,
               event_created_at_ms, nonce, ciphertext
        FROM sync_events
        WHERE is_applied = 0
        ORDER BY cursor ASC
        LIMIT ?
        "#,
    )
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list unapplied sync events")?;
    rows.into_iter().map(event_from_row).collect()
}

pub async fn pending_events_for_target(
    pool: &SqlitePool,
    target_id: &str,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT e.cursor, e.event_id, e.origin_device_id, e.origin_sequence,
               e.event_created_at_ms, e.nonce, e.ciphertext
        FROM sync_events e
        LEFT JOIN sync_deliveries d
          ON d.event_id = e.event_id AND d.target_id = ?
        WHERE d.event_id IS NULL
        ORDER BY e.cursor ASC
        LIMIT ?
        "#,
    )
    .bind(target_id)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list pending sync events")?;
    rows.into_iter().map(event_from_row).collect()
}

pub async fn events_after_cursor(
    pool: &SqlitePool,
    after_cursor: u64,
    limit: u16,
) -> Result<Vec<StoredSyncEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT cursor, event_id, origin_device_id, origin_sequence,
               event_created_at_ms, nonce, ciphertext
        FROM sync_events
        WHERE cursor > ?
        ORDER BY cursor ASC
        LIMIT ?
        "#,
    )
    .bind(i64::try_from(after_cursor).unwrap_or(i64::MAX))
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .context("list sync events after cursor")?;
    rows.into_iter().map(event_from_row).collect()
}

pub async fn blobs_for_events(pool: &SqlitePool, event_ids: &[String]) -> Result<Vec<StoredBlob>> {
    if event_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT DISTINCT blob_id, encrypted_path, size FROM sync_event_blobs WHERE event_id IN (",
    );
    let mut separated = query.separated(", ");
    for event_id in event_ids {
        separated.push_bind(event_id);
    }
    query.push(")");
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("list sync event blobs")?;
    rows.into_iter()
        .map(|row| {
            let size: i64 = row.try_get("size")?;
            Ok(StoredBlob {
                blob_id: row.try_get("blob_id")?,
                encrypted_path: row.try_get("encrypted_path")?,
                size: u64::try_from(size).context("negative encrypted blob size")?,
            })
        })
        .collect()
}

pub async fn mark_delivered(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    let now = Utc::now();
    let mut transaction = pool
        .begin()
        .await
        .context("begin sync delivery transaction")?;
    for event_id in event_ids {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO sync_deliveries (event_id, target_id, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("mark sync event delivered")?;
        sqlx::query(
            r#"
            INSERT INTO sync_delivery_states (
                event_id, target_id, state, last_error, last_attempt_at,
                last_success_at, created_at, updated_at
            ) VALUES (?, ?, 'success', NULL, ?, ?, ?, ?)
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                state = 'success',
                last_error = NULL,
                last_attempt_at = excluded.last_attempt_at,
                last_success_at = excluded.last_success_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("record successful sync delivery")?;
    }
    transaction
        .commit()
        .await
        .context("commit sync deliveries")?;
    Ok(())
}

pub async fn mark_delivery_syncing(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
) -> Result<()> {
    set_delivery_state(pool, target_id, event_ids, "syncing", None).await
}

pub async fn mark_delivery_error(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    error: &str,
) -> Result<()> {
    set_delivery_state(pool, target_id, event_ids, "error", Some(error)).await
}

async fn set_delivery_state(
    pool: &SqlitePool,
    target_id: &str,
    event_ids: &[String],
    state: &str,
    error: Option<&str>,
) -> Result<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    let mut transaction = pool.begin().await.context("begin sync state transaction")?;
    for event_id in event_ids {
        sqlx::query(
            r#"
            INSERT INTO sync_delivery_states (
                event_id, target_id, state, last_error, last_attempt_at,
                last_success_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(event_id, target_id) DO UPDATE SET
                state = excluded.state,
                last_error = excluded.last_error,
                last_attempt_at = excluded.last_attempt_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(event_id)
        .bind(target_id)
        .bind(state)
        .bind(error)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("record sync delivery state")?;
    }
    transaction
        .commit()
        .await
        .context("commit sync delivery states")?;
    Ok(())
}

pub async fn upsert_peer(pool: &SqlitePool, peer: &PeerAnnouncement) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_peers (
            device_id, device_name, platform, endpoint_id, direct_addresses,
            relay_urls, pull_cursor, last_seen_ms, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            device_name = excluded.device_name,
            platform = excluded.platform,
            endpoint_id = excluded.endpoint_id,
            direct_addresses = excluded.direct_addresses,
            relay_urls = excluded.relay_urls,
            last_seen_ms = excluded.last_seen_ms,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&peer.device_id)
    .bind(&peer.device_name)
    .bind(&peer.platform)
    .bind(&peer.endpoint_id)
    .bind(serde_json::to_string(&peer.direct_addresses)?)
    .bind(serde_json::to_string(&peer.relay_urls)?)
    .bind(peer.last_seen_ms)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("upsert sync peer")?;
    Ok(())
}

/// Refreshes the volatile routes of a paired endpoint discovered on the local network.
pub async fn update_peer_routes(
    pool: &SqlitePool,
    endpoint_id: &str,
    direct_addresses: &[String],
    relay_urls: &[String],
) -> Result<bool> {
    let now = Utc::now();
    let direct_addresses = serde_json::to_string(direct_addresses)?;
    let relay_urls = serde_json::to_string(relay_urls)?;
    let result = sqlx::query(
        r#"
        UPDATE sync_peers
        SET direct_addresses = CASE WHEN ? = '[]' THEN direct_addresses ELSE ? END,
            relay_urls = CASE WHEN ? = '[]' THEN relay_urls ELSE ? END,
            last_seen_ms = ?, updated_at = ?
        WHERE endpoint_id = ?
          AND ((? != '[]' AND direct_addresses != ?)
            OR (? != '[]' AND relay_urls != ?))
        "#,
    )
    .bind(&direct_addresses)
    .bind(&direct_addresses)
    .bind(&relay_urls)
    .bind(&relay_urls)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(endpoint_id)
    .bind(&direct_addresses)
    .bind(&direct_addresses)
    .bind(&relay_urls)
    .bind(&relay_urls)
    .execute(pool)
    .await
    .context("refresh discovered peer routes")?;
    Ok(result.rows_affected() > 0)
}

pub async fn list_peers(pool: &SqlitePool) -> Result<Vec<SyncPeer>> {
    let rows = sqlx::query(
        r#"
        SELECT device_id, device_name, platform, endpoint_id, direct_addresses,
               relay_urls, pull_cursor, last_seen_ms
        FROM sync_peers
        ORDER BY last_seen_ms DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list sync peers")?;
    rows.into_iter()
        .map(|row| {
            let direct: String = row.try_get("direct_addresses")?;
            let relays: String = row.try_get("relay_urls")?;
            let cursor: i64 = row.try_get("pull_cursor")?;
            Ok(SyncPeer {
                announcement: PeerAnnouncement {
                    device_id: row.try_get("device_id")?,
                    device_name: row.try_get("device_name")?,
                    platform: row.try_get("platform")?,
                    endpoint_id: row.try_get("endpoint_id")?,
                    direct_addresses: serde_json::from_str(&direct)
                        .context("invalid peer direct addresses")?,
                    relay_urls: serde_json::from_str(&relays).context("invalid peer relay URLs")?,
                    last_seen_ms: row.try_get("last_seen_ms")?,
                },
                pull_cursor: u64::try_from(cursor).context("negative peer cursor")?,
            })
        })
        .collect()
}

pub async fn set_peer_cursor(pool: &SqlitePool, device_id: &str, cursor: u64) -> Result<()> {
    sqlx::query("UPDATE sync_peers SET pull_cursor = ?, updated_at = ? WHERE device_id = ?")
        .bind(i64::try_from(cursor).unwrap_or(i64::MAX))
        .bind(Utc::now())
        .bind(device_id)
        .execute(pool)
        .await
        .context("update peer sync cursor")?;
    Ok(())
}

pub async fn mark_peer_connecting(pool: &SqlitePool, device_id: &str) -> Result<()> {
    set_peer_connection(pool, device_id, "connecting", None, None, None, false).await
}

pub async fn mark_peer_offline(
    pool: &SqlitePool,
    device_id: &str,
    error: Option<&str>,
) -> Result<()> {
    set_peer_connection(pool, device_id, "offline", None, None, error, false).await
}

pub async fn mark_peer_error(pool: &SqlitePool, device_id: &str, error: &str) -> Result<()> {
    set_peer_connection(pool, device_id, "error", None, None, Some(error), false).await
}

pub async fn mark_peer_online(
    pool: &SqlitePool,
    device_id: &str,
    connected_address: Option<&str>,
    transport: Option<&str>,
) -> Result<()> {
    set_peer_connection(
        pool,
        device_id,
        "online",
        connected_address,
        transport,
        None,
        true,
    )
    .await
}

async fn set_peer_connection(
    pool: &SqlitePool,
    device_id: &str,
    state: &str,
    connected_address: Option<&str>,
    transport: Option<&str>,
    error: Option<&str>,
    succeeded: bool,
) -> Result<()> {
    let now = Utc::now();
    let success = succeeded.then_some(now);
    sqlx::query(
        r#"
        INSERT INTO sync_peer_connections (
            device_id, state, connected_address, transport, last_attempt_at,
            last_success_at, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            state = excluded.state,
            connected_address = COALESCE(excluded.connected_address, sync_peer_connections.connected_address),
            transport = COALESCE(excluded.transport, sync_peer_connections.transport),
            last_attempt_at = excluded.last_attempt_at,
            last_success_at = COALESCE(excluded.last_success_at, sync_peer_connections.last_success_at),
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(device_id)
    .bind(state)
    .bind(connected_address)
    .bind(transport)
    .bind(now)
    .bind(success)
    .bind(error)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("update sync peer connection")?;
    Ok(())
}

pub async fn list_peer_statuses(pool: &SqlitePool) -> Result<Vec<SyncPeerStatus>> {
    let rows = sqlx::query(
        r#"
        SELECT p.device_id, p.device_name, p.platform, p.endpoint_id,
               p.direct_addresses, p.relay_urls, p.last_seen_ms,
               c.state, c.connected_address, c.transport, c.last_attempt_at,
               c.last_success_at, c.last_error
        FROM sync_peers p
        LEFT JOIN sync_peer_connections c ON c.device_id = p.device_id
        ORDER BY p.last_seen_ms DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list sync peer statuses")?;
    rows.into_iter()
        .map(|row| {
            let direct: String = row.try_get("direct_addresses")?;
            let relays: String = row.try_get("relay_urls")?;
            let state: Option<String> = row.try_get("state")?;
            let last_seen_ms: i64 = row.try_get("last_seen_ms")?;
            Ok(SyncPeerStatus {
                device_id: row.try_get("device_id")?,
                device_name: row.try_get("device_name")?,
                platform: row.try_get("platform")?,
                endpoint_id: row.try_get("endpoint_id")?,
                direct_addresses: serde_json::from_str(&direct)
                    .context("invalid peer direct addresses")?,
                relay_urls: serde_json::from_str(&relays).context("invalid peer relay URLs")?,
                state: channel_state(state.as_deref()),
                connected_address: row.try_get("connected_address")?,
                transport: row.try_get("transport")?,
                last_seen_at: Utc
                    .timestamp_millis_opt(last_seen_ms)
                    .single()
                    .map(|value| value.to_rfc3339()),
                last_attempt_at: row.try_get("last_attempt_at")?,
                last_success_at: row.try_get("last_success_at")?,
                last_error: row.try_get("last_error")?,
            })
        })
        .collect()
}

fn channel_state(value: Option<&str>) -> SyncChannelState {
    match value {
        Some("connecting") => SyncChannelState::Connecting,
        Some("online") => SyncChannelState::Online,
        Some("error") => SyncChannelState::Error,
        _ => SyncChannelState::Idle,
    }
}

pub async fn cloud_cursor(pool: &SqlitePool) -> Result<u64> {
    state_value(pool, CLOUD_CURSOR_KEY)
        .await?
        .as_deref()
        .unwrap_or("0")
        .parse()
        .context("invalid cloud sync cursor")
}

pub async fn set_cloud_cursor(pool: &SqlitePool, cursor: u64) -> Result<()> {
    set_state_value(pool, CLOUD_CURSOR_KEY, &cursor.to_string()).await
}

pub async fn mark_success(pool: &SqlitePool) -> Result<()> {
    set_state_value(pool, LAST_SUCCESS_KEY, &Utc::now().to_rfc3339()).await
}

pub async fn last_success(pool: &SqlitePool) -> Result<Option<String>> {
    state_value(pool, LAST_SUCCESS_KEY).await
}

pub async fn mark_pending_item(pool: &SqlitePool, item_id: &str, reason: &str) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_pending_items (item_id, reason, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET reason = excluded.reason, updated_at = excluded.updated_at
        "#,
    )
    .bind(item_id)
    .bind(reason)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("mark manual sync item")?;
    Ok(())
}

pub async fn clear_pending_item(pool: &SqlitePool, item_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM sync_pending_items WHERE item_id = ?")
        .bind(item_id)
        .execute(pool)
        .await
        .context("clear manual sync item")?;
    Ok(())
}

pub async fn status_counts(pool: &SqlitePool) -> Result<(u64, u64, u64)> {
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sync_events e WHERE NOT EXISTS (SELECT 1 FROM sync_deliveries d WHERE d.event_id = e.event_id)",
    )
    .fetch_one(pool)
    .await?;
    let manual: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_pending_items")
        .fetch_one(pool)
        .await?;
    let peers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_peers")
        .fetch_one(pool)
        .await?;
    Ok((
        pending.max(0) as u64,
        manual.max(0) as u64,
        peers.max(0) as u64,
    ))
}

pub async fn item_statuses(pool: &SqlitePool, item_ids: &[String]) -> Result<Vec<SyncItemStatus>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    let peer_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_peers")
        .fetch_one(pool)
        .await?;
    let mut statuses = item_ids
        .iter()
        .map(|id| {
            let mut status = SyncItemStatus::idle(id.clone());
            status.lan.total_targets = peer_count.max(0) as u64;
            status.cloud.total_targets = 1;
            (id.clone(), status)
        })
        .collect::<HashMap<_, _>>();

    let mut manual_query =
        sqlx::QueryBuilder::new("SELECT item_id FROM sync_pending_items WHERE item_id IN (");
    let mut manual_ids = manual_query.separated(", ");
    for item_id in item_ids {
        manual_ids.push_bind(item_id);
    }
    manual_query.push(")");
    for row in manual_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        if let Some(status) = statuses.get_mut(&item_id) {
            status.lan.state = SyncItemState::Manual;
            status.cloud.state = SyncItemState::Manual;
        }
    }

    let mut event_query = sqlx::QueryBuilder::new(
        r#"
        WITH latest AS (
            SELECT item_id, event_id,
                   ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY created_at DESC) AS rank
            FROM sync_item_events
            WHERE item_id IN (
        "#,
    );
    let mut event_ids = event_query.separated(", ");
    for item_id in item_ids {
        event_ids.push_bind(item_id);
    }
    event_query.push(
        r#")
        )
        SELECT latest.item_id, targets.target_id, targets.state, targets.last_error
        FROM latest
        JOIN (
            SELECT d.event_id, d.target_id, 'success' AS state, NULL AS last_error
            FROM sync_deliveries d
            UNION ALL
            SELECT s.event_id, s.target_id, s.state, s.last_error
            FROM sync_delivery_states s
            WHERE NOT EXISTS (
                SELECT 1 FROM sync_deliveries d
                WHERE d.event_id = s.event_id AND d.target_id = s.target_id
            )
        ) targets ON targets.event_id = latest.event_id
        WHERE latest.rank = 1
        "#,
    );
    for row in event_query.build().fetch_all(pool).await? {
        let item_id: String = row.try_get("item_id")?;
        let target_id: String = row.try_get("target_id")?;
        let state: String = row.try_get("state")?;
        let last_error: Option<String> = row.try_get("last_error")?;
        let Some(status) = statuses.get_mut(&item_id) else {
            continue;
        };
        let channel = if target_id == "cloud" {
            &mut status.cloud
        } else if target_id.starts_with("peer:") {
            &mut status.lan
        } else {
            continue;
        };
        match state.as_str() {
            "success" => {
                channel.state = SyncItemState::Success;
                channel.delivered_targets = channel.delivered_targets.saturating_add(1);
                channel.last_error = None;
            }
            "syncing" if channel.state != SyncItemState::Success => {
                channel.state = SyncItemState::Syncing;
            }
            "error" if channel.state != SyncItemState::Success => {
                channel.state = SyncItemState::Error;
                channel.last_error = last_error;
            }
            _ => {}
        }
    }
    Ok(item_ids
        .iter()
        .filter_map(|id| statuses.remove(id))
        .collect())
}

pub async fn clear_group_state(pool: &SqlitePool) -> Result<()> {
    let mut transaction = pool.begin().await.context("begin clearing sync state")?;
    for statement in [
        "DELETE FROM sync_peer_connections",
        "DELETE FROM sync_delivery_states",
        "DELETE FROM sync_item_events",
        "DELETE FROM sync_deliveries",
        "DELETE FROM sync_event_blobs",
        "DELETE FROM sync_events",
        "DELETE FROM sync_peers",
        "DELETE FROM sync_state",
        "DELETE FROM sync_pending_items",
    ] {
        sqlx::query(statement)
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("run {statement}"))?;
    }
    transaction
        .commit()
        .await
        .context("commit clearing sync state")?;
    Ok(())
}

fn event_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredSyncEvent> {
    let cursor: i64 = row.try_get("cursor")?;
    let sequence: i64 = row.try_get("origin_sequence")?;
    Ok(StoredSyncEvent {
        cursor: u64::try_from(cursor).context("negative local sync cursor")?,
        event: EncryptedEvent {
            event_id: row.try_get("event_id")?,
            origin_device_id: row.try_get("origin_device_id")?,
            origin_sequence: u64::try_from(sequence).context("negative sync origin sequence")?,
            created_at_ms: row.try_get("event_created_at_ms")?,
            nonce: row.try_get("nonce")?,
            ciphertext: row.try_get("ciphertext")?,
        },
    })
}

async fn state_value(pool: &SqlitePool, key: &str) -> Result<Option<String>> {
    sqlx::query_scalar("SELECT value FROM sync_state WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
        .context("read sync state")
}

async fn set_state_value(pool: &SqlitePool, key: &str, value: &str) -> Result<()> {
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_state (key, value, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        "#,
    )
    .bind(key)
    .bind(value)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .context("update sync state")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::test_support::memory_pool;

    use super::*;

    #[tokio::test]
    async fn any_successful_route_clears_the_pending_event_count() {
        let pool = memory_pool().await;
        insert_test_event(&pool, "event-1").await;

        assert_eq!(status_counts(&pool).await.unwrap().0, 1);

        mark_delivered(&pool, "peer:android", &["event-1".into()])
            .await
            .unwrap();

        assert_eq!(status_counts(&pool).await.unwrap().0, 0);
    }

    #[tokio::test]
    async fn item_status_tracks_each_route_independently() {
        let pool = memory_pool().await;
        insert_test_item(&pool, "item-1").await;
        insert_test_event(&pool, "event-1").await;
        link_event_to_item(&pool, "item-1", "event-1", "local")
            .await
            .unwrap();
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "Pixel".into(),
                platform: "android".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: vec!["127.0.0.1:35555".into()],
                relay_urls: Vec::new(),
                last_seen_ms: 100,
            },
        )
        .await
        .unwrap();
        mark_delivery_error(&pool, "cloud", &["event-1".into()], "offline")
            .await
            .unwrap();
        mark_delivered(&pool, "peer:android", &["event-1".into()])
            .await
            .unwrap();

        let status = item_statuses(&pool, &["item-1".into()])
            .await
            .unwrap()
            .remove(0);

        assert_eq!(status.lan.state, SyncItemState::Success);
        assert_eq!(status.lan.delivered_targets, 1);
        assert_eq!(status.lan.total_targets, 1);
        assert_eq!(status.cloud.state, SyncItemState::Error);
        assert_eq!(status.cloud.last_error.as_deref(), Some("offline"));
    }

    #[tokio::test]
    async fn discovered_routes_update_known_peer_without_erasing_missing_routes() {
        let pool = memory_pool().await;
        upsert_peer(
            &pool,
            &PeerAnnouncement {
                device_id: "android".into(),
                device_name: "Pixel".into(),
                platform: "android".into(),
                endpoint_id: "endpoint".into(),
                direct_addresses: vec!["10.0.0.2:35555".into()],
                relay_urls: vec!["https://relay.example".into()],
                last_seen_ms: 100,
            },
        )
        .await
        .unwrap();

        assert!(
            update_peer_routes(&pool, "endpoint", &["10.0.0.3:35555".into()], &[])
                .await
                .unwrap()
        );
        let peer = list_peers(&pool).await.unwrap().remove(0).announcement;
        assert_eq!(peer.direct_addresses, ["10.0.0.3:35555"]);
        assert_eq!(peer.relay_urls, ["https://relay.example"]);
        assert!(
            !update_peer_routes(&pool, "unknown", &["10.0.0.4:35555".into()], &[])
                .await
                .unwrap()
        );
    }

    async fn insert_test_item(pool: &SqlitePool, item_id: &str) {
        sqlx::query(
            r#"
            INSERT INTO clipboard_items (
                id, kind, content, content_hash, platform, created_at, updated_at
            ) VALUES (?, 'text', 'test', ?, 'test', '2026-01-01', '2026-01-01')
            "#,
        )
        .bind(item_id)
        .bind(format!("hash-{item_id}"))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_test_event(pool: &SqlitePool, event_id: &str) {
        insert_event(
            pool,
            &EncryptedEvent {
                event_id: event_id.into(),
                origin_device_id: "mac".into(),
                origin_sequence: if event_id == "event-1" { 1 } else { 2 },
                created_at_ms: 100,
                nonce: vec![1; 24],
                ciphertext: vec![2; 32],
            },
            true,
            &[],
        )
        .await
        .unwrap();
    }
}
