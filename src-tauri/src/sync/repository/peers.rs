use super::*;

#[cfg(test)]
pub async fn upsert_peer(pool: &SqlitePool, peer: &PeerAnnouncement) -> Result<()> {
    let mut connection = pool
        .acquire()
        .await
        .context("acquire sync peer connection")?;
    upsert_peer_on(&mut connection, peer).await
}

/// Updates peer identity without importing routes announced outside local discovery.
pub async fn upsert_peer_identity(pool: &SqlitePool, peer: &PeerAnnouncement) -> Result<()> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin sync peer identity transaction")?;
    let previous_endpoint: Option<String> =
        sqlx::query_scalar("SELECT endpoint_id FROM sync_peers WHERE device_id = ?")
            .bind(&peer.device_id)
            .fetch_optional(&mut *transaction)
            .await
            .context("read existing sync peer identity")?;
    let endpoint_changed = previous_endpoint
        .as_deref()
        .is_some_and(|endpoint_id| endpoint_id != peer.endpoint_id);
    let now = Utc::now();
    sqlx::query(
        r#"
        INSERT INTO sync_peers (
            device_id, device_name, platform, endpoint_id, direct_addresses,
            relay_urls, pull_cursor, last_seen_ms, created_at, updated_at
        )
        SELECT ?, ?, ?, ?, '[]', '[]', 0, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM sync_removed_peers
            WHERE (device_id = ? OR endpoint_id = ?)
              AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
        )
        ON CONFLICT(device_id) DO UPDATE SET
            device_name = excluded.device_name,
            platform = excluded.platform,
            endpoint_id = excluded.endpoint_id,
            direct_addresses = CASE
                WHEN sync_peers.endpoint_id = excluded.endpoint_id
                THEN sync_peers.direct_addresses
                ELSE excluded.direct_addresses
            END,
            relay_urls = CASE
                WHEN sync_peers.endpoint_id = excluded.endpoint_id
                THEN sync_peers.relay_urls
                ELSE excluded.relay_urls
            END,
            last_seen_ms = CASE
                WHEN sync_peers.endpoint_id = excluded.endpoint_id
                THEN sync_peers.last_seen_ms
                ELSE excluded.last_seen_ms
            END,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&peer.device_id)
    .bind(&peer.device_name)
    .bind(&peer.platform)
    .bind(&peer.endpoint_id)
    .bind(peer.last_seen_ms)
    .bind(now)
    .bind(now)
    .bind(&peer.device_id)
    .bind(&peer.endpoint_id)
    .execute(&mut *transaction)
    .await
    .context("upsert sync peer identity")?;

    if endpoint_changed {
        sqlx::query(
            r#"
            UPDATE sync_peer_connections
            SET state = 'offline', connected_address = NULL, transport = NULL,
                last_error = NULL, updated_at = ?
            WHERE device_id = ?
            "#,
        )
        .bind(now)
        .bind(&peer.device_id)
        .execute(&mut *transaction)
        .await
        .context("invalidate routes for changed sync peer identity")?;
    }

    transaction
        .commit()
        .await
        .context("commit sync peer identity")?;
    Ok(())
}

async fn upsert_peer_on(connection: &mut SqliteConnection, peer: &PeerAnnouncement) -> Result<()> {
    let now = Utc::now();
    let direct_addresses = lan_direct_addresses(&peer.direct_addresses);
    sqlx::query(
        r#"
        INSERT INTO sync_peers (
            device_id, device_name, platform, endpoint_id, direct_addresses,
            relay_urls, pull_cursor, last_seen_ms, created_at, updated_at
        )
        SELECT ?, ?, ?, ?, ?, ?, 0, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM sync_removed_peers
            WHERE (device_id = ? OR endpoint_id = ?)
              AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
        )
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
    .bind(serde_json::to_string(&direct_addresses)?)
    .bind("[]")
    .bind(peer.last_seen_ms)
    .bind(now)
    .bind(now)
    .bind(&peer.device_id)
    .bind(&peer.endpoint_id)
    .execute(connection)
    .await
    .context("upsert sync peer")?;
    Ok(())
}

async fn restore_peer_on(
    connection: &mut SqliteConnection,
    device_id: &str,
    endpoint_id: &str,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query("DELETE FROM sync_removed_peers WHERE endpoint_id = ? AND device_id <> ?")
        .bind(endpoint_id)
        .bind(device_id)
        .execute(&mut *connection)
        .await
        .context("remove stale endpoint membership")?;
    sqlx::query(
        r#"
        INSERT INTO sync_removed_peers (
            device_id, endpoint_id, removed_at_ms, restored_at_ms, created_at, updated_at
        ) VALUES (?, ?, 0, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            endpoint_id = excluded.endpoint_id,
            restored_at_ms = MAX(COALESCE(sync_removed_peers.restored_at_ms, 0), excluded.restored_at_ms),
            updated_at = excluded.updated_at
        "#,
    )
    .bind(device_id)
    .bind(endpoint_id)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(now)
    .execute(connection)
    .await
    .context("restore removed sync peer")?;
    Ok(())
}

/// Restores a removed member and stores its current route as one membership transaction.
pub async fn restore_and_upsert_peer(pool: &SqlitePool, peer: &PeerAnnouncement) -> Result<()> {
    let mut transaction = pool.begin().await.context("begin approving sync peer")?;
    restore_peer_on(&mut transaction, &peer.device_id, &peer.endpoint_id).await?;
    upsert_peer_on(&mut transaction, peer).await?;
    transaction
        .commit()
        .await
        .context("commit approved sync peer")?;
    Ok(())
}

pub async fn is_peer_removed(
    pool: &SqlitePool,
    device_id: &str,
    endpoint_id: &str,
) -> Result<bool> {
    let removed: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM sync_removed_peers
            WHERE (device_id = ? OR endpoint_id = ?)
              AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
        )
        "#,
    )
    .bind(device_id)
    .bind(endpoint_id)
    .fetch_one(pool)
    .await
    .context("check removed sync peer")?;
    Ok(removed != 0)
}

pub async fn removed_peer_ids(pool: &SqlitePool) -> Result<HashSet<String>> {
    let rows = sqlx::query_scalar(
        r#"
        SELECT device_id FROM sync_removed_peers
        WHERE restored_at_ms IS NULL OR removed_at_ms > restored_at_ms
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list removed sync peers")?;
    Ok(rows.into_iter().collect())
}

pub async fn removed_devices(pool: &SqlitePool) -> Result<Vec<RemovedDevice>> {
    let rows = sqlx::query(
        r#"
        SELECT device_id, endpoint_id, removed_at_ms, restored_at_ms
        FROM sync_removed_peers
        WHERE endpoint_id NOT LIKE 'legacy:%'
        ORDER BY removed_at_ms ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("list removed sync devices")?;
    rows.into_iter()
        .map(|row| {
            Ok(RemovedDevice {
                device_id: row.try_get("device_id")?,
                endpoint_id: row.try_get("endpoint_id")?,
                removed_at_ms: row.try_get("removed_at_ms")?,
                restored_at_ms: row.try_get("restored_at_ms")?,
            })
        })
        .collect()
}

pub async fn latest_removed_at_ms(pool: &SqlitePool) -> Result<i64> {
    let value: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT MAX(MAX(removed_at_ms, COALESCE(restored_at_ms, 0)))
        FROM sync_removed_peers WHERE endpoint_id NOT LIKE 'legacy:%'
        "#,
    )
    .fetch_one(pool)
    .await
    .context("read latest removed device timestamp")?;
    Ok(value.unwrap_or(0))
}

/// 合并其他设备或 Hub 传来的删除标记，并清掉对应路由和投递状态。
pub async fn merge_removed_devices(pool: &SqlitePool, devices: &[RemovedDevice]) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("begin merging removed sync devices")?;
    for device in devices {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO sync_removed_peers (
                device_id, endpoint_id, removed_at_ms, restored_at_ms, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                endpoint_id = excluded.endpoint_id,
                removed_at_ms = MAX(sync_removed_peers.removed_at_ms, excluded.removed_at_ms),
                restored_at_ms = CASE
                    WHEN excluded.restored_at_ms IS NULL THEN sync_removed_peers.restored_at_ms
                    ELSE MAX(COALESCE(sync_removed_peers.restored_at_ms, 0), excluded.restored_at_ms)
                END,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&device.device_id)
        .bind(&device.endpoint_id)
        .bind(device.removed_at_ms)
        .bind(device.restored_at_ms)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .context("merge removed sync device")?;

        let (stored_removed_at_ms, stored_restored_at_ms): (i64, Option<i64>) = sqlx::query_as(
            "SELECT removed_at_ms, restored_at_ms FROM sync_removed_peers WHERE device_id = ?",
        )
        .bind(&device.device_id)
        .fetch_one(&mut *transaction)
        .await
        .context("read merged sync device membership")?;
        let effectively_removed = stored_restored_at_ms
            .is_none_or(|restored_at_ms| stored_removed_at_ms > restored_at_ms);
        if effectively_removed {
            let target_id = format!("peer:{}", device.device_id);
            sqlx::query("DELETE FROM sync_delivery_targets WHERE target_id = ?")
                .bind(&target_id)
                .execute(&mut *transaction)
                .await
                .context("clear removed peer deliveries")?;
            sqlx::query("DELETE FROM sync_peers WHERE device_id = ? OR endpoint_id = ?")
                .bind(&device.device_id)
                .bind(&device.endpoint_id)
                .execute(&mut *transaction)
                .await
                .context("purge removed sync peer route")?;
        }
    }
    transaction
        .commit()
        .await
        .context("commit removed sync devices")?;
    Ok(())
}

/// 忘记单个对端及其投递状态，并阻止发现与 Hub 设备列表将它自动加回。
pub async fn remove_peer(pool: &SqlitePool, device_id: &str) -> Result<Option<RemovedDevice>> {
    let mut transaction = pool.begin().await.context("begin removing sync peer")?;
    let endpoint_id: Option<String> =
        sqlx::query_scalar("SELECT endpoint_id FROM sync_peers WHERE device_id = ?")
            .bind(device_id)
            .fetch_optional(&mut *transaction)
            .await
            .context("find sync peer to remove")?;
    let Some(endpoint_id) = endpoint_id else {
        transaction
            .rollback()
            .await
            .context("rollback missing sync peer removal")?;
        return Ok(None);
    };

    let now = Utc::now();
    let removed = RemovedDevice {
        device_id: device_id.to_owned(),
        endpoint_id,
        removed_at_ms: now.timestamp_millis(),
        restored_at_ms: None,
    };
    sqlx::query(
        r#"
        INSERT INTO sync_removed_peers (
            device_id, endpoint_id, removed_at_ms, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            endpoint_id = excluded.endpoint_id,
            removed_at_ms = excluded.removed_at_ms,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&removed.device_id)
    .bind(&removed.endpoint_id)
    .bind(removed.removed_at_ms)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .context("remember removed sync peer")?;

    let target_id = format!("peer:{device_id}");
    sqlx::query("DELETE FROM sync_delivery_targets WHERE target_id = ?")
        .bind(&target_id)
        .execute(&mut *transaction)
        .await
        .context("clear forgotten peer deliveries")?;
    sqlx::query("DELETE FROM sync_peers WHERE device_id = ?")
        .bind(device_id)
        .execute(&mut *transaction)
        .await
        .context("delete sync peer")?;
    transaction
        .commit()
        .await
        .context("commit removing sync peer")?;
    Ok(Some(removed))
}

/// Refreshes the volatile routes of a paired endpoint discovered on the local network.
pub async fn update_peer_routes(
    pool: &SqlitePool,
    endpoint_id: &str,
    direct_addresses: &[String],
    refresh_presence: bool,
) -> Result<bool> {
    let now = Utc::now();
    let direct_addresses = serde_json::to_string(&lan_direct_addresses(direct_addresses))?;
    let result = sqlx::query(
        r#"
        UPDATE sync_peers
        SET direct_addresses = CASE WHEN ? = '[]' THEN direct_addresses ELSE ? END,
            relay_urls = '[]',
            last_seen_ms = ?, updated_at = ?
        WHERE endpoint_id = ?
          AND ((? != '[]' AND direct_addresses != ?)
            OR relay_urls != '[]')
        "#,
    )
    .bind(&direct_addresses)
    .bind(&direct_addresses)
    .bind(now.timestamp_millis())
    .bind(now)
    .bind(endpoint_id)
    .bind(&direct_addresses)
    .bind(&direct_addresses)
    .execute(pool)
    .await
    .context("refresh discovered peer routes")?;
    let routes_changed = result.rows_affected() > 0;
    if !routes_changed && refresh_presence {
        sqlx::query("UPDATE sync_peers SET last_seen_ms = ?, updated_at = ? WHERE endpoint_id = ?")
            .bind(now.timestamp_millis())
            .bind(now)
            .bind(endpoint_id)
            .execute(pool)
            .await
            .context("refresh discovered peer presence")?;
    }
    Ok(routes_changed)
}

/// Resolves a paired device from its stable Iroh endpoint identity.
pub async fn peer_device_id_by_endpoint(
    pool: &SqlitePool,
    endpoint_id: &str,
) -> Result<Option<String>> {
    sqlx::query_scalar("SELECT device_id FROM sync_peers WHERE endpoint_id = ?")
        .bind(endpoint_id)
        .fetch_optional(pool)
        .await
        .context("find peer by endpoint")
}

fn lan_direct_addresses(addresses: &[String]) -> Vec<String> {
    addresses
        .iter()
        .filter_map(|value| value.parse::<SocketAddr>().ok())
        .filter(|address| is_lan_ip(address.ip()))
        .map(|address| address.to_string())
        .collect()
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
