use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use ecopaste_sync_protocol::{
    CloudDevice, CloudEvent, DeviceAnnouncement, EncryptedEvent, PeerAnnouncement, RemovedDevice,
    SyncEventKind,
};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
};
use subtle::ConstantTimeEq;

#[derive(Debug)]
pub struct HistoryGap;

impl std::fmt::Display for HistoryGap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("同步游标早于 Hub 当前保留范围")
    }
}

impl std::error::Error for HistoryGap {}

#[derive(Debug)]
pub struct QuotaExceeded;

impl std::fmt::Display for QuotaExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("同步空间已达到 Hub 存储配额")
    }
}

impl std::error::Error for QuotaExceeded {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HubUsage {
    pub used_bytes: u64,
    pub event_count: u64,
    pub blob_count: u64,
    pub oldest_event_received_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct CleanupPolicy {
    pub retention_cutoff_ms: Option<i64>,
    pub legacy_blob_cutoff_ms: Option<i64>,
    pub max_events: Option<u64>,
    pub target_bytes: Option<u64>,
    pub orphan_cutoff_ms: i64,
    pub batch_size: u32,
}

#[derive(Debug, Default)]
pub struct CleanupResult {
    pub deleted_events: u64,
    pub deleted_blob_ids: Vec<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct Repository {
    pool: SqlitePool,
}

impl Repository {
    /// Opens the hub database and applies forward-only migrations.
    pub async fn open(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePool::connect_with(options)
            .await
            .with_context(|| format!("open SQLite database at {}", path.display()))?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("apply sync hub migrations")?;

        Ok(Self { pool })
    }

    /// Creates a group. Repeating the same request with the same token is idempotent.
    pub async fn create_group(&self, group_id: &str, access_token: &[u8]) -> Result<()> {
        validate_identifier("group_id", group_id)?;
        validate_access_token(access_token)?;
        let token_hash = blake3::hash(access_token);
        let result = sqlx::query(
            "INSERT OR IGNORE INTO sync_groups (group_id, access_token_hash, created_at_ms) VALUES (?, ?, ?)",
        )
        .bind(group_id)
        .bind(token_hash.as_bytes().as_slice())
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .context("insert sync group")?;

        if result.rows_affected() == 0 {
            self.authenticate(group_id, access_token).await?;
        }
        Ok(())
    }

    /// Checks a group's bearer token without comparing plaintext secrets.
    pub async fn authenticate(&self, group_id: &str, access_token: &[u8]) -> Result<()> {
        validate_identifier("group_id", group_id)?;
        validate_access_token(access_token)?;
        let stored: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT access_token_hash FROM sync_groups WHERE group_id = ?")
                .bind(group_id)
                .fetch_optional(&self.pool)
                .await
                .context("read sync group token")?;
        let Some(stored) = stored else {
            bail!("unauthorized");
        };
        let actual = blake3::hash(access_token);
        if stored.len() != actual.as_bytes().len()
            || stored.ct_eq(actual.as_bytes().as_slice()).unwrap_u8() != 1
        {
            bail!("unauthorized");
        }
        Ok(())
    }

    /// Updates cloud-visible device identity without retaining LAN transport routes.
    pub async fn upsert_device(&self, group_id: &str, device: &DeviceAnnouncement) -> Result<()> {
        validate_identifier("device_id", &device.device_id)?;
        validate_identifier("endpoint_id", &device.endpoint_id)?;
        if self
            .is_removed_device(group_id, &device.device_id, &device.endpoint_id)
            .await?
        {
            bail!("device was removed from this sync group");
        }
        if device.device_name.chars().count() > 80
            || device.platform.len() > 24
            || device.app_version.len() > 64
        {
            bail!("invalid device announcement");
        }
        if device.direct_addresses.len() > 16 || device.relay_urls.len() > 8 {
            bail!("too many device addresses");
        }
        sqlx::query(
            r#"
            INSERT INTO devices (
                group_id, device_id, device_name, platform, endpoint_id,
                direct_addresses, relay_urls, app_version, last_seen_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, device_id) DO UPDATE SET
                device_name = excluded.device_name,
                platform = excluded.platform,
                endpoint_id = excluded.endpoint_id,
                direct_addresses = excluded.direct_addresses,
                relay_urls = excluded.relay_urls,
                app_version = excluded.app_version,
                last_seen_ms = excluded.last_seen_ms
            "#,
        )
        .bind(group_id)
        .bind(&device.device_id)
        .bind(&device.device_name)
        .bind(&device.platform)
        .bind(&device.endpoint_id)
        .bind(Vec::<u8>::new())
        .bind(Vec::<u8>::new())
        .bind(&device.app_version)
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .context("upsert sync device")?;
        Ok(())
    }

    /// Refreshes the last-seen timestamp while a device keeps an authenticated watch open.
    pub async fn touch_device(&self, group_id: &str, endpoint_id: &str) -> Result<()> {
        sqlx::query("UPDATE devices SET last_seen_ms = ? WHERE group_id = ? AND endpoint_id = ?")
            .bind(now_ms())
            .bind(group_id)
            .bind(endpoint_id)
            .execute(&self.pool)
            .await
            .context("touch sync device")?;
        Ok(())
    }

    /// Returns every active member. Removed devices are absent from this directory.
    pub async fn list_devices(&self, group_id: &str) -> Result<Vec<CloudDevice>> {
        let rows = sqlx::query(
            r#"
            SELECT device_id, device_name, platform, endpoint_id,
                   app_version, last_seen_ms
            FROM devices
            WHERE group_id = ?
            ORDER BY last_seen_ms DESC, device_name ASC
            LIMIT 256
            "#,
        )
        .bind(group_id)
        .fetch_all(&self.pool)
        .await
        .context("list sync devices")?;

        rows.into_iter()
            .map(|row| {
                Ok(CloudDevice {
                    device_id: row.try_get("device_id")?,
                    device_name: row.try_get("device_name")?,
                    platform: row.try_get("platform")?,
                    endpoint_id: row.try_get("endpoint_id")?,
                    app_version: row.try_get("app_version")?,
                    last_seen_ms: row.try_get("last_seen_ms")?,
                    online: false,
                })
            })
            .collect()
    }

    pub async fn is_removed_endpoint(&self, group_id: &str, endpoint_id: &str) -> Result<bool> {
        let removed: i64 = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM removed_devices
                WHERE group_id = ? AND endpoint_id = ?
                  AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
            )
            "#,
        )
        .bind(group_id)
        .bind(endpoint_id)
        .fetch_one(&self.pool)
        .await
        .context("check removed sync endpoint")?;
        Ok(removed != 0)
    }

    pub async fn is_removed_device(
        &self,
        group_id: &str,
        device_id: &str,
        endpoint_id: &str,
    ) -> Result<bool> {
        let removed: i64 = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM removed_devices
                WHERE group_id = ? AND (device_id = ? OR endpoint_id = ?)
                  AND (restored_at_ms IS NULL OR removed_at_ms > restored_at_ms)
            )
            "#,
        )
        .bind(group_id)
        .bind(device_id)
        .bind(endpoint_id)
        .fetch_one(&self.pool)
        .await
        .context("check removed sync device")?;
        Ok(removed != 0)
    }

    /// Merges group-wide removal tombstones and purges matching routes from the Hub directory.
    pub async fn merge_removed_devices(
        &self,
        group_id: &str,
        devices: &[RemovedDevice],
    ) -> Result<(Vec<RemovedDevice>, bool)> {
        if devices.is_empty() {
            return Ok((self.list_removed_devices(group_id).await?, false));
        }

        let mut transaction = self
            .pool
            .begin()
            .await
            .context("begin merging removed devices")?;
        let mut changed = false;
        for device in devices {
            validate_identifier("device_id", &device.device_id)?;
            validate_identifier("endpoint_id", &device.endpoint_id)?;
            let now = now_ms();
            let stored = sqlx::query(
                r#"
                INSERT INTO removed_devices (
                    group_id, device_id, endpoint_id, removed_at_ms, restored_at_ms,
                    created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id, device_id) DO UPDATE SET
                    endpoint_id = excluded.endpoint_id,
                    removed_at_ms = MAX(removed_devices.removed_at_ms, excluded.removed_at_ms),
                    restored_at_ms = CASE
                        WHEN excluded.restored_at_ms IS NULL THEN removed_devices.restored_at_ms
                        ELSE MAX(COALESCE(removed_devices.restored_at_ms, 0), excluded.restored_at_ms)
                    END,
                    updated_at_ms = excluded.updated_at_ms
                WHERE removed_devices.endpoint_id != excluded.endpoint_id
                   OR excluded.removed_at_ms > removed_devices.removed_at_ms
                   OR COALESCE(excluded.restored_at_ms, 0) >
                      COALESCE(removed_devices.restored_at_ms, 0)
                "#,
            )
            .bind(group_id)
            .bind(&device.device_id)
            .bind(&device.endpoint_id)
            .bind(device.removed_at_ms)
            .bind(device.restored_at_ms)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .context("store removed sync device")?;
            changed |= stored.rows_affected() > 0;
            let (stored_removed_at_ms, stored_restored_at_ms): (i64, Option<i64>) = sqlx::query_as(
                r#"
                    SELECT removed_at_ms, restored_at_ms FROM removed_devices
                    WHERE group_id = ? AND device_id = ?
                    "#,
            )
            .bind(group_id)
            .bind(&device.device_id)
            .fetch_one(&mut *transaction)
            .await
            .context("read merged device membership")?;
            if stored_restored_at_ms
                .is_none_or(|restored_at_ms| stored_removed_at_ms > restored_at_ms)
            {
                sqlx::query(
                    "DELETE FROM devices WHERE group_id = ? AND (device_id = ? OR endpoint_id = ?)",
                )
                .bind(group_id)
                .bind(&device.device_id)
                .bind(&device.endpoint_id)
                .execute(&mut *transaction)
                .await
                .context("purge removed sync device route")?;
            }
        }
        transaction
            .commit()
            .await
            .context("commit removed sync devices")?;
        Ok((self.list_removed_devices(group_id).await?, changed))
    }

    pub async fn list_removed_devices(&self, group_id: &str) -> Result<Vec<RemovedDevice>> {
        let rows = sqlx::query(
            r#"
            SELECT device_id, endpoint_id, removed_at_ms, restored_at_ms
            FROM removed_devices
            WHERE group_id = ?
            ORDER BY removed_at_ms ASC
            "#,
        )
        .bind(group_id)
        .fetch_all(&self.pool)
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

    #[cfg(test)]
    pub async fn latest_removed_at_ms(&self, group_id: &str) -> Result<i64> {
        Ok(self.group_watermarks(group_id).await?.1)
    }

    pub async fn insert_events(
        &self,
        group_id: &str,
        events: &[EncryptedEvent],
        max_available_bytes: Option<u64>,
    ) -> Result<Vec<String>> {
        let mut transaction = self.pool.begin().await.context("begin event transaction")?;
        let referenced_blob_ids = events
            .iter()
            .flat_map(|event| event.blob_ids.iter())
            .collect::<HashSet<_>>();
        if !referenced_blob_ids.is_empty() {
            let mut query = sqlx::QueryBuilder::new("SELECT COUNT(*) FROM blobs WHERE group_id = ");
            query.push_bind(group_id).push(" AND blob_id IN (");
            let mut separated = query.separated(", ");
            for blob_id in &referenced_blob_ids {
                validate_blob_id(blob_id)?;
                separated.push_bind(blob_id.as_str());
            }
            query.push(")");
            let stored_count: i64 = query
                .build_query_scalar()
                .fetch_one(&mut *transaction)
                .await
                .context("validate encrypted event blob references")?;
            if usize::try_from(stored_count).unwrap_or_default() != referenced_blob_ids.len() {
                bail!("event references a missing blob");
            }
        }
        let mut used_bytes = if max_available_bytes.is_some() {
            let value: i64 = sqlx::query_scalar(
                "SELECT event_bytes + blob_bytes FROM sync_groups WHERE group_id = ?",
            )
            .bind(group_id)
            .fetch_one(&mut *transaction)
            .await
            .context("read sync group usage before inserting events")?;
            u64::try_from(value).context("negative sync group usage")?
        } else {
            0
        };
        let mut inserted_bytes = 0_u64;
        let mut inserted_count = 0_u64;
        let mut accepted = Vec::with_capacity(events.len());
        for event in events {
            validate_identifier("event_id", &event.event_id)?;
            validate_identifier("origin_device_id", &event.origin_device_id)?;
            if event.nonce.len() != 24 || event.ciphertext.is_empty() {
                bail!("invalid encrypted event");
            }
            let mut blob_ids = event.blob_ids.clone();
            blob_ids.sort_unstable();
            blob_ids.dedup();
            if blob_ids.len() > 256 {
                bail!("too many event blob references");
            }
            for blob_id in &blob_ids {
                validate_blob_id(blob_id)?;
            }
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO events (
                    group_id, event_id, origin_device_id, origin_sequence,
                    created_at_ms, received_at_ms, nonce, ciphertext, event_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(group_id)
            .bind(&event.event_id)
            .bind(&event.origin_device_id)
            .bind(i64::try_from(event.origin_sequence).context("origin sequence is too large")?)
            .bind(event.created_at_ms)
            .bind(now_ms())
            .bind(&event.nonce)
            .bind(&event.ciphertext)
            .bind(event.kind.as_i64())
            .execute(&mut *transaction)
            .await
            .context("insert encrypted event")?;
            if result.rows_affected() == 1 {
                let event_bytes = u64::try_from(event.nonce.len() + event.ciphertext.len())
                    .context("encrypted event is too large")?;
                if max_available_bytes
                    .is_some_and(|maximum| used_bytes.saturating_add(event_bytes) > maximum)
                {
                    return Err(QuotaExceeded.into());
                }
                used_bytes = used_bytes.saturating_add(event_bytes);
                inserted_bytes = inserted_bytes.saturating_add(event_bytes);
                inserted_count = inserted_count.saturating_add(1);
                for blob_id in blob_ids {
                    sqlx::query(
                        r#"
                        INSERT INTO event_blob_refs (
                            group_id, event_id, blob_id, created_at_ms
                        ) VALUES (?, ?, ?, ?)
                        "#,
                    )
                    .bind(group_id)
                    .bind(&event.event_id)
                    .bind(blob_id)
                    .bind(now_ms())
                    .execute(&mut *transaction)
                    .await
                    .context("insert encrypted event blob reference")?;
                }
                accepted.push(event.event_id.clone());
            }
        }
        if inserted_count > 0 {
            sqlx::query(
                r#"
                UPDATE sync_groups
                SET event_bytes = event_bytes + ?, event_count = event_count + ?
                WHERE group_id = ?
                "#,
            )
            .bind(i64::try_from(inserted_bytes).context("event byte usage is too large")?)
            .bind(i64::try_from(inserted_count).context("event count is too large")?)
            .bind(group_id)
            .execute(&mut *transaction)
            .await
            .context("update sync group event usage")?;
        }
        transaction
            .commit()
            .await
            .context("commit encrypted events")?;
        Ok(accepted)
    }

    /// Reads encrypted cloud events after the caller's delivery cursor.
    pub async fn list_events(
        &self,
        group_id: &str,
        after_cursor: u64,
        limit: u16,
    ) -> Result<Vec<CloudEvent>> {
        let rows = sqlx::query(
            r#"
            SELECT cursor, event_id, origin_device_id, origin_sequence,
                   created_at_ms, nonce, ciphertext, event_kind
            FROM events
            WHERE group_id = ? AND cursor > ?
            ORDER BY cursor ASC
            LIMIT ?
            "#,
        )
        .bind(group_id)
        .bind(i64::try_from(after_cursor).unwrap_or(i64::MAX))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .context("list encrypted events")?;

        let mut events = rows
            .into_iter()
            .map(cloud_event_from_row)
            .collect::<Result<Vec<_>>>()?;
        self.attach_blob_ids(group_id, &mut events).await?;
        Ok(events)
    }

    pub async fn latest_cursor(&self, group_id: &str) -> Result<u64> {
        let cursor: i64 = sqlx::query_scalar(
            r#"
            SELECT MAX(
                pruned_through_cursor,
                COALESCE((SELECT MAX(cursor) FROM events WHERE group_id = ?), 0)
            )
            FROM sync_groups
            WHERE group_id = ?
            "#,
        )
        .bind(group_id)
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read latest encrypted event cursor")?;
        u64::try_from(cursor).context("negative event cursor")
    }

    /// 同一快照读取事件与成员变更水位，减少长连接唤醒时的数据库往返。
    pub async fn group_watermarks(&self, group_id: &str) -> Result<(u64, i64)> {
        let (cursor, removed): (i64, i64) = sqlx::query_as(
            "SELECT MAX(pruned_through_cursor, \
                     COALESCE((SELECT MAX(cursor) FROM events WHERE group_id = ?), 0)), \
             COALESCE((SELECT MAX(MAX(removed_at_ms, COALESCE(restored_at_ms, 0))) \
             FROM removed_devices WHERE group_id = ?), 0) \
             FROM sync_groups WHERE group_id = ?",
        )
        .bind(group_id)
        .bind(group_id)
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read sync group watermarks")?;
        Ok((
            u64::try_from(cursor).context("negative event cursor")?,
            removed,
        ))
    }

    /// Reads one newest-first page without changing any device delivery cursor.
    pub async fn list_events_before(
        &self,
        group_id: &str,
        before_cursor: Option<u64>,
        limit: u16,
    ) -> Result<(Vec<CloudEvent>, Option<u64>)> {
        let fetch_limit = i64::from(limit) + 1;
        let rows = if let Some(before_cursor) = before_cursor {
            sqlx::query(
                r#"
                SELECT cursor, event_id, origin_device_id, origin_sequence,
                       created_at_ms, nonce, ciphertext, event_kind
                FROM events
                WHERE group_id = ? AND event_kind = ? AND cursor < ?
                ORDER BY cursor DESC
                LIMIT ?
                "#,
            )
            .bind(group_id)
            .bind(SyncEventKind::Content.as_i64())
            .bind(i64::try_from(before_cursor).unwrap_or(i64::MAX))
            .bind(fetch_limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT cursor, event_id, origin_device_id, origin_sequence,
                       created_at_ms, nonce, ciphertext, event_kind
                FROM events
                WHERE group_id = ? AND event_kind = ?
                ORDER BY cursor DESC
                LIMIT ?
                "#,
            )
            .bind(group_id)
            .bind(SyncEventKind::Content.as_i64())
            .bind(fetch_limit)
            .fetch_all(&self.pool)
            .await
        }
        .context("list encrypted event page")?;

        let mut events = rows
            .into_iter()
            .map(cloud_event_from_row)
            .collect::<Result<Vec<_>>>()?;
        let has_more = events.len() > usize::from(limit);
        events.truncate(usize::from(limit));
        let next_before_cursor = has_more.then(|| {
            events
                .last()
                .expect("non-empty page when another event exists")
                .cursor
        });
        self.attach_blob_ids(group_id, &mut events).await?;
        Ok((events, next_before_cursor))
    }

    pub async fn event_count(&self, group_id: &str) -> Result<u64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE group_id = ? AND event_kind = ?")
                .bind(group_id)
                .bind(SyncEventKind::Content.as_i64())
                .fetch_one(&self.pool)
                .await
                .context("count encrypted events")?;
        u64::try_from(count).context("negative event count")
    }

    /// Returns recently seen group members without exposing LAN transport routes.
    pub async fn list_peers(
        &self,
        group_id: &str,
        excluding_device_id: &str,
    ) -> Result<Vec<PeerAnnouncement>> {
        let rows = sqlx::query(
            r#"
            SELECT device_id, device_name, platform, endpoint_id, last_seen_ms
            FROM devices
            WHERE group_id = ? AND device_id != ?
            ORDER BY last_seen_ms DESC
            LIMIT 64
            "#,
        )
        .bind(group_id)
        .bind(excluding_device_id)
        .fetch_all(&self.pool)
        .await
        .context("list sync peers")?;

        rows.into_iter()
            .map(|row| {
                Ok(PeerAnnouncement {
                    device_id: row.try_get("device_id")?,
                    device_name: row.try_get("device_name")?,
                    platform: row.try_get("platform")?,
                    endpoint_id: row.try_get("endpoint_id")?,
                    direct_addresses: Vec::new(),
                    relay_urls: Vec::new(),
                    last_seen_ms: row.try_get("last_seen_ms")?,
                })
            })
            .collect()
    }

    pub async fn record_blob(
        &self,
        group_id: &str,
        blob_id: &str,
        size: u64,
        managed: bool,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await.context("begin recording blob")?;
        let existing_size: Option<i64> =
            sqlx::query_scalar("SELECT size FROM blobs WHERE group_id = ? AND blob_id = ?")
                .bind(group_id)
                .bind(blob_id)
                .fetch_optional(&mut *transaction)
                .await
                .context("read existing encrypted blob metadata")?;
        let size = i64::try_from(size).context("blob is too large")?;
        sqlx::query(
            r#"
            INSERT INTO blobs (
                group_id, blob_id, size, created_at_ms, gc_protected
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(group_id, blob_id) DO UPDATE SET
                size = excluded.size,
                created_at_ms = excluded.created_at_ms,
                gc_protected = blobs.gc_protected
            "#,
        )
        .bind(group_id)
        .bind(blob_id)
        .bind(size)
        .bind(now_ms())
        .bind(i64::from(!managed))
        .execute(&mut *transaction)
        .await
        .context("record encrypted blob")?;
        let (byte_delta, count_delta) = match existing_size {
            Some(existing_size) => (
                size.checked_sub(existing_size)
                    .context("encrypted blob usage overflow")?,
                0_i64,
            ),
            None => (size, 1_i64),
        };
        sqlx::query(
            r#"
            UPDATE sync_groups
            SET blob_bytes = blob_bytes + ?, blob_count = blob_count + ?
            WHERE group_id = ?
            "#,
        )
        .bind(byte_delta)
        .bind(count_delta)
        .bind(group_id)
        .execute(&mut *transaction)
        .await
        .context("update sync group blob usage")?;
        transaction.commit().await.context("commit recorded blob")?;
        Ok(())
    }

    pub async fn blob_size(&self, group_id: &str, blob_id: &str) -> Result<Option<u64>> {
        let size: Option<i64> =
            sqlx::query_scalar("SELECT size FROM blobs WHERE group_id = ? AND blob_id = ?")
                .bind(group_id)
                .bind(blob_id)
                .fetch_optional(&self.pool)
                .await
                .context("read encrypted blob metadata")?;
        size.map(|value| u64::try_from(value).context("negative blob size"))
            .transpose()
    }

    /// Refreshes the orphan grace period when a client reuses an already stored blob.
    pub async fn touch_blob(&self, group_id: &str, blob_id: &str) -> Result<()> {
        sqlx::query("UPDATE blobs SET created_at_ms = ? WHERE group_id = ? AND blob_id = ?")
            .bind(now_ms())
            .bind(group_id)
            .bind(blob_id)
            .execute(&self.pool)
            .await
            .context("refresh legacy Hub blob protection")?;
        Ok(())
    }

    /// Returns exact logical payload usage without forcing SQLite file compaction.
    pub async fn group_usage(&self, group_id: &str) -> Result<HubUsage> {
        let row = sqlx::query(
            r#"
            SELECT
                event_bytes + blob_bytes AS used_bytes,
                event_count,
                blob_count,
                (SELECT MIN(received_at_ms) FROM events WHERE group_id = ?)
                    AS oldest_event_received_at_ms
            FROM sync_groups
            WHERE group_id = ?
            "#,
        )
        .bind(group_id)
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read sync group usage")?;
        let used_bytes: i64 = row.try_get("used_bytes")?;
        let event_count: i64 = row.try_get("event_count")?;
        let blob_count: i64 = row.try_get("blob_count")?;
        Ok(HubUsage {
            used_bytes: u64::try_from(used_bytes).context("negative sync group byte usage")?,
            event_count: u64::try_from(event_count).context("negative sync group event count")?,
            blob_count: u64::try_from(blob_count).context("negative sync group blob count")?,
            oldest_event_received_at_ms: row.try_get("oldest_event_received_at_ms")?,
        })
    }

    pub async fn group_used_bytes(&self, group_id: &str) -> Result<u64> {
        let used: i64 = sqlx::query_scalar(
            "SELECT event_bytes + blob_bytes FROM sync_groups WHERE group_id = ?",
        )
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read sync group byte usage")?;
        u64::try_from(used).context("negative sync group byte usage")
    }

    pub async fn list_group_ids(&self) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT group_id FROM sync_groups ORDER BY group_id")
            .fetch_all(&self.pool)
            .await
            .context("list sync groups")
    }

    pub async fn ensure_cursor_available(&self, group_id: &str, cursor: u64) -> Result<()> {
        if cursor == 0 {
            return Ok(());
        }
        let pruned: i64 =
            sqlx::query_scalar("SELECT pruned_through_cursor FROM sync_groups WHERE group_id = ?")
                .bind(group_id)
                .fetch_one(&self.pool)
                .await
                .context("read retained sync cursor floor")?;
        if cursor < u64::try_from(pruned).context("negative retained cursor floor")? {
            return Err(HistoryGap.into());
        }
        Ok(())
    }

    /// Returns the complete cursor range still retained by the Hub.
    pub async fn bootstrap_cursor_bounds(&self, group_id: &str) -> Result<(u64, u64)> {
        let (pruned, latest): (i64, i64) = sqlx::query_as(
            r#"
            SELECT pruned_through_cursor,
                   MAX(pruned_through_cursor,
                       COALESCE((SELECT MAX(cursor) FROM events WHERE group_id = ?), 0))
            FROM sync_groups
            WHERE group_id = ?
            "#,
        )
        .bind(group_id)
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read bootstrap cursor bounds")?;
        Ok((
            u64::try_from(pruned).context("negative retained cursor floor")?,
            u64::try_from(latest).context("negative latest bootstrap cursor")?,
        ))
    }

    /// Deletes one bounded oldest-first batch and returns blob files that became unreferenced.
    pub async fn cleanup_group(
        &self,
        group_id: &str,
        policy: CleanupPolicy,
    ) -> Result<CleanupResult> {
        let batch_size = policy.batch_size.max(1);
        let mut transaction = self.pool.begin().await.context("begin Hub cleanup")?;
        let stale_blob_rows = sqlx::query(
            r#"
            SELECT blob_id, size FROM blobs b
            WHERE group_id = ?
              AND (
                  (gc_protected = 0 AND created_at_ms < ?)
                  OR (gc_protected = 1 AND ? IS NOT NULL AND created_at_ms < ?)
              )
              AND NOT EXISTS (
                  SELECT 1 FROM event_blob_refs r
                  WHERE r.group_id = b.group_id AND r.blob_id = b.blob_id
              )
            ORDER BY created_at_ms ASC
            LIMIT ?
            "#,
        )
        .bind(group_id)
        .bind(policy.orphan_cutoff_ms)
        .bind(policy.legacy_blob_cutoff_ms)
        .bind(policy.legacy_blob_cutoff_ms)
        .bind(i64::from(batch_size))
        .fetch_all(&mut *transaction)
        .await
        .context("list stale unreferenced Hub blobs")?;
        let stale_blob_batch_full = stale_blob_rows.len() == batch_size as usize;
        let mut deleted_blob_ids = Vec::with_capacity(stale_blob_rows.len());
        let mut deleted_blob_bytes = 0_u64;
        for row in stale_blob_rows {
            let blob_id: String = row.try_get("blob_id")?;
            let size = u64::try_from(row.try_get::<i64, _>("size")?)
                .context("negative stale Hub blob size")?;
            sqlx::query("DELETE FROM blobs WHERE group_id = ? AND blob_id = ?")
                .bind(group_id)
                .bind(&blob_id)
                .execute(&mut *transaction)
                .await
                .context("delete stale Hub blob metadata")?;
            deleted_blob_ids.push(blob_id);
            deleted_blob_bytes = deleted_blob_bytes.saturating_add(size);
        }
        if !deleted_blob_ids.is_empty() {
            sqlx::query(
                r#"
                UPDATE sync_groups
                SET blob_bytes = MAX(0, blob_bytes - ?),
                    blob_count = MAX(0, blob_count - ?)
                WHERE group_id = ?
                "#,
            )
            .bind(i64::try_from(deleted_blob_bytes).context("stale Hub blob usage is too large")?)
            .bind(i64::try_from(deleted_blob_ids.len()).context("too many stale Hub blobs")?)
            .bind(group_id)
            .execute(&mut *transaction)
            .await
            .context("update stale Hub blob usage")?;
        }

        let usage_row = sqlx::query(
            r#"
            SELECT event_bytes + blob_bytes AS used_bytes, event_count
            FROM sync_groups
            WHERE group_id = ?
            "#,
        )
        .bind(group_id)
        .fetch_one(&mut *transaction)
        .await
        .context("read Hub cleanup usage")?;
        let used_bytes = u64::try_from(usage_row.try_get::<i64, _>("used_bytes")?)
            .context("negative Hub cleanup usage")?;
        let event_count = u64::try_from(usage_row.try_get::<i64, _>("event_count")?)
            .context("negative Hub cleanup event count")?;
        let candidates = sqlx::query(
            r#"
            SELECT cursor, length(nonce) + length(ciphertext) AS event_bytes
            FROM events
            WHERE group_id = ?
            ORDER BY cursor ASC
            LIMIT ?
            "#,
        )
        .bind(group_id)
        .bind(i64::from(batch_size))
        .fetch_all(&mut *transaction)
        .await
        .context("list Hub cleanup candidates")?;
        if candidates.is_empty() {
            transaction
                .commit()
                .await
                .context("commit orphan cleanup")?;
            return Ok(CleanupResult {
                deleted_blob_ids,
                has_more: stale_blob_batch_full,
                ..CleanupResult::default()
            });
        }

        let count_excess = policy
            .max_events
            .map_or(0, |maximum| event_count.saturating_sub(maximum));
        let time_boundary = if let Some(cutoff) = policy.retention_cutoff_ms {
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT MAX(cursor) FROM events WHERE group_id = ? AND received_at_ms < ?",
            )
            .bind(group_id)
            .bind(cutoff)
            .fetch_one(&mut *transaction)
            .await
            .context("read Hub retention time boundary")?
        } else {
            None
        };
        let time_excess = time_boundary.map_or(0, |boundary| {
            candidates
                .iter()
                .take_while(|row| row.get::<i64, _>("cursor") <= boundary)
                .count() as u64
        });
        let mut delete_count = count_excess.max(time_excess).min(u64::from(batch_size)) as usize;
        let quota_excess = policy
            .target_bytes
            .map_or(0, |maximum| used_bytes.saturating_sub(maximum));

        if quota_excess > 0 {
            let last_candidate_cursor: i64 = candidates
                .last()
                .expect("cleanup candidates are non-empty")
                .get("cursor");
            let orphanable = sqlx::query(
                r#"
                SELECT r.blob_id, b.size, MAX(e.cursor) AS last_cursor
                FROM event_blob_refs r
                JOIN events e
                  ON e.group_id = r.group_id AND e.event_id = r.event_id
                JOIN blobs b
                  ON b.group_id = r.group_id AND b.blob_id = r.blob_id
                WHERE r.group_id = ? AND b.gc_protected = 0
                GROUP BY r.blob_id, b.size
                HAVING MAX(e.cursor) <= ?
                "#,
            )
            .bind(group_id)
            .bind(last_candidate_cursor)
            .fetch_all(&mut *transaction)
            .await
            .context("list blobs released by Hub cleanup")?;
            let mut released_by_cursor: HashMap<i64, (u64, Vec<String>)> = HashMap::new();
            for row in orphanable {
                let cursor: i64 = row.try_get("last_cursor")?;
                let size = u64::try_from(row.try_get::<i64, _>("size")?)
                    .context("negative Hub blob size")?;
                let entry = released_by_cursor.entry(cursor).or_default();
                entry.0 = entry.0.saturating_add(size);
                entry.1.push(row.try_get("blob_id")?);
            }
            let mut released = 0_u64;
            for (index, row) in candidates.iter().enumerate() {
                released = released.saturating_add(
                    u64::try_from(row.get::<i64, _>("event_bytes")).unwrap_or_default(),
                );
                let cursor: i64 = row.get("cursor");
                if let Some((bytes, _)) = released_by_cursor.get(&cursor) {
                    released = released.saturating_add(*bytes);
                }
                if released >= quota_excess {
                    delete_count = delete_count.max(index + 1);
                    break;
                }
                delete_count = candidates.len();
            }

            if delete_count > 0 {
                let deleted_through: i64 = candidates[delete_count - 1].get("cursor");
                for (cursor, (_, blob_ids)) in released_by_cursor {
                    if cursor <= deleted_through {
                        deleted_blob_ids.extend(blob_ids);
                    }
                }
            }
        }

        if delete_count == 0 {
            transaction
                .commit()
                .await
                .context("commit idle Hub cleanup")?;
            return Ok(CleanupResult {
                deleted_blob_ids,
                has_more: stale_blob_batch_full,
                ..CleanupResult::default()
            });
        }

        let deleted_through: i64 = candidates[delete_count - 1].get("cursor");
        let deleted_event_bytes = candidates[..delete_count]
            .iter()
            .map(|row| u64::try_from(row.get::<i64, _>("event_bytes")).unwrap_or_default())
            .sum::<u64>();
        let released_blob_rows = sqlx::query(
            r#"
            SELECT r.blob_id, b.size
            FROM event_blob_refs r
            JOIN events e
              ON e.group_id = r.group_id AND e.event_id = r.event_id
            JOIN blobs b
              ON b.group_id = r.group_id AND b.blob_id = r.blob_id
            WHERE r.group_id = ? AND b.gc_protected = 0
            GROUP BY r.blob_id
            HAVING MAX(e.cursor) <= ?
            "#,
        )
        .bind(group_id)
        .bind(deleted_through)
        .fetch_all(&mut *transaction)
        .await
        .context("list blobs released by retained event deletion")?;
        let released_blob_sizes = released_blob_rows
            .into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("blob_id")?,
                    u64::try_from(row.try_get::<i64, _>("size")?)
                        .context("negative released Hub blob size")?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        deleted_blob_ids.extend(released_blob_sizes.keys().cloned());
        sqlx::query("DELETE FROM events WHERE group_id = ? AND cursor <= ?")
            .bind(group_id)
            .bind(deleted_through)
            .execute(&mut *transaction)
            .await
            .context("delete retained Hub events")?;
        sqlx::query(
            "UPDATE sync_groups SET pruned_through_cursor = MAX(pruned_through_cursor, ?) WHERE group_id = ?",
        )
        .bind(deleted_through)
        .bind(group_id)
        .execute(&mut *transaction)
        .await
        .context("advance retained Hub cursor floor")?;
        deleted_blob_ids.sort_unstable();
        deleted_blob_ids.dedup();
        let mut released_blob_bytes = 0_u64;
        let mut released_blob_count = 0_u64;
        for blob_id in &deleted_blob_ids {
            let deleted = sqlx::query(
                r#"
                DELETE FROM blobs
                WHERE group_id = ? AND blob_id = ? AND gc_protected = 0
                  AND NOT EXISTS (
                      SELECT 1 FROM event_blob_refs r
                      WHERE r.group_id = blobs.group_id AND r.blob_id = blobs.blob_id
                  )
                "#,
            )
            .bind(group_id)
            .bind(blob_id)
            .execute(&mut *transaction)
            .await
            .context("delete released Hub blob metadata")?;
            if deleted.rows_affected() > 0 {
                released_blob_bytes = released_blob_bytes.saturating_add(
                    released_blob_sizes
                        .get(blob_id)
                        .copied()
                        .unwrap_or_default(),
                );
                released_blob_count = released_blob_count.saturating_add(1);
            }
        }
        sqlx::query(
            r#"
            UPDATE sync_groups
            SET event_bytes = MAX(0, event_bytes - ?),
                event_count = MAX(0, event_count - ?),
                blob_bytes = MAX(0, blob_bytes - ?),
                blob_count = MAX(0, blob_count - ?)
            WHERE group_id = ?
            "#,
        )
        .bind(i64::try_from(deleted_event_bytes).context("deleted event usage is too large")?)
        .bind(i64::try_from(delete_count).context("too many deleted Hub events")?)
        .bind(i64::try_from(released_blob_bytes).context("released blob usage is too large")?)
        .bind(i64::try_from(released_blob_count).context("too many released Hub blobs")?)
        .bind(group_id)
        .execute(&mut *transaction)
        .await
        .context("update retained Hub usage")?;
        transaction.commit().await.context("commit Hub cleanup")?;
        Ok(CleanupResult {
            deleted_events: delete_count as u64,
            deleted_blob_ids,
            has_more: stale_blob_batch_full || delete_count == candidates.len(),
        })
    }

    async fn attach_blob_ids(&self, group_id: &str, events: &mut [CloudEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut query = sqlx::QueryBuilder::new(
            "SELECT event_id, blob_id FROM event_blob_refs WHERE group_id = ",
        );
        query.push_bind(group_id).push(" AND event_id IN (");
        let mut separated = query.separated(", ");
        for event in events.iter() {
            separated.push_bind(&event.event.event_id);
        }
        query.push(") ORDER BY event_id, blob_id");
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .context("list encrypted event blob references")?;
        let mut by_event: HashMap<String, Vec<String>> = HashMap::new();
        for row in rows {
            by_event
                .entry(row.try_get("event_id")?)
                .or_default()
                .push(row.try_get("blob_id")?);
        }
        for event in events {
            event.event.blob_ids = by_event.remove(&event.event.event_id).unwrap_or_default();
        }
        Ok(())
    }
}

pub fn validate_identifier(name: &str, value: &str) -> Result<()> {
    let valid = (8..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if !valid {
        bail!("invalid {name}");
    }
    Ok(())
}

fn validate_blob_id(value: &str) -> Result<()> {
    validate_identifier("blob_id", value)?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("invalid blob_id");
    }
    Ok(())
}

fn validate_access_token(value: &[u8]) -> Result<()> {
    if !(32..=128).contains(&value.len()) {
        bail!("invalid access token");
    }
    Ok(())
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn cloud_event_from_row(row: sqlx::sqlite::SqliteRow) -> Result<CloudEvent> {
    let cursor: i64 = row.try_get("cursor")?;
    let sequence: i64 = row.try_get("origin_sequence")?;
    Ok(CloudEvent {
        cursor: u64::try_from(cursor).context("negative event cursor")?,
        event: EncryptedEvent {
            event_id: row.try_get("event_id")?,
            origin_device_id: row.try_get("origin_device_id")?,
            origin_sequence: u64::try_from(sequence).context("negative origin sequence")?,
            created_at_ms: row.try_get("created_at_ms")?,
            nonce: row.try_get("nonce")?,
            ciphertext: row.try_get("ciphertext")?,
            kind: event_kind_from_row(&row)?,
            blob_ids: Vec::new(),
        },
    })
}

fn event_kind_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<SyncEventKind> {
    let value: i64 = row.try_get("event_kind")?;
    SyncEventKind::from_i64(value).context("invalid synchronized event kind")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn combined_watermarks_preserve_group_isolation_and_restoration() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        for group in ["group_123", "group_456"] {
            repository.create_group(group, &[7; 32]).await.unwrap();
        }
        assert_eq!(
            repository.group_watermarks("group_123").await.unwrap(),
            (0, 0)
        );
        let event = EncryptedEvent {
            event_id: "event_123".into(),
            origin_device_id: "device_123".into(),
            origin_sequence: 1,
            created_at_ms: 1,
            nonce: vec![1; 24],
            ciphertext: vec![1],
            kind: SyncEventKind::Content,
            blob_ids: Vec::new(),
        };
        repository
            .insert_events("group_123", &[event], None)
            .await
            .unwrap();
        let removed = RemovedDevice {
            device_id: "device_456".into(),
            endpoint_id: "endpoint_456".into(),
            removed_at_ms: 100,
            restored_at_ms: Some(200),
        };
        repository
            .merge_removed_devices("group_123", std::slice::from_ref(&removed))
            .await
            .unwrap();
        repository
            .merge_removed_devices(
                "group_456",
                &[RemovedDevice {
                    removed_at_ms: 999,
                    ..removed
                }],
            )
            .await
            .unwrap();
        let (cursor, removed) = repository.group_watermarks("group_123").await.unwrap();
        assert_eq!(cursor, repository.latest_cursor("group_123").await.unwrap());
        assert!(cursor > 0);
        assert_eq!(removed, 200);
        assert_eq!(
            repository.group_watermarks("group_456").await.unwrap(),
            (0, 999)
        );
    }

    #[tokio::test]
    async fn group_authentication_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        let token = [7_u8; 32];

        repository.create_group("group_123", &token).await.unwrap();
        repository.create_group("group_123", &token).await.unwrap();

        assert!(
            repository
                .authenticate("group_123", &[8_u8; 32])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cloud_history_is_newest_first_and_paged() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let events = (1..=3)
            .map(|sequence| EncryptedEvent {
                event_id: format!("event_{sequence:03}"),
                origin_device_id: "device_123".into(),
                origin_sequence: sequence,
                created_at_ms: sequence as i64,
                nonce: vec![sequence as u8; 24],
                ciphertext: vec![sequence as u8],
                kind: SyncEventKind::Content,
                blob_ids: Vec::new(),
            })
            .collect::<Vec<_>>();
        repository
            .insert_events("group_123", &events, None)
            .await
            .unwrap();
        repository
            .insert_events(
                "group_123",
                &[EncryptedEvent {
                    event_id: "state_004".into(),
                    origin_device_id: "device_123".into(),
                    origin_sequence: 4,
                    created_at_ms: 4,
                    nonce: vec![4; 24],
                    ciphertext: vec![4],
                    kind: SyncEventKind::ItemState,
                    blob_ids: Vec::new(),
                }],
                None,
            )
            .await
            .unwrap();

        let (first, next) = repository
            .list_events_before("group_123", None, 2)
            .await
            .unwrap();
        assert_eq!(
            first.iter().map(|item| item.cursor).collect::<Vec<_>>(),
            vec![3, 2]
        );
        assert_eq!(next, Some(2));

        let (second, next) = repository
            .list_events_before("group_123", next, 2)
            .await
            .unwrap();
        assert_eq!(
            second.iter().map(|item| item.cursor).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(next, None);
        assert_eq!(repository.event_count("group_123").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn removed_device_is_purged_and_cannot_reannounce() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let device = DeviceAnnouncement {
            device_id: "device_123".into(),
            device_name: "Android".into(),
            platform: "android".into(),
            endpoint_id: "endpoint_123".into(),
            direct_addresses: Vec::new(),
            relay_urls: Vec::new(),
            app_version: "1.1.154".into(),
        };
        repository
            .upsert_device("group_123", &device)
            .await
            .unwrap();

        let removed = RemovedDevice {
            device_id: device.device_id.clone(),
            endpoint_id: device.endpoint_id.clone(),
            removed_at_ms: 100,
            restored_at_ms: None,
        };
        repository
            .merge_removed_devices("group_123", std::slice::from_ref(&removed))
            .await
            .unwrap();

        assert!(
            repository
                .list_peers("group_123", "another_device")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repository
                .is_removed_device("group_123", &device.device_id, &device.endpoint_id)
                .await
                .unwrap()
        );
        assert_eq!(
            repository.latest_removed_at_ms("group_123").await.unwrap(),
            100
        );
        assert!(
            repository
                .upsert_device("group_123", &device)
                .await
                .is_err()
        );

        let restored = RemovedDevice {
            restored_at_ms: Some(200),
            ..removed
        };
        repository
            .merge_removed_devices("group_123", &[restored])
            .await
            .unwrap();
        assert!(
            !repository
                .is_removed_device("group_123", &device.device_id, &device.endpoint_id)
                .await
                .unwrap()
        );
        repository
            .upsert_device("group_123", &device)
            .await
            .unwrap();
        assert_eq!(
            repository.latest_removed_at_ms("group_123").await.unwrap(),
            200
        );
    }

    #[tokio::test]
    async fn device_directory_does_not_store_or_return_lan_routes() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        repository
            .upsert_device(
                "group_123",
                &DeviceAnnouncement {
                    device_id: "device_123".into(),
                    device_name: "Android".into(),
                    platform: "android".into(),
                    endpoint_id: "endpoint_123".into(),
                    direct_addresses: vec!["172.19.0.1:35555".into()],
                    relay_urls: vec!["https://relay.example".into()],
                    app_version: "1.1.160".into(),
                },
            )
            .await
            .unwrap();

        let stored_lengths: (i64, i64) = sqlx::query_as(
            "SELECT length(direct_addresses), length(relay_urls) FROM devices WHERE group_id = 'group_123' AND device_id = 'device_123'",
        )
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(stored_lengths, (0, 0));

        let peer = repository
            .list_peers("group_123", "another_device")
            .await
            .unwrap()
            .remove(0);
        assert!(peer.direct_addresses.is_empty());
        assert!(peer.relay_urls.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_uses_complete_retained_hub_range() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let events = (1..=5).map(test_event).collect::<Vec<_>>();
        repository
            .insert_events("group_123", &events, None)
            .await
            .unwrap();
        repository
            .cleanup_group(
                "group_123",
                CleanupPolicy {
                    retention_cutoff_ms: None,
                    legacy_blob_cutoff_ms: None,
                    max_events: Some(2),
                    target_bytes: None,
                    orphan_cutoff_ms: i64::MAX,
                    batch_size: 10,
                },
            )
            .await
            .unwrap();
        let (start, latest) = repository
            .bootstrap_cursor_bounds("group_123")
            .await
            .unwrap();

        assert_eq!(start, 3);
        assert_eq!(latest, 5);
    }

    #[tokio::test]
    async fn cleanup_advances_gap_floor_and_reclaims_referenced_blobs() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let blob_id = "a".repeat(64);
        repository
            .record_blob("group_123", &blob_id, 128, true)
            .await
            .unwrap();
        let mut events = (1..=3).map(test_event).collect::<Vec<_>>();
        events[0].blob_ids.push(blob_id.clone());
        repository
            .insert_events("group_123", &events, None)
            .await
            .unwrap();

        let result = repository
            .cleanup_group(
                "group_123",
                CleanupPolicy {
                    retention_cutoff_ms: None,
                    legacy_blob_cutoff_ms: None,
                    max_events: Some(1),
                    target_bytes: None,
                    orphan_cutoff_ms: i64::MAX,
                    batch_size: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.deleted_events, 2);
        assert_eq!(result.deleted_blob_ids, vec![blob_id]);
        assert!(
            repository
                .ensure_cursor_available("group_123", 0)
                .await
                .is_ok()
        );
        assert!(
            repository
                .ensure_cursor_available("group_123", 1)
                .await
                .is_err()
        );
        assert!(
            repository
                .ensure_cursor_available("group_123", 2)
                .await
                .is_ok()
        );
        assert_eq!(repository.latest_cursor("group_123").await.unwrap(), 3);
        let usage = repository.group_usage("group_123").await.unwrap();
        assert_eq!(usage.blob_count, 0);
        assert_eq!(usage.event_count, 1);
        assert_eq!(usage.used_bytes, 40);
    }

    #[tokio::test]
    async fn cleanup_reclaims_recent_blob_after_its_last_event_is_pruned() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let blob_id = "b".repeat(64);
        repository
            .record_blob("group_123", &blob_id, 128, true)
            .await
            .unwrap();
        let mut event = test_event(1);
        event.blob_ids.push(blob_id.clone());
        repository
            .insert_events("group_123", &[event], None)
            .await
            .unwrap();

        let result = repository
            .cleanup_group(
                "group_123",
                CleanupPolicy {
                    retention_cutoff_ms: None,
                    legacy_blob_cutoff_ms: None,
                    max_events: Some(0),
                    target_bytes: None,
                    orphan_cutoff_ms: now_ms() - 86_400_000,
                    batch_size: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.deleted_events, 1);
        assert_eq!(result.deleted_blob_ids, vec![blob_id]);
        let usage = repository.group_usage("group_123").await.unwrap();
        assert_eq!(usage.event_count, 0);
        assert_eq!(usage.blob_count, 0);
    }

    #[tokio::test]
    async fn event_with_missing_blob_reference_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();
        let mut event = test_event(1);
        event.blob_ids.push("c".repeat(64));

        assert!(
            repository
                .insert_events("group_123", &[event], None)
                .await
                .is_err()
        );
        assert_eq!(repository.event_count("group_123").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn event_quota_rejection_rolls_back_the_batch() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7_u8; 32])
            .await
            .unwrap();

        let error = repository
            .insert_events("group_123", &[test_event(1)], Some(8))
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<QuotaExceeded>().is_some());
        assert_eq!(
            repository
                .group_usage("group_123")
                .await
                .unwrap()
                .event_count,
            0
        );
    }

    fn test_event(sequence: u64) -> EncryptedEvent {
        EncryptedEvent {
            event_id: format!("event_{sequence:03}"),
            origin_device_id: "device_123".into(),
            origin_sequence: sequence,
            created_at_ms: sequence as i64,
            nonce: vec![sequence as u8; 24],
            ciphertext: vec![sequence as u8; 16],
            kind: SyncEventKind::Content,
            blob_ids: Vec::new(),
        }
    }
}
