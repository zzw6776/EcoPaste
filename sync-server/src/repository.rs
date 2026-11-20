use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use ecopaste_sync_protocol::{
    AcceptedItem, CloudDevice, DeviceAnnouncement, MAX_ITEMS_PER_BATCH, PeerAnnouncement,
    RemovedDevice, SyncItemRecord, SyncItemWrite,
};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
};
use subtle::ConstantTimeEq;

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
    pub item_count: u64,
    pub blob_count: u64,
    pub oldest_item_received_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct CleanupPolicy {
    pub retention_cutoff_ms: Option<i64>,
    pub max_items: Option<u64>,
    pub target_bytes: Option<u64>,
    pub orphan_cutoff_ms: i64,
    pub batch_size: u32,
}

#[derive(Debug, Default)]
pub struct CleanupResult {
    pub deleted_items: u64,
    pub deleted_blob_ids: Vec<String>,
    pub has_more: bool,
}

#[derive(Debug, Default)]
pub struct UpsertItemsResult {
    pub accepted: Vec<AcceptedItem>,
    pub conflicts: Vec<SyncItemRecord>,
    pub pre_write_latest_seq: u64,
    pub latest_seq: u64,
}

#[derive(Debug, Default)]
pub struct ItemsWindow {
    pub items: Vec<SyncItemRecord>,
    pub latest_seq: u64,
    pub pruned_through_seq: u64,
    pub had_more: bool,
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

    /// Upserts encrypted item snapshots with per-item optimistic CAS on `hub_rev`.
    ///
    /// Writes whose `expected_hub_rev` lags behind the stored revision are not rejected with an
    /// error; their current Hub snapshot is returned in `conflicts` so the caller can CRDT-merge.
    pub async fn upsert_items(
        &self,
        group_id: &str,
        writes: &[SyncItemWrite],
        max_available_bytes: Option<u64>,
    ) -> Result<UpsertItemsResult> {
        let mut transaction = self.pool.begin().await.context("begin item upsert")?;
        let (pre_write_latest_seq_raw, used_bytes_raw): (i64, i64) = sqlx::query_as(
            "SELECT latest_seq, event_bytes + blob_bytes FROM sync_groups WHERE group_id = ?",
        )
        .bind(group_id)
        .fetch_one(&mut *transaction)
        .await
        .context("read sync group sequence before upserting items")?;
        let pre_write_latest_seq =
            u64::try_from(pre_write_latest_seq_raw).context("negative group latest_seq")?;
        let mut next_seq = pre_write_latest_seq;
        let mut used_bytes = u64::try_from(used_bytes_raw).context("negative sync group usage")?;
        let mut byte_delta = 0_i64;
        let mut count_delta = 0_i64;
        let mut accepted = Vec::with_capacity(writes.len());
        let mut conflict_keys = Vec::new();
        let mut seen_conflict_keys = HashSet::new();

        for write in writes {
            validate_blob_id(&write.item_key)?;
            validate_identifier("origin_device_id", &write.origin_device_id)?;
            if write.nonce.len() != 24 || write.ciphertext.is_empty() {
                bail!("invalid encrypted item snapshot");
            }
            let mut content_blobs = if write.deleted {
                Vec::new()
            } else {
                write.blob_ids.clone()
            };
            content_blobs.sort_unstable();
            content_blobs.dedup();
            let mut optional_blobs = if write.deleted {
                Vec::new()
            } else {
                write.optional_blob_ids.clone()
            };
            optional_blobs.sort_unstable();
            optional_blobs.dedup();
            optional_blobs.retain(|blob_id| !content_blobs.contains(blob_id));
            if content_blobs.len().saturating_add(optional_blobs.len()) > 256 {
                bail!("too many item blob references");
            }
            for blob_id in content_blobs.iter().chain(optional_blobs.iter()) {
                validate_blob_id(blob_id)?;
            }

            let existing: Option<(i64, i64)> = sqlx::query_as(
                "SELECT hub_rev, length(nonce) + length(ciphertext) \
                 FROM items WHERE group_id = ? AND item_key = ?",
            )
            .bind(group_id)
            .bind(&write.item_key)
            .fetch_optional(&mut *transaction)
            .await
            .context("read existing hub item revision")?;

            let new_bytes = i64::try_from(write.nonce.len() + write.ciphertext.len())
                .context("encrypted item snapshot is too large")?;
            let now = now_ms();

            let new_hub_rev = match existing {
                Some((existing_hub_rev_raw, existing_bytes)) => {
                    let existing_hub_rev =
                        u64::try_from(existing_hub_rev_raw).context("negative hub_rev")?;
                    if existing_hub_rev != write.expected_hub_rev {
                        if seen_conflict_keys.insert(write.item_key.clone()) {
                            conflict_keys.push(write.item_key.clone());
                        }
                        continue;
                    }
                    let delta = new_bytes - existing_bytes;
                    if delta > 0
                        && max_available_bytes.is_some_and(|maximum| {
                            used_bytes.saturating_add(delta as u64) > maximum
                        })
                    {
                        return Err(QuotaExceeded.into());
                    }
                    next_seq = next_seq.checked_add(1).context("group seq overflow")?;
                    let new_hub_rev = existing_hub_rev
                        .checked_add(1)
                        .context("item hub_rev overflow")?;
                    sqlx::query(
                        r#"
                        UPDATE items
                        SET seq = ?, hub_rev = ?, origin_device_id = ?, updated_at_ms = ?,
                            received_at_ms = ?, deleted = ?, protected = ?, nonce = ?, ciphertext = ?
                        WHERE group_id = ? AND item_key = ?
                        "#,
                    )
                    .bind(i64::try_from(next_seq).context("group seq exceeds i64")?)
                    .bind(i64::try_from(new_hub_rev).context("item hub_rev exceeds i64")?)
                    .bind(&write.origin_device_id)
                    .bind(write.updated_at_ms)
                    .bind(now)
                    .bind(i64::from(write.deleted))
                    .bind(i64::from(write.protected && !write.deleted))
                    .bind(&write.nonce)
                    .bind(&write.ciphertext)
                    .bind(group_id)
                    .bind(&write.item_key)
                    .execute(&mut *transaction)
                    .await
                    .context("update encrypted hub item")?;

                    used_bytes = apply_signed_delta(used_bytes, delta);
                    byte_delta = byte_delta
                        .checked_add(delta)
                        .context("item byte delta overflow")?;
                    new_hub_rev
                }
                None => {
                    if max_available_bytes.is_some_and(|maximum| {
                        used_bytes.saturating_add(new_bytes as u64) > maximum
                    }) {
                        return Err(QuotaExceeded.into());
                    }
                    next_seq = next_seq.checked_add(1).context("group seq overflow")?;
                    // If the row was previously evicted by Hub cleanup, allow the client to
                    // recreate it cleanly with a strictly monotonic revision.
                    let new_hub_rev = write.expected_hub_rev.saturating_add(1).max(1);
                    sqlx::query(
                        r#"
                        INSERT INTO items (
                            group_id, item_key, seq, hub_rev, origin_device_id,
                            updated_at_ms, received_at_ms, deleted, protected, nonce, ciphertext
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        "#,
                    )
                    .bind(group_id)
                    .bind(&write.item_key)
                    .bind(i64::try_from(next_seq).context("group seq exceeds i64")?)
                    .bind(i64::try_from(new_hub_rev).context("item hub_rev exceeds i64")?)
                    .bind(&write.origin_device_id)
                    .bind(write.updated_at_ms)
                    .bind(now)
                    .bind(i64::from(write.deleted))
                    .bind(i64::from(write.protected && !write.deleted))
                    .bind(&write.nonce)
                    .bind(&write.ciphertext)
                    .execute(&mut *transaction)
                    .await
                    .context("insert encrypted hub item")?;

                    used_bytes = used_bytes.saturating_add(new_bytes as u64);
                    byte_delta = byte_delta
                        .checked_add(new_bytes)
                        .context("item byte delta overflow")?;
                    count_delta = count_delta
                        .checked_add(1)
                        .context("item count delta overflow")?;
                    new_hub_rev
                }
            };

            replace_item_blob_refs(
                &mut transaction,
                group_id,
                &write.item_key,
                &content_blobs,
                &optional_blobs,
                now,
            )
            .await?;

            let mut referenced_blobs = content_blobs;
            referenced_blobs.extend(optional_blobs);
            let available_blob_ids =
                find_and_touch_available_blobs(&mut transaction, group_id, &referenced_blobs, now)
                    .await?;

            accepted.push(AcceptedItem {
                item_key: write.item_key.clone(),
                hub_rev: new_hub_rev,
                seq: next_seq,
                available_blob_ids,
            });
        }

        if !accepted.is_empty() {
            sqlx::query(
                r#"
                UPDATE sync_groups
                SET latest_seq = ?,
                    event_bytes = MAX(0, event_bytes + ?),
                    event_count = MAX(0, event_count + ?)
                WHERE group_id = ?
                "#,
            )
            .bind(i64::try_from(next_seq).context("group seq exceeds i64")?)
            .bind(byte_delta)
            .bind(count_delta)
            .bind(group_id)
            .execute(&mut *transaction)
            .await
            .context("update sync group item stats")?;
        }

        let conflicts = if conflict_keys.is_empty() {
            Vec::new()
        } else {
            load_items_by_keys(&mut transaction, group_id, &conflict_keys).await?
        };

        transaction.commit().await.context("commit item upsert")?;
        Ok(UpsertItemsResult {
            accepted,
            conflicts,
            pre_write_latest_seq,
            latest_seq: next_seq,
        })
    }

    /// Reads one newest-first window of item snapshots above `after_seq` and below `before_seq`.
    ///
    /// When `max_seq_inclusive` is set (e.g., to the pre-write `latest_seq`), snapshots just
    /// written by the caller in the same RPC are omitted without creating gaps in `latest_seq`.
    pub async fn list_items_window(
        &self,
        group_id: &str,
        after_seq: u64,
        before_seq: Option<u64>,
        max_seq_inclusive: Option<u64>,
        limit: u16,
    ) -> Result<ItemsWindow> {
        let limit = limit.clamp(1, MAX_ITEMS_PER_BATCH);
        let mut transaction = self.pool.begin().await.context("begin item window read")?;
        let (latest_seq_raw, pruned_through_seq_raw): (i64, i64) = sqlx::query_as(
            "SELECT latest_seq, pruned_through_seq FROM sync_groups WHERE group_id = ?",
        )
        .bind(group_id)
        .fetch_one(&mut *transaction)
        .await
        .context("read sync group watermarks")?;
        let latest_seq = u64::try_from(latest_seq_raw).context("negative group latest_seq")?;
        let pruned_through_seq =
            u64::try_from(pruned_through_seq_raw).context("negative group pruned_through_seq")?;

        let upper_bound = match (before_seq, max_seq_inclusive) {
            (Some(before), Some(max_inc)) => Some(before.min(max_inc.saturating_add(1))),
            (Some(before), None) => Some(before),
            (None, Some(max_inc)) => Some(max_inc.saturating_add(1)),
            (None, None) => None,
        };

        let fetch_limit = i64::from(limit) + 1;
        let rows = sqlx::query(
            r#"
            SELECT seq, hub_rev, item_key, origin_device_id, updated_at_ms,
                   deleted, protected, nonce, ciphertext
            FROM items
            WHERE group_id = ?
              AND seq > ?
              AND (? IS NULL OR seq < ?)
            ORDER BY seq DESC
            LIMIT ?
            "#,
        )
        .bind(group_id)
        .bind(i64::try_from(after_seq).unwrap_or(i64::MAX))
        .bind(upper_bound.map(|value| i64::try_from(value).unwrap_or(i64::MAX)))
        .bind(upper_bound.map(|value| i64::try_from(value).unwrap_or(i64::MAX)))
        .bind(fetch_limit)
        .fetch_all(&mut *transaction)
        .await
        .context("list encrypted item window")?;

        let had_more = rows.len() > usize::from(limit);
        let mut items = rows
            .into_iter()
            .take(usize::from(limit))
            .map(sync_item_record_from_row)
            .collect::<Result<Vec<_>>>()?;
        attach_item_blobs(&mut transaction, group_id, &mut items).await?;
        transaction
            .commit()
            .await
            .context("commit item window read")?;

        Ok(ItemsWindow {
            items,
            latest_seq,
            pruned_through_seq,
            had_more,
        })
    }

    /// Reads one newest-first page of non-deleted items for the cloud history drawer.
    pub async fn list_active_items_page(
        &self,
        group_id: &str,
        before_seq: Option<u64>,
        limit: u16,
    ) -> Result<(Vec<SyncItemRecord>, Option<u64>, u64)> {
        let limit = limit.clamp(1, 100);
        let fetch_limit = i64::from(limit) + 1;
        let mut transaction = self.pool.begin().await.context("begin active items page")?;
        let rows = sqlx::query(
            r#"
            SELECT seq, hub_rev, item_key, origin_device_id, updated_at_ms,
                   deleted, protected, nonce, ciphertext
            FROM items
            WHERE group_id = ?
              AND deleted = 0
              AND (? IS NULL OR seq < ?)
            ORDER BY seq DESC
            LIMIT ?
            "#,
        )
        .bind(group_id)
        .bind(before_seq.map(|value| i64::try_from(value).unwrap_or(i64::MAX)))
        .bind(before_seq.map(|value| i64::try_from(value).unwrap_or(i64::MAX)))
        .bind(fetch_limit)
        .fetch_all(&mut *transaction)
        .await
        .context("list active cloud items")?;

        let total_raw: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE group_id = ? AND deleted = 0")
                .bind(group_id)
                .fetch_one(&mut *transaction)
                .await
                .context("count active cloud items")?;
        let total = u64::try_from(total_raw).context("negative active item count")?;

        let has_more = rows.len() > usize::from(limit);
        let mut items = rows
            .into_iter()
            .take(usize::from(limit))
            .map(sync_item_record_from_row)
            .collect::<Result<Vec<_>>>()?;
        let next_before_seq = has_more.then(|| {
            items
                .last()
                .expect("non-empty page when another item exists")
                .seq
        });
        attach_item_blobs(&mut transaction, group_id, &mut items).await?;
        transaction
            .commit()
            .await
            .context("commit active items page")?;
        Ok((items, next_before_seq, total))
    }

    /// Reads the durable group watermarks `(latest_seq, latest_removed_at_ms)` from one snapshot.
    pub async fn group_watermarks(&self, group_id: &str) -> Result<(u64, i64)> {
        let (latest_seq, removed): (i64, i64) = sqlx::query_as(
            "SELECT latest_seq, \
                    COALESCE((SELECT MAX(MAX(removed_at_ms, COALESCE(restored_at_ms, 0))) \
                    FROM removed_devices WHERE group_id = ?), 0) \
             FROM sync_groups WHERE group_id = ?",
        )
        .bind(group_id)
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read sync group watermarks")?;
        Ok((
            u64::try_from(latest_seq).context("negative group latest_seq")?,
            removed,
        ))
    }

    /// Records a stored blob and bumps `seq` (without changing `hub_rev`) on every referencing item
    /// so watchers pull the newly available blob identifier.
    pub async fn record_blob(&self, group_id: &str, blob_id: &str, size: u64) -> Result<bool> {
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
            INSERT INTO blobs (group_id, blob_id, size, created_at_ms, gc_protected)
            VALUES (?, ?, ?, ?, 0)
            ON CONFLICT(group_id, blob_id) DO UPDATE SET
                size = excluded.size,
                created_at_ms = excluded.created_at_ms,
                gc_protected = 0
            "#,
        )
        .bind(group_id)
        .bind(blob_id)
        .bind(size)
        .bind(now_ms())
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
            SET blob_bytes = MAX(0, blob_bytes + ?),
                blob_count = MAX(0, blob_count + ?)
            WHERE group_id = ?
            "#,
        )
        .bind(byte_delta)
        .bind(count_delta)
        .bind(group_id)
        .execute(&mut *transaction)
        .await
        .context("update sync group blob usage")?;

        let item_keys = sqlx::query_scalar::<_, String>(
            "SELECT item_key FROM item_blob_refs \
             WHERE group_id = ? AND blob_id = ? ORDER BY item_key",
        )
        .bind(group_id)
        .bind(blob_id)
        .fetch_all(&mut *transaction)
        .await
        .context("list items affected by blob availability")?;
        let items_bumped = !item_keys.is_empty();
        if items_bumped {
            let bump_count = i64::try_from(item_keys.len())
                .context("too many items affected by blob availability")?;
            let last_seq: i64 = sqlx::query_scalar(
                "UPDATE sync_groups SET latest_seq = latest_seq + ? \
                 WHERE group_id = ? RETURNING latest_seq",
            )
            .bind(bump_count)
            .bind(group_id)
            .fetch_one(&mut *transaction)
            .await
            .context("advance group latest_seq for blob availability")?;
            let first_seq = last_seq
                .checked_sub(bump_count - 1)
                .context("group seq overflow")?;
            for (offset, item_key) in item_keys.into_iter().enumerate() {
                let seq = first_seq
                    .checked_add(i64::try_from(offset).context("item offset overflow")?)
                    .context("group seq overflow")?;
                sqlx::query("UPDATE items SET seq = ? WHERE group_id = ? AND item_key = ?")
                    .bind(seq)
                    .bind(group_id)
                    .bind(item_key)
                    .execute(&mut *transaction)
                    .await
                    .context("publish item blob availability")?;
            }
        }
        transaction.commit().await.context("commit recorded blob")?;
        Ok(items_bumped)
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
            .context("refresh Hub blob orphan grace period")?;
        Ok(())
    }

    /// Returns exact logical payload usage without forcing SQLite file compaction.
    pub async fn group_usage(&self, group_id: &str) -> Result<HubUsage> {
        let row = sqlx::query(
            r#"
            SELECT
                event_bytes + blob_bytes AS used_bytes,
                (SELECT COUNT(*) FROM items WHERE group_id = ? AND deleted = 0) AS active_count,
                blob_count,
                (SELECT MIN(received_at_ms) FROM items WHERE group_id = ? AND deleted = 0)
                    AS oldest_item_received_at_ms
            FROM sync_groups
            WHERE group_id = ?
            "#,
        )
        .bind(group_id)
        .bind(group_id)
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read sync group usage")?;
        let used_bytes: i64 = row.try_get("used_bytes")?;
        let item_count: i64 = row.try_get("active_count")?;
        let blob_count: i64 = row.try_get("blob_count")?;
        Ok(HubUsage {
            used_bytes: u64::try_from(used_bytes).context("negative sync group byte usage")?,
            item_count: u64::try_from(item_count).context("negative sync group item count")?,
            blob_count: u64::try_from(blob_count).context("negative sync group blob count")?,
            oldest_item_received_at_ms: row.try_get("oldest_item_received_at_ms")?,
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

    /// Returns the blob files that still have durable metadata for one group.
    pub async fn blob_ids(&self, group_id: &str) -> Result<HashSet<String>> {
        let ids = sqlx::query_scalar("SELECT blob_id FROM blobs WHERE group_id = ?")
            .bind(group_id)
            .fetch_all(&self.pool)
            .await
            .context("list sync group blobs")?;
        Ok(ids.into_iter().collect())
    }

    /// Evicts oldest unprotected items and reclaims unreferenced blobs in one bounded batch.
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
              AND created_at_ms < ?
              AND NOT EXISTS (
                  SELECT 1 FROM item_blob_refs r
                  WHERE r.group_id = b.group_id AND r.blob_id = b.blob_id
              )
            ORDER BY created_at_ms ASC
            LIMIT ?
            "#,
        )
        .bind(group_id)
        .bind(policy.orphan_cutoff_ms)
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

        let used_bytes_raw: i64 = sqlx::query_scalar(
            "SELECT event_bytes + blob_bytes FROM sync_groups WHERE group_id = ?",
        )
        .bind(group_id)
        .fetch_one(&mut *transaction)
        .await
        .context("read Hub cleanup usage")?;
        let used_bytes = u64::try_from(used_bytes_raw).context("negative Hub cleanup usage")?;

        struct CleanupCandidate {
            seq: i64,
            item_key: String,
            received_at_ms: i64,
            item_bytes: u64,
        }

        let rows = sqlx::query(
            r#"
            SELECT seq, item_key, received_at_ms, length(nonce) + length(ciphertext) AS item_bytes
            FROM items
            WHERE group_id = ? AND protected = 0
            ORDER BY
                CASE WHEN ? IS NOT NULL AND received_at_ms < ? THEN 0 ELSE 1 END ASC,
                updated_at_ms ASC,
                seq ASC
            LIMIT ?
            "#,
        )
        .bind(group_id)
        .bind(policy.retention_cutoff_ms)
        .bind(policy.retention_cutoff_ms)
        .bind(i64::from(batch_size))
        .fetch_all(&mut *transaction)
        .await
        .context("list Hub cleanup candidates")?;
        if rows.is_empty() {
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
        let candidates = rows
            .into_iter()
            .map(|row| {
                Ok(CleanupCandidate {
                    seq: row.try_get("seq")?,
                    item_key: row.try_get("item_key")?,
                    received_at_ms: row.try_get("received_at_ms")?,
                    item_bytes: u64::try_from(row.try_get::<i64, _>("item_bytes")?)
                        .unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let count_excess = if let Some(maximum) = policy.max_items {
            let eligible: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM items WHERE group_id = ? AND protected = 0",
            )
            .bind(group_id)
            .fetch_one(&mut *transaction)
            .await
            .context("count eligible cleanup items")?;
            u64::try_from(eligible)
                .context("negative Hub cleanup item count")?
                .saturating_sub(maximum)
        } else {
            0
        };
        let time_excess = policy.retention_cutoff_ms.map_or(0, |cutoff| {
            candidates
                .iter()
                .take_while(|candidate| candidate.received_at_ms < cutoff)
                .count() as u64
        });
        let mut delete_count = count_excess.max(time_excess).min(candidates.len() as u64) as usize;
        let quota_excess = policy
            .target_bytes
            .map_or(0, |maximum| used_bytes.saturating_sub(maximum));

        let mut candidate_rank = HashMap::with_capacity(candidates.len());
        for (index, candidate) in candidates.iter().enumerate() {
            candidate_rank.insert(candidate.item_key.clone(), index);
        }
        let mut ref_query = sqlx::QueryBuilder::new(
            "SELECT r.blob_id, b.size, r.item_key \
             FROM item_blob_refs r \
             JOIN blobs b ON b.group_id = r.group_id AND b.blob_id = r.blob_id \
             WHERE r.group_id = ",
        );
        ref_query
            .push_bind(group_id)
            .push(
                " AND r.blob_id IN (SELECT DISTINCT blob_id FROM item_blob_refs WHERE group_id = ",
            )
            .push_bind(group_id)
            .push(" AND item_key IN (");
        let mut separated = ref_query.separated(", ");
        for candidate in &candidates {
            separated.push_bind(&candidate.item_key);
        }
        ref_query.push("))");
        let blob_ref_rows = ref_query
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("list blobs referenced by Hub cleanup candidates")?;

        let mut blob_refs: HashMap<String, (u64, bool, usize)> = HashMap::new();
        for row in blob_ref_rows {
            let blob_id: String = row.try_get("blob_id")?;
            let size =
                u64::try_from(row.try_get::<i64, _>("size")?).context("negative Hub blob size")?;
            let item_key: String = row.try_get("item_key")?;
            let entry = blob_refs.entry(blob_id).or_insert((size, true, 0));
            if let Some(&rank) = candidate_rank.get(&item_key) {
                entry.2 = entry.2.max(rank);
            } else {
                entry.1 = false;
            }
        }
        let mut released_by_rank: HashMap<usize, (u64, Vec<(String, u64)>)> = HashMap::new();
        for (blob_id, (size, all_in_candidates, release_rank)) in blob_refs {
            if !all_in_candidates {
                continue;
            }
            let entry = released_by_rank.entry(release_rank).or_default();
            entry.0 = entry.0.saturating_add(size);
            entry.1.push((blob_id, size));
        }

        if quota_excess > 0 {
            let mut released = 0_u64;
            let mut quota_delete_count = candidates.len();
            for (index, candidate) in candidates.iter().enumerate() {
                released = released.saturating_add(candidate.item_bytes);
                if let Some((bytes, _)) = released_by_rank.get(&index) {
                    released = released.saturating_add(*bytes);
                }
                if released >= quota_excess {
                    quota_delete_count = index + 1;
                    break;
                }
            }
            delete_count = delete_count.max(quota_delete_count);
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

        let selected = &candidates[..delete_count];
        let deleted_through = selected
            .iter()
            .map(|candidate| candidate.seq)
            .max()
            .unwrap_or(0);
        let deleted_item_bytes = selected
            .iter()
            .map(|candidate| candidate.item_bytes)
            .sum::<u64>();
        let mut released_blob_candidates = HashMap::new();
        for (rank, (_, blobs)) in released_by_rank {
            if rank < delete_count {
                for (blob_id, size) in blobs {
                    released_blob_candidates.insert(blob_id.clone(), size);
                    deleted_blob_ids.push(blob_id);
                }
            }
        }

        let mut delete_items = sqlx::QueryBuilder::new("DELETE FROM items WHERE group_id = ");
        delete_items.push_bind(group_id).push(" AND item_key IN (");
        let mut separated = delete_items.separated(", ");
        for candidate in selected {
            separated.push_bind(&candidate.item_key);
        }
        delete_items.push(")");
        delete_items
            .build()
            .execute(&mut *transaction)
            .await
            .context("delete evicted Hub items")?;

        deleted_blob_ids.sort_unstable();
        deleted_blob_ids.dedup();
        let mut released_blob_bytes = 0_u64;
        let mut released_blob_count = 0_u64;
        for blob_id in &deleted_blob_ids {
            let deleted = sqlx::query(
                r#"
                DELETE FROM blobs
                WHERE group_id = ? AND blob_id = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM item_blob_refs r
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
                    released_blob_candidates
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
            SET pruned_through_seq = MAX(pruned_through_seq, ?),
                event_bytes = MAX(0, event_bytes - ?),
                event_count = MAX(0, event_count - ?),
                blob_bytes = MAX(0, blob_bytes - ?),
                blob_count = MAX(0, blob_count - ?)
            WHERE group_id = ?
            "#,
        )
        .bind(deleted_through)
        .bind(i64::try_from(deleted_item_bytes).context("deleted item usage is too large")?)
        .bind(i64::try_from(delete_count).context("too many deleted Hub items")?)
        .bind(i64::try_from(released_blob_bytes).context("released blob usage is too large")?)
        .bind(i64::try_from(released_blob_count).context("too many released Hub blobs")?)
        .bind(group_id)
        .execute(&mut *transaction)
        .await
        .context("update retained Hub usage")?;

        transaction.commit().await.context("commit Hub cleanup")?;
        Ok(CleanupResult {
            deleted_items: delete_count as u64,
            deleted_blob_ids,
            has_more: stale_blob_batch_full
                || (candidates.len() == batch_size as usize && delete_count == candidates.len()),
        })
    }
}

async fn replace_item_blob_refs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group_id: &str,
    item_key: &str,
    content_blobs: &[String],
    optional_blobs: &[String],
    now: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM item_blob_refs WHERE group_id = ? AND item_key = ?")
        .bind(group_id)
        .bind(item_key)
        .execute(&mut **transaction)
        .await
        .context("clear item blob references")?;
    for blob_id in content_blobs {
        sqlx::query(
            "INSERT INTO item_blob_refs (group_id, item_key, blob_id, optional, created_at_ms) \
             VALUES (?, ?, ?, 0, ?)",
        )
        .bind(group_id)
        .bind(item_key)
        .bind(blob_id)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .context("insert item content blob reference")?;
    }
    for blob_id in optional_blobs {
        sqlx::query(
            "INSERT INTO item_blob_refs (group_id, item_key, blob_id, optional, created_at_ms) \
             VALUES (?, ?, ?, 1, ?)",
        )
        .bind(group_id)
        .bind(item_key)
        .bind(blob_id)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .context("insert item optional blob reference")?;
    }
    Ok(())
}

async fn load_items_by_keys(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group_id: &str,
    item_keys: &[String],
) -> Result<Vec<SyncItemRecord>> {
    if item_keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT seq, hub_rev, item_key, origin_device_id, updated_at_ms, \
                deleted, protected, nonce, ciphertext \
         FROM items WHERE group_id = ",
    );
    query.push_bind(group_id).push(" AND item_key IN (");
    let mut separated = query.separated(", ");
    for item_key in item_keys {
        separated.push_bind(item_key);
    }
    query.push(") ORDER BY seq DESC");
    let rows = query
        .build()
        .fetch_all(&mut **transaction)
        .await
        .context("load conflicting hub items")?;
    let mut items = rows
        .into_iter()
        .map(sync_item_record_from_row)
        .collect::<Result<Vec<_>>>()?;
    attach_item_blobs(transaction, group_id, &mut items).await?;
    Ok(items)
}

/// Finds referenced blobs already stored for the group and refreshes their orphan grace timestamp.
async fn find_and_touch_available_blobs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group_id: &str,
    blob_ids: &[String],
    now: i64,
) -> Result<Vec<String>> {
    if blob_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::new("SELECT blob_id FROM blobs WHERE group_id = ");
    query.push_bind(group_id).push(" AND blob_id IN (");
    let mut separated = query.separated(", ");
    for blob_id in blob_ids {
        separated.push_bind(blob_id);
    }
    query.push(") ORDER BY blob_id");
    let available: Vec<String> = query
        .build_query_scalar()
        .fetch_all(&mut **transaction)
        .await
        .context("lookup available blobs for accepted sync item")?;
    if !available.is_empty() {
        let mut touch = sqlx::QueryBuilder::new("UPDATE blobs SET created_at_ms = ");
        touch
            .push_bind(now)
            .push(" WHERE group_id = ")
            .push_bind(group_id)
            .push(" AND blob_id IN (");
        let mut separated = touch.separated(", ");
        for blob_id in &available {
            separated.push_bind(blob_id);
        }
        touch.push(")");
        touch
            .build()
            .execute(&mut **transaction)
            .await
            .context("refresh Hub blob orphan grace period for accepted item")?;
    }
    Ok(available)
}

async fn attach_item_blobs(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    group_id: &str,
    items: &mut [SyncItemRecord],
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT refs.item_key, refs.blob_id, refs.optional, \
                blobs.blob_id IS NOT NULL AS available \
         FROM item_blob_refs refs LEFT JOIN blobs \
         ON blobs.group_id = refs.group_id AND blobs.blob_id = refs.blob_id \
         WHERE refs.group_id = ",
    );
    query.push_bind(group_id).push(" AND refs.item_key IN (");
    let mut separated = query.separated(", ");
    for item in items.iter() {
        separated.push_bind(&item.item_key);
    }
    query.push(") ORDER BY refs.item_key, refs.blob_id");
    let rows = query
        .build()
        .fetch_all(&mut **transaction)
        .await
        .context("list item blob references")?;

    let mut content_by_item: HashMap<String, Vec<String>> = HashMap::new();
    let mut optional_by_item: HashMap<String, Vec<String>> = HashMap::new();
    let mut available_by_item: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let item_key: String = row.try_get("item_key")?;
        let blob_id: String = row.try_get("blob_id")?;
        let optional: i64 = row.try_get("optional")?;
        if row.try_get::<bool, _>("available")? {
            available_by_item
                .entry(item_key.clone())
                .or_default()
                .push(blob_id.clone());
        }
        if optional != 0 {
            optional_by_item.entry(item_key).or_default().push(blob_id);
        } else {
            content_by_item.entry(item_key).or_default().push(blob_id);
        }
    }
    for item in items {
        item.blob_ids = content_by_item.remove(&item.item_key).unwrap_or_default();
        item.optional_blob_ids = optional_by_item.remove(&item.item_key).unwrap_or_default();
        item.available_blob_ids = available_by_item.remove(&item.item_key).unwrap_or_default();
    }
    Ok(())
}

fn sync_item_record_from_row(row: sqlx::sqlite::SqliteRow) -> Result<SyncItemRecord> {
    let seq: i64 = row.try_get("seq")?;
    let hub_rev: i64 = row.try_get("hub_rev")?;
    Ok(SyncItemRecord {
        seq: u64::try_from(seq).context("negative item seq")?,
        hub_rev: u64::try_from(hub_rev).context("negative item hub_rev")?,
        item_key: row.try_get("item_key")?,
        origin_device_id: row.try_get("origin_device_id")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
        deleted: row.try_get::<i64, _>("deleted")? != 0,
        protected: row.try_get::<i64, _>("protected")? != 0,
        nonce: row.try_get("nonce")?,
        ciphertext: row.try_get("ciphertext")?,
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
        available_blob_ids: Vec::new(),
    })
}

fn apply_signed_delta(base: u64, delta: i64) -> u64 {
    if delta >= 0 {
        base.saturating_add(delta as u64)
    } else {
        base.saturating_sub(delta.unsigned_abs())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_write(
        item_key_char: char,
        expected_hub_rev: u64,
        updated_at_ms: i64,
    ) -> SyncItemWrite {
        SyncItemWrite {
            item_key: item_key_char.to_string().repeat(64),
            expected_hub_rev,
            local_seq: 1,
            origin_device_id: "device_123456".into(),
            updated_at_ms,
            deleted: false,
            protected: false,
            nonce: vec![1; 24],
            ciphertext: vec![2; 64],
            blob_ids: Vec::new(),
            optional_blob_ids: Vec::new(),
        }
    }

    #[tokio::test]
    async fn upsert_cas_accepts_matching_rev_and_returns_conflicts_without_failing_batch() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7; 32])
            .await
            .unwrap();

        let first = repository
            .upsert_items("group_123", &[sample_write('a', 0, 100)], None)
            .await
            .unwrap();
        assert_eq!(first.accepted.len(), 1);
        assert_eq!(first.accepted[0].hub_rev, 1);
        assert_eq!(first.accepted[0].seq, 1);
        assert!(first.conflicts.is_empty());

        let stale = sample_write('a', 0, 120);
        let fresh = sample_write('b', 0, 130);
        let second = repository
            .upsert_items("group_123", &[stale, fresh], None)
            .await
            .unwrap();
        assert_eq!(second.accepted.len(), 1);
        assert_eq!(second.accepted[0].item_key, "b".repeat(64));
        assert_eq!(second.accepted[0].seq, 2);
        assert_eq!(second.conflicts.len(), 1);
        assert_eq!(second.conflicts[0].item_key, "a".repeat(64));
        assert_eq!(second.conflicts[0].hub_rev, 1);

        let updated = sample_write('a', 1, 140);
        let third = repository
            .upsert_items("group_123", &[updated], None)
            .await
            .unwrap();
        assert_eq!(third.accepted.len(), 1);
        assert_eq!(third.accepted[0].hub_rev, 2);
        assert_eq!(third.accepted[0].seq, 3);
        assert!(third.conflicts.is_empty());
    }

    #[tokio::test]
    async fn record_blob_bumps_item_seq_without_changing_hub_rev() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7; 32])
            .await
            .unwrap();

        let blob_id = "1".repeat(64);
        let icon_id = "2".repeat(64);
        let mut write = sample_write('a', 0, 100);
        write.blob_ids = vec![blob_id.clone()];
        write.optional_blob_ids = vec![icon_id.clone()];
        let inserted = repository
            .upsert_items("group_123", &[write], None)
            .await
            .unwrap();
        assert_eq!(inserted.accepted[0].seq, 1);
        assert_eq!(inserted.accepted[0].hub_rev, 1);

        let before = repository
            .list_items_window("group_123", 0, None, None, 10)
            .await
            .unwrap();
        assert_eq!(before.items.len(), 1);
        assert!(before.items[0].available_blob_ids.is_empty());

        assert!(
            repository
                .record_blob("group_123", &blob_id, 2048)
                .await
                .unwrap()
        );
        let after = repository
            .list_items_window("group_123", 1, None, None, 10)
            .await
            .unwrap();
        assert_eq!(after.latest_seq, 2);
        assert_eq!(after.items.len(), 1);
        assert_eq!(after.items[0].seq, 2);
        assert_eq!(after.items[0].hub_rev, 1);
        assert_eq!(after.items[0].blob_ids, vec![blob_id.clone()]);
        assert_eq!(after.items[0].optional_blob_ids, vec![icon_id]);
        assert_eq!(after.items[0].available_blob_ids, vec![blob_id]);
    }

    #[tokio::test]
    async fn tombstone_releases_blob_refs_and_protected_items_survive_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7; 32])
            .await
            .unwrap();

        let blob_a = "1".repeat(64);
        let blob_b = "2".repeat(64);
        let mut item_a = sample_write('a', 0, 100);
        item_a.blob_ids = vec![blob_a.clone()];
        let mut item_b = sample_write('b', 0, 200);
        item_b.protected = true;
        item_b.blob_ids = vec![blob_b.clone()];
        repository
            .upsert_items("group_123", &[item_a, item_b], None)
            .await
            .unwrap();
        repository
            .record_blob("group_123", &blob_a, 4096)
            .await
            .unwrap();
        repository
            .record_blob("group_123", &blob_b, 4096)
            .await
            .unwrap();

        let mut tombstone = sample_write('a', 1, 300);
        tombstone.deleted = true;
        tombstone.ciphertext = vec![9; 16];
        repository
            .upsert_items("group_123", &[tombstone], None)
            .await
            .unwrap();

        let (active, next_before, total) = repository
            .list_active_items_page("group_123", None, 10)
            .await
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(next_before, None);
        assert_eq!(active[0].item_key, "b".repeat(64));

        let cleaned = repository
            .cleanup_group(
                "group_123",
                CleanupPolicy {
                    retention_cutoff_ms: None,
                    max_items: Some(0),
                    target_bytes: Some(0),
                    orphan_cutoff_ms: now_ms() + 10_000,
                    batch_size: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(cleaned.deleted_items, 1);
        assert_eq!(cleaned.deleted_blob_ids, vec![blob_a]);
        let usage = repository.group_usage("group_123").await.unwrap();
        assert_eq!(usage.item_count, 1);
        assert_eq!(usage.blob_count, 1);
    }

    #[tokio::test]
    async fn window_pagination_returns_newest_first_without_skipping_older_rows() {
        let directory = tempfile::tempdir().unwrap();
        let repository = Repository::open(&directory.path().join("hub.sqlite3"))
            .await
            .unwrap();
        repository
            .create_group("group_123", &[7; 32])
            .await
            .unwrap();

        let writes = ['a', 'b', 'c', 'd']
            .into_iter()
            .enumerate()
            .map(|(index, ch)| sample_write(ch, 0, (index as i64 + 1) * 100))
            .collect::<Vec<_>>();
        repository
            .upsert_items("group_123", &writes, None)
            .await
            .unwrap();

        let page1 = repository
            .list_items_window("group_123", 1, None, None, 2)
            .await
            .unwrap();
        assert_eq!(page1.latest_seq, 4);
        assert!(page1.had_more);
        assert_eq!(
            page1.items.iter().map(|item| item.seq).collect::<Vec<_>>(),
            vec![4, 3]
        );

        let page2 = repository
            .list_items_window("group_123", 1, Some(3), None, 2)
            .await
            .unwrap();
        assert!(!page2.had_more);
        assert_eq!(
            page2.items.iter().map(|item| item.seq).collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[tokio::test]
    async fn upgrade_migration_preserves_groups_and_devices_while_resetting_legacy_events() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("legacy-hub.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        let mut migrator = sqlx::migrate!("./migrations");
        migrator.migrations = std::borrow::Cow::Owned(
            migrator
                .migrations
                .iter()
                .filter(|migration| migration.version < 12)
                .cloned()
                .collect(),
        );
        migrator.run(&pool).await.unwrap();

        let token = [9_u8; 32];
        let token_hash = blake3::hash(&token);
        sqlx::query(
            "INSERT INTO sync_groups (group_id, access_token_hash, created_at_ms) VALUES (?, ?, 1)",
        )
        .bind("group_legacy")
        .bind(token_hash.as_bytes().as_slice())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO devices (group_id, device_id, device_name, platform, endpoint_id, \
             direct_addresses, relay_urls, app_version, last_seen_ms) \
             VALUES ('group_legacy', 'device_legacy', 'Mac', 'macos', 'endpoint_legacy', X'', X'', '1.1.230', 100)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let upgraded = Repository::open(&db_path).await.unwrap();
        upgraded.authenticate("group_legacy", &token).await.unwrap();
        let devices = upgraded.list_devices("group_legacy").await.unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].device_id, "device_legacy");
        assert_eq!(
            upgraded.group_watermarks("group_legacy").await.unwrap(),
            (0, 0)
        );
    }
}
