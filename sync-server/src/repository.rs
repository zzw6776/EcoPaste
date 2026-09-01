use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use ecopaste_sync_protocol::{
    CloudEvent, DeviceAnnouncement, EncryptedEvent, PeerAnnouncement, RemovedDevice,
};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
};
use subtle::ConstantTimeEq;

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

    /// Updates the peer route cache advertised to the rest of the group.
    pub async fn upsert_device(&self, group_id: &str, device: &DeviceAnnouncement) -> Result<()> {
        validate_identifier("device_id", &device.device_id)?;
        validate_identifier("endpoint_id", &device.endpoint_id)?;
        if self
            .is_removed_device(group_id, &device.device_id, &device.endpoint_id)
            .await?
        {
            bail!("device was removed from this sync group");
        }
        if device.device_name.chars().count() > 80 || device.platform.len() > 24 {
            bail!("invalid device announcement");
        }
        if device.direct_addresses.len() > 16 || device.relay_urls.len() > 8 {
            bail!("too many device addresses");
        }
        let direct_addresses = encode_lines(&device.direct_addresses)?;
        let relay_urls = encode_lines(&device.relay_urls)?;
        sqlx::query(
            r#"
            INSERT INTO devices (
                group_id, device_id, device_name, platform, endpoint_id,
                direct_addresses, relay_urls, last_seen_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, device_id) DO UPDATE SET
                device_name = excluded.device_name,
                platform = excluded.platform,
                endpoint_id = excluded.endpoint_id,
                direct_addresses = excluded.direct_addresses,
                relay_urls = excluded.relay_urls,
                last_seen_ms = excluded.last_seen_ms
            "#,
        )
        .bind(group_id)
        .bind(&device.device_id)
        .bind(&device.device_name)
        .bind(&device.platform)
        .bind(&device.endpoint_id)
        .bind(direct_addresses)
        .bind(relay_urls)
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .context("upsert sync device")?;
        Ok(())
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

    pub async fn latest_removed_at_ms(&self, group_id: &str) -> Result<i64> {
        let value: Option<i64> = sqlx::query_scalar(
            r#"
                SELECT MAX(MAX(removed_at_ms, COALESCE(restored_at_ms, 0)))
                FROM removed_devices WHERE group_id = ?
                "#,
        )
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .context("read latest removed device timestamp")?;
        Ok(value.unwrap_or(0))
    }

    /// Inserts encrypted events and returns the IDs accepted by this hub.
    pub async fn insert_events(
        &self,
        group_id: &str,
        events: &[EncryptedEvent],
    ) -> Result<Vec<String>> {
        let mut transaction = self.pool.begin().await.context("begin event transaction")?;
        let mut accepted = Vec::with_capacity(events.len());
        for event in events {
            validate_identifier("event_id", &event.event_id)?;
            validate_identifier("origin_device_id", &event.origin_device_id)?;
            if event.nonce.len() != 24 || event.ciphertext.is_empty() {
                bail!("invalid encrypted event");
            }
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO events (
                    group_id, event_id, origin_device_id, origin_sequence,
                    created_at_ms, nonce, ciphertext
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(group_id)
            .bind(&event.event_id)
            .bind(&event.origin_device_id)
            .bind(i64::try_from(event.origin_sequence).context("origin sequence is too large")?)
            .bind(event.created_at_ms)
            .bind(&event.nonce)
            .bind(&event.ciphertext)
            .execute(&mut *transaction)
            .await
            .context("insert encrypted event")?;
            if result.rows_affected() == 1 {
                accepted.push(event.event_id.clone());
            }
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
                   created_at_ms, nonce, ciphertext
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

        rows.into_iter()
            .map(|row| {
                let cursor: i64 = row.try_get("cursor")?;
                let sequence: i64 = row.try_get("origin_sequence")?;
                Ok(CloudEvent {
                    cursor: u64::try_from(cursor).context("negative event cursor")?,
                    event: EncryptedEvent {
                        event_id: row.try_get("event_id")?,
                        origin_device_id: row.try_get("origin_device_id")?,
                        origin_sequence: u64::try_from(sequence)
                            .context("negative origin sequence")?,
                        created_at_ms: row.try_get("created_at_ms")?,
                        nonce: row.try_get("nonce")?,
                        ciphertext: row.try_get("ciphertext")?,
                    },
                })
            })
            .collect()
    }

    pub async fn latest_cursor(&self, group_id: &str) -> Result<u64> {
        let cursor: Option<i64> =
            sqlx::query_scalar("SELECT MAX(cursor) FROM events WHERE group_id = ?")
                .bind(group_id)
                .fetch_one(&self.pool)
                .await
                .context("read latest encrypted event cursor")?;
        cursor
            .map(|value| u64::try_from(value).context("negative event cursor"))
            .transpose()
            .map(|value| value.unwrap_or(0))
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
                       created_at_ms, nonce, ciphertext
                FROM events
                WHERE group_id = ? AND cursor < ?
                ORDER BY cursor DESC
                LIMIT ?
                "#,
            )
            .bind(group_id)
            .bind(i64::try_from(before_cursor).unwrap_or(i64::MAX))
            .bind(fetch_limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT cursor, event_id, origin_device_id, origin_sequence,
                       created_at_ms, nonce, ciphertext
                FROM events
                WHERE group_id = ?
                ORDER BY cursor DESC
                LIMIT ?
                "#,
            )
            .bind(group_id)
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
        Ok((events, next_before_cursor))
    }

    pub async fn event_count(&self, group_id: &str) -> Result<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE group_id = ?")
            .bind(group_id)
            .fetch_one(&self.pool)
            .await
            .context("count encrypted events")?;
        u64::try_from(count).context("negative event count")
    }

    /// Returns recently seen peers. Stale entries remain useful as an offline LAN route cache.
    pub async fn list_peers(
        &self,
        group_id: &str,
        excluding_device_id: &str,
    ) -> Result<Vec<PeerAnnouncement>> {
        let rows = sqlx::query(
            r#"
            SELECT device_id, device_name, platform, endpoint_id,
                   direct_addresses, relay_urls, last_seen_ms
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
                let direct: Vec<u8> = row.try_get("direct_addresses")?;
                let relays: Vec<u8> = row.try_get("relay_urls")?;
                Ok(PeerAnnouncement {
                    device_id: row.try_get("device_id")?,
                    device_name: row.try_get("device_name")?,
                    platform: row.try_get("platform")?,
                    endpoint_id: row.try_get("endpoint_id")?,
                    direct_addresses: decode_lines(&direct)?,
                    relay_urls: decode_lines(&relays)?,
                    last_seen_ms: row.try_get("last_seen_ms")?,
                })
            })
            .collect()
    }

    pub async fn record_blob(&self, group_id: &str, blob_id: &str, size: u64) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO blobs (group_id, blob_id, size, created_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(group_id)
        .bind(blob_id)
        .bind(i64::try_from(size).context("blob is too large")?)
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .context("record encrypted blob")?;
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

fn validate_access_token(value: &[u8]) -> Result<()> {
    if !(32..=128).contains(&value.len()) {
        bail!("invalid access token");
    }
    Ok(())
}

fn encode_lines(values: &[String]) -> Result<Vec<u8>> {
    if values.iter().any(|value| value.contains(['\n', '\r'])) {
        bail!("invalid address");
    }
    Ok(values.join("\n").into_bytes())
}

fn decode_lines(value: &[u8]) -> Result<Vec<String>> {
    let value = std::str::from_utf8(value).context("invalid address encoding")?;
    if value.is_empty() {
        return Ok(Vec::new());
    }
    Ok(value.lines().map(str::to_owned).collect())
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
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            })
            .collect::<Vec<_>>();
        repository
            .insert_events("group_123", &events)
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
}
