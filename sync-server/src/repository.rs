use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use ecopaste_sync_protocol::{CloudEvent, DeviceAnnouncement, EncryptedEvent, PeerAnnouncement};
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions};
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
            .foreign_keys(true);
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
}
