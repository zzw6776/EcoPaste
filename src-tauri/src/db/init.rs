use anyhow::{Context, Result as AnyResult};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::ConnectOptions;
use sqlx::SqlitePool;
use tauri::AppHandle;

use crate::core::Result;
use crate::db::db_path;

const LEGACY_SYNC_REMOVAL_CHECKSUM: [u8; 48] = [
    0x47, 0x00, 0xd9, 0xb3, 0xe1, 0x6c, 0xdf, 0xf1, 0xa0, 0x9c, 0x04, 0xe5, 0xe5, 0x1a, 0x7c, 0xf7,
    0x66, 0x66, 0x0e, 0x97, 0x28, 0x2e, 0xde, 0x34, 0x6f, 0x35, 0x95, 0x62, 0x4d, 0xe3, 0x1e, 0x54,
    0xb3, 0x63, 0x66, 0x83, 0x31, 0x3f, 0x47, 0xe1, 0x5b, 0x6f, 0xec, 0x50, 0xa8, 0x1e, 0x72, 0x2a,
];
const CURRENT_SYNC_REMOVAL_CHECKSUM: [u8; 48] = [
    0x47, 0xef, 0xae, 0x30, 0x9b, 0x22, 0xa6, 0x47, 0x18, 0x75, 0xbb, 0x68, 0x47, 0x83, 0xcd, 0xc0,
    0xdc, 0x6e, 0x18, 0x44, 0x88, 0xff, 0x70, 0x21, 0x5f, 0xde, 0x1f, 0xb8, 0xbc, 0xd8, 0xa0, 0x2c,
    0x95, 0xc4, 0x20, 0xe9, 0xea, 0x78, 0x89, 0xcc, 0x9b, 0xc4, 0x64, 0x44, 0x72, 0x7c, 0x4e, 0x47,
];

pub async fn init(app: &AppHandle) -> Result<SqlitePool> {
    let path = db_path(app)?;

    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .disable_statement_logging();

    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .with_context(|| format!("failed to open sqlite database at {path:?}"))?;

    repair_legacy_sync_removal_migration(&pool).await?;
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("failed to run sqlite migrations")?;

    log::info!("sqlite pool ready at {path:?}");
    Ok(pool)
}

/// Repairs the one development-only draft of migration 4 that was already applied locally.
/// All other checksum mismatches are still rejected by SQLx.
async fn repair_legacy_sync_removal_migration(pool: &SqlitePool) -> AnyResult<()> {
    let migration_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations')",
    )
    .fetch_one(pool)
    .await
    .context("check migration metadata table")?;
    if migration_table_exists == 0 {
        return Ok(());
    }

    let checksum: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT checksum FROM _sqlx_migrations WHERE version = 4 AND success = 1",
    )
    .fetch_optional(pool)
    .await
    .context("read migration 4 checksum")?;
    if checksum.as_deref() != Some(LEGACY_SYNC_REMOVAL_CHECKSUM.as_slice()) {
        return Ok(());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("begin legacy sync removal migration repair")?;
    sqlx::query("ALTER TABLE sync_removed_peers RENAME TO sync_removed_peers_legacy")
        .execute(&mut *transaction)
        .await
        .context("rename legacy sync removal table")?;
    sqlx::query(
        r#"
        CREATE TABLE sync_removed_peers (
            device_id     TEXT PRIMARY KEY NOT NULL,
            endpoint_id   TEXT NOT NULL,
            removed_at_ms INTEGER NOT NULL,
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL
        )
        "#,
    )
    .execute(&mut *transaction)
    .await
    .context("create current sync removal table")?;
    sqlx::query(
        r#"
        INSERT INTO sync_removed_peers (
            device_id, endpoint_id, removed_at_ms, created_at, updated_at
        )
        SELECT
            device_id,
            'legacy:' || device_id,
            COALESCE(CAST(unixepoch(created_at) AS INTEGER) * 1000, 0),
            created_at,
            updated_at
        FROM sync_removed_peers_legacy
        "#,
    )
    .execute(&mut *transaction)
    .await
    .context("copy legacy sync removal records")?;
    sqlx::query("DROP TABLE sync_removed_peers_legacy")
        .execute(&mut *transaction)
        .await
        .context("drop legacy sync removal table")?;
    sqlx::query(
        "CREATE UNIQUE INDEX idx_sync_removed_peers_endpoint ON sync_removed_peers(endpoint_id)",
    )
    .execute(&mut *transaction)
    .await
    .context("create sync removal endpoint index")?;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = 4")
        .bind(CURRENT_SYNC_REMOVAL_CHECKSUM.as_slice())
        .execute(&mut *transaction)
        .await
        .context("finalize migration 4 checksum repair")?;
    transaction
        .commit()
        .await
        .context("commit legacy sync removal migration repair")?;
    log::info!("upgraded development draft of sync removal migration 4");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::*;

    #[tokio::test]
    async fn upgrades_only_the_known_migration_4_draft() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str("sqlite::memory:")
                    .unwrap()
                    .foreign_keys(true),
            )
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        sqlx::query("DROP INDEX idx_sync_removed_peers_endpoint")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE sync_removed_peers")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE sync_removed_peers (
                device_id  TEXT PRIMARY KEY NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sync_removed_peers VALUES ('device-old', '2026-08-27T00:00:00Z', '2026-08-27T00:00:00Z')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = 4")
            .bind(LEGACY_SYNC_REMOVAL_CHECKSUM.as_slice())
            .execute(&pool)
            .await
            .unwrap();

        repair_legacy_sync_removal_migration(&pool).await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let endpoint_id: String = sqlx::query_scalar(
            "SELECT endpoint_id FROM sync_removed_peers WHERE device_id = 'device-old'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(endpoint_id, "legacy:device-old");
        let checksum: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 4")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(checksum, CURRENT_SYNC_REMOVAL_CHECKSUM);
    }
}
