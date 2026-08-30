use anyhow::{Context, Result as AnyResult};
use sqlx::migrate::Migrator;
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
const MIGRATION_LINE_ENDING_CHECKSUMS: &[(i64, &str, &str)] = &[
    (
        1,
        "BFA656AA68EED66F8DAB5BACB9EFA4BAFEB11713FD417239E05F7B4C5757330FAE5EAFEEB138771C6D18785670C44B05",
        "FF50E7101EE04305EAA30B28054F33EE98204CAE357373FF8C9F7B3850017F7F036EC3E113A0222C3C4DA7367FD48820",
    ),
    (
        2,
        "003C5F2D9F84F34D1EECD9F30E83193023A27D2D7CFC36DF5D56DC6E24961BA6DC04C838FC9C6B5BB65AFB8A698A67DD",
        "19976B4817A306175B668686FADA351F42EB9F54C2BFF6EB14D21FD1FC10912C405AB41A64EBF4D853C426AC33489CA3",
    ),
    (
        3,
        "7CDBE7365CAF9F454F23D609486FF7590E91526386DD38E834058109A7DBEE473F754B6933D57C6F6F4F46079180F816",
        "C234128C28C13A0FCBDEC3E02EC72F6AEC69559D6D4E961F36E73A82425FCC721605126FF16DD0826840528D9C107BC8",
    ),
    (
        4,
        "47EFAE309B22A6471875BB684783CDC0DC6E184488FF70215FDE1FB8BCD8A02C95C420E9EA7889CC9BC46444727C4E47",
        "A47E804ADA57351FE2BD8E809752CD649C37D7F7E0EB17D8A7C22A9479B4C8D8C18F6F16C14D3361559E7204604638C1",
    ),
    (
        5,
        "429DB5768D8C5F46979E3D311593F253FAE2C95F25B5C99B5F85689349B94C6B95C0DD6DDC2544C17288B8561212E1C9",
        "A1097555C72F05C411C94313690F383423FB8226A9C705C67283F2AA1A53BA659AAE888E360E16470EAEA488C192908D",
    ),
    (
        6,
        "EE1D091BFA7DF1DE81149BD494588BA4FE608DF8224960CF479D97F59D2629F0D0B04F8BA3F36EEFE11C9ED489852688",
        "66B4269DA5DF217805A3A0CAC198718A094E7C48D6CC90C114D0585189CEF4DA073D0D8C46610B0A6DB850C6AA31898D",
    ),
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

    let migrator = sqlx::migrate!("./migrations");
    repair_legacy_sync_removal_migration(&pool).await?;
    repair_migration_line_endings(&pool, &migrator).await?;
    migrator
        .run(&pool)
        .await
        .context("failed to run sqlite migrations")?;

    log::info!("sqlite pool ready at {path:?}");
    Ok(pool)
}

/// Accepts only the known LF/CRLF checksum pairs for otherwise identical migrations.
async fn repair_migration_line_endings(pool: &SqlitePool, migrator: &Migrator) -> AnyResult<()> {
    let migration_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations')",
    )
    .fetch_one(pool)
    .await
    .context("check migration metadata table for line ending repair")?;
    if migration_table_exists == 0 {
        return Ok(());
    }

    let mut repairs = Vec::new();
    for &(version, lf_checksum, crlf_checksum) in MIGRATION_LINE_ENDING_CHECKSUMS {
        let Some(migration) = migrator
            .iter()
            .find(|migration| migration.version == version)
        else {
            continue;
        };
        let current_checksum = checksum_hex(migration.checksum.as_ref());
        if current_checksum != lf_checksum && current_checksum != crlf_checksum {
            continue;
        }

        let applied_checksum: Option<String> = sqlx::query_scalar(
            "SELECT hex(checksum) FROM _sqlx_migrations WHERE version = ? AND success = 1",
        )
        .bind(version)
        .fetch_optional(pool)
        .await
        .with_context(|| format!("read migration {version} checksum for line ending repair"))?;
        let alternate_checksum = if current_checksum == lf_checksum {
            crlf_checksum
        } else {
            lf_checksum
        };
        if applied_checksum.as_deref() == Some(alternate_checksum) {
            repairs.push((version, migration.checksum.to_vec()));
        }
    }
    if repairs.is_empty() {
        return Ok(());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("begin migration line ending repair")?;
    for (version, checksum) in &repairs {
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = ? AND success = 1")
            .bind(checksum)
            .bind(version)
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("repair migration {version} line ending checksum"))?;
    }
    transaction
        .commit()
        .await
        .context("commit migration line ending repair")?;
    log::info!(
        "normalized {} migration checksum(s) for platform line endings",
        repairs.len()
    );
    Ok(())
}

fn checksum_hex(checksum: &[u8]) -> String {
    checksum.iter().map(|byte| format!("{byte:02X}")).collect()
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

    fn decode_checksum(checksum: &str) -> Vec<u8> {
        checksum
            .as_bytes()
            .chunks_exact(2)
            .map(|digits| u8::from_str_radix(std::str::from_utf8(digits).unwrap(), 16).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn accepts_known_platform_line_endings_only() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str("sqlite::memory:").unwrap())
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run(&pool).await.unwrap();

        for &(version, lf_checksum, crlf_checksum) in MIGRATION_LINE_ENDING_CHECKSUMS {
            let migration = migrator
                .iter()
                .find(|migration| migration.version == version)
                .unwrap();
            let current_checksum = checksum_hex(migration.checksum.as_ref());
            assert!(current_checksum == lf_checksum || current_checksum == crlf_checksum);
            let alternate_checksum = if current_checksum == lf_checksum {
                crlf_checksum
            } else {
                lf_checksum
            };
            sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = ?")
                .bind(decode_checksum(alternate_checksum))
                .bind(version)
                .execute(&pool)
                .await
                .unwrap();
        }

        repair_migration_line_endings(&pool, &migrator)
            .await
            .unwrap();
        migrator.run(&pool).await.unwrap();
    }

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
