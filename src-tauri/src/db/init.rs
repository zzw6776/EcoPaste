use std::time::Duration;

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
const LEGACY_REMOTE_FILE_CONTENT_CHECKSUM: &str =
    "DD908558755B6704B5A3E98AC0F48A2B2B2EB78082D4E05C7B8B96B39E82A882D27B517DD551A5C02FAB3EAA45C3E081";
const CURRENT_REMOTE_FILE_CONTENT_CHECKSUM: &str =
    "27AF35544968CA90B8CB0EF932828CEE190E37918E4A0DB862F65AE47F7C087443BACE95FA5F2E51BBCEB725F1EA01E2";
const LEGACY_CONTENT_AVAILABILITY_CHECKSUM: &str =
    "5997EDC9F723F05C6D110CD1FA5D9D6F122A32ACD08DA3FB0609268E462AAA5745DC6B8EB15AA79A06B4496B23058264";
const CURRENT_CONTENT_AVAILABILITY_CHECKSUM: &str =
    "52EDB0AA5436E20531BBB1BEDCF88BEF032A1B3BFC91DE879C207964BD13C2BF8BEAEBBEAA8057A4B5A852F893968224";
const LEGACY_CONTENT_OBJECTS_CHECKSUM: &str =
    "B88CBBF51A4D0D062E15E607E3B0B15D00B631668450016928CF414E8DE01BC4B49926449A9E4EE111D8744A8B07DE39";
const CURRENT_CONTENT_OBJECTS_CHECKSUM: &str =
    "8D1CFA204FC191222F229A2C59880F9D3AD5E9122AAAE9857CF16AF3FFF24FCFB72901170B237A35C3C65487092DE6D6";
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
    (
        7,
        "2D3D5B5D8981AE2DCA0F2E4EEFA0FD1C39B5AF685732071846B158114DC79F840496C7C90405619C7E2E133A18444AF1",
        "DCD2AB5781F83B9F14011784289374ED87F9A64A603EFF2B163F50C7BB303C6F0BD1834AE84E1AF06F31D7B5051474B0",
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
        .busy_timeout(Duration::from_secs(5))
        .disable_statement_logging();

    let pool = SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .with_context(|| format!("failed to open sqlite database at {path:?}"))?;

    let migrator = sqlx::migrate!("./migrations");
    repair_legacy_sync_removal_migration(&pool).await?;
    repair_remote_file_content_draft(&pool, &migrator).await?;
    repair_content_availability_draft(&pool, &migrator).await?;
    repair_content_objects_draft(&pool, &migrator).await?;
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

/// Normalizes the one distributed draft of migration 29 whose SQL differed only in timestamp text.
/// Unknown checksums still reach SQLx unchanged and remain fatal.
async fn repair_remote_file_content_draft(pool: &SqlitePool, migrator: &Migrator) -> AnyResult<()> {
    let migration_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations')",
    )
    .fetch_one(pool)
    .await
    .context("check migration metadata table for remote file draft repair")?;
    if migration_table_exists == 0 {
        return Ok(());
    }

    let migration = migrator
        .iter()
        .find(|migration| migration.version == 29)
        .context("migration 29 is missing")?;
    if checksum_hex(migration.checksum.as_ref()) != CURRENT_REMOTE_FILE_CONTENT_CHECKSUM {
        return Ok(());
    }
    let applied_checksum: Option<String> = sqlx::query_scalar(
        "SELECT hex(checksum) FROM _sqlx_migrations WHERE version = 29 AND success = 1",
    )
    .fetch_optional(pool)
    .await
    .context("read migration 29 checksum")?;
    if applied_checksum.as_deref() != Some(LEGACY_REMOTE_FILE_CONTENT_CHECKSUM) {
        return Ok(());
    }

    let content_state_column_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('sync_deliveries') WHERE name = 'content_delivered')",
    )
    .fetch_one(pool)
    .await
    .context("verify migration 29 delivery state schema")?;
    if content_state_column_exists == 0 {
        anyhow::bail!("migration 29 draft schema is incomplete");
    }

    let result = sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = ? WHERE version = 29 AND success = 1 AND checksum = ?",
    )
    .bind(migration.checksum.to_vec())
    .bind(decode_hex_checksum(LEGACY_REMOTE_FILE_CONTENT_CHECKSUM)?)
    .execute(pool)
    .await
    .context("normalize migration 29 draft checksum")?;
    if result.rows_affected() != 1 {
        anyhow::bail!("migration 29 draft checksum changed during normalization");
    }
    log::info!("normalized distributed draft of remote file content migration 29");
    Ok(())
}

/// Completes the one known migration 30 draft before adopting its current checksum.
/// A partial or unknown schema is left untouched so SQLx still rejects it.
async fn repair_content_availability_draft(
    pool: &SqlitePool,
    migrator: &Migrator,
) -> AnyResult<()> {
    let migration_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations')",
    )
    .fetch_one(pool)
    .await
    .context("check migration metadata table for content availability draft repair")?;
    if migration_table_exists == 0 {
        return Ok(());
    }

    let migration = migrator
        .iter()
        .find(|migration| migration.version == 30)
        .context("migration 30 is missing")?;
    if checksum_hex(migration.checksum.as_ref()) != CURRENT_CONTENT_AVAILABILITY_CHECKSUM {
        return Ok(());
    }
    let applied_checksum: Option<String> = sqlx::query_scalar(
        "SELECT hex(checksum) FROM _sqlx_migrations WHERE version = 30 AND success = 1",
    )
    .fetch_optional(pool)
    .await
    .context("read migration 30 checksum")?;
    if applied_checksum.as_deref() != Some(LEGACY_CONTENT_AVAILABILITY_CHECKSUM) {
        return Ok(());
    }

    let schema_parts = content_availability_schema_parts(pool).await?;
    if schema_parts != 0 {
        anyhow::bail!("migration 30 draft schema is partially applied");
    }

    let mut transaction = pool
        .begin()
        .await
        .context("begin migration 30 draft completion")?;
    sqlx::raw_sql(migration.sql.clone())
        .execute(&mut *transaction)
        .await
        .context("complete migration 30 draft schema")?;
    let completed_parts = content_availability_schema_parts(&mut *transaction).await?;
    if completed_parts != 4 {
        anyhow::bail!("migration 30 completed schema is incomplete");
    }
    let result = sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = ? WHERE version = 30 AND success = 1 AND checksum = ?",
    )
    .bind(migration.checksum.to_vec())
    .bind(decode_hex_checksum(LEGACY_CONTENT_AVAILABILITY_CHECKSUM)?)
    .execute(&mut *transaction)
    .await
    .context("normalize migration 30 draft checksum")?;
    if result.rows_affected() != 1 {
        anyhow::bail!("migration 30 draft checksum changed during completion");
    }
    transaction
        .commit()
        .await
        .context("commit migration 30 draft completion")?;
    log::info!("completed distributed draft of content availability migration 30");
    Ok(())
}

async fn content_availability_schema_parts<'e, E>(executor: E) -> AnyResult<i64>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query_scalar(
        r#"
        SELECT
            EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_event_ingress') +
            EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_local_replicas') +
            EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_target_replicas') +
            EXISTS(SELECT 1 FROM pragma_table_info('sync_peer_receipts') WHERE name = 'content_state')
        "#,
    )
    .fetch_one(executor)
    .await
    .context("inspect content availability schema")
}

/// Adopts the current checksum for the one known migration 31 draft after proving that its
/// materialized table already has the exact current structure.
async fn repair_content_objects_draft(pool: &SqlitePool, migrator: &Migrator) -> AnyResult<()> {
    let migration_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations')",
    )
    .fetch_one(pool)
    .await
    .context("check migration metadata table for content objects draft repair")?;
    if migration_table_exists == 0 {
        return Ok(());
    }

    let migration = migrator
        .iter()
        .find(|migration| migration.version == 31)
        .context("migration 31 is missing")?;
    if checksum_hex(migration.checksum.as_ref()) != CURRENT_CONTENT_OBJECTS_CHECKSUM {
        return Ok(());
    }
    let applied_checksum: Option<String> = sqlx::query_scalar(
        "SELECT hex(checksum) FROM _sqlx_migrations WHERE version = 31 AND success = 1",
    )
    .fetch_optional(pool)
    .await
    .context("read migration 31 checksum")?;
    if applied_checksum.as_deref() != Some(LEGACY_CONTENT_OBJECTS_CHECKSUM) {
        return Ok(());
    }
    if !content_objects_schema_is_current(pool).await? {
        anyhow::bail!("migration 31 draft schema does not match the current structure");
    }

    let result = sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = ? WHERE version = 31 AND success = 1 AND checksum = ?",
    )
    .bind(migration.checksum.to_vec())
    .bind(decode_hex_checksum(LEGACY_CONTENT_OBJECTS_CHECKSUM)?)
    .execute(pool)
    .await
    .context("normalize migration 31 draft checksum")?;
    if result.rows_affected() != 1 {
        anyhow::bail!("migration 31 draft checksum changed during normalization");
    }
    log::info!("normalized distributed draft of content objects migration 31");
    Ok(())
}

async fn content_objects_schema_is_current(pool: &SqlitePool) -> AnyResult<bool> {
    let columns_match: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) = 8 AND SUM(CASE
            WHEN name = 'group_id' AND type = 'TEXT' AND "notnull" = 1 AND pk = 1 THEN 1
            WHEN name = 'plaintext_hash' AND type = 'TEXT' AND "notnull" = 1 AND pk = 2 THEN 1
            WHEN name = 'blob_id' AND type = 'TEXT' AND "notnull" = 1 AND pk = 0 THEN 1
            WHEN name = 'stream_nonce' AND type = 'BLOB' AND "notnull" = 1 AND pk = 0 THEN 1
            WHEN name IN ('original_size', 'encrypted_size')
                AND type = 'INTEGER' AND "notnull" = 1 AND pk = 0 THEN 1
            WHEN name IN ('created_at', 'updated_at')
                AND type = 'TEXT' AND "notnull" = 1 AND pk = 0 THEN 1
            ELSE 0
        END) = 8
        FROM pragma_table_info('sync_content_objects')
        "#,
    )
    .fetch_one(pool)
    .await
    .context("inspect content objects columns")?;
    if columns_match == 0 {
        return Ok(false);
    }

    let unique_blob_index_exists: i64 = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM pragma_index_list('sync_content_objects') indexes
            WHERE indexes."unique" = 1
              AND (SELECT COUNT(*) FROM pragma_index_info(indexes.name)) = 2
              AND (SELECT name FROM pragma_index_info(indexes.name) WHERE seqno = 0) = 'group_id'
              AND (SELECT name FROM pragma_index_info(indexes.name) WHERE seqno = 1) = 'blob_id'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .context("inspect content objects unique index")?;
    Ok(unique_blob_index_exists != 0)
}

fn decode_hex_checksum(checksum: &str) -> AnyResult<Vec<u8>> {
    let (pairs, remainder) = checksum.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        anyhow::bail!("invalid odd-length migration checksum");
    }
    pairs
        .iter()
        .map(|digits| {
            let digits = std::str::from_utf8(digits).context("invalid migration checksum text")?;
            u8::from_str_radix(digits, 16).context("invalid migration checksum hex")
        })
        .collect()
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
    async fn search_index_upgrade_preserves_content_and_skips_unchanged_text() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str("sqlite::memory:")
                    .unwrap()
                    .foreign_keys(true),
            )
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run_to(8, &pool).await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO clipboard_items (
                id, kind, content, content_hash, search_text, note,
                platform, created_at, updated_at
            ) VALUES (
                'retained', 'text', 'alphabody', 'hash', 'alphabody', 'alphamemo',
                'macos', '2026-01-01', '2026-01-01'
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        migrator.run(&pool).await.unwrap();
        migrator.run(&pool).await.unwrap();
        assert_fts_matches(&pool, "alphabody", &["retained"]).await;
        assert_fts_matches(&pool, "alphamemo", &["retained"]).await;

        sqlx::query(
            r#"
            INSERT INTO clipboard_groups (id, name, icon, created_at, updated_at)
            VALUES ('group', 'group', 'folder', '2026-01-01', '2026-01-01')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let before = total_changes(&pool).await;
        sqlx::query(
            r#"
            UPDATE clipboard_items SET
                is_favorite = 1, is_pinned = 1, group_id = 'group',
                use_count = use_count + 1, updated_at = '2026-01-02'
            WHERE id = 'retained'
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(total_changes(&pool).await - before, 1);

        let mut previous = (Some("alphabody"), Some("alphamemo"));
        for (search_text, note) in [
            previous,
            (Some("betabody"), Some("alphamemo")),
            (Some("betabody"), Some("betamemo")),
            (None, Some("betamemo")),
            (None, None),
            (None, None),
            (Some(""), Some("")),
            (Some(""), Some("")),
            (Some("alphabody"), None),
            (Some("alphabody"), Some("alphamemo")),
        ] {
            let before = total_changes(&pool).await;
            sqlx::query(
                "UPDATE clipboard_items SET search_text = ?, note = ? WHERE id = 'retained'",
            )
            .bind(search_text)
            .bind(note)
            .execute(&pool)
            .await
            .unwrap();
            if previous == (search_text, note) {
                assert_eq!(total_changes(&pool).await - before, 1);
            }

            for token in ["alphabody", "betabody", "alphamemo", "betamemo"] {
                let expected: &[&str] = if search_text == Some(token) || note == Some(token) {
                    &["retained"]
                } else {
                    &[]
                };
                assert_fts_matches(&pool, token, expected).await;
            }
            sqlx::query(
                "INSERT INTO clipboard_items_fts(clipboard_items_fts, rank) VALUES ('integrity-check', 1)",
            )
            .execute(&pool)
            .await
            .unwrap();
            previous = (search_text, note);
        }

        sqlx::query(
            r#"
            INSERT INTO clipboard_items (
                id, kind, content, content_hash, search_text, platform, created_at, updated_at
            ) VALUES (
                'inserted', 'text', 'alphabody', 'new-hash', 'alphabody',
                'macos', '2026-01-02', '2026-01-02'
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_fts_matches(&pool, "alphabody", &["inserted", "retained"]).await;

        sqlx::query("DELETE FROM clipboard_items WHERE id = 'retained'")
            .execute(&pool)
            .await
            .unwrap();
        assert_fts_matches(&pool, "alphabody", &["inserted"]).await;
        assert_fts_matches(&pool, "alphamemo", &[]).await;
        sqlx::query(
            "INSERT INTO clipboard_items_fts(clipboard_items_fts, rank) VALUES ('integrity-check', 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
    }

    /// 单连接测试库的累计写入数包含 FTS 影子表，可区分仅更新主表和重建索引。
    async fn total_changes(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar("SELECT total_changes()")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// 通过 MATCH 读取真实倒排索引，避免外部内容表的普通 SELECT 掩盖失效索引。
    async fn assert_fts_matches(pool: &SqlitePool, token: &str, expected: &[&str]) {
        let ids: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT id FROM clipboard_items
            WHERE rowid IN (
                SELECT rowid FROM clipboard_items_fts WHERE clipboard_items_fts MATCH ?
            )
            ORDER BY id
            "#,
        )
        .bind(token)
        .fetch_all(pool)
        .await
        .unwrap();
        assert_eq!(ids, expected, "search token: {token}");
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
                .bind(decode_hex_checksum(alternate_checksum).unwrap())
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
    async fn normalizes_only_the_known_migration_29_draft() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str("sqlite::memory:").unwrap())
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run_to(34, &pool).await.unwrap();
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = 29")
            .bind(decode_hex_checksum(LEGACY_REMOTE_FILE_CONTENT_CHECKSUM).unwrap())
            .execute(&pool)
            .await
            .unwrap();

        repair_remote_file_content_draft(&pool, &migrator)
            .await
            .unwrap();
        migrator.run(&pool).await.unwrap();

        let checksum: String =
            sqlx::query_scalar("SELECT hex(checksum) FROM _sqlx_migrations WHERE version = 29")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(checksum, CURRENT_REMOTE_FILE_CONTENT_CHECKSUM);
    }

    #[tokio::test]
    async fn completes_only_the_known_migration_30_draft() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str("sqlite::memory:").unwrap())
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run_to(29, &pool).await.unwrap();
        sqlx::query(
            r#"
            INSERT INTO _sqlx_migrations (
                version, description, success, checksum, execution_time
            ) VALUES (30, 'content availability draft', 1, ?, 0)
            "#,
        )
        .bind(decode_hex_checksum(LEGACY_CONTENT_AVAILABILITY_CHECKSUM).unwrap())
        .execute(&pool)
        .await
        .unwrap();

        repair_content_availability_draft(&pool, &migrator)
            .await
            .unwrap();
        migrator.run(&pool).await.unwrap();

        let checksum: String =
            sqlx::query_scalar("SELECT hex(checksum) FROM _sqlx_migrations WHERE version = 30")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(checksum, CURRENT_CONTENT_AVAILABILITY_CHECKSUM);
        let consolidated_tables: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_delivery_targets') + EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_local_content')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(consolidated_tables, 2);
    }

    #[tokio::test]
    async fn normalizes_only_the_known_migration_31_draft() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str("sqlite::memory:").unwrap())
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        migrator.run(&pool).await.unwrap();
        sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = 31")
            .bind(decode_hex_checksum(LEGACY_CONTENT_OBJECTS_CHECKSUM).unwrap())
            .execute(&pool)
            .await
            .unwrap();

        repair_content_objects_draft(&pool, &migrator)
            .await
            .unwrap();
        migrator.run(&pool).await.unwrap();

        let checksum: String =
            sqlx::query_scalar("SELECT hex(checksum) FROM _sqlx_migrations WHERE version = 31")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(checksum, CURRENT_CONTENT_OBJECTS_CHECKSUM);
        assert!(content_objects_schema_is_current(&pool).await.unwrap());
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
