use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha384};

fn main() {
    verify_migration_lock();
    tauri_build::build()
}

/// Keeps every embedded migration byte-for-byte aligned with the reviewed lock file.
fn verify_migration_lock() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"));
    let migrations_dir = manifest_dir.join("migrations");
    let lock_path = manifest_dir.join("migrations.lock");

    println!("cargo:rerun-if-changed={}", migrations_dir.display());
    println!("cargo:rerun-if-changed={}", lock_path.display());

    let expected = read_migration_lock(&lock_path);
    let actual = read_migrations(&migrations_dir);
    if expected != actual {
        panic!(
            "migration lock mismatch; applied migrations are immutable. Add schema changes in a new numbered migration and append its SHA-384 entry to {}",
            lock_path.display()
        );
    }
}

fn read_migration_lock(path: &Path) -> BTreeMap<u64, (String, String)> {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let mut entries = BTreeMap::new();
    for (line_index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split_whitespace().collect();
        assert_eq!(
            fields.len(),
            3,
            "invalid migration lock entry at {}:{}",
            path.display(),
            line_index + 1
        );
        let version = fields[0].parse::<u64>().unwrap_or_else(|error| {
            panic!(
                "invalid migration version at {}:{}: {error}",
                path.display(),
                line_index + 1
            )
        });
        let checksum = fields[1].to_ascii_uppercase();
        assert!(
            checksum.len() == 96 && checksum.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid SHA-384 checksum at {}:{}",
            path.display(),
            line_index + 1
        );
        let previous = entries.insert(version, (fields[2].to_owned(), checksum));
        assert!(
            previous.is_none(),
            "duplicate migration version {version} in {}",
            path.display()
        );
    }
    entries
}

fn read_migrations(directory: &Path) -> BTreeMap<u64, (String, String)> {
    let mut entries = BTreeMap::new();
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
    {
        let path = entry
            .unwrap_or_else(|error| panic!("failed to inspect migration: {error}"))
            .path();
        if path.extension().and_then(|value| value.to_str()) != Some("sql") {
            continue;
        }
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_else(|| panic!("migration filename is not valid UTF-8: {}", path.display()))
            .to_owned();
        let version = filename
            .split_once('_')
            .map(|(prefix, _)| prefix)
            .unwrap_or_else(|| panic!("migration filename lacks a numeric prefix: {filename}"))
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("invalid migration filename {filename}: {error}"));
        let bytes = fs::read(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let checksum = format!("{:X}", Sha384::digest(bytes));
        let previous = entries.insert(version, (filename, checksum));
        assert!(
            previous.is_none(),
            "duplicate migration version {version} in {}",
            directory.display()
        );
    }
    entries
}
