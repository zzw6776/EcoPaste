use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use chacha20poly1305::{
    aead::{
        stream::{DecryptorBE32, EncryptorBE32},
        Aead, KeyInit, Payload,
    },
    XChaCha20Poly1305, XNonce,
};
use ecopaste_sync_protocol::EncryptedEvent;
use rand::RngCore;
use walkdir::WalkDir;
use zip::{write::SimpleFileOptions, CompressionMethod, ZipArchive, ZipWriter};

use super::model::{ClipboardEnvelope, StoredBlob};

const BLOB_MAGIC: &[u8; 8] = b"ECOSBL01";
const STREAM_NONCE_LEN: usize = 19;
const CHUNK_SIZE: usize = 64 * 1024;
const TAG_SIZE: usize = 16;
const EVENT_AAD_MAGIC: &[u8; 8] = b"ECOEVA01";

struct TemporaryFileCleanup {
    armed: bool,
    path: PathBuf,
}

pub struct FingerprintedBlob {
    pub blob: StoredBlob,
    pub plaintext_hash: String,
}

pub struct ArchivedDirectory {
    pub content_size: u64,
    pub fingerprint: String,
}

impl TemporaryFileCleanup {
    fn new(path: PathBuf) -> Self {
        Self { armed: true, path }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryFileCleanup {
    fn drop(&mut self) {
        if self.armed {
            fs::remove_file(&self.path).ok();
        }
    }
}

pub fn encrypt_event(
    key: &[u8; 32],
    event_id: String,
    origin_device_id: String,
    origin_sequence: u64,
    created_at_ms: i64,
    envelope: &ClipboardEnvelope,
) -> Result<EncryptedEvent> {
    let plaintext = minicbor::to_vec(envelope).context("failed to encode sync event")?;
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize sync cipher")?;
    let mut nonce = [0_u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let aad = event_aad(&event_id, &origin_device_id, origin_sequence, created_at_ms);
    let encrypted = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext.as_slice(),
                aad: &aad,
            },
        )
        .map_err(|_| anyhow::anyhow!("failed to encrypt sync event"))?;
    let mut ciphertext = Vec::with_capacity(EVENT_AAD_MAGIC.len() + encrypted.len());
    ciphertext.extend_from_slice(EVENT_AAD_MAGIC);
    ciphertext.extend_from_slice(&encrypted);
    Ok(EncryptedEvent {
        event_id,
        origin_device_id,
        origin_sequence,
        created_at_ms,
        nonce: nonce.to_vec(),
        ciphertext,
    })
}

pub fn decrypt_event(key: &[u8; 32], event: &EncryptedEvent) -> Result<ClipboardEnvelope> {
    if event.nonce.len() != 24 {
        bail!("invalid sync event nonce");
    }
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize sync cipher")?;
    let plaintext = if let Some(ciphertext) = event.ciphertext.strip_prefix(EVENT_AAD_MAGIC) {
        let aad = event_aad(
            &event.event_id,
            &event.origin_device_id,
            event.origin_sequence,
            event.created_at_ms,
        );
        cipher.decrypt(
            XNonce::from_slice(&event.nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
    } else {
        // 已生成的测试数据没有 AAD 标记，继续支持解密；新事件无法降级到此分支。
        cipher.decrypt(
            XNonce::from_slice(&event.nonce),
            event.ciphertext.as_slice(),
        )
    }
    .map_err(|_| anyhow::anyhow!("sync event authentication failed"))?;
    minicbor::decode(&plaintext).context("failed to decode sync event")
}

fn event_aad(
    event_id: &str,
    origin_device_id: &str,
    origin_sequence: u64,
    created_at_ms: i64,
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(event_id.len() + origin_device_id.len() + 24);
    aad.extend_from_slice(&(event_id.len() as u32).to_be_bytes());
    aad.extend_from_slice(event_id.as_bytes());
    aad.extend_from_slice(&(origin_device_id.len() as u32).to_be_bytes());
    aad.extend_from_slice(origin_device_id.as_bytes());
    aad.extend_from_slice(&origin_sequence.to_be_bytes());
    aad.extend_from_slice(&created_at_ms.to_be_bytes());
    aad
}

/// Encrypts a file in fixed-size authenticated chunks, keeping memory bounded for large files.
pub fn encrypt_blob(source: &Path, blob_root: &Path, key: &[u8; 32]) -> Result<StoredBlob> {
    Ok(encrypt_blob_with_fingerprint(source, blob_root, key)?.blob)
}

/// Encrypts once and returns the plaintext hash calculated from the same read buffer.
pub fn encrypt_blob_with_fingerprint(
    source: &Path,
    blob_root: &Path,
    key: &[u8; 32],
) -> Result<FingerprintedBlob> {
    let mut stream_nonce = [0_u8; STREAM_NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut stream_nonce);
    encrypt_blob_with_nonce(source, blob_root, key, stream_nonce)
}

/// Encrypts a small content-addressed asset deterministically inside one sync space.
/// The nonce is reused only for the same plaintext hash, so equality is intentionally
/// visible within that space while ciphertext remains unlinkable across spaces.
pub fn encrypt_stable_blob(
    source: &Path,
    blob_root: &Path,
    key: &[u8; 32],
    plaintext_hash: &str,
) -> Result<StoredBlob> {
    if plaintext_hash.len() != 64
        || !plaintext_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        || hash_file(source)? != plaintext_hash
    {
        bail!("source app icon hash mismatch");
    }
    let mut nonce_input = Vec::with_capacity(32 + plaintext_hash.len());
    nonce_input.extend_from_slice(b"ecopaste/source-icon/v1\0");
    nonce_input.extend_from_slice(plaintext_hash.as_bytes());
    let digest = blake3::keyed_hash(key, &nonce_input);
    let mut stream_nonce = [0_u8; STREAM_NONCE_LEN];
    stream_nonce.copy_from_slice(&digest.as_bytes()[..STREAM_NONCE_LEN]);
    Ok(encrypt_blob_with_nonce(source, blob_root, key, stream_nonce)?.blob)
}

fn encrypt_blob_with_nonce(
    source: &Path,
    blob_root: &Path,
    key: &[u8; 32],
    stream_nonce: [u8; STREAM_NONCE_LEN],
) -> Result<FingerprintedBlob> {
    let source_size = source
        .metadata()
        .with_context(|| format!("failed to stat sync source {source:?}"))?
        .len();
    fs::create_dir_all(blob_root)
        .with_context(|| format!("failed to create sync blob root {blob_root:?}"))?;
    let temporary = blob_root.join(format!("{}.part", uuid::Uuid::new_v4()));
    let mut temporary_cleanup = TemporaryFileCleanup::new(temporary.clone());
    let input =
        File::open(source).with_context(|| format!("failed to open sync source {source:?}"))?;
    let output = File::create(&temporary)
        .with_context(|| format!("failed to create encrypted sync blob {temporary:?}"))?;
    let mut reader = BufReader::new(input);
    let mut writer = BufWriter::new(output);
    writer.write_all(BLOB_MAGIC)?;
    writer.write_all(&stream_nonce)?;

    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize blob cipher")?;
    let mut encryptor = Some(EncryptorBE32::from_aead(
        cipher,
        stream_nonce.as_slice().into(),
    ));
    let mut remaining = source_size;
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    let mut plaintext_hasher = blake3::Hasher::new();
    if remaining == 0 {
        let ciphertext = encryptor
            .take()
            .expect("stream encryptor available")
            .encrypt_last(&[][..])
            .map_err(|_| anyhow::anyhow!("failed to encrypt empty blob"))?;
        writer.write_all(&ciphertext)?;
    } else {
        while remaining > 0 {
            let length = usize::try_from(remaining.min(CHUNK_SIZE as u64)).unwrap_or(CHUNK_SIZE);
            reader.read_exact(&mut buffer[..length])?;
            plaintext_hasher.update(&buffer[..length]);
            remaining -= length as u64;
            let ciphertext = if remaining == 0 {
                encryptor
                    .take()
                    .expect("stream encryptor available")
                    .encrypt_last(&buffer[..length])
                    .map_err(|_| anyhow::anyhow!("failed to encrypt final blob chunk"))?
            } else {
                encryptor
                    .as_mut()
                    .expect("stream encryptor available")
                    .encrypt_next(&buffer[..length])
                    .map_err(|_| anyhow::anyhow!("failed to encrypt blob chunk"))?
            };
            writer.write_all(&ciphertext)?;
        }
    }
    writer.flush()?;
    drop(writer);

    let blob_id = hash_file(&temporary)?;
    let destination = blob_path(blob_root, &blob_id)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    if destination.exists() {
        fs::remove_file(&temporary)?;
    } else {
        fs::rename(&temporary, &destination)?;
    }
    temporary_cleanup.disarm();
    let encrypted_size = destination.metadata()?.len();
    Ok(FingerprintedBlob {
        blob: StoredBlob {
            blob_id,
            encrypted_path: destination.to_string_lossy().into_owned(),
            size: encrypted_size,
        },
        plaintext_hash: plaintext_hasher.finalize().to_hex().to_string(),
    })
}

/// Decrypts a blob to an atomic temporary file and verifies every STREAM chunk tag.
pub fn decrypt_blob(
    encrypted: &Path,
    destination: &Path,
    original_size: u64,
    key: &[u8; 32],
) -> Result<()> {
    decrypt_blob_with_fingerprint(encrypted, destination, original_size, key).map(|_| ())
}

/// Decrypts once and returns the plaintext hash calculated from the authenticated chunks.
pub fn decrypt_blob_with_fingerprint(
    encrypted: &Path,
    destination: &Path,
    original_size: u64,
    key: &[u8; 32],
) -> Result<String> {
    let input = File::open(encrypted)
        .with_context(|| format!("failed to open encrypted sync blob {encrypted:?}"))?;
    let mut reader = BufReader::new(input);
    let mut magic = [0_u8; BLOB_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if &magic != BLOB_MAGIC {
        bail!("invalid encrypted sync blob");
    }
    let mut stream_nonce = [0_u8; STREAM_NONCE_LEN];
    reader.read_exact(&mut stream_nonce)?;
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize blob cipher")?;
    let mut decryptor = Some(DecryptorBE32::from_aead(
        cipher,
        stream_nonce.as_slice().into(),
    ));

    let parent = destination
        .parent()
        .context("sync file destination has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = destination.with_extension(format!("part-{}", uuid::Uuid::new_v4()));
    let mut temporary_cleanup = TemporaryFileCleanup::new(temporary.clone());
    let output = File::create(&temporary)?;
    let mut writer = BufWriter::new(output);
    let mut remaining = original_size;
    let mut buffer = vec![0_u8; CHUNK_SIZE + TAG_SIZE];
    let mut plaintext_hasher = blake3::Hasher::new();
    if remaining == 0 {
        reader.read_exact(&mut buffer[..TAG_SIZE])?;
        let plaintext = decryptor
            .take()
            .expect("stream decryptor available")
            .decrypt_last(&buffer[..TAG_SIZE])
            .map_err(|_| anyhow::anyhow!("empty sync blob authentication failed"))?;
        plaintext_hasher.update(&plaintext);
        writer.write_all(&plaintext)?;
    } else {
        while remaining > 0 {
            let plaintext_length =
                usize::try_from(remaining.min(CHUNK_SIZE as u64)).unwrap_or(CHUNK_SIZE);
            let encrypted_length = plaintext_length + TAG_SIZE;
            reader.read_exact(&mut buffer[..encrypted_length])?;
            remaining -= plaintext_length as u64;
            let plaintext = if remaining == 0 {
                decryptor
                    .take()
                    .expect("stream decryptor available")
                    .decrypt_last(&buffer[..encrypted_length])
                    .map_err(|_| anyhow::anyhow!("final sync blob authentication failed"))?
            } else {
                decryptor
                    .as_mut()
                    .expect("stream decryptor available")
                    .decrypt_next(&buffer[..encrypted_length])
                    .map_err(|_| anyhow::anyhow!("sync blob authentication failed"))?
            };
            plaintext_hasher.update(&plaintext);
            writer.write_all(&plaintext)?;
        }
    }
    writer.flush()?;
    drop(writer);
    if reader.read(&mut [0_u8; 1])? != 0 {
        bail!("encrypted sync blob has trailing data");
    }
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(temporary, destination)?;
    temporary_cleanup.disarm();
    Ok(plaintext_hasher.finalize().to_hex().to_string())
}

pub fn blob_path(root: &Path, blob_id: &str) -> Result<PathBuf> {
    if blob_id.len() != 64 || !blob_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("invalid blob id");
    }
    Ok(root.join(&blob_id[..2]).join(blob_id))
}

pub fn hash_file(path: &Path) -> Result<String> {
    let mut file = BufReader::new(File::open(path)?);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// 把目录压缩为 ZIP，并在同一次读取中计算与绝对路径、遍历顺序无关的目录树指纹。
pub fn archive_directory(source: &Path, destination: &Path) -> Result<ArchivedDirectory> {
    let output = File::create(destination)?;
    let mut archive = ZipWriter::new(BufWriter::new(output));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut original_size = 0_u64;
    let mut entries = WalkDir::new(source)
        .follow_links(false)
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| portable_relative_path(entry.path(), source));
    let mut entry_hashes = Vec::new();
    for entry in entries {
        let path = entry.path();
        let relative = path.strip_prefix(source)?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let name = portable_relative_path(path, source);
        if entry.file_type().is_symlink() {
            continue;
        }
        if entry.file_type().is_dir() {
            archive.add_directory(format!("{name}/"), options)?;
            entry_hashes.push(directory_entry_hash(&name));
        } else if entry.file_type().is_file() {
            archive.start_file(&name, options)?;
            let mut input = File::open(path)?;
            let (written, content_hash) = copy_with_hash(&mut input, &mut archive)?;
            original_size = original_size
                .checked_add(written)
                .context("directory content size overflow")?;
            entry_hashes.push(file_entry_hash(&name, &content_hash));
        }
    }
    archive.finish()?;
    Ok(ArchivedDirectory {
        content_size: original_size,
        fingerprint: directory_tree_hash(entry_hashes),
    })
}

/// 解压目录归档，并在同一次输出中重建与发送端一致的目录树指纹。
pub fn extract_directory_archive(archive_path: &Path, destination: &Path) -> Result<String> {
    let input = File::open(archive_path)?;
    let mut archive = ZipArchive::new(BufReader::new(input))?;
    fs::create_dir_all(destination)?;
    let mut entry_hashes = Vec::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let Some(relative) = entry.enclosed_name() else {
            bail!("directory archive contains an unsafe path");
        };
        let relative = relative.to_path_buf();
        let name = relative.to_string_lossy().replace('\\', "/");
        let output = destination.join(&relative);
        if entry.is_dir() {
            fs::create_dir_all(&output)?;
            entry_hashes.push(directory_entry_hash(name.trim_end_matches('/')));
            continue;
        }
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(output)?;
        let (_, content_hash) = copy_with_hash(&mut entry, &mut file)?;
        entry_hashes.push(file_entry_hash(&name, &content_hash));
    }
    Ok(directory_tree_hash(entry_hashes))
}

fn portable_relative_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn copy_with_hash(reader: &mut impl Read, writer: &mut impl Write) -> Result<(u64, String)> {
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    let mut written = 0_u64;
    let mut hasher = blake3::Hasher::new();
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        written = written
            .checked_add(read as u64)
            .context("copied content size overflow")?;
    }
    Ok((written, hasher.finalize().to_hex().to_string()))
}

fn directory_entry_hash(name: &str) -> blake3::Hash {
    tree_entry_hash(b'd', name, None)
}

fn file_entry_hash(name: &str, content_hash: &str) -> blake3::Hash {
    tree_entry_hash(b'f', name, Some(content_hash))
}

fn tree_entry_hash(kind: u8, name: &str, content_hash: Option<&str>) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ecopaste-directory-entry-v1\0");
    hasher.update(&[kind]);
    update_length_prefixed(&mut hasher, name.as_bytes());
    if let Some(content_hash) = content_hash {
        update_length_prefixed(&mut hasher, content_hash.as_bytes());
    }
    hasher.finalize()
}

fn directory_tree_hash(mut entry_hashes: Vec<blake3::Hash>) -> String {
    entry_hashes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ecopaste-directory-tree-v1\0");
    for entry_hash in entry_hashes {
        hasher.update(entry_hash.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn update_length_prefixed(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_archive_reports_original_file_total() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let nested = source.join("nested");
        let archive = temporary.path().join("directory.zip");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(source.join("first.txt"), b"abc").unwrap();
        std::fs::write(nested.join("second.txt"), b"12345").unwrap();

        let archived = archive_directory(&source, &archive).unwrap();

        assert_eq!(archived.content_size, 8);
        let destination = temporary.path().join("destination");
        let extracted_fingerprint = extract_directory_archive(&archive, &destination).unwrap();
        assert_eq!(extracted_fingerprint, archived.fingerprint);
    }

    #[test]
    fn directory_fingerprint_ignores_absolute_root() {
        let temporary = tempfile::tempdir().unwrap();
        let first = temporary.path().join("first-root");
        let second = temporary.path().join("second-root");
        std::fs::create_dir_all(first.join("nested")).unwrap();
        std::fs::create_dir_all(second.join("nested")).unwrap();
        std::fs::write(first.join("a.txt"), b"same").unwrap();
        std::fs::write(second.join("a.txt"), b"same").unwrap();
        std::fs::write(first.join("nested/b.txt"), b"tree").unwrap();
        std::fs::write(second.join("nested/b.txt"), b"tree").unwrap();

        let first_archive = archive_directory(&first, &temporary.path().join("first.zip")).unwrap();
        let second_archive =
            archive_directory(&second, &temporary.path().join("second.zip")).unwrap();

        assert_eq!(first_archive.fingerprint, second_archive.fingerprint);
    }

    #[test]
    fn blob_stream_round_trip_covers_chunk_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        let destination = directory.path().join("destination.bin");
        let bytes = (0..(CHUNK_SIZE + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&source, &bytes).unwrap();
        let key = [5_u8; 32];

        let encrypted = encrypt_blob(&source, &directory.path().join("blobs"), &key).unwrap();
        let decrypted_hash = decrypt_blob_with_fingerprint(
            Path::new(&encrypted.encrypted_path),
            &destination,
            bytes.len() as u64,
            &key,
        )
        .unwrap();

        assert_eq!(fs::read(destination).unwrap(), bytes);
        assert_eq!(decrypted_hash, blake3::hash(&bytes).to_hex().to_string());
    }

    #[test]
    fn stable_blob_is_deduplicated_inside_one_space_and_scoped_between_spaces() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.png");
        fs::write(&source, b"normalized icon bytes").unwrap();
        let plaintext_hash = blake3::hash(b"normalized icon bytes").to_hex().to_string();

        let first = encrypt_stable_blob(
            &source,
            &directory.path().join("first"),
            &[1_u8; 32],
            &plaintext_hash,
        )
        .unwrap();
        let repeated = encrypt_stable_blob(
            &source,
            &directory.path().join("second"),
            &[1_u8; 32],
            &plaintext_hash,
        )
        .unwrap();
        let other_space = encrypt_stable_blob(
            &source,
            &directory.path().join("third"),
            &[2_u8; 32],
            &plaintext_hash,
        )
        .unwrap();

        assert_eq!(first.blob_id, repeated.blob_id);
        assert_eq!(
            fs::read(first.encrypted_path).unwrap(),
            fs::read(repeated.encrypted_path).unwrap()
        );
        assert_ne!(first.blob_id, other_space.blob_id);
    }

    #[test]
    fn event_metadata_is_authenticated() {
        let key = [9_u8; 32];
        let envelope = ClipboardEnvelope {
            version: 1,
            item: super::super::model::SyncedClipboardItem {
                kind: "text".into(),
                sub_kind: None,
                content: "test".into(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(4),
                width: None,
                height: None,
                is_sensitive: false,
                source_platform: "macos".into(),
                created_at_ms: 1,
                content_hash: "hash".into(),
                updated_at_ms: Some(2),
                source_revision: Some("revision".into()),
            },
            blobs: Vec::new(),
            source_app: None,
        };
        let event = encrypt_event(
            &key,
            "event-123".into(),
            "device-123".into(),
            1,
            2,
            &envelope,
        )
        .unwrap();
        assert_eq!(decrypt_event(&key, &event).unwrap(), envelope);

        let mut tampered = event;
        tampered.origin_sequence = 2;
        assert!(decrypt_event(&key, &tampered).is_err());

        let nonce = [3_u8; 24];
        let cipher = XChaCha20Poly1305::new_from_slice(&key).unwrap();
        let legacy_ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                minicbor::to_vec(&envelope).unwrap().as_slice(),
            )
            .unwrap();
        let legacy = EncryptedEvent {
            event_id: "legacy-event".into(),
            origin_device_id: "legacy-device".into(),
            origin_sequence: 1,
            created_at_ms: 2,
            nonce: nonce.to_vec(),
            ciphertext: legacy_ciphertext,
        };
        assert_eq!(decrypt_event(&key, &legacy).unwrap(), envelope);
    }

    #[test]
    fn blob_path_rejects_traversal_and_short_ids() {
        let root = Path::new("blobs");
        assert!(blob_path(root, "../escape").is_err());
        assert!(blob_path(root, &"a".repeat(64)).is_ok());
    }
}
