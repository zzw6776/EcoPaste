use super::activity::SyncActivityGuard;
use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use chacha20poly1305::{
    aead::{
        stream::{DecryptorBE32, EncryptorBE32},
        Aead, KeyInit, Payload,
    },
    XChaCha20Poly1305, XNonce,
};
use ecopaste_sync_protocol::{EncryptedEvent, RetentionMapping, SyncEventKind};
use rand::RngCore;
use tokio::io::{
    AsyncRead as TokioAsyncRead, AsyncReadExt as TokioAsyncReadExt, AsyncWrite as TokioAsyncWrite,
    AsyncWriteExt as TokioAsyncWriteExt,
};
use walkdir::WalkDir;
use zip::{write::SimpleFileOptions, CompressionMethod, ZipArchive, ZipWriter};

use super::model::{ClipboardEnvelope, DecryptedSyncEvent, StoredBlob, SyncedItemStatePatch};

const BLOB_MAGIC: &[u8; 8] = b"ECOSBL01";
const STREAM_NONCE_LEN: usize = 19;
const CHUNK_SIZE: usize = 64 * 1024;
const TAG_SIZE: usize = 16;
pub(crate) const EVENT_AAD_MAGIC: &[u8; 8] = b"ECOEVA02";
const STREAM_BLOB_ID_DOMAIN: &[u8] = b"ecopaste/stream-blob-id/v1\0";

/// Advance only when the event decoder gains support for previously deferred formats.
pub const RETENTION_DECODER_REVISION: u32 = 1;

struct TemporaryFileCleanup {
    armed: bool,
    path: PathBuf,
}

pub struct FingerprintedBlob {
    pub blob: StoredBlob,
    #[cfg(test)]
    pub plaintext_hash: String,
}

pub struct ArchivedDirectory {
    pub content_size: u64,
    pub fingerprint: String,
}

/// Incremental file encryptor used by direct uploads without a staged ciphertext file.
pub struct BlobStreamEncryptor {
    encryptor: Option<EncryptorBE32<XChaCha20Poly1305>>,
    remaining: u64,
    plaintext_hasher: blake3::Hasher,
    stream_nonce: [u8; STREAM_NONCE_LEN],
}

impl BlobStreamEncryptor {
    pub fn new(key: &[u8; 32], stream_nonce: &[u8], original_size: u64) -> Result<Self> {
        let stream_nonce: [u8; STREAM_NONCE_LEN] = stream_nonce
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid sync stream nonce"))?;
        let cipher =
            XChaCha20Poly1305::new_from_slice(key).context("failed to initialize blob cipher")?;
        Ok(Self {
            encryptor: Some(EncryptorBE32::from_aead(
                cipher,
                stream_nonce.as_slice().into(),
            )),
            remaining: original_size,
            plaintext_hasher: blake3::Hasher::new(),
            stream_nonce,
        })
    }

    pub fn header(&self) -> Vec<u8> {
        let mut header = Vec::with_capacity(BLOB_MAGIC.len() + STREAM_NONCE_LEN);
        header.extend_from_slice(BLOB_MAGIC);
        header.extend_from_slice(&self.stream_nonce);
        header
    }

    pub fn encrypt_next(&mut self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let expected = if self.remaining == 0 {
            0
        } else {
            usize::try_from(self.remaining.min(CHUNK_SIZE as u64)).unwrap_or(CHUNK_SIZE)
        };
        if plaintext.len() != expected {
            bail!("sync source size changed during upload");
        }
        self.plaintext_hasher.update(plaintext);
        self.remaining = self.remaining.saturating_sub(plaintext.len() as u64);
        if self.remaining == 0 {
            self.encryptor
                .take()
                .context("stream encryptor already finalized")?
                .encrypt_last(plaintext)
                .map_err(|_| anyhow::anyhow!("failed to encrypt final blob chunk"))
        } else {
            self.encryptor
                .as_mut()
                .context("stream encryptor already finalized")?
                .encrypt_next(plaintext)
                .map_err(|_| anyhow::anyhow!("failed to encrypt blob chunk"))
        }
    }

    pub fn finish(self, key: &[u8; 32], expected_blob_id: &str) -> Result<String> {
        if self.remaining != 0 || self.encryptor.is_some() {
            bail!("sync source ended before the expected size");
        }
        let plaintext_hash = self.plaintext_hasher.finalize().to_hex().to_string();
        let actual_blob_id = stream_blob_id(key, &self.stream_nonce, &plaintext_hash)?;
        if actual_blob_id != expected_blob_id {
            bail!("sync source content changed before upload");
        }
        Ok(plaintext_hash)
    }
}

pub fn new_stream_nonce() -> Vec<u8> {
    let mut stream_nonce = [0_u8; STREAM_NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut stream_nonce);
    stream_nonce.to_vec()
}

/// Commits a random stream nonce and plaintext digest without exposing either to the Hub.
pub fn stream_blob_id(key: &[u8; 32], stream_nonce: &[u8], plaintext_hash: &str) -> Result<String> {
    if stream_nonce.len() != STREAM_NONCE_LEN
        || plaintext_hash.len() != 64
        || !plaintext_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("invalid streaming blob identity");
    }
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(STREAM_BLOB_ID_DOMAIN);
    hasher.update(stream_nonce);
    hasher.update(plaintext_hash.as_bytes());
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn encrypted_blob_size(original_size: u64) -> Result<u64> {
    let chunks = if original_size == 0 {
        1
    } else {
        original_size.div_ceil(CHUNK_SIZE as u64)
    };
    original_size
        .checked_add((BLOB_MAGIC.len() + STREAM_NONCE_LEN) as u64)
        .and_then(|size| size.checked_add(chunks.saturating_mul(TAG_SIZE as u64)))
        .context("encrypted blob size overflow")
}

pub fn verify_stream_blob_id(
    key: &[u8; 32],
    encrypted_path: &Path,
    plaintext_hash: &str,
    expected_blob_id: &str,
) -> Result<()> {
    let nonce = blob_stream_nonce(encrypted_path)?;
    if stream_blob_id(key, &nonce, plaintext_hash)? != expected_blob_id {
        bail!("streaming blob identity mismatch");
    }
    Ok(())
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

fn encrypt_raw_event(
    key: &[u8; 32],
    event_id: String,
    origin_device_id: String,
    origin_sequence: u64,
    created_at_ms: i64,
    kind: SyncEventKind,
    plaintext: &[u8],
) -> Result<EncryptedEvent> {
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize sync cipher")?;
    let mut nonce = [0_u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let aad = event_aad(
        &event_id,
        &origin_device_id,
        origin_sequence,
        created_at_ms,
        kind,
    );
    let encrypted = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
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
        kind,
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
        retention_key: None,
    })
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
    let mut event = encrypt_raw_event(
        key,
        event_id,
        origin_device_id,
        origin_sequence,
        created_at_ms,
        SyncEventKind::Content,
        &plaintext,
    )?;
    event.retention_key = Some(retention_key(key, &envelope.item.sync_key));
    Ok(event)
}

pub fn encrypt_item_state(
    key: &[u8; 32],
    event_id: String,
    origin_device_id: String,
    origin_sequence: u64,
    created_at_ms: i64,
    patch: &SyncedItemStatePatch,
) -> Result<EncryptedEvent> {
    let plaintext = minicbor::to_vec(patch).context("failed to encode synchronized item state")?;
    let mut event = encrypt_raw_event(
        key,
        event_id,
        origin_device_id,
        origin_sequence,
        created_at_ms,
        SyncEventKind::ItemState,
        &plaintext,
    )?;
    event.retention_key = Some(retention_key(key, &patch.sync_key));
    Ok(event)
}

fn decrypt_event_plaintext(key: &[u8; 32], event: &EncryptedEvent) -> Result<Vec<u8>> {
    if event.nonce.len() != 24 {
        bail!("invalid sync event nonce");
    }
    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize sync cipher")?;
    let ciphertext = event
        .ciphertext
        .strip_prefix(EVENT_AAD_MAGIC)
        .context("invalid synchronized event format")?;
    let aad = event_aad(
        &event.event_id,
        &event.origin_device_id,
        event.origin_sequence,
        event.created_at_ms,
        event.kind,
    );
    let plaintext = cipher
        .decrypt(
            XNonce::from_slice(&event.nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| anyhow::anyhow!("sync event authentication failed"))?;
    Ok(plaintext)
}

pub fn decrypt_sync_event(key: &[u8; 32], event: &EncryptedEvent) -> Result<DecryptedSyncEvent> {
    let plaintext = decrypt_event_plaintext(key, event)?;
    let decoded = match event.kind {
        SyncEventKind::Content => {
            let envelope = minicbor::decode(&plaintext).context("failed to decode sync event")?;
            DecryptedSyncEvent::Clipboard(Box::new(envelope))
        }
        SyncEventKind::ItemState => {
            let patch =
                minicbor::decode(&plaintext).context("failed to decode synchronized item state")?;
            DecryptedSyncEvent::ItemState(patch)
        }
    };
    if event
        .retention_key
        .as_ref()
        .is_some_and(|value| value != &retention_key(key, decoded.sync_key()))
    {
        bail!("sync event retention identity mismatch");
    }
    Ok(decoded)
}

/// Derives a space-specific opaque identity without exposing content hashes or the content key.
pub fn retention_key(key: &[u8; 32], sync_key: &str) -> String {
    let mut hash = blake3::Hasher::new_keyed(key);
    hash.update(b"ecopaste/retention/1\0");
    hash.update(sync_key.as_bytes());
    hash.finalize().to_hex().to_string()
}

/// Reconstructs legacy attachment references from authenticated ciphertext without fetching files.
pub fn retention_mapping(key: &[u8; 32], event: &EncryptedEvent) -> Result<RetentionMapping> {
    let decoded = decrypt_sync_event(key, event)?;
    let mut blob_ids = Vec::new();
    if let DecryptedSyncEvent::Clipboard(envelope) = &decoded {
        blob_ids.extend(envelope.blobs.iter().map(|blob| blob.blob_id.clone()));
        if let Some(icon) = envelope
            .source_app
            .as_ref()
            .and_then(|source| source.icon.as_ref())
        {
            blob_ids.push(icon.blob_id.clone());
        }
    }
    blob_ids.sort_unstable();
    blob_ids.dedup();
    Ok(RetentionMapping {
        event_id: event.event_id.clone(),
        item_key: retention_key(key, decoded.sync_key()),
        blob_ids,
    })
}

pub fn decrypt_event(key: &[u8; 32], event: &EncryptedEvent) -> Result<ClipboardEnvelope> {
    match decrypt_sync_event(key, event)? {
        DecryptedSyncEvent::Clipboard(envelope) => Ok(*envelope),
        DecryptedSyncEvent::ItemState(_) => {
            bail!("expected clipboard envelope, got synchronized item state")
        }
    }
}

fn event_aad(
    event_id: &str,
    origin_device_id: &str,
    origin_sequence: u64,
    created_at_ms: i64,
    kind: SyncEventKind,
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(event_id.len() + origin_device_id.len() + 25);
    aad.extend_from_slice(&(event_id.len() as u32).to_be_bytes());
    aad.extend_from_slice(event_id.as_bytes());
    aad.extend_from_slice(&(origin_device_id.len() as u32).to_be_bytes());
    aad.extend_from_slice(origin_device_id.as_bytes());
    aad.extend_from_slice(&origin_sequence.to_be_bytes());
    aad.extend_from_slice(&created_at_ms.to_be_bytes());
    aad.push(kind.as_i64() as u8);
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
    encrypt_blob_with_nonce(source, blob_root, key, stream_nonce, || false)
}

/// Encrypts staged content while checking whether its enclosing transfer was canceled.
pub fn encrypt_blob_with_cancel(
    source: &Path,
    blob_root: &Path,
    key: &[u8; 32],
    canceled: impl FnMut() -> bool,
) -> Result<StoredBlob> {
    let mut stream_nonce = [0_u8; STREAM_NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut stream_nonce);
    Ok(encrypt_blob_with_nonce(source, blob_root, key, stream_nonce, canceled)?.blob)
}

/// Recreates a staged ciphertext from its original source and previously persisted nonce.
pub fn encrypt_blob_with_stream_nonce(
    source: &Path,
    blob_root: &Path,
    key: &[u8; 32],
    stream_nonce: &[u8],
) -> Result<StoredBlob> {
    let stream_nonce: [u8; STREAM_NONCE_LEN] = stream_nonce
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid sync stream nonce"))?;
    Ok(encrypt_blob_with_nonce(source, blob_root, key, stream_nonce, || false)?.blob)
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
    Ok(encrypt_blob_with_nonce(source, blob_root, key, stream_nonce, || false)?.blob)
}

fn encrypt_blob_with_nonce(
    source: &Path,
    blob_root: &Path,
    key: &[u8; 32],
    stream_nonce: [u8; STREAM_NONCE_LEN],
    mut canceled: impl FnMut() -> bool,
) -> Result<FingerprintedBlob> {
    let _sync_activity = SyncActivityGuard::acquire();
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
    let mut ciphertext_hasher = blake3::Hasher::new();
    writer.write_all(BLOB_MAGIC)?;
    ciphertext_hasher.update(BLOB_MAGIC);
    writer.write_all(&stream_nonce)?;
    ciphertext_hasher.update(&stream_nonce);

    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize blob cipher")?;
    let mut encryptor = Some(EncryptorBE32::from_aead(
        cipher,
        stream_nonce.as_slice().into(),
    ));
    let mut remaining = source_size;
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    let mut plaintext_hasher = blake3::Hasher::new();
    if canceled() {
        bail!("transfer canceled");
    }
    if remaining == 0 {
        let ciphertext = encryptor
            .take()
            .expect("stream encryptor available")
            .encrypt_last(&[][..])
            .map_err(|_| anyhow::anyhow!("failed to encrypt empty blob"))?;
        writer.write_all(&ciphertext)?;
        ciphertext_hasher.update(&ciphertext);
    } else {
        while remaining > 0 {
            if canceled() {
                bail!("transfer canceled");
            }
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
            ciphertext_hasher.update(&ciphertext);
        }
    }
    if canceled() {
        bail!("transfer canceled");
    }
    writer.flush()?;
    drop(writer);

    let blob_id = ciphertext_hasher.finalize().to_hex().to_string();
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
            source_path: None,
            source_is_directory: false,
            stream_nonce: Some(stream_nonce.to_vec()),
        },
        #[cfg(test)]
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
    let _sync_activity = SyncActivityGuard::acquire();
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

/// Immutable authenticated-stream contract shared by desktop and Android destinations.
pub struct BlobStreamSpec<'a> {
    pub original_size: u64,
    pub encrypted_size: u64,
    pub expected_hash: &'a str,
    pub stream_commitment: bool,
    pub key: &'a [u8; 32],
    pub idle_timeout: Duration,
}

/// Authenticates and decrypts one network blob directly into the caller's destination stream.
pub async fn decrypt_blob_stream<R, W, F, C>(
    reader: &mut R,
    writer: &mut W,
    spec: BlobStreamSpec<'_>,
    mut on_progress: F,
    canceled: C,
) -> Result<String>
where
    R: TokioAsyncRead + Unpin,
    W: TokioAsyncWrite + Unpin,
    F: FnMut(u64),
    C: Fn() -> bool,
{
    let _sync_activity = SyncActivityGuard::acquire();
    let BlobStreamSpec {
        original_size,
        encrypted_size,
        expected_hash,
        stream_commitment,
        key,
        idle_timeout,
    } = spec;
    let header_size = (BLOB_MAGIC.len() + STREAM_NONCE_LEN) as u64;
    if encrypted_size < header_size.saturating_add(TAG_SIZE as u64) {
        bail!("invalid encrypted sync blob size");
    }

    let mut ciphertext_hasher = blake3::Hasher::new();
    let mut magic = [0_u8; BLOB_MAGIC.len()];
    tokio::time::timeout(
        idle_timeout,
        TokioAsyncReadExt::read_exact(reader, &mut magic),
    )
    .await
    .context("remote blob download idle timeout")??;
    if &magic != BLOB_MAGIC {
        bail!("invalid encrypted sync blob");
    }
    ciphertext_hasher.update(&magic);
    let mut stream_nonce = [0_u8; STREAM_NONCE_LEN];
    tokio::time::timeout(
        idle_timeout,
        TokioAsyncReadExt::read_exact(reader, &mut stream_nonce),
    )
    .await
    .context("remote blob download idle timeout")??;
    ciphertext_hasher.update(&stream_nonce);
    let mut transferred = header_size;
    on_progress(transferred);

    let cipher =
        XChaCha20Poly1305::new_from_slice(key).context("failed to initialize blob cipher")?;
    let mut decryptor = Some(DecryptorBE32::from_aead(
        cipher,
        stream_nonce.as_slice().into(),
    ));
    let mut remaining = original_size;
    let mut buffer = vec![0_u8; CHUNK_SIZE + TAG_SIZE];
    let mut plaintext_hasher = blake3::Hasher::new();
    loop {
        if canceled() {
            bail!("transfer canceled");
        }
        let plaintext_length = if original_size == 0 {
            0
        } else {
            usize::try_from(remaining.min(CHUNK_SIZE as u64)).unwrap_or(CHUNK_SIZE)
        };
        let encrypted_length = plaintext_length + TAG_SIZE;
        tokio::time::timeout(
            idle_timeout,
            TokioAsyncReadExt::read_exact(reader, &mut buffer[..encrypted_length]),
        )
        .await
        .context("remote blob download idle timeout")??;
        ciphertext_hasher.update(&buffer[..encrypted_length]);
        transferred = transferred.saturating_add(encrypted_length as u64);
        on_progress(transferred);
        remaining = remaining.saturating_sub(plaintext_length as u64);
        let is_last = original_size == 0 || remaining == 0;
        let plaintext = if is_last {
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
        tokio::time::timeout(
            idle_timeout,
            TokioAsyncWriteExt::write_all(writer, &plaintext),
        )
        .await
        .context("remote destination write idle timeout")??;
        if is_last {
            break;
        }
    }
    tokio::time::timeout(idle_timeout, TokioAsyncWriteExt::flush(writer))
        .await
        .context("remote destination flush timeout")??;
    if transferred != encrypted_size {
        bail!("encrypted sync blob size mismatch");
    }
    let plaintext_hash = plaintext_hasher.finalize().to_hex().to_string();
    let actual_id = if stream_commitment {
        stream_blob_id(key, &stream_nonce, &plaintext_hash)?
    } else {
        ciphertext_hasher.finalize().to_hex().to_string()
    };
    if actual_id != expected_hash {
        bail!("blob hash mismatch");
    }
    Ok(plaintext_hash)
}

pub fn blob_path(root: &Path, blob_id: &str) -> Result<PathBuf> {
    if blob_id.len() != 64 || !blob_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("invalid blob id");
    }
    Ok(root.join(&blob_id[..2]).join(blob_id))
}

pub fn hash_file(path: &Path) -> Result<String> {
    hash_file_with_progress(path, || false, |_| {})
}

pub fn hash_file_with_progress(
    path: &Path,
    mut canceled: impl FnMut() -> bool,
    mut on_progress: impl FnMut(u64),
) -> Result<String> {
    let mut file = BufReader::new(File::open(path)?);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    let mut processed = 0_u64;
    loop {
        if canceled() {
            bail!("transfer canceled");
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        processed = processed.saturating_add(read as u64);
        on_progress(processed);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Reads the authenticated-stream nonce needed to reproduce temporary ciphertext later.
pub fn blob_stream_nonce(path: &Path) -> Result<Vec<u8>> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut magic = [0_u8; BLOB_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if &magic != BLOB_MAGIC {
        bail!("invalid encrypted sync blob");
    }
    let mut nonce = [0_u8; STREAM_NONCE_LEN];
    reader.read_exact(&mut nonce)?;
    Ok(nonce.to_vec())
}

/// 把目录压缩为 ZIP，并在同一次读取中计算与绝对路径、遍历顺序无关的目录树指纹。
pub fn archive_directory(source: &Path, destination: &Path) -> Result<ArchivedDirectory> {
    archive_directory_with_progress(source, destination, || false, |_| {})
}

/// Archives a directory while reporting logical bytes and honoring transfer cancellation.
pub fn archive_directory_with_progress(
    source: &Path,
    destination: &Path,
    mut canceled: impl FnMut() -> bool,
    mut on_progress: impl FnMut(u64),
) -> Result<ArchivedDirectory> {
    let _sync_activity = SyncActivityGuard::acquire();
    let output = File::create(destination)?;
    let mut archive = ZipWriter::new(BufWriter::new(output));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut original_size = 0_u64;
    let mut entries = Vec::new();
    for entry in WalkDir::new(source).follow_links(false) {
        if canceled() {
            bail!("transfer canceled");
        }
        entries.push(entry?);
    }
    entries.sort_by_key(|entry| portable_relative_path(entry.path(), source));
    let mut entry_hashes = Vec::new();
    for entry in entries {
        if canceled() {
            bail!("transfer canceled");
        }
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
            let progress_base = original_size;
            let mut report_file_progress = |written| {
                on_progress(progress_base.saturating_add(written));
            };
            let (written, content_hash) = copy_with_hash_with_hooks(
                &mut input,
                &mut archive,
                &mut canceled,
                &mut report_file_progress,
            )?;
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
    extract_directory_archive_with_cancel(archive_path, destination, || false)
}

/// 解压目录归档，并在复制大文件时响应外部取消请求。
pub fn extract_directory_archive_with_cancel(
    archive_path: &Path,
    destination: &Path,
    mut canceled: impl FnMut() -> bool,
) -> Result<String> {
    let input = File::open(archive_path)?;
    let mut archive = ZipArchive::new(BufReader::new(input))?;
    fs::create_dir_all(destination)?;
    let mut entry_hashes = Vec::new();
    for index in 0..archive.len() {
        if canceled() {
            bail!("transfer canceled");
        }
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
        let (_, content_hash) = copy_with_hash_with_cancel(&mut entry, &mut file, &mut canceled)?;
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

fn copy_with_hash_with_cancel(
    reader: &mut impl Read,
    writer: &mut impl Write,
    canceled: &mut impl FnMut() -> bool,
) -> Result<(u64, String)> {
    copy_with_hash_with_hooks(reader, writer, canceled, &mut |_| {})
}

fn copy_with_hash_with_hooks(
    reader: &mut impl Read,
    writer: &mut impl Write,
    canceled: &mut impl FnMut() -> bool,
    on_progress: &mut impl FnMut(u64),
) -> Result<(u64, String)> {
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    let mut written = 0_u64;
    let mut hasher = blake3::Hasher::new();
    loop {
        if canceled() {
            bail!("transfer canceled");
        }
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        written = written
            .checked_add(read as u64)
            .context("copied content size overflow")?;
        on_progress(written);
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
    fn directory_extraction_stops_during_file_copy_when_canceled() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let archive = temporary.path().join("directory.zip");
        let destination = temporary.path().join("destination");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("large.bin"), vec![1_u8; CHUNK_SIZE * 2]).unwrap();
        archive_directory(&source, &archive).unwrap();
        let checks = std::cell::Cell::new(0_u8);

        let error = extract_directory_archive_with_cancel(&archive, &destination, || {
            let current = checks.get();
            checks.set(current.saturating_add(1));
            current > 0
        })
        .unwrap_err();

        assert!(error.to_string().contains("transfer canceled"));
        assert!(checks.get() >= 2);
    }

    #[test]
    fn directory_archiving_stops_during_file_copy_when_canceled() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let archive = temporary.path().join("directory.zip");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("large.bin"), vec![1_u8; CHUNK_SIZE * 2]).unwrap();
        let processed = std::cell::Cell::new(0_u64);

        let error = archive_directory_with_progress(
            &source,
            &archive,
            || processed.get() >= CHUNK_SIZE as u64,
            |bytes| processed.set(bytes),
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("transfer canceled"));
        assert_eq!(processed.get(), CHUNK_SIZE as u64);
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

        let first_path = temporary.path().join("first.zip");
        let second_path = temporary.path().join("second.zip");
        let first_archive = archive_directory(&first, &first_path).unwrap();
        let second_archive = archive_directory(&second, &second_path).unwrap();

        assert_eq!(first_archive.fingerprint, second_archive.fingerprint);
        assert_eq!(
            fs::read(first_path).unwrap(),
            fs::read(second_path).unwrap()
        );
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
    fn staged_ciphertext_can_be_recreated_with_the_original_nonce() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        fs::write(&source, b"reproducible payload").unwrap();
        let root = directory.path().join("staging");
        let key = [6_u8; 32];
        let original = encrypt_blob(&source, &root, &key).unwrap();
        let nonce = original.stream_nonce.clone().unwrap();
        fs::remove_file(&original.encrypted_path).unwrap();

        let rebuilt = encrypt_blob_with_stream_nonce(&source, &root, &key, &nonce).unwrap();

        assert_eq!(rebuilt.blob_id, original.blob_id);
        assert_eq!(rebuilt.size, original.size);
    }

    #[test]
    fn staged_encryption_removes_partial_output_when_canceled() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        let root = directory.path().join("staging");
        fs::write(&source, vec![1_u8; CHUNK_SIZE * 2]).unwrap();
        let checks = std::cell::Cell::new(0_u8);

        let error = encrypt_blob_with_cancel(&source, &root, &[6_u8; 32], || {
            let current = checks.get();
            checks.set(current.saturating_add(1));
            current > 1
        })
        .unwrap_err();

        assert!(error.to_string().contains("transfer canceled"));
        assert_eq!(fs::read_dir(root).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn remote_blob_stream_decrypts_directly_with_exact_progress() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        let destination = directory.path().join("destination.bin");
        let bytes = (0..(CHUNK_SIZE * 2 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&source, &bytes).unwrap();
        let key = [5_u8; 32];
        let encrypted = encrypt_blob(&source, &directory.path().join("blobs"), &key).unwrap();
        let mut reader = tokio::fs::File::open(&encrypted.encrypted_path)
            .await
            .unwrap();
        let mut writer = tokio::fs::File::create(&destination).await.unwrap();
        let mut transferred = 0_u64;

        let plaintext_hash = decrypt_blob_stream(
            &mut reader,
            &mut writer,
            BlobStreamSpec {
                original_size: bytes.len() as u64,
                encrypted_size: encrypted.size,
                expected_hash: &encrypted.blob_id,
                stream_commitment: false,
                key: &key,
                idle_timeout: Duration::from_secs(1),
            },
            |current| transferred = current,
            || false,
        )
        .await
        .unwrap();
        drop(writer);

        assert_eq!(transferred, encrypted.size);
        assert_eq!(tokio::fs::read(destination).await.unwrap(), bytes);
        assert_eq!(plaintext_hash, blake3::hash(&bytes).to_hex().to_string());
    }

    #[test]
    fn streaming_blob_identity_round_trips_without_ciphertext_staging() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("encrypted.bin");
        let bytes = (0..(CHUNK_SIZE * 2 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let key = [6_u8; 32];
        let nonce = new_stream_nonce();
        let plaintext_hash = blake3::hash(&bytes).to_hex().to_string();
        let blob_id = stream_blob_id(&key, &nonce, &plaintext_hash).unwrap();
        let mut encryptor = BlobStreamEncryptor::new(&key, &nonce, bytes.len() as u64).unwrap();
        let mut encrypted = encryptor.header();
        for chunk in bytes.chunks(CHUNK_SIZE) {
            encrypted.extend_from_slice(&encryptor.encrypt_next(chunk).unwrap());
        }
        encryptor.finish(&key, &blob_id).unwrap();

        assert_eq!(
            encrypted.len() as u64,
            encrypted_blob_size(bytes.len() as u64).unwrap()
        );
        fs::write(&destination, encrypted).unwrap();
        let restored = directory.path().join("restored.bin");
        let restored_hash =
            decrypt_blob_with_fingerprint(&destination, &restored, bytes.len() as u64, &key)
                .unwrap();
        verify_stream_blob_id(&key, &destination, &restored_hash, &blob_id).unwrap();
        assert_eq!(fs::read(restored).unwrap(), bytes);
    }

    #[test]
    fn streaming_blob_rejects_a_source_changed_after_identity_creation() {
        let key = [7_u8; 32];
        let original = b"original";
        let changed = b"modified";
        let nonce = new_stream_nonce();
        let blob_id =
            stream_blob_id(&key, &nonce, blake3::hash(original).to_hex().as_str()).unwrap();
        let mut encryptor = BlobStreamEncryptor::new(&key, &nonce, changed.len() as u64).unwrap();
        let _ = encryptor.encrypt_next(changed).unwrap();

        assert!(encryptor.finish(&key, &blob_id).is_err());
    }

    /// 独立按 ECOSBL01 格式组装 AEAD nonce，固定格式并避免复用被测 STREAM 写入路径。
    fn encode_blob_reference(plaintext: &[u8], key: &[u8; 32], stream_nonce: &[u8; 19]) -> Vec<u8> {
        let cipher = XChaCha20Poly1305::new_from_slice(key).unwrap();
        let mut encoded = b"ECOSBL01".to_vec();
        encoded.extend_from_slice(stream_nonce);
        let chunk_count = plaintext.len().div_ceil(64 * 1024).max(1);

        for index in 0..chunk_count {
            let mut nonce = [0_u8; 24];
            nonce[..19].copy_from_slice(stream_nonce);
            nonce[19..23].copy_from_slice(&(index as u32).to_be_bytes());
            nonce[23] = u8::from(index + 1 == chunk_count);
            let start = index * 64 * 1024;
            let end = plaintext.len().min(start + 64 * 1024);
            let ciphertext = cipher
                .encrypt(XNonce::from_slice(&nonce), &plaintext[start..end])
                .unwrap();
            encoded.extend_from_slice(&ciphertext);
        }

        encoded
    }

    #[test]
    fn blob_stream_preserves_wire_bytes_and_blob_id_at_chunk_boundaries() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        let destination = directory.path().join("destination.bin");
        let blob_root = directory.path().join("blobs");
        let key = [5_u8; 32];
        let nonce = [9_u8; 19];

        for size in [
            0,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            CHUNK_SIZE * 2,
            CHUNK_SIZE * 2 + 17,
        ] {
            let bytes = (0..size)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>();
            fs::write(&source, &bytes).unwrap();
            let reference = encode_blob_reference(&bytes, &key, &nonce);
            let expected_id = blake3::hash(&reference).to_hex().to_string();
            let expected_plaintext_hash = blake3::hash(&bytes).to_hex().to_string();

            let encrypted =
                encrypt_blob_with_nonce(&source, &blob_root, &key, nonce, || false).unwrap();
            let encrypted_path = Path::new(&encrypted.blob.encrypted_path);

            assert_eq!(fs::read(encrypted_path).unwrap(), reference, "size={size}");
            assert_eq!(encrypted.blob.blob_id, expected_id, "size={size}");
            assert_eq!(
                hash_file(encrypted_path).unwrap(),
                expected_id,
                "size={size}"
            );
            assert_eq!(encrypted.blob.size, reference.len() as u64, "size={size}");
            assert_eq!(
                encrypted.plaintext_hash, expected_plaintext_hash,
                "size={size}"
            );
            assert_eq!(
                encrypted_path,
                blob_path(&blob_root, &expected_id).unwrap(),
                "size={size}"
            );

            let decrypted_hash =
                decrypt_blob_with_fingerprint(encrypted_path, &destination, size as u64, &key)
                    .unwrap();

            assert_eq!(fs::read(&destination).unwrap(), bytes, "size={size}");
            assert_eq!(decrypted_hash, expected_plaintext_hash, "size={size}");
        }
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
                sync_key: "hash".into(),
                updated_at_ms: Some(2),
                source_revision: Some("revision".into()),
            },
            blobs: Vec::new(),
            source_app: None,
            content_identity: Some("a".repeat(64)),
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
        let anonymous = retention_key(&key, "hash");
        assert_eq!(event.retention_key.as_deref(), Some(anonymous.as_str()));
        assert_ne!(anonymous, retention_key(&[8; 32], "hash"));
        assert_ne!(anonymous, retention_key(&key, "other-hash"));
        let mut legacy = event.clone();
        legacy.retention_key = None;
        assert_eq!(decrypt_event(&key, &legacy).unwrap(), envelope);
        let mapping = retention_mapping(&key, &legacy).unwrap();
        assert_eq!(mapping.item_key, anonymous);
        let mut incorrect_identity = event.clone();
        incorrect_identity.retention_key = Some("0".repeat(64));
        assert!(decrypt_event(&key, &incorrect_identity).is_err());

        let mut tampered = event;
        tampered.origin_sequence = 2;
        assert!(decrypt_event(&key, &tampered).is_err());

        let mut tampered = encrypt_event(
            &key,
            "event-kind".into(),
            "device-123".into(),
            2,
            3,
            &envelope,
        )
        .unwrap();
        tampered.kind = SyncEventKind::ItemState;
        assert!(decrypt_sync_event(&key, &tampered).is_err());
    }

    #[test]
    fn item_state_round_trips_polymorphically() {
        let key = [7_u8; 32];
        let patch = SyncedItemStatePatch {
            sync_key: "hash-patch".into(),
            is_favorite: Some(true),
            is_pinned: Some(false),
            note: Some("note text".into()),
            note_changed: true,
            is_present: Some(false),
        };
        let event = encrypt_item_state(
            &key,
            "patch-event".into(),
            "device-patch".into(),
            1,
            12345,
            &patch,
        )
        .unwrap();
        match decrypt_sync_event(&key, &event).unwrap() {
            DecryptedSyncEvent::ItemState(decoded) => assert_eq!(decoded, patch),
            _ => panic!("expected synchronized item state"),
        }
        assert!(decrypt_event(&key, &event).is_err());
        let mapping = retention_mapping(&key, &event).unwrap();
        assert_eq!(mapping.item_key, retention_key(&key, &patch.sync_key));
        assert!(mapping.blob_ids.is_empty());
    }

    #[test]
    fn retention_mapping_recovers_content_and_optional_icon_references() {
        let key = [6; 32];
        let envelope = ClipboardEnvelope {
            version: 1,
            item: super::super::model::SyncedClipboardItem {
                kind: "image".into(),
                sub_kind: None,
                content: "".into(),
                search_text: None,
                summary: None,
                file_types: None,
                size: Some(32),
                width: Some(2),
                height: Some(2),
                is_sensitive: false,
                source_platform: "android".into(),
                created_at_ms: 1,
                sync_key: "protected-image".into(),
                updated_at_ms: None,
                source_revision: None,
            },
            blobs: vec![super::super::model::BlobManifest {
                blob_id: "b".repeat(64),
                name: "image.png".into(),
                original_size: 32,
                encrypted_size: 64,
                role: super::super::model::BlobRole::Image,
                file_index: None,
                is_directory_archive: false,
                stream_commitment: false,
            }],
            source_app: Some(super::super::model::SourceAppRef {
                version: 1,
                source_key: "app".into(),
                platform: "android".into(),
                display_name: "app".into(),
                icon: Some(super::super::model::SourceIconRef {
                    icon_hash: "hash".into(),
                    blob_id: "a".repeat(64),
                    original_size: 8,
                    encrypted_size: 40,
                }),
                accent_start: None,
                accent_end: None,
            }),
            content_identity: Some("c".repeat(64)),
        };
        let mut event =
            encrypt_event(&key, "image-event".into(), "device".into(), 1, 1, &envelope).unwrap();
        event.retention_key = None;
        let mapping = retention_mapping(&key, &event).unwrap();
        assert_eq!(mapping.event_id, event.event_id);
        assert_eq!(mapping.blob_ids, ["a".repeat(64), "b".repeat(64)]);
    }

    #[test]
    fn blob_path_rejects_traversal_and_short_ids() {
        let root = Path::new("blobs");
        assert!(blob_path(root, "../escape").is_err());
        assert!(blob_path(root, &"a".repeat(64)).is_ok());
    }
}
