//! Attachment I/O deadlines follow byte progress rather than total file duration.
use std::{
    future::Future,
    time::{Duration, Instant},
};

use anyhow::Result;
use std::path::Path;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::settings::Language;

#[derive(Debug, Clone, Copy, Default)]
pub(super) enum UploadPhase {
    #[default]
    OpenStream,
    SendRequest,
    WaitForReady,
    ReadFile,
    WriteData,
    Flush,
    WaitForConfirmation,
}

impl UploadPhase {
    fn key(self) -> crate::i18n::sync::Key {
        use crate::i18n::sync::Key;
        match self {
            Self::OpenStream => Key::OpenUpload,
            Self::SendRequest => Key::SendUploadRequest,
            Self::WaitForReady => Key::WaitForUpload,
            Self::ReadFile => Key::ReadUploadFile,
            Self::WriteData | Self::Flush => Key::WriteUpload,
            Self::WaitForConfirmation => Key::ConfirmUpload,
        }
    }
}

#[derive(Debug)]
struct UploadTimeout {
    phase: UploadPhase,
    language: Language,
}

impl std::fmt::Display for UploadTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(crate::i18n::sync::label(self.language, self.phase.key()))
    }
}

impl std::error::Error for UploadTimeout {}

/// A local disk stall must not invalidate a healthy shared network connection.
pub(super) fn is_network_timeout(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<UploadTimeout>()
        .is_some_and(|timeout| !matches!(timeout.phase, UploadPhase::ReadFile))
}

/// Bounds one operation and retains its phase for connection recovery and user feedback.
pub(super) async fn timed<F, T, E>(
    duration: Duration,
    phase: UploadPhase,
    language: Language,
    operation: F,
) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: Into<anyhow::Error>,
{
    tokio::time::timeout(duration, operation)
        .await
        .map_err(|_| anyhow::Error::new(UploadTimeout { phase, language }))?
        .map_err(Into::into)
}

pub(super) struct UploadProgress {
    pub phase: UploadPhase,
    pub written_bytes: u64,
    pub first_write_ms: Option<u128>,
    connection_id: usize,
    blob_ref: String,
    started_at: Instant,
    transfer: Option<super::progress::TransferHandle>,
}

pub(super) struct FileEncryptionSpec<'a> {
    pub expected_encrypted_size: u64,
    pub expected_blob_id: &'a str,
    pub key: [u8; 32],
    pub stream_nonce: &'a [u8],
    pub idle_timeout: Duration,
    pub language: Language,
}

impl UploadProgress {
    pub fn new(
        connection_id: usize,
        blob_ref: &str,
        transfer: Option<super::progress::TransferHandle>,
    ) -> Self {
        Self {
            phase: UploadPhase::OpenStream,
            written_bytes: 0,
            first_write_ms: None,
            connection_id,
            blob_ref: blob_ref.to_owned(),
            started_at: Instant::now(),
            transfer,
        }
    }

    pub fn canceled(&self) -> bool {
        self.transfer
            .as_ref()
            .is_some_and(super::progress::TransferHandle::canceled)
    }

    fn record_write(&mut self, size: usize) {
        if self.first_write_ms.is_none() {
            let elapsed = self.started_at.elapsed().as_millis();
            self.first_write_ms = Some(elapsed);
            log::info!(
                "sync blob first write: connectionId={} blobRef={} firstWriteMs={} atMs={}",
                self.connection_id,
                self.blob_ref,
                elapsed,
                chrono::Utc::now().timestamp_millis(),
            );
        }
        self.written_bytes += size as u64;
        if let Some(transfer) = &self.transfer {
            transfer.transferred(self.written_bytes);
        }
    }
}

/// Resets the write deadline on every accepted byte, including partial writes within one buffer.
pub(super) async fn copy<R, W>(
    reader: &mut R,
    writer: &mut W,
    idle_timeout: Duration,
    language: Language,
    progress: &mut UploadProgress,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        if progress.canceled() {
            anyhow::bail!("transfer canceled");
        }
        progress.phase = UploadPhase::ReadFile;
        let read = timed(
            idle_timeout,
            progress.phase,
            language,
            reader.read(&mut buffer),
        )
        .await?;
        if read == 0 {
            break;
        }
        let mut remaining = &buffer[..read];
        progress.phase = UploadPhase::WriteData;
        while !remaining.is_empty() {
            if progress.canceled() {
                anyhow::bail!("transfer canceled");
            }
            let written = timed(
                idle_timeout,
                progress.phase,
                language,
                writer.write(remaining),
            )
            .await?;
            if written == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
            }
            progress.record_write(written);
            remaining = &remaining[written..];
        }
    }
    progress.phase = UploadPhase::Flush;
    timed(idle_timeout, progress.phase, language, writer.flush()).await
}

/// Reads plaintext once, encrypts fixed-size authenticated chunks and writes them immediately.
pub(super) async fn encrypt_file<W>(
    source: &Path,
    writer: &mut W,
    spec: FileEncryptionSpec<'_>,
    progress: &mut UploadProgress,
) -> Result<String>
where
    W: AsyncWrite + Unpin,
{
    let FileEncryptionSpec {
        expected_encrypted_size,
        expected_blob_id,
        key,
        stream_nonce,
        idle_timeout,
        language,
    } = spec;
    let mut file = tokio::fs::File::open(source).await?;
    let original_size = file.metadata().await?.len();
    if super::crypto::encrypted_blob_size(original_size)? != expected_encrypted_size {
        anyhow::bail!("sync source size changed before upload");
    }
    let mut encryptor = super::crypto::BlobStreamEncryptor::new(&key, stream_nonce, original_size)?;
    write_encrypted_chunk(
        writer,
        &encryptor.header(),
        idle_timeout,
        language,
        progress,
    )
    .await?;

    let mut remaining = original_size;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        if progress.canceled() {
            anyhow::bail!("transfer canceled");
        }
        let length = if original_size == 0 {
            0
        } else {
            usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len())
        };
        progress.phase = UploadPhase::ReadFile;
        if length > 0 {
            timed(
                idle_timeout,
                progress.phase,
                language,
                file.read_exact(&mut buffer[..length]),
            )
            .await?;
        }
        let ciphertext = encryptor.encrypt_next(&buffer[..length])?;
        write_encrypted_chunk(writer, &ciphertext, idle_timeout, language, progress).await?;
        remaining = remaining.saturating_sub(length as u64);
        if original_size == 0 || remaining == 0 {
            break;
        }
    }
    progress.phase = UploadPhase::Flush;
    timed(idle_timeout, progress.phase, language, writer.flush()).await?;
    if progress.written_bytes != expected_encrypted_size {
        anyhow::bail!("encrypted sync blob size mismatch");
    }
    encryptor.finish(&key, expected_blob_id)
}

async fn write_encrypted_chunk<W>(
    writer: &mut W,
    mut bytes: &[u8],
    idle_timeout: Duration,
    language: Language,
    progress: &mut UploadProgress,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    progress.phase = UploadPhase::WriteData;
    while !bytes.is_empty() {
        if progress.canceled() {
            anyhow::bail!("transfer canceled");
        }
        let written = timed(idle_timeout, progress.phase, language, writer.write(bytes)).await?;
        if written == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
        }
        progress.record_write(written);
        bytes = &bytes[written..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn partial_writes_keep_a_slow_upload_alive_beyond_one_deadline() {
        let bytes = vec![7_u8; 24];
        let (mut writer, mut receiver) = tokio::io::duplex(1);
        let consumer = tokio::spawn(async move {
            let mut actual = Vec::new();
            let mut byte = [0_u8];
            while receiver.read(&mut byte).await.unwrap() > 0 {
                actual.push(byte[0]);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            actual
        });
        let mut progress = UploadProgress::new(0, "test", None);
        copy(
            &mut bytes.as_slice(),
            &mut writer,
            Duration::from_millis(200),
            Language::ZhCN,
            &mut progress,
        )
        .await
        .unwrap();
        drop(writer);
        assert_eq!(consumer.await.unwrap(), bytes);
        assert_eq!(progress.written_bytes, 24);
        assert!(progress.first_write_ms.is_some());
        assert!(progress.started_at.elapsed() > Duration::from_millis(200));
    }

    #[tokio::test]
    async fn file_encryption_streams_without_a_ciphertext_file() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        let encrypted = directory.path().join("encrypted.bin");
        let restored = directory.path().join("restored.bin");
        let bytes = (0..(64 * 1024 * 2 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&source, &bytes).unwrap();
        let key = [9_u8; 32];
        let nonce = super::super::crypto::new_stream_nonce();
        let plaintext_hash = blake3::hash(&bytes).to_hex().to_string();
        let blob_id = super::super::crypto::stream_blob_id(&key, &nonce, &plaintext_hash).unwrap();
        let encrypted_size = super::super::crypto::encrypted_blob_size(bytes.len() as u64).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let encrypted_path = encrypted.clone();
        let consumer = tokio::spawn(async move {
            let mut file = tokio::fs::File::create(&encrypted_path).await.unwrap();
            tokio::io::copy(&mut reader, &mut file).await.unwrap();
        });
        let mut progress = UploadProgress::new(0, "stream", None);

        let actual_hash = encrypt_file(
            &source,
            &mut writer,
            FileEncryptionSpec {
                expected_encrypted_size: encrypted_size,
                expected_blob_id: &blob_id,
                key,
                stream_nonce: &nonce,
                idle_timeout: Duration::from_secs(1),
                language: Language::ZhCN,
            },
            &mut progress,
        )
        .await
        .unwrap();
        drop(writer);
        consumer.await.unwrap();

        let restored_hash = super::super::crypto::decrypt_blob_with_fingerprint(
            &encrypted,
            &restored,
            bytes.len() as u64,
            &key,
        )
        .unwrap();
        super::super::crypto::verify_stream_blob_id(&key, &encrypted, &restored_hash, &blob_id)
            .unwrap();
        assert_eq!(actual_hash, plaintext_hash);
        assert_eq!(progress.written_bytes, encrypted_size);
        assert_eq!(std::fs::read(restored).unwrap(), bytes);
    }

    #[tokio::test]
    async fn stopped_network_write_reports_exact_partial_progress() {
        let bytes = [7_u8; 32];
        let (mut writer, _receiver) = tokio::io::duplex(2);
        let mut progress = UploadProgress::new(0, "test", None);
        let error = copy(
            &mut bytes.as_slice(),
            &mut writer,
            Duration::from_millis(20),
            Language::ZhCN,
            &mut progress,
        )
        .await
        .unwrap_err();
        assert!(is_network_timeout(&error));
        assert_eq!(error.to_string(), "附件上传长时间没有进展");
        assert_eq!(progress.written_bytes, 2);
        assert!(progress.first_write_ms.is_some());
    }

    #[tokio::test]
    async fn stalled_disk_read_does_not_invalidate_the_network() {
        let (_source, mut reader) = tokio::io::duplex(8);
        let (mut writer, _receiver) = tokio::io::duplex(8);
        let mut progress = UploadProgress::new(0, "test", None);
        let error = copy(
            &mut reader,
            &mut writer,
            Duration::from_millis(20),
            Language::EnUS,
            &mut progress,
        )
        .await
        .unwrap_err();
        assert!(!is_network_timeout(&error));
        assert_eq!(error.to_string(), "Timed out reading the local attachment");
        assert_eq!(progress.written_bytes, 0);
        assert_eq!(progress.first_write_ms, None);
    }

    #[tokio::test]
    async fn every_network_phase_has_a_classifiable_deadline() {
        for phase in [
            UploadPhase::OpenStream,
            UploadPhase::SendRequest,
            UploadPhase::WaitForReady,
            UploadPhase::Flush,
            UploadPhase::WaitForConfirmation,
        ] {
            let never = std::future::pending::<std::io::Result<()>>();
            let error = timed(Duration::from_millis(10), phase, Language::ZhCN, never)
                .await
                .unwrap_err();
            assert!(is_network_timeout(&error), "{phase:?}");
        }
        let error = anyhow::anyhow!("quota exceeded");
        assert!(!is_network_timeout(&error));
    }
}
