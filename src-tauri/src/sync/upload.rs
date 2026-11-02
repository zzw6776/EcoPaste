//! Attachment I/O deadlines follow byte progress rather than total file duration.
use std::{
    future::Future,
    time::{Duration, Instant},
};

use anyhow::Result;
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
}

impl UploadProgress {
    pub fn new(connection_id: usize, blob_ref: &str) -> Self {
        Self {
            phase: UploadPhase::OpenStream,
            written_bytes: 0,
            first_write_ms: None,
            connection_id,
            blob_ref: blob_ref.to_owned(),
            started_at: Instant::now(),
        }
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
        let mut progress = UploadProgress::new(0, "test");
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
    async fn stopped_network_write_reports_exact_partial_progress() {
        let bytes = [7_u8; 32];
        let (mut writer, _receiver) = tokio::io::duplex(2);
        let mut progress = UploadProgress::new(0, "test");
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
        let mut progress = UploadProgress::new(0, "test");
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
