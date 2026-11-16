//! Bounded multipart transport shared by the Hub and every client platform.
//! Encryption and final commit stay with the caller; this module only moves bytes.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinSet,
};

use crate::{Request, Response, read_frame, write_frame};

pub const PARALLEL_PARTS: usize = 4;
pub const ATTACHMENTS_PER_DIRECTION: usize = 2;
// 64 authenticated 64-KiB encryption blocks. The first part also contains the header.
const PART_BYTES: u64 = 4 * 1024 * 1024 + 64 * 16;
const HEADER_BYTES: u64 = 27;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Dropping the parent response cancels its part workers and releases their bounded buffers.
pub async fn until_stopped<T>(
    control: &SendStream,
    work: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        result = work => result,
        _ = control.stopped() => bail!("transfer canceled"),
    }
}

fn part_offset(index: u64) -> u64 {
    if index == 0 {
        0
    } else {
        HEADER_BYTES + index * PART_BYTES
    }
}

fn part_size(size: u64, index: u64) -> usize {
    let capacity = PART_BYTES + if index == 0 { HEADER_BYTES } else { 0 };
    size.saturating_sub(part_offset(index)).min(capacity) as usize
}

fn part_count(size: u64) -> u64 {
    size.saturating_sub(HEADER_BYTES)
        .div_ceil(PART_BYTES)
        .max(1)
}

/// Deadlines measure idle time, including slow connections that take minutes per part.
async fn read_bytes<R: AsyncRead + Unpin>(reader: &mut R, bytes: &mut [u8]) -> Result<()> {
    for block in bytes.chunks_mut(64 * 1024) {
        let mut offset = 0;
        while offset < block.len() {
            let count =
                tokio::time::timeout(IDLE_TIMEOUT, reader.read(&mut block[offset..])).await??;
            if count == 0 {
                bail!("transfer part ended early");
            }
            offset += count;
        }
    }
    Ok(())
}

async fn write_bytes<W: AsyncWrite + Unpin>(writer: &mut W, bytes: &[u8]) -> Result<()> {
    for block in bytes.chunks(64 * 1024) {
        let mut offset = 0;
        while offset < block.len() {
            let count =
                tokio::time::timeout(IDLE_TIMEOUT, writer.write(&block[offset..])).await??;
            if count == 0 {
                bail!("transfer part write stopped");
            }
            offset += count;
        }
    }
    Ok(())
}

/// Attachment admission is independent for each direction and content category.
pub struct TransferLimits([Arc<Semaphore>; 4]);

impl Default for TransferLimits {
    fn default() -> Self {
        Self(std::array::from_fn(|_| {
            Arc::new(Semaphore::new(ATTACHMENTS_PER_DIRECTION))
        }))
    }
}

impl TransferLimits {
    pub async fn acquire(&self, files: bool, sending: bool) -> Result<OwnedSemaphorePermit> {
        Ok(self.0[usize::from(files) * 2 + usize::from(sending)]
            .clone()
            .acquire_owned()
            .await?)
    }
}

struct PartRequest {
    index: u64,
    send: SendStream,
    recv: RecvStream,
}

struct Session {
    size: u64,
    receiving: bool,
    queues: [mpsc::Sender<PartRequest>; PARALLEL_PARTS],
    completed: [AtomicU64; PARALLEL_PARTS],
}

/// Registry belongs to one authenticated connection; identifiers never cross connections.
#[derive(Default)]
pub struct Transfers(Mutex<HashMap<u64, Arc<Session>>>);

pub struct RegisteredTransfer {
    registry: Arc<Transfers>,
    id: u64,
    session: Arc<Session>,
    receivers: [Arc<tokio::sync::Mutex<mpsc::Receiver<PartRequest>>>; PARALLEL_PARTS],
}

impl Drop for RegisteredTransfer {
    fn drop(&mut self) {
        self.registry
            .0
            .lock()
            .expect("transfer registry poisoned")
            .remove(&self.id);
    }
}

impl Transfers {
    /// The parent request authenticates and reserves storage before publishing this session.
    pub fn register(
        self: &Arc<Self>,
        id: u64,
        size: u64,
        receiving: bool,
    ) -> Result<RegisteredTransfer> {
        let mut registry = self.0.lock().expect("transfer registry poisoned");
        if registry.contains_key(&id) {
            bail!("duplicate transfer identifier");
        }
        let channels: Vec<_> = (0..PARALLEL_PARTS).map(|_| mpsc::channel(2)).collect();
        let (senders, receivers): (Vec<_>, Vec<_>) = channels.into_iter().unzip();
        let session = Arc::new(Session {
            size,
            receiving,
            queues: senders.try_into().ok().expect("part channels"),
            completed: std::array::from_fn(|_| AtomicU64::new(0)),
        });
        registry.insert(id, session.clone());
        Ok(RegisteredTransfer {
            registry: self.clone(),
            id,
            session,
            receivers: receivers
                .into_iter()
                .map(|rx| Arc::new(tokio::sync::Mutex::new(rx)))
                .collect::<Vec<_>>()
                .try_into()
                .ok()
                .expect("part receivers"),
        })
    }

    /// Parts use their parent's admission slot and cannot occupy control-request permits.
    pub async fn route(
        &self,
        id: u64,
        index: u64,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<()> {
        let session = self
            .0
            .lock()
            .expect("transfer registry poisoned")
            .get(&id)
            .cloned()
            .context("transfer is no longer active")?;
        if index >= part_count(session.size) {
            bail!("invalid transfer part");
        }
        let lane = index as usize % PARALLEL_PARTS;
        if session.receiving && session.completed[lane].load(Ordering::Acquire) > index {
            // A lost acknowledgement may retry a committed part; never append it twice.
            let mut remaining = part_size(session.size, index);
            let mut buffer = [0u8; 64 * 1024];
            while remaining > 0 {
                let count = remaining.min(buffer.len());
                read_bytes(&mut recv, &mut buffer[..count]).await?;
                remaining -= count;
            }
            write_frame(&mut send, &Response::BlobStored).await?;
            send.finish()?;
            return Ok(());
        }
        session.queues[lane]
            .try_send(PartRequest { index, send, recv })
            .map_err(|_| anyhow::anyhow!("transfer part is already pending"))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct PartPeer(Peer);

#[derive(Clone)]
enum Peer {
    Client {
        connection: Connection,
        transfer_id: u64,
    },
    Server {
        session: Arc<Session>,
        receivers: [Arc<tokio::sync::Mutex<mpsc::Receiver<PartRequest>>>; PARALLEL_PARTS],
    },
}

impl RegisteredTransfer {
    pub fn peer(&self) -> PartPeer {
        PartPeer(Peer::Server {
            session: self.session.clone(),
            receivers: self.receivers.clone(),
        })
    }
}

impl PartPeer {
    pub fn client(connection: Connection, transfer_id: u64) -> Self {
        Self(Peer::Client {
            connection,
            transfer_id,
        })
    }
    async fn stream(&self, index: u64) -> Result<(SendStream, RecvStream)> {
        match &self.0 {
            Peer::Client {
                connection,
                transfer_id,
            } => {
                let (mut send, recv) = connection.open_bi().await?;
                write_frame(
                    &mut send,
                    &Request::BlobPart {
                        transfer_id: *transfer_id,
                        index,
                    },
                )
                .await?;
                Ok((send, recv))
            }
            Peer::Server { receivers, .. } => {
                let mut receiver = receivers[index as usize % PARALLEL_PARTS].lock().await;
                loop {
                    let request = receiver.recv().await.context("transfer ended")?;
                    if request.index == index {
                        return Ok((request.send, request.recv));
                    }
                    // Only retries of an older stream may still be queued here.
                    if request.index > index {
                        bail!("unexpected transfer part order");
                    }
                }
            }
        }
    }

    async fn send_part(&self, index: u64, data: &[u8]) -> Result<()> {
        let mut failure = None;
        for _ in 0..3 {
            let result = async {
                let (mut send, mut recv) =
                    tokio::time::timeout(IDLE_TIMEOUT, self.stream(index)).await??;
                write_bytes(&mut send, data).await?;
                send.finish()?;
                match tokio::time::timeout(IDLE_TIMEOUT, read_frame(&mut recv)).await?? {
                    Response::BlobStored => Ok(()),
                    _ => bail!("invalid part acknowledgement"),
                }
            }
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => failure = Some(error),
            }
        }
        Err(failure.expect("part attempt"))
    }

    async fn receive_part(&self, index: u64, size: usize) -> Result<(Vec<u8>, SendStream)> {
        let mut failure = None;
        for _ in 0..3 {
            let result = async {
                let (send, mut recv) =
                    tokio::time::timeout(IDLE_TIMEOUT, self.stream(index)).await??;
                let mut data = vec![0u8; size];
                read_bytes(&mut recv, &mut data).await?;
                Ok((data, send))
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) => failure = Some(error),
            }
        }
        Err(failure.expect("part attempt"))
    }
}

/// Reads once in order and keeps at most four encrypted parts in flight.
pub async fn send<R: AsyncRead + Unpin>(
    reader: &mut R,
    peer: PartPeer,
    size: u64,
    mut progress: impl FnMut(u64),
) -> Result<()> {
    let mut tasks = JoinSet::new();
    let mut index = 0;
    let mut completed = 0;
    loop {
        while index < part_count(size) && tasks.len() < PARALLEL_PARTS {
            let mut data = vec![0u8; part_size(size, index)];
            read_bytes(reader, &mut data).await?;
            let peer = peer.clone();
            let part = index;
            tasks.spawn(async move {
                peer.send_part(part, &data).await?;
                Ok::<_, anyhow::Error>(data.len() as u64)
            });
            index += 1;
        }
        let Some(done) = tasks.join_next().await else {
            break;
        };
        completed += done??;
        progress(completed);
    }
    Ok(())
}

/// Receives parts concurrently, then writes once in order to native files or Android SAF.
pub async fn receive<W: AsyncWrite + Unpin>(
    writer: &mut W,
    peer: PartPeer,
    size: u64,
    mut progress: impl FnMut(u64),
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(PARALLEL_PARTS);
    let mut tasks = JoinSet::new();
    for lane in 0..PARALLEL_PARTS {
        let peer = peer.clone();
        let tx = tx.clone();
        tasks.spawn(async move {
            let mut index = lane as u64;
            while index < part_count(size) {
                let (data, mut send) = peer.receive_part(index, part_size(size, index)).await?;
                let (written, wait_written) = oneshot::channel();
                tx.send((index, data, written))
                    .await
                    .map_err(|_| anyhow::anyhow!("transfer canceled"))?;
                wait_written.await.context("transfer canceled")?;
                if let Peer::Server { session, .. } = &peer.0 {
                    session.completed[lane].store(index + 1, Ordering::Release);
                }
                // Retransmission of a lost ACK is handled by the server registry.
                write_frame(&mut send, &Response::BlobStored).await?;
                send.finish()?;
                index += PARALLEL_PARTS as u64;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    drop(tx);
    let mut pending = std::collections::BTreeMap::new();
    let mut next = 0;
    let mut completed = 0;
    while next < part_count(size) {
        tokio::select! {
            value = rx.recv() => {
                let (index, data, written) = value.context("transfer ended early")?;
                pending.insert(index, (data, written));
                while let Some((data, written)) = pending.remove(&next) {
                    writer.write_all(&data).await?;
                    completed += data.len() as u64;
                    progress(completed);
                    let _ = written.send(());
                    next += 1;
                }
            }
            done = tasks.join_next(), if !tasks.is_empty() => { done.context("transfer ended")???; }
        }
    }
    while let Some(done) = tasks.join_next().await {
        done??;
    }
    writer.shutdown().await?;
    Ok(())
}
