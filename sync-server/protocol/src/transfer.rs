//! Bounded multipart transport shared by the Hub and every client platform.
//! Encryption and final commit stay with the caller; this module only moves bytes.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinSet,
    time::MissedTickBehavior,
};

use crate::{Request, Response, read_frame, write_frame};

static ACTIVE_WIRE_SENDS: LazyLock<Mutex<HashMap<usize, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct ActiveWireSendGuard {
    connection_id: usize,
}

impl ActiveWireSendGuard {
    fn enter(connection_id: usize) -> Self {
        let mut counts = ACTIVE_WIRE_SENDS
            .lock()
            .expect("active wire sends poisoned");
        *counts.entry(connection_id).or_insert(0) += 1;
        Self { connection_id }
    }

    fn count(connection_id: usize) -> usize {
        ACTIVE_WIRE_SENDS
            .lock()
            .expect("active wire sends poisoned")
            .get(&connection_id)
            .copied()
            .unwrap_or(1)
            .max(1)
    }
}

impl Drop for ActiveWireSendGuard {
    fn drop(&mut self) {
        let mut counts = ACTIVE_WIRE_SENDS
            .lock()
            .expect("active wire sends poisoned");
        if let Some(count) = counts.get_mut(&self.connection_id) {
            if *count <= 1 {
                counts.remove(&self.connection_id);
            } else {
                *count -= 1;
            }
        }
    }
}

#[derive(Default)]
struct WireTransferProgress {
    buffered_bytes: u64,
    confirmed_bytes: u64,
    wire_bytes: u64,
    last_reported: u64,
    prev_tx: u64,
    prev_lost: u64,
    carry_lost: u64,
}

impl WireTransferProgress {
    fn new(tx_bytes: u64, lost_bytes: u64) -> Self {
        Self {
            prev_tx: tx_bytes,
            prev_lost: lost_bytes,
            ..Self::default()
        }
    }

    fn record_buffered(&mut self, chunk: u64) {
        self.buffered_bytes = self.buffered_bytes.saturating_add(chunk);
    }

    fn record_wire_sample(
        &mut self,
        tx_bytes: u64,
        lost_bytes: u64,
        active_share: usize,
        total_size: u64,
    ) -> Option<u64> {
        let delta_tx = tx_bytes.saturating_sub(self.prev_tx);
        let delta_lost = lost_bytes.saturating_sub(self.prev_lost);
        self.prev_tx = tx_bytes;
        self.prev_lost = lost_bytes;
        self.carry_lost = self.carry_lost.saturating_add(delta_lost);
        let net_tx = delta_tx.saturating_sub(self.carry_lost);
        self.carry_lost = self.carry_lost.saturating_sub(delta_tx);

        let share = u64::try_from(active_share.max(1)).unwrap_or(1);
        // Compensate ~6% QUIC/AEAD framing overhead and divide fairly across concurrent uploads on the same connection.
        let payload_delta = (net_tx.saturating_mul(94) / 100) / share;
        let ceiling = self.buffered_bytes.min(total_size.saturating_sub(1));
        self.wire_bytes = self
            .wire_bytes
            .saturating_add(payload_delta)
            .max(self.confirmed_bytes)
            .min(ceiling);
        if self.wire_bytes > self.last_reported {
            self.last_reported = self.wire_bytes;
            Some(self.wire_bytes)
        } else {
            None
        }
    }

    fn record_confirmed(
        &mut self,
        part_bytes: u64,
        all_done: bool,
        total_size: u64,
    ) -> Option<u64> {
        self.confirmed_bytes = self.confirmed_bytes.saturating_add(part_bytes);
        if self.wire_bytes < self.confirmed_bytes {
            self.wire_bytes = self.confirmed_bytes;
        }
        let reported = if all_done {
            self.confirmed_bytes
        } else {
            self.wire_bytes
                .max(self.confirmed_bytes)
                .min(total_size.saturating_sub(1))
        };
        if reported > self.last_reported {
            self.last_reported = reported;
            Some(reported)
        } else {
            None
        }
    }
}

pub const PARALLEL_PARTS: usize = 4;
pub const ATTACHMENTS_PER_DIRECTION: usize = 2;
/// Per-stream QUIC receive window sized for high-throughput LAN/WAN multipart transfers.
pub const STREAM_RECEIVE_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
/// Connection-level QUIC send window sized to keep all parallel 4-MiB parts in flight.
pub const SEND_WINDOW_BYTES: u64 = 32 * 1024 * 1024;
// 64 authenticated 64-KiB encryption blocks. The first part also contains the header.
const PART_BYTES: u64 = 4 * 1024 * 1024 + 64 * 16;
const HEADER_BYTES: u64 = 27;
const IO_BLOCK_BYTES: usize = 256 * 1024;
const RECEIVE_BUFFER_PERMITS: usize = PARALLEL_PARTS + 2;
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

/// Reads from a local source (`duplex` pipe or local file) without imposing a network timeout;
/// upstream producers own local disk I/O deadlines.
async fn read_source_bytes<R: AsyncRead + Unpin>(reader: &mut R, bytes: &mut [u8]) -> Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let count = reader.read(&mut bytes[offset..]).await?;
        if count == 0 {
            bail!("transfer part ended early");
        }
        offset += count;
    }
    Ok(())
}

/// Deadlines measure idle time, including slow connections that take minutes per part.
async fn read_bytes<R: AsyncRead + Unpin>(
    reader: &mut R,
    bytes: &mut [u8],
    on_read: Option<&AtomicU64>,
) -> Result<()> {
    for block in bytes.chunks_mut(IO_BLOCK_BYTES) {
        let mut offset = 0;
        while offset < block.len() {
            let count =
                tokio::time::timeout(IDLE_TIMEOUT, reader.read(&mut block[offset..])).await??;
            if count == 0 {
                bail!("transfer part ended early");
            }
            offset += count;
            if let Some(counter) = on_read {
                counter.fetch_add(count as u64, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

async fn write_bytes<W: AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    on_write: Option<&mpsc::UnboundedSender<u64>>,
    timeout: Duration,
) -> Result<()> {
    for block in bytes.chunks(IO_BLOCK_BYTES) {
        let mut offset = 0;
        while offset < block.len() {
            let count = tokio::time::timeout(timeout, writer.write(&block[offset..])).await??;
            if count == 0 {
                bail!("transfer part write stopped");
            }
            offset += count;
            if let Some(tx) = on_write {
                let _ = tx.send(count as u64);
            }
        }
    }
    Ok(())
}

/// Consumes FIN before releasing a successful stream; dropping it earlier sends STOP_SENDING.
async fn finish_read(recv: &mut RecvStream, timeout: Duration) -> Result<()> {
    let mut trailing = [0u8; 1];
    let count = tokio::time::timeout(timeout, AsyncReadExt::read(recv, &mut trailing))
        .await
        .context("wait for transfer part finish timeout")?
        .context("read transfer part finish")?;
    if count != 0 {
        bail!("unexpected trailing transfer part data");
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
    queues: [mpsc::Sender<PartRequest>; PARALLEL_PARTS],
}

/// Registry belongs to one authenticated connection; identifiers never cross connections.
#[derive(Default)]
pub struct Transfers(Mutex<HashMap<u64, Arc<Session>>>);

pub struct RegisteredTransfer {
    registry: Arc<Transfers>,
    id: u64,
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
    pub fn register(self: &Arc<Self>, id: u64, size: u64) -> Result<RegisteredTransfer> {
        let mut registry = self.0.lock().expect("transfer registry poisoned");
        if registry.contains_key(&id) {
            bail!("duplicate transfer identifier");
        }
        let channels: Vec<_> = (0..PARALLEL_PARTS).map(|_| mpsc::channel(2)).collect();
        let (senders, receivers): (Vec<_>, Vec<_>) = channels.into_iter().unzip();
        let session = Arc::new(Session {
            size,
            queues: senders.try_into().expect("part channels"),
        });
        registry.insert(id, session);
        Ok(RegisteredTransfer {
            registry: self.clone(),
            id,
            receivers: receivers
                .into_iter()
                .map(|rx| Arc::new(tokio::sync::Mutex::new(rx)))
                .collect::<Vec<_>>()
                .try_into()
                .expect("part receivers"),
        })
    }

    /// Parts use their parent's admission slot and cannot occupy control-request permits.
    pub async fn route(
        &self,
        id: u64,
        index: u64,
        send: SendStream,
        recv: RecvStream,
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
        session.queues[lane]
            .try_send(PartRequest { index, send, recv })
            .map_err(|_| anyhow::anyhow!("transfer part is already pending"))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct PartPeer {
    peer: Peer,
    idle_timeout: Duration,
}

#[derive(Clone)]
enum Peer {
    Client {
        connection: Connection,
        transfer_id: u64,
        priority: i32,
    },
    Server {
        receivers: [Arc<tokio::sync::Mutex<mpsc::Receiver<PartRequest>>>; PARALLEL_PARTS],
        connection: Option<Connection>,
    },
}

impl RegisteredTransfer {
    pub fn peer(&self) -> PartPeer {
        PartPeer {
            peer: Peer::Server {
                receivers: self.receivers.clone(),
                connection: None,
            },
            idle_timeout: IDLE_TIMEOUT,
        }
    }

    pub fn peer_with_connection(&self, connection: Connection) -> PartPeer {
        PartPeer {
            peer: Peer::Server {
                receivers: self.receivers.clone(),
                connection: Some(connection),
            },
            idle_timeout: IDLE_TIMEOUT,
        }
    }
}

impl PartPeer {
    pub fn client(connection: Connection, transfer_id: u64) -> Self {
        Self {
            peer: Peer::Client {
                connection,
                transfer_id,
                priority: -1,
            },
            idle_timeout: IDLE_TIMEOUT,
        }
    }

    /// Sets the QUIC stream send priority for outbound part streams (higher sends first).
    pub fn with_priority(mut self, priority: i32) -> Self {
        if let Peer::Client {
            priority: current, ..
        } = &mut self.peer
        {
            *current = priority;
        }
        self
    }

    /// Overrides the per-block network idle timeout for part stream writes.
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    fn priority(&self) -> i32 {
        match &self.peer {
            Peer::Client { priority, .. } => *priority,
            Peer::Server { .. } => -1,
        }
    }

    async fn stream(&self, index: u64) -> Result<(SendStream, RecvStream)> {
        match &self.peer {
            Peer::Client {
                connection,
                transfer_id,
                ..
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
                let request = receiver.recv().await.context("transfer ended")?;
                if request.index != index {
                    bail!("unexpected transfer part order");
                }
                Ok((request.send, request.recv))
            }
        }
    }

    async fn send_part(
        &self,
        index: u64,
        data: &[u8],
        on_write: Option<mpsc::UnboundedSender<u64>>,
    ) -> Result<()> {
        let io_timeout = self.idle_timeout;
        let ack_timeout = self.idle_timeout.max(Duration::from_secs(30));
        let (mut send, mut recv) = tokio::time::timeout(io_timeout, self.stream(index)).await??;
        // Leave control frames (priority 0) ahead of attachment bytes, while letting realtime
        // attachments preempt historical backfill and optional source icons.
        send.set_priority(self.priority())?;
        write_bytes(&mut send, data, on_write.as_ref(), io_timeout).await?;
        send.finish()?;
        // Peer receivers acknowledge Response::BlobStored right after writing the part and
        // may drop RecvStream before consuming FIN, which emits a benign STOP_SENDING(0).
        match tokio::time::timeout(ack_timeout, read_frame(&mut recv)).await?? {
            Response::BlobStored => {
                let _ = finish_read(&mut recv, io_timeout).await;
                Ok(())
            }
            _ => bail!("invalid part acknowledgement"),
        }
    }

    async fn receive_part(
        &self,
        index: u64,
        size: usize,
        on_read: Option<Arc<AtomicU64>>,
    ) -> Result<(Vec<u8>, SendStream)> {
        let (send, mut recv) = tokio::time::timeout(IDLE_TIMEOUT, self.stream(index)).await??;
        let mut data = vec![0u8; size];
        read_bytes(&mut recv, &mut data, on_read.as_deref()).await?;
        finish_read(&mut recv, IDLE_TIMEOUT).await?;
        Ok((data, send))
    }
}

#[derive(Default)]
struct WireReceiveProgress {
    completed_bytes: u64,
    last_reported: u64,
}

impl WireReceiveProgress {
    fn sample(&mut self, wire_received: u64, total_size: u64) -> Option<u64> {
        let reported = wire_received
            .max(self.completed_bytes)
            .min(total_size.saturating_sub(1));
        if reported > self.last_reported {
            self.last_reported = reported;
            Some(reported)
        } else {
            None
        }
    }

    fn record_written(&mut self, written: u64, total_size: u64) -> Option<u64> {
        self.completed_bytes = self.completed_bytes.saturating_add(written);
        let reported = self.completed_bytes.min(total_size.saturating_sub(1));
        if reported > self.last_reported {
            self.last_reported = reported;
            Some(reported)
        } else {
            None
        }
    }
}

/// Reads once in order, keeping up to four encrypted parts in flight while prefetching the next.
pub async fn send<R: AsyncRead + Unpin>(
    reader: &mut R,
    peer: PartPeer,
    size: u64,
    mut progress: impl FnMut(u64),
) -> Result<()> {
    let total_parts = part_count(size);
    let mut tasks = JoinSet::new();
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<u64>();
    let wire_peer = match &peer.peer {
        Peer::Client { connection, .. }
        | Peer::Server {
            connection: Some(connection),
            ..
        } => Some((
            connection.clone(),
            ActiveWireSendGuard::enter(connection.stable_id()),
        )),
        Peer::Server {
            connection: None, ..
        } => None,
    };
    let mut tracker = wire_peer
        .as_ref()
        .map(|(conn, _)| {
            let stats = conn.stats();
            WireTransferProgress::new(stats.udp_tx.bytes, stats.lost_bytes)
        })
        .unwrap_or_default();
    let mut wire_tick = tokio::time::interval(Duration::from_millis(120));
    wire_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    wire_tick.tick().await;
    let mut index = 0;
    let mut prefetched: Option<(u64, Vec<u8>)> = None;
    loop {
        while tasks.len() < PARALLEL_PARTS {
            let (part, data) = if let Some(ready) = prefetched.take() {
                ready
            } else if index < total_parts {
                let mut data = vec![0u8; part_size(size, index)];
                read_source_bytes(reader, &mut data).await?;
                let part = index;
                index += 1;
                (part, data)
            } else {
                break;
            };
            let peer = peer.clone();
            let tx = progress_tx.clone();
            tasks.spawn(async move {
                peer.send_part(part, &data, Some(tx)).await?;
                Ok::<_, anyhow::Error>(data.len() as u64)
            });
        }
        // Overlap reading/encrypting the next 4-MiB part with the four QUIC part transfers already in flight.
        if prefetched.is_none() && index < total_parts {
            let mut data = vec![0u8; part_size(size, index)];
            read_source_bytes(reader, &mut data).await?;
            prefetched = Some((index, data));
            index += 1;
        }
        if tasks.is_empty() {
            break;
        }
        loop {
            tokio::select! {
                biased;
                done = tasks.join_next() => {
                    let Some(done) = done else {
                        break;
                    };
                    let part_len = done??;
                    let all_done = tasks.is_empty() && index >= total_parts && prefetched.is_none();
                    if let Some(reported) = tracker.record_confirmed(part_len, all_done, size) {
                        progress(reported);
                    }
                    break;
                }
                chunk = progress_rx.recv() => {
                    if let Some(chunk) = chunk {
                        tracker.record_buffered(chunk);
                    }
                }
                _ = wire_tick.tick(), if wire_peer.is_some() => {
                    if let Some((conn, _)) = &wire_peer {
                        let stats = conn.stats();
                        let share = ActiveWireSendGuard::count(conn.stable_id());
                        if let Some(reported) = tracker.record_wire_sample(
                            stats.udp_tx.bytes,
                            stats.lost_bytes,
                            share,
                            size,
                        ) {
                            progress(reported);
                        }
                    }
                }
            }
        }
    }
    if tracker.confirmed_bytes > tracker.last_reported {
        progress(tracker.confirmed_bytes);
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
    let buffer_permits = Arc::new(Semaphore::new(RECEIVE_BUFFER_PERMITS));
    let (tx, mut rx) = mpsc::channel(RECEIVE_BUFFER_PERMITS);
    let wire_received = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    for lane in 0..PARALLEL_PARTS {
        let peer = peer.clone();
        let tx = tx.clone();
        let buffer_permits = buffer_permits.clone();
        let wire_received = wire_received.clone();
        tasks.spawn(async move {
            let mut index = lane as u64;
            while index < part_count(size) {
                let permit = buffer_permits
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow::anyhow!("transfer canceled"))?;
                let (data, mut send) = peer
                    .receive_part(index, part_size(size, index), Some(wire_received.clone()))
                    .await?;
                // Memory stays strictly bounded by `buffer_permits`; releasing part credit now
                // lets the sender pipeline the next part while decryption and disk I/O run.
                write_frame(&mut send, &Response::BlobStored).await?;
                send.finish()?;
                tx.send((index, data, permit))
                    .await
                    .map_err(|_| anyhow::anyhow!("transfer canceled"))?;
                index += PARALLEL_PARTS as u64;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    drop(tx);
    let mut pending = std::collections::BTreeMap::new();
    let mut next = 0;
    let mut tracker = WireReceiveProgress::default();
    let mut receive_tick = tokio::time::interval(Duration::from_millis(120));
    receive_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    receive_tick.tick().await;
    while next < part_count(size) {
        tokio::select! {
            biased;
            value = rx.recv() => {
                let (index, data, permit) = value.context("transfer ended early")?;
                pending.insert(index, (data, permit));
                while let Some((data, permit)) = pending.remove(&next) {
                    writer.write_all(&data).await?;
                    let written = data.len() as u64;
                    drop(permit);
                    next += 1;
                    if let Some(reported) = tracker.record_written(written, size) {
                        progress(reported);
                    }
                }
            }
            done = tasks.join_next(), if !tasks.is_empty() => { done.context("transfer ended")???; }
            _ = receive_tick.tick() => {
                let wire = wire_received.load(Ordering::Relaxed);
                if let Some(reported) = tracker.sample(wire, size) {
                    progress(reported);
                }
            }
        }
    }
    while let Some(done) = tasks.join_next().await {
        done??;
    }
    writer.shutdown().await?;
    if tracker.completed_bytes > tracker.last_reported {
        progress(tracker.completed_bytes);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{WireReceiveProgress, WireTransferProgress};

    #[test]
    fn wire_receive_progress_advances_linearly_while_waiting_for_ordered_parts() {
        let total_size = 10 * 1024 * 1024;
        let mut tracker = WireReceiveProgress::default();

        // Parallel lanes receive chunks before part 0 flushes -> linear samples without jumping to 100%.
        let first = tracker.sample(2 * 1024 * 1024, total_size).unwrap();
        assert_eq!(first, 2 * 1024 * 1024);
        let second = tracker.sample(6 * 1024 * 1024, total_size).unwrap();
        assert_eq!(second, 6 * 1024 * 1024);

        // Flushing ordered parts must never regress or prematurely hit 100% before shutdown.
        assert_eq!(tracker.record_written(4 * 1024 * 1024, total_size), None);
        assert_eq!(tracker.sample(total_size, total_size), Some(total_size - 1));
    }

    #[test]
    fn wire_progress_tracks_real_tx_linearly_and_subtracts_retransmits() {
        let total_size = 10 * 1024 * 1024;
        let mut tracker = WireTransferProgress::new(1_000, 100);
        // Simulate QUIC send-buffer burst of all 10 MiB immediately on stream write.
        tracker.record_buffered(total_size);
        assert_eq!(tracker.last_reported, 0);

        // Actual wire samples advance linearly and never jump straight to 99%.
        let first = tracker
            .record_wire_sample(1_000 + 2 * 1024 * 1024, 100, 1, total_size)
            .unwrap();
        assert!(first > 1_800_000 && first < 2 * 1024 * 1024);

        // Packet loss + retransmission of 1 MiB must not advance reported progress.
        assert_eq!(
            tracker.record_wire_sample(1_000 + 2 * 1024 * 1024, 100 + 1024 * 1024, 1, total_size),
            None
        );
        assert_eq!(
            tracker.record_wire_sample(1_000 + 3 * 1024 * 1024, 100 + 1024 * 1024, 1, total_size),
            None
        );

        // Subsequent non-lost wire transmission advances proportionally, and final ACK finishes at 100%.
        let second = tracker
            .record_wire_sample(1_000 + 5 * 1024 * 1024, 100 + 1024 * 1024, 1, total_size)
            .unwrap();
        assert!(second > first && second < 5 * 1024 * 1024);
        assert_eq!(
            tracker.record_confirmed(total_size, true, total_size),
            Some(total_size)
        );
    }
}
