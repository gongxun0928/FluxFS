//! Synchronous `ChunkStore` facade over dedicated tonic ChunkWorker processes.
//!
//! FUSE callbacks are synchronous. A dedicated RPC thread owns a Tokio runtime
//! so callers never nest/block the Meta/FUSE runtime. The initial v0 placement
//! fixes the first `required` endpoints as the authoritative replica set; extra
//! endpoints are repair spares.

use crate::ChunkStore;
use fluxfs_proto::chunk::v1::{
    ContainsChunkRequest, DeleteChunkRequest, GetChunkRequest, HealthRequest, ListChunksRequest,
    PutChunkRequest,
};
use fluxfs_proto::ChunkWorkerClient;
use fluxfs_types::{ChunkId, ChunkPage, FluxError, Result, WorkerTargetId, CHUNK_SIZE};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

const MAX_CHUNK_RPC_MESSAGE: usize = CHUNK_SIZE as usize + 64 * 1024;
pub const DEFAULT_MAX_PENDING_CHUNK_OPS: usize = 64;
/// Bounded inventory page for repair/scrub (replaces `limit=u32::MAX` sweeps).
pub const REPAIR_PAGE_SIZE: usize = 256;
const BACKGROUND_REPAIR_IDLE: Duration = Duration::from_secs(2);

type RpcReply<T> = mpsc::Sender<Result<T>>;

enum Command {
    Put {
        data: Vec<u8>,
        reply: RpcReply<ChunkId>,
    },
    Get {
        id: ChunkId,
        reply: RpcReply<Vec<u8>>,
    },
    Contains {
        id: ChunkId,
        reply: RpcReply<bool>,
    },
    AvailableWorkers {
        reply: RpcReply<Vec<u64>>,
    },
    Repair {
        reply: RpcReply<RepairReport>,
    },
    /// One bounded scrub page (cursor advanced inside the RPC thread).
    RepairPass {
        limit: usize,
        reply: RpcReply<RepairReport>,
    },
    ListChunksPage {
        cursor: Option<ChunkId>,
        limit: usize,
        reply: RpcReply<ChunkPage>,
    },
    TargetCount {
        reply: RpcReply<u64>,
    },
    DeleteTarget {
        id: ChunkId,
        target: WorkerTargetId,
        reply: RpcReply<()>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairReport {
    pub healthy_workers: Vec<u64>,
    pub checked_chunks: usize,
    pub repaired_replicas: usize,
    /// `true` when another [`RemoteReplicatedChunkStore::repair_pass`] is needed.
    pub more: bool,
}

/// RF=N client whose ACK requires distinct durable Worker process responses.
pub struct RemoteReplicatedChunkStore {
    sender: Option<mpsc::SyncSender<Command>>,
    rpc_thread: Option<thread::JoinHandle<()>>,
    gc_sender: Option<mpsc::SyncSender<Command>>,
    gc_thread: Option<thread::JoinHandle<()>>,
    scrub_stop: Option<Arc<AtomicBool>>,
    scrub_thread: Option<thread::JoinHandle<()>>,
}

impl RemoteReplicatedChunkStore {
    pub fn new(worker_endpoints: Vec<String>, required: usize) -> Result<Self> {
        Self::new_with_max_pending(worker_endpoints, required, DEFAULT_MAX_PENDING_CHUNK_OPS)
    }

    pub fn new_with_max_pending(
        worker_endpoints: Vec<String>,
        required: usize,
        max_pending: usize,
    ) -> Result<Self> {
        if required == 0 || worker_endpoints.len() < required {
            return Err(FluxError::InvalidArg(format!(
                "remote replication requires {required} workers, got {} endpoints",
                worker_endpoints.len()
            )));
        }
        if max_pending == 0 {
            return Err(FluxError::InvalidArg(
                "remote chunk max_pending must be greater than zero".into(),
            ));
        }
        let mut channels = Vec::with_capacity(worker_endpoints.len());
        for endpoint in worker_endpoints {
            let channel = Endpoint::from_shared(endpoint.clone())
                .map_err(|error| FluxError::InvalidArg(format!("{endpoint}: {error}")))?
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(2))
                .connect_lazy();
            channels.push(channel);
        }

        let (sender, receiver) = mpsc::sync_channel(max_pending);
        let (gc_sender, gc_receiver) = mpsc::sync_channel(8);
        let gc_channels = channels.clone();
        let rpc_thread = thread::Builder::new()
            .name("fluxfs-chunk-rpc".into())
            .spawn(move || rpc_loop(channels, required, receiver))
            .map_err(|error| FluxError::Io(format!("spawn chunk RPC thread: {error}")))?;
        let gc_thread = thread::Builder::new()
            .name("fluxfs-chunk-gc-rpc".into())
            .spawn(move || rpc_loop(gc_channels, required, gc_receiver))
            .map_err(|error| FluxError::Io(format!("spawn chunk GC RPC thread: {error}")))?;

        let scrub_stop = Arc::new(AtomicBool::new(false));
        let scrub_flag = Arc::clone(&scrub_stop);
        let scrub_sender = gc_sender.clone();
        let scrub_thread = thread::Builder::new()
            .name("fluxfs-chunk-repair-scrub".into())
            .spawn(move || {
                while !scrub_flag.load(Ordering::Relaxed) {
                    let (reply, response) = mpsc::channel();
                    match scrub_sender.try_send(Command::RepairPass {
                        limit: REPAIR_PAGE_SIZE,
                        reply,
                    }) {
                        Ok(()) => {
                            let more = matches!(
                                response.recv(),
                                Ok(Ok(report)) if report.more || report.repaired_replicas > 0
                            );
                            let sleep = if more {
                                Duration::from_millis(50)
                            } else {
                                BACKGROUND_REPAIR_IDLE
                            };
                            let mut left = sleep;
                            while left > Duration::ZERO && !scrub_flag.load(Ordering::Relaxed) {
                                let step = Duration::from_millis(50).min(left);
                                thread::sleep(step);
                                left = left.saturating_sub(step);
                            }
                        }
                        Err(_) => thread::sleep(BACKGROUND_REPAIR_IDLE),
                    }
                }
            })
            .map_err(|error| FluxError::Io(format!("spawn chunk repair scrub: {error}")))?;

        Ok(Self {
            sender: Some(sender),
            rpc_thread: Some(rpc_thread),
            gc_sender: Some(gc_sender),
            gc_thread: Some(gc_thread),
            scrub_stop: Some(scrub_stop),
            scrub_thread: Some(scrub_thread),
        })
    }

    pub fn available_workers(&self) -> Result<Vec<u64>> {
        self.call(|reply| Command::AvailableWorkers { reply })
    }

    /// Scrub all reachable Worker inventories and restore every known chunk to RF=N.
    ///
    /// Inventory is walked in bounded pages ([`REPAIR_PAGE_SIZE`]); the call still
    /// waits until catch-up finishes (used when topology changes before Put ACK).
    pub fn repair(&self) -> Result<RepairReport> {
        self.call(|reply| Command::Repair { reply })
    }

    /// One throttled scrub page on the low-priority GC RPC pool.
    pub fn repair_pass(&self, limit: usize) -> Result<RepairReport> {
        self.call_gc(|reply| Command::RepairPass { limit, reply })
    }

    fn call<T>(&self, command: impl FnOnce(RpcReply<T>) -> Command) -> Result<T> {
        let (reply, response) = mpsc::channel();
        self.sender
            .as_ref()
            .ok_or(FluxError::Busy)?
            .try_send(command(reply))
            .map_err(|_| FluxError::Busy)?;
        response.recv().map_err(|_| FluxError::Busy)?
    }

    fn call_gc<T>(&self, command: impl FnOnce(RpcReply<T>) -> Command) -> Result<T> {
        let (reply, response) = mpsc::channel();
        self.gc_sender
            .as_ref()
            .ok_or(FluxError::Busy)?
            .try_send(command(reply))
            .map_err(|_| FluxError::Busy)?;
        response.recv().map_err(|_| FluxError::Busy)?
    }
}

impl ChunkStore for RemoteReplicatedChunkStore {
    fn put(&self, data: &[u8]) -> Result<ChunkId> {
        self.call(|reply| Command::Put {
            data: data.to_vec(),
            reply,
        })
    }

    fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
        self.call(|reply| Command::Get { id: *id, reply })
    }

    fn contains(&self, id: &ChunkId) -> Result<bool> {
        self.call(|reply| Command::Contains { id: *id, reply })
    }

    fn list_chunks(&self) -> Result<Vec<ChunkId>> {
        let mut chunks = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_chunks_page(cursor, 1024)?;
            chunks.extend(page.chunks);
            let Some(next) = page.next_cursor else { break };
            cursor = Some(next);
        }
        Ok(chunks)
    }

    fn list_chunks_page(&self, cursor: Option<ChunkId>, limit: usize) -> Result<ChunkPage> {
        self.call_gc(|reply| Command::ListChunksPage {
            cursor,
            limit,
            reply,
        })
    }

    fn delete(&self, id: &ChunkId) -> Result<()> {
        for target in self.gc_delete_targets()? {
            self.delete_from_target(id, target)?;
        }
        Ok(())
    }

    fn gc_delete_targets(&self) -> Result<Vec<WorkerTargetId>> {
        let count = self.call_gc(|reply| Command::TargetCount { reply })?;
        Ok((0..count).map(WorkerTargetId).collect())
    }

    fn delete_from_target(&self, id: &ChunkId, target: WorkerTargetId) -> Result<()> {
        self.call_gc(|reply| Command::DeleteTarget {
            id: *id,
            target,
            reply,
        })
    }
}

impl Drop for RemoteReplicatedChunkStore {
    fn drop(&mut self) {
        if let Some(stop) = self.scrub_stop.take() {
            stop.store(true, Ordering::Relaxed);
        }
        self.sender.take();
        self.gc_sender.take();
        if let Some(thread) = self.scrub_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.rpc_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.gc_thread.take() {
            let _ = thread.join();
        }
    }
}

fn rpc_loop(channels: Vec<Channel>, required: usize, receiver: mpsc::Receiver<Command>) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return,
    };
    let mut clients = channels
        .into_iter()
        .map(|channel| {
            ChunkWorkerClient::new(channel)
                .max_decoding_message_size(MAX_CHUNK_RPC_MESSAGE)
                .max_encoding_message_size(MAX_CHUNK_RPC_MESSAGE)
        })
        .collect::<Vec<_>>();
    let mut last_healthy = BTreeSet::new();
    let mut repair_cursor: Option<ChunkId> = None;
    while let Ok(command) = receiver.recv() {
        runtime.block_on(handle_command(
            &mut clients,
            required,
            &mut last_healthy,
            &mut repair_cursor,
            command,
        ));
    }
}

async fn handle_command(
    clients: &mut [ChunkWorkerClient<Channel>],
    required: usize,
    last_healthy: &mut BTreeSet<u64>,
    repair_cursor: &mut Option<ChunkId>,
    command: Command,
) {
    match command {
        Command::Put { data, reply } => {
            let expected = ChunkId::from_bytes(&data);
            let healthy = match healthy_workers(clients).await {
                Ok(healthy) => healthy,
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return;
                }
            };
            let current = healthy.keys().copied().collect::<BTreeSet<_>>();
            if &current != last_healthy {
                *repair_cursor = None;
                match repair_with_health(clients, required, &healthy, repair_cursor).await {
                    Ok(_) => *last_healthy = current,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
            let mut durable_workers = BTreeSet::new();
            let mut errors = Vec::new();
            let mut overloaded = false;
            for (worker_id, index) in &healthy {
                match put_one(&mut clients[*index], *worker_id, &data, expected).await {
                    Ok(()) => {
                        durable_workers.insert(*worker_id);
                        if durable_workers.len() == required {
                            break;
                        }
                    }
                    Err(error) => {
                        overloaded |= error == FluxError::Busy;
                        errors.push(error.to_string());
                    }
                }
            }
            let result = if durable_workers.len() >= required {
                Ok(expected)
            } else if overloaded {
                Err(FluxError::Busy)
            } else {
                Err(FluxError::Io(format!(
                    "remote chunk {} reached {}/{} distinct durable workers: {}",
                    expected.to_hex(),
                    durable_workers.len(),
                    required,
                    errors.join("; ")
                )))
            };
            let _ = reply.send(result);
        }
        Command::Get { id, reply } => {
            let mut errors = Vec::new();
            let mut result = None;
            let mut source_worker = None;
            let mut overloaded = false;
            for client in clients.iter_mut() {
                match client
                    .get_chunk(GetChunkRequest {
                        chunk_id: id.as_bytes().to_vec(),
                    })
                    .await
                {
                    Ok(response) => {
                        let response = response.into_inner();
                        let data = response.data;
                        if ChunkId::from_bytes(&data) == id {
                            result = Some(data);
                            source_worker = Some(response.worker_id);
                            break;
                        }
                        errors.push("worker returned data with wrong checksum".into());
                    }
                    Err(error) => {
                        overloaded |= error.code() == tonic::Code::ResourceExhausted;
                        errors.push(error.to_string());
                    }
                }
            }
            if let (Some(data), Some(source_worker)) = (result.as_ref(), source_worker) {
                if let Ok(healthy) = healthy_workers(clients).await {
                    let mut repaired = BTreeSet::from([source_worker]);
                    for (worker_id, index) in healthy {
                        if repaired.len() == required {
                            break;
                        }
                        if repaired.contains(&worker_id) {
                            continue;
                        }
                        if put_one(&mut clients[index], worker_id, data, id)
                            .await
                            .is_ok()
                        {
                            repaired.insert(worker_id);
                        }
                    }
                }
            }
            let result = match result {
                Some(data) => Ok(data),
                None if overloaded => Err(FluxError::Busy),
                None => Err(FluxError::Io(format!(
                    "no readable remote replica for {}: {}",
                    id.to_hex(),
                    errors.join("; ")
                ))),
            };
            let _ = reply.send(result);
        }
        Command::Contains { id, reply } => {
            let mut present_workers = BTreeSet::new();
            for client in clients.iter_mut() {
                if let Ok(response) = client
                    .contains_chunk(ContainsChunkRequest {
                        chunk_id: id.as_bytes().to_vec(),
                    })
                    .await
                {
                    let response = response.into_inner();
                    if response.present {
                        present_workers.insert(response.worker_id);
                    }
                }
            }
            let _ = reply.send(Ok(present_workers.len() >= required));
        }
        Command::AvailableWorkers { reply } => {
            let result = healthy_workers(clients).await.map(|healthy| {
                let workers = healthy.keys().copied().collect::<Vec<_>>();
                *last_healthy = workers.iter().copied().collect();
                workers
            });
            let _ = reply.send(result);
        }
        Command::Repair { reply } => {
            let result = async {
                let healthy = healthy_workers(clients).await?;
                *repair_cursor = None;
                let report = repair_with_health(clients, required, &healthy, repair_cursor).await?;
                *last_healthy = healthy.keys().copied().collect();
                Ok(report)
            }
            .await;
            let _ = reply.send(result);
        }
        Command::RepairPass { limit, reply } => {
            let result = async {
                let healthy = healthy_workers(clients).await?;
                let current = healthy.keys().copied().collect::<BTreeSet<_>>();
                if &current != last_healthy {
                    *repair_cursor = None;
                    *last_healthy = current;
                }
                repair_pass_with_health(clients, required, &healthy, repair_cursor, limit).await
            }
            .await;
            let _ = reply.send(result);
        }
        Command::ListChunksPage {
            cursor,
            limit,
            reply,
        } => {
            let result = all_chunks_page(clients, cursor, limit).await;
            let _ = reply.send(result);
        }
        Command::TargetCount { reply } => {
            let _ = reply.send(Ok(clients.len().try_into().unwrap_or(u64::MAX)));
        }
        Command::DeleteTarget { id, target, reply } => {
            let result = match usize::try_from(target.0)
                .ok()
                .and_then(|index| clients.get_mut(index))
            {
                Some(client) => client
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: id.as_bytes().to_vec(),
                    })
                    .await
                    .map(|_| ())
                    .map_err(rpc_status_error),
                None => Err(FluxError::InvalidArg(format!(
                    "unknown chunk delete target {}",
                    target.0
                ))),
            };
            let _ = reply.send(result);
        }
    }
}

async fn all_chunks_page(
    clients: &mut [ChunkWorkerClient<Channel>],
    cursor: Option<ChunkId>,
    limit: usize,
) -> Result<ChunkPage> {
    if limit == 0 {
        return Err(FluxError::InvalidArg(
            "chunk inventory page limit must be non-zero".into(),
        ));
    }
    let mut chunks = Vec::new();
    let mut reached = 0usize;
    let mut worker_has_more = false;
    for client in clients.iter_mut() {
        if let Ok(response) = client
            .list_chunks(ListChunksRequest {
                after_chunk_id: cursor
                    .map(|chunk| chunk.as_bytes().to_vec())
                    .unwrap_or_default(),
                limit: limit.try_into().unwrap_or(u32::MAX),
            })
            .await
        {
            reached += 1;
            let response = response.into_inner();
            worker_has_more |= !response.next_cursor.is_empty();
            for raw in response.chunk_ids {
                chunks.push(ChunkId::try_from(raw.as_slice())?);
            }
        }
    }
    if reached == 0 {
        return Err(FluxError::Io("chunk inventory reached no workers".into()));
    }
    chunks.sort_by_key(ChunkId::to_hex);
    chunks.dedup();
    let has_more = worker_has_more || chunks.len() > limit;
    chunks.truncate(limit);
    let next_cursor = has_more.then(|| *chunks.last().expect("non-empty inventory page"));
    Ok(ChunkPage {
        chunks,
        next_cursor,
    })
}

async fn healthy_workers(
    clients: &mut [ChunkWorkerClient<Channel>],
) -> Result<BTreeMap<u64, usize>> {
    let mut healthy = BTreeMap::new();
    for (index, client) in clients.iter_mut().enumerate() {
        if let Ok(response) = client.health(HealthRequest {}).await {
            let response = response.into_inner();
            if response.ready && healthy.insert(response.worker_id, index).is_some() {
                return Err(FluxError::InvalidArg(format!(
                    "duplicate remote worker id {}",
                    response.worker_id
                )));
            }
        }
    }
    Ok(healthy)
}

async fn put_one(
    client: &mut ChunkWorkerClient<Channel>,
    expected_worker: u64,
    data: &[u8],
    expected_chunk: ChunkId,
) -> Result<()> {
    let response = client
        .put_chunk(PutChunkRequest {
            data: data.to_vec(),
        })
        .await
        .map_err(rpc_status_error)?
        .into_inner();
    let chunk = ChunkId::try_from(response.chunk_id.as_slice())?;
    if response.worker_id != expected_worker || chunk != expected_chunk || !response.durable {
        return Err(FluxError::Io(format!(
            "worker {expected_worker} returned mismatched/non-durable chunk"
        )));
    }
    Ok(())
}

fn rpc_status_error(error: tonic::Status) -> FluxError {
    if error.code() == tonic::Code::ResourceExhausted {
        FluxError::Busy
    } else {
        FluxError::Io(error.to_string())
    }
}

async fn repair_with_health(
    clients: &mut [ChunkWorkerClient<Channel>],
    required: usize,
    healthy: &BTreeMap<u64, usize>,
    repair_cursor: &mut Option<ChunkId>,
) -> Result<RepairReport> {
    *repair_cursor = None;
    let mut checked_chunks = 0usize;
    let mut repaired_replicas = 0usize;
    loop {
        let page =
            repair_pass_with_health(clients, required, healthy, repair_cursor, REPAIR_PAGE_SIZE)
                .await?;
        checked_chunks += page.checked_chunks;
        repaired_replicas += page.repaired_replicas;
        if !page.more {
            return Ok(RepairReport {
                healthy_workers: healthy.keys().copied().collect(),
                checked_chunks,
                repaired_replicas,
                more: false,
            });
        }
    }
}

async fn repair_pass_with_health(
    clients: &mut [ChunkWorkerClient<Channel>],
    required: usize,
    healthy: &BTreeMap<u64, usize>,
    repair_cursor: &mut Option<ChunkId>,
    limit: usize,
) -> Result<RepairReport> {
    if healthy.len() < required {
        return Err(FluxError::Io(format!(
            "repair requires {required} healthy workers, found {}",
            healthy.len()
        )));
    }
    if limit == 0 {
        return Err(FluxError::InvalidArg(
            "repair page limit must be non-zero".into(),
        ));
    }

    let page = inventory_page_with_holders(clients, healthy, *repair_cursor, limit).await?;
    let checked_chunks = page.chunks.len();
    let mut repaired_replicas = 0usize;

    for (chunk, mut holders) in page.chunks {
        if holders.len() >= required {
            continue;
        }
        let source = holders
            .iter()
            .next()
            .and_then(|worker_id| healthy.get(worker_id))
            .copied()
            .ok_or_else(|| FluxError::Io(format!("no source replica for {}", chunk.to_hex())))?;
        let response = clients[source]
            .get_chunk(GetChunkRequest {
                chunk_id: chunk.as_bytes().to_vec(),
            })
            .await
            .map_err(rpc_status_error)?
            .into_inner();
        if ChunkId::from_bytes(&response.data) != chunk {
            return Err(FluxError::Io(format!(
                "repair source checksum mismatch for {}",
                chunk.to_hex()
            )));
        }
        for (worker_id, index) in healthy {
            if holders.len() >= required {
                break;
            }
            if holders.contains(worker_id) {
                continue;
            }
            put_one(&mut clients[*index], *worker_id, &response.data, chunk).await?;
            holders.insert(*worker_id);
            repaired_replicas += 1;
        }
        if holders.len() < required {
            return Err(FluxError::Io(format!(
                "repair left {} at {}/{} replicas",
                chunk.to_hex(),
                holders.len(),
                required
            )));
        }
    }

    *repair_cursor = page.next_cursor;
    Ok(RepairReport {
        healthy_workers: healthy.keys().copied().collect(),
        checked_chunks,
        repaired_replicas,
        more: page.next_cursor.is_some(),
    })
}

struct InventoryPage {
    chunks: BTreeMap<ChunkId, BTreeSet<u64>>,
    next_cursor: Option<ChunkId>,
}

async fn inventory_page_with_holders(
    clients: &mut [ChunkWorkerClient<Channel>],
    healthy: &BTreeMap<u64, usize>,
    cursor: Option<ChunkId>,
    limit: usize,
) -> Result<InventoryPage> {
    let mut inventory = BTreeMap::<ChunkId, BTreeSet<u64>>::new();
    let mut worker_has_more = false;
    let after = cursor
        .map(|chunk| chunk.as_bytes().to_vec())
        .unwrap_or_default();
    for (worker_id, index) in healthy {
        let response = clients[*index]
            .list_chunks(ListChunksRequest {
                after_chunk_id: after.clone(),
                limit: limit.try_into().unwrap_or(u32::MAX),
            })
            .await
            .map_err(rpc_status_error)?
            .into_inner();
        if response.worker_id != *worker_id {
            return Err(FluxError::Io(format!(
                "inventory worker id mismatch: expected {worker_id}, got {}",
                response.worker_id
            )));
        }
        worker_has_more |= !response.next_cursor.is_empty();
        for raw in response.chunk_ids {
            inventory
                .entry(ChunkId::try_from(raw.as_slice())?)
                .or_default()
                .insert(*worker_id);
        }
    }

    let mut keys = inventory.keys().copied().collect::<Vec<_>>();
    keys.sort_by_key(ChunkId::to_hex);
    let has_more = worker_has_more || keys.len() > limit;
    keys.truncate(limit);
    let next_cursor = has_more.then(|| *keys.last().expect("non-empty inventory page"));
    let mut page_chunks = BTreeMap::new();
    for key in keys {
        if let Some(holders) = inventory.remove(&key) {
            page_chunks.insert(key, holders);
        }
    }
    Ok(InventoryPage {
        chunks: page_chunks,
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_too_few_worker_endpoints() {
        assert!(RemoteReplicatedChunkStore::new(vec!["http://127.0.0.1:1".into()], 2).is_err());
    }

    #[test]
    fn rejects_zero_pending_capacity() {
        assert!(matches!(
            RemoteReplicatedChunkStore::new_with_max_pending(
                vec!["http://127.0.0.1:1".into()],
                1,
                0,
            ),
            Err(FluxError::InvalidArg(_))
        ));
    }

    #[test]
    fn full_client_queue_returns_busy_without_blocking() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        let (reply, _response) = mpsc::channel();
        sender
            .try_send(Command::AvailableWorkers { reply })
            .unwrap();
        let store = RemoteReplicatedChunkStore {
            sender: Some(sender),
            rpc_thread: None,
            gc_sender: None,
            gc_thread: None,
            scrub_stop: None,
            scrub_thread: None,
        };
        assert_eq!(store.available_workers(), Err(FluxError::Busy));
    }

    #[test]
    fn repair_page_size_is_bounded() {
        const {
            assert!(REPAIR_PAGE_SIZE > 0);
            assert!(REPAIR_PAGE_SIZE < u32::MAX as usize);
        }
    }

    #[test]
    fn saturated_foreground_queue_does_not_block_gc_queue() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        let (reply, _response) = mpsc::channel();
        sender
            .try_send(Command::AvailableWorkers { reply })
            .unwrap();
        let (gc_sender, gc_receiver) = mpsc::sync_channel(1);
        let gc_thread = thread::spawn(move || {
            if let Ok(Command::TargetCount { reply }) = gc_receiver.recv() {
                let _ = reply.send(Ok(3));
            }
        });
        let store = RemoteReplicatedChunkStore {
            sender: Some(sender),
            rpc_thread: None,
            gc_sender: Some(gc_sender),
            gc_thread: Some(gc_thread),
            scrub_stop: None,
            scrub_thread: None,
        };
        assert_eq!(
            store.gc_delete_targets().unwrap(),
            vec![WorkerTargetId(0), WorkerTargetId(1), WorkerTargetId(2)]
        );
    }
}
