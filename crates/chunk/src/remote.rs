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
use fluxfs_types::{ChunkId, FluxError, Result, CHUNK_SIZE};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

const MAX_CHUNK_RPC_MESSAGE: usize = CHUNK_SIZE as usize + 64 * 1024;
pub const DEFAULT_MAX_PENDING_CHUNK_OPS: usize = 64;

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
    ListChunks {
        reply: RpcReply<Vec<ChunkId>>,
    },
    Delete {
        id: ChunkId,
        reply: RpcReply<()>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairReport {
    pub healthy_workers: Vec<u64>,
    pub checked_chunks: usize,
    pub repaired_replicas: usize,
}

/// RF=N client whose ACK requires distinct durable Worker process responses.
pub struct RemoteReplicatedChunkStore {
    sender: Option<mpsc::SyncSender<Command>>,
    rpc_thread: Option<thread::JoinHandle<()>>,
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
        let rpc_thread = thread::Builder::new()
            .name("fluxfs-chunk-rpc".into())
            .spawn(move || rpc_loop(channels, required, receiver))
            .map_err(|error| FluxError::Io(format!("spawn chunk RPC thread: {error}")))?;
        Ok(Self {
            sender: Some(sender),
            rpc_thread: Some(rpc_thread),
        })
    }

    pub fn available_workers(&self) -> Result<Vec<u64>> {
        self.call(|reply| Command::AvailableWorkers { reply })
    }

    /// Scrub all reachable Worker inventories and restore every known chunk to RF=N.
    pub fn repair(&self) -> Result<RepairReport> {
        self.call(|reply| Command::Repair { reply })
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
        self.call(|reply| Command::ListChunks { reply })
    }

    fn delete(&self, id: &ChunkId) -> Result<()> {
        self.call(|reply| Command::Delete { id: *id, reply })
    }
}

impl Drop for RemoteReplicatedChunkStore {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.rpc_thread.take() {
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
    while let Ok(command) = receiver.recv() {
        runtime.block_on(handle_command(
            &mut clients,
            required,
            &mut last_healthy,
            command,
        ));
    }
}

async fn handle_command(
    clients: &mut [ChunkWorkerClient<Channel>],
    required: usize,
    last_healthy: &mut BTreeSet<u64>,
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
                match repair_with_health(clients, required, &healthy).await {
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
                let report = repair_with_health(clients, required, &healthy).await?;
                *last_healthy = healthy.keys().copied().collect();
                Ok(report)
            }
            .await;
            let _ = reply.send(result);
        }
        Command::ListChunks { reply } => {
            let result = all_chunks(clients).await;
            let _ = reply.send(result);
        }
        Command::Delete { id, reply } => {
            let mut reached = 0usize;
            let mut errors = Vec::new();
            for client in clients.iter_mut() {
                match client
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: id.as_bytes().to_vec(),
                    })
                    .await
                {
                    Ok(_) => reached += 1,
                    Err(error) => errors.push(error.to_string()),
                }
            }
            let result = if reached == 0 {
                Err(FluxError::Io(format!(
                    "delete {} reached no workers: {}",
                    id.to_hex(),
                    errors.join("; ")
                )))
            } else {
                Ok(())
            };
            let _ = reply.send(result);
        }
    }
}

async fn all_chunks(clients: &mut [ChunkWorkerClient<Channel>]) -> Result<Vec<ChunkId>> {
    let mut chunks = Vec::new();
    let mut reached = 0usize;
    for client in clients.iter_mut() {
        if let Ok(response) = client.list_chunks(ListChunksRequest {}).await {
            reached += 1;
            for raw in response.into_inner().chunk_ids {
                chunks.push(ChunkId::try_from(raw.as_slice())?);
            }
        }
    }
    if reached == 0 {
        return Err(FluxError::Io("chunk inventory reached no workers".into()));
    }
    chunks.sort_by_key(ChunkId::to_hex);
    chunks.dedup();
    Ok(chunks)
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
) -> Result<RepairReport> {
    if healthy.len() < required {
        return Err(FluxError::Io(format!(
            "repair requires {required} healthy workers, found {}",
            healthy.len()
        )));
    }

    let mut inventory = BTreeMap::<ChunkId, BTreeSet<u64>>::new();
    for (worker_id, index) in healthy {
        let response = clients[*index]
            .list_chunks(ListChunksRequest {})
            .await
            .map_err(rpc_status_error)?
            .into_inner();
        if response.worker_id != *worker_id {
            return Err(FluxError::Io(format!(
                "inventory worker id mismatch: expected {worker_id}, got {}",
                response.worker_id
            )));
        }
        for raw in response.chunk_ids {
            inventory
                .entry(ChunkId::try_from(raw.as_slice())?)
                .or_default()
                .insert(*worker_id);
        }
    }

    let checked_chunks = inventory.len();
    let mut repaired_replicas = 0;
    for (chunk, mut holders) in inventory {
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

    Ok(RepairReport {
        healthy_workers: healthy.keys().copied().collect(),
        checked_chunks,
        repaired_replicas,
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
        };
        assert_eq!(store.available_workers(), Err(FluxError::Busy));
    }
}
