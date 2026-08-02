//! Synchronous `ChunkStore` facade over dedicated tonic ChunkWorker processes.
//!
//! FUSE callbacks are synchronous. A dedicated RPC thread owns a Tokio runtime
//! so callers never nest/block the Meta/FUSE runtime. The initial v0 placement
//! fixes the first `required` endpoints as the authoritative replica set; extra
//! endpoints are repair spares.

use crate::ChunkStore;
use fluxfs_proto::chunk::v1::{
    ContainsChunkRequest, GetChunkRequest, HealthRequest, PutChunkRequest,
};
use fluxfs_proto::ChunkWorkerClient;
use fluxfs_types::{ChunkId, FluxError, Result, CHUNK_SIZE};
use std::collections::BTreeSet;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

const MAX_CHUNK_RPC_MESSAGE: usize = CHUNK_SIZE as usize + 64 * 1024;

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
}

/// RF=N client whose ACK requires distinct durable Worker process responses.
pub struct RemoteReplicatedChunkStore {
    sender: Option<mpsc::Sender<Command>>,
    rpc_thread: Option<thread::JoinHandle<()>>,
}

impl RemoteReplicatedChunkStore {
    pub fn new(worker_endpoints: Vec<String>, required: usize) -> Result<Self> {
        if required == 0 || worker_endpoints.len() < required {
            return Err(FluxError::InvalidArg(format!(
                "remote replication requires {required} workers, got {} endpoints",
                worker_endpoints.len()
            )));
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

        let (sender, receiver) = mpsc::channel();
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

    fn call<T>(&self, command: impl FnOnce(RpcReply<T>) -> Command) -> Result<T> {
        let (reply, response) = mpsc::channel();
        self.sender
            .as_ref()
            .ok_or(FluxError::Busy)?
            .send(command(reply))
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
    while let Ok(command) = receiver.recv() {
        runtime.block_on(handle_command(&mut clients, required, command));
    }
}

async fn handle_command(
    clients: &mut [ChunkWorkerClient<Channel>],
    required: usize,
    command: Command,
) {
    match command {
        Command::Put { data, reply } => {
            let expected = ChunkId::from_bytes(&data);
            let mut durable_workers = BTreeSet::new();
            let mut errors = Vec::new();
            for client in clients.iter_mut().take(required) {
                match client
                    .put_chunk(PutChunkRequest { data: data.clone() })
                    .await
                {
                    Ok(response) => {
                        let response = response.into_inner();
                        match ChunkId::try_from(response.chunk_id.as_slice()) {
                            Ok(id) if id == expected && response.durable => {
                                durable_workers.insert(response.worker_id);
                            }
                            Ok(_) => {
                                errors.push("worker returned mismatched/non-durable chunk".into())
                            }
                            Err(error) => errors.push(error.to_string()),
                        }
                    }
                    Err(error) => errors.push(error.to_string()),
                }
            }
            let result = if durable_workers.len() >= required {
                Ok(expected)
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
            for client in clients.iter_mut().take(required) {
                match client
                    .get_chunk(GetChunkRequest {
                        chunk_id: id.as_bytes().to_vec(),
                    })
                    .await
                {
                    Ok(response) => {
                        let data = response.into_inner().data;
                        if ChunkId::from_bytes(&data) == id {
                            result = Some(data);
                            break;
                        }
                        errors.push("worker returned data with wrong checksum".into());
                    }
                    Err(error) => errors.push(error.to_string()),
                }
            }
            let _ = reply.send(result.ok_or_else(|| {
                FluxError::Io(format!(
                    "no readable remote replica for {}: {}",
                    id.to_hex(),
                    errors.join("; ")
                ))
            }));
        }
        Command::Contains { id, reply } => {
            let mut present_workers = BTreeSet::new();
            for client in clients.iter_mut().take(required) {
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
            let mut workers = BTreeSet::new();
            for client in clients.iter_mut() {
                if let Ok(response) = client.health(HealthRequest {}).await {
                    let response = response.into_inner();
                    if response.ready {
                        workers.insert(response.worker_id);
                    }
                }
            }
            let _ = reply.send(Ok(workers.into_iter().collect()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_too_few_worker_endpoints() {
        assert!(RemoteReplicatedChunkStore::new(vec!["http://127.0.0.1:1".into()], 2).is_err());
    }
}
