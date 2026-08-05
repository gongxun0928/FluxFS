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
use fluxfs_proto::meta::v1::GetWorkerMembershipRequest;
use fluxfs_proto::meta_codec::decode_worker_membership;
use fluxfs_proto::{ChunkWorkerClient, MetaServiceClient};
use fluxfs_types::{
    ChunkId, ChunkPage, FluxError, Result, WorkerMembership, WorkerTargetId, CHUNK_SIZE,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

const MAX_CHUNK_RPC_MESSAGE: usize = CHUNK_SIZE as usize + 64 * 1024;
pub const DEFAULT_MAX_PENDING_CHUNK_OPS: usize = 64;
/// Bounded inventory page for repair/scrub (replaces `limit=u32::MAX` sweeps).
pub const REPAIR_PAGE_SIZE: usize = 256;
const BACKGROUND_REPAIR_IDLE: Duration = Duration::from_secs(2);

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

type RpcReply<T> = mpsc::Sender<Result<T>>;

enum Command {
    Put {
        data: Vec<u8>,
        reply: RpcReply<ChunkId>,
    },
    Get {
        id: ChunkId,
        promote_cache: bool,
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
    Targets {
        reply: RpcReply<Vec<WorkerTargetId>>,
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

    /// Plaintext constructor (tests only — production dials via
    /// [`Self::new_with_max_pending_tls`]).
    pub fn new_with_max_pending(
        worker_endpoints: Vec<String>,
        required: usize,
        max_pending: usize,
    ) -> Result<Self> {
        Self::new_with_max_pending_tls(worker_endpoints, required, max_pending, None, true)
    }

    /// Construct with optional TLS (task #30 C1 Phase 2).
    ///
    /// - `tls=Some(opts)`: each worker endpoint is dialed with the shared
    ///   client TLS config (mTLS identity + CA verification).
    /// - `tls=None`: plaintext; `insecure_dev` must be true.
    pub fn new_with_max_pending_tls(
        worker_endpoints: Vec<String>,
        required: usize,
        max_pending: usize,
        tls: Option<fluxfs_tls::ClientTlsOptions>,
        insecure_dev: bool,
    ) -> Result<Self> {
        let targets = worker_endpoints
            .into_iter()
            .enumerate()
            .map(|(index, endpoint)| {
                (
                    WorkerTargetId(index.try_into().unwrap_or(u64::MAX)),
                    endpoint,
                )
            })
            .collect();
        Self::new_with_targets(
            targets,
            required,
            max_pending,
            None,
            None,
            tls,
            insecure_dev,
        )
    }

    pub fn new_with_membership(
        membership: WorkerMembership,
        required: usize,
        max_pending: usize,
        now_ms: u64,
    ) -> Result<Self> {
        let targets = membership
            .active_at(now_ms)
            .map(|worker| (worker.id, worker.endpoint.clone()))
            .collect();
        Self::new_with_targets(
            targets,
            required,
            max_pending,
            Some(membership),
            None,
            None,
            true,
        )
    }

    pub fn new_with_membership_discovery(
        membership: WorkerMembership,
        meta_endpoint: String,
        required: usize,
        max_pending: usize,
        now_ms: u64,
    ) -> Result<Self> {
        Self::new_with_membership_discovery_tls(
            membership,
            meta_endpoint,
            required,
            max_pending,
            now_ms,
            None,
            true,
        )
    }

    pub fn new_with_membership_discovery_tls(
        membership: WorkerMembership,
        meta_endpoint: String,
        required: usize,
        max_pending: usize,
        now_ms: u64,
        tls: Option<fluxfs_tls::ClientTlsOptions>,
        insecure_dev: bool,
    ) -> Result<Self> {
        let targets = membership
            .active_at(now_ms)
            .map(|worker| (worker.id, worker.endpoint.clone()))
            .collect();
        let endpoint =
            if meta_endpoint.starts_with("http://") || meta_endpoint.starts_with("https://") {
                meta_endpoint
            } else if tls.is_some() {
                format!("https://{meta_endpoint}")
            } else {
                format!("http://{meta_endpoint}")
            };
        Self::new_with_targets(
            targets,
            required,
            max_pending,
            Some(membership),
            Some(endpoint),
            tls,
            insecure_dev,
        )
    }

    fn new_with_targets(
        targets: Vec<(WorkerTargetId, String)>,
        required: usize,
        max_pending: usize,
        membership: Option<WorkerMembership>,
        meta_endpoint: Option<String>,
        tls: Option<fluxfs_tls::ClientTlsOptions>,
        insecure_dev: bool,
    ) -> Result<Self> {
        if required == 0 || targets.len() < required {
            return Err(FluxError::InvalidArg(format!(
                "remote replication requires {required} workers, got {} targets",
                targets.len()
            )));
        }
        if max_pending == 0 {
            return Err(FluxError::InvalidArg(
                "remote chunk max_pending must be greater than zero".into(),
            ));
        }
        let tls_cfg = if let Some(opts) = tls.as_ref() {
            opts.build_config_blocking()
                .map_err(|e| FluxError::Meta(e.to_string()))?
        } else {
            None
        };
        let mut channels = Vec::with_capacity(targets.len());
        for (id, endpoint) in targets {
            let ep = configured_endpoint(&endpoint, tls_cfg.as_ref(), insecure_dev)?;
            channels.push((id, ep.connect_lazy()));
        }

        let (sender, receiver) = mpsc::sync_channel(max_pending);
        let (gc_sender, gc_receiver) = mpsc::sync_channel(8);
        let gc_channels = channels.clone();
        let rpc_thread = thread::Builder::new()
            .name("fluxfs-chunk-rpc".into())
            .spawn({
                let membership = membership.clone();
                let meta_endpoint = meta_endpoint.clone();
                let tls_cfg = tls_cfg.clone();
                move || {
                    rpc_loop(
                        channels,
                        membership,
                        meta_endpoint,
                        tls_cfg,
                        insecure_dev,
                        required,
                        receiver,
                    )
                }
            })
            .map_err(|error| FluxError::Io(format!("spawn chunk RPC thread: {error}")))?;
        let gc_thread = thread::Builder::new()
            .name("fluxfs-chunk-gc-rpc".into())
            .spawn(move || {
                rpc_loop(
                    gc_channels,
                    membership,
                    meta_endpoint,
                    tls_cfg,
                    insecure_dev,
                    required,
                    gc_receiver,
                )
            })
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
        self.get_with_promote(id, false)
    }

    fn get_with_promote(&self, id: &ChunkId, promote_cache: bool) -> Result<Vec<u8>> {
        self.call(|reply| Command::Get {
            id: *id,
            promote_cache,
            reply,
        })
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
        self.call_gc(|reply| Command::Targets { reply })
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

struct WorkerRpcClient {
    id: WorkerTargetId,
    client: ChunkWorkerClient<Channel>,
}

fn rpc_loop(
    channels: Vec<(WorkerTargetId, Channel)>,
    mut membership: Option<WorkerMembership>,
    meta_endpoint: Option<String>,
    tls_cfg: Option<ClientTlsConfig>,
    insecure_dev: bool,
    required: usize,
    receiver: mpsc::Receiver<Command>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return,
    };
    let mut clients = channels
        .into_iter()
        .map(|(id, channel)| WorkerRpcClient {
            id,
            client: ChunkWorkerClient::new(channel)
                .max_decoding_message_size(MAX_CHUNK_RPC_MESSAGE)
                .max_encoding_message_size(MAX_CHUNK_RPC_MESSAGE),
        })
        .collect::<Vec<_>>();
    let mut last_healthy = BTreeSet::new();
    // Exclusive per-worker after-cursor for repair inventory merge.
    // Absence means "start from the beginning" for that worker.
    let mut repair_cursor: BTreeMap<u64, ChunkId> = BTreeMap::new();
    let mut meta_client = {
        let _enter = runtime.enter();
        meta_endpoint.and_then(|endpoint| {
            configured_endpoint(&endpoint, tls_cfg.as_ref(), insecure_dev)
                .ok()
                .map(|endpoint| MetaServiceClient::new(endpoint.connect_lazy()))
        })
    };
    let mut last_membership_refresh = Instant::now() - Duration::from_secs(2);
    while let Ok(command) = receiver.recv() {
        if last_membership_refresh.elapsed() >= Duration::from_secs(1) {
            last_membership_refresh = Instant::now();
            if let Some(meta) = meta_client.as_mut() {
                let refreshed = runtime.block_on(async {
                    meta.get_worker_membership(GetWorkerMembershipRequest {})
                        .await
                        .map_err(rpc_status_error)
                        .and_then(|response| {
                            decode_worker_membership(&response.into_inner().membership_json)
                        })
                });
                if let Ok(refreshed) = refreshed {
                    let routing_rebuilt = apply_membership_refresh(
                        &mut clients,
                        &mut membership,
                        refreshed,
                        |refreshed| {
                            let _enter = runtime.enter();
                            worker_clients_from_membership(
                                refreshed,
                                tls_cfg.as_ref(),
                                insecure_dev,
                            )
                        },
                    );
                    if routing_rebuilt {
                        last_healthy.clear();
                        repair_cursor.clear();
                    }
                }
            }
        }
        runtime.block_on(handle_command(
            &mut clients,
            membership.as_ref(),
            required,
            &mut last_healthy,
            &mut repair_cursor,
            command,
        ));
    }
}

/// Commit a refreshed membership only when its matching channel set exists.
/// Lease-only refreshes may update liveness in place; topology refreshes are
/// atomic with client rebuild so placement never gets ahead of routing.
fn apply_membership_refresh<F>(
    clients: &mut Vec<WorkerRpcClient>,
    membership: &mut Option<WorkerMembership>,
    refreshed: WorkerMembership,
    build_clients: F,
) -> bool
where
    F: FnOnce(&WorkerMembership) -> Result<Vec<WorkerRpcClient>>,
{
    let desired = refreshed
        .active_at(unix_time_millis())
        .map(|worker| worker.id)
        .collect::<BTreeSet<_>>();
    let current = clients.iter().map(|client| client.id).collect();
    let routing_changed = membership
        .as_ref()
        .is_none_or(|old| old.epoch != refreshed.epoch)
        || desired != current;
    if !routing_changed {
        // Lease-only heartbeat: same topology and channels, fresher deadline.
        *membership = Some(refreshed);
        return false;
    }
    let Ok(next_clients) = build_clients(&refreshed) else {
        // Keep the old membership paired with the old channel set.
        return false;
    };
    *clients = next_clients;
    *membership = Some(refreshed);
    true
}

fn worker_clients_from_membership(
    membership: &WorkerMembership,
    tls_cfg: Option<&ClientTlsConfig>,
    insecure_dev: bool,
) -> Result<Vec<WorkerRpcClient>> {
    membership
        .active_at(unix_time_millis())
        .map(|worker| {
            let endpoint = configured_endpoint(&worker.endpoint, tls_cfg, insecure_dev)?;
            Ok(WorkerRpcClient {
                id: worker.id,
                client: ChunkWorkerClient::new(endpoint.connect_lazy())
                    .max_decoding_message_size(MAX_CHUNK_RPC_MESSAGE)
                    .max_encoding_message_size(MAX_CHUNK_RPC_MESSAGE),
            })
        })
        .collect()
}

fn configured_endpoint(
    endpoint: &str,
    tls_cfg: Option<&ClientTlsConfig>,
    insecure_dev: bool,
) -> Result<Endpoint> {
    let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else if tls_cfg.is_some() {
        format!("https://{endpoint}")
    } else {
        format!("http://{endpoint}")
    };
    let tls_enabled = tls_cfg.is_some();
    let insecure = fluxfs_tls::InsecureDev::allow(insecure_dev);
    insecure
        .check_endpoint(&url)
        .map_err(|error| FluxError::InvalidArg(error.to_string()))?;
    insecure
        .check_scheme_matches_tls(&url, tls_enabled)
        .map_err(|error| FluxError::InvalidArg(error.to_string()))?;
    let mut configured = Endpoint::from_shared(url)
        .map_err(|error| FluxError::InvalidArg(format!("{endpoint}: {error}")))?
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2));
    if let Some(tls) = tls_cfg {
        configured = configured
            .tls_config(tls.clone())
            .map_err(|error| FluxError::InvalidArg(format!("{endpoint}: tls: {error}")))?;
    }
    Ok(configured)
}

async fn handle_command(
    clients: &mut [WorkerRpcClient],
    membership: Option<&WorkerMembership>,
    required: usize,
    last_healthy: &mut BTreeSet<u64>,
    repair_cursor: &mut BTreeMap<u64, ChunkId>,
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
                repair_cursor.clear();
                match repair_with_health(clients, membership, required, &healthy, repair_cursor)
                    .await
                {
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
            let selected = match membership {
                Some(membership) => match crate::select_worker_targets(
                    membership,
                    &expected,
                    required,
                    unix_time_millis(),
                ) {
                    Ok(workers) => workers
                        .into_iter()
                        .map(|worker| worker.id.0)
                        .collect::<BTreeSet<_>>(),
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                },
                None => healthy.keys().copied().collect(),
            };
            for (worker_id, index) in &healthy {
                if !selected.contains(worker_id) {
                    continue;
                }
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
        Command::Get {
            id,
            promote_cache,
            reply,
        } => {
            let mut errors = Vec::new();
            let mut result = None;
            let mut source_worker = None;
            let mut overloaded = false;
            for client in clients.iter_mut() {
                match client
                    .client
                    .get_chunk(GetChunkRequest {
                        chunk_id: id.as_bytes().to_vec(),
                        promote_cache,
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
                    let desired = membership
                        .and_then(|membership| {
                            crate::select_worker_targets(
                                membership,
                                &id,
                                required,
                                unix_time_millis(),
                            )
                            .ok()
                        })
                        .map(|workers| {
                            workers
                                .into_iter()
                                .map(|worker| worker.id.0)
                                .collect::<BTreeSet<_>>()
                        });
                    for (worker_id, index) in healthy {
                        let placed = desired.as_ref().map_or_else(
                            || repaired.len(),
                            |desired| repaired.intersection(desired).count(),
                        );
                        if placed == required {
                            break;
                        }
                        if repaired.contains(&worker_id) {
                            continue;
                        }
                        if desired
                            .as_ref()
                            .is_some_and(|desired| !desired.contains(&worker_id))
                        {
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
                    .client
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
                repair_cursor.clear();
                let report =
                    repair_with_health(clients, membership, required, &healthy, repair_cursor)
                        .await?;
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
                    repair_cursor.clear();
                    *last_healthy = current;
                }
                repair_pass_with_health(
                    clients,
                    membership,
                    required,
                    &healthy,
                    repair_cursor,
                    limit,
                )
                .await
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
        Command::Targets { reply } => {
            let _ = reply.send(Ok(clients.iter().map(|client| client.id).collect()));
        }
        Command::DeleteTarget { id, target, reply } => {
            let result = match clients.iter_mut().find(|client| client.id == target) {
                Some(client) => client
                    .client
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
    clients: &mut [WorkerRpcClient],
    cursor: Option<ChunkId>,
    limit: usize,
) -> Result<ChunkPage> {
    if limit == 0 {
        return Err(FluxError::InvalidArg(
            "chunk inventory page limit must be non-zero".into(),
        ));
    }
    // Single global cursor API: merge with per-worker local cursors seeded from
    // the same exclusive after, then advance only keys actually returned.
    let mut local: BTreeMap<u64, ChunkId> = BTreeMap::new();
    if let Some(cursor) = cursor {
        for client in clients.iter() {
            local.insert(client.id.0, cursor);
        }
    }
    let healthy: BTreeMap<u64, usize> = clients
        .iter()
        .enumerate()
        .map(|(index, client)| (client.id.0, index))
        .collect();
    let page = inventory_page_with_holders(clients, &healthy, &mut local, limit).await?;
    Ok(ChunkPage {
        chunks: page.chunks.keys().copied().collect(),
        next_cursor: page.more.then(|| {
            *page
                .chunks
                .keys()
                .next_back()
                .expect("non-empty page when more")
        }),
    })
}

async fn healthy_workers(clients: &mut [WorkerRpcClient]) -> Result<BTreeMap<u64, usize>> {
    let mut healthy = BTreeMap::new();
    for (index, client) in clients.iter_mut().enumerate() {
        if let Ok(response) = client.client.health(HealthRequest {}).await {
            let response = response.into_inner();
            if response.ready && response.worker_id != client.id.0 {
                return Err(FluxError::InvalidArg(format!(
                    "configured worker id {} answered as {}",
                    client.id.0, response.worker_id
                )));
            }
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
    client: &mut WorkerRpcClient,
    expected_worker: u64,
    data: &[u8],
    expected_chunk: ChunkId,
) -> Result<()> {
    let response = client
        .client
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
    clients: &mut [WorkerRpcClient],
    membership: Option<&WorkerMembership>,
    required: usize,
    healthy: &BTreeMap<u64, usize>,
    repair_cursor: &mut BTreeMap<u64, ChunkId>,
) -> Result<RepairReport> {
    repair_cursor.clear();
    let mut checked_chunks = 0usize;
    let mut repaired_replicas = 0usize;
    loop {
        let page = repair_pass_with_health(
            clients,
            membership,
            required,
            healthy,
            repair_cursor,
            REPAIR_PAGE_SIZE,
        )
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
    clients: &mut [WorkerRpcClient],
    membership: Option<&WorkerMembership>,
    required: usize,
    healthy: &BTreeMap<u64, usize>,
    repair_cursor: &mut BTreeMap<u64, ChunkId>,
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

    let page = inventory_page_with_holders(clients, healthy, repair_cursor, limit).await?;
    let checked_chunks = page.chunks.len();
    let mut repaired_replicas = 0usize;

    for (chunk, mut holders) in page.chunks {
        let desired = membership
            .map(|membership| {
                crate::select_worker_targets(membership, &chunk, required, unix_time_millis()).map(
                    |workers| {
                        workers
                            .into_iter()
                            .map(|worker| worker.id.0)
                            .collect::<BTreeSet<_>>()
                    },
                )
            })
            .transpose()?;
        let desired_holders = desired.as_ref().map_or_else(
            || holders.len(),
            |desired| holders.intersection(desired).count(),
        );
        if desired_holders >= required {
            continue;
        }
        let source = holders
            .iter()
            .next()
            .and_then(|worker_id| healthy.get(worker_id))
            .copied()
            .ok_or_else(|| FluxError::Io(format!("no source replica for {}", chunk.to_hex())))?;
        let response = clients[source]
            .client
            .get_chunk(GetChunkRequest {
                chunk_id: chunk.as_bytes().to_vec(),
                promote_cache: false,
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
            let placed = desired.as_ref().map_or_else(
                || holders.len(),
                |desired| holders.intersection(desired).count(),
            );
            if placed >= required {
                break;
            }
            if holders.contains(worker_id) {
                continue;
            }
            if desired
                .as_ref()
                .is_some_and(|desired| !desired.contains(worker_id))
            {
                continue;
            }
            put_one(&mut clients[*index], *worker_id, &response.data, chunk).await?;
            holders.insert(*worker_id);
            repaired_replicas += 1;
        }
        let placed = desired.as_ref().map_or_else(
            || holders.len(),
            |desired| holders.intersection(desired).count(),
        );
        if placed < required {
            return Err(FluxError::Io(format!(
                "repair left {} at {}/{} replicas",
                chunk.to_hex(),
                placed,
                required
            )));
        }
    }

    Ok(RepairReport {
        healthy_workers: healthy.keys().copied().collect(),
        checked_chunks,
        repaired_replicas,
        more: page.more,
    })
}

struct InventoryPage {
    chunks: BTreeMap<ChunkId, BTreeSet<u64>>,
    more: bool,
}

/// K-way merge of per-worker inventory pages with independent exclusive cursors.
///
/// A shared global `after` + truncate-to-limit is incorrect when workers hold
/// disjoint key ranges: keys fetched but truncated from the merged page can be
/// skipped if the walk stops early, and holder sets can be incomplete. Each
/// worker only advances past keys it contributed to an emitted page entry.
async fn inventory_page_with_holders(
    clients: &mut [WorkerRpcClient],
    healthy: &BTreeMap<u64, usize>,
    cursor: &mut BTreeMap<u64, ChunkId>,
    limit: usize,
) -> Result<InventoryPage> {
    let mut buffers: BTreeMap<u64, VecDeque<ChunkId>> = BTreeMap::new();
    let mut may_have_more: BTreeMap<u64, bool> = BTreeMap::new();
    for worker_id in healthy.keys().copied() {
        buffers.insert(worker_id, VecDeque::new());
        may_have_more.insert(worker_id, true);
    }

    let mut page_chunks = BTreeMap::<ChunkId, BTreeSet<u64>>::new();
    while page_chunks.len() < limit {
        for (worker_id, index) in healthy {
            let buf_empty = buffers
                .get(worker_id)
                .is_none_or(std::collections::VecDeque::is_empty);
            if !buf_empty || !may_have_more.get(worker_id).copied().unwrap_or(false) {
                continue;
            }
            let after = cursor.get(worker_id).copied();
            let response = clients[*index]
                .client
                .list_chunks(ListChunksRequest {
                    after_chunk_id: after
                        .map(|chunk| chunk.as_bytes().to_vec())
                        .unwrap_or_default(),
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
            let has_more = !response.next_cursor.is_empty();
            let buf = buffers.get_mut(worker_id).expect("buffer for healthy worker");
            for raw in response.chunk_ids {
                buf.push_back(ChunkId::try_from(raw.as_slice())?);
            }
            if buf.is_empty() {
                may_have_more.insert(*worker_id, false);
            } else {
                may_have_more.insert(*worker_id, has_more);
            }
        }

        let Some(min_key) = buffers
            .values()
            .filter_map(|buf| buf.front().copied())
            .min()
        else {
            break;
        };
        let mut holders = BTreeSet::new();
        for (worker_id, buf) in buffers.iter_mut() {
            if buf.front().copied() == Some(min_key) {
                buf.pop_front();
                holders.insert(*worker_id);
                cursor.insert(*worker_id, min_key);
            }
        }
        page_chunks.insert(min_key, holders);
    }

    let more = page_chunks.len() == limit
        && (buffers.values().any(|buf| !buf.is_empty())
            || may_have_more.values().any(|more| *more));
    Ok(InventoryPage {
        chunks: page_chunks,
        more,
    })
}

#[cfg(test)]
/// Pure k-way merge used by unit tests (same cursor advance rules as the RPC path).
fn take_merged_inventory_page_from_lists(
    worker_lists: &BTreeMap<u64, Vec<ChunkId>>,
    cursor: &mut BTreeMap<u64, ChunkId>,
    limit: usize,
) -> InventoryPage {
    let mut buffers: BTreeMap<u64, VecDeque<ChunkId>> = BTreeMap::new();
    let mut may_have_more: BTreeMap<u64, bool> = BTreeMap::new();
    for worker_id in worker_lists.keys().copied() {
        buffers.insert(worker_id, VecDeque::new());
        may_have_more.insert(worker_id, true);
    }

    let mut page_chunks = BTreeMap::<ChunkId, BTreeSet<u64>>::new();
    while page_chunks.len() < limit {
        for (worker_id, list) in worker_lists {
            let buf_empty = buffers
                .get(worker_id)
                .is_none_or(std::collections::VecDeque::is_empty);
            if !buf_empty || !may_have_more.get(worker_id).copied().unwrap_or(false) {
                continue;
            }
            let after = cursor.get(worker_id).copied();
            let mut page = list
                .iter()
                .copied()
                .filter(|chunk| after.is_none_or(|cursor| *chunk > cursor))
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>();
            let has_more = page.len() > limit;
            page.truncate(limit);
            let buf = buffers.get_mut(worker_id).expect("buffer");
            buf.extend(page.iter().copied());
            if buf.is_empty() {
                may_have_more.insert(*worker_id, false);
            } else {
                may_have_more.insert(*worker_id, has_more);
            }
        }

        let Some(min_key) = buffers
            .values()
            .filter_map(|buf| buf.front().copied())
            .min()
        else {
            break;
        };
        let mut holders = BTreeSet::new();
        for (worker_id, buf) in buffers.iter_mut() {
            if buf.front().copied() == Some(min_key) {
                buf.pop_front();
                holders.insert(*worker_id);
                cursor.insert(*worker_id, min_key);
            }
        }
        page_chunks.insert(min_key, holders);
    }

    let more = page_chunks.len() == limit
        && (buffers.values().any(|buf| !buf.is_empty())
            || may_have_more.values().any(|more| *more));
    InventoryPage {
        chunks: page_chunks,
        more,
    }
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

    fn raw_id(byte: u8) -> ChunkId {
        ChunkId::from_raw([byte; 32])
    }

    #[test]
    fn merged_inventory_uneven_workers_does_not_skip_keys() {
        // gpt56 #27 counterexample: A=[a,c] (exact page, no next), B=[b], limit=2.
        let a = raw_id(1);
        let b = raw_id(2);
        let c = raw_id(3);
        assert!(a < b && b < c);

        let workers = BTreeMap::from([(1u64, vec![a, c]), (2u64, vec![b])]);
        let mut cursor = BTreeMap::new();
        let mut got = Vec::new();
        loop {
            let page = take_merged_inventory_page_from_lists(&workers, &mut cursor, 2);
            got.extend(page.chunks.keys().copied());
            if !page.more {
                break;
            }
        }
        assert_eq!(got, vec![a, b, c]);
    }

    #[test]
    fn merged_inventory_skewed_ranges_cover_all_keys_and_holders() {
        // Disjoint ranges across workers with small page size.
        let keys: Vec<ChunkId> = (1u8..=9).map(raw_id).collect();
        let workers = BTreeMap::from([
            (1u64, vec![keys[0], keys[1], keys[2], keys[6]]), // 1,2,3,7
            (2u64, vec![keys[3], keys[4], keys[5]]),          // 4,5,6
            (3u64, vec![keys[2], keys[7], keys[8]]),          // 3,8,9 (3 shared w/ W1)
        ]);
        let mut cursor = BTreeMap::new();
        let mut got = BTreeMap::<ChunkId, BTreeSet<u64>>::new();
        loop {
            let page = take_merged_inventory_page_from_lists(&workers, &mut cursor, 2);
            for (chunk, holders) in page.chunks {
                assert!(
                    got.insert(chunk, holders.clone()).is_none(),
                    "duplicate chunk {}",
                    chunk.to_hex()
                );
            }
            if !page.more {
                break;
            }
        }
        assert_eq!(got.len(), 9);
        assert_eq!(got.get(&keys[2]).unwrap(), &BTreeSet::from([1u64, 3]));
        for (i, key) in keys.iter().enumerate() {
            assert!(got.contains_key(key), "missing key index {i}");
        }
    }

    #[test]
    fn failed_channel_rebuild_does_not_advance_membership() {
        let old = WorkerMembership::default();
        let refreshed = WorkerMembership {
            epoch: 1,
            workers: Vec::new(),
        };
        let mut membership = Some(old.clone());
        let mut clients = Vec::new();
        let rebuilt = apply_membership_refresh(&mut clients, &mut membership, refreshed, |_| {
            Err(FluxError::InvalidArg("invalid refreshed endpoint".into()))
        });
        assert!(!rebuilt);
        assert_eq!(membership, Some(old));
        assert!(clients.is_empty());
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
            if let Ok(Command::Targets { reply }) = gc_receiver.recv() {
                let _ = reply.send(Ok(vec![
                    WorkerTargetId(0),
                    WorkerTargetId(1),
                    WorkerTargetId(2),
                ]));
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
