use anyhow::{Context, Result};
use clap::Parser;
use fluxfs_chunk::{ChunkStore, FoyerCacheConfig, FoyerChunkStore};
use fluxfs_meta::{MetaStore, RemoteMetaStore};
use fluxfs_metrics::{spawn_prometheus, FluxMetrics};
use fluxfs_proto::chunk::v1::{
    ContainsChunkRequest, ContainsChunkResponse, DeleteChunkRequest, DeleteChunkResponse,
    GetChunkRequest, GetChunkResponse, HealthRequest, HealthResponse, ListChunksRequest,
    ListChunksResponse, PutChunkRequest, PutChunkResponse,
};
use fluxfs_proto::{ChunkWorker, ChunkWorkerServer};
use fluxfs_types::{ChunkId, FluxError, WorkerRegistration, WorkerTargetId, CHUNK_SIZE};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

const MAX_CHUNK_RPC_MESSAGE: usize = CHUNK_SIZE as usize + 64 * 1024;

#[derive(Parser, Debug)]
#[command(name = "fluxfs-chunkworker", about = "FluxFS durable ChunkWorker")]
struct Cli {
    #[arg(long)]
    worker_id: u64,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long)]
    listen: SocketAddr,
    /// Maximum concurrent data operations. Excess RPCs fail fast with RESOURCE_EXHAUSTED.
    #[arg(long, default_value_t = 128)]
    max_in_flight: usize,
    /// Independent low-priority GC operations; never consume foreground permits.
    #[arg(long, default_value_t = 1)]
    gc_max_in_flight: usize,
    /// Optional Prometheus text endpoint, e.g. 127.0.0.1:9102 (`GET /metrics`).
    #[arg(long)]
    metrics_listen: Option<SocketAddr>,
    /// Background pack compaction interval. `0` disables. Compacts when more
    /// than one segment file exists (reclaims delete holes without blocking
    /// foreground put/get — compaction holds the pack write lock briefly).
    #[arg(long, default_value_t = 300)]
    compact_interval_secs: u64,
    /// Clean/hot HybridCache DRAM capacity (task #29 / P0-B8). Dirty PutChunk
    /// remains packfile-authoritative and is not write-through to this cache.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    cache_memory_bytes: usize,
    /// Clean/hot HybridCache SSD tier capacity. `0` = memory-only.
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    cache_disk_bytes: usize,
    /// Directory for the foyer SSD device. Defaults to `<data-dir>/foyer-cache`.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    // ===== C1 mTLS (task #30) =====
    /// Cluster CA cert (PEM) used to verify client certs. Required when
    /// --tls-server-cert is set (mTLS); production default.
    #[arg(long)]
    tls_ca_cert: Option<PathBuf>,
    /// Server identity cert (PEM). Setting this enables TLS.
    #[arg(long)]
    tls_server_cert: Option<PathBuf>,
    /// Server identity key (PEM). Paired with --tls-server-cert.
    #[arg(long)]
    tls_server_key: Option<PathBuf>,
    /// Explicit plaintext opt-in (tests only). Production MUST pass TLS flags.
    #[arg(long, default_value_t = false)]
    allow_insecure_dev: bool,
    /// MetaMaster address used for durable membership registration/heartbeats.
    #[arg(long)]
    meta_addr: Option<String>,
    /// Client-reachable tonic endpoint. Defaults to `http://<listen>`.
    #[arg(long)]
    advertise_endpoint: Option<String>,
    #[arg(long, default_value = "local")]
    failure_domain: String,
    /// Administrative capacity advertised to placement.
    #[arg(long, default_value_t = 1 << 40)]
    capacity_bytes: u64,
    /// Initial available capacity. Defaults to capacity.
    #[arg(long)]
    available_bytes: Option<u64>,
    #[arg(long, default_value_t = 5)]
    heartbeat_interval_secs: u64,
    #[arg(long, default_value_t = 15)]
    lease_secs: u64,
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn registration(cli: &Cli) -> Result<WorkerRegistration> {
    let lease_ms = cli
        .lease_secs
        .checked_mul(1000)
        .context("--lease-secs overflow")?;
    let registration = WorkerRegistration {
        id: WorkerTargetId(cli.worker_id),
        endpoint: cli.advertise_endpoint.clone().unwrap_or_else(|| {
            let scheme = if cli.tls_server_cert.is_some() {
                "https"
            } else {
                "http"
            };
            format!("{scheme}://{}", cli.listen)
        }),
        failure_domain: cli.failure_domain.clone(),
        capacity_bytes: cli.capacity_bytes,
        available_bytes: cli.available_bytes.unwrap_or(cli.capacity_bytes),
        lease_deadline_ms: unix_time_millis().saturating_add(lease_ms),
    };
    registration.validate().map_err(anyhow::Error::msg)?;
    Ok(registration)
}

fn client_tls(cli: &Cli) -> Result<Option<fluxfs_tls::ClientTlsOptions>> {
    let opts = fluxfs_tls::ClientTlsOptions::from_cli(
        cli.tls_ca_cert.clone(),
        cli.tls_server_cert.clone(),
        cli.tls_server_key.clone(),
        None,
    )
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(opts.enabled.then_some(opts))
}

struct ChunkSvc {
    worker_id: u64,
    store: Arc<FoyerChunkStore>,
    in_flight: Arc<Semaphore>,
    gc_in_flight: Arc<Semaphore>,
    metrics: Arc<FluxMetrics>,
}

impl ChunkSvc {
    fn try_enter(&self) -> Result<OwnedSemaphorePermit, Status> {
        try_enter(&self.in_flight)
    }

    fn try_enter_gc(&self) -> Result<OwnedSemaphorePermit, Status> {
        try_enter(&self.gc_in_flight)
    }
}

fn try_enter(in_flight: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, Status> {
    Arc::clone(in_flight)
        .try_acquire_owned()
        .map_err(|_| Status::resource_exhausted("chunk worker in-flight limit reached"))
}

#[tonic::async_trait]
impl ChunkWorker for ChunkSvc {
    async fn put_chunk(
        &self,
        request: Request<PutChunkRequest>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        fluxfs_tls::require_in_extensions(&request, fluxfs_types::auth::Capability::PutChunk)?;
        FluxMetrics::inc(&self.metrics.chunk_rpc_total);
        let _permit = self.try_enter().inspect_err(|_status| {
            FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
        })?;
        let data = request.into_inner().data;
        let nbytes = data.len() as u64;
        let store = Arc::clone(&self.store);
        let chunk = tokio::task::spawn_blocking(move || store.put(&data))
            .await
            .map_err(|error| {
                FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
                Status::internal(format!("chunk put task: {error}"))
            })?
            .map_err(|error| {
                FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
                status_from_flux(error)
            })?;
        FluxMetrics::add(&self.metrics.chunk_put_bytes_total, nbytes);
        Ok(Response::new(PutChunkResponse {
            chunk_id: chunk.as_bytes().to_vec(),
            worker_id: self.worker_id,
            durable: true,
        }))
    }

    async fn get_chunk(
        &self,
        request: Request<GetChunkRequest>,
    ) -> Result<Response<GetChunkResponse>, Status> {
        fluxfs_tls::require_in_extensions(&request, fluxfs_types::auth::Capability::GetChunk)?;
        FluxMetrics::inc(&self.metrics.chunk_rpc_total);
        let _permit = self.try_enter().inspect_err(|_status| {
            FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
        })?;
        let req = request.into_inner();
        let promote_cache = req.promote_cache;
        let chunk = ChunkId::try_from(req.chunk_id.as_slice()).map_err(|error| {
            FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
            status_from_flux(error)
        })?;
        let store = Arc::clone(&self.store);
        let data = tokio::task::spawn_blocking(move || store.get_with_promote(&chunk, promote_cache))
            .await
            .map_err(|error| {
                FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
                Status::internal(format!("chunk get task: {error}"))
            })?
            .map_err(|error| {
                FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
                status_from_flux(error)
            })?;
        Ok(Response::new(GetChunkResponse {
            data,
            worker_id: self.worker_id,
        }))
    }

    async fn contains_chunk(
        &self,
        request: Request<ContainsChunkRequest>,
    ) -> Result<Response<ContainsChunkResponse>, Status> {
        fluxfs_tls::require_in_extensions(&request, fluxfs_types::auth::Capability::GetChunk)?;
        let _permit = self.try_enter()?;
        let chunk = ChunkId::try_from(request.into_inner().chunk_id.as_slice())
            .map_err(status_from_flux)?;
        let store = Arc::clone(&self.store);
        let present = tokio::task::spawn_blocking(move || store.contains(&chunk))
            .await
            .map_err(|error| Status::internal(format!("chunk contains task: {error}")))?
            .map_err(status_from_flux)?;
        Ok(Response::new(ContainsChunkResponse {
            present,
            worker_id: self.worker_id,
        }))
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        // Health is called by orchestration liveness probes; require any
        // authenticated principal (ReadMeta is held by both meta and
        // client-admin, the two roles admitted by for_worker()).
        fluxfs_tls::require_in_extensions(&request, fluxfs_types::auth::Capability::ReadMeta)?;
        Ok(Response::new(HealthResponse {
            worker_id: self.worker_id,
            ready: true,
        }))
    }

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        fluxfs_tls::require_in_extensions(&request, fluxfs_types::auth::Capability::GetChunk)?;
        let _permit = self.try_enter_gc()?;
        let request = request.into_inner();
        let limit = usize::try_from(request.limit)
            .ok()
            .filter(|limit| *limit > 0)
            .ok_or_else(|| Status::invalid_argument("inventory limit must be non-zero"))?;
        let cursor = if request.after_chunk_id.is_empty() {
            None
        } else {
            Some(ChunkId::try_from(request.after_chunk_id.as_slice()).map_err(status_from_flux)?)
        };
        let store = Arc::clone(&self.store);
        let page = tokio::task::spawn_blocking(move || store.list_chunks_page(cursor, limit))
            .await
            .map_err(|error| Status::internal(format!("chunk inventory task: {error}")))?
            .map_err(status_from_flux)?;
        Ok(Response::new(ListChunksResponse {
            chunk_ids: page
                .chunks
                .into_iter()
                .map(|chunk| chunk.as_bytes().to_vec())
                .collect(),
            worker_id: self.worker_id,
            next_cursor: page
                .next_cursor
                .map(|chunk| chunk.as_bytes().to_vec())
                .unwrap_or_default(),
        }))
    }

    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        fluxfs_tls::require_in_extensions(&request, fluxfs_types::auth::Capability::DeleteChunk)?;
        let _permit = self.try_enter_gc()?;
        let chunk = ChunkId::try_from(request.into_inner().chunk_id.as_slice())
            .map_err(status_from_flux)?;
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || store.delete(&chunk))
            .await
            .map_err(|error| Status::internal(format!("chunk delete task: {error}")))?
            .map_err(status_from_flux)?;
        Ok(Response::new(DeleteChunkResponse {
            worker_id: self.worker_id,
        }))
    }
}

fn status_from_flux(error: FluxError) -> Status {
    match error {
        FluxError::NotFound => Status::not_found("chunk not found"),
        FluxError::InvalidArg(message) => Status::invalid_argument(message),
        FluxError::Busy => Status::unavailable("chunk worker busy"),
        other => Status::internal(other.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    fluxfs_tls::install_crypto_provider();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    if cli.max_in_flight == 0 || cli.gc_max_in_flight == 0 {
        anyhow::bail!("--max-in-flight and --gc-max-in-flight must be greater than zero");
    }
    if cli.heartbeat_interval_secs == 0 || cli.lease_secs <= cli.heartbeat_interval_secs {
        anyhow::bail!("--lease-secs must exceed a non-zero --heartbeat-interval-secs");
    }
    let cache_dir = cli
        .cache_dir
        .clone()
        .unwrap_or_else(|| cli.data_dir.join("foyer-cache"));
    let cache_cfg = FoyerCacheConfig::new(
        cache_dir.clone(),
        cli.cache_memory_bytes,
        cli.cache_disk_bytes,
    );
    let store = Arc::new(
        FoyerChunkStore::open(&cli.data_dir, cache_cfg)
            .await
            .context("open chunk store + foyer HybridCache")?,
    );
    let metrics = FluxMetrics::new();
    if let Some(addr) = cli.metrics_listen {
        spawn_prometheus(addr, Arc::clone(&metrics));
        println!("fluxfs-chunkworker metrics on http://{addr}/metrics");
    }
    if cli.compact_interval_secs > 0 {
        let compact_store = Arc::clone(&store);
        let interval = Duration::from_secs(cli.compact_interval_secs);
        std::thread::Builder::new()
            .name("fluxfs-chunk-compact".into())
            .spawn(move || loop {
                std::thread::sleep(interval);
                match compact_store.segment_file_count() {
                    Ok(n) if n > 1 => match compact_store.compact() {
                        Ok(report) => tracing::info!(
                            live = report.live_chunks,
                            removed = report.removed_segments,
                            "background pack compaction"
                        ),
                        Err(error) => tracing::warn!(%error, "background pack compaction failed"),
                    },
                    Ok(_) => {}
                    Err(error) => tracing::warn!(%error, "segment_file_count failed"),
                }
            })
            .context("spawn pack compaction thread")?;
    }
    let service = ChunkSvc {
        worker_id: cli.worker_id,
        store,
        in_flight: Arc::new(Semaphore::new(cli.max_in_flight)),
        gc_in_flight: Arc::new(Semaphore::new(cli.gc_max_in_flight)),
        metrics,
    };
    let heartbeat = if let Some(meta_addr) = &cli.meta_addr {
        let meta = Arc::new(
            RemoteMetaStore::connect_tls(meta_addr, client_tls(&cli)?, cli.allow_insecure_dev)
                .context("connect MetaMaster")?,
        );
        let initial = registration(&cli)?;
        meta.register_worker(&initial)
            .context("initial Worker membership registration")?;
        let template = initial;
        let lease_ms = cli.lease_secs * 1000;
        let interval = Duration::from_secs(cli.heartbeat_interval_secs);
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let mut next = template.clone();
                next.lease_deadline_ms = unix_time_millis().saturating_add(lease_ms);
                if let Err(error) = meta.register_worker(&next) {
                    tracing::warn!(%error, "Worker membership heartbeat failed");
                }
            }
        }))
    } else {
        None
    };
    println!(
        "fluxfs-chunkworker id={} listening on {} data_dir={} cache_dir={} cache_memory_bytes={} cache_disk_bytes={} max_in_flight={} compact_interval_secs={}",
        cli.worker_id,
        cli.listen,
        cli.data_dir.display(),
        cache_dir.display(),
        cli.cache_memory_bytes,
        cli.cache_disk_bytes,
        cli.max_in_flight,
        cli.compact_interval_secs
    );
    // ===== C1 mTLS wiring (task #30 Phase 2 + Phase 3 authz) =====
    use fluxfs_tls::{AuthzInterceptor, ServerTlsOptions};
    let tls_opts = ServerTlsOptions::from_cli(
        cli.tls_ca_cert.clone(),
        cli.tls_server_cert.clone(),
        cli.tls_server_key.clone(),
        cli.allow_insecure_dev,
    )
    .context("tls options")?;
    let tls_config = tls_opts
        .build_config()
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let result = if let Some(tls) = tls_config {
        tracing::info!(
            "chunkworker TLS enabled (mTLS require-client-cert={}) + Phase 3 authz interceptor",
            !tls_opts.allow_no_client_cert
        );
        // Phase 3: role-level authz. Worker accepts Meta + ClientAdmin dials.
        // Per-RPC capability is enforced per-handler via
        // `require_in_extensions`.
        let authz = std::sync::Arc::new(AuthzInterceptor::for_worker());
        tonic::transport::Server::builder()
            .tls_config(tls)
            .context("tls_config")?
            .layer(tonic::service::InterceptorLayer::new(move |req| {
                authz.check_and_attach(req)
            }))
            .add_service(
                ChunkWorkerServer::new(service)
                    .max_decoding_message_size(MAX_CHUNK_RPC_MESSAGE)
                    .max_encoding_message_size(MAX_CHUNK_RPC_MESSAGE),
            )
            .serve(cli.listen)
            .await
            .context("serve chunk worker")
    } else {
        tracing::warn!(
            "chunkworker in INSECURE-DEV plaintext mode (--allow-insecure-dev); injecting dev-bootstrap Principal (ClientAdmin caps) so per-handler require(cap) still runs"
        );
        tonic::transport::Server::builder()
            .layer(fluxfs_tls::AuthzInterceptor::dev_bootstrap_layer())
            .add_service(
                ChunkWorkerServer::new(service)
                    .max_decoding_message_size(MAX_CHUNK_RPC_MESSAGE)
                    .max_encoding_message_size(MAX_CHUNK_RPC_MESSAGE),
            )
            .serve(cli.listen)
            .await
            .context("serve chunk worker")
    };
    if let Some(task) = heartbeat {
        task.abort();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_flight_gate_rejects_without_queueing() {
        let gate = Arc::new(Semaphore::new(1));
        let _held = try_enter(&gate).unwrap();
        let error = try_enter(&gate).unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }
}
