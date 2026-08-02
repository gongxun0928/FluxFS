use anyhow::{Context, Result};
use clap::Parser;
use fluxfs_chunk::{ChunkStore, DiskChunkStore};
use fluxfs_metrics::{spawn_prometheus, FluxMetrics};
use fluxfs_proto::chunk::v1::{
    ContainsChunkRequest, ContainsChunkResponse, DeleteChunkRequest, DeleteChunkResponse,
    GetChunkRequest, GetChunkResponse, HealthRequest, HealthResponse, ListChunksRequest,
    ListChunksResponse, PutChunkRequest, PutChunkResponse,
};
use fluxfs_proto::{ChunkWorker, ChunkWorkerServer};
use fluxfs_types::{ChunkId, FluxError, CHUNK_SIZE};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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
}

struct ChunkSvc {
    worker_id: u64,
    store: Arc<DiskChunkStore>,
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
        FluxMetrics::inc(&self.metrics.chunk_rpc_total);
        let _permit = self.try_enter().inspect_err(|_status| {
            FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
        })?;
        let chunk =
            ChunkId::try_from(request.into_inner().chunk_id.as_slice()).map_err(|error| {
                FluxMetrics::inc(&self.metrics.chunk_rpc_error_total);
                status_from_flux(error)
            })?;
        let store = Arc::clone(&self.store);
        let data = tokio::task::spawn_blocking(move || store.get(&chunk))
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
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            worker_id: self.worker_id,
            ready: true,
        }))
    }

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    if cli.max_in_flight == 0 || cli.gc_max_in_flight == 0 {
        anyhow::bail!("--max-in-flight and --gc-max-in-flight must be greater than zero");
    }
    let store = Arc::new(DiskChunkStore::open(&cli.data_dir).context("open chunk store")?);
    let metrics = FluxMetrics::new();
    if let Some(addr) = cli.metrics_listen {
        spawn_prometheus(addr, Arc::clone(&metrics));
        println!("fluxfs-chunkworker metrics on http://{addr}/metrics");
    }
    let service = ChunkSvc {
        worker_id: cli.worker_id,
        store,
        in_flight: Arc::new(Semaphore::new(cli.max_in_flight)),
        gc_in_flight: Arc::new(Semaphore::new(cli.gc_max_in_flight)),
        metrics,
    };
    println!(
        "fluxfs-chunkworker id={} listening on {} data_dir={} max_in_flight={}",
        cli.worker_id,
        cli.listen,
        cli.data_dir.display(),
        cli.max_in_flight
    );
    tonic::transport::Server::builder()
        .add_service(
            ChunkWorkerServer::new(service)
                .max_decoding_message_size(MAX_CHUNK_RPC_MESSAGE)
                .max_encoding_message_size(MAX_CHUNK_RPC_MESSAGE),
        )
        .serve(cli.listen)
        .await
        .context("serve chunk worker")?;
    Ok(())
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
