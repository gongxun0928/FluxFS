use anyhow::{Context, Result};
use clap::Parser;
use fluxfs_chunk::{ChunkStore, DiskChunkStore};
use fluxfs_proto::chunk::v1::{
    ContainsChunkRequest, ContainsChunkResponse, GetChunkRequest, GetChunkResponse, HealthRequest,
    HealthResponse, ListChunksRequest, ListChunksResponse, PutChunkRequest, PutChunkResponse,
};
use fluxfs_proto::{ChunkWorker, ChunkWorkerServer};
use fluxfs_types::{ChunkId, FluxError, CHUNK_SIZE};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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
}

struct ChunkSvc {
    worker_id: u64,
    store: Arc<DiskChunkStore>,
}

#[tonic::async_trait]
impl ChunkWorker for ChunkSvc {
    async fn put_chunk(
        &self,
        request: Request<PutChunkRequest>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        let data = request.into_inner().data;
        let store = Arc::clone(&self.store);
        let chunk = tokio::task::spawn_blocking(move || store.put(&data))
            .await
            .map_err(|error| Status::internal(format!("chunk put task: {error}")))?
            .map_err(status_from_flux)?;
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
        let chunk = ChunkId::try_from(request.into_inner().chunk_id.as_slice())
            .map_err(status_from_flux)?;
        let store = Arc::clone(&self.store);
        let data = tokio::task::spawn_blocking(move || store.get(&chunk))
            .await
            .map_err(|error| Status::internal(format!("chunk get task: {error}")))?
            .map_err(status_from_flux)?;
        Ok(Response::new(GetChunkResponse {
            data,
            worker_id: self.worker_id,
        }))
    }

    async fn contains_chunk(
        &self,
        request: Request<ContainsChunkRequest>,
    ) -> Result<Response<ContainsChunkResponse>, Status> {
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
        _request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let store = Arc::clone(&self.store);
        let chunks = tokio::task::spawn_blocking(move || store.list_chunks())
            .await
            .map_err(|error| Status::internal(format!("chunk inventory task: {error}")))?
            .map_err(status_from_flux)?;
        Ok(Response::new(ListChunksResponse {
            chunk_ids: chunks
                .into_iter()
                .map(|chunk| chunk.as_bytes().to_vec())
                .collect(),
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
    let store = Arc::new(DiskChunkStore::open(&cli.data_dir).context("open chunk store")?);
    let service = ChunkSvc {
        worker_id: cli.worker_id,
        store,
    };
    println!(
        "fluxfs-chunkworker id={} listening on {} data_dir={}",
        cli.worker_id,
        cli.listen,
        cli.data_dir.display()
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
