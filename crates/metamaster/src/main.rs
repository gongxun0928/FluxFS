use anyhow::{Context, Result};
use clap::Parser;
use fluxfs_meta::{
    start_single_voter, FluxRaft, HeedMetaStore, MetaRaftRequest, MetaRaftResponse, MetaStore,
};
use fluxfs_proto::meta::v1::{
    CreateRequest, CreateResponse, GetInodeRequest, GetInodeResponse, GetManifestRequest,
    GetManifestResponse, LookupRequest, LookupResponse, PingRequest, PingResponse, PutInodeRequest,
    PutInodeResponse, PutManifestRequest, PutManifestResponse, ReaddirRequest, ReaddirResponse,
    UnlinkRequest, UnlinkResponse,
};
use fluxfs_proto::meta_codec::{
    decode_inode, decode_manifest, encode_dentries, encode_inode, encode_manifest,
    file_type_from_wire, status_from_flux,
};
use fluxfs_proto::{MetaService, MetaServiceServer};
use fluxfs_types::{FluxError, ManifestId};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Parser, Debug)]
#[command(
    name = "fluxfs-metamaster",
    about = "FluxFS MetaMaster (heed + openraft single-voter + tonic)"
)]
struct Cli {
    /// Persist MetaStore (heed) directory.
    #[arg(long, default_value = "/tmp/fluxfs-meta")]
    data_dir: PathBuf,
    /// Listen address, e.g. 127.0.0.1:50051
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,
}

struct MetaSvc {
    store: Arc<HeedMetaStore>,
    raft: FluxRaft,
}

impl MetaSvc {
    async fn write(&self, req: MetaRaftRequest) -> std::result::Result<MetaRaftResponse, Status> {
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| Status::unavailable(format!("raft write: {e}")))?;
        Ok(resp.data)
    }

    fn map_resp_inode(resp: MetaRaftResponse) -> std::result::Result<fluxfs_types::Inode, Status> {
        match resp {
            MetaRaftResponse::Inode(inode) => Ok(*inode),
            MetaRaftResponse::Err { message } => Err(status_from_flux(FluxError::Meta(message))),
            other => Err(Status::internal(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_resp_empty(resp: MetaRaftResponse) -> std::result::Result<(), Status> {
        match resp {
            MetaRaftResponse::Empty => Ok(()),
            MetaRaftResponse::Err { message } => Err(status_from_flux(FluxError::Meta(message))),
            other => Err(Status::internal(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_resp_manifest_id(resp: MetaRaftResponse) -> std::result::Result<u64, Status> {
        match resp {
            MetaRaftResponse::ManifestId(id) => Ok(id),
            MetaRaftResponse::Err { message } => Err(status_from_flux(FluxError::Meta(message))),
            other => Err(Status::internal(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }
}

#[tonic::async_trait]
impl MetaService for MetaSvc {
    async fn ping(&self, _req: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            version: env!("CARGO_PKG_VERSION").into(),
        }))
    }

    async fn get_inode(
        &self,
        req: Request<GetInodeRequest>,
    ) -> Result<Response<GetInodeResponse>, Status> {
        let id = req.into_inner().id;
        let inode = self.store.get_inode(id).map_err(status_from_flux)?;
        Ok(Response::new(GetInodeResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
        }))
    }

    async fn lookup(
        &self,
        req: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let r = req.into_inner();
        let inode = self
            .store
            .lookup(r.parent, &r.name)
            .map_err(status_from_flux)?;
        Ok(Response::new(LookupResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
        }))
    }

    async fn create(
        &self,
        req: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let r = req.into_inner();
        let ft = file_type_from_wire(r.file_type).map_err(status_from_flux)?;
        let resp = self
            .write(MetaRaftRequest::Create {
                parent: r.parent,
                name: r.name,
                file_type: ft,
                mode: r.mode,
                uid: r.uid,
                gid: r.gid,
            })
            .await?;
        let inode = Self::map_resp_inode(resp)?;
        Ok(Response::new(CreateResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
        }))
    }

    async fn readdir(
        &self,
        req: Request<ReaddirRequest>,
    ) -> Result<Response<ReaddirResponse>, Status> {
        let dir = req.into_inner().dir;
        let dentries = self.store.readdir(dir).map_err(status_from_flux)?;
        Ok(Response::new(ReaddirResponse {
            dentries_json: encode_dentries(&dentries).map_err(status_from_flux)?,
        }))
    }

    async fn put_inode(
        &self,
        req: Request<PutInodeRequest>,
    ) -> Result<Response<PutInodeResponse>, Status> {
        let inode = decode_inode(&req.into_inner().inode_json).map_err(status_from_flux)?;
        let resp = self
            .write(MetaRaftRequest::PutInode {
                inode: Box::new(inode),
            })
            .await?;
        Self::map_resp_empty(resp)?;
        Ok(Response::new(PutInodeResponse {}))
    }

    async fn put_manifest(
        &self,
        req: Request<PutManifestRequest>,
    ) -> Result<Response<PutManifestResponse>, Status> {
        let manifest =
            decode_manifest(&req.into_inner().manifest_json).map_err(status_from_flux)?;
        let resp = self
            .write(MetaRaftRequest::PutManifest {
                manifest: Box::new(manifest),
            })
            .await?;
        let id = Self::map_resp_manifest_id(resp)?;
        Ok(Response::new(PutManifestResponse { manifest_id: id }))
    }

    async fn get_manifest(
        &self,
        req: Request<GetManifestRequest>,
    ) -> Result<Response<GetManifestResponse>, Status> {
        let id = ManifestId(req.into_inner().id);
        let manifest = self.store.get_manifest(id).map_err(status_from_flux)?;
        Ok(Response::new(GetManifestResponse {
            manifest_json: encode_manifest(&manifest).map_err(status_from_flux)?,
        }))
    }

    async fn unlink(
        &self,
        req: Request<UnlinkRequest>,
    ) -> Result<Response<UnlinkResponse>, Status> {
        let r = req.into_inner();
        let resp = self
            .write(MetaRaftRequest::Unlink {
                parent: r.parent,
                name: r.name,
            })
            .await?;
        Self::map_resp_empty(resp)?;
        Ok(Response::new(UnlinkResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    std::fs::create_dir_all(&cli.data_dir)?;
    let store = Arc::new(HeedMetaStore::open(&cli.data_dir).context("open heed meta")?);
    let raft = start_single_voter(store.clone(), &cli.listen.to_string())
        .await
        .context("start openraft single-voter")?;
    let svc = MetaSvc { store, raft };
    println!(
        "fluxfs-metamaster listening on {} data_dir={} raft=single-voter",
        cli.listen,
        cli.data_dir.display()
    );
    tonic::transport::Server::builder()
        .add_service(MetaServiceServer::new(svc))
        .serve(cli.listen)
        .await
        .context("serve")?;
    Ok(())
}
