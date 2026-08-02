use anyhow::{Context, Result};
use clap::Parser;
use fluxfs_meta::{HeedMetaStore, MetaStore};
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
use fluxfs_types::ManifestId;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Parser, Debug)]
#[command(name = "fluxfs-metamaster", about = "FluxFS MetaMaster (heed + tonic)")]
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
        let inode = self
            .store
            .create(r.parent, &r.name, ft, r.mode, r.uid, r.gid)
            .map_err(status_from_flux)?;
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
        self.store.put_inode(&inode).map_err(status_from_flux)?;
        Ok(Response::new(PutInodeResponse {}))
    }

    async fn put_manifest(
        &self,
        req: Request<PutManifestRequest>,
    ) -> Result<Response<PutManifestResponse>, Status> {
        let manifest =
            decode_manifest(&req.into_inner().manifest_json).map_err(status_from_flux)?;
        let id = self
            .store
            .put_manifest(&manifest)
            .map_err(status_from_flux)?;
        Ok(Response::new(PutManifestResponse { manifest_id: id.0 }))
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
        self.store
            .unlink(r.parent, &r.name)
            .map_err(status_from_flux)?;
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
    let svc = MetaSvc { store };
    println!(
        "fluxfs-metamaster listening on {} data_dir={}",
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
