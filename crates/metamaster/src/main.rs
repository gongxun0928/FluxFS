use anyhow::{Context, Result};
use clap::Parser;
use fluxfs_meta::{
    start_single_voter, FluxRaft, HeedMetaStore, MetaRaftRequest, MetaRaftResponse, MetaStore,
};
use fluxfs_proto::meta::v1::{
    BeginFlushRequest, BeginFlushResponse, BeginGcRequest, BeginGcResponse, CommitFlushRequest,
    CommitFlushResponse, CommitInodeManifestRequest, CommitInodeManifestResponse, CreateRequest,
    CreateResponse, CurrentGcPlanRequest, CurrentGcPlanResponse, FailFlushConflictRequest,
    FailFlushConflictResponse, FinishGcRequest, FinishGcResponse, GetInodeRequest,
    GetInodeResponse, GetManifestRequest, GetManifestResponse, ImportExternalRequest,
    ImportExternalResponse, ListFlushIntentsRequest, ListFlushIntentsResponse, LookupRequest,
    LookupResponse, PingRequest, PingResponse, PutInodeRequest, PutInodeResponse,
    PutManifestRequest, PutManifestResponse, ReaddirRequest, ReaddirResponse, UnlinkRequest,
    UnlinkResponse,
};
use fluxfs_proto::meta_codec::{
    decode_flush_intent, decode_inode, decode_manifest, decode_ufs_object, encode_dentries,
    encode_flush_intents, encode_gc_plan, encode_inode, encode_manifest, file_type_from_wire,
    status_from_flux,
};
use fluxfs_proto::{MetaService, MetaServiceServer};
use fluxfs_types::{FlushId, GcLeaseId, ManifestId, RequestOpId};
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
            MetaRaftResponse::Err(err) => Err(status_from_flux(err)),
            other => Err(Status::internal(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_resp_empty(resp: MetaRaftResponse) -> std::result::Result<(), Status> {
        match resp {
            MetaRaftResponse::Empty => Ok(()),
            MetaRaftResponse::Err(err) => Err(status_from_flux(err)),
            other => Err(Status::internal(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_resp_manifest_id(resp: MetaRaftResponse) -> std::result::Result<u64, Status> {
        match resp {
            MetaRaftResponse::ManifestId(id) => Ok(id),
            MetaRaftResponse::Err(err) => Err(status_from_flux(err)),
            other => Err(Status::internal(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_resp_gc_plan(
        resp: MetaRaftResponse,
    ) -> std::result::Result<fluxfs_types::GcPlan, Status> {
        match resp {
            MetaRaftResponse::GcPlan(plan) => Ok(*plan),
            MetaRaftResponse::Err(err) => Err(status_from_flux(err)),
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
        let request_id = parse_request_op_id(&r.request_id);
        let resp = self
            .write(MetaRaftRequest::Create {
                request_id,
                parent: r.parent,
                name: r.name,
                file_type: ft,
                mode: r.mode,
                uid: r.uid,
                gid: r.gid,
                expected_parent_generation: parent_gen_cas(r.expected_parent_generation),
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
                request_id: Some(RequestOpId::random()),
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
                request_id: Some(RequestOpId::random()),
                manifest: Box::new(manifest),
            })
            .await?;
        let id = Self::map_resp_manifest_id(resp)?;
        Ok(Response::new(PutManifestResponse { manifest_id: id }))
    }

    async fn commit_inode_manifest(
        &self,
        req: Request<CommitInodeManifestRequest>,
    ) -> Result<Response<CommitInodeManifestResponse>, Status> {
        let r = req.into_inner();
        let inode = decode_inode(&r.inode_json).map_err(status_from_flux)?;
        let manifest = decode_manifest(&r.manifest_json).map_err(status_from_flux)?;
        let request_id = parse_request_op_id(&r.request_id);
        let resp = self
            .write(MetaRaftRequest::CommitInodeManifest {
                request_id,
                expected_generation: r.expected_generation,
                inode: Box::new(inode),
                manifest: Box::new(manifest),
            })
            .await?;
        let inode = Self::map_resp_inode(resp)?;
        let manifest_id = inode
            .manifest_id
            .ok_or_else(|| Status::internal("commit missing manifest_id"))?
            .0;
        Ok(Response::new(CommitInodeManifestResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
            manifest_id,
        }))
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

    async fn begin_flush(
        &self,
        req: Request<BeginFlushRequest>,
    ) -> Result<Response<BeginFlushResponse>, Status> {
        let r = req.into_inner();
        let intent = decode_flush_intent(&r.intent_json).map_err(status_from_flux)?;
        let response = self
            .write(MetaRaftRequest::BeginFlush {
                request_id: parse_request_op_id(&r.request_id),
                expected_generation: r.expected_generation,
                inode: r.inode,
                intent: Box::new(intent),
            })
            .await?;
        let inode = Self::map_resp_inode(response)?;
        Ok(Response::new(BeginFlushResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
        }))
    }

    async fn commit_flush(
        &self,
        req: Request<CommitFlushRequest>,
    ) -> Result<Response<CommitFlushResponse>, Status> {
        let r = req.into_inner();
        let published_ufs = decode_ufs_object(&r.published_ufs_json).map_err(status_from_flux)?;
        let response = self
            .write(MetaRaftRequest::CommitFlush {
                request_id: parse_request_op_id(&r.request_id),
                expected_generation: r.expected_generation,
                inode: r.inode,
                flush_id: FlushId(r.flush_id),
                published_ufs: Box::new(published_ufs),
            })
            .await?;
        let inode = Self::map_resp_inode(response)?;
        Ok(Response::new(CommitFlushResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
        }))
    }

    async fn fail_flush_conflict(
        &self,
        req: Request<FailFlushConflictRequest>,
    ) -> Result<Response<FailFlushConflictResponse>, Status> {
        let r = req.into_inner();
        let response = self
            .write(MetaRaftRequest::FailFlushConflict {
                request_id: parse_request_op_id(&r.request_id),
                expected_generation: r.expected_generation,
                inode: r.inode,
                flush_id: FlushId(r.flush_id),
                error: r.error,
            })
            .await?;
        let inode = Self::map_resp_inode(response)?;
        Ok(Response::new(FailFlushConflictResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
        }))
    }

    async fn list_flush_intents(
        &self,
        _req: Request<ListFlushIntentsRequest>,
    ) -> Result<Response<ListFlushIntentsResponse>, Status> {
        let intents = self.store.list_flush_intents().map_err(status_from_flux)?;
        Ok(Response::new(ListFlushIntentsResponse {
            intents_json: encode_flush_intents(&intents).map_err(status_from_flux)?,
        }))
    }

    async fn begin_gc(
        &self,
        req: Request<BeginGcRequest>,
    ) -> Result<Response<BeginGcResponse>, Status> {
        let r = req.into_inner();
        let response = self
            .write(MetaRaftRequest::BeginGc {
                request_id: parse_request_op_id(&r.request_id),
                lease_id: GcLeaseId(r.lease_id),
            })
            .await?;
        let plan = Self::map_resp_gc_plan(response)?;
        Ok(Response::new(BeginGcResponse {
            plan_json: encode_gc_plan(&plan).map_err(status_from_flux)?,
        }))
    }

    async fn current_gc_plan(
        &self,
        _req: Request<CurrentGcPlanRequest>,
    ) -> Result<Response<CurrentGcPlanResponse>, Status> {
        let plan = self.store.current_gc_plan().map_err(status_from_flux)?;
        let (present, plan_json) = match plan {
            Some(plan) => (true, encode_gc_plan(&plan).map_err(status_from_flux)?),
            None => (false, Vec::new()),
        };
        Ok(Response::new(CurrentGcPlanResponse { present, plan_json }))
    }

    async fn finish_gc(
        &self,
        req: Request<FinishGcRequest>,
    ) -> Result<Response<FinishGcResponse>, Status> {
        let r = req.into_inner();
        let response = self
            .write(MetaRaftRequest::FinishGc {
                request_id: parse_request_op_id(&r.request_id),
                lease_id: GcLeaseId(r.lease_id),
            })
            .await?;
        Self::map_resp_empty(response)?;
        Ok(Response::new(FinishGcResponse {}))
    }

    async fn import_external(
        &self,
        req: Request<ImportExternalRequest>,
    ) -> Result<Response<ImportExternalResponse>, Status> {
        let r = req.into_inner();
        let inode = decode_inode(&r.inode_json).map_err(status_from_flux)?;
        let manifest = if r.manifest_json.is_empty() {
            None
        } else {
            Some(decode_manifest(&r.manifest_json).map_err(status_from_flux)?)
        };
        let resp = self
            .write(MetaRaftRequest::ImportExternal {
                request_id: parse_request_op_id(&r.request_id),
                parent: r.parent,
                name: r.name,
                inode: Box::new(inode),
                manifest: manifest.map(Box::new),
                expected_parent_generation: parent_gen_cas(r.expected_parent_generation),
            })
            .await?;
        let inode = Self::map_resp_inode(resp)?;
        Ok(Response::new(ImportExternalResponse {
            inode_json: encode_inode(&inode).map_err(status_from_flux)?,
        }))
    }

    async fn unlink(
        &self,
        req: Request<UnlinkRequest>,
    ) -> Result<Response<UnlinkResponse>, Status> {
        let r = req.into_inner();
        let resp = self
            .write(MetaRaftRequest::Unlink {
                request_id: Some(RequestOpId::random()),
                parent: r.parent,
                name: r.name,
                expected_parent_generation: parent_gen_cas(r.expected_parent_generation),
            })
            .await?;
        Self::map_resp_empty(resp)?;
        Ok(Response::new(UnlinkResponse {}))
    }
}

fn parent_gen_cas(wire: u64) -> Option<u64> {
    // Proto uses 0 as unset; directory generations start at 1.
    if wire == 0 {
        None
    } else {
        Some(wire)
    }
}

fn parse_request_op_id(s: &str) -> Option<RequestOpId> {
    if s.is_empty() {
        return Some(RequestOpId::random());
    }
    let bytes = hex::decode(s).ok()?;
    let arr: [u8; 16] = bytes.try_into().ok()?;
    Some(RequestOpId::from_bytes(arr))
}

// Minimal hex decode for request ids (lowercase/uppercase).
mod hex {
    pub fn decode(s: &str) -> Result<Vec<u8>, ()> {
        if !s.len().is_multiple_of(2) {
            return Err(());
        }
        let mut out = Vec::with_capacity(s.len() / 2);
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let hi = from_hex(bytes[i])?;
            let lo = from_hex(bytes[i + 1])?;
            out.push((hi << 4) | lo);
            i += 2;
        }
        Ok(out)
    }
    fn from_hex(b: u8) -> Result<u8, ()> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err(()),
        }
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
    let raft_dir = cli.data_dir.join("raft");
    let raft = start_single_voter(store.clone(), &raft_dir, &cli.listen.to_string())
        .await
        .context("start openraft single-voter")?;
    let svc = MetaSvc { store, raft };
    println!(
        "fluxfs-metamaster listening on {} data_dir={} raft=single-voter durable_log={}",
        cli.listen,
        cli.data_dir.display(),
        raft_dir.display()
    );
    tonic::transport::Server::builder()
        .add_service(MetaServiceServer::new(svc))
        .serve(cli.listen)
        .await
        .context("serve")?;
    Ok(())
}
