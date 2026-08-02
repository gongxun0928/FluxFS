//! Sync [`MetaStore`] façade over tonic MetaService (for FUSE / CLI).

use crate::store::MetaStore;
use fluxfs_proto::meta::v1::{
    BeginFlushRequest, BeginGcRequest, CommitFlushRequest, CommitInodeManifestRequest,
    CreateRequest, CurrentGcPlanRequest, FailFlushConflictRequest, FinishGcRequest,
    GetInodeRequest, GetManifestRequest, ImportExternalRequest, ListFlushIntentsRequest,
    LookupRequest, PutInodeRequest, PutManifestRequest, ReaddirRequest, UnlinkRequest,
};
use fluxfs_proto::meta_codec::{
    decode_dentries, decode_flush_intents, decode_gc_plan, decode_inode, decode_manifest,
    encode_flush_intent, encode_inode, encode_manifest, encode_ufs_object, file_type_to_wire,
    flux_from_status,
};
use fluxfs_proto::MetaServiceClient;
use fluxfs_types::{
    Dentry, FileType, FlushId, FlushIntent, FluxError, GcLeaseId, GcPlan, Inode, InodeId, Manifest,
    ManifestId, RequestOpId, Result, UfsObject, ROOT_INODE,
};
use std::future::Future;
use std::sync::Mutex;
use tokio::runtime::{Handle, Runtime};
use tonic::transport::Channel;

pub struct RemoteMetaStore {
    /// Owned runtime only when constructed outside an existing Tokio context.
    rt: Option<Runtime>,
    client: Mutex<MetaServiceClient<Channel>>,
}

impl RemoteMetaStore {
    pub fn connect(addr: impl AsRef<str>) -> Result<Self> {
        let addr = addr.as_ref().to_string();
        let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else {
            format!("http://{addr}")
        };

        let (rt, client) = if let Ok(handle) = Handle::try_current() {
            let client = tokio::task::block_in_place(|| {
                handle.block_on(MetaServiceClient::connect(endpoint.clone()))
            })
            .map_err(|e| FluxError::Meta(format!("meta connect: {e}")))?;
            (None, client)
        } else {
            let rt = Runtime::new().map_err(|e| FluxError::Meta(e.to_string()))?;
            let client = rt
                .block_on(MetaServiceClient::connect(endpoint))
                .map_err(|e| FluxError::Meta(format!("meta connect: {e}")))?;
            (Some(rt), client)
        };

        Ok(Self {
            rt,
            client: Mutex::new(client),
        })
    }

    fn client(&self) -> Result<MetaServiceClient<Channel>> {
        self.client
            .lock()
            .map(|g| g.clone())
            .map_err(|_| FluxError::Meta("meta client lock poisoned".into()))
    }

    fn block_on<T>(&self, fut: impl Future<Output = T>) -> T {
        if let Ok(handle) = Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else if let Some(rt) = &self.rt {
            rt.block_on(fut)
        } else {
            // Constructed under a runtime that has since exited — spin a one-shot.
            let rt = Runtime::new().expect("create temporary runtime");
            rt.block_on(fut)
        }
    }
}

impl MetaStore for RemoteMetaStore {
    fn root(&self) -> InodeId {
        ROOT_INODE
    }

    fn get_inode(&self, id: InodeId) -> Result<Inode> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.get_inode(GetInodeRequest { id }).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async {
                c.lookup(LookupRequest {
                    parent,
                    name: name.to_string(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn create_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async {
                c.create(CreateRequest {
                    parent,
                    name: name.to_string(),
                    file_type: file_type_to_wire(file_type),
                    mode,
                    uid,
                    gid,
                    request_id: RequestOpId::random().to_hex(),
                    expected_parent_generation: expected_parent_generation.unwrap_or(0),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn readdir(&self, dir: InodeId) -> Result<Vec<Dentry>> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.readdir(ReaddirRequest { dir }).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_dentries(&resp.dentries_json)
    }

    fn put_inode(&self, inode: &Inode) -> Result<()> {
        let inode_json = encode_inode(inode)?;
        let mut c = self.client()?;
        self.block_on(async { c.put_inode(PutInodeRequest { inode_json }).await })
            .map_err(flux_from_status)?;
        Ok(())
    }

    fn put_manifest(&self, manifest: &Manifest) -> Result<ManifestId> {
        let manifest_json = encode_manifest(manifest)?;
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.put_manifest(PutManifestRequest { manifest_json }).await })
            .map_err(flux_from_status)?
            .into_inner();
        Ok(ManifestId(resp.manifest_id))
    }

    fn commit_inode_manifest_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        let inode_json = encode_inode(inode)?;
        let manifest_json = encode_manifest(manifest)?;
        let mut c = self.client()?;
        let resp = self
            .block_on(async {
                c.commit_inode_manifest(CommitInodeManifestRequest {
                    expected_generation,
                    inode_json,
                    manifest_json,
                    request_id: op_id.to_hex(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.get_manifest(GetManifestRequest { id: id.0 }).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_manifest(&resp.manifest_json)
    }

    fn begin_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        intent: &FlushIntent,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let request = BeginFlushRequest {
            inode,
            expected_generation,
            intent_json: encode_flush_intent(intent)?,
            request_id: op_id.to_hex(),
        };
        let response = self
            .block_on(async { c.begin_flush(request).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn commit_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        published_ufs: &UfsObject,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let request = CommitFlushRequest {
            inode,
            expected_generation,
            flush_id: flush_id.0,
            published_ufs_json: encode_ufs_object(published_ufs)?,
            request_id: op_id.to_hex(),
        };
        let response = self
            .block_on(async { c.commit_flush(request).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn fail_flush_conflict(
        &self,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        error: &str,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let request = FailFlushConflictRequest {
            inode,
            expected_generation,
            flush_id: flush_id.0,
            error: error.to_string(),
            request_id: RequestOpId::random().to_hex(),
        };
        let response = self
            .block_on(async { c.fail_flush_conflict(request).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn list_flush_intents(&self) -> Result<Vec<(InodeId, FlushIntent)>> {
        let mut c = self.client()?;
        let response = self
            .block_on(async { c.list_flush_intents(ListFlushIntentsRequest {}).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_flush_intents(&response.intents_json)
    }

    fn begin_gc(&self, lease_id: GcLeaseId) -> Result<GcPlan> {
        let mut c = self.client()?;
        let response = self
            .block_on(async {
                c.begin_gc(BeginGcRequest {
                    lease_id: lease_id.0,
                    request_id: RequestOpId::random().to_hex(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_gc_plan(&response.plan_json)
    }

    fn current_gc_plan(&self) -> Result<Option<GcPlan>> {
        let mut c = self.client()?;
        let response = self
            .block_on(async { c.current_gc_plan(CurrentGcPlanRequest {}).await })
            .map_err(flux_from_status)?
            .into_inner();
        if response.present {
            decode_gc_plan(&response.plan_json).map(Some)
        } else {
            Ok(None)
        }
    }

    fn finish_gc(&self, lease_id: GcLeaseId) -> Result<()> {
        let mut c = self.client()?;
        self.block_on(async {
            c.finish_gc(FinishGcRequest {
                lease_id: lease_id.0,
                request_id: RequestOpId::random().to_hex(),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn import_external_with_id(
        &self,
        op_id: RequestOpId,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        inode: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode> {
        let inode_json = encode_inode(inode)?;
        let manifest_json = match manifest {
            Some(m) => encode_manifest(m)?,
            None => Vec::new(),
        };
        let mut c = self.client()?;
        let response = self
            .block_on(async {
                c.import_external(ImportExternalRequest {
                    parent,
                    name: name.to_string(),
                    inode_json,
                    manifest_json,
                    request_id: op_id.to_hex(),
                    expected_parent_generation: expected_parent_generation.unwrap_or(0),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn unlink_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
    ) -> Result<()> {
        let mut c = self.client()?;
        self.block_on(async {
            c.unlink(UnlinkRequest {
                parent,
                name: name.to_string(),
                expected_parent_generation: expected_parent_generation.unwrap_or(0),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }
}
