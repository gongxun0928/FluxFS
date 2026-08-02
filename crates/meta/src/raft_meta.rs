//! [`MetaStore`] façade that routes every mutation through OpenRaft.

use crate::heed_store::HeedMetaStore;
use crate::raft_types::{FluxRaft, MetaRaftRequest, MetaRaftResponse};
use crate::store::MetaStore;
use fluxfs_types::{
    Dentry, FileType, FluxError, Inode, InodeId, Manifest, ManifestId, RequestOpId, Result,
    ROOT_INODE,
};
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

/// Production MetaStore: reads from Heed, writes via Raft (single-voter today).
pub struct RaftMetaStore {
    store: Arc<HeedMetaStore>,
    raft: FluxRaft,
    handle: Option<Handle>,
    rt: Option<Runtime>,
}

impl RaftMetaStore {
    pub fn new(store: Arc<HeedMetaStore>, raft: FluxRaft) -> Self {
        let (handle, rt) = match Handle::try_current() {
            Ok(handle) => (Some(handle), None),
            Err(_) => (
                None,
                Some(Runtime::new().expect("create runtime for RaftMetaStore")),
            ),
        };
        Self {
            store,
            raft,
            handle,
            rt,
        }
    }

    /// Own the Tokio runtime that drives OpenRaft (co-located mount path).
    pub fn new_owned(store: Arc<HeedMetaStore>, raft: FluxRaft, rt: Runtime) -> Self {
        Self {
            store,
            raft,
            handle: Some(rt.handle().clone()),
            rt: Some(rt),
        }
    }

    fn block_on<T>(&self, fut: impl Future<Output = T>) -> T {
        if Handle::try_current().is_ok() {
            let handle = self.handle.as_ref().expect("runtime handle captured");
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else if let Some(handle) = &self.handle {
            handle.block_on(fut)
        } else if let Some(rt) = &self.rt {
            rt.block_on(fut)
        } else {
            unreachable!("RaftMetaStore executor missing")
        }
    }

    fn write(&self, req: MetaRaftRequest) -> Result<MetaRaftResponse> {
        let resp = self
            .block_on(self.raft.client_write(req))
            .map_err(|e| FluxError::Meta(format!("raft write: {e}")))?;
        Ok(resp.data)
    }

    fn map_inode(resp: MetaRaftResponse) -> Result<Inode> {
        match resp {
            MetaRaftResponse::Inode(inode) => Ok(*inode),
            MetaRaftResponse::Err(err) => Err(err),
            other => Err(FluxError::Meta(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_empty(resp: MetaRaftResponse) -> Result<()> {
        match resp {
            MetaRaftResponse::Empty => Ok(()),
            MetaRaftResponse::Err(err) => Err(err),
            other => Err(FluxError::Meta(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_manifest_id(resp: MetaRaftResponse) -> Result<ManifestId> {
        match resp {
            MetaRaftResponse::ManifestId(id) => Ok(ManifestId(id)),
            MetaRaftResponse::Err(err) => Err(err),
            other => Err(FluxError::Meta(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }
}

impl MetaStore for RaftMetaStore {
    fn root(&self) -> InodeId {
        ROOT_INODE
    }

    fn get_inode(&self, id: InodeId) -> Result<Inode> {
        self.store.get_inode(id)
    }

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        self.store.lookup(parent, name)
    }

    fn create(
        &self,
        parent: InodeId,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        let resp = self.write(MetaRaftRequest::Create {
            request_id: Some(RequestOpId::new()),
            parent,
            name: name.to_string(),
            file_type,
            mode,
            uid,
            gid,
        })?;
        Self::map_inode(resp)
    }

    fn readdir(&self, dir: InodeId) -> Result<Vec<Dentry>> {
        self.store.readdir(dir)
    }

    fn put_inode(&self, inode: &Inode) -> Result<()> {
        let resp = self.write(MetaRaftRequest::PutInode {
            request_id: Some(RequestOpId::new()),
            inode: Box::new(inode.clone()),
        })?;
        Self::map_empty(resp)
    }

    fn put_manifest(&self, manifest: &Manifest) -> Result<ManifestId> {
        let resp = self.write(MetaRaftRequest::PutManifest {
            request_id: Some(RequestOpId::new()),
            manifest: Box::new(manifest.clone()),
        })?;
        Self::map_manifest_id(resp)
    }

    fn commit_inode_manifest(
        &self,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        self.commit_inode_manifest_with_id(RequestOpId::new(), expected_generation, inode, manifest)
    }

    fn commit_inode_manifest_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        let resp = self.write(MetaRaftRequest::CommitInodeManifest {
            request_id: Some(op_id),
            expected_generation,
            inode: Box::new(inode.clone()),
            manifest: Box::new(manifest.clone()),
        })?;
        Self::map_inode(resp)
    }

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest> {
        self.store.get_manifest(id)
    }

    fn unlink(&self, parent: InodeId, name: &str) -> Result<()> {
        let resp = self.write(MetaRaftRequest::Unlink {
            request_id: Some(RequestOpId::new()),
            parent,
            name: name.to_string(),
        })?;
        Self::map_empty(resp)
    }
}
