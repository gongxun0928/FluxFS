//! [`MetaStore`] façade that routes every mutation through OpenRaft.

use crate::heed_store::HeedMetaStore;
use crate::raft_types::{FluxRaft, MetaRaftRequest, MetaRaftResponse};
use crate::store::MetaStore;
use fluxfs_types::{
    ChunkId, Dentry, FileType, FlushId, FlushIntent, FluxError, GcBatch, GcLeaseId, GcPlan,
    GcTombstone, Inode, InodeId, Manifest, ManifestId, RequestOpId, Result, UfsObject,
    WorkerMembership, WorkerRegistration, WorkerTargetId, WriteTicketId, ROOT_INODE,
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
        // Sample wall time once before propose; apply never reads a clock.
        let req = req.with_ledger_now(crate::unix_time_millis());
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

    fn map_gc_plan(resp: MetaRaftResponse) -> Result<GcPlan> {
        match resp {
            MetaRaftResponse::GcPlan(plan) => Ok(*plan),
            MetaRaftResponse::Err(err) => Err(err),
            other => Err(FluxError::Meta(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_gc_batch(resp: MetaRaftResponse) -> Result<GcBatch> {
        match resp {
            MetaRaftResponse::GcBatch(batch) => Ok(*batch),
            MetaRaftResponse::Err(err) => Err(err),
            other => Err(FluxError::Meta(format!(
                "unexpected raft response: {other:?}"
            ))),
        }
    }

    fn map_worker_membership(resp: MetaRaftResponse) -> Result<WorkerMembership> {
        match resp {
            MetaRaftResponse::WorkerMembership(membership) => Ok(*membership),
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

    fn register_worker(&self, registration: &WorkerRegistration) -> Result<WorkerMembership> {
        Self::map_worker_membership(self.write(MetaRaftRequest::RegisterWorker {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            registration: registration.clone(),
        })?)
    }

    fn worker_membership(&self) -> Result<WorkerMembership> {
        self.store.worker_membership()
    }

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        self.store.lookup(parent, name)
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
        let resp = self.write(MetaRaftRequest::Create {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            parent,
            name: name.to_string(),
            file_type,
            mode,
            uid,
            gid,
            expected_parent_generation,
        })?;
        Self::map_inode(resp)
    }

    fn readdir(&self, dir: InodeId) -> Result<Vec<Dentry>> {
        self.store.readdir(dir)
    }

    fn put_inode(&self, inode: &Inode) -> Result<()> {
        let resp = self.write(MetaRaftRequest::PutInode {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            inode: Box::new(inode.clone()),
        })?;
        Self::map_empty(resp)
    }

    fn put_inode_cas(&self, expected_generation: u64, inode: &Inode) -> Result<Inode> {
        let resp = self.write(MetaRaftRequest::PutInodeCas {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            expected_generation,
            inode: Box::new(inode.clone()),
        })?;
        Self::map_inode(resp)
    }

    fn put_manifest(&self, manifest: &Manifest) -> Result<ManifestId> {
        let resp = self.write(MetaRaftRequest::PutManifest {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
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
        self.commit_inode_manifest_with_id(
            RequestOpId::random(),
            expected_generation,
            inode,
            manifest,
        )
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
            ledger_now_unix_ms: 0,
            expected_generation,
            inode: Box::new(inode.clone()),
            manifest: Box::new(manifest.clone()),
        })?;
        Self::map_inode(resp)
    }

    fn reserve_chunks(
        &self,
        ticket: WriteTicketId,
        inode: InodeId,
        expected_generation: u64,
        chunks: &[ChunkId],
    ) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::ReserveChunks {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            ticket,
            inode,
            expected_generation,
            chunks: chunks.to_vec(),
            expires_at_unix_ms: crate::write_reservation_deadline(),
        })?)
    }

    fn abort_chunk_reservation(&self, ticket: WriteTicketId) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::AbortChunkReservation {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            ticket,
        })?)
    }

    fn expire_chunk_reservations(&self, max_to_expire: usize) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::ExpireChunkReservations {
            request_id: None,
            ledger_now_unix_ms: 0,
            cutoff_unix_ms: crate::unix_time_millis(),
            max_to_expire: max_to_expire.try_into().unwrap_or(u64::MAX),
        })?)
    }

    fn prune_client_requests(&self, max_to_prune: usize) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::PruneClientRequests {
            request_id: None,
            ledger_now_unix_ms: 0,
            cutoff_unix_ms: crate::unix_time_millis(),
            max_to_prune: max_to_prune.try_into().unwrap_or(u64::MAX),
        })?)
    }

    fn commit_inode_manifest_reserved_with_id(
        &self,
        op_id: RequestOpId,
        ticket: WriteTicketId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        Self::map_inode(self.write(MetaRaftRequest::CommitInodeManifestReserved {
            request_id: Some(op_id),
            ledger_now_unix_ms: 0,
            ticket,
            expected_generation,
            inode: Box::new(inode.clone()),
            manifest: Box::new(manifest.clone()),
        })?)
    }

    fn tombstone_gc_batch(&self, candidates: &[ChunkId]) -> Result<GcBatch> {
        Self::map_gc_batch(self.write(MetaRaftRequest::TombstoneGcBatch {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            candidates: candidates.to_vec(),
        })?)
    }

    fn list_gc_tombstones(&self) -> Result<Vec<GcTombstone>> {
        self.store.list_gc_tombstones()
    }

    fn initialize_gc_delete_targets(
        &self,
        chunks: &[ChunkId],
        targets: &[WorkerTargetId],
    ) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::InitializeGcDeleteTargets {
            request_id: None,
            ledger_now_unix_ms: 0,
            chunks: chunks.to_vec(),
            targets: targets.to_vec(),
        })?)
    }

    fn acknowledge_gc_deletes(&self, deleted: &[(ChunkId, WorkerTargetId)]) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::AcknowledgeGcDeletes {
            request_id: None,
            ledger_now_unix_ms: 0,
            deleted: deleted.to_vec(),
        })?)
    }

    fn finalize_gc_tombstones(&self, chunks: &[ChunkId]) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::FinalizeGcTombstones {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            chunks: chunks.to_vec(),
        })?)
    }

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest> {
        self.store.get_manifest(id)
    }

    fn begin_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        intent: &FlushIntent,
    ) -> Result<Inode> {
        Self::map_inode(self.write(MetaRaftRequest::BeginFlush {
            request_id: Some(op_id),
            ledger_now_unix_ms: 0,
            expected_generation,
            inode,
            intent: Box::new(intent.clone()),
        })?)
    }

    fn commit_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        published_ufs: &UfsObject,
    ) -> Result<Inode> {
        Self::map_inode(self.write(MetaRaftRequest::CommitFlush {
            request_id: Some(op_id),
            ledger_now_unix_ms: 0,
            expected_generation,
            inode,
            flush_id,
            published_ufs: Box::new(published_ufs.clone()),
        })?)
    }

    fn fail_flush_conflict(
        &self,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        error: &str,
    ) -> Result<Inode> {
        Self::map_inode(self.write(MetaRaftRequest::FailFlushConflict {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            expected_generation,
            inode,
            flush_id,
            error: error.to_string(),
        })?)
    }

    fn list_flush_intents(&self) -> Result<Vec<(InodeId, FlushIntent)>> {
        self.store.list_flush_intents()
    }

    fn begin_gc(&self, lease_id: GcLeaseId) -> Result<GcPlan> {
        Self::map_gc_plan(self.write(MetaRaftRequest::BeginGc {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            lease_id,
        })?)
    }

    fn current_gc_plan(&self) -> Result<Option<GcPlan>> {
        self.store.current_gc_plan()
    }

    fn finish_gc(&self, lease_id: GcLeaseId) -> Result<()> {
        Self::map_empty(self.write(MetaRaftRequest::FinishGc {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            lease_id,
        })?)
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
        Self::map_inode(self.write(MetaRaftRequest::ImportExternal {
            request_id: Some(op_id),
            ledger_now_unix_ms: 0,
            parent,
            name: name.to_string(),
            inode: Box::new(inode.clone()),
            manifest: manifest.map(|m| Box::new(m.clone())),
            expected_parent_generation,
        })?)
    }

    fn unlink_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
    ) -> Result<()> {
        let resp = self.write(MetaRaftRequest::Unlink {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            parent,
            name: name.to_string(),
            expected_parent_generation,
        })?;
        Self::map_empty(resp)
    }

    fn rmdir_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
    ) -> Result<()> {
        let resp = self.write(MetaRaftRequest::Rmdir {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            parent,
            name: name.to_string(),
            expected_parent_generation,
        })?;
        Self::map_empty(resp)
    }

    fn rename_cas(
        &self,
        expected_old_parent_generation: Option<u64>,
        old_parent: InodeId,
        old_name: &str,
        expected_new_parent_generation: Option<u64>,
        new_parent: InodeId,
        new_name: &str,
        no_replace: bool,
    ) -> Result<Inode> {
        Self::map_inode(self.write(MetaRaftRequest::Rename {
            request_id: Some(RequestOpId::random()),
            ledger_now_unix_ms: 0,
            old_parent,
            old_name: old_name.to_string(),
            expected_old_parent_generation,
            new_parent,
            new_name: new_name.to_string(),
            expected_new_parent_generation,
            no_replace,
        })?)
    }
}
