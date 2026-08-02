use fluxfs_types::{
    ChunkId, Dentry, FileType, FlushId, FlushIntent, GcBatch, GcLeaseId, GcPlan, GcTombstone,
    Inode, InodeId, Manifest, ManifestId, RequestOpId, Result, UfsObject, WorkerMembership,
    WorkerRegistration, WorkerTargetId, WriteTicketId, ROOT_INODE,
};

/// Engine-agnostic metadata API frozen for W1.
///
/// Implementations: [`crate::HeedMetaStore`] (default). Future: slatedb / Mantle-scale LSM
/// must satisfy this trait without changing VFS callers.
pub trait MetaStore: Send + Sync {
    fn root(&self) -> InodeId {
        ROOT_INODE
    }

    fn get_inode(&self, id: InodeId) -> Result<Inode>;

    /// Register or renew a stable Worker identity. Callers sample the lease
    /// deadline before submitting the replicated mutation.
    fn register_worker(&self, registration: &WorkerRegistration) -> Result<WorkerMembership>;

    /// Return the durable membership, including expired entries. Placement
    /// filters them against a caller-sampled time.
    fn worker_membership(&self) -> Result<WorkerMembership>;

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode>;

    fn create(
        &self,
        parent: InodeId,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        self.create_cas(None, parent, name, file_type, mode, uid, gid)
    }

    /// Create under `parent` with optional directory-generation CAS.
    ///
    /// When `expected_parent_generation` is `Some`, succeeds only if the
    /// durable parent directory's `generation` matches; on success the parent
    /// generation is incremented in the same transaction.
    #[allow(clippy::too_many_arguments)]
    fn create_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode>;

    fn readdir(&self, dir: InodeId) -> Result<Vec<Dentry>>;

    /// Update durable inode fields (locality, size, ufs pointer, generation, …).
    fn put_inode(&self, inode: &Inode) -> Result<()>;

    /// Persist an immutable manifest snapshot; returns its allocated id.
    fn put_manifest(&self, manifest: &Manifest) -> Result<ManifestId>;

    /// Atomically allocate+store `manifest` and CAS-update `inode` head.
    ///
    /// Succeeds only when the durable inode's `generation` equals
    /// `expected_generation`. On success the returned inode has `manifest_id`
    /// filled; on CAS failure returns [`fluxfs_types::FluxError::CasFailed`]
    /// and leaves the previous head untouched.
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

    /// Same as [`Self::commit_inode_manifest`] but with an explicit op id for retries.
    fn commit_inode_manifest_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode>;

    fn reserve_chunks(
        &self,
        ticket: WriteTicketId,
        inode: InodeId,
        expected_generation: u64,
        chunks: &[ChunkId],
    ) -> Result<()>;

    fn abort_chunk_reservation(&self, ticket: WriteTicketId) -> Result<()>;

    /// Expire a bounded number of abandoned tickets. The implementation must
    /// embed its sampled cutoff in the replicated command.
    fn expire_chunk_reservations(&self, max_to_expire: usize) -> Result<()>;

    fn commit_inode_manifest_reserved_with_id(
        &self,
        op_id: RequestOpId,
        ticket: WriteTicketId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode>;

    /// Tombstone a bounded candidate set iff no current manifest or active
    /// pre-Put reservation references it. Also reclaims unreachable manifests.
    fn tombstone_gc_batch(&self, candidates: &[ChunkId]) -> Result<GcBatch>;

    fn list_gc_tombstones(&self) -> Result<Vec<GcTombstone>>;

    fn initialize_gc_delete_targets(
        &self,
        chunks: &[ChunkId],
        targets: &[WorkerTargetId],
    ) -> Result<()>;

    fn acknowledge_gc_deletes(&self, deleted: &[(ChunkId, WorkerTargetId)]) -> Result<()>;

    fn finalize_gc_tombstones(&self, chunks: &[ChunkId]) -> Result<()>;

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest>;

    fn begin_flush(
        &self,
        expected_generation: u64,
        inode: InodeId,
        intent: &FlushIntent,
    ) -> Result<Inode> {
        self.begin_flush_with_id(RequestOpId::random(), expected_generation, inode, intent)
    }

    fn begin_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        intent: &FlushIntent,
    ) -> Result<Inode>;

    fn commit_flush(
        &self,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        published_ufs: &UfsObject,
    ) -> Result<Inode> {
        self.commit_flush_with_id(
            RequestOpId::random(),
            expected_generation,
            inode,
            flush_id,
            published_ufs,
        )
    }

    fn commit_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        published_ufs: &UfsObject,
    ) -> Result<Inode>;

    fn fail_flush_conflict(
        &self,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        error: &str,
    ) -> Result<Inode>;

    fn list_flush_intents(&self) -> Result<Vec<(InodeId, FlushIntent)>>;

    fn begin_gc(&self, lease_id: GcLeaseId) -> Result<GcPlan>;

    fn current_gc_plan(&self) -> Result<Option<GcPlan>>;

    fn finish_gc(&self, lease_id: GcLeaseId) -> Result<()>;

    /// Atomically import an External inode (+ optional manifest) under `parent/name`.
    fn import_external(
        &self,
        parent: InodeId,
        name: &str,
        inode: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode> {
        self.import_external_with_id(RequestOpId::random(), None, parent, name, inode, manifest)
    }

    fn import_external_with_id(
        &self,
        op_id: RequestOpId,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        inode: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode>;

    /// Unlink name from parent directory (inode/chunk GC deferred).
    fn unlink(&self, parent: InodeId, name: &str) -> Result<()> {
        self.unlink_cas(None, parent, name)
    }

    /// Unlink with optional directory-generation CAS (see [`Self::create_cas`]).
    fn unlink_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
    ) -> Result<()>;
}
