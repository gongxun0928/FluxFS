use fluxfs_types::{
    Dentry, FileType, FlushId, FlushIntent, Inode, InodeId, Manifest, ManifestId, RequestOpId,
    Result, UfsObject, ROOT_INODE,
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

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode>;

    fn create(
        &self,
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

    /// Atomically import an External inode (+ optional manifest) under `parent/name`.
    fn import_external(
        &self,
        parent: InodeId,
        name: &str,
        inode: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode> {
        self.import_external_with_id(RequestOpId::random(), parent, name, inode, manifest)
    }

    fn import_external_with_id(
        &self,
        op_id: RequestOpId,
        parent: InodeId,
        name: &str,
        inode: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode>;

    /// Unlink name from parent directory (inode/chunk GC deferred).
    fn unlink(&self, parent: InodeId, name: &str) -> Result<()>;
}
