use fluxfs_types::{Dentry, FileType, Inode, InodeId, Manifest, ManifestId, Result, ROOT_INODE};

/// Engine-agnostic metadata API frozen for W1.
///
/// Implementations: [`crate::RocksMetaStore`] (default). Engine types must not
/// leak into VFS callers.
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

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest>;

    /// Unlink name from parent directory (inode/chunk GC deferred).
    fn unlink(&self, parent: InodeId, name: &str) -> Result<()>;
}
