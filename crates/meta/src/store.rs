use fluxfs_types::{Dentry, FileType, Inode, InodeId, Result, ROOT_INODE};

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
}
