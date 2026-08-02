//! Internal FluxFS client surface for alpha (CLI + FUSE + tests).

use fluxfs_chunk::ChunkStore;
use fluxfs_meta::MetaStore;
use fluxfs_types::{ChunkId, FileType, Inode, InodeId, Result, ROOT_INODE};

pub struct FluxClient<M: MetaStore, C: ChunkStore> {
    pub meta: M,
    pub chunks: C,
}

impl<M: MetaStore, C: ChunkStore> FluxClient<M, C> {
    pub fn new(meta: M, chunks: C) -> Self {
        Self { meta, chunks }
    }

    pub fn root(&self) -> InodeId {
        self.meta.root()
    }

    pub fn mkdir(&self, parent: InodeId, name: &str) -> Result<Inode> {
        self.meta
            .create(parent, name, FileType::Directory, 0o755, 0, 0)
    }

    pub fn create_file(&self, parent: InodeId, name: &str) -> Result<Inode> {
        self.meta
            .create(parent, name, FileType::Regular, 0o644, 0, 0)
    }

    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        self.meta.lookup(parent, name)
    }

    pub fn lookup_path(&self, path: &str) -> Result<Inode> {
        let mut cur = ROOT_INODE;
        let path = path.trim_matches('/');
        if path.is_empty() {
            return self.meta.get_inode(ROOT_INODE);
        }
        for part in path.split('/') {
            cur = self.meta.lookup(cur, part)?.id;
        }
        self.meta.get_inode(cur)
    }

    pub fn put_chunk(&self, data: &[u8]) -> Result<ChunkId> {
        self.chunks.put(data)
    }

    pub fn get_chunk(&self, id: &ChunkId) -> Result<Vec<u8>> {
        self.chunks.get(id)
    }
}
