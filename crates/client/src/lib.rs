//! Internal FluxFS client surface for alpha (CLI + FUSE + tests).

use fluxfs_chunk::ChunkStore;
use fluxfs_meta::MetaStore;
use fluxfs_types::{
    ChunkId, DataGen, DIRTY_WRITE_CAP_BYTES, Extent, FileType, FluxError, Inode, InodeId, Manifest,
    Result, ROOT_INODE,
};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct FluxClient<M: MetaStore, C: ChunkStore> {
    pub meta: M,
    pub chunks: C,
    io_lock: Mutex<()>,
}

impl<M: MetaStore, C: ChunkStore> FluxClient<M, C> {
    pub fn new(meta: M, chunks: C) -> Self {
        Self {
            meta,
            chunks,
            io_lock: Mutex::new(()),
        }
    }

    pub fn root(&self) -> InodeId {
        self.meta.root()
    }

    pub fn mkdir(&self, parent: InodeId, name: &str, mode: u32, uid: u32, gid: u32) -> Result<Inode> {
        self.meta
            .create(parent, name, FileType::Directory, mode, uid, gid)
    }

    pub fn create_file(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        self.meta
            .create(parent, name, FileType::Regular, mode, uid, gid)
    }

    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        self.meta.lookup(parent, name)
    }

    pub fn get_inode(&self, id: InodeId) -> Result<Inode> {
        self.meta.get_inode(id)
    }

    pub fn readdir(&self, dir: InodeId) -> Result<Vec<fluxfs_types::Dentry>> {
        self.meta.readdir(dir)
    }

    pub fn unlink(&self, parent: InodeId, name: &str) -> Result<()> {
        self.meta.unlink(parent, name)
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

    /// Assemble full file bytes from the inode's current manifest (Local extents only).
    pub fn read_all(&self, ino: InodeId) -> Result<Vec<u8>> {
        let inode = self.meta.get_inode(ino)?;
        if inode.file_type != FileType::Regular {
            return Err(FluxError::IsDirectory);
        }
        if inode.size == 0 || inode.manifest_id.is_none() {
            return Ok(Vec::new());
        }
        let mid = inode.manifest_id.unwrap();
        let manifest = self.meta.get_manifest(mid)?;
        let mut buf = vec![0u8; inode.size as usize];
        for ext in &manifest.extents {
            match ext {
                Extent::Local {
                    offset,
                    len,
                    chunk,
                } => {
                    let data = self.chunks.get(chunk)?;
                    if data.len() as u64 != *len {
                        return Err(FluxError::Io(format!(
                            "chunk len mismatch: meta={len} actual={}",
                            data.len()
                        )));
                    }
                    let start = *offset as usize;
                    let end = start + *len as usize;
                    if end > buf.len() {
                        return Err(FluxError::Io("extent past EOF".into()));
                    }
                    buf[start..end].copy_from_slice(&data);
                }
                Extent::UfsRange { .. } => {
                    return Err(FluxError::Capability(
                        "UfsRange read not wired in ephemeral mount path".into(),
                    ));
                }
            }
        }
        Ok(buf)
    }

    pub fn read_at(&self, ino: InodeId, offset: u64, size: u32) -> Result<Vec<u8>> {
        let data = self.read_all(ino)?;
        if offset as usize >= data.len() {
            return Ok(Vec::new());
        }
        let start = offset as usize;
        let end = (start + size as usize).min(data.len());
        Ok(data[start..end].to_vec())
    }

    /// Whole-file rewrite write path (MVP). Durable via ChunkStore + manifest CAS on inode.
    pub fn write_at(&self, ino: InodeId, offset: u64, data: &[u8]) -> Result<u32> {
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| FluxError::Io("io lock poisoned".into()))?;
        let mut inode = self.meta.get_inode(ino)?;
        if inode.file_type != FileType::Regular {
            return Err(FluxError::IsDirectory);
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| FluxError::InvalidArg("offset overflow".into()))?;
        if end > DIRTY_WRITE_CAP_BYTES {
            return Err(FluxError::Capability(format!(
                "write would exceed {} byte Dirty/Ephemeral cap",
                DIRTY_WRITE_CAP_BYTES
            )));
        }

        let mut buf = if inode.size == 0 {
            Vec::new()
        } else {
            self.read_all(ino)?
        };
        if (buf.len() as u64) < end {
            buf.resize(end as usize, 0);
        }
        let start = offset as usize;
        buf[start..start + data.len()].copy_from_slice(data);

        let chunk = self.chunks.put(&buf)?;
        let gen = DataGen(inode.head_gen.0.saturating_add(1));
        let manifest = Manifest {
            inode: ino,
            gen,
            size: buf.len() as u64,
            extents: vec![Extent::Local {
                offset: 0,
                len: buf.len() as u64,
                chunk,
            }],
        };
        let mid = self.meta.put_manifest(&manifest)?;
        let now = now_ms();
        inode.size = buf.len() as u64;
        inode.head_gen = gen;
        inode.generation = inode.generation.saturating_add(1);
        inode.manifest_id = Some(mid);
        inode.mtime_ms = now;
        inode.ctime_ms = now;
        self.meta.put_inode(&inode)?;
        Ok(data.len() as u32)
    }

    pub fn truncate(&self, ino: InodeId, size: u64) -> Result<Inode> {
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| FluxError::Io("io lock poisoned".into()))?;
        let mut inode = self.meta.get_inode(ino)?;
        if inode.file_type != FileType::Regular {
            return Err(FluxError::IsDirectory);
        }
        if size > DIRTY_WRITE_CAP_BYTES {
            return Err(FluxError::Capability(format!(
                "truncate exceeds {} byte cap",
                DIRTY_WRITE_CAP_BYTES
            )));
        }
        let mut buf = self.read_all(ino)?;
        buf.resize(size as usize, 0);
        let mid = if size == 0 {
            None
        } else {
            let chunk = self.chunks.put(&buf)?;
            let gen = DataGen(inode.head_gen.0.saturating_add(1));
            let manifest = Manifest {
                inode: ino,
                gen,
                size,
                extents: vec![Extent::Local {
                    offset: 0,
                    len: size,
                    chunk,
                }],
            };
            inode.head_gen = gen;
            Some(self.meta.put_manifest(&manifest)?)
        };
        let now = now_ms();
        if size == 0 {
            inode.head_gen = DataGen(inode.head_gen.0.saturating_add(1));
        }
        inode.size = size;
        inode.manifest_id = mid;
        inode.generation = inode.generation.saturating_add(1);
        inode.mtime_ms = now;
        inode.ctime_ms = now;
        self.meta.put_inode(&inode)?;
        Ok(inode)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxfs_chunk::DiskChunkStore;
    use fluxfs_meta::HeedMetaStore;

    #[test]
    fn write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks);
        let f = client
            .create_file(ROOT_INODE, "a.txt", 0o644, 0, 0)
            .unwrap();
        client.write_at(f.id, 0, b"hello").unwrap();
        client.write_at(f.id, 5, b" world").unwrap();
        let got = client.read_all(f.id).unwrap();
        assert_eq!(got, b"hello world");
        let part = client.read_at(f.id, 6, 5).unwrap();
        assert_eq!(part, b"world");
    }
}
