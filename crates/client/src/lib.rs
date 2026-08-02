//! Internal FluxFS client surface for alpha (CLI + FUSE + tests).

use fluxfs_chunk::ChunkStore;
use fluxfs_meta::MetaStore;
use fluxfs_types::{
    BackingMode, ChunkId, DataGen, DataState, Extent, FileType, FluxError, Inode, InodeId,
    LocalityFields, LocalityLabel, Manifest, OpState, Origin, Result, UfsObject, UfsVersion,
    CHUNK_SIZE, DIRTY_WRITE_CAP_BYTES, ROOT_INODE,
};
use fluxfs_ufs::{Ufs, UfsEntryMode, UfsProbe};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::runtime::{Handle, Runtime};

struct UfsRuntime {
    ufs: Ufs,
    /// Owned runtime only when constructed outside an existing Tokio context.
    rt: Option<Runtime>,
}

impl UfsRuntime {
    fn new(ufs: Ufs) -> Result<Self> {
        let rt = if Handle::try_current().is_ok() {
            None
        } else {
            Some(Runtime::new().map_err(|e| FluxError::Ufs(e.to_string()))?)
        };
        Ok(Self { ufs, rt })
    }

    fn block_on<T>(&self, fut: impl Future<Output = T>) -> T {
        if let Ok(handle) = Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else if let Some(rt) = &self.rt {
            rt.block_on(fut)
        } else {
            let rt = Runtime::new().expect("create temporary runtime");
            rt.block_on(fut)
        }
    }
}

pub struct FluxClient<M: MetaStore, C: ChunkStore> {
    pub meta: M,
    pub chunks: C,
    io_lock: Mutex<()>,
    ufs: Option<UfsRuntime>,
    /// Relative UFS path for imported/local namespace nodes (`""` = UFS root).
    ufs_paths: Mutex<HashMap<InodeId, String>>,
}

impl<M: MetaStore, C: ChunkStore> FluxClient<M, C> {
    pub fn new(meta: M, chunks: C) -> Self {
        Self {
            meta,
            chunks,
            io_lock: Mutex::new(()),
            ufs: None,
            ufs_paths: Mutex::new(HashMap::from([(ROOT_INODE, String::new())])),
        }
    }

    /// Attach OpenDAL UFS for External lazy namespace (read-only vertical).
    pub fn with_ufs(mut self, ufs: Ufs) -> Result<Self> {
        self.ufs = Some(UfsRuntime::new(ufs)?);
        Ok(self)
    }

    pub fn has_ufs(&self) -> bool {
        self.ufs.is_some()
    }

    pub fn root(&self) -> InodeId {
        self.meta.root()
    }

    fn reject_ufs_mutation(&self) -> Result<()> {
        if self.ufs.is_some() {
            Err(FluxError::Capability(
                "UFS-backed mount is read-only until Dirty copy-up is wired".into(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn mkdir(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        self.reject_ufs_mutation()?;
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
        self.reject_ufs_mutation()?;
        self.meta
            .create(parent, name, FileType::Regular, mode, uid, gid)
    }

    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        match self.meta.lookup(parent, name) {
            Ok(ino) => {
                self.ensure_ufs_path(parent, ino.id, name);
                Ok(ino)
            }
            Err(FluxError::NotFound) if self.ufs.is_some() => self.lazy_import(parent, name),
            Err(e) => Err(e),
        }
    }

    pub fn get_inode(&self, id: InodeId) -> Result<Inode> {
        self.meta.get_inode(id)
    }

    pub fn readdir(&self, dir: InodeId) -> Result<Vec<fluxfs_types::Dentry>> {
        if self.ufs.is_some() {
            self.hydrate_dir_from_ufs(dir)?;
        }
        self.meta.readdir(dir)
    }

    pub fn unlink(&self, parent: InodeId, name: &str) -> Result<()> {
        self.reject_ufs_mutation()?;
        self.meta.unlink(parent, name)
    }

    pub fn lookup_path(&self, path: &str) -> Result<Inode> {
        let mut cur = ROOT_INODE;
        let path = path.trim_matches('/');
        if path.is_empty() {
            return self.meta.get_inode(ROOT_INODE);
        }
        for part in path.split('/') {
            cur = self.lookup(cur, part)?.id;
        }
        self.meta.get_inode(cur)
    }

    pub fn put_chunk(&self, data: &[u8]) -> Result<ChunkId> {
        self.chunks.put(data)
    }

    pub fn get_chunk(&self, id: &ChunkId) -> Result<Vec<u8>> {
        self.chunks.get(id)
    }

    /// Assemble full file bytes from the inode's current manifest.
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
                Extent::Local { offset, len, chunk } => {
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
                Extent::UfsRange {
                    offset,
                    len,
                    ufs_key,
                    offset_in_object,
                    ..
                } => {
                    let data = self.ufs_read_range(ufs_key, *offset_in_object, *len)?;
                    if data.len() as u64 != *len {
                        return Err(FluxError::Io(format!(
                            "ufs range len mismatch: want={len} got={}",
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
        self.reject_ufs_mutation()?;
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

        let gen = DataGen(inode.head_gen.0.saturating_add(1));
        let manifest = self.build_local_manifest(ino, gen, &buf)?;
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
        self.reject_ufs_mutation()?;
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
            let gen = DataGen(inode.head_gen.0.saturating_add(1));
            let manifest = self.build_local_manifest(ino, gen, &buf)?;
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

    /// Split one logical file image into bounded content-addressed RPC/storage chunks.
    /// A failure leaves only unreachable chunks because the manifest is committed last.
    fn build_local_manifest(&self, ino: InodeId, gen: DataGen, data: &[u8]) -> Result<Manifest> {
        let mut extents = Vec::with_capacity(data.len().div_ceil(CHUNK_SIZE as usize));
        for (index, bytes) in data.chunks(CHUNK_SIZE as usize).enumerate() {
            let chunk = self.chunks.put(bytes)?;
            extents.push(Extent::Local {
                offset: index as u64 * CHUNK_SIZE,
                len: bytes.len() as u64,
                chunk,
            });
        }
        Ok(Manifest {
            inode: ino,
            gen,
            size: data.len() as u64,
            extents,
        })
    }

    fn ufs_read_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let ufs = self
            .ufs
            .as_ref()
            .ok_or_else(|| FluxError::Capability("UfsRange read requires --ufs mount".into()))?;
        ufs.block_on(ufs.ufs.read_range(key, offset, len))
    }

    fn parent_ufs_path(&self, parent: InodeId) -> Result<String> {
        let guard = self
            .ufs_paths
            .lock()
            .map_err(|_| FluxError::Io("ufs path lock poisoned".into()))?;
        guard.get(&parent).cloned().ok_or_else(|| {
            FluxError::InvalidArg(format!("missing UFS path cache for inode {parent}"))
        })
    }

    fn remember_ufs_path(&self, id: InodeId, path: String) {
        if let Ok(mut g) = self.ufs_paths.lock() {
            g.insert(id, path);
        }
    }

    fn ensure_ufs_path(&self, parent: InodeId, child: InodeId, name: &str) {
        if self.ufs.is_none() {
            return;
        }
        let Ok(mut g) = self.ufs_paths.lock() else {
            return;
        };
        if g.contains_key(&child) {
            return;
        }
        let parent_path = g.get(&parent).cloned().unwrap_or_default();
        g.insert(child, join_rel(&parent_path, name));
    }

    fn hydrate_dir_from_ufs(&self, dir: InodeId) -> Result<()> {
        let ufs = self.ufs.as_ref().expect("checked");
        let path = self.parent_ufs_path(dir).unwrap_or_default();
        let entries = ufs.block_on(ufs.ufs.list(&path))?;
        for ent in entries {
            let Some(name) = entry_name(&ent.path, &path) else {
                continue;
            };
            if self.meta.lookup(dir, name).is_ok() {
                continue;
            }
            let _ = self.lazy_import(dir, name)?;
        }
        Ok(())
    }

    fn lazy_import(&self, parent: InodeId, name: &str) -> Result<Inode> {
        let ufs = self
            .ufs
            .as_ref()
            .ok_or_else(|| FluxError::Capability("no UFS".into()))?;
        let parent_path = self.parent_ufs_path(parent).unwrap_or_default();
        let rel = join_rel(&parent_path, name);

        match ufs.block_on(ufs.ufs.probe(&rel)) {
            Ok(UfsProbe::File(obj)) => self.commit_external_file(parent, name, &rel, obj),
            Ok(UfsProbe::Dir) => self.commit_external_dir(parent, name, &rel),
            Err(FluxError::NotFound) => {
                if self.ufs_looks_like_dir(ufs, &parent_path, &rel, name)? {
                    self.commit_external_dir(parent, name, &rel)
                } else {
                    Err(FluxError::NotFound)
                }
            }
            Err(e) => Err(e),
        }
    }

    fn ufs_looks_like_dir(
        &self,
        ufs: &UfsRuntime,
        parent_path: &str,
        rel: &str,
        name: &str,
    ) -> Result<bool> {
        let parent_entries = ufs.block_on(ufs.ufs.list(parent_path))?;
        if parent_entries
            .iter()
            .any(|e| entry_name(&e.path, parent_path) == Some(name) && e.mode == UfsEntryMode::Dir)
        {
            return Ok(true);
        }
        // S3 prefix: non-empty list under rel ⇒ directory.
        let children = ufs.block_on(ufs.ufs.list(rel))?;
        Ok(!children.is_empty())
    }

    fn commit_external_file(
        &self,
        parent: InodeId,
        name: &str,
        rel: &str,
        obj: UfsObject,
    ) -> Result<Inode> {
        // create() currently stamps Ephemeral; convert via put_inode.
        let mut inode = match self
            .meta
            .create(parent, name, FileType::Regular, 0o644, 0, 0)
        {
            Ok(i) => i,
            Err(FluxError::AlreadyExists) => return self.meta.lookup(parent, name),
            Err(e) => return Err(e),
        };
        let version = UfsVersion(
            obj.etag
                .clone()
                .unwrap_or_else(|| format!("size:{}", obj.size)),
        );
        let mid = if obj.size == 0 {
            None
        } else {
            let manifest = Manifest {
                inode: inode.id,
                gen: DataGen(0),
                size: obj.size,
                extents: vec![Extent::UfsRange {
                    offset: 0,
                    len: obj.size,
                    ufs_key: rel.to_string(),
                    ufs_version: version.clone(),
                    offset_in_object: 0,
                }],
            };
            Some(self.meta.put_manifest(&manifest)?)
        };
        let now = now_ms();
        inode.size = obj.size;
        inode.head_gen = DataGen(0);
        inode.ufs_gen = DataGen(0);
        inode.ufs_base_version = Some(version);
        inode.ufs = Some(UfsObject {
            key: rel.to_string(),
            size: obj.size,
            etag: obj.etag,
            mtime_ms: obj.mtime_ms,
        });
        inode.manifest_id = mid;
        inode.locality_fields = Some(LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::UfsClean,
            op_state: OpState::None,
            origin: Origin::Imported,
        });
        inode.locality = LocalityLabel::External;
        inode.mtime_ms = obj.mtime_ms.unwrap_or(now);
        inode.ctime_ms = now;
        inode.atime_ms = now;
        self.meta.put_inode(&inode)?;
        self.remember_ufs_path(inode.id, rel.to_string());
        Ok(inode)
    }

    fn commit_external_dir(&self, parent: InodeId, name: &str, rel: &str) -> Result<Inode> {
        let mut inode = match self
            .meta
            .create(parent, name, FileType::Directory, 0o755, 0, 0)
        {
            Ok(i) => i,
            Err(FluxError::AlreadyExists) => return self.meta.lookup(parent, name),
            Err(e) => return Err(e),
        };
        let now = now_ms();
        inode.locality_fields = Some(LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::UfsClean,
            op_state: OpState::None,
            origin: Origin::Imported,
        });
        inode.locality = LocalityLabel::External;
        inode.ufs = Some(UfsObject {
            key: format!("{}/", rel.trim_end_matches('/')),
            size: 0,
            etag: None,
            mtime_ms: Some(now),
        });
        inode.ctime_ms = now;
        inode.mtime_ms = now;
        inode.atime_ms = now;
        self.meta.put_inode(&inode)?;
        self.remember_ufs_path(inode.id, rel.to_string());
        Ok(inode)
    }
}

fn join_rel(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), name)
    }
}

fn entry_name<'a>(path: &'a str, parent: &str) -> Option<&'a str> {
    let path = path.trim_start_matches('/').trim_end_matches('/');
    let parent = parent.trim_start_matches('/').trim_end_matches('/');
    let rest = if parent.is_empty() {
        path
    } else if let Some(r) = path.strip_prefix(parent) {
        r.trim_start_matches('/')
    } else {
        return path.rsplit('/').next();
    };
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some(rest)
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

    #[test]
    fn write_splits_file_at_chunk_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks);
        let file = client
            .create_file(ROOT_INODE, "big.bin", 0o644, 0, 0)
            .unwrap();
        let mut data = vec![0u8; (CHUNK_SIZE as usize) + 16];
        data[0] = 1;
        data[CHUNK_SIZE as usize] = 2;
        client.write_at(file.id, 0, &data).unwrap();
        let got = client.read_all(file.id).unwrap();
        assert_eq!(got, data);
        let mid = client.get_inode(file.id).unwrap().manifest_id.unwrap();
        let manifest = client.meta.get_manifest(mid).unwrap();
        assert_eq!(manifest.extents.len(), 2);
    }

    #[test]
    fn external_lazy_lookup_and_read_via_local_ufs() {
        let dir = tempfile::tempdir().unwrap();
        let ufs_root = dir.path().join("ufs");
        std::fs::create_dir_all(ufs_root.join("sub")).unwrap();
        std::fs::write(ufs_root.join("hello.txt"), b"external-bytes").unwrap();
        std::fs::write(ufs_root.join("sub/nested.txt"), b"nested").unwrap();

        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let ufs = Ufs::local(&ufs_root).unwrap();
        let client = FluxClient::new(meta, chunks).with_ufs(ufs).unwrap();

        let hello = client.lookup(ROOT_INODE, "hello.txt").unwrap();
        assert_eq!(hello.locality, LocalityLabel::External);
        assert_eq!(client.read_all(hello.id).unwrap(), b"external-bytes");

        let dents = client.readdir(ROOT_INODE).unwrap();
        assert!(dents.iter().any(|d| d.name == "sub"));
        let sub = client.lookup(ROOT_INODE, "sub").unwrap();
        assert_eq!(sub.file_type, FileType::Directory);
        let nested = client.lookup(sub.id, "nested.txt").unwrap();
        assert_eq!(client.read_all(nested.id).unwrap(), b"nested");

        let err = client
            .create_file(ROOT_INODE, "x", 0o644, 0, 0)
            .unwrap_err();
        assert!(matches!(err, FluxError::Capability(_)));
    }
}
