//! RocksDB-backed MetaStore + shared DB open for Raft CFs.
//!
//! Column families:
//! - `meta`     — Raft vote / committed / last_purged
//! - `logs`     — Raft log entries
//! - `sm_meta`  — last_applied / membership
//! - `sm_data`  — inodes / dentries / manifests / counters

#![allow(clippy::result_large_err)]

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};

use crate::raft_types::{MetaRaftRequest, MetaRaftResponse, SmAppliedMeta};
use crate::store::MetaStore;
use fluxfs_types::{
    BackingMode, DataGen, DataState, Dentry, FileType, FluxError, Inode, InodeId, LocalityFields,
    LocalityLabel, Manifest, ManifestId, OpState, Origin, Result, ROOT_INODE,
};

pub const CF_META: &str = "meta";
pub const CF_LOGS: &str = "logs";
pub const CF_SM_META: &str = "sm_meta";
pub const CF_SM_DATA: &str = "sm_data";

const KEY_SM_LAST_APPLIED: &[u8] = b"last_applied";
const KEY_SM_LAST_MEMBERSHIP: &[u8] = b"last_membership";
const KEY_NEXT_INODE: &[u8] = b"c/next_inode";
const KEY_NEXT_MANIFEST: &[u8] = b"c/next_manifest";

/// Full MetaStore snapshot payload for OpenRaft install/build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaSnapshotData {
    pub inodes: Vec<Inode>,
    pub dentries: Vec<Dentry>,
    pub manifests: Vec<(u64, Manifest)>,
    pub next_inode: u64,
    pub next_manifest: u64,
    pub sm: SmAppliedMeta,
}

pub struct RocksMetaStore {
    db: Arc<DB>,
}

impl RocksMetaStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref()).map_err(|e| FluxError::Io(e.to_string()))?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = [
            ColumnFamilyDescriptor::new(CF_META, Options::default()),
            ColumnFamilyDescriptor::new(CF_LOGS, Options::default()),
            ColumnFamilyDescriptor::new(CF_SM_META, Options::default()),
            ColumnFamilyDescriptor::new(CF_SM_DATA, Options::default()),
        ];
        let db = DB::open_cf_descriptors(&opts, path.as_ref(), cfs)
            .map_err(|e| FluxError::Meta(format!("open rocksdb: {e}")))?;
        let store = Self { db: Arc::new(db) };
        store.ensure_root()?;
        Ok(store)
    }

    pub fn db(&self) -> Arc<DB> {
        self.db.clone()
    }

    fn cf_sm_data(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_SM_DATA).expect("cf sm_data")
    }

    fn cf_sm_meta(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_SM_META).expect("cf sm_meta")
    }

    fn ensure_root(&self) -> Result<()> {
        let cf = self.cf_sm_data();
        if self
            .db
            .get_cf(cf, inode_key(ROOT_INODE))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_some()
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let now = now_ms();
        let root = Inode {
            id: ROOT_INODE,
            file_type: FileType::Directory,
            mode: 0o755,
            uid: 0,
            gid: 0,
            size: 0,
            mtime_ms: now,
            ctime_ms: now,
            atime_ms: now,
            link_count: 2,
            generation: 1,
            head_gen: DataGen(1),
            ufs_gen: DataGen(0),
            ufs_base_version: None,
            locality: LocalityLabel::Ephemeral,
            locality_fields: Some(LocalityFields {
                backing_mode: BackingMode::Ephemeral,
                data_state: DataState::Ephemeral,
                op_state: OpState::None,
                origin: Origin::FluxCreated,
            }),
            ufs: None,
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        batch.put_cf(
            cf,
            inode_key(ROOT_INODE),
            serde_json::to_vec(&root).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        batch.put_cf(
            cf,
            KEY_NEXT_INODE,
            ROOT_INODE.saturating_add(1).to_be_bytes(),
        );
        batch.put_cf(cf, KEY_NEXT_MANIFEST, 1u64.to_be_bytes());
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    pub fn load_sm_meta(&self) -> Result<SmAppliedMeta> {
        let cf = self.cf_sm_meta();
        let last_applied_log = match self
            .db
            .get_cf(cf, KEY_SM_LAST_APPLIED)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            Some(bytes) => {
                Some(serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))?)
            }
            None => None,
        };
        let last_membership = match self
            .db
            .get_cf(cf, KEY_SM_LAST_MEMBERSHIP)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            Some(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))?
            }
            None => Default::default(),
        };
        Ok(SmAppliedMeta {
            last_applied_log,
            last_membership,
        })
    }

    fn put_sm_meta_batch(&self, batch: &mut WriteBatch, sm: &SmAppliedMeta) -> Result<()> {
        let cf = self.cf_sm_meta();
        batch.put_cf(
            cf,
            KEY_SM_LAST_APPLIED,
            serde_json::to_vec(&sm.last_applied_log).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        batch.put_cf(
            cf,
            KEY_SM_LAST_MEMBERSHIP,
            serde_json::to_vec(&sm.last_membership).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        Ok(())
    }

    pub fn save_sm_meta_only(&self, sm: &SmAppliedMeta) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.put_sm_meta_batch(&mut batch, sm)?;
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    /// Apply Raft-normal request + SM markers in one WriteBatch.
    pub fn apply_raft_request(
        &self,
        req: &MetaRaftRequest,
        sm: &SmAppliedMeta,
    ) -> Result<MetaRaftResponse> {
        let mut batch = WriteBatch::default();
        let resp = match req {
            MetaRaftRequest::Create {
                parent,
                name,
                file_type,
                mode,
                uid,
                gid,
            } => match self
                .create_in_batch(&mut batch, *parent, name, *file_type, *mode, *uid, *gid)
            {
                Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                Err(e) => MetaRaftResponse::Err(e),
            },
            MetaRaftRequest::PutInode { inode } => match self.put_inode_in_batch(&mut batch, inode)
            {
                Ok(()) => MetaRaftResponse::Empty,
                Err(e) => MetaRaftResponse::Err(e),
            },
            MetaRaftRequest::PutManifest { manifest } => {
                match self.put_manifest_in_batch(&mut batch, manifest) {
                    Ok(id) => MetaRaftResponse::ManifestId(id.0),
                    Err(e) => MetaRaftResponse::Err(e),
                }
            }
            MetaRaftRequest::Unlink { parent, name } => {
                match self.unlink_in_batch(&mut batch, *parent, name) {
                    Ok(()) => MetaRaftResponse::Empty,
                    Err(e) => MetaRaftResponse::Err(e),
                }
            }
        };
        self.put_sm_meta_batch(&mut batch, sm)?;
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(resp)
    }

    pub fn export_snapshot(&self, sm: &SmAppliedMeta) -> Result<MetaSnapshotData> {
        let cf = self.cf_sm_data();
        let mut inodes = Vec::new();
        let mut dentries = Vec::new();
        let mut manifests = Vec::new();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        for item in iter {
            let (k, v) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            if k.starts_with(b"i/") {
                inodes
                    .push(serde_json::from_slice(&v).map_err(|e| FluxError::Meta(e.to_string()))?);
            } else if k.starts_with(b"d/") {
                let rest = &k[2..];
                let s = std::str::from_utf8(rest).map_err(|e| FluxError::Meta(e.to_string()))?;
                let (parent_hex, name) = s
                    .split_once('\0')
                    .ok_or_else(|| FluxError::Meta("bad dentry key".into()))?;
                let parent = u64::from_str_radix(parent_hex, 16)
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
                let child = u64_from_bytes(&v)?;
                dentries.push(Dentry {
                    parent,
                    name: name.to_string(),
                    child,
                });
            } else if k.starts_with(b"m/") {
                let id = u64_from_bytes(&k[2..])?;
                let manifest: Manifest =
                    serde_json::from_slice(&v).map_err(|e| FluxError::Meta(e.to_string()))?;
                manifests.push((id, manifest));
            }
        }
        let next_inode = self.read_u64(cf, KEY_NEXT_INODE)?;
        let next_manifest = self.read_u64(cf, KEY_NEXT_MANIFEST)?;
        Ok(MetaSnapshotData {
            inodes,
            dentries,
            manifests,
            next_inode,
            next_manifest,
            sm: sm.clone(),
        })
    }

    pub fn install_snapshot_data(&self, snap: &MetaSnapshotData) -> Result<()> {
        let cf = self.cf_sm_data();
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        for item in iter {
            let (k, _) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            batch.delete_cf(cf, k);
        }
        for inode in &snap.inodes {
            batch.put_cf(
                cf,
                inode_key(inode.id),
                serde_json::to_vec(inode).map_err(|e| FluxError::Meta(e.to_string()))?,
            );
        }
        for d in &snap.dentries {
            batch.put_cf(cf, dentry_key(d.parent, &d.name), d.child.to_be_bytes());
        }
        for (id, manifest) in &snap.manifests {
            batch.put_cf(
                cf,
                manifest_key(*id),
                serde_json::to_vec(manifest).map_err(|e| FluxError::Meta(e.to_string()))?,
            );
        }
        batch.put_cf(cf, KEY_NEXT_INODE, snap.next_inode.to_be_bytes());
        batch.put_cf(cf, KEY_NEXT_MANIFEST, snap.next_manifest.to_be_bytes());
        self.put_sm_meta_batch(&mut batch, &snap.sm)?;
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    fn read_u64(&self, cf: &rocksdb::ColumnFamily, key: &[u8]) -> Result<u64> {
        let bytes = self
            .db
            .get_cf(cf, key)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or_else(|| {
                FluxError::Meta(format!("missing key {}", String::from_utf8_lossy(key)))
            })?;
        u64_from_bytes(&bytes)
    }

    fn alloc_inode(&self, batch: &mut WriteBatch) -> Result<InodeId> {
        let cf = self.cf_sm_data();
        let id = self.read_u64(cf, KEY_NEXT_INODE)?;
        batch.put_cf(cf, KEY_NEXT_INODE, (id + 1).to_be_bytes());
        Ok(id)
    }

    fn alloc_manifest_id(&self, batch: &mut WriteBatch) -> Result<ManifestId> {
        let cf = self.cf_sm_data();
        let id = self.read_u64(cf, KEY_NEXT_MANIFEST)?;
        batch.put_cf(cf, KEY_NEXT_MANIFEST, (id + 1).to_be_bytes());
        Ok(ManifestId(id))
    }

    #[allow(clippy::too_many_arguments)]
    fn create_in_batch(
        &self,
        batch: &mut WriteBatch,
        parent: InodeId,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        if name.is_empty() || name.contains('/') || name == "." || name == ".." {
            return Err(FluxError::InvalidArg(format!("bad name: {name}")));
        }
        let cf = self.cf_sm_data();
        let parent_ino: Inode = {
            let bytes = self
                .db
                .get_cf(cf, inode_key(parent))
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .ok_or(FluxError::NotFound)?;
            serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))?
        };
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        if self
            .db
            .get_cf(cf, dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_some()
        {
            return Err(FluxError::AlreadyExists);
        }
        let id = self.alloc_inode(batch)?;
        let now = now_ms();
        let inode = Inode {
            id,
            file_type,
            mode,
            uid,
            gid,
            size: 0,
            mtime_ms: now,
            ctime_ms: now,
            atime_ms: now,
            link_count: if file_type == FileType::Directory {
                2
            } else {
                1
            },
            generation: 1,
            head_gen: DataGen(1),
            ufs_gen: DataGen(0),
            ufs_base_version: None,
            locality: LocalityLabel::Ephemeral,
            locality_fields: Some(LocalityFields {
                backing_mode: BackingMode::Ephemeral,
                data_state: DataState::Ephemeral,
                op_state: OpState::None,
                origin: Origin::FluxCreated,
            }),
            ufs: None,
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        batch.put_cf(
            cf,
            inode_key(id),
            serde_json::to_vec(&inode).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        batch.put_cf(cf, dentry_key(parent, name), id.to_be_bytes());
        let mut parent_ino = parent_ino;
        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        batch.put_cf(
            cf,
            inode_key(parent),
            serde_json::to_vec(&parent_ino).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        Ok(inode)
    }

    fn put_inode_in_batch(&self, batch: &mut WriteBatch, inode: &Inode) -> Result<()> {
        let cf = self.cf_sm_data();
        batch.put_cf(
            cf,
            inode_key(inode.id),
            serde_json::to_vec(inode).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        Ok(())
    }

    fn put_manifest_in_batch(
        &self,
        batch: &mut WriteBatch,
        manifest: &Manifest,
    ) -> Result<ManifestId> {
        manifest.validate()?;
        let cf = self.cf_sm_data();
        let id = self.alloc_manifest_id(batch)?;
        batch.put_cf(
            cf,
            manifest_key(id.0),
            serde_json::to_vec(manifest).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        Ok(id)
    }

    fn unlink_in_batch(&self, batch: &mut WriteBatch, parent: InodeId, name: &str) -> Result<()> {
        let cf = self.cf_sm_data();
        let mut parent_ino: Inode = {
            let bytes = self
                .db
                .get_cf(cf, inode_key(parent))
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .ok_or(FluxError::NotFound)?;
            serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))?
        };
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        let key = dentry_key(parent, name);
        if self
            .db
            .get_cf(cf, &key)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_none()
        {
            return Err(FluxError::NotFound);
        }
        batch.delete_cf(cf, &key);
        let now = now_ms();
        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        batch.put_cf(
            cf,
            inode_key(parent),
            serde_json::to_vec(&parent_ino).map_err(|e| FluxError::Meta(e.to_string()))?,
        );
        Ok(())
    }
}

impl MetaStore for RocksMetaStore {
    fn get_inode(&self, id: InodeId) -> Result<Inode> {
        let cf = self.cf_sm_data();
        let bytes = self
            .db
            .get_cf(cf, inode_key(id))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?;
        serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))
    }

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        let parent_ino = self.get_inode(parent)?;
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        let cf = self.cf_sm_data();
        let child_raw = self
            .db
            .get_cf(cf, dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?;
        let child_id = u64_from_bytes(&child_raw)?;
        self.get_inode(child_id)
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
        let mut batch = WriteBatch::default();
        let inode = self.create_in_batch(&mut batch, parent, name, file_type, mode, uid, gid)?;
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(inode)
    }

    fn readdir(&self, dir: InodeId) -> Result<Vec<Dentry>> {
        let dir_ino = self.get_inode(dir)?;
        if dir_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        let cf = self.cf_sm_data();
        let prefix = format!("d/{dir:016x}\0");
        let mut out = Vec::new();
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (k, v) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            if !k.starts_with(prefix.as_bytes()) {
                break;
            }
            let s = std::str::from_utf8(&k[2..]).map_err(|e| FluxError::Meta(e.to_string()))?;
            let name = s
                .split_once('\0')
                .map(|(_, n)| n.to_string())
                .ok_or_else(|| FluxError::Meta("bad dentry key".into()))?;
            out.push(Dentry {
                parent: dir,
                name,
                child: u64_from_bytes(&v)?,
            });
        }
        Ok(out)
    }

    fn put_inode(&self, inode: &Inode) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.put_inode_in_batch(&mut batch, inode)?;
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    fn put_manifest(&self, manifest: &Manifest) -> Result<ManifestId> {
        let mut batch = WriteBatch::default();
        let id = self.put_manifest_in_batch(&mut batch, manifest)?;
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(id)
    }

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest> {
        let cf = self.cf_sm_data();
        let bytes = self
            .db
            .get_cf(cf, manifest_key(id.0))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?;
        serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))
    }

    fn unlink(&self, parent: InodeId, name: &str) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.unlink_in_batch(&mut batch, parent, name)?;
        self.db
            .write(batch)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }
}

fn inode_key(id: InodeId) -> Vec<u8> {
    let mut k = b"i/".to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

fn manifest_key(id: u64) -> Vec<u8> {
    let mut k = b"m/".to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

fn dentry_key(parent: InodeId, name: &str) -> Vec<u8> {
    format!("d/{parent:016x}\0{name}").into_bytes()
}

fn u64_from_bytes(bytes: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| FluxError::Meta("bad u64 bytes".into()))?;
    Ok(u64::from_be_bytes(arr))
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
    use crate::MetaStore;

    #[test]
    fn create_lookup_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksMetaStore::open(dir.path()).unwrap();
        let f = store
            .create(
                ROOT_INODE,
                "hello.txt",
                FileType::Regular,
                0o644,
                1000,
                1000,
            )
            .unwrap();
        let got = store.lookup(ROOT_INODE, "hello.txt").unwrap();
        assert_eq!(got.id, f.id);
        assert_eq!(got.file_type, FileType::Regular);
        let entries = store.readdir(ROOT_INODE).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello.txt");
    }
}
