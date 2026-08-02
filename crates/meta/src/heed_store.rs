use crate::store::MetaStore;
use fluxfs_types::{
    Dentry, FileType, FluxError, Inode, InodeId, LocalityLabel, Result, ROOT_INODE,
};
use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

type InodeDb = Database<Bytes, Bytes>;
type DentryDb = Database<Str, Bytes>;
type MetaDb = Database<Str, Bytes>;

pub struct HeedMetaStore {
    env: Env,
    inodes: InodeDb,
    dentries: DentryDb,
    meta: MetaDb,
    /// Serialize writers; LMDB allows one write txn at a time anyway.
    write_lock: Mutex<()>,
}

impl HeedMetaStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref()).map_err(|e| FluxError::Io(e.to_string()))?;
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(512 * 1024 * 1024)
                .max_dbs(8)
                .open(path.as_ref())
                .map_err(|e| FluxError::Meta(e.to_string()))?
        };

        let mut wtxn = env.write_txn().map_err(|e| FluxError::Meta(e.to_string()))?;
        let inodes: InodeDb = env
            .create_database(&mut wtxn, Some("inodes"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let dentries: DentryDb = env
            .create_database(&mut wtxn, Some("dentries"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let meta: MetaDb = env
            .create_database(&mut wtxn, Some("meta"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        if inodes
            .get(&wtxn, &inode_key(ROOT_INODE))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_none()
        {
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
                locality: LocalityLabel::Ephemeral,
                ufs: None,
                extent_root: None,
            };
            put_inode_raw(&inodes, &mut wtxn, &root)?;
            meta.put(&mut wtxn, "next_inode", &u64_bytes(ROOT_INODE + 1))
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        }

        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;

        Ok(Self {
            env,
            inodes,
            dentries,
            meta,
            write_lock: Mutex::new(()),
        })
    }

    fn alloc_inode(&self, wtxn: &mut heed::RwTxn) -> Result<InodeId> {
        let raw = self
            .meta
            .get(wtxn, "next_inode")
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or_else(|| FluxError::Meta("missing next_inode".into()))?;
        let id = u64_from_bytes(raw)?;
        self.meta
            .put(wtxn, "next_inode", &u64_bytes(id + 1))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(id)
    }
}

impl MetaStore for HeedMetaStore {
    fn get_inode(&self, id: InodeId) -> Result<Inode> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let bytes = self
            .inodes
            .get(&rtxn, &inode_key(id))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        drop(rtxn);
        serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))
    }

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let parent_bytes = self
            .inodes
            .get(&rtxn, &inode_key(parent))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let parent_ino: Inode =
            serde_json::from_slice(&parent_bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        let child_raw = self
            .dentries
            .get(&rtxn, &dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let child_id = u64_from_bytes(&child_raw)?;
        let child_bytes = self
            .inodes
            .get(&rtxn, &inode_key(child_id))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        drop(rtxn);
        serde_json::from_slice(&child_bytes).map_err(|e| FluxError::Meta(e.to_string()))
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
        if name.is_empty() || name.contains('/') || name == "." || name == ".." {
            return Err(FluxError::InvalidArg(format!("bad name: {name}")));
        }
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;

        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        let parent_bytes = self
            .inodes
            .get(&wtxn, &inode_key(parent))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let mut parent_ino: Inode =
            serde_json::from_slice(&parent_bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }

        if self
            .dentries
            .get(&wtxn, &dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_some()
        {
            return Err(FluxError::AlreadyExists);
        }

        let id = self.alloc_inode(&mut wtxn)?;
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
            locality: LocalityLabel::Ephemeral,
            ufs: None,
            extent_root: None,
        };
        put_inode_raw(&self.inodes, &mut wtxn, &inode)?;
        self.dentries
            .put(&mut wtxn, &dentry_key(parent, name), &u64_bytes(id))
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        put_inode_raw(&self.inodes, &mut wtxn, &parent_ino)?;

        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(inode)
    }

    fn readdir(&self, dir: InodeId) -> Result<Vec<Dentry>> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let dir_bytes = self
            .inodes
            .get(&rtxn, &inode_key(dir))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let dir_ino: Inode =
            serde_json::from_slice(&dir_bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        if dir_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        let prefix = format!("{dir:016x}\0");
        let mut out = Vec::new();
        let iter = self
            .dentries
            .prefix_iter(&rtxn, &prefix)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        for item in iter {
            let (key, val) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let name = key
                .split_once('\0')
                .map(|(_, n)| n.to_string())
                .ok_or_else(|| FluxError::Meta("bad dentry key".into()))?;
            out.push(Dentry {
                parent: dir,
                name,
                child: u64_from_bytes(val)?,
            });
        }
        drop(rtxn);
        Ok(out)
    }

    fn put_inode(&self, inode: &Inode) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        put_inode_raw(&self.inodes, &mut wtxn, inode)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }
}

fn put_inode_raw(db: &InodeDb, wtxn: &mut heed::RwTxn, inode: &Inode) -> Result<()> {
    let bytes = serde_json::to_vec(inode).map_err(|e| FluxError::Meta(e.to_string()))?;
    db.put(wtxn, &inode_key(inode.id), &bytes)
        .map_err(|e| FluxError::Meta(e.to_string()))
}

fn inode_key(id: InodeId) -> [u8; 8] {
    id.to_be_bytes()
}

fn dentry_key(parent: InodeId, name: &str) -> String {
    format!("{parent:016x}\0{name}")
}

fn u64_bytes(v: u64) -> [u8; 8] {
    v.to_be_bytes()
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
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let f = store
            .create(ROOT_INODE, "hello.txt", FileType::Regular, 0o644, 1000, 1000)
            .unwrap();
        let got = store.lookup(ROOT_INODE, "hello.txt").unwrap();
        assert_eq!(got.id, f.id);
        assert_eq!(got.file_type, FileType::Regular);
        let entries = store.readdir(ROOT_INODE).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello.txt");
    }
}
