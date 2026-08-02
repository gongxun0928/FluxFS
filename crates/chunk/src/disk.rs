use crate::store::ChunkStore;
use fluxfs_types::{ChunkId, FluxError, Result};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Content-addressed chunks on local filesystem (W1 smoke / durability baseline).
pub struct DiskChunkStore {
    root: PathBuf,
    write_lock: Mutex<()>,
}

impl DiskChunkStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects")).map_err(|e| FluxError::Io(e.to_string()))?;
        Ok(Self {
            root,
            write_lock: Mutex::new(()),
        })
    }

    pub(crate) fn object_path(&self, id: &ChunkId) -> PathBuf {
        let hex = id.to_hex();
        self.root
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
    }
}

impl ChunkStore for DiskChunkStore {
    fn put(&self, data: &[u8]) -> Result<ChunkId> {
        let id = ChunkId::from_bytes(data);
        let path = self.object_path(&id);
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Io("chunk write lock poisoned".into()))?;

        // A prior process may have left a corrupt object at the content address.
        // Trust bytes, not existence: a repeated put must repair that replica.
        if fs::read(&path)
            .map(|existing| ChunkId::from_bytes(&existing) == id)
            .unwrap_or(false)
        {
            return Ok(id);
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| FluxError::Io(e.to_string()))?;
        }

        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("tmp-{}-{sequence}", std::process::id()));
        let write_result = (|| -> std::io::Result<()> {
            let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            file.write_all(data)?;
            file.sync_all()?;
            fs::rename(&tmp, &path)?;
            // Persist the directory entry before reporting the replica durable.
            if let Some(parent) = path.parent() {
                fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&tmp);
            return Err(FluxError::Io(error.to_string()));
        }
        Ok(id)
    }

    fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
        let path = self.object_path(id);
        let data = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FluxError::NotFound
            } else {
                FluxError::Io(e.to_string())
            }
        })?;
        if ChunkId::from_bytes(&data) != *id {
            return Err(FluxError::Io(format!(
                "chunk checksum mismatch: {}",
                id.to_hex()
            )));
        }
        Ok(data)
    }

    fn contains(&self, id: &ChunkId) -> Result<bool> {
        Ok(self.object_path(id).exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::open(dir.path()).unwrap();
        let id = store.put(b"fluxfs-chunk").unwrap();
        let got = store.get(&id).unwrap();
        assert_eq!(got, b"fluxfs-chunk");
        assert!(store.contains(&id).unwrap());
    }

    #[test]
    fn repeated_put_repairs_corrupt_object() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskChunkStore::open(dir.path()).unwrap();
        let id = store.put(b"authoritative").unwrap();
        fs::write(store.object_path(&id), b"corrupt").unwrap();
        assert!(store.get(&id).is_err());
        assert_eq!(store.put(b"authoritative").unwrap(), id);
        assert_eq!(store.get(&id).unwrap(), b"authoritative");
    }
}
