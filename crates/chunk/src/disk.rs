use crate::store::ChunkStore;
use fluxfs_types::{ChunkId, FluxError, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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

    fn object_path(&self, id: &ChunkId) -> PathBuf {
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
        if path.exists() {
            return Ok(id);
        }
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Io("chunk write lock poisoned".into()))?;
        if path.exists() {
            return Ok(id);
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| FluxError::Io(e.to_string()))?;
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data).map_err(|e| FluxError::Io(e.to_string()))?;
        fs::rename(&tmp, &path).map_err(|e| FluxError::Io(e.to_string()))?;
        Ok(id)
    }

    fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
        let path = self.object_path(id);
        fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FluxError::NotFound
            } else {
                FluxError::Io(e.to_string())
            }
        })
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
}
