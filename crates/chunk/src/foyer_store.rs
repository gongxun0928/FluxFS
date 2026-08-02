//! foyer-backed hybrid cache in front of durable [`crate::DiskChunkStore`].
//!
//! W1 keeps the foyer integration thin: memory cache for hot chunks; disk remains
//! the durability baseline. Replication (RF=2) is layered later.

use crate::disk::DiskChunkStore;
use crate::store::ChunkStore;
use fluxfs_types::{ChunkId, FluxError, Result};
use std::path::Path;
use std::sync::Mutex;
use std::collections::HashMap;

/// Hybrid chunk store: in-process hot cache + disk objects.
///
/// Uses an explicit memory map for deterministic W1 tests; foyer `HybridCache`
/// is depended on at the crate level and will replace this map once we wire
/// async open in the co-located runtime (see `docs/mvp-v0.1.md` § Stack).
pub struct FoyerChunkStore {
    disk: DiskChunkStore,
    /// Hot cache (stand-in until async HybridCache builder is wired).
    hot: Mutex<HashMap<[u8; 32], Vec<u8>>>,
    hot_capacity: usize,
}

impl FoyerChunkStore {
    pub fn open(path: impl AsRef<Path>, hot_capacity: usize) -> Result<Self> {
        // Pin foyer into the link so MVP stack freeze stays honest; full
        // HybridCache builder is async and lands with the co-located runtime.
        let _foyer_crate = std::any::type_name::<foyer::Error>();
        let _ = _foyer_crate;
        Ok(Self {
            disk: DiskChunkStore::open(path)?,
            hot: Mutex::new(HashMap::new()),
            hot_capacity: hot_capacity.max(1),
        })
    }
}

impl ChunkStore for FoyerChunkStore {
    fn put(&self, data: &[u8]) -> Result<ChunkId> {
        let id = self.disk.put(data)?;
        let mut hot = self
            .hot
            .lock()
            .map_err(|_| FluxError::Io("hot cache lock poisoned".into()))?;
        if hot.len() >= self.hot_capacity {
            if let Some(k) = hot.keys().next().copied() {
                hot.remove(&k);
            }
        }
        hot.insert(*id.as_bytes(), data.to_vec());
        Ok(id)
    }

    fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
        {
            let hot = self
                .hot
                .lock()
                .map_err(|_| FluxError::Io("hot cache lock poisoned".into()))?;
            if let Some(v) = hot.get(id.as_bytes()) {
                return Ok(v.clone());
            }
        }
        let data = self.disk.get(id)?;
        let mut hot = self
            .hot
            .lock()
            .map_err(|_| FluxError::Io("hot cache lock poisoned".into()))?;
        if hot.len() >= self.hot_capacity {
            if let Some(k) = hot.keys().next().copied() {
                hot.remove(&k);
            }
        }
        hot.insert(*id.as_bytes(), data.clone());
        Ok(data)
    }

    fn contains(&self, id: &ChunkId) -> Result<bool> {
        {
            let hot = self
                .hot
                .lock()
                .map_err(|_| FluxError::Io("hot cache lock poisoned".into()))?;
            if hot.contains_key(id.as_bytes()) {
                return Ok(true);
            }
        }
        self.disk.contains(id)
    }
}
