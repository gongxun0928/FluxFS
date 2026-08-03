//! foyer-backed hybrid cache in front of durable [`crate::DiskChunkStore`].
//!
//! Architecture (P0-B8 / task #29 + #38):
//! - **DiskChunkStore** remains the authoritative durable store for Dirty
//!   (and all Worker PutChunk) data. Eviction from the hybrid cache never
//!   deletes or mutates packfile contents.
//! - **foyer HybridCache** (DRAM + optional SSD) is a Clean/hot tier.
//!   `put` never inserts. Durable `get` does **not** auto-warm; callers pass
//!   `promote_cache=true` (via [`ChunkStore::get_with_promote`] / GetChunk RPC)
//!   for Clean reads so Dirty traffic does not consume foyer DRAM.

use crate::disk::DiskChunkStore;
use crate::pack::CompactReport;
use crate::store::ChunkStore;
use fluxfs_types::{ChunkId, ChunkPage, FluxError, Result, WorkerTargetId};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, HybridCacheProperties, Location, PsyncIoEngineConfig, RecoverMode,
};
use std::path::{Path, PathBuf};
use tokio::runtime::Handle;

/// Configuration for the Clean/hot HybridCache tier.
#[derive(Debug, Clone)]
pub struct FoyerCacheConfig {
    /// DRAM capacity in bytes (foyer memory weighter sums value lengths).
    pub memory_capacity_bytes: usize,
    /// SSD cache device capacity. `0` disables the disk tier (memory-only).
    pub disk_capacity_bytes: usize,
    /// Directory for the foyer disk-cache device (created if missing).
    pub cache_dir: PathBuf,
}

impl FoyerCacheConfig {
    pub fn new(
        cache_dir: impl Into<PathBuf>,
        memory_capacity_bytes: usize,
        disk_capacity_bytes: usize,
    ) -> Self {
        Self {
            memory_capacity_bytes: memory_capacity_bytes.max(64 * 1024),
            disk_capacity_bytes,
            cache_dir: cache_dir.into(),
        }
    }
}

/// Hybrid chunk store: authoritative disk + evictable Clean/hot HybridCache.
pub struct FoyerChunkStore {
    disk: DiskChunkStore,
    cache: HybridCache<Vec<u8>, Vec<u8>>,
    /// Runtime that owns the foyer spawner; used to bridge sync [`ChunkStore`].
    runtime: Handle,
}

impl FoyerChunkStore {
    /// Open durable disk under `data_dir` and build a foyer HybridCache.
    ///
    /// Must be called from a Tokio runtime (foyer storage open is async).
    pub async fn open(data_dir: impl AsRef<Path>, config: FoyerCacheConfig) -> Result<Self> {
        let disk = DiskChunkStore::open(data_dir)?;
        let cache = build_hybrid_cache(&config).await?;
        Ok(Self {
            disk,
            cache,
            runtime: Handle::current(),
        })
    }

    /// Insert bytes into the hybrid cache without touching the packfile.
    ///
    /// Production reaches this when GetChunk sets `promote_cache=true` (Clean
    /// reads). Dirty / repair paths omit promotion so foyer DRAM stays Clean-biased.
    pub fn cache_clean(&self, id: &ChunkId, data: &[u8]) {
        let props = HybridCacheProperties::default().with_location(Location::Default);
        self.cache
            .insert_with_properties(id.as_bytes().to_vec(), data.to_vec(), props);
    }

    /// True if the hybrid cache currently holds `id` (memory or disk tier).
    pub fn cache_contains(&self, id: &ChunkId) -> bool {
        self.cache.contains(id.as_bytes().as_slice())
    }

    /// Drop a key from the hybrid cache only (packfile untouched).
    pub fn cache_remove(&self, id: &ChunkId) {
        self.cache.remove(id.as_bytes().as_slice());
    }

    /// Flush hybrid cache state and close the disk tier (for restart tests).
    pub async fn close_cache(&self) -> Result<()> {
        self.cache
            .close()
            .await
            .map_err(|e| FluxError::Io(format!("foyer close: {e}")))
    }

    /// Rewrite live chunks into a fresh segment (forwards to pack store).
    pub fn compact(&self) -> Result<CompactReport> {
        self.disk.compact()
    }

    /// Number of on-disk pack segment files.
    pub fn segment_file_count(&self) -> Result<usize> {
        self.disk.segment_file_count()
    }

    fn cache_get(&self, id: &ChunkId) -> Result<Option<Vec<u8>>> {
        let key = id.as_bytes().to_vec();
        // Bridged from sync ChunkStore (ChunkWorker uses spawn_blocking).
        let entry = self
            .runtime
            .block_on(self.cache.get(&key))
            .map_err(|e| FluxError::Io(format!("foyer get: {e}")))?;
        Ok(entry.map(|e| e.value().clone()))
    }
}

async fn build_hybrid_cache(config: &FoyerCacheConfig) -> Result<HybridCache<Vec<u8>, Vec<u8>>> {
    std::fs::create_dir_all(&config.cache_dir).map_err(|e| {
        FluxError::Io(format!(
            "create foyer cache dir {}: {e}",
            config.cache_dir.display()
        ))
    })?;

    let memory = config.memory_capacity_bytes;
    let builder = HybridCacheBuilder::new()
        .with_name("fluxfs-clean-chunks")
        // Persist Clean entries to the SSD tier on insert so a Worker restart
        // can recover hot Clean data without re-reading the packfile / UFS.
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .with_flush_on_close(true)
        .memory(memory)
        .with_weighter(|_k, v: &Vec<u8>| v.len().max(1))
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_recover_mode(RecoverMode::Quiet);

    let hybrid = if config.disk_capacity_bytes == 0 {
        // Memory-only: noop storage engine (still a real HybridCache).
        builder
            .build()
            .await
            .map_err(|e| FluxError::Io(format!("foyer open (memory-only): {e}")))?
    } else {
        let device = FsDeviceBuilder::new(&config.cache_dir)
            .with_capacity(config.disk_capacity_bytes)
            .build()
            .map_err(|e| FluxError::Io(format!("foyer device: {e}")))?;
        // Keep block size modest so small test capacities still admit entries.
        // Default prod (256 MiB disk) → raw /4 = 64 MiB → clamp to 4 MiB max.
        // Sub-MiB disk fixtures should prefer disk_capacity_bytes=0 (memory-only).
        let block_size = (config.disk_capacity_bytes / 4).clamp(64 * 1024, 4 * 1024 * 1024);
        builder
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(block_size))
            .build()
            .await
            .map_err(|e| FluxError::Io(format!("foyer open: {e}")))?
    };
    Ok(hybrid)
}

impl ChunkStore for FoyerChunkStore {
    fn put(&self, data: &[u8]) -> Result<ChunkId> {
        // Authoritative Dirty path: packfile only. Never insert into the
        // evictable HybridCache so Dirty durability cannot depend on foyer.
        self.disk.put(data)
    }

    fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
        self.get_with_promote(id, false)
    }

    fn get_with_promote(&self, id: &ChunkId, promote_cache: bool) -> Result<Vec<u8>> {
        if let Some(data) = self.cache_get(id)? {
            return Ok(data);
        }
        let data = self.disk.get(id)?;
        if promote_cache {
            self.cache_clean(id, &data);
        }
        Ok(data)
    }

    fn contains(&self, id: &ChunkId) -> Result<bool> {
        // Authoritative durability for RF/repair/inventory — foyer is never a
        // replica source of truth (Clean cache-only entries must not count).
        self.disk.contains(id)
    }

    fn list_chunks(&self) -> Result<Vec<ChunkId>> {
        self.disk.list_chunks()
    }

    fn list_chunks_page(&self, cursor: Option<ChunkId>, limit: usize) -> Result<ChunkPage> {
        self.disk.list_chunks_page(cursor, limit)
    }

    fn delete(&self, id: &ChunkId) -> Result<()> {
        self.cache_remove(id);
        self.disk.delete(id)
    }

    fn gc_delete_targets(&self) -> Result<Vec<WorkerTargetId>> {
        self.disk.gc_delete_targets()
    }

    fn delete_from_target(&self, id: &ChunkId, target: WorkerTargetId) -> Result<()> {
        // GC uses this entry point; keep the same dual-clear as delete().
        self.cache_remove(id);
        self.disk.delete_from_target(id, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    async fn open_store(disk_cap: usize) -> (tempfile::TempDir, FoyerChunkStore) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = FoyerCacheConfig::new(dir.path().join("foyer"), 256 * 1024, disk_cap);
        let store = FoyerChunkStore::open(dir.path().join("pack"), cfg)
            .await
            .unwrap();
        (dir, store)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_stays_out_of_cache_until_promote() {
        let (_dir, store) = open_store(0).await;
        let store = Arc::new(store);
        let s = Arc::clone(&store);
        let id = tokio::task::spawn_blocking(move || s.put(b"dirty-bytes"))
            .await
            .unwrap()
            .unwrap();
        assert!(
            !store.cache_contains(&id),
            "Dirty put must not enter HybridCache"
        );
        let s = Arc::clone(&store);
        let got = tokio::task::spawn_blocking(move || s.get(&id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, b"dirty-bytes");
        assert!(
            !store.cache_contains(&id),
            "default get must not promote Dirty reads"
        );
        let s = Arc::clone(&store);
        let got = tokio::task::spawn_blocking(move || s.get_with_promote(&id, true))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, b"dirty-bytes");
        assert!(
            store.cache_contains(&id),
            "promote_cache=true must warm HybridCache"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_eviction_does_not_drop_durable_dirty() {
        let (_dir, store) = open_store(0).await;
        let store = Arc::new(store);
        // Tiny memory (weighter = len) so inserts thrash the DRAM tier.
        // Re-open with smaller memory via cache_clean pressure on same store:
        // fill with many clean keys, then verify Dirty packfile bytes survive.
        let s = Arc::clone(&store);
        let dirty_id = tokio::task::spawn_blocking(move || s.put(b"authoritative-dirty"))
            .await
            .unwrap()
            .unwrap();
        assert!(!store.cache_contains(&dirty_id));

        for i in 0..64u32 {
            let payload = vec![i as u8; 8 * 1024];
            let id = ChunkId::from_bytes(&payload);
            store.cache_clean(&id, &payload);
        }
        store.cache_remove(&dirty_id);

        let s = Arc::clone(&store);
        let got = tokio::task::spawn_blocking(move || s.get(&dirty_id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, b"authoritative-dirty");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clean_survives_cache_restart_on_ssd_tier() {
        let dir = tempfile::tempdir().unwrap();
        let pack = dir.path().join("pack");
        let foyer_dir = dir.path().join("foyer");
        let cfg = FoyerCacheConfig::new(&foyer_dir, 1024 * 1024, 16 * 1024 * 1024);
        let payload = vec![7u8; 3 * 1024];
        let id = ChunkId::from_bytes(&payload);

        {
            let store = FoyerChunkStore::open(&pack, cfg.clone()).await.unwrap();
            // Cache-only Clean entry — never written to packfile, so a reopen
            // hit cannot be explained by DiskChunkStore fallback.
            store.cache_clean(&id, &payload);
            assert!(store.cache_contains(&id));
            let s = Arc::new(store);
            let s2 = Arc::clone(&s);
            assert!(
                !tokio::task::spawn_blocking(move || s2.contains(&id))
                    .await
                    .unwrap()
                    .unwrap(),
                "cache-only Clean must not count as durable contains"
            );
            s.close_cache().await.unwrap();
        }

        let store = FoyerChunkStore::open(&pack, cfg).await.unwrap();
        assert!(
            store.cache_contains(&id),
            "SSD tier must recover Clean entry without packfile"
        );
        let s = Arc::new(store);
        let s2 = Arc::clone(&s);
        let got = tokio::task::spawn_blocking(move || s2.get(&id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, payload);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn contains_ignores_cache_only_and_gc_delete_clears_cache() {
        let (_dir, store) = open_store(0).await;
        let store = Arc::new(store);
        let s = Arc::clone(&store);
        let id = tokio::task::spawn_blocking(move || s.put(b"gc-me"))
            .await
            .unwrap()
            .unwrap();
        store.cache_clean(&id, b"gc-me");
        assert!(store.cache_contains(&id));
        let s = Arc::clone(&store);
        assert!(tokio::task::spawn_blocking(move || s.contains(&id))
            .await
            .unwrap()
            .unwrap());

        let s = Arc::clone(&store);
        tokio::task::spawn_blocking(move || {
            s.delete_from_target(&id, fluxfs_types::WorkerTargetId(0))
        })
        .await
        .unwrap()
        .unwrap();
        assert!(!store.cache_contains(&id));
        let s = Arc::clone(&store);
        assert!(!tokio::task::spawn_blocking(move || s.contains(&id))
            .await
            .unwrap()
            .unwrap());
        let s = Arc::clone(&store);
        let err = tokio::task::spawn_blocking(move || s.get(&id))
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, FluxError::NotFound));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_removes_cache_and_disk() {
        let (_dir, store) = open_store(0).await;
        let store = Arc::new(store);
        let s = Arc::clone(&store);
        let id = tokio::task::spawn_blocking(move || s.put(b"to-delete"))
            .await
            .unwrap()
            .unwrap();
        store.cache_clean(&id, b"to-delete");
        assert!(store.cache_contains(&id));
        let s = Arc::clone(&store);
        tokio::task::spawn_blocking(move || s.delete(&id))
            .await
            .unwrap()
            .unwrap();
        assert!(!store.cache_contains(&id));
        let s = Arc::clone(&store);
        let err = tokio::task::spawn_blocking(move || s.get(&id))
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, FluxError::NotFound));
    }
}
