//! Bounded, version-aware UFS range cache with parallel misses and single-flight.
//!
//! P0-B8 / task #39: Clean/External hot path uses foyer `HybridCache` (DRAM +
//! optional SSD). Dirty `Extent::Local` traffic never enters this cache — it
//! stays on the ChunkWorker packfile path.

use crate::Ufs;
use fluxfs_types::{FluxError, Result, UfsObject};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, PsyncIoEngineConfig, RecoverMode, Source,
};
use futures::future::try_join_all;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[derive(Debug, Clone)]
pub struct ReadPathConfig {
    /// Cache/fetch granularity. The MVP default limits amplification while still
    /// allowing four requests to cover one 4 MiB FluxFS chunk in parallel.
    pub part_size: u64,
    /// Soft bound on cached parts (maps to foyer DRAM weighter capacity).
    pub max_cached_parts: usize,
    /// Number of parts scheduled after the requested range.
    pub prefetch_parts: usize,
    /// Optional SSD tier capacity. `0` = memory-only HybridCache.
    pub disk_capacity_bytes: usize,
    /// Directory for the foyer disk device when `disk_capacity_bytes > 0`.
    pub cache_dir: Option<PathBuf>,
}

impl Default for ReadPathConfig {
    fn default() -> Self {
        Self {
            part_size: 1024 * 1024,
            max_cached_parts: 256,
            prefetch_parts: 2,
            disk_capacity_bytes: 0,
            cache_dir: None,
        }
    }
}

impl ReadPathConfig {
    fn memory_capacity_bytes(&self) -> usize {
        let part = self.part_size.max(1) as usize;
        self.max_cached_parts.max(1).saturating_mul(part).max(64 * 1024)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadPathStats {
    pub backend_fetches: u64,
    pub cache_hits: u64,
}

/// Read policy layered above OpenDAL.
///
/// Required parts are fetched concurrently. Concurrent readers of one part
/// share the same foyer `get_or_fetch`, and successful parts remain in a
/// bounded HybridCache (DRAM + optional SSD).
pub struct UfsReadPath {
    ufs: Ufs,
    config: ReadPathConfig,
    cache: HybridCache<String, Vec<u8>>,
    backend_fetches: AtomicU64,
    cache_hits: AtomicU64,
}

impl UfsReadPath {
    /// Open the Clean/External HybridCache. Must run on a Tokio runtime.
    pub async fn open(ufs: Ufs, config: ReadPathConfig) -> Result<Arc<Self>> {
        if config.part_size == 0 {
            return Err(FluxError::InvalidArg("UFS part_size must be > 0".into()));
        }
        if config.max_cached_parts == 0 {
            return Err(FluxError::InvalidArg(
                "UFS max_cached_parts must be > 0".into(),
            ));
        }
        let cache = build_hybrid_cache(&config).await?;
        Ok(Arc::new(Self {
            ufs,
            config,
            cache,
            backend_fetches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }))
    }

    /// Flush and close the HybridCache disk tier (tests / graceful shutdown).
    pub async fn close_cache(&self) -> Result<()> {
        self.cache
            .close()
            .await
            .map_err(|e| FluxError::Ufs(format!("foyer close: {e}")))
    }

    /// Sync helper for call sites that already own / can borrow a runtime.
    pub fn new(ufs: Ufs, config: ReadPathConfig) -> Result<Arc<Self>> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(Self::open(ufs, config))),
            Err(_) => {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| FluxError::Ufs(format!("ufs read-path runtime: {e}")))?;
                rt.block_on(Self::open(ufs, config))
            }
        }
    }

    pub fn stats(&self) -> ReadPathStats {
        ReadPathStats {
            backend_fetches: self.backend_fetches.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
        }
    }

    /// Read one logical range. `object` pins the size and, when present, ETag.
    pub async fn read(
        self: &Arc<Self>,
        rel: &str,
        object: &UfsObject,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        if len == 0 || offset >= object.size {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(len).min(object.size);
        let first = offset / self.config.part_size;
        let last = (end - 1) / self.config.part_size;
        let version = version_token(object);

        let futures = (first..=last)
            .map(|index| self.get_part(rel.to_string(), version.clone(), object.clone(), index));
        let parts = try_join_all(futures).await?;

        let mut result = Vec::with_capacity((end - offset) as usize);
        for (position, part) in parts.into_iter().enumerate() {
            let index = first + position as u64;
            let part_start = index * self.config.part_size;
            let copy_start = offset.saturating_sub(part_start) as usize;
            let copy_end = (end.min(part_start + part.len() as u64) - part_start) as usize;
            if copy_end < copy_start || copy_end > part.len() {
                return Err(FluxError::Ufs("cached UFS part bounds mismatch".into()));
            }
            result.extend_from_slice(&part[copy_start..copy_end]);
        }

        self.spawn_prefetch(rel, object, last + 1);
        Ok(result)
    }

    async fn get_part(
        self: &Arc<Self>,
        rel: String,
        version: String,
        object: UfsObject,
        index: u64,
    ) -> Result<Arc<Vec<u8>>> {
        let key = part_cache_key(&rel, &version, index);
        let this = Arc::clone(self);
        let fetch_key = key.clone();
        let entry = self
            .cache
            .get_or_fetch(&key, || {
                let this = Arc::clone(&this);
                let rel = rel.clone();
                let object = object.clone();
                async move {
                    this.backend_fetches.fetch_add(1, Ordering::Relaxed);
                    let start = index * this.config.part_size;
                    let expected = this.config.part_size.min(object.size - start);
                    let data = this
                        .ufs
                        .read_range_pinned(&rel, start, expected, object.etag.as_deref())
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    if data.len() as u64 != expected {
                        return Err(anyhow::anyhow!(
                            "short UFS range read: path={rel} offset={start} expected={expected} actual={}",
                            data.len()
                        ));
                    }
                    Ok(data)
                }
            })
            .await
            .map_err(|e| FluxError::Ufs(format!("foyer get_or_fetch {fetch_key}: {e}")))?;
        if matches!(entry.source(), Source::Memory | Source::Disk) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Arc::new(entry.value().clone()))
    }

    fn spawn_prefetch(self: &Arc<Self>, rel: &str, object: &UfsObject, first: u64) {
        let total_parts = object.size.div_ceil(self.config.part_size);
        let end = first
            .saturating_add(self.config.prefetch_parts as u64)
            .min(total_parts);
        let version = version_token(object);
        for index in first..end {
            let this = Arc::clone(self);
            let rel = rel.to_string();
            let object = object.clone();
            let version = version.clone();
            tokio::spawn(async move {
                let _ = this.get_part(rel, version, object, index).await;
            });
        }
    }
}

async fn build_hybrid_cache(config: &ReadPathConfig) -> Result<HybridCache<String, Vec<u8>>> {
    let memory = config.memory_capacity_bytes();
    let builder = HybridCacheBuilder::new()
        .with_name("fluxfs-ufs-clean")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .with_flush_on_close(true)
        .memory(memory)
        .with_weighter(|_k, v: &Vec<u8>| v.len().max(1))
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_recover_mode(RecoverMode::Quiet);

    let hybrid = if config.disk_capacity_bytes == 0 {
        builder
            .build()
            .await
            .map_err(|e| FluxError::Ufs(format!("foyer open (memory-only): {e}")))?
    } else {
        let dir = config.cache_dir.clone().ok_or_else(|| {
            FluxError::InvalidArg(
                "UFS Clean cache_dir required when disk_capacity_bytes > 0".into(),
            )
        })?;
        std::fs::create_dir_all(&dir)
            .map_err(|e| FluxError::Ufs(format!("create ufs foyer dir {}: {e}", dir.display())))?;
        let device = FsDeviceBuilder::new(&dir)
            .with_capacity(config.disk_capacity_bytes)
            .build()
            .map_err(|e| FluxError::Ufs(format!("foyer device: {e}")))?;
        let block_size = (config.disk_capacity_bytes / 4).clamp(64 * 1024, 4 * 1024 * 1024);
        builder
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(block_size))
            .build()
            .await
            .map_err(|e| FluxError::Ufs(format!("foyer open: {e}")))?
    };
    Ok(hybrid)
}

fn part_cache_key(rel: &str, version: &str, index: u64) -> String {
    format!("{rel}\0{version}\0{index}")
}

fn version_token(object: &UfsObject) -> String {
    object.etag.clone().unwrap_or_else(|| {
        format!(
            "size={};mtime={}",
            object.size,
            object.mtime_ms.unwrap_or_default()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(prefetch_parts: usize) -> ReadPathConfig {
        ReadPathConfig {
            part_size: 4,
            max_cached_parts: 16,
            prefetch_parts,
            disk_capacity_bytes: 0,
            cache_dir: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parallel_parts_are_cached() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        ufs.write_full("obj", b"0123456789abcdef").await.unwrap();
        let object = ufs.head("obj").await.unwrap();
        let path = UfsReadPath::open(ufs, config(0)).await.unwrap();

        assert_eq!(
            path.read("obj", &object, 2, 10).await.unwrap(),
            b"23456789ab"
        );
        assert_eq!(path.stats().backend_fetches, 3);
        assert_eq!(
            path.read("obj", &object, 2, 10).await.unwrap(),
            b"23456789ab"
        );
        assert_eq!(path.stats().backend_fetches, 3);
        assert_eq!(path.stats().cache_hits, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_readers_share_one_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        ufs.write_full("obj", b"01234567").await.unwrap();
        let object = ufs.head("obj").await.unwrap();
        let path = UfsReadPath::open(ufs, config(0)).await.unwrap();

        let reads = (0..16).map(|_| {
            let path = Arc::clone(&path);
            let object = object.clone();
            async move { path.read("obj", &object, 0, 4).await }
        });
        let results = try_join_all(reads).await.unwrap();
        assert!(results.iter().all(|data| data == b"0123"));
        assert_eq!(path.stats().backend_fetches, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn short_range_is_rejected_and_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        ufs.write_full("obj", b"abc").await.unwrap();
        let mut object = ufs.head("obj").await.unwrap();
        object.size = 4;
        let path = UfsReadPath::open(ufs, config(0)).await.unwrap();

        assert!(path.read("obj", &object, 0, 4).await.is_err());
        assert!(path.read("obj", &object, 0, 4).await.is_err());
        assert_eq!(path.stats().backend_fetches, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clean_ssd_tier_survives_reopen() {
        let ufs_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(ufs_dir.path()).unwrap();
        ufs.write_full("obj", b"0123456789abcdef").await.unwrap();
        let object = ufs.head("obj").await.unwrap();

        let cfg = ReadPathConfig {
            part_size: 4,
            max_cached_parts: 16,
            prefetch_parts: 0,
            disk_capacity_bytes: 16 * 1024 * 1024,
            cache_dir: Some(cache_dir.path().to_path_buf()),
        };
        {
            let path = UfsReadPath::open(ufs.clone(), cfg.clone()).await.unwrap();
            assert_eq!(path.read("obj", &object, 0, 8).await.unwrap(), b"01234567");
            path.close_cache().await.unwrap();
        }

        // New process-equivalent: reopen cache against same SSD dir; delete UFS
        // bytes so a miss would fail — hit must come from foyer SSD tier.
        std::fs::remove_file(ufs_dir.path().join("obj")).unwrap();
        let path = UfsReadPath::open(ufs, cfg).await.unwrap();
        assert_eq!(path.read("obj", &object, 0, 8).await.unwrap(), b"01234567");
        assert_eq!(path.stats().backend_fetches, 0);
        assert!(path.stats().cache_hits >= 1);
    }
}
