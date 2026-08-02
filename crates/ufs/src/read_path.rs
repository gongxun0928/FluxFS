//! Bounded, version-aware UFS range cache with parallel misses and single-flight.

use crate::Ufs;
use fluxfs_types::{FluxError, Result, UfsObject};
use futures::future::try_join_all;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

#[derive(Debug, Clone)]
pub struct ReadPathConfig {
    /// Cache/fetch granularity. The MVP default limits amplification while still
    /// allowing four requests to cover one 4 MiB FluxFS chunk in parallel.
    pub part_size: u64,
    pub max_cached_parts: usize,
    /// Number of parts scheduled after the requested range.
    pub prefetch_parts: usize,
}

impl Default for ReadPathConfig {
    fn default() -> Self {
        Self {
            part_size: 1024 * 1024,
            max_cached_parts: 256,
            prefetch_parts: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadPathStats {
    pub backend_fetches: u64,
    pub cache_hits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartKey {
    path: String,
    version: String,
    index: u64,
}

type PartResult = Result<Arc<Vec<u8>>>;
type PartCell = Arc<OnceCell<PartResult>>;

#[derive(Default)]
struct CacheState {
    entries: HashMap<PartKey, PartCell>,
    insertion_order: VecDeque<PartKey>,
}

/// Read policy layered above OpenDAL.
///
/// Required parts are fetched concurrently. Concurrent readers of one part
/// share the same future, and successful parts remain in a bounded FIFO cache.
pub struct UfsReadPath {
    ufs: Ufs,
    config: ReadPathConfig,
    cache: Mutex<CacheState>,
    backend_fetches: AtomicU64,
    cache_hits: AtomicU64,
}

impl UfsReadPath {
    pub fn new(ufs: Ufs, config: ReadPathConfig) -> Result<Arc<Self>> {
        if config.part_size == 0 {
            return Err(FluxError::InvalidArg("UFS part_size must be > 0".into()));
        }
        if config.max_cached_parts == 0 {
            return Err(FluxError::InvalidArg(
                "UFS max_cached_parts must be > 0".into(),
            ));
        }
        Ok(Arc::new(Self {
            ufs,
            config,
            cache: Mutex::new(CacheState::default()),
            backend_fetches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }))
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
        let key = PartKey {
            path: rel.clone(),
            version,
            index,
        };
        let (cell, existed) = {
            let mut cache = self.cache.lock().await;
            if let Some(cell) = cache.entries.get(&key) {
                (Arc::clone(cell), true)
            } else {
                while cache.entries.len() >= self.config.max_cached_parts {
                    let Some(oldest) = cache.insertion_order.pop_front() else {
                        break;
                    };
                    cache.entries.remove(&oldest);
                }
                let cell = Arc::new(OnceCell::new());
                cache.entries.insert(key.clone(), Arc::clone(&cell));
                cache.insertion_order.push_back(key.clone());
                (cell, false)
            }
        };
        if existed {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        }

        let result = cell
            .get_or_init(|| async {
                self.backend_fetches.fetch_add(1, Ordering::Relaxed);
                let start = index * self.config.part_size;
                let expected = self.config.part_size.min(object.size - start);
                let data = self
                    .ufs
                    .read_range_pinned(&rel, start, expected, object.etag.as_deref())
                    .await?;
                if data.len() as u64 != expected {
                    return Err(FluxError::Ufs(format!(
                        "short UFS range read: path={rel} offset={start} expected={expected} actual={}",
                        data.len()
                    )));
                }
                Ok(Arc::new(data))
            })
            .await
            .clone();

        if result.is_err() {
            let mut cache = self.cache.lock().await;
            if cache
                .entries
                .get(&key)
                .is_some_and(|cached| Arc::ptr_eq(cached, &cell))
            {
                cache.entries.remove(&key);
            }
        }
        result
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
        }
    }

    #[tokio::test]
    async fn parallel_parts_are_cached() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        ufs.write_full("obj", b"0123456789abcdef").await.unwrap();
        let object = ufs.head("obj").await.unwrap();
        let path = UfsReadPath::new(ufs, config(0)).unwrap();

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

    #[tokio::test]
    async fn concurrent_readers_share_one_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        ufs.write_full("obj", b"01234567").await.unwrap();
        let object = ufs.head("obj").await.unwrap();
        let path = UfsReadPath::new(ufs, config(0)).unwrap();

        let reads = (0..16).map(|_| {
            let path = Arc::clone(&path);
            let object = object.clone();
            async move { path.read("obj", &object, 0, 4).await }
        });
        let results = try_join_all(reads).await.unwrap();
        assert!(results.iter().all(|data| data == b"0123"));
        assert_eq!(path.stats().backend_fetches, 1);
    }

    #[tokio::test]
    async fn short_range_is_rejected_and_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        ufs.write_full("obj", b"abc").await.unwrap();
        let mut object = ufs.head("obj").await.unwrap();
        object.size = 4;
        let path = UfsReadPath::new(ufs, config(0)).unwrap();

        assert!(path.read("obj", &object, 0, 4).await.is_err());
        assert!(path.read("obj", &object, 0, 4).await.is_err());
        assert_eq!(path.stats().backend_fetches, 2);
    }
}
