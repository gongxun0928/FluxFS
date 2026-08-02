//! Append-only chunk segments + durable location index (B7 / #28).
//!
//! Layout:
//! - `segments/seg-NNNNNN.dat`: framed records
//!   `[magic:u32 LE][len:u32 LE][chunk_id:32][payload]`
//!   Integrity is content-addressed: `ChunkId::from_bytes(payload)` must equal
//!   the stored id (blake3). There is no separate CRC32 field.
//! - `index/`: heed map `ChunkId → {seg, offset, len}`
//!
//! Durability: after appending a record, the segment file **and** the
//! `segments/` directory are fsync'd before the index commit. Readers share an
//! [`RwLock`] with writers/compaction so old segment files are never unlinked
//! while a get/list still holds a read guard.

use fluxfs_types::{ChunkId, ChunkPage, FluxError, Result};
use heed::types::{Bytes, SerdeJson};
use heed::{Database, Env, EnvOpenOptions};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

const RECORD_MAGIC: u32 = 0x4658_4b31; // "FXK1"
const INDEX_DB: &str = "chunk_index";
const META_DB: &str = "chunk_meta";
const KEY_ACTIVE_SEG: &str = "active_segment";
const KEY_NEXT_SEG: &str = "next_segment";
const DEFAULT_SEGMENT_ROLL_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct IndexEntry {
    segment: u64,
    offset: u64,
    len: u32,
}

type IndexDb = Database<Bytes, SerdeJson<IndexEntry>>;
type MetaDb = Database<heed::types::Str, Bytes>;

/// Durable pack-backed chunk store used by [`crate::DiskChunkStore`].
pub struct PackStore {
    root: PathBuf,
    env: Env,
    index: IndexDb,
    meta: MetaDb,
    /// Shared by get/list (read) and put/delete/compact (write). Holding a write
    /// guard covers the entire compact rewrite + old-segment unlink window.
    lock: RwLock<()>,
    segment_roll_bytes: u64,
}

impl PackStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join("segments")).map_err(|e| FluxError::Io(e.to_string()))?;
        fs::create_dir_all(root.join("index")).map_err(|e| FluxError::Io(e.to_string()))?;
        // Persist directory entries created above before we rely on them.
        sync_dir(&root)?;
        sync_dir(&root.join("segments"))?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(512 * 1024 * 1024)
                .max_dbs(8)
                .open(root.join("index"))
                .map_err(|e| FluxError::Io(e.to_string()))?
        };
        let mut wtxn = env.write_txn().map_err(|e| FluxError::Io(e.to_string()))?;
        let index: IndexDb = env
            .create_database(&mut wtxn, Some(INDEX_DB))
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let meta: MetaDb = env
            .create_database(&mut wtxn, Some(META_DB))
            .map_err(|e| FluxError::Io(e.to_string()))?;
        if meta
            .get(&wtxn, KEY_NEXT_SEG)
            .map_err(|e| FluxError::Io(e.to_string()))?
            .is_none()
        {
            meta.put(&mut wtxn, KEY_NEXT_SEG, &1u64.to_be_bytes())
                .map_err(|e| FluxError::Io(e.to_string()))?;
            meta.put(&mut wtxn, KEY_ACTIVE_SEG, &1u64.to_be_bytes())
                .map_err(|e| FluxError::Io(e.to_string()))?;
        }
        wtxn.commit().map_err(|e| FluxError::Io(e.to_string()))?;

        let store = Self {
            root,
            env,
            index,
            meta,
            lock: RwLock::new(()),
            segment_roll_bytes: DEFAULT_SEGMENT_ROLL_BYTES,
        };
        store.migrate_legacy_objects()?;
        Ok(store)
    }

    fn segment_path(&self, segment: u64) -> PathBuf {
        self.root
            .join("segments")
            .join(format!("seg-{segment:06}.dat"))
    }

    fn segments_dir(&self) -> PathBuf {
        self.root.join("segments")
    }

    fn read_u64_meta(&self, txn: &heed::RoTxn, key: &str) -> Result<u64> {
        let bytes = self
            .meta
            .get(txn, key)
            .map_err(|e| FluxError::Io(e.to_string()))?
            .ok_or_else(|| FluxError::Io(format!("missing pack meta {key}")))?;
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| FluxError::Io(format!("bad pack meta {key}")))?;
        Ok(u64::from_be_bytes(arr))
    }

    fn put_u64_meta(&self, txn: &mut heed::RwTxn, key: &str, value: u64) -> Result<()> {
        self.meta
            .put(txn, key, &value.to_be_bytes())
            .map_err(|e| FluxError::Io(e.to_string()))
    }

    /// Import leftover MVP `objects/` trees into the pack index once.
    fn migrate_legacy_objects(&self) -> Result<()> {
        let objects = self.root.join("objects");
        if !objects.exists() {
            return Ok(());
        }
        let mut imported = 0usize;
        for first in fs::read_dir(&objects).map_err(|e| FluxError::Io(e.to_string()))? {
            let first = first.map_err(|e| FluxError::Io(e.to_string()))?;
            if !first
                .file_type()
                .map_err(|e| FluxError::Io(e.to_string()))?
                .is_dir()
            {
                continue;
            }
            for second in fs::read_dir(first.path()).map_err(|e| FluxError::Io(e.to_string()))? {
                let second = second.map_err(|e| FluxError::Io(e.to_string()))?;
                if !second
                    .file_type()
                    .map_err(|e| FluxError::Io(e.to_string()))?
                    .is_dir()
                {
                    continue;
                }
                for object in
                    fs::read_dir(second.path()).map_err(|e| FluxError::Io(e.to_string()))?
                {
                    let object = object.map_err(|e| FluxError::Io(e.to_string()))?;
                    if !object
                        .file_type()
                        .map_err(|e| FluxError::Io(e.to_string()))?
                        .is_file()
                    {
                        continue;
                    }
                    let data = fs::read(object.path()).map_err(|e| FluxError::Io(e.to_string()))?;
                    let id = ChunkId::from_bytes(&data);
                    let hex = id.to_hex();
                    let expected = objects.join(&hex[..2]).join(&hex[2..4]).join(&hex);
                    if object.path() != expected {
                        continue;
                    }
                    if self.contains(&id)? {
                        let _ = fs::remove_file(object.path());
                        continue;
                    }
                    self.put(&data)?;
                    let _ = fs::remove_file(object.path());
                    imported += 1;
                }
            }
        }
        if imported > 0 {
            tracing::info!(imported, "migrated legacy chunk objects into pack segments");
        }
        let _ = fs::remove_dir_all(&objects);
        Ok(())
    }

    pub fn put(&self, data: &[u8]) -> Result<ChunkId> {
        let id = ChunkId::from_bytes(data);
        let _guard = self
            .lock
            .write()
            .map_err(|_| FluxError::Io("pack write lock poisoned".into()))?;
        self.put_locked(data, id)
    }

    fn put_locked(&self, data: &[u8], id: ChunkId) -> Result<ChunkId> {
        if let Some(existing) = self.get_if_valid_unlocked(&id)? {
            if existing == data {
                return Ok(id);
            }
        }

        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let mut active = self.read_u64_meta(&rtxn, KEY_ACTIVE_SEG)?;
        let mut next_seg = self.read_u64_meta(&rtxn, KEY_NEXT_SEG)?;
        drop(rtxn);

        let mut seg_path = self.segment_path(active);
        let created_new = !seg_path.exists();
        if created_new {
            File::create(&seg_path).map_err(|e| FluxError::Io(e.to_string()))?;
            sync_dir(&self.segments_dir())?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&seg_path)
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let offset = file
            .seek(SeekFrom::End(0))
            .map_err(|e| FluxError::Io(e.to_string()))?;
        if offset >= self.segment_roll_bytes {
            active = next_seg;
            next_seg = next_seg.saturating_add(1);
            seg_path = self.segment_path(active);
            File::create(&seg_path).map_err(|e| FluxError::Io(e.to_string()))?;
            sync_dir(&self.segments_dir())?;
            file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(&seg_path)
                .map_err(|e| FluxError::Io(e.to_string()))?;
        }
        let offset = file
            .seek(SeekFrom::End(0))
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let len = u32::try_from(data.len())
            .map_err(|_| FluxError::InvalidArg("chunk larger than u32::MAX".into()))?;

        file.write_all(&RECORD_MAGIC.to_le_bytes())
            .map_err(|e| FluxError::Io(e.to_string()))?;
        file.write_all(&len.to_le_bytes())
            .map_err(|e| FluxError::Io(e.to_string()))?;
        file.write_all(id.as_bytes())
            .map_err(|e| FluxError::Io(e.to_string()))?;
        file.write_all(data)
            .map_err(|e| FluxError::Io(e.to_string()))?;
        file.sync_all().map_err(|e| FluxError::Io(e.to_string()))?;
        // Persist the directory entry for a newly created (or rolled) segment
        // before the index may point at it.
        sync_dir(&self.segments_dir())?;

        let entry = IndexEntry {
            segment: active,
            offset,
            len,
        };
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Io(e.to_string()))?;
        self.index
            .put(&mut wtxn, id.as_bytes().as_slice(), &entry)
            .map_err(|e| FluxError::Io(e.to_string()))?;
        self.put_u64_meta(&mut wtxn, KEY_ACTIVE_SEG, active)?;
        self.put_u64_meta(&mut wtxn, KEY_NEXT_SEG, next_seg)?;
        wtxn.commit().map_err(|e| FluxError::Io(e.to_string()))?;
        Ok(id)
    }

    pub fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
        let _guard = self
            .lock
            .read()
            .map_err(|_| FluxError::Io("pack read lock poisoned".into()))?;
        self.get_if_valid_unlocked(id)?.ok_or(FluxError::NotFound)
    }

    fn get_if_valid_unlocked(&self, id: &ChunkId) -> Result<Option<Vec<u8>>> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let Some(entry) = self
            .index
            .get(&rtxn, id.as_bytes().as_slice())
            .map_err(|e| FluxError::Io(e.to_string()))?
        else {
            return Ok(None);
        };
        drop(rtxn);
        match self.read_record(&entry) {
            Ok(data) if ChunkId::from_bytes(&data) == *id => Ok(Some(data)),
            Ok(_) | Err(_) => Ok(None),
        }
    }

    fn read_record(&self, entry: &IndexEntry) -> Result<Vec<u8>> {
        let mut file = File::open(self.segment_path(entry.segment))
            .map_err(|e| FluxError::Io(e.to_string()))?;
        file.seek(SeekFrom::Start(entry.offset))
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let mut header = [0u8; 4 + 4 + 32];
        file.read_exact(&mut header)
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let mut stored_id = [0u8; 32];
        stored_id.copy_from_slice(&header[8..40]);
        if magic != RECORD_MAGIC || len != entry.len {
            return Err(FluxError::Io("pack record header mismatch".into()));
        }
        let mut data = vec![0u8; len as usize];
        file.read_exact(&mut data)
            .map_err(|e| FluxError::Io(e.to_string()))?;
        if ChunkId::from_raw(stored_id) != ChunkId::from_bytes(&data) {
            return Err(FluxError::Io("pack record content-hash mismatch".into()));
        }
        Ok(data)
    }

    pub fn contains(&self, id: &ChunkId) -> Result<bool> {
        let _guard = self
            .lock
            .read()
            .map_err(|_| FluxError::Io("pack read lock poisoned".into()))?;
        Ok(self.get_if_valid_unlocked(id)?.is_some())
    }

    pub fn delete(&self, id: &ChunkId) -> Result<()> {
        let _guard = self
            .lock
            .write()
            .map_err(|_| FluxError::Io("pack write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let _ = self
            .index
            .delete(&mut wtxn, id.as_bytes().as_slice())
            .map_err(|e| FluxError::Io(e.to_string()))?;
        wtxn.commit().map_err(|e| FluxError::Io(e.to_string()))?;
        Ok(())
    }

    pub fn list_chunks(&self) -> Result<Vec<ChunkId>> {
        let _guard = self
            .lock
            .read()
            .map_err(|_| FluxError::Io("pack read lock poisoned".into()))?;
        self.list_chunks_unlocked()
    }

    fn list_chunks_unlocked(&self) -> Result<Vec<ChunkId>> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let mut ids = Vec::new();
        let iter = self
            .index
            .iter(&rtxn)
            .map_err(|e| FluxError::Io(e.to_string()))?;
        for item in iter {
            let (key, entry) = item.map_err(|e| FluxError::Io(e.to_string()))?;
            let id = ChunkId::try_from(key)?;
            if self.read_record(&entry).is_ok() {
                ids.push(id);
            }
        }
        ids.sort_by_key(ChunkId::to_hex);
        ids.dedup();
        Ok(ids)
    }

    pub fn list_chunks_page(&self, cursor: Option<ChunkId>, limit: usize) -> Result<ChunkPage> {
        if limit == 0 {
            return Err(FluxError::InvalidArg(
                "chunk inventory page limit must be non-zero".into(),
            ));
        }
        let chunks = self.list_chunks()?;
        let mut page = chunks
            .into_iter()
            .filter(|chunk| cursor.is_none_or(|cursor| *chunk > cursor))
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        page.truncate(limit);
        let next_cursor = has_more.then(|| *page.last().expect("non-empty page"));
        Ok(ChunkPage {
            chunks: page,
            next_cursor,
        })
    }

    pub fn segment_file_count(&self) -> Result<usize> {
        let dir = self.segments_dir();
        if !dir.exists() {
            return Ok(0);
        }
        let mut n = 0usize;
        for entry in fs::read_dir(&dir).map_err(|e| FluxError::Io(e.to_string()))? {
            let entry = entry.map_err(|e| FluxError::Io(e.to_string()))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("seg-") && name.ends_with(".dat") {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Rewrite live chunks into a fresh segment and drop unreferenced segment files.
    ///
    /// Holds the write lock for the whole rewrite + unlink window so concurrent
    /// readers never observe a deleted segment path.
    pub fn compact(&self) -> Result<CompactReport> {
        let _guard = self
            .lock
            .write()
            .map_err(|_| FluxError::Io("pack write lock poisoned".into()))?;

        let live = self.list_chunks_unlocked()?;
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Io(e.to_string()))?;
        let mut next_seg = self.read_u64_meta(&rtxn, KEY_NEXT_SEG)?;
        drop(rtxn);

        let mut target = next_seg;
        let path = loop {
            let candidate = self.segment_path(target);
            next_seg = target.saturating_add(1);
            if !candidate.exists() {
                break candidate;
            }
            target = next_seg;
        };
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|e| FluxError::Io(e.to_string()))?;
        sync_dir(&self.segments_dir())?;

        let mut new_entries = Vec::with_capacity(live.len());
        for id in &live {
            let data = self.get_if_valid_unlocked(id)?.ok_or(FluxError::NotFound)?;
            let offset = file
                .stream_position()
                .map_err(|e| FluxError::Io(e.to_string()))?;
            let len = u32::try_from(data.len())
                .map_err(|_| FluxError::InvalidArg("chunk larger than u32::MAX".into()))?;
            file.write_all(&RECORD_MAGIC.to_le_bytes())
                .map_err(|e| FluxError::Io(e.to_string()))?;
            file.write_all(&len.to_le_bytes())
                .map_err(|e| FluxError::Io(e.to_string()))?;
            file.write_all(id.as_bytes())
                .map_err(|e| FluxError::Io(e.to_string()))?;
            file.write_all(&data)
                .map_err(|e| FluxError::Io(e.to_string()))?;
            new_entries.push((
                *id,
                IndexEntry {
                    segment: target,
                    offset,
                    len,
                },
            ));
        }
        file.sync_all().map_err(|e| FluxError::Io(e.to_string()))?;
        sync_dir(&self.segments_dir())?;

        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Io(e.to_string()))?;
        {
            let keys: Vec<Vec<u8>> = self
                .index
                .iter(&wtxn)
                .map_err(|e| FluxError::Io(e.to_string()))?
                .map(|i| i.map(|(k, _)| k.to_vec()))
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| FluxError::Io(e.to_string()))?;
            for k in keys {
                self.index
                    .delete(&mut wtxn, &k)
                    .map_err(|e| FluxError::Io(e.to_string()))?;
            }
        }
        for (id, entry) in &new_entries {
            self.index
                .put(&mut wtxn, id.as_bytes().as_slice(), entry)
                .map_err(|e| FluxError::Io(e.to_string()))?;
        }
        self.put_u64_meta(&mut wtxn, KEY_ACTIVE_SEG, target)?;
        self.put_u64_meta(&mut wtxn, KEY_NEXT_SEG, next_seg)?;
        wtxn.commit().map_err(|e| FluxError::Io(e.to_string()))?;

        // Unlink obsolete segments only while the write lock is still held, so
        // no reader can still be opening those paths.
        let mut removed_segments = 0usize;
        let keep = format!("seg-{target:06}.dat");
        if let Ok(entries) = fs::read_dir(self.segments_dir()) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name == keep {
                    continue;
                }
                if name.starts_with("seg-") && name.ends_with(".dat") {
                    let _ = fs::remove_file(entry.path());
                    removed_segments += 1;
                }
            }
        }
        sync_dir(&self.segments_dir())?;

        Ok(CompactReport {
            live_chunks: new_entries.len(),
            removed_segments,
        })
    }
}

fn sync_dir(path: &Path) -> Result<()> {
    let file = File::open(path).map_err(|e| FluxError::Io(e.to_string()))?;
    file.sync_all().map_err(|e| FluxError::Io(e.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactReport {
    pub live_chunks: usize,
    pub removed_segments: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn pack_put_get_delete_and_compact() {
        let dir = tempfile::tempdir().unwrap();
        let store = PackStore::open(dir.path()).unwrap();
        let a = store.put(b"alpha-chunk").unwrap();
        let b = store.put(b"beta-chunk").unwrap();
        assert_eq!(store.get(&a).unwrap(), b"alpha-chunk");
        assert_eq!(store.get(&b).unwrap(), b"beta-chunk");
        store.delete(&a).unwrap();
        assert!(!store.contains(&a).unwrap());
        assert!(matches!(store.get(&a), Err(FluxError::NotFound)));
        let report = store.compact().unwrap();
        assert_eq!(report.live_chunks, 1);
        assert!(report.removed_segments >= 1);
        assert_eq!(store.get(&b).unwrap(), b"beta-chunk");
    }

    #[test]
    fn put_survives_reopen_after_dir_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let id = {
            let store = PackStore::open(dir.path()).unwrap();
            store.put(b"durable-after-reopen").unwrap()
        };
        let store = PackStore::open(dir.path()).unwrap();
        assert_eq!(store.get(&id).unwrap(), b"durable-after-reopen");
        assert!(dir.path().join("segments").join("seg-000001.dat").exists());
    }

    #[test]
    fn concurrent_get_list_with_compact_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(PackStore::open(dir.path()).unwrap());
        let mut ids = Vec::new();
        for i in 0..32u8 {
            ids.push(store.put(&[i; 64]).unwrap());
        }
        let drop_id = ids[0];
        store.delete(&drop_id).unwrap();

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let store = Arc::clone(&store);
                let ids = ids.clone();
                thread::spawn(move || {
                    for _ in 0..40 {
                        let _ = store.list_chunks();
                        for id in ids.iter().skip(1).take(8) {
                            let _ = store.get(id);
                        }
                    }
                })
            })
            .collect();

        for _ in 0..8 {
            let _ = store.compact();
            let _ = store.put(b"extra-during-compact");
        }
        for t in readers {
            t.join().unwrap();
        }
        assert!(store.get(&ids[1]).is_ok());
        assert!(matches!(store.get(&drop_id), Err(FluxError::NotFound)));
    }

    #[test]
    fn migrates_legacy_object_tree() {
        use std::fs;
        let dir = tempfile::tempdir().unwrap();
        for payload in [b"legacy-one".as_slice(), b"legacy-two".as_slice()] {
            let id = ChunkId::from_bytes(payload);
            let hex = id.to_hex();
            let path = dir
                .path()
                .join("objects")
                .join(&hex[..2])
                .join(&hex[2..4])
                .join(&hex);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, payload).unwrap();
        }
        let store = PackStore::open(dir.path()).unwrap();
        let id = ChunkId::from_bytes(b"legacy-one");
        assert_eq!(store.get(&id).unwrap(), b"legacy-one");
        assert!(!dir.path().join("objects").exists());
    }

    #[test]
    fn list_page_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let store = PackStore::open(dir.path()).unwrap();
        for i in 0..8u8 {
            store.put(&[i; 16]).unwrap();
        }
        let page = store.list_chunks_page(None, 3).unwrap();
        assert_eq!(page.chunks.len(), 3);
        assert!(page.next_cursor.is_some());
        let page2 = store.list_chunks_page(page.next_cursor, 3).unwrap();
        assert_eq!(page2.chunks.len(), 3);
        assert!(page.chunks.iter().all(|c| !page2.chunks.contains(c)));
    }
}
