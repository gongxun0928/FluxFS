//! Internal FluxFS client surface for alpha (CLI + FUSE + tests).

use fluxfs_chunk::ChunkStore;
use fluxfs_meta::MetaStore;
use fluxfs_types::{
    BackingMode, ChunkId, DataGen, DataState, Extent, FileType, FlushId, FlushIntent, FluxError,
    Inode, InodeId, LocalityFields, LocalityLabel, Manifest, OpState, Origin, RequestOpId, Result,
    UfsObject, UfsVersion, WriteTicketId, CHUNK_SIZE, DIRTY_WRITE_CAP_BYTES, ROOT_INODE,
};
use fluxfs_ufs::{ReadPathConfig, ReadPathStats, Ufs, UfsEntryMode, UfsProbe, UfsReadPath};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::runtime::{Handle, Runtime};

struct UfsRuntime {
    ufs: Ufs,
    reads: std::sync::Arc<UfsReadPath>,
    handle: Option<Handle>,
    /// Owned runtime only when constructed outside an existing Tokio context.
    rt: Option<Runtime>,
}

impl UfsRuntime {
    fn new(ufs: Ufs) -> Result<Self> {
        let reads = UfsReadPath::new(ufs.clone(), ReadPathConfig::default())?;
        let (handle, rt) = match Handle::try_current() {
            Ok(handle) => (Some(handle), None),
            Err(_) => (
                None,
                Some(Runtime::new().map_err(|e| FluxError::Ufs(e.to_string()))?),
            ),
        };
        Ok(Self {
            ufs,
            reads,
            handle,
            rt,
        })
    }

    fn block_on<T>(&self, fut: impl Future<Output = T>) -> T {
        if Handle::try_current().is_ok() {
            let handle = self.handle.as_ref().expect("runtime handle captured");
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else if let Some(handle) = &self.handle {
            handle.block_on(fut)
        } else if let Some(rt) = &self.rt {
            rt.block_on(fut)
        } else {
            unreachable!("UFS executor missing")
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
    /// Last content address considered by bounded background GC.
    gc_cursor: Mutex<Option<ChunkId>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushRecoveryReport {
    pub completed: usize,
    pub conflicts: usize,
    pub pending: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrphanGcReport {
    pub removed_manifests: usize,
    pub removed_chunks: usize,
}

impl<M: MetaStore, C: ChunkStore> FluxClient<M, C> {
    pub fn new(meta: M, chunks: C) -> Self {
        Self {
            meta,
            chunks,
            io_lock: Mutex::new(()),
            ufs: None,
            ufs_paths: Mutex::new(HashMap::from([(ROOT_INODE, String::new())])),
            gc_cursor: Mutex::new(None),
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

    pub fn ufs_read_stats(&self) -> Option<ReadPathStats> {
        self.ufs.as_ref().map(|ufs| ufs.reads.stats())
    }

    pub fn root(&self) -> InodeId {
        self.meta.root()
    }

    fn reject_ufs_mutation(&self) -> Result<()> {
        if self.ufs.is_some() {
            Err(FluxError::ReadOnly)
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

    /// Flush one Dirty UFS-backed inode through the durable intent protocol.
    /// Ephemeral and already-clean files are already durable at their declared tier.
    pub fn flush_inode(&self, ino: InodeId) -> Result<Inode> {
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| FluxError::Io("io lock poisoned".into()))?;
        let inode = self.meta.get_inode(ino)?;
        if inode.file_type != FileType::Regular {
            return Err(FluxError::IsDirectory);
        }
        let Some(fields) = inode.locality_fields.as_ref() else {
            return Err(FluxError::Meta(
                "flush inode missing locality fields".into(),
            ));
        };
        if fields.backing_mode == BackingMode::Ephemeral || fields.data_state == DataState::UfsClean
        {
            return Ok(inode);
        }
        if fields.data_state == DataState::DirtyConflict {
            return Err(FluxError::DirtyConflict);
        }
        if let Some(intent) = inode.flush_intent.clone() {
            return self.complete_flush_intent(inode.id, &intent);
        }
        if fields.data_state != DataState::Dirty {
            return Err(FluxError::Busy);
        }

        let manifest_id = inode
            .manifest_id
            .ok_or_else(|| FluxError::Meta("Dirty inode missing manifest".into()))?;
        let manifest = self.meta.get_manifest(manifest_id)?;
        if manifest.gen != inode.head_gen || manifest.size != inode.size {
            return Err(FluxError::Meta(
                "Dirty inode head does not match manifest snapshot".into(),
            ));
        }
        let bytes = self.read_inode_range(&inode, 0, inode.size)?;
        let target_digest = ChunkId::from_bytes(&bytes);
        let target = inode
            .ufs
            .as_ref()
            .ok_or_else(|| FluxError::Meta("Dirty inode missing UFS target".into()))?;
        let expected_ufs_version = match target.etag.clone() {
            Some(etag) => Some(UfsVersion(etag)),
            None if fields.origin == Origin::FluxCreated => None,
            None => {
                return Err(FluxError::Capability(
                    "safe overwrite of imported data requires a UFS ETag".into(),
                ));
            }
        };
        let intent = FlushIntent {
            flush_id: FlushId::random(),
            snapshot_gen: inode.head_gen,
            snapshot_manifest_root: manifest.root_digest(),
            expected_ufs_version,
            target_digest,
        };
        self.meta.begin_flush(inode.generation, inode.id, &intent)?;
        self.complete_flush_intent(inode.id, &intent)
    }

    /// Reconcile durable intents after process restart. Transient failures stay
    /// pending for the next pass; detected external conflicts are durably marked.
    pub fn reconcile_flushes(&self) -> Result<FlushRecoveryReport> {
        let mut report = FlushRecoveryReport::default();
        for (inode, intent) in self.meta.list_flush_intents()? {
            match self.complete_flush_intent(inode, &intent) {
                Ok(_) => report.completed += 1,
                Err(FluxError::DirtyConflict) => report.conflicts += 1,
                Err(_) => report.pending += 1,
            }
        }
        Ok(report)
    }

    /// Resume a GC interrupted by process failure, without starting a new pass.
    ///
    /// Prefer [`Self::release_interrupted_gc_lease`] on the mount critical path:
    /// completing a full physical sweep here is stop-the-world and can stall
    /// startup. Kept for explicit/admin GC tooling and tests.
    pub fn resume_orphan_gc(&self) -> Result<Option<OrphanGcReport>> {
        let Some(plan) = self.meta.current_gc_plan()? else {
            return Ok(None);
        };
        self.execute_gc_plan(plan).map(Some)
    }

    /// Drop a leftover stop-the-world GC lease without deleting chunks.
    ///
    /// Used so mount can serve traffic instead of blocking on an incomplete
    /// quiesced sweep. Orphan chunks remain until reservation/tombstone GC.
    pub fn release_interrupted_gc_lease(&self) -> Result<bool> {
        let Some(plan) = self.meta.current_gc_plan()? else {
            return Ok(false);
        };
        self.meta.finish_gc(plan.lease_id)?;
        Ok(true)
    }

    /// Concurrent mark/tombstone/delete pass. Durable pre-Put reservations and
    /// tombstones close the writer-vs-delete race without a global Meta lease.
    pub fn run_orphan_gc(&self) -> Result<OrphanGcReport> {
        self.run_concurrent_gc_pass(256)
    }

    pub fn run_concurrent_gc_pass(&self, batch_size: usize) -> Result<OrphanGcReport> {
        if batch_size == 0 {
            return Err(FluxError::InvalidArg(
                "GC batch size must be non-zero".into(),
            ));
        }
        let mut report = OrphanGcReport::default();
        let pending = self.meta.list_gc_tombstones()?;
        if !pending.is_empty() {
            let batch = &pending[..pending.len().min(batch_size)];
            for chunk in batch {
                self.chunks.delete(chunk)?;
                report.removed_chunks += 1;
            }
            self.meta.finalize_gc_tombstones(batch)?;
            return Ok(report);
        }
        let mut inventory = self.chunks.list_chunks()?;
        inventory.sort_by_key(ChunkId::to_hex);
        let mut cursor = self
            .gc_cursor
            .lock()
            .map_err(|_| FluxError::Io("GC cursor lock poisoned".into()))?;
        let start = match *cursor {
            Some(last) => inventory
                .iter()
                .position(|chunk| *chunk > last)
                .unwrap_or(0),
            None => 0,
        };
        let end = start.saturating_add(batch_size).min(inventory.len());
        let candidates = &inventory[start..end];
        *cursor = if end == inventory.len() {
            None
        } else {
            candidates.last().copied()
        };
        drop(cursor);

        let batch = self.meta.tombstone_gc_batch(candidates)?;
        report.removed_manifests += batch.removed_manifests;
        for chunk in &batch.tombstoned_chunks {
            self.chunks.delete(chunk)?;
            report.removed_chunks += 1;
        }
        self.meta.finalize_gc_tombstones(&batch.tombstoned_chunks)?;
        Ok(report)
    }

    fn execute_gc_plan(&self, plan: fluxfs_types::GcPlan) -> Result<OrphanGcReport> {
        let live = plan.live_chunks.iter().copied().collect::<BTreeSet<_>>();
        let mut removed_chunks = 0;
        for chunk in self.chunks.list_chunks()? {
            if !live.contains(&chunk) {
                self.chunks.delete(&chunk)?;
                removed_chunks += 1;
            }
        }
        self.meta.finish_gc(plan.lease_id)?;
        Ok(OrphanGcReport {
            removed_manifests: plan.removed_manifests,
            removed_chunks,
        })
    }

    fn complete_flush_intent(&self, ino: InodeId, intent: &FlushIntent) -> Result<Inode> {
        let ufs = self
            .ufs
            .as_ref()
            .ok_or_else(|| FluxError::Capability("flush requires a UFS mount".into()))?;
        let inode = self.meta.get_inode(ino)?;
        if inode.flush_intent.as_ref().map(|i| i.flush_id) != Some(intent.flush_id) {
            return Err(FluxError::Busy);
        }
        let object = inode
            .ufs
            .as_ref()
            .ok_or_else(|| FluxError::Meta("Dirty inode missing UFS target".into()))?;
        let manifest = self.meta.get_manifest(
            inode
                .manifest_id
                .ok_or_else(|| FluxError::Meta("flushing inode missing manifest".into()))?,
        )?;
        if manifest.root_digest() != intent.snapshot_manifest_root {
            return self.record_flush_conflict(
                ino,
                intent.flush_id,
                "flush snapshot manifest changed",
            );
        }

        let published = match ufs.block_on(ufs.ufs.find_verified_publish(
            &object.key,
            inode.size,
            &intent.target_digest,
        ))? {
            Some(published) => published,
            None => {
                let bytes = self.read_inode_range(&inode, 0, inode.size)?;
                if ChunkId::from_bytes(&bytes) != intent.target_digest {
                    return self.record_flush_conflict(
                        ino,
                        intent.flush_id,
                        "flush reconstructed bytes do not match target digest",
                    );
                }
                match ufs.block_on(
                    ufs.ufs.publish_full_verified(
                        &object.key,
                        &bytes,
                        intent
                            .expected_ufs_version
                            .as_ref()
                            .map(|version| version.0.as_str()),
                        &intent.target_digest,
                    ),
                ) {
                    Ok(published) => published,
                    Err(FluxError::DirtyConflict) => {
                        return self.record_flush_conflict(
                            ino,
                            intent.flush_id,
                            "conditional UFS publish detected external version drift",
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        self.commit_flush_retry(ino, intent.flush_id, &published)
    }

    fn commit_flush_retry(
        &self,
        ino: InodeId,
        flush_id: FlushId,
        published: &UfsObject,
    ) -> Result<Inode> {
        for _ in 0..4 {
            let current = self.meta.get_inode(ino)?;
            match self
                .meta
                .commit_flush(current.generation, ino, flush_id, published)
            {
                Err(FluxError::CasFailed { .. }) => continue,
                result => return result,
            }
        }
        Err(FluxError::Busy)
    }

    fn record_flush_conflict(
        &self,
        ino: InodeId,
        flush_id: FlushId,
        message: &str,
    ) -> Result<Inode> {
        for _ in 0..4 {
            let current = self.meta.get_inode(ino)?;
            match self
                .meta
                .fail_flush_conflict(current.generation, ino, flush_id, message)
            {
                Err(FluxError::CasFailed { .. }) => continue,
                Ok(_) => return Err(FluxError::DirtyConflict),
                Err(error) => return Err(error),
            }
        }
        Err(FluxError::Busy)
    }

    /// Assemble full file bytes from the inode's current manifest.
    pub fn read_all(&self, ino: InodeId) -> Result<Vec<u8>> {
        let inode = self.meta.get_inode(ino)?;
        self.read_inode_range(&inode, 0, inode.size)
    }

    pub fn read_at(&self, ino: InodeId, offset: u64, size: u32) -> Result<Vec<u8>> {
        let inode = self.meta.get_inode(ino)?;
        self.read_inode_range(&inode, offset, size as u64)
    }

    /// Durable random write. External files copy up only the touched chunk-aligned
    /// windows; Ephemeral files retain the MVP whole-file rewrite path.
    pub fn write_at(&self, ino: InodeId, offset: u64, data: &[u8]) -> Result<u32> {
        if data.is_empty() {
            return Ok(0);
        }
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

        if matches!(
            inode.locality_fields.as_ref().map(|f| f.backing_mode),
            Some(BackingMode::UfsBacked)
        ) {
            return self.write_ufs_backed(&mut inode, offset, data, end);
        }

        let mut buf = if inode.size == 0 {
            Vec::new()
        } else {
            self.read_inode_range(&inode, 0, inode.size)?
        };
        if (buf.len() as u64) < end {
            buf.resize(end as usize, 0);
        }
        let start = offset as usize;
        buf[start..start + data.len()].copy_from_slice(data);

        let gen = DataGen(inode.head_gen.0.saturating_add(1));
        let (manifest, staged) = self.build_local_manifest(ino, gen, &buf);
        let now = now_ms();
        inode.size = buf.len() as u64;
        inode.head_gen = gen;
        inode.generation = inode.generation.saturating_add(1);
        inode.mtime_ms = now;
        inode.ctime_ms = now;
        self.commit_staged_manifest(
            inode.generation.saturating_sub(1),
            &inode,
            &manifest,
            &staged,
        )?;
        Ok(data.len() as u32)
    }

    /// Copy up each touched 4 MiB window as a durable Local extent, then atomically
    /// switch the inode head. Untouched bytes remain pinned UFS ranges.
    fn write_ufs_backed(
        &self,
        inode: &mut Inode,
        offset: u64,
        data: &[u8],
        end: u64,
    ) -> Result<u32> {
        let expected_generation = inode.generation;
        let old_size = inode.size;
        let new_size = old_size.max(end);
        let gen = DataGen(inode.head_gen.0.saturating_add(1));
        let mut manifest = match inode.manifest_id {
            Some(id) => self.meta.get_manifest(id)?,
            None => Manifest::empty(inode.id, inode.head_gen),
        };
        let mut staged = Vec::new();

        let first_window = offset / CHUNK_SIZE;
        let last_window = (end - 1) / CHUNK_SIZE;
        for window_index in first_window..=last_window {
            let window_start = window_index
                .checked_mul(CHUNK_SIZE)
                .ok_or_else(|| FluxError::InvalidArg("chunk offset overflow".into()))?;
            let window_end = window_start
                .checked_add(CHUNK_SIZE)
                .ok_or_else(|| FluxError::InvalidArg("chunk end overflow".into()))?
                .min(new_size);
            let window_len = window_end - window_start;
            let old_window_end = window_end.min(old_size);
            let mut bytes = if window_start < old_window_end {
                self.read_inode_range(inode, window_start, old_window_end - window_start)?
            } else {
                Vec::new()
            };
            bytes.resize(window_len as usize, 0);

            let overlay_start = offset.max(window_start);
            let overlay_end = end.min(window_end);
            let src_start = (overlay_start - offset) as usize;
            let dst_start = (overlay_start - window_start) as usize;
            let overlay_len = (overlay_end - overlay_start) as usize;
            bytes[dst_start..dst_start + overlay_len]
                .copy_from_slice(&data[src_start..src_start + overlay_len]);

            // ChunkStore::put is the RF=2 durability boundary. Metadata remains
            // unreachable until commit_inode_manifest succeeds below.
            let chunk = ChunkId::from_bytes(&bytes);
            staged.push((chunk, bytes));
            manifest = manifest.replace_range(
                Extent::Local {
                    offset: window_start,
                    len: window_len,
                    chunk,
                },
                gen,
            )?;
        }

        manifest.size = new_size;
        let now = now_ms();
        inode.size = new_size;
        inode.head_gen = gen;
        inode.generation = inode.generation.saturating_add(1);
        inode.locality_fields = Some(LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::Dirty,
            op_state: OpState::None,
            origin: inode
                .locality_fields
                .as_ref()
                .map(|f| f.origin)
                .unwrap_or(Origin::Imported),
        });
        inode.locality = LocalityLabel::derive(
            inode.locality_fields.as_ref().expect("just assigned"),
            inode.head_gen,
            inode.ufs_gen,
        );
        inode.mtime_ms = now;
        inode.ctime_ms = now;
        self.commit_staged_manifest(expected_generation, inode, &manifest, &staged)?;
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
        let gen = DataGen(inode.head_gen.0.saturating_add(1));
        let (manifest, staged) = self.build_local_manifest(ino, gen, &buf);
        inode.head_gen = gen;
        let now = now_ms();
        inode.size = size;
        inode.generation = inode.generation.saturating_add(1);
        inode.mtime_ms = now;
        inode.ctime_ms = now;
        self.commit_staged_manifest(
            inode.generation.saturating_sub(1),
            &inode,
            &manifest,
            &staged,
        )
    }

    /// Split one logical file image into bounded content-addressed RPC/storage chunks.
    /// Build hashes before Put so Meta can durably reserve every content address.
    fn build_local_manifest(
        &self,
        ino: InodeId,
        gen: DataGen,
        data: &[u8],
    ) -> (Manifest, Vec<(ChunkId, Vec<u8>)>) {
        let mut extents = Vec::with_capacity(data.len().div_ceil(CHUNK_SIZE as usize));
        let mut staged = Vec::with_capacity(extents.capacity());
        for (index, bytes) in data.chunks(CHUNK_SIZE as usize).enumerate() {
            let chunk = ChunkId::from_bytes(bytes);
            extents.push(Extent::Local {
                offset: index as u64 * CHUNK_SIZE,
                len: bytes.len() as u64,
                chunk,
            });
            staged.push((chunk, bytes.to_vec()));
        }
        (
            Manifest {
                inode: ino,
                gen,
                size: data.len() as u64,
                extents,
            },
            staged,
        )
    }

    fn commit_staged_manifest(
        &self,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
        staged: &[(ChunkId, Vec<u8>)],
    ) -> Result<Inode> {
        let ticket = WriteTicketId::random();
        let local_chunks = manifest
            .extents
            .iter()
            .filter_map(|extent| match extent {
                Extent::Local { chunk, .. } => Some(*chunk),
                Extent::UfsRange { .. } => None,
            })
            .collect::<Vec<_>>();
        self.meta
            .reserve_chunks(ticket, inode.id, expected_generation, &local_chunks)?;
        let result = (|| {
            for (expected, bytes) in staged {
                let actual = self.chunks.put(bytes)?;
                if actual != *expected {
                    return Err(FluxError::Io("ChunkStore returned wrong content id".into()));
                }
            }
            self.meta.commit_inode_manifest_reserved_with_id(
                RequestOpId::random(),
                ticket,
                expected_generation,
                inode,
                manifest,
            )
        })();
        if result.is_err() {
            let _ = self.meta.abort_chunk_reservation(ticket);
        }
        result
    }

    fn read_inode_range(&self, inode: &Inode, offset: u64, len: u64) -> Result<Vec<u8>> {
        if inode.file_type != FileType::Regular {
            return Err(FluxError::IsDirectory);
        }
        if len == 0 || offset >= inode.size || inode.manifest_id.is_none() {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(len).min(inode.size);
        let output_len = usize::try_from(end - offset)
            .map_err(|_| FluxError::Capability("read range exceeds address space".into()))?;
        let mut output = vec![0u8; output_len];
        let manifest = self.meta.get_manifest(inode.manifest_id.unwrap())?;

        for extent in &manifest.extents {
            let (extent_offset, extent_len) = match extent {
                Extent::Local { offset, len, .. } | Extent::UfsRange { offset, len, .. } => {
                    (*offset, *len)
                }
            };
            let extent_end = extent_offset.saturating_add(extent_len);
            let overlap_start = offset.max(extent_offset);
            let overlap_end = end.min(extent_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let source_start = overlap_start - extent_offset;
            let overlap_len = overlap_end - overlap_start;
            let data = match extent {
                Extent::Local { len, chunk, .. } => {
                    let chunk_data = self.chunks.get(chunk)?;
                    if chunk_data.len() as u64 != *len {
                        return Err(FluxError::Io(format!(
                            "chunk len mismatch: meta={len} actual={}",
                            chunk_data.len()
                        )));
                    }
                    let start = source_start as usize;
                    let end = start + overlap_len as usize;
                    chunk_data[start..end].to_vec()
                }
                Extent::UfsRange {
                    ufs_key,
                    offset_in_object,
                    ..
                } => self.ufs_read_range(
                    ufs_key,
                    inode.ufs.as_ref().ok_or_else(|| {
                        FluxError::Meta("External inode missing pinned UFS object".into())
                    })?,
                    offset_in_object.saturating_add(source_start),
                    overlap_len,
                )?,
            };
            if data.len() as u64 != overlap_len {
                return Err(FluxError::Io(format!(
                    "range len mismatch: want={overlap_len} got={}",
                    data.len()
                )));
            }
            let target_start = (overlap_start - offset) as usize;
            output[target_start..target_start + data.len()].copy_from_slice(&data);
        }
        Ok(output)
    }

    fn ufs_read_range(
        &self,
        key: &str,
        object: &UfsObject,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        let ufs = self
            .ufs
            .as_ref()
            .ok_or_else(|| FluxError::Capability("UfsRange read requires --ufs mount".into()))?;
        ufs.block_on(ufs.reads.read(key, object, offset, len))
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
        let version = UfsVersion(
            obj.etag
                .clone()
                .unwrap_or_else(|| format!("size:{}", obj.size)),
        );
        let now = now_ms();
        let template = Inode {
            id: 0,
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: obj.size,
            mtime_ms: obj.mtime_ms.unwrap_or(now),
            ctime_ms: now,
            atime_ms: now,
            link_count: 1,
            generation: 1,
            head_gen: DataGen(0),
            ufs_gen: DataGen(0),
            ufs_base_version: Some(version.clone()),
            locality: LocalityLabel::External,
            locality_fields: Some(LocalityFields {
                backing_mode: BackingMode::UfsBacked,
                data_state: DataState::UfsClean,
                op_state: OpState::None,
                origin: Origin::Imported,
            }),
            ufs: Some(UfsObject {
                key: rel.to_string(),
                size: obj.size,
                etag: obj.etag,
                mtime_ms: obj.mtime_ms,
            }),
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let manifest = if obj.size == 0 {
            None
        } else {
            Some(Manifest {
                inode: 0,
                gen: DataGen(0),
                size: obj.size,
                extents: vec![Extent::UfsRange {
                    offset: 0,
                    len: obj.size,
                    ufs_key: rel.to_string(),
                    ufs_version: version,
                    offset_in_object: 0,
                }],
            })
        };
        let inode = match self
            .meta
            .import_external(parent, name, &template, manifest.as_ref())
        {
            Ok(i) => i,
            Err(FluxError::AlreadyExists) => return self.meta.lookup(parent, name),
            Err(e) => return Err(e),
        };
        self.remember_ufs_path(inode.id, rel.to_string());
        Ok(inode)
    }

    fn commit_external_dir(&self, parent: InodeId, name: &str, rel: &str) -> Result<Inode> {
        let now = now_ms();
        let template = Inode {
            id: 0,
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
            head_gen: DataGen(0),
            ufs_gen: DataGen(0),
            ufs_base_version: None,
            locality: LocalityLabel::External,
            locality_fields: Some(LocalityFields {
                backing_mode: BackingMode::UfsBacked,
                data_state: DataState::UfsClean,
                op_state: OpState::None,
                origin: Origin::Imported,
            }),
            ufs: Some(UfsObject {
                key: format!("{}/", rel.trim_end_matches('/')),
                size: 0,
                etag: None,
                mtime_ms: Some(now),
            }),
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let inode = match self.meta.import_external(parent, name, &template, None) {
            Ok(i) => i,
            Err(FluxError::AlreadyExists) => return self.meta.lookup(parent, name),
            Err(e) => return Err(e),
        };
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
    fn orphan_gc_reclaims_superseded_chunks_and_preserves_live_data() {
        let dir = tempfile::tempdir().unwrap();
        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks);
        let file = client
            .create_file(ROOT_INODE, "gc.bin", 0o644, 0, 0)
            .unwrap();
        client.write_at(file.id, 0, b"old bytes").unwrap();
        let old = client.chunks.list_chunks().unwrap();
        assert_eq!(old.len(), 1);
        client.write_at(file.id, 0, b"new bytes").unwrap();
        assert_eq!(client.chunks.list_chunks().unwrap().len(), 2);

        let report = client.run_orphan_gc().unwrap();
        assert_eq!(report.removed_manifests, 1);
        assert_eq!(report.removed_chunks, 1);
        assert!(!client.chunks.contains(&old[0]).unwrap());
        assert_eq!(client.read_all(file.id).unwrap(), b"new bytes");
        assert!(client.meta.current_gc_plan().unwrap().is_none());
    }

    #[test]
    fn concurrent_gc_respects_reservations_and_resumes_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let meta_path = dir.path().join("meta");
        let chunks_path = dir.path().join("chunks");
        let client = FluxClient::new(
            HeedMetaStore::open(&meta_path).unwrap(),
            DiskChunkStore::open(&chunks_path).unwrap(),
        );
        let file = client
            .create_file(ROOT_INODE, "reservation", 0o644, 0, 0)
            .unwrap();
        let bytes = b"staged-but-not-committed";
        let chunk = client.chunks.put(bytes).unwrap();
        let ticket = WriteTicketId(500);
        client
            .meta
            .reserve_chunks(ticket, file.id, file.generation, &[chunk])
            .unwrap();
        assert_eq!(client.run_concurrent_gc_pass(1).unwrap().removed_chunks, 0);
        assert!(client.chunks.contains(&chunk).unwrap());

        client.meta.abort_chunk_reservation(ticket).unwrap();
        let batch = client.meta.tombstone_gc_batch(&[chunk]).unwrap();
        assert_eq!(batch.tombstoned_chunks, vec![chunk]);
        // Crash after physical delete but before tombstone finalize.
        client.chunks.delete(&chunk).unwrap();
        drop(client);

        let recovered = FluxClient::new(
            HeedMetaStore::open(&meta_path).unwrap(),
            DiskChunkStore::open(&chunks_path).unwrap(),
        );
        recovered.run_concurrent_gc_pass(1).unwrap();
        assert!(recovered.meta.list_gc_tombstones().unwrap().is_empty());
        assert!(!recovered.chunks.contains(&chunk).unwrap());
    }

    #[test]
    fn concurrent_gc_pass_never_deletes_more_than_its_batch_budget() {
        let dir = tempfile::tempdir().unwrap();
        let client = FluxClient::new(
            HeedMetaStore::open(dir.path().join("meta")).unwrap(),
            DiskChunkStore::open(dir.path().join("chunks")).unwrap(),
        );
        for data in [b"orphan-one".as_slice(), b"orphan-two", b"orphan-three"] {
            client.chunks.put(data).unwrap();
        }
        assert_eq!(client.chunks.list_chunks().unwrap().len(), 3);
        assert_eq!(client.run_concurrent_gc_pass(1).unwrap().removed_chunks, 1);
        assert_eq!(client.chunks.list_chunks().unwrap().len(), 2);
        assert_eq!(client.run_concurrent_gc_pass(1).unwrap().removed_chunks, 1);
        assert_eq!(client.chunks.list_chunks().unwrap().len(), 1);
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
        assert_eq!(client.read_at(hello.id, 2, 4).unwrap(), b"tern");
        assert_eq!(client.ufs_read_stats().unwrap().backend_fetches, 1);
        assert_eq!(client.read_at(hello.id, 2, 4).unwrap(), b"tern");
        assert_eq!(client.ufs_read_stats().unwrap().backend_fetches, 1);
        assert!(client.ufs_read_stats().unwrap().cache_hits >= 1);
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
        assert_eq!(err, FluxError::ReadOnly);
    }

    #[test]
    fn external_small_read_does_not_materialize_large_object() {
        let dir = tempfile::tempdir().unwrap();
        let ufs_root = dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        let mut large = vec![0u8; 8 * 1024 * 1024];
        for (index, byte) in large.iter_mut().enumerate() {
            *byte = (index % 251) as u8;
        }
        std::fs::write(ufs_root.join("large.bin"), &large).unwrap();

        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks)
            .with_ufs(Ufs::local(&ufs_root).unwrap())
            .unwrap();
        let inode = client.lookup(ROOT_INODE, "large.bin").unwrap();
        let offset = 3 * 1024 * 1024 + 17;
        assert_eq!(
            client.read_at(inode.id, offset as u64, 64).unwrap(),
            large[offset..offset + 64]
        );
        // One demanded part plus at most two background prefetch parts, never
        // all eight MiB for this 64-byte FUSE read.
        assert!(client.ufs_read_stats().unwrap().backend_fetches <= 3);
    }

    #[test]
    fn external_random_write_copies_up_only_touched_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let ufs_root = dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        let mut original = vec![0u8; (3 * CHUNK_SIZE + 123) as usize];
        for (index, byte) in original.iter_mut().enumerate() {
            *byte = (index % 251) as u8;
        }
        std::fs::write(ufs_root.join("large.bin"), &original).unwrap();

        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks)
            .with_ufs(Ufs::local(&ufs_root).unwrap())
            .unwrap();
        let inode = client.lookup(ROOT_INODE, "large.bin").unwrap();

        let first_offset = CHUNK_SIZE + 17;
        client
            .write_at(inode.id, first_offset, b"first-copy-up")
            .unwrap();
        let second_offset = CHUNK_SIZE + 101;
        client
            .write_at(inode.id, second_offset, b"second-write")
            .unwrap();

        let mut expected = original.clone();
        expected[first_offset as usize..first_offset as usize + b"first-copy-up".len()]
            .copy_from_slice(b"first-copy-up");
        expected[second_offset as usize..second_offset as usize + b"second-write".len()]
            .copy_from_slice(b"second-write");
        assert_eq!(client.read_all(inode.id).unwrap(), expected);
        assert_eq!(std::fs::read(ufs_root.join("large.bin")).unwrap(), original);

        let dirty = client.get_inode(inode.id).unwrap();
        assert_eq!(dirty.locality, LocalityLabel::Dirty);
        assert_eq!(dirty.head_gen, DataGen(2));
        assert_eq!(dirty.ufs_gen, DataGen(0));
        let manifest = client
            .meta
            .get_manifest(dirty.manifest_id.unwrap())
            .unwrap();
        assert_eq!(manifest.extents.len(), 3);
        assert!(matches!(manifest.extents[0], Extent::UfsRange { .. }));
        assert!(matches!(manifest.extents[1], Extent::Local { .. }));
        assert!(matches!(manifest.extents[2], Extent::UfsRange { .. }));
    }

    #[test]
    fn recovery_replays_intents_before_and_after_ufs_publish() {
        let dir = tempfile::tempdir().unwrap();
        let meta_path = dir.path().join("meta");
        let chunks_path = dir.path().join("chunks");
        let ufs_root = dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();

        let meta = HeedMetaStore::open(&meta_path).unwrap();
        let mut before_put = meta
            .create(ROOT_INODE, "before.bin", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        let mut after_put = meta
            .create(ROOT_INODE, "after.bin", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        for (inode, key) in [
            (&mut before_put, "before.bin"),
            (&mut after_put, "after.bin"),
        ] {
            inode.locality_fields = Some(LocalityFields {
                backing_mode: BackingMode::UfsBacked,
                data_state: DataState::Dirty,
                op_state: OpState::None,
                origin: Origin::FluxCreated,
            });
            inode.locality = LocalityLabel::Dirty;
            inode.ufs = Some(UfsObject {
                key: key.into(),
                size: 0,
                etag: None,
                mtime_ms: None,
            });
            meta.put_inode(inode).unwrap();
        }

        let ufs = Ufs::local(&ufs_root).unwrap();
        let publish_ufs = ufs.clone();
        let client = FluxClient::new(meta, DiskChunkStore::open(&chunks_path).unwrap())
            .with_ufs(ufs)
            .unwrap();
        client
            .write_at(before_put.id, 0, b"replay-before-put")
            .unwrap();
        client
            .write_at(after_put.id, 0, b"replay-after-put")
            .unwrap();

        let begin = |inode_id| {
            let inode = client.get_inode(inode_id).unwrap();
            let manifest = client
                .meta
                .get_manifest(inode.manifest_id.unwrap())
                .unwrap();
            let bytes = client.read_all(inode_id).unwrap();
            let intent = FlushIntent {
                flush_id: FlushId::random(),
                snapshot_gen: inode.head_gen,
                snapshot_manifest_root: manifest.root_digest(),
                expected_ufs_version: None,
                target_digest: ChunkId::from_bytes(&bytes),
            };
            client
                .meta
                .begin_flush(inode.generation, inode_id, &intent)
                .unwrap();
            (intent, bytes)
        };
        let (_before_intent, _before_bytes) = begin(before_put.id);
        let (after_intent, after_bytes) = begin(after_put.id);

        // Simulate crash after the conditional Put and before CommitFlush.
        Runtime::new()
            .unwrap()
            .block_on(publish_ufs.publish_full_verified(
                "after.bin",
                &after_bytes,
                None,
                &after_intent.target_digest,
            ))
            .unwrap();
        drop(client);

        let recovered = FluxClient::new(
            HeedMetaStore::open(&meta_path).unwrap(),
            DiskChunkStore::open(&chunks_path).unwrap(),
        )
        .with_ufs(Ufs::local(&ufs_root).unwrap())
        .unwrap();
        let report = recovered.reconcile_flushes().unwrap();
        assert_eq!(report.completed, 2);
        assert_eq!(report.conflicts, 0);
        assert_eq!(report.pending, 0);
        assert_eq!(
            recovered.read_all(before_put.id).unwrap(),
            b"replay-before-put"
        );
        assert_eq!(
            recovered.read_all(after_put.id).unwrap(),
            b"replay-after-put"
        );
        for inode_id in [before_put.id, after_put.id] {
            let inode = recovered.get_inode(inode_id).unwrap();
            assert_eq!(inode.locality, LocalityLabel::Clean);
            assert!(inode.flush_intent.is_none());
            let manifest = recovered
                .meta
                .get_manifest(inode.manifest_id.unwrap())
                .unwrap();
            assert!(matches!(
                manifest.extents.as_slice(),
                [Extent::UfsRange { .. }]
            ));
        }
    }
}
