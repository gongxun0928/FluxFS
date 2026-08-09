//! Internal FluxFS client surface for alpha (CLI + FUSE + tests).

use fluxfs_chunk::ChunkStore;
use fluxfs_meta::MetaStore;
use fluxfs_metrics::FluxMetrics;
use fluxfs_types::{
    BackingMode, ChunkId, DataGen, DataState, Extent, ExtentTree, FileType, FlushId, FlushIntent,
    FluxError, Inode, InodeId, LocalityFields, LocalityLabel, Manifest, OpState, Origin,
    RequestOpId, Result, UfsObject, UfsVersion, WriteTicketId, CHUNK_SIZE, ROOT_INODE,
};
use fluxfs_ufs::{ReadPathConfig, ReadPathStats, Ufs, UfsEntryMode, UfsProbe, UfsReadPath};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::io::{Read, Write};
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
    fn new(ufs: Ufs, config: ReadPathConfig) -> Result<Self> {
        // Capture / create the runtime first so foyer HybridCache open can
        // bind to the same Tokio spawner used by subsequent UFS reads.
        // Always keep a Handle even for an owned Runtime so nested calls that
        // see Handle::try_current() (inside block_on) can use block_in_place
        // instead of re-entering Runtime::block_on (deadlock).
        let (handle, rt) = match Handle::try_current() {
            Ok(handle) => (handle, None),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .thread_name("fluxfs-ufs")
                    .build()
                    .map_err(|e| FluxError::Ufs(e.to_string()))?;
                let handle = rt.handle().clone();
                (handle, Some(rt))
            }
        };
        // Never call block_in_place unless already inside a runtime — sync
        // unit tests own a Runtime and must drive open via Runtime::block_on.
        let reads = if let Some(rt) = &rt {
            rt.block_on(UfsReadPath::open(ufs.clone(), config))?
        } else {
            tokio::task::block_in_place(|| handle.block_on(UfsReadPath::open(ufs.clone(), config)))?
        };
        Ok(Self {
            ufs,
            reads,
            handle: Some(handle),
            rt,
        })
    }

    fn block_on<T>(&self, fut: impl Future<Output = T>) -> T {
        if Handle::try_current().is_ok() {
            let handle = self.handle.as_ref().expect("runtime handle captured");
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else if let Some(rt) = &self.rt {
            rt.block_on(fut)
        } else if let Some(handle) = &self.handle {
            handle.block_on(fut)
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

/// POSIX inode attributes supplied by one FUSE `setattr` request.
///
/// Keeping the fields together lets size and metadata changes share one inode
/// generation CAS instead of a truncate followed by an unsafe whole-inode put.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InodeSetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime_ms: Option<i64>,
    pub mtime_ms: Option<i64>,
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

    /// Attach OpenDAL UFS with the default in-memory Clean read cache (tests).
    pub fn with_ufs(self, ufs: Ufs) -> Result<Self> {
        self.with_ufs_config(ufs, ReadPathConfig::default())
    }

    /// Attach OpenDAL UFS with an explicit Clean/External HybridCache config.
    ///
    /// Production mounts should pass [`ReadPathConfig::for_mount`] so the SSD
    /// tier is enabled under `<data-dir>/ufs-foyer-cache`.
    pub fn with_ufs_config(mut self, ufs: Ufs, config: ReadPathConfig) -> Result<Self> {
        self.ufs = Some(UfsRuntime::new(ufs, config)?);
        Ok(self)
    }

    pub fn has_ufs(&self) -> bool {
        self.ufs.is_some()
    }

    pub fn ufs_read_stats(&self) -> Option<ReadPathStats> {
        self.ufs.as_ref().map(|ufs| ufs.reads.stats())
    }

    /// Inspect the Clean/External read-path cache configuration (if attached).
    pub fn ufs_read_config(&self) -> Option<&ReadPathConfig> {
        self.ufs.as_ref().map(|ufs| ufs.reads.config())
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
        reject_imported_inode_mutation(&self.meta.get_inode(parent)?)?;
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
        reject_imported_inode_mutation(&self.meta.get_inode(parent)?)?;
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
        reject_imported_inode_mutation(&self.meta.get_inode(parent)?)?;
        let inode = self.meta.lookup(parent, name)?;
        reject_imported_inode_mutation(&inode)?;
        self.meta.unlink(parent, name)
    }

    pub fn rmdir(&self, parent: InodeId, name: &str) -> Result<()> {
        self.reject_ufs_mutation()?;
        reject_imported_inode_mutation(&self.meta.get_inode(parent)?)?;
        let inode = self.meta.lookup(parent, name)?;
        reject_imported_inode_mutation(&inode)?;
        self.meta.rmdir(parent, name)
    }

    pub fn rename(
        &self,
        old_parent: InodeId,
        old_name: &str,
        new_parent: InodeId,
        new_name: &str,
        no_replace: bool,
    ) -> Result<Inode> {
        self.reject_ufs_mutation()?;
        reject_imported_inode_mutation(&self.meta.get_inode(old_parent)?)?;
        let source = self.meta.lookup(old_parent, old_name)?;
        reject_imported_inode_mutation(&source)?;
        reject_imported_inode_mutation(&self.meta.get_inode(new_parent)?)?;
        match self.meta.lookup(new_parent, new_name) {
            Ok(destination) => reject_imported_inode_mutation(&destination)?,
            Err(FluxError::NotFound) => {}
            Err(error) => return Err(error),
        }
        self.meta
            .rename(old_parent, old_name, new_parent, new_name, no_replace)
    }

    pub fn lookup_path(&self, path: &str) -> Result<Inode> {
        let mut inode = self.meta.get_inode(ROOT_INODE)?;
        for part in path_components(path)? {
            inode = self.lookup(inode.id, part)?;
        }
        Ok(inode)
    }

    /// Resolve a path into its parent directory and final POSIX name.
    pub fn resolve_parent(&self, path: &str) -> Result<(Inode, String)> {
        let parts = path_components(path)?;
        let (name, parents) = parts
            .split_last()
            .ok_or_else(|| FluxError::InvalidArg("root has no parent name".into()))?;
        let mut parent = self.meta.get_inode(ROOT_INODE)?;
        for part in parents {
            parent = self.lookup(parent.id, part)?;
            if parent.file_type != FileType::Directory {
                return Err(FluxError::NotDirectory);
            }
        }
        Ok((parent, (*name).to_string()))
    }

    pub fn mkdir_path(&self, path: &str, mode: u32, uid: u32, gid: u32) -> Result<Inode> {
        let (parent, name) = self.resolve_parent(path)?;
        self.mkdir(parent.id, &name, mode, uid, gid)
    }

    pub fn create_file_path(&self, path: &str, mode: u32, uid: u32, gid: u32) -> Result<Inode> {
        let (parent, name) = self.resolve_parent(path)?;
        self.create_file(parent.id, &name, mode, uid, gid)
    }

    pub fn unlink_path(&self, path: &str) -> Result<()> {
        let (parent, name) = self.resolve_parent(path)?;
        self.unlink(parent.id, &name)
    }

    pub fn rmdir_path(&self, path: &str) -> Result<()> {
        let (parent, name) = self.resolve_parent(path)?;
        self.rmdir(parent.id, &name)
    }

    pub fn rename_path(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<Inode> {
        let (old_parent, old_name) = self.resolve_parent(old_path)?;
        let (new_parent, new_name) = self.resolve_parent(new_path)?;
        self.rename(
            old_parent.id,
            &old_name,
            new_parent.id,
            &new_name,
            no_replace,
        )
    }

    pub fn setattr_path(&self, path: &str, attrs: InodeSetAttr) -> Result<Inode> {
        let inode = self.lookup_path(path)?;
        self.setattr(inode.id, attrs)
    }

    /// Stream one inode to a writer without materializing the whole file.
    pub fn read_to_writer<W: Write>(&self, ino: InodeId, writer: &mut W) -> Result<u64> {
        let inode = self.meta.get_inode(ino)?;
        if inode.file_type != FileType::Regular {
            return Err(FluxError::IsDirectory);
        }
        let mut offset = 0_u64;
        while offset < inode.size {
            let len = (inode.size - offset).min(CHUNK_SIZE) as u32;
            let bytes = self.read_at(ino, offset, len)?;
            if bytes.is_empty() {
                return Err(FluxError::Io("short streaming read".into()));
            }
            writer
                .write_all(&bytes)
                .map_err(|error| FluxError::Io(error.to_string()))?;
            offset = offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| FluxError::InvalidArg("stream offset overflow".into()))?;
        }
        Ok(offset)
    }

    /// Replace an inode with bytes read incrementally from `reader`.
    pub fn write_from_reader<R: Read>(&self, ino: InodeId, reader: &mut R) -> Result<u64> {
        self.truncate(ino, 0)?;
        let mut buffer = vec![0_u8; CHUNK_SIZE as usize];
        let mut offset = 0_u64;
        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(|error| FluxError::Io(error.to_string()))?;
            if read == 0 {
                break;
            }
            self.write_at(ino, offset, &buffer[..read])?;
            offset = offset
                .checked_add(read as u64)
                .ok_or_else(|| FluxError::InvalidArg("stream offset overflow".into()))?;
        }
        Ok(offset)
    }

    pub fn put_chunk(&self, data: &[u8]) -> Result<ChunkId> {
        self.chunks.put(data)
    }

    pub fn get_chunk(&self, id: &ChunkId) -> Result<Vec<u8>> {
        self.chunks.get(id)
    }

    /// Fetch a chunk, optionally promoting into Worker Clean/hot HybridCache.
    pub fn get_chunk_with_promote(&self, id: &ChunkId, promote_cache: bool) -> Result<Vec<u8>> {
        self.chunks.get_with_promote(id, promote_cache)
    }

    /// Flush one Dirty UFS-backed inode through the durable intent protocol.
    /// Ephemeral and already-clean files are already durable at their declared tier.
    pub fn flush_inode(&self, ino: InodeId) -> Result<Inode> {
        let span = tracing::info_span!("flush_inode", inode = ino);
        let _enter = span.enter();
        let _timer = fluxfs_metrics::process_metrics().map(|m| m.flush_latency_ms.start_timer());
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
        let result = if let Some(intent) = inode.flush_intent.clone() {
            self.complete_flush_intent(inode.id, &intent)
        } else if fields.data_state != DataState::Dirty {
            Err(FluxError::Busy)
        } else {
            let manifest_id = inode
                .manifest_id
                .ok_or_else(|| FluxError::Meta("Dirty inode missing manifest".into()))?;
            let manifest = self.meta.get_manifest(manifest_id)?;
            if manifest.gen != inode.head_gen || manifest.size != inode.size {
                return Err(FluxError::Meta(
                    "Dirty inode head does not match manifest snapshot".into(),
                ));
            }
            let target_digest = self.inode_digest(&inode)?;
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
        };
        result
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
        let span = tracing::info_span!("gc_pass", batch_size);
        let _enter = span.enter();
        let _timer = fluxfs_metrics::process_metrics().map(|m| m.gc_pass_latency_ms.start_timer());
        if let Some(m) = fluxfs_metrics::process_metrics() {
            FluxMetrics::inc(&m.gc_pass_total);
        }
        let mut report = OrphanGcReport::default();
        // Expiration is bounded by the same scheduler budget. Late writers are
        // fenced because reserved commit requires the ticket to still exist.
        // ExpireChunkReservations apply also prunes expired client_requests.
        self.meta.expire_chunk_reservations(batch_size)?;
        let pending = self.meta.list_gc_tombstones()?;
        if let Some(m) = fluxfs_metrics::process_metrics() {
            FluxMetrics::set(&m.gc_tombstone_pending, pending.len() as u64);
        }
        if !pending.is_empty() {
            let batch = &pending[..pending.len().min(batch_size)];
            report.removed_chunks += self.reclaim_tombstones(batch)?;
        } else {
            let mut cursor = self
                .gc_cursor
                .lock()
                .map_err(|_| FluxError::Io("GC cursor lock poisoned".into()))?;
            let page = self.chunks.list_chunks_page(*cursor, batch_size)?;
            *cursor = page.next_cursor;
            drop(cursor);

            let batch = self.meta.tombstone_gc_batch(&page.chunks)?;
            report.removed_manifests += batch.removed_manifests;
            if let Some(m) = fluxfs_metrics::process_metrics() {
                FluxMetrics::add(&m.gc_tombstone_total, batch.tombstoned_chunks.len() as u64);
            }
            let created = self
                .meta
                .list_gc_tombstones()?
                .into_iter()
                .filter(|tombstone| batch.tombstoned_chunks.contains(&tombstone.chunk))
                .collect::<Vec<_>>();
            report.removed_chunks += self.reclaim_tombstones(&created)?;
        }
        Ok(report)
    }

    fn reclaim_tombstones(&self, tombstones: &[fluxfs_types::GcTombstone]) -> Result<usize> {
        let targets = self.chunks.gc_delete_targets()?;
        let uninitialized = tombstones
            .iter()
            .filter(|tombstone| !tombstone.targets_initialized)
            .map(|tombstone| tombstone.chunk)
            .collect::<Vec<_>>();
        if !uninitialized.is_empty() {
            self.meta
                .initialize_gc_delete_targets(&uninitialized, &targets)?;
        }

        let mut acknowledged = Vec::new();
        let mut completed = Vec::new();
        for tombstone in tombstones {
            let pending = if tombstone.targets_initialized {
                tombstone.pending_targets.as_slice()
            } else {
                targets.as_slice()
            };
            let mut failed = false;
            for target in pending {
                match self.chunks.delete_from_target(&tombstone.chunk, *target) {
                    Ok(()) => acknowledged.push((tombstone.chunk, *target)),
                    Err(_) => failed = true,
                }
            }
            if !failed {
                completed.push(tombstone.chunk);
            }
        }
        if !acknowledged.is_empty() {
            self.meta.acknowledge_gc_deletes(&acknowledged)?;
        }
        if !completed.is_empty() {
            self.meta.finalize_gc_tombstones(&completed)?;
        }
        Ok(completed.len())
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
                let mut writer = match ufs.block_on(
                    ufs.ufs.begin_verified_publish(
                        &object.key,
                        inode.size,
                        intent
                            .expected_ufs_version
                            .as_ref()
                            .map(|version| version.0.as_str()),
                        &intent.target_digest,
                    ),
                ) {
                    Ok(writer) => writer,
                    Err(FluxError::DirtyConflict) => {
                        return self.record_flush_conflict(
                            ino,
                            intent.flush_id,
                            "conditional UFS publish detected external version drift",
                        );
                    }
                    Err(error) => return Err(error),
                };
                let upload =
                    self.for_each_inode_chunk(&inode, |bytes| ufs.block_on(writer.write(bytes)));
                if let Err(error) = upload {
                    let _ = ufs.block_on(writer.abort());
                    return Err(error);
                }
                match ufs.block_on(writer.finish()) {
                    Ok(published) => published,
                    Err(FluxError::InvalidArg(_)) => {
                        return self.record_flush_conflict(
                            ino,
                            intent.flush_id,
                            "flush reconstructed bytes do not match target digest",
                        );
                    }
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
                Ok(inode) => {
                    if let Some(m) = fluxfs_metrics::process_metrics() {
                        FluxMetrics::inc(&m.flush_complete_total);
                    }
                    return Ok(inode);
                }
                Err(error) => return Err(error),
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
                Ok(_) => {
                    if let Some(m) = fluxfs_metrics::process_metrics() {
                        FluxMetrics::inc(&m.flush_conflict_total);
                    }
                    return Err(FluxError::DirtyConflict);
                }
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

    /// Durable random write. Only touched chunk-aligned windows are reconstructed,
    /// so sparse and multi-GiB files do not require whole-file buffering.
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
        if inode
            .locality_fields
            .as_ref()
            .is_some_and(|fields| fields.data_state == DataState::DirtyConflict)
        {
            return Err(FluxError::DirtyConflict);
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| FluxError::InvalidArg("offset overflow".into()))?;
        self.write_chunked(&mut inode, offset, data, end)
    }

    /// Copy up each touched 4 MiB window as a durable Local extent, then atomically
    /// switch the inode head. Untouched bytes remain pinned UFS ranges.
    fn write_chunked(&self, inode: &mut Inode, offset: u64, data: &[u8], end: u64) -> Result<u32> {
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
        mark_data_mutated(inode);
        inode.mtime_ms = now;
        inode.ctime_ms = now;
        self.commit_staged_manifest(expected_generation, inode, &manifest, &staged)?;
        Ok(data.len() as u32)
    }

    pub fn truncate(&self, ino: InodeId, size: u64) -> Result<Inode> {
        self.setattr(
            ino,
            InodeSetAttr {
                size: Some(size),
                ..InodeSetAttr::default()
            },
        )
    }

    /// Apply one POSIX setattr operation under the data-path lock and one inode
    /// generation CAS. A size change publishes its new manifest and all other
    /// supplied fields atomically with that head; metadata-only updates use the
    /// lighter `put_inode_cas` Raft mutation.
    pub fn setattr(&self, ino: InodeId, attrs: InodeSetAttr) -> Result<Inode> {
        let _guard = self
            .io_lock
            .lock()
            .map_err(|_| FluxError::Io("io lock poisoned".into()))?;
        let mut inode = self.meta.get_inode(ino)?;

        if let Some(size) = attrs.size {
            if inode.file_type != FileType::Regular {
                return Err(FluxError::IsDirectory);
            }
            if size != inode.size {
                return self.truncate_locked(&mut inode, size, &attrs);
            }
        }

        if !apply_inode_attrs(&mut inode, &attrs) {
            return Ok(inode);
        }

        let expected_generation = inode.generation;
        inode.generation = inode.generation.saturating_add(1);
        inode.ctime_ms = now_ms();
        self.meta.put_inode_cas(expected_generation, &inode)
    }

    /// Truncate (or sparse-extend) a regular file. On UFS-backed mounts this is
    /// Dirty copy-up, matching [`Self::write_at`]: touched Local windows are
    /// reconstructed, untouched `UfsRange`s stay pinned.
    fn truncate_locked(&self, inode: &mut Inode, size: u64, attrs: &InodeSetAttr) -> Result<Inode> {
        if inode
            .locality_fields
            .as_ref()
            .is_some_and(|fields| fields.data_state == DataState::DirtyConflict)
        {
            return Err(FluxError::DirtyConflict);
        }
        let expected_generation = inode.generation;
        let gen = DataGen(inode.head_gen.0.saturating_add(1));
        let current = match inode.manifest_id {
            Some(id) => self.meta.get_manifest(id)?,
            None => Manifest::empty(inode.id, inode.head_gen),
        };
        let mut extents = Vec::new();
        let mut staged = Vec::new();
        for extent in &current.extents {
            let start = extent.offset();
            if start >= size {
                break;
            }
            let end = start
                .checked_add(extent.len())
                .ok_or_else(|| FluxError::InvalidArg("extent end overflow".into()))?;
            if end <= size {
                extents.push(extent.clone());
                continue;
            }
            let keep = size - start;
            match extent {
                Extent::Local { chunk, .. } => {
                    let bytes = self.chunks.get(chunk)?;
                    let keep = usize::try_from(keep).map_err(|_| {
                        FluxError::Capability("partial chunk exceeds address space".into())
                    })?;
                    if bytes.len() < keep {
                        return Err(FluxError::Io("chunk shorter than manifest extent".into()));
                    }
                    let bytes = bytes[..keep].to_vec();
                    let chunk = ChunkId::from_bytes(&bytes);
                    extents.push(Extent::Local {
                        offset: start,
                        len: keep as u64,
                        chunk,
                    });
                    staged.push((chunk, bytes));
                }
                Extent::UfsRange {
                    ufs_key,
                    ufs_version,
                    offset_in_object,
                    ..
                } => extents.push(Extent::UfsRange {
                    offset: start,
                    len: keep,
                    ufs_key: ufs_key.clone(),
                    ufs_version: ufs_version.clone(),
                    offset_in_object: *offset_in_object,
                }),
            }
        }
        let manifest = Manifest {
            inode: inode.id,
            gen,
            size,
            extents: ExtentTree::try_from(extents)?,
        };
        inode.head_gen = gen;
        let now = now_ms();
        inode.size = size;
        inode.generation = inode.generation.saturating_add(1);
        mark_data_mutated(inode);
        inode.mtime_ms = now;
        inode.ctime_ms = now;
        apply_inode_attrs(inode, attrs);
        self.commit_staged_manifest(expected_generation, inode, &manifest, &staged)
    }

    fn for_each_inode_chunk(
        &self,
        inode: &Inode,
        mut consume: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut offset = 0;
        while offset < inode.size {
            let len = (inode.size - offset).min(CHUNK_SIZE);
            let bytes = self.read_inode_range(inode, offset, len)?;
            if bytes.len() as u64 != len {
                return Err(FluxError::Io(format!(
                    "streamed inode range length mismatch: want={len} got={}",
                    bytes.len()
                )));
            }
            consume(&bytes)?;
            offset = offset
                .checked_add(len)
                .ok_or_else(|| FluxError::InvalidArg("stream offset overflow".into()))?;
        }
        Ok(())
    }

    fn inode_digest(&self, inode: &Inode) -> Result<ChunkId> {
        let mut hasher = blake3::Hasher::new();
        self.for_each_inode_chunk(inode, |bytes| {
            hasher.update(bytes);
            Ok(())
        })?;
        Ok(ChunkId::from_raw(*hasher.finalize().as_bytes()))
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
                    // Clean (and Flushing→Clean) local extents may warm Worker
                    // foyer; Dirty/Ephemeral/Conflict skip so DRAM stays Clean-biased.
                    let promote_cache = matches!(
                        inode.locality,
                        LocalityLabel::Clean | LocalityLabel::External
                    ) || inode
                        .locality_fields
                        .as_ref()
                        .is_some_and(|f| f.data_state == DataState::UfsClean);
                    let chunk_data = self.chunks.get_with_promote(chunk, promote_cache)?;
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
                extents: ExtentTree::singleton(Extent::UfsRange {
                    offset: 0,
                    len: obj.size,
                    ufs_key: rel.to_string(),
                    ufs_version: version,
                    offset_in_object: 0,
                })?,
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

/// After a data-plane mutation, persist Dirty/Ephemeral locality fields and the
/// derived product label. Keeps `head_gen > ufs_gen ⇒ Dirty` coherent on disk.
fn mark_data_mutated(inode: &mut Inode) {
    let old_fields = inode.locality_fields.clone().unwrap_or_default();
    inode.locality_fields = Some(match old_fields.backing_mode {
        BackingMode::UfsBacked => LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::Dirty,
            op_state: OpState::None,
            origin: old_fields.origin,
        },
        BackingMode::Ephemeral => LocalityFields {
            backing_mode: BackingMode::Ephemeral,
            data_state: DataState::Ephemeral,
            op_state: OpState::None,
            origin: old_fields.origin,
        },
    });
    inode.locality = LocalityLabel::derive(
        inode.locality_fields.as_ref().expect("just assigned"),
        inode.head_gen,
        inode.ufs_gen,
    );
}

fn reject_imported_inode_mutation(inode: &Inode) -> Result<()> {
    let imported = inode.ufs.is_some()
        || inode.locality_fields.as_ref().is_some_and(|fields| {
            fields.backing_mode == BackingMode::UfsBacked || fields.origin == Origin::Imported
        });
    if imported {
        Err(FluxError::ReadOnly)
    } else {
        Ok(())
    }
}

fn path_components(path: &str) -> Result<Vec<&str>> {
    if !path.starts_with('/') {
        return Err(FluxError::InvalidArg(
            "FluxFS paths must be absolute".into(),
        ));
    }
    let mut parts = Vec::new();
    for part in path.split('/').filter(|part| !part.is_empty()) {
        if part == "." || part == ".." {
            return Err(FluxError::InvalidArg(format!(
                "path component is reserved: {part}"
            )));
        }
        if part.as_bytes().contains(&0) {
            return Err(FluxError::InvalidArg("path component contains NUL".into()));
        }
        if part.len() > 255 {
            return Err(FluxError::InvalidArg(format!(
                "path component too long: {} bytes",
                part.len()
            )));
        }
        parts.push(part);
    }
    Ok(parts)
}

fn apply_inode_attrs(inode: &mut Inode, attrs: &InodeSetAttr) -> bool {
    let mut touched = false;
    if let Some(mode) = attrs.mode {
        inode.mode = mode & 0o7777;
        touched = true;
    }
    if let Some(uid) = attrs.uid {
        inode.uid = uid;
        touched = true;
    }
    if let Some(gid) = attrs.gid {
        inode.gid = gid;
        touched = true;
    }
    if let Some(atime_ms) = attrs.atime_ms {
        inode.atime_ms = atime_ms;
        touched = true;
    }
    if let Some(mtime_ms) = attrs.mtime_ms {
        inode.mtime_ms = mtime_ms;
        touched = true;
    }
    touched
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxfs_chunk::DiskChunkStore;
    use fluxfs_meta::HeedMetaStore;
    use fluxfs_types::WorkerTargetId;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct FailingTargetStore {
        replicas: [DiskChunkStore; 2],
        fail_second: Arc<AtomicBool>,
    }

    impl ChunkStore for FailingTargetStore {
        fn put(&self, data: &[u8]) -> Result<ChunkId> {
            let id = self.replicas[0].put(data)?;
            assert_eq!(self.replicas[1].put(data)?, id);
            Ok(id)
        }

        fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
            self.replicas[0]
                .get(id)
                .or_else(|_| self.replicas[1].get(id))
        }

        fn contains(&self, id: &ChunkId) -> Result<bool> {
            Ok(self.replicas[0].contains(id)? || self.replicas[1].contains(id)?)
        }

        fn list_chunks(&self) -> Result<Vec<ChunkId>> {
            let mut chunks = self.replicas[0].list_chunks()?;
            chunks.extend(self.replicas[1].list_chunks()?);
            chunks.sort_by_key(ChunkId::to_hex);
            chunks.dedup();
            Ok(chunks)
        }

        fn delete(&self, id: &ChunkId) -> Result<()> {
            self.delete_from_target(id, WorkerTargetId(0))?;
            self.delete_from_target(id, WorkerTargetId(1))
        }

        fn gc_delete_targets(&self) -> Result<Vec<WorkerTargetId>> {
            Ok(vec![WorkerTargetId(0), WorkerTargetId(1)])
        }

        fn delete_from_target(&self, id: &ChunkId, target: WorkerTargetId) -> Result<()> {
            if target == WorkerTargetId(1) && self.fail_second.load(Ordering::SeqCst) {
                return Err(FluxError::Io("worker 1 unavailable".into()));
            }
            self.replicas
                .get(target.0 as usize)
                .ok_or_else(|| FluxError::InvalidArg("bad target".into()))?
                .delete(id)
        }
    }

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
    fn path_sdk_streams_crud_rename_and_rmdir() {
        let dir = tempfile::tempdir().unwrap();
        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks);

        client.mkdir_path("/a", 0o755, 10, 20).unwrap();
        client.mkdir_path("/b", 0o755, 10, 20).unwrap();
        let file = client
            .create_file_path("/a/data.bin", 0o640, 10, 20)
            .unwrap();
        let payload = vec![0x5a; CHUNK_SIZE as usize + 123];
        let mut source = std::io::Cursor::new(payload.clone());
        assert_eq!(
            client.write_from_reader(file.id, &mut source).unwrap(),
            payload.len() as u64
        );
        let mut output = Vec::new();
        assert_eq!(
            client.read_to_writer(file.id, &mut output).unwrap(),
            payload.len() as u64
        );
        assert_eq!(output, payload);

        let moved = client
            .rename_path("/a/data.bin", "/b/moved.bin", true)
            .unwrap();
        assert_eq!(moved.id, file.id);
        assert_eq!(client.lookup_path("/b/moved.bin").unwrap().id, file.id);
        assert_eq!(
            client.lookup_path("/a/data.bin").unwrap_err(),
            FluxError::NotFound
        );

        assert_eq!(client.rmdir_path("/b").unwrap_err(), FluxError::NotEmpty);
        client.unlink_path("/b/moved.bin").unwrap();
        client.rmdir_path("/b").unwrap();
        client.rmdir_path("/a").unwrap();
        assert_eq!(client.lookup_path("/b").unwrap_err(), FluxError::NotFound);
        assert!(matches!(
            client.lookup_path("/../escape"),
            Err(FluxError::InvalidArg(_))
        ));
    }

    #[test]
    fn imported_namespace_delete_and_rename_fail_closed_without_ufs_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let mut imported = meta
            .create(ROOT_INODE, "external", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        imported.locality_fields = Some(LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::UfsClean,
            op_state: OpState::None,
            origin: Origin::Imported,
        });
        imported.locality = LocalityLabel::External;
        imported.ufs = Some(UfsObject {
            key: "external".into(),
            size: 0,
            etag: Some("etag".into()),
            mtime_ms: None,
        });
        meta.put_inode(&imported).unwrap();
        let client = FluxClient::new(meta, chunks);

        assert_eq!(
            client.unlink(ROOT_INODE, "external"),
            Err(FluxError::ReadOnly)
        );
        assert_eq!(
            client
                .rename(ROOT_INODE, "external", ROOT_INODE, "renamed", false)
                .unwrap_err(),
            FluxError::ReadOnly
        );
        assert_eq!(
            client.lookup(ROOT_INODE, "external").unwrap().id,
            imported.id
        );

        let mut imported_dir = client
            .meta
            .create(ROOT_INODE, "external-dir", FileType::Directory, 0o755, 0, 0)
            .unwrap();
        imported_dir.locality_fields = Some(LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::UfsClean,
            op_state: OpState::None,
            origin: Origin::Imported,
        });
        imported_dir.locality = LocalityLabel::External;
        client.meta.put_inode(&imported_dir).unwrap();
        assert_eq!(
            client
                .create_file(imported_dir.id, "child", 0o644, 0, 0)
                .unwrap_err(),
            FluxError::ReadOnly
        );
        assert_eq!(
            client
                .mkdir(imported_dir.id, "child-dir", 0o755, 0, 0)
                .unwrap_err(),
            FluxError::ReadOnly
        );
        assert!(matches!(
            client.lookup_path("relative/path"),
            Err(FluxError::InvalidArg(_))
        ));
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
    fn sparse_write_and_truncate_beyond_one_gib_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks);
        let file = client
            .create_file(ROOT_INODE, "large-sparse.bin", 0o644, 0, 0)
            .unwrap();
        let large_size = (1_u64 << 30) + CHUNK_SIZE + 37;

        let grown = client.truncate(file.id, large_size).unwrap();
        assert_eq!(grown.size, large_size);
        let grown_manifest = client
            .meta
            .get_manifest(grown.manifest_id.unwrap())
            .unwrap();
        assert!(grown_manifest.extents.is_empty());

        let marker_offset = large_size - 11;
        client
            .write_at(file.id, marker_offset, b"large-file!")
            .unwrap();
        assert_eq!(
            client.read_at(file.id, marker_offset - 8, 19).unwrap(),
            [vec![0; 8], b"large-file!".to_vec()].concat()
        );
        let dirty = client.get_inode(file.id).unwrap();
        let dirty_manifest = client
            .meta
            .get_manifest(dirty.manifest_id.unwrap())
            .unwrap();
        assert_eq!(dirty_manifest.extents.len(), 1);

        let shrunk = client.truncate(file.id, marker_offset + 5).unwrap();
        assert_eq!(shrunk.size, marker_offset + 5);
        assert_eq!(
            client.read_at(file.id, marker_offset, 32).unwrap(),
            b"large"
        );
        let shrunk_manifest = client
            .meta
            .get_manifest(shrunk.manifest_id.unwrap())
            .unwrap();
        assert_eq!(shrunk_manifest.extents.len(), 1);
        let tail = shrunk_manifest.extents.iter().next().unwrap();
        assert_eq!(tail.offset() + tail.len(), marker_offset + 5);
        assert!(tail.len() <= CHUNK_SIZE);
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
    fn gc_delete_progress_survives_down_worker_and_meta_restart() {
        let dir = tempfile::tempdir().unwrap();
        let meta_path = dir.path().join("meta");
        let replica_paths = [dir.path().join("worker-0"), dir.path().join("worker-1")];
        let fail_second = Arc::new(AtomicBool::new(true));
        let chunks = FailingTargetStore {
            replicas: [
                DiskChunkStore::open(&replica_paths[0]).unwrap(),
                DiskChunkStore::open(&replica_paths[1]).unwrap(),
            ],
            fail_second: Arc::clone(&fail_second),
        };
        let client = FluxClient::new(HeedMetaStore::open(&meta_path).unwrap(), chunks);
        let orphan = client.chunks.put(b"durable-delete-retry").unwrap();
        assert_eq!(client.run_concurrent_gc_pass(1).unwrap().removed_chunks, 0);
        let tombstone = client.meta.list_gc_tombstones().unwrap().remove(0);
        assert!(tombstone.targets_initialized);
        assert_eq!(tombstone.pending_targets, vec![WorkerTargetId(1)]);
        assert!(!client.chunks.replicas[0].contains(&orphan).unwrap());
        assert!(client.chunks.replicas[1].contains(&orphan).unwrap());
        drop(client);

        fail_second.store(false, Ordering::SeqCst);
        let recovered = FluxClient::new(
            HeedMetaStore::open(&meta_path).unwrap(),
            FailingTargetStore {
                replicas: [
                    DiskChunkStore::open(&replica_paths[0]).unwrap(),
                    DiskChunkStore::open(&replica_paths[1]).unwrap(),
                ],
                fail_second,
            },
        );
        assert_eq!(
            recovered.run_concurrent_gc_pass(1).unwrap().removed_chunks,
            1
        );
        assert!(recovered.meta.list_gc_tombstones().unwrap().is_empty());
        assert!(!recovered.chunks.replicas[1].contains(&orphan).unwrap());
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
        let extents: Vec<_> = manifest.extents.iter().collect();
        assert!(matches!(extents[0], Extent::UfsRange { .. }));
        assert!(matches!(extents[1], Extent::Local { .. }));
        assert!(matches!(extents[2], Extent::UfsRange { .. }));
    }

    #[test]
    fn external_truncate_marks_dirty_and_preserves_pinned_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let ufs_root = dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        let mut original = vec![0u8; (2 * CHUNK_SIZE + 50) as usize];
        for (index, byte) in original.iter_mut().enumerate() {
            *byte = (index % 251) as u8;
        }
        std::fs::write(ufs_root.join("trunc.bin"), &original).unwrap();

        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let client = FluxClient::new(meta, chunks)
            .with_ufs(Ufs::local(&ufs_root).unwrap())
            .unwrap();
        let inode = client.lookup(ROOT_INODE, "trunc.bin").unwrap();
        assert_eq!(inode.locality, LocalityLabel::External);

        let new_size = CHUNK_SIZE + 10;
        let truncated = client
            .setattr(
                inode.id,
                InodeSetAttr {
                    size: Some(new_size),
                    mode: Some(0o600),
                    atime_ms: Some(1_234),
                    mtime_ms: Some(5_678),
                    ..InodeSetAttr::default()
                },
            )
            .unwrap();
        assert_eq!(truncated.size, new_size);
        assert_eq!(truncated.mode, 0o600);
        assert_eq!(truncated.atime_ms, 1_234);
        assert_eq!(truncated.mtime_ms, 5_678);
        assert_eq!(truncated.generation, inode.generation + 1);
        assert_eq!(truncated.locality, LocalityLabel::Dirty);
        let fields = truncated.locality_fields.as_ref().unwrap();
        assert_eq!(fields.data_state, DataState::Dirty);
        assert_eq!(fields.backing_mode, BackingMode::UfsBacked);

        let expected = original[..new_size as usize].to_vec();
        assert_eq!(client.read_all(truncated.id).unwrap(), expected);
        // Backing object unchanged until fsync publish.
        assert_eq!(std::fs::read(ufs_root.join("trunc.bin")).unwrap(), original);

        let manifest = client
            .meta
            .get_manifest(truncated.manifest_id.unwrap())
            .unwrap();
        // Pure External shrink keeps a shorter pinned UfsRange (no Local copy-up
        // needed); Dirty comes from head_gen / data_state.
        let extents: Vec<_> = manifest.extents.iter().collect();
        assert_eq!(extents.len(), 1);
        assert!(matches!(
            extents[0],
            Extent::UfsRange { offset: 0, len, .. } if *len == new_size
        ));

        // Same-size truncate is a no-op (no generation bump).
        let again = client.truncate(truncated.id, new_size).unwrap();
        assert_eq!(again.generation, truncated.generation);

        // Metadata-only setattr is also a single generation CAS on UFS mounts.
        let chmod = client
            .setattr(
                truncated.id,
                InodeSetAttr {
                    mode: Some(0o640),
                    ..InodeSetAttr::default()
                },
            )
            .unwrap();
        assert_eq!(chmod.mode, 0o640);
        assert_eq!(chmod.generation, truncated.generation + 1);

        // After a Dirty Local window exists, truncate must keep Dirty fields.
        client
            .write_at(truncated.id, CHUNK_SIZE / 2, b"mid")
            .unwrap();
        let after_write = client.get_inode(truncated.id).unwrap();
        assert_eq!(
            after_write.locality_fields.as_ref().unwrap().data_state,
            DataState::Dirty
        );
        let shrunk = client.truncate(after_write.id, 4).unwrap();
        assert_eq!(shrunk.size, 4);
        assert_eq!(
            shrunk.locality_fields.as_ref().unwrap().data_state,
            DataState::Dirty
        );
        assert_eq!(shrunk.locality, LocalityLabel::Dirty);

        let zeroed = client.truncate(shrunk.id, 0).unwrap();
        assert_eq!(zeroed.size, 0);
        assert_eq!(zeroed.locality, LocalityLabel::Dirty);
        assert!(client.read_all(zeroed.id).unwrap().is_empty());
    }

    #[test]
    fn dirty_conflict_rejects_data_mutation_but_allows_metadata_setattr() {
        let dir = tempfile::tempdir().unwrap();
        let meta = HeedMetaStore::open(dir.path().join("meta")).unwrap();
        let chunks = DiskChunkStore::open(dir.path().join("chunks")).unwrap();
        let mut inode = meta
            .create(ROOT_INODE, "conflict.bin", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        inode.locality_fields = Some(LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::DirtyConflict,
            op_state: OpState::None,
            origin: Origin::Imported,
        });
        inode.locality = LocalityLabel::Dirty;
        meta.put_inode(&inode).unwrap();
        let client = FluxClient::new(meta, chunks);

        assert_eq!(
            client.write_at(inode.id, 0, b"blocked"),
            Err(FluxError::DirtyConflict)
        );
        assert_eq!(
            client.truncate(inode.id, 10).unwrap_err(),
            FluxError::DirtyConflict
        );
        let chmod = client
            .setattr(
                inode.id,
                InodeSetAttr {
                    mode: Some(0o600),
                    ..InodeSetAttr::default()
                },
            )
            .unwrap();
        assert_eq!(chmod.mode, 0o600);
        assert_eq!(
            chmod.locality_fields.unwrap().data_state,
            DataState::DirtyConflict
        );
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
                manifest.extents.iter().next(),
                Some(Extent::UfsRange { .. })
            ));
        }
    }

    #[test]
    fn mount_ufs_cache_config_reaches_read_path_with_ssd_tier() {
        let dir = tempfile::tempdir().unwrap();
        let ufs_root = dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        std::fs::write(ufs_root.join("obj.bin"), b"hello-clean-cache").unwrap();

        let cfg = ReadPathConfig::for_mount(dir.path(), 16 * 1024 * 1024, 16 * 1024 * 1024, None);
        assert_eq!(
            cfg.cache_dir.as_deref(),
            Some(dir.path().join("ufs-foyer-cache").as_path())
        );
        assert_ne!(
            cfg.cache_dir.as_deref(),
            Some(dir.path().join("foyer-cache").as_path()),
            "UFS cache dir must stay isolated from Worker foyer-cache"
        );
        assert!(cfg.disk_capacity_bytes > 0);

        let client = FluxClient::new(
            HeedMetaStore::open(dir.path().join("meta")).unwrap(),
            DiskChunkStore::open(dir.path().join("chunks")).unwrap(),
        )
        .with_ufs_config(Ufs::local(&ufs_root).unwrap(), cfg.clone())
        .unwrap();

        let live = client.ufs_read_config().expect("ufs attached");
        assert_eq!(live.disk_capacity_bytes, cfg.disk_capacity_bytes);
        assert_eq!(live.cache_dir, cfg.cache_dir);
        assert!(live.cache_dir.as_ref().unwrap().exists());

        let inode = client.lookup(ROOT_INODE, "obj.bin").unwrap();
        assert_eq!(client.read_all(inode.id).unwrap(), b"hello-clean-cache");
        assert!(client.ufs_read_stats().unwrap().backend_fetches >= 1);
    }
}
