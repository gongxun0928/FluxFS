use crate::raft_types::{MetaRaftRequest, MetaRaftResponse, SmAppliedMeta};
use crate::store::MetaStore;
use fluxfs_types::{
    BackingMode, ChunkId, ChunkReservation, DataGen, DataState, Dentry, Extent, FileType, FlushId,
    FlushIntent, FluxError, GcBatch, GcLeaseId, GcPlan, Inode, InodeId, LocalityFields,
    LocalityLabel, Manifest, ManifestId, OpState, Origin, Result, UfsObject, UfsVersion,
    WriteTicketId, ROOT_INODE,
};
use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};
use openraft::BasicNode;
use openraft::StoredMembership;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

type InodeDb = Database<Bytes, Bytes>;
type DentryDb = Database<Str, Bytes>;
type MetaDb = Database<Str, Bytes>;
type ManifestDb = Database<Bytes, Bytes>;
type RequestDb = Database<Str, Bytes>;

const KEY_SM_LAST_APPLIED: &str = "raft_sm_last_applied";
const KEY_SM_LAST_MEMBERSHIP: &str = "raft_sm_last_membership";
const KEY_GC_LEASE: &str = "gc_lease";
const RESERVATION_PREFIX: &str = "write_reservation:";
const TOMBSTONE_PREFIX: &str = "gc_tombstone:";

/// Full MetaStore snapshot payload for OpenRaft install/build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaSnapshotData {
    pub inodes: Vec<Inode>,
    pub dentries: Vec<Dentry>,
    pub manifests: Vec<(u64, Manifest)>,
    pub next_inode: u64,
    pub next_manifest: u64,
    pub sm: SmAppliedMeta,
    /// Retained Meta mutation results keyed by [`fluxfs_types::RequestOpId`].
    #[serde(default)]
    pub client_requests: Vec<(String, MetaRaftResponse)>,
    /// A persistent stop-the-world lease makes physical chunk sweeping safe.
    #[serde(default)]
    pub gc_lease: Option<GcLeaseId>,
    #[serde(default)]
    pub chunk_reservations: Vec<ChunkReservation>,
    #[serde(default)]
    pub gc_tombstones: Vec<ChunkId>,
}

pub struct HeedMetaStore {
    env: Env,
    inodes: InodeDb,
    dentries: DentryDb,
    meta: MetaDb,
    manifests: ManifestDb,
    /// Durable request-id → apply result ledger for client retry dedup.
    client_requests: RequestDb,
    /// Serialize writers; LMDB allows one write txn at a time anyway.
    write_lock: Mutex<()>,
}

impl HeedMetaStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref()).map_err(|e| FluxError::Io(e.to_string()))?;
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(512 * 1024 * 1024)
                .max_dbs(16)
                .open(path.as_ref())
                .map_err(|e| FluxError::Meta(e.to_string()))?
        };

        let mut wtxn = env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let inodes: InodeDb = env
            .create_database(&mut wtxn, Some("inodes"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let dentries: DentryDb = env
            .create_database(&mut wtxn, Some("dentries"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let meta: MetaDb = env
            .create_database(&mut wtxn, Some("meta"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let manifests: ManifestDb = env
            .create_database(&mut wtxn, Some("manifests"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let client_requests: RequestDb = env
            .create_database(&mut wtxn, Some("client_requests"))
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
                head_gen: DataGen(1),
                ufs_gen: DataGen(0),
                ufs_base_version: None,
                locality: LocalityLabel::Ephemeral,
                locality_fields: Some(LocalityFields {
                    backing_mode: BackingMode::Ephemeral,
                    data_state: DataState::Ephemeral,
                    op_state: OpState::None,
                    origin: Origin::FluxCreated,
                }),
                ufs: None,
                extent_root: None,
                manifest_id: None,
                flush_intent: None,
                last_error: None,
            };
            put_inode_raw(&inodes, &mut wtxn, &root)?;
            meta.put(&mut wtxn, "next_inode", &u64_bytes(ROOT_INODE + 1))
                .map_err(|e| FluxError::Meta(e.to_string()))?;
            meta.put(&mut wtxn, "next_manifest", &u64_bytes(1))
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        } else if meta
            .get(&wtxn, "next_manifest")
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_none()
        {
            meta.put(&mut wtxn, "next_manifest", &u64_bytes(1))
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        }

        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;

        Ok(Self {
            env,
            inodes,
            dentries,
            meta,
            manifests,
            client_requests,
            write_lock: Mutex::new(()),
        })
    }

    pub fn load_sm_meta(&self) -> Result<SmAppliedMeta> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let last_applied_log = match self
            .meta
            .get(&rtxn, KEY_SM_LAST_APPLIED)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            Some(bytes) => {
                Some(serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?)
            }
            None => None,
        };
        let last_membership = match self
            .meta
            .get(&rtxn, KEY_SM_LAST_MEMBERSHIP)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            Some(bytes) => {
                serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?
            }
            None => StoredMembership::<u64, BasicNode>::default(),
        };
        Ok(SmAppliedMeta {
            last_applied_log,
            last_membership,
        })
    }

    fn put_sm_meta_raw(&self, wtxn: &mut heed::RwTxn, sm: &SmAppliedMeta) -> Result<()> {
        let applied =
            serde_json::to_vec(&sm.last_applied_log).map_err(|e| FluxError::Meta(e.to_string()))?;
        self.meta
            .put(wtxn, KEY_SM_LAST_APPLIED, &applied)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let membership =
            serde_json::to_vec(&sm.last_membership).map_err(|e| FluxError::Meta(e.to_string()))?;
        self.meta
            .put(wtxn, KEY_SM_LAST_MEMBERSHIP, &membership)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    /// Apply a Raft-normal request and persist SM markers in the same write txn.
    pub fn apply_raft_request(
        &self,
        req: &MetaRaftRequest,
        sm: &SmAppliedMeta,
    ) -> Result<MetaRaftResponse> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        if let Some(op_id) = req.request_id().filter(|id| !id.is_none()) {
            if let Some(cached) = self.get_client_request_in_txn(&wtxn, &op_id.to_hex())? {
                // Still advance SM markers so the Raft log entry is durable.
                self.put_sm_meta_raw(&mut wtxn, sm)?;
                wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
                return Ok(cached);
            }
        }

        let gc_blocked = self.gc_lease_in_txn(&wtxn)?.is_some()
            && !matches!(
                req,
                MetaRaftRequest::BeginGc { .. } | MetaRaftRequest::FinishGc { .. }
            );
        let resp = if gc_blocked {
            MetaRaftResponse::Err(FluxError::Busy)
        } else {
            match req {
                MetaRaftRequest::Create {
                    parent,
                    name,
                    file_type,
                    mode,
                    uid,
                    gid,
                    expected_parent_generation,
                    ..
                } => {
                    match self.create_in_txn(
                        &mut wtxn,
                        *expected_parent_generation,
                        *parent,
                        name,
                        *file_type,
                        *mode,
                        *uid,
                        *gid,
                    ) {
                        Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::PutInode { inode, .. } => {
                    match put_inode_raw(&self.inodes, &mut wtxn, inode.as_ref()) {
                        Ok(()) => MetaRaftResponse::Empty,
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::PutManifest { manifest, .. } => {
                    match self.put_manifest_in_txn(&mut wtxn, manifest.as_ref()) {
                        Ok(id) => MetaRaftResponse::ManifestId(id.0),
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::CommitInodeManifest {
                    expected_generation,
                    inode,
                    manifest,
                    ..
                } => {
                    if manifest_has_local(manifest) {
                        MetaRaftResponse::Err(FluxError::InvalidArg(
                            "Local manifest commit requires a pre-Put reservation".into(),
                        ))
                    } else {
                        match self.commit_inode_manifest_in_txn(
                            &mut wtxn,
                            *expected_generation,
                            inode.as_ref(),
                            manifest.as_ref(),
                        ) {
                            Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                            Err(e) => MetaRaftResponse::Err(e),
                        }
                    }
                }
                MetaRaftRequest::ReserveChunks {
                    ticket,
                    inode,
                    expected_generation,
                    chunks,
                    expires_at_unix_ms,
                    ..
                } => match self.reserve_chunks_in_txn(
                    &mut wtxn,
                    *ticket,
                    *inode,
                    *expected_generation,
                    chunks,
                    *expires_at_unix_ms,
                ) {
                    Ok(()) => MetaRaftResponse::Empty,
                    Err(e) => MetaRaftResponse::Err(e),
                },
                MetaRaftRequest::AbortChunkReservation { ticket, .. } => {
                    match self.abort_reservation_in_txn(&mut wtxn, *ticket) {
                        Ok(()) => MetaRaftResponse::Empty,
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::ExpireChunkReservations {
                    cutoff_unix_ms,
                    max_to_expire,
                    ..
                } => match self.expire_reservations_in_txn(
                    &mut wtxn,
                    *cutoff_unix_ms,
                    usize::try_from(*max_to_expire).unwrap_or(usize::MAX),
                ) {
                    Ok(_) => MetaRaftResponse::Empty,
                    Err(e) => MetaRaftResponse::Err(e),
                },
                MetaRaftRequest::CommitInodeManifestReserved {
                    ticket,
                    expected_generation,
                    inode,
                    manifest,
                    ..
                } => match self.commit_reserved_in_txn(
                    &mut wtxn,
                    *ticket,
                    *expected_generation,
                    inode,
                    manifest,
                ) {
                    Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                    Err(e) => MetaRaftResponse::Err(e),
                },
                MetaRaftRequest::TombstoneGcBatch { candidates, .. } => {
                    match self.tombstone_gc_batch_in_txn(&mut wtxn, candidates) {
                        Ok(batch) => MetaRaftResponse::GcBatch(Box::new(batch)),
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::FinalizeGcTombstones { chunks, .. } => {
                    match self.finalize_tombstones_in_txn(&mut wtxn, chunks) {
                        Ok(()) => MetaRaftResponse::Empty,
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::BeginFlush {
                    expected_generation,
                    inode,
                    intent,
                    ..
                } => match self.begin_flush_in_txn(
                    &mut wtxn,
                    *expected_generation,
                    *inode,
                    intent.as_ref(),
                ) {
                    Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                    Err(e) => MetaRaftResponse::Err(e),
                },
                MetaRaftRequest::CommitFlush {
                    expected_generation,
                    inode,
                    flush_id,
                    published_ufs,
                    ..
                } => match self.commit_flush_in_txn(
                    &mut wtxn,
                    *expected_generation,
                    *inode,
                    *flush_id,
                    published_ufs.as_ref(),
                ) {
                    Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                    Err(e) => MetaRaftResponse::Err(e),
                },
                MetaRaftRequest::FailFlushConflict {
                    expected_generation,
                    inode,
                    flush_id,
                    error,
                    ..
                } => match self.fail_flush_conflict_in_txn(
                    &mut wtxn,
                    *expected_generation,
                    *inode,
                    *flush_id,
                    error,
                ) {
                    Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                    Err(e) => MetaRaftResponse::Err(e),
                },
                MetaRaftRequest::BeginGc { lease_id, .. } => {
                    match self.begin_gc_in_txn(&mut wtxn, *lease_id) {
                        Ok(plan) => MetaRaftResponse::GcPlan(Box::new(plan)),
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::FinishGc { lease_id, .. } => {
                    match self.finish_gc_in_txn(&mut wtxn, *lease_id) {
                        Ok(()) => MetaRaftResponse::Empty,
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
                MetaRaftRequest::ImportExternal {
                    parent,
                    name,
                    inode,
                    manifest,
                    expected_parent_generation,
                    ..
                } => match self.import_external_in_txn(
                    &mut wtxn,
                    *expected_parent_generation,
                    *parent,
                    name,
                    inode.as_ref(),
                    manifest.as_deref(),
                ) {
                    Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                    Err(e) => MetaRaftResponse::Err(e),
                },
                MetaRaftRequest::Unlink {
                    parent,
                    name,
                    expected_parent_generation,
                    ..
                } => {
                    match self.unlink_in_txn(&mut wtxn, *expected_parent_generation, *parent, name)
                    {
                        Ok(()) => MetaRaftResponse::Empty,
                        Err(e) => MetaRaftResponse::Err(e),
                    }
                }
            }
        };

        if let Some(op_id) = req.request_id().filter(|id| !id.is_none()) {
            // Retain successes and typed application errors so retries are stable.
            self.put_client_request_in_txn(&mut wtxn, &op_id.to_hex(), &resp)?;
        }

        // Always advance applied markers with the mutation attempt (including typed Err).
        self.put_sm_meta_raw(&mut wtxn, sm)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(resp)
    }

    fn get_client_request_in_txn(
        &self,
        txn: &heed::RwTxn<'_>,
        op_id: &str,
    ) -> Result<Option<MetaRaftResponse>> {
        let Some(bytes) = self
            .client_requests
            .get(txn, op_id)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        else {
            return Ok(None);
        };
        let resp = serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(Some(resp))
    }

    fn put_client_request_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        op_id: &str,
        resp: &MetaRaftResponse,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(resp).map_err(|e| FluxError::Meta(e.to_string()))?;
        self.client_requests
            .put(txn, op_id, &bytes)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    pub fn save_sm_meta_only(&self, sm: &SmAppliedMeta) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        self.put_sm_meta_raw(&mut wtxn, sm)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    pub fn export_snapshot(&self, sm: &SmAppliedMeta) -> Result<MetaSnapshotData> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let mut inodes = Vec::new();
        let iter = self
            .inodes
            .iter(&rtxn)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        for item in iter {
            let (_k, v) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            inodes.push(serde_json::from_slice(v).map_err(|e| FluxError::Meta(e.to_string()))?);
        }
        let mut dentries = Vec::new();
        let diter = self
            .dentries
            .iter(&rtxn)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        for item in diter {
            let (key, val) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let (parent_hex, name) = key
                .split_once('\0')
                .ok_or_else(|| FluxError::Meta("bad dentry key".into()))?;
            let parent =
                u64::from_str_radix(parent_hex, 16).map_err(|e| FluxError::Meta(e.to_string()))?;
            dentries.push(Dentry {
                parent,
                name: name.to_string(),
                child: u64_from_bytes(val)?,
            });
        }
        let mut manifests = Vec::new();
        let miter = self
            .manifests
            .iter(&rtxn)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        for item in miter {
            let (k, v) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let id = u64_from_bytes(k)?;
            let manifest: Manifest =
                serde_json::from_slice(v).map_err(|e| FluxError::Meta(e.to_string()))?;
            manifests.push((id, manifest));
        }
        let next_inode = u64_from_bytes(
            self.meta
                .get(&rtxn, "next_inode")
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .ok_or_else(|| FluxError::Meta("missing next_inode".into()))?,
        )?;
        let next_manifest = u64_from_bytes(
            self.meta
                .get(&rtxn, "next_manifest")
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .ok_or_else(|| FluxError::Meta("missing next_manifest".into()))?,
        )?;
        let mut client_requests = Vec::new();
        let riter = self
            .client_requests
            .iter(&rtxn)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        for item in riter {
            let (k, v) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let resp: MetaRaftResponse =
                serde_json::from_slice(v).map_err(|e| FluxError::Meta(e.to_string()))?;
            client_requests.push((k.to_string(), resp));
        }
        let gc_lease = self.gc_lease_in_txn(&rtxn)?;
        let chunk_reservations = self.list_reservations_in_txn(&rtxn)?;
        let gc_tombstones = self.list_tombstones_in_txn(&rtxn)?;
        drop(rtxn);
        Ok(MetaSnapshotData {
            inodes,
            dentries,
            manifests,
            next_inode,
            next_manifest,
            sm: sm.clone(),
            client_requests,
            gc_lease,
            chunk_reservations,
            gc_tombstones,
        })
    }

    pub fn install_snapshot_data(&self, snap: &MetaSnapshotData) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        // Clear existing app DBs (keep raft log env separate).
        {
            let keys: Vec<Vec<u8>> = self
                .inodes
                .iter(&wtxn)
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .map(|i| i.map(|(k, _)| k.to_vec()))
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| FluxError::Meta(e.to_string()))?;
            for k in keys {
                self.inodes
                    .delete(&mut wtxn, &k)
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
            }
        }
        {
            let keys: Vec<String> = self
                .dentries
                .iter(&wtxn)
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .map(|i| i.map(|(k, _)| k.to_string()))
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| FluxError::Meta(e.to_string()))?;
            for k in keys {
                self.dentries
                    .delete(&mut wtxn, &k)
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
            }
        }
        {
            let keys: Vec<Vec<u8>> = self
                .manifests
                .iter(&wtxn)
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .map(|i| i.map(|(k, _)| k.to_vec()))
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| FluxError::Meta(e.to_string()))?;
            for k in keys {
                self.manifests
                    .delete(&mut wtxn, &k)
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
            }
        }

        for inode in &snap.inodes {
            put_inode_raw(&self.inodes, &mut wtxn, inode)?;
        }
        for d in &snap.dentries {
            self.dentries
                .put(
                    &mut wtxn,
                    &dentry_key(d.parent, &d.name),
                    &u64_bytes(d.child),
                )
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        }
        for (id, manifest) in &snap.manifests {
            let bytes = serde_json::to_vec(manifest).map_err(|e| FluxError::Meta(e.to_string()))?;
            self.manifests
                .put(&mut wtxn, &inode_key(*id), &bytes)
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        }
        {
            let keys: Vec<String> = self
                .client_requests
                .iter(&wtxn)
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .map(|i| i.map(|(k, _)| k.to_string()))
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| FluxError::Meta(e.to_string()))?;
            for k in keys {
                self.client_requests
                    .delete(&mut wtxn, &k)
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
            }
        }
        for (op_id, resp) in &snap.client_requests {
            self.put_client_request_in_txn(&mut wtxn, op_id, resp)?;
        }
        self.meta
            .put(&mut wtxn, "next_inode", &u64_bytes(snap.next_inode))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        self.meta
            .put(&mut wtxn, "next_manifest", &u64_bytes(snap.next_manifest))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        match snap.gc_lease {
            Some(lease) => self
                .meta
                .put(&mut wtxn, KEY_GC_LEASE, &u64_bytes(lease.0))
                .map_err(|e| FluxError::Meta(e.to_string()))?,
            None => {
                self.meta
                    .delete(&mut wtxn, KEY_GC_LEASE)
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
            }
        }
        self.clear_meta_prefix_in_txn(&mut wtxn, RESERVATION_PREFIX)?;
        self.clear_meta_prefix_in_txn(&mut wtxn, TOMBSTONE_PREFIX)?;
        for reservation in &snap.chunk_reservations {
            self.put_reservation_in_txn(&mut wtxn, reservation)?;
        }
        for chunk in &snap.gc_tombstones {
            self.meta
                .put(&mut wtxn, &tombstone_key(chunk), &[])
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        }
        self.put_sm_meta_raw(&mut wtxn, &snap.sm)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    /// Load parent directory, optional generation CAS, then bump generation/mtime.
    fn load_and_bump_parent_dir(
        &self,
        wtxn: &mut heed::RwTxn,
        parent: InodeId,
        expected_parent_generation: Option<u64>,
    ) -> Result<Inode> {
        let parent_bytes = self
            .inodes
            .get(wtxn, &inode_key(parent))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let mut parent_ino: Inode =
            serde_json::from_slice(&parent_bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        if let Some(expected) = expected_parent_generation {
            if parent_ino.generation != expected {
                return Err(FluxError::CasFailed {
                    expected,
                    actual: parent_ino.generation,
                });
            }
        }
        let now = now_ms();
        parent_ino.generation = parent_ino.generation.saturating_add(1);
        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        put_inode_raw(&self.inodes, wtxn, &parent_ino)?;
        Ok(parent_ino)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        expected_parent_generation: Option<u64>,
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
        if self
            .dentries
            .get(wtxn, &dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_some()
        {
            return Err(FluxError::AlreadyExists);
        }
        // CAS + bump parent before allocating child so failed CAS leaves no orphan id.
        self.load_and_bump_parent_dir(wtxn, parent, expected_parent_generation)?;
        let id = self.alloc_inode(wtxn)?;
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
            head_gen: DataGen(1),
            ufs_gen: DataGen(0),
            ufs_base_version: None,
            locality: LocalityLabel::Ephemeral,
            locality_fields: Some(LocalityFields {
                backing_mode: BackingMode::Ephemeral,
                data_state: DataState::Ephemeral,
                op_state: OpState::None,
                origin: Origin::FluxCreated,
            }),
            ufs: None,
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        put_inode_raw(&self.inodes, wtxn, &inode)?;
        self.dentries
            .put(wtxn, &dentry_key(parent, name), &u64_bytes(id))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(inode)
    }

    fn put_manifest_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        manifest: &Manifest,
    ) -> Result<ManifestId> {
        manifest.validate()?;
        let id = self.alloc_manifest_id(wtxn)?;
        let bytes = serde_json::to_vec(manifest).map_err(|e| FluxError::Meta(e.to_string()))?;
        self.manifests
            .put(wtxn, &inode_key(id.0), &bytes)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(id)
    }

    fn import_external_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        template: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode> {
        if name.is_empty() || name.contains('/') || name == "." || name == ".." {
            return Err(FluxError::InvalidArg(format!("bad name: {name}")));
        }
        if !matches!(template.locality, LocalityLabel::External)
            && !matches!(
                template.locality_fields.as_ref().map(|f| f.origin),
                Some(Origin::Imported)
            )
        {
            return Err(FluxError::InvalidArg(
                "import_external requires External/Imported locality".into(),
            ));
        }
        if self
            .dentries
            .get(wtxn, &dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_some()
        {
            return Err(FluxError::AlreadyExists);
        }
        self.load_and_bump_parent_dir(wtxn, parent, expected_parent_generation)?;

        let id = self.alloc_inode(wtxn)?;
        let mut inode = template.clone();
        inode.id = id;
        inode.locality = LocalityLabel::External;
        if inode.locality_fields.is_none() {
            inode.locality_fields = Some(LocalityFields {
                backing_mode: BackingMode::UfsBacked,
                data_state: DataState::UfsClean,
                op_state: OpState::None,
                origin: Origin::Imported,
            });
        }

        if let Some(m) = manifest {
            let mut m = m.clone();
            m.inode = id;
            m.validate()?;
            let mid = self.put_manifest_in_txn(wtxn, &m)?;
            inode.manifest_id = Some(mid);
            inode.size = m.size;
        } else {
            inode.manifest_id = None;
        }

        put_inode_raw(&self.inodes, wtxn, &inode)?;
        self.dentries
            .put(wtxn, &dentry_key(parent, name), &u64_bytes(id))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(inode)
    }

    fn commit_inode_manifest_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        if manifest.inode != inode.id {
            return Err(FluxError::InvalidArg(
                "manifest.inode must match inode.id".into(),
            ));
        }
        let current_bytes = self
            .inodes
            .get(wtxn, &inode_key(inode.id))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let current: Inode =
            serde_json::from_slice(&current_bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        if current.generation != expected_generation {
            return Err(FluxError::CasFailed {
                expected: expected_generation,
                actual: current.generation,
            });
        }
        if current.flush_intent.is_some() {
            return Err(FluxError::Busy);
        }
        let mid = self.put_manifest_in_txn(wtxn, manifest)?;
        let mut next = inode.clone();
        next.manifest_id = Some(mid);
        put_inode_raw(&self.inodes, wtxn, &next)?;
        Ok(next)
    }

    fn begin_flush_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        expected_generation: u64,
        inode_id: InodeId,
        intent: &FlushIntent,
    ) -> Result<Inode> {
        let mut inode = get_inode_raw(&self.inodes, wtxn, inode_id)?;
        check_generation(&inode, expected_generation)?;
        if inode.file_type != FileType::Regular {
            return Err(FluxError::IsDirectory);
        }
        let fields = inode
            .locality_fields
            .as_mut()
            .ok_or_else(|| FluxError::Meta("flush inode missing locality fields".into()))?;
        if fields.backing_mode != BackingMode::UfsBacked || fields.data_state != DataState::Dirty {
            return Err(FluxError::InvalidArg(
                "only UFS-backed Dirty files can flush".into(),
            ));
        }
        if inode.head_gen != intent.snapshot_gen {
            return Err(FluxError::CasFailed {
                expected: intent.snapshot_gen.0,
                actual: inode.head_gen.0,
            });
        }
        if inode.flush_intent.is_some() || !matches!(fields.op_state, OpState::None) {
            return Err(FluxError::Busy);
        }
        fields.op_state = OpState::Flushing {
            intent: intent.clone(),
        };
        inode.flush_intent = Some(intent.clone());
        inode.last_error = None;
        inode.generation = inode.generation.saturating_add(1);
        inode.locality = LocalityLabel::derive(fields, inode.head_gen, inode.ufs_gen);
        put_inode_raw(&self.inodes, wtxn, &inode)?;
        Ok(inode)
    }

    fn commit_flush_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        expected_generation: u64,
        inode_id: InodeId,
        flush_id: FlushId,
        published_ufs: &UfsObject,
    ) -> Result<Inode> {
        let mut inode = get_inode_raw(&self.inodes, wtxn, inode_id)?;
        check_generation(&inode, expected_generation)?;
        let intent = matching_flush_intent(&inode, flush_id)?.clone();
        let fields = inode
            .locality_fields
            .as_mut()
            .ok_or_else(|| FluxError::Meta("flush inode missing locality fields".into()))?;

        inode.generation = inode.generation.saturating_add(1);
        inode.flush_intent = None;
        fields.op_state = OpState::None;
        if inode.head_gen == intent.snapshot_gen {
            let version = published_ufs
                .etag
                .clone()
                .map(UfsVersion)
                .unwrap_or_else(|| UfsVersion(format!("digest:{}", intent.target_digest.to_hex())));
            let clean_manifest = Manifest {
                inode: inode.id,
                gen: intent.snapshot_gen,
                size: inode.size,
                extents: if inode.size == 0 {
                    Vec::new()
                } else {
                    vec![Extent::UfsRange {
                        offset: 0,
                        len: inode.size,
                        ufs_key: published_ufs.key.clone(),
                        ufs_version: version.clone(),
                        offset_in_object: 0,
                    }]
                },
            };
            inode.manifest_id = if inode.size == 0 {
                None
            } else {
                Some(self.put_manifest_in_txn(wtxn, &clean_manifest)?)
            };
            inode.ufs = Some(published_ufs.clone());
            inode.ufs_gen = intent.snapshot_gen;
            inode.ufs_base_version = Some(version);
            fields.data_state = DataState::UfsClean;
            inode.last_error = None;
        } else {
            fields.data_state = DataState::DirtyConflict;
            inode.last_error = Some(
                "head advanced during flush; published snapshot not installed as clean".into(),
            );
        }
        inode.locality = LocalityLabel::derive(fields, inode.head_gen, inode.ufs_gen);
        put_inode_raw(&self.inodes, wtxn, &inode)?;
        Ok(inode)
    }

    fn fail_flush_conflict_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        expected_generation: u64,
        inode_id: InodeId,
        flush_id: FlushId,
        error: &str,
    ) -> Result<Inode> {
        let mut inode = get_inode_raw(&self.inodes, wtxn, inode_id)?;
        check_generation(&inode, expected_generation)?;
        matching_flush_intent(&inode, flush_id)?;
        let fields = inode
            .locality_fields
            .as_mut()
            .ok_or_else(|| FluxError::Meta("flush inode missing locality fields".into()))?;
        fields.data_state = DataState::DirtyConflict;
        fields.op_state = OpState::None;
        inode.flush_intent = None;
        inode.last_error = Some(error.to_string());
        inode.generation = inode.generation.saturating_add(1);
        inode.locality = LocalityLabel::derive(fields, inode.head_gen, inode.ufs_gen);
        put_inode_raw(&self.inodes, wtxn, &inode)?;
        Ok(inode)
    }

    fn unlink_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
    ) -> Result<()> {
        let key = dentry_key(parent, name);
        let child_raw = self
            .dentries
            .get(wtxn, &key)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let child_id = u64_from_bytes(&child_raw)?;
        self.load_and_bump_parent_dir(wtxn, parent, expected_parent_generation)?;
        self.dentries
            .delete(wtxn, &key)
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        let mut child = get_inode_raw(&self.inodes, wtxn, child_id)?;
        if child.link_count == 0 {
            return Err(FluxError::Meta(format!(
                "inode {child_id} already has link_count=0"
            )));
        }
        child.link_count -= 1;
        child.ctime_ms = now_ms();
        if child.link_count == 0 {
            // Drop live manifest refs so concurrent GC can reclaim chunks.
            // Abort in-flight write reservations for this inode (deterministic
            // expire of abandoned tickets is T2; unlink must not leave forever-live
            // reservations pinning deleted-file chunks).
            for reservation in self.list_reservations_in_txn(wtxn)? {
                if reservation.inode == child_id {
                    self.abort_reservation_in_txn(wtxn, reservation.ticket)?;
                }
            }
            self.inodes
                .delete(wtxn, &inode_key(child_id))
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        } else {
            put_inode_raw(&self.inodes, wtxn, &child)?;
        }
        Ok(())
    }

    fn reservation_in_txn(
        &self,
        txn: &heed::RoTxn<'_>,
        ticket: WriteTicketId,
    ) -> Result<Option<ChunkReservation>> {
        self.meta
            .get(txn, &reservation_key(ticket))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .map(|bytes| serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string())))
            .transpose()
    }

    fn put_reservation_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        reservation: &ChunkReservation,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(reservation).map_err(|e| FluxError::Meta(e.to_string()))?;
        self.meta
            .put(txn, &reservation_key(reservation.ticket), &bytes)
            .map_err(|e| FluxError::Meta(e.to_string()))
    }

    fn list_reservations_in_txn(&self, txn: &heed::RoTxn<'_>) -> Result<Vec<ChunkReservation>> {
        let mut out = Vec::new();
        for item in self
            .meta
            .prefix_iter(txn, RESERVATION_PREFIX)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            let (_, bytes) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            out.push(serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?);
        }
        out.sort_by_key(|reservation: &ChunkReservation| reservation.ticket.0);
        Ok(out)
    }

    fn list_tombstones_in_txn(&self, txn: &heed::RoTxn<'_>) -> Result<Vec<ChunkId>> {
        let mut out = Vec::new();
        for item in self
            .meta
            .prefix_iter(txn, TOMBSTONE_PREFIX)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            let (key, _) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let hex = key
                .strip_prefix(TOMBSTONE_PREFIX)
                .ok_or_else(|| FluxError::Meta("bad tombstone key".into()))?;
            out.push(chunk_from_hex(hex)?);
        }
        out.sort_by_key(ChunkId::to_hex);
        Ok(out)
    }

    fn clear_meta_prefix_in_txn(&self, txn: &mut heed::RwTxn<'_>, prefix: &str) -> Result<()> {
        let keys = self
            .meta
            .prefix_iter(txn, prefix)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .map(|item| {
                item.map(|(key, _)| key.to_string())
                    .map_err(|e| FluxError::Meta(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        for key in keys {
            self.meta
                .delete(txn, &key)
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        }
        Ok(())
    }

    fn reserve_chunks_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        ticket: WriteTicketId,
        inode: InodeId,
        expected_generation: u64,
        chunks: &[ChunkId],
        expires_at_unix_ms: u64,
    ) -> Result<()> {
        let current = get_inode_raw(&self.inodes, txn, inode)?;
        check_generation(&current, expected_generation)?;
        let mut chunks = chunks.to_vec();
        chunks.sort_by_key(ChunkId::to_hex);
        chunks.dedup();
        for chunk in &chunks {
            if self
                .meta
                .get(txn, &tombstone_key(chunk))
                .map_err(|e| FluxError::Meta(e.to_string()))?
                .is_some()
            {
                return Err(FluxError::Busy);
            }
        }
        let reservation = ChunkReservation {
            ticket,
            inode,
            expected_generation,
            chunks,
            expires_at_unix_ms,
        };
        if let Some(existing) = self.reservation_in_txn(txn, ticket)? {
            return if existing == reservation {
                Ok(())
            } else {
                Err(FluxError::AlreadyExists)
            };
        }
        self.put_reservation_in_txn(txn, &reservation)
    }

    fn abort_reservation_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        ticket: WriteTicketId,
    ) -> Result<()> {
        self.meta
            .delete(txn, &reservation_key(ticket))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    fn expire_reservations_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        cutoff_unix_ms: u64,
        max_to_expire: usize,
    ) -> Result<usize> {
        if max_to_expire == 0 {
            return Ok(0);
        }
        let mut expired = self
            .list_reservations_in_txn(txn)?
            .into_iter()
            .filter(|reservation| reservation.expires_at_unix_ms <= cutoff_unix_ms)
            .collect::<Vec<_>>();
        expired.sort_by_key(|reservation| (reservation.expires_at_unix_ms, reservation.ticket.0));
        expired.truncate(max_to_expire);
        for reservation in &expired {
            self.abort_reservation_in_txn(txn, reservation.ticket)?;
        }
        Ok(expired.len())
    }

    fn commit_reserved_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        ticket: WriteTicketId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        let reservation = self
            .reservation_in_txn(txn, ticket)?
            .ok_or(FluxError::Busy)?;
        if reservation.inode != inode.id || reservation.expected_generation != expected_generation {
            return Err(FluxError::InvalidArg(
                "write reservation does not match inode generation".into(),
            ));
        }
        let mut local = manifest
            .extents
            .iter()
            .filter_map(|extent| match extent {
                Extent::Local { chunk, .. } => Some(*chunk),
                Extent::UfsRange { .. } => None,
            })
            .collect::<Vec<_>>();
        local.sort_by_key(ChunkId::to_hex);
        local.dedup();
        if local != reservation.chunks {
            return Err(FluxError::InvalidArg(
                "manifest Local chunks do not match write reservation".into(),
            ));
        }
        let committed =
            self.commit_inode_manifest_in_txn(txn, expected_generation, inode, manifest)?;
        self.abort_reservation_in_txn(txn, ticket)?;
        Ok(committed)
    }

    fn tombstone_gc_batch_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        candidates: &[ChunkId],
    ) -> Result<GcBatch> {
        let mut active_manifests = BTreeSet::new();
        for item in self
            .inodes
            .iter(txn)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            let (_, bytes) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let inode: Inode =
                serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
            if let Some(id) = inode.manifest_id {
                active_manifests.insert(id.0);
            }
        }
        let manifests = self
            .manifests
            .iter(txn)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .map(|item| {
                item.map_err(|e| FluxError::Meta(e.to_string()))
                    .and_then(|(key, bytes)| {
                        Ok((
                            u64_from_bytes(key)?,
                            serde_json::from_slice::<Manifest>(bytes)
                                .map_err(|e| FluxError::Meta(e.to_string()))?,
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut live = BTreeSet::new();
        let mut removed_manifests = 0;
        for (id, manifest) in manifests {
            if active_manifests.contains(&id) {
                for extent in manifest.extents {
                    if let Extent::Local { chunk, .. } = extent {
                        live.insert(chunk);
                    }
                }
            } else {
                self.manifests
                    .delete(txn, &inode_key(id))
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
                removed_manifests += 1;
            }
        }
        for reservation in self.list_reservations_in_txn(txn)? {
            live.extend(reservation.chunks);
        }
        let mut tombstoned = Vec::new();
        let mut unique = candidates.iter().copied().collect::<BTreeSet<_>>();
        for chunk in unique.iter() {
            if live.contains(chunk) {
                continue;
            }
            self.meta
                .put(txn, &tombstone_key(chunk), &[])
                .map_err(|e| FluxError::Meta(e.to_string()))?;
            tombstoned.push(*chunk);
        }
        unique.clear();
        Ok(GcBatch {
            tombstoned_chunks: tombstoned,
            removed_manifests,
        })
    }

    fn finalize_tombstones_in_txn(
        &self,
        txn: &mut heed::RwTxn<'_>,
        chunks: &[ChunkId],
    ) -> Result<()> {
        for chunk in chunks {
            self.meta
                .delete(txn, &tombstone_key(chunk))
                .map_err(|e| FluxError::Meta(e.to_string()))?;
        }
        Ok(())
    }

    fn gc_lease_in_txn(&self, txn: &heed::RoTxn<'_>) -> Result<Option<GcLeaseId>> {
        self.meta
            .get(txn, KEY_GC_LEASE)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .map(u64_from_bytes)
            .transpose()
            .map(|lease| lease.map(GcLeaseId))
    }

    fn gc_plan_in_txn(&self, txn: &heed::RoTxn<'_>, lease_id: GcLeaseId) -> Result<GcPlan> {
        let mut active = BTreeSet::new();
        for item in self
            .inodes
            .iter(txn)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            let (_, bytes) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let inode: Inode =
                serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
            if let Some(id) = inode.manifest_id {
                active.insert(id.0);
            }
        }
        let mut live_chunks = BTreeSet::<ChunkId>::new();
        for id in active {
            let Some(bytes) = self
                .manifests
                .get(txn, &inode_key(id))
                .map_err(|e| FluxError::Meta(e.to_string()))?
            else {
                return Err(FluxError::Meta(format!("active manifest {id} is missing")));
            };
            let manifest: Manifest =
                serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
            for extent in manifest.extents {
                if let Extent::Local { chunk, .. } = extent {
                    live_chunks.insert(chunk);
                }
            }
        }
        Ok(GcPlan {
            lease_id,
            live_chunks: live_chunks.into_iter().collect(),
            removed_manifests: 0,
        })
    }

    fn begin_gc_in_txn(&self, txn: &mut heed::RwTxn<'_>, lease_id: GcLeaseId) -> Result<GcPlan> {
        if let Some(current) = self.gc_lease_in_txn(txn)? {
            return if current == lease_id {
                self.gc_plan_in_txn(txn, lease_id)
            } else {
                Err(FluxError::Busy)
            };
        }
        self.meta
            .put(txn, KEY_GC_LEASE, &u64_bytes(lease_id.0))
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        let mut active = BTreeSet::new();
        for item in self
            .inodes
            .iter(txn)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            let (_, bytes) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let inode: Inode =
                serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
            if let Some(id) = inode.manifest_id {
                active.insert(id.0);
            }
        }
        let all: Vec<u64> = self
            .manifests
            .iter(txn)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .map(|item| {
                item.map_err(|e| FluxError::Meta(e.to_string()))
                    .and_then(|(k, _)| u64_from_bytes(k))
            })
            .collect::<Result<_>>()?;
        let mut removed = 0;
        for id in all {
            if !active.contains(&id) {
                self.manifests
                    .delete(txn, &inode_key(id))
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
                removed += 1;
            }
        }
        let mut plan = self.gc_plan_in_txn(txn, lease_id)?;
        plan.removed_manifests = removed;
        Ok(plan)
    }

    fn finish_gc_in_txn(&self, txn: &mut heed::RwTxn<'_>, lease_id: GcLeaseId) -> Result<()> {
        if self.gc_lease_in_txn(txn)? != Some(lease_id) {
            return Err(FluxError::Busy);
        }
        self.meta
            .delete(txn, KEY_GC_LEASE)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    fn with_write_txn<T>(
        &self,
        operation: impl FnOnce(&Self, &mut heed::RwTxn<'_>) -> Result<T>,
    ) -> Result<T> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        if self.gc_lease_in_txn(&wtxn)?.is_some() {
            return Err(FluxError::Busy);
        }
        let result = operation(self, &mut wtxn)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(result)
    }

    fn alloc_manifest_id(&self, wtxn: &mut heed::RwTxn) -> Result<ManifestId> {
        let raw = self
            .meta
            .get(wtxn, "next_manifest")
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or_else(|| FluxError::Meta("missing next_manifest".into()))?;
        let id = u64_from_bytes(raw)?;
        self.meta
            .put(wtxn, "next_manifest", &u64_bytes(id + 1))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(ManifestId(id))
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

    fn create_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        self.with_write_txn(|store, wtxn| {
            store.create_in_txn(
                wtxn,
                expected_parent_generation,
                parent,
                name,
                file_type,
                mode,
                uid,
                gid,
            )
        })
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
        if self.gc_lease_in_txn(&wtxn)?.is_some() {
            return Err(FluxError::Busy);
        }
        put_inode_raw(&self.inodes, &mut wtxn, inode)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    fn put_manifest(&self, manifest: &Manifest) -> Result<ManifestId> {
        manifest.validate()?;
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        if self.gc_lease_in_txn(&wtxn)?.is_some() {
            return Err(FluxError::Busy);
        }
        let id = self.put_manifest_in_txn(&mut wtxn, manifest)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(id)
    }

    fn commit_inode_manifest_with_id(
        &self,
        op_id: fluxfs_types::RequestOpId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        if manifest_has_local(manifest) {
            return Err(FluxError::InvalidArg(
                "Local manifest commit requires a pre-Put reservation".into(),
            ));
        }
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        if self.gc_lease_in_txn(&wtxn)?.is_some() {
            return Err(FluxError::Busy);
        }
        if !op_id.is_none() {
            if let Some(cached) = self.get_client_request_in_txn(&wtxn, &op_id.to_hex())? {
                return match cached {
                    MetaRaftResponse::Inode(inode) => Ok(*inode),
                    MetaRaftResponse::Err(err) => Err(err),
                    other => Err(FluxError::Meta(format!(
                        "bad retained commit response: {other:?}"
                    ))),
                };
            }
        }
        let next = match self.commit_inode_manifest_in_txn(
            &mut wtxn,
            expected_generation,
            inode,
            manifest,
        ) {
            Ok(inode) => {
                if !op_id.is_none() {
                    self.put_client_request_in_txn(
                        &mut wtxn,
                        &op_id.to_hex(),
                        &MetaRaftResponse::Inode(Box::new(inode.clone())),
                    )?;
                }
                inode
            }
            Err(err) => {
                if !op_id.is_none() {
                    self.put_client_request_in_txn(
                        &mut wtxn,
                        &op_id.to_hex(),
                        &MetaRaftResponse::Err(err.clone()),
                    )?;
                }
                wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
                return Err(err);
            }
        };
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(next)
    }

    fn reserve_chunks(
        &self,
        ticket: WriteTicketId,
        inode: InodeId,
        expected_generation: u64,
        chunks: &[ChunkId],
    ) -> Result<()> {
        self.with_write_txn(|store, txn| {
            store.reserve_chunks_in_txn(
                txn,
                ticket,
                inode,
                expected_generation,
                chunks,
                crate::write_reservation_deadline(),
            )
        })
    }

    fn abort_chunk_reservation(&self, ticket: WriteTicketId) -> Result<()> {
        self.with_write_txn(|store, txn| store.abort_reservation_in_txn(txn, ticket))
    }

    fn expire_chunk_reservations(&self, max_to_expire: usize) -> Result<()> {
        self.with_write_txn(|store, txn| {
            store
                .expire_reservations_in_txn(txn, crate::unix_time_millis(), max_to_expire)
                .map(|_| ())
        })
    }

    fn commit_inode_manifest_reserved_with_id(
        &self,
        _op_id: fluxfs_types::RequestOpId,
        ticket: WriteTicketId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        self.with_write_txn(|store, txn| {
            store.commit_reserved_in_txn(txn, ticket, expected_generation, inode, manifest)
        })
    }

    fn tombstone_gc_batch(&self, candidates: &[ChunkId]) -> Result<GcBatch> {
        self.with_write_txn(|store, txn| store.tombstone_gc_batch_in_txn(txn, candidates))
    }

    fn list_gc_tombstones(&self) -> Result<Vec<ChunkId>> {
        let txn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        self.list_tombstones_in_txn(&txn)
    }

    fn finalize_gc_tombstones(&self, chunks: &[ChunkId]) -> Result<()> {
        self.with_write_txn(|store, txn| store.finalize_tombstones_in_txn(txn, chunks))
    }

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let bytes = self
            .manifests
            .get(&rtxn, &inode_key(id.0))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        drop(rtxn);
        serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))
    }

    fn begin_flush_with_id(
        &self,
        _op_id: fluxfs_types::RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        intent: &FlushIntent,
    ) -> Result<Inode> {
        self.with_write_txn(|this, txn| {
            this.begin_flush_in_txn(txn, expected_generation, inode, intent)
        })
    }

    fn commit_flush_with_id(
        &self,
        _op_id: fluxfs_types::RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        published_ufs: &UfsObject,
    ) -> Result<Inode> {
        self.with_write_txn(|this, txn| {
            this.commit_flush_in_txn(txn, expected_generation, inode, flush_id, published_ufs)
        })
    }

    fn fail_flush_conflict(
        &self,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        error: &str,
    ) -> Result<Inode> {
        self.with_write_txn(|this, txn| {
            this.fail_flush_conflict_in_txn(txn, expected_generation, inode, flush_id, error)
        })
    }

    fn list_flush_intents(&self) -> Result<Vec<(InodeId, FlushIntent)>> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let mut result = Vec::new();
        for item in self
            .inodes
            .iter(&rtxn)
            .map_err(|e| FluxError::Meta(e.to_string()))?
        {
            let (_, bytes) = item.map_err(|e| FluxError::Meta(e.to_string()))?;
            let inode: Inode =
                serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
            if let Some(intent) = inode.flush_intent {
                result.push((inode.id, intent));
            }
        }
        result.sort_by_key(|(inode, _)| *inode);
        Ok(result)
    }

    fn begin_gc(&self, lease_id: GcLeaseId) -> Result<GcPlan> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut txn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let plan = self.begin_gc_in_txn(&mut txn, lease_id)?;
        txn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(plan)
    }

    fn current_gc_plan(&self) -> Result<Option<GcPlan>> {
        let txn = self
            .env
            .read_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let Some(lease) = self.gc_lease_in_txn(&txn)? else {
            return Ok(None);
        };
        self.gc_plan_in_txn(&txn, lease).map(Some)
    }

    fn finish_gc(&self, lease_id: GcLeaseId) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut txn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        self.finish_gc_in_txn(&mut txn, lease_id)?;
        txn.commit().map_err(|e| FluxError::Meta(e.to_string()))
    }

    fn import_external_with_id(
        &self,
        op_id: fluxfs_types::RequestOpId,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        inode: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        if self.gc_lease_in_txn(&wtxn)?.is_some() {
            return Err(FluxError::Busy);
        }
        if !op_id.is_none() {
            if let Some(cached) = self.get_client_request_in_txn(&wtxn, &op_id.to_hex())? {
                return match cached {
                    MetaRaftResponse::Inode(inode) => Ok(*inode),
                    MetaRaftResponse::Err(err) => Err(err),
                    other => Err(FluxError::Meta(format!(
                        "bad retained import response: {other:?}"
                    ))),
                };
            }
        }
        let next = match self.import_external_in_txn(
            &mut wtxn,
            expected_parent_generation,
            parent,
            name,
            inode,
            manifest,
        ) {
            Ok(inode) => {
                if !op_id.is_none() {
                    self.put_client_request_in_txn(
                        &mut wtxn,
                        &op_id.to_hex(),
                        &MetaRaftResponse::Inode(Box::new(inode.clone())),
                    )?;
                }
                inode
            }
            Err(err) => {
                if !op_id.is_none() {
                    self.put_client_request_in_txn(
                        &mut wtxn,
                        &op_id.to_hex(),
                        &MetaRaftResponse::Err(err.clone()),
                    )?;
                }
                wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
                return Err(err);
            }
        };
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(next)
    }

    fn unlink_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
    ) -> Result<()> {
        self.with_write_txn(|store, wtxn| {
            store.unlink_in_txn(wtxn, expected_parent_generation, parent, name)
        })
    }
}

fn get_inode_raw(inodes: &InodeDb, txn: &heed::RwTxn<'_>, id: InodeId) -> Result<Inode> {
    let bytes = inodes
        .get(txn, &inode_key(id))
        .map_err(|e| FluxError::Meta(e.to_string()))?
        .ok_or(FluxError::NotFound)?;
    serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))
}

fn check_generation(inode: &Inode, expected: u64) -> Result<()> {
    if inode.generation != expected {
        return Err(FluxError::CasFailed {
            expected,
            actual: inode.generation,
        });
    }
    Ok(())
}

fn matching_flush_intent(inode: &Inode, flush_id: FlushId) -> Result<&FlushIntent> {
    match inode.flush_intent.as_ref() {
        Some(intent) if intent.flush_id == flush_id => Ok(intent),
        _ => Err(FluxError::Busy),
    }
}

fn manifest_has_local(manifest: &Manifest) -> bool {
    manifest
        .extents
        .iter()
        .any(|extent| matches!(extent, Extent::Local { .. }))
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

fn reservation_key(ticket: WriteTicketId) -> String {
    format!("{RESERVATION_PREFIX}{:016x}", ticket.0)
}

fn tombstone_key(chunk: &ChunkId) -> String {
    format!("{TOMBSTONE_PREFIX}{}", chunk.to_hex())
}

fn chunk_from_hex(value: &str) -> Result<ChunkId> {
    if value.len() != 64 {
        return Err(FluxError::Meta("bad tombstone chunk id".into()));
    }
    let mut raw = [0u8; 32];
    for (index, byte) in raw.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
    }
    Ok(ChunkId::from_raw(raw))
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
    use fluxfs_types::RequestOpId;

    #[test]
    fn create_lookup_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let f = store
            .create(
                ROOT_INODE,
                "hello.txt",
                FileType::Regular,
                0o644,
                1000,
                1000,
            )
            .unwrap();
        let got = store.lookup(ROOT_INODE, "hello.txt").unwrap();
        assert_eq!(got.id, f.id);
        assert_eq!(got.file_type, FileType::Regular);
        let entries = store.readdir(ROOT_INODE).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello.txt");
    }

    #[test]
    fn commit_inode_manifest_cas_and_atomicity() {
        use fluxfs_types::{DataGen, Manifest};

        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let file = store
            .create(ROOT_INODE, "a.bin", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        let base_gen = file.generation;

        let mut next = file.clone();
        next.size = 4;
        next.generation = base_gen.saturating_add(1);
        next.head_gen = DataGen(1);
        let manifest = Manifest {
            inode: file.id,
            gen: DataGen(1),
            size: 4,
            extents: Vec::new(),
        };
        let committed = store
            .commit_inode_manifest(base_gen, &next, &manifest)
            .expect("first commit");
        assert_eq!(committed.generation, base_gen + 1);
        let mid = committed.manifest_id.expect("manifest id");
        assert_eq!(store.get_manifest(mid).unwrap().size, 4);

        let stale = store.commit_inode_manifest(base_gen, &next, &manifest);
        assert_eq!(
            stale.unwrap_err(),
            FluxError::CasFailed {
                expected: base_gen,
                actual: base_gen + 1
            }
        );
        // Head must remain the successful commit.
        assert_eq!(store.get_inode(file.id).unwrap().generation, base_gen + 1);
        assert_eq!(store.get_inode(file.id).unwrap().manifest_id, Some(mid));
    }

    #[test]
    fn flush_intent_blocks_writes_and_commits_clean_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let mut file = store
            .create(ROOT_INODE, "dirty.bin", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        file.size = 5;
        file.head_gen = DataGen(2);
        file.ufs_gen = DataGen(1);
        file.locality_fields = Some(LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::Dirty,
            op_state: OpState::None,
            origin: Origin::Imported,
        });
        file.locality = LocalityLabel::Dirty;
        store.put_inode(&file).unwrap();

        let intent = FlushIntent {
            flush_id: FlushId(7),
            snapshot_gen: file.head_gen,
            snapshot_manifest_root: fluxfs_types::ChunkId::from_bytes(b"manifest"),
            expected_ufs_version: Some(UfsVersion("old-etag".into())),
            target_digest: fluxfs_types::ChunkId::from_bytes(b"hello"),
        };
        let flushing = store
            .begin_flush(file.generation, file.id, &intent)
            .unwrap();
        assert_eq!(flushing.generation, file.generation + 1);
        assert_eq!(
            store.list_flush_intents().unwrap(),
            vec![(file.id, intent.clone())]
        );

        let mut attempted_write = flushing.clone();
        attempted_write.generation += 1;
        let attempted_manifest = Manifest {
            inode: file.id,
            gen: DataGen(3),
            size: 5,
            extents: Vec::new(),
        };
        assert_eq!(
            store
                .commit_inode_manifest(flushing.generation, &attempted_write, &attempted_manifest)
                .unwrap_err(),
            FluxError::Busy
        );

        let published = UfsObject {
            key: "dirty.bin".into(),
            size: 5,
            etag: Some("new-etag".into()),
            mtime_ms: Some(42),
        };
        let clean = store
            .commit_flush(flushing.generation, file.id, intent.flush_id, &published)
            .unwrap();
        assert_eq!(clean.locality, LocalityLabel::External);
        assert_eq!(clean.ufs_gen, clean.head_gen);
        assert!(clean.flush_intent.is_none());
        assert!(store.list_flush_intents().unwrap().is_empty());
        let manifest = store.get_manifest(clean.manifest_id.unwrap()).unwrap();
        assert!(matches!(
            manifest.extents.as_slice(),
            [Extent::UfsRange { ufs_version, .. }] if ufs_version == &UfsVersion("new-etag".into())
        ));
    }

    #[test]
    fn gc_lease_persists_blocks_mutations_and_reclaims_only_unreachable() {
        let dir = tempfile::tempdir().unwrap();
        let lease = GcLeaseId(42);
        let live = ChunkId::from_bytes(b"live");
        let orphan = ChunkId::from_bytes(b"orphan");
        let active_mid;
        let orphan_mid;
        {
            let store = HeedMetaStore::open(dir.path()).unwrap();
            let file = store
                .create(ROOT_INODE, "active", FileType::Regular, 0o644, 0, 0)
                .unwrap();
            let mut next = file.clone();
            next.size = 4;
            next.generation += 1;
            next.head_gen = DataGen(2);
            let active = Manifest {
                inode: file.id,
                gen: DataGen(2),
                size: 4,
                extents: vec![Extent::Local {
                    offset: 0,
                    len: 4,
                    chunk: live,
                }],
            };
            let ticket = WriteTicketId(7);
            store
                .reserve_chunks(ticket, file.id, file.generation, &[live])
                .unwrap();
            let committed = store
                .commit_inode_manifest_reserved_with_id(
                    RequestOpId::random(),
                    ticket,
                    file.generation,
                    &next,
                    &active,
                )
                .unwrap();
            active_mid = committed.manifest_id.unwrap();
            orphan_mid = store
                .put_manifest(&Manifest {
                    inode: file.id,
                    gen: DataGen(1),
                    size: 6,
                    extents: vec![Extent::Local {
                        offset: 0,
                        len: 6,
                        chunk: orphan,
                    }],
                })
                .unwrap();

            let plan = store.begin_gc(lease).unwrap();
            assert_eq!(plan.live_chunks, vec![live]);
            assert_eq!(plan.removed_manifests, 1);
            assert_eq!(store.get_manifest(active_mid).unwrap(), active);
            assert_eq!(store.get_manifest(orphan_mid), Err(FluxError::NotFound));
            assert!(matches!(
                store.create(ROOT_INODE, "blocked", FileType::Regular, 0o644, 0, 0),
                Err(FluxError::Busy)
            ));

            let snapshot = store.export_snapshot(&SmAppliedMeta::default()).unwrap();
            let restored_dir = tempfile::tempdir().unwrap();
            let restored = HeedMetaStore::open(restored_dir.path()).unwrap();
            restored.install_snapshot_data(&snapshot).unwrap();
            let restored_plan = restored.current_gc_plan().unwrap().unwrap();
            assert_eq!(restored_plan.lease_id, lease);
            assert_eq!(restored_plan.live_chunks, vec![live]);
        }

        let store = HeedMetaStore::open(dir.path()).unwrap();
        let resumed = store.current_gc_plan().unwrap().unwrap();
        assert_eq!(resumed.lease_id, lease);
        assert_eq!(resumed.live_chunks, vec![live]);
        assert_eq!(resumed.removed_manifests, 0);
        assert_eq!(store.finish_gc(GcLeaseId(99)), Err(FluxError::Busy));
        store.finish_gc(lease).unwrap();
        assert!(store.current_gc_plan().unwrap().is_none());
        store
            .create(ROOT_INODE, "unblocked", FileType::Regular, 0o644, 0, 0)
            .unwrap();
    }

    #[test]
    fn reservations_and_tombstones_fence_concurrent_gc() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let file = store
            .create(ROOT_INODE, "race", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        let reserved = ChunkId::from_bytes(b"reserved-before-put");
        let orphan = ChunkId::from_bytes(b"orphan-candidate");
        let ticket = WriteTicketId(100);
        store
            .reserve_chunks(ticket, file.id, file.generation, &[reserved])
            .unwrap();

        let batch = store.tombstone_gc_batch(&[reserved, orphan]).unwrap();
        assert_eq!(batch.tombstoned_chunks, vec![orphan]);
        assert_eq!(store.list_gc_tombstones().unwrap(), vec![orphan]);
        assert_eq!(
            store.reserve_chunks(WriteTicketId(101), file.id, file.generation, &[orphan]),
            Err(FluxError::Busy)
        );

        let snapshot = store.export_snapshot(&SmAppliedMeta::default()).unwrap();
        let restored_dir = tempfile::tempdir().unwrap();
        let restored = HeedMetaStore::open(restored_dir.path()).unwrap();
        restored.install_snapshot_data(&snapshot).unwrap();
        assert_eq!(restored.list_gc_tombstones().unwrap(), vec![orphan]);
        assert!(restored
            .reservation_in_txn(&restored.env.read_txn().unwrap(), ticket)
            .unwrap()
            .is_some());

        store.finalize_gc_tombstones(&[orphan]).unwrap();
        store
            .reserve_chunks(WriteTicketId(101), file.id, file.generation, &[orphan])
            .unwrap();
        store.abort_chunk_reservation(WriteTicketId(101)).unwrap();

        let mut next = file.clone();
        next.generation += 1;
        next.head_gen = DataGen(2);
        next.size = 19;
        let manifest = Manifest {
            inode: file.id,
            gen: DataGen(2),
            size: 19,
            extents: vec![Extent::Local {
                offset: 0,
                len: 19,
                chunk: reserved,
            }],
        };
        store
            .commit_inode_manifest_reserved_with_id(
                RequestOpId::random(),
                ticket,
                file.generation,
                &next,
                &manifest,
            )
            .unwrap();
        assert!(store
            .tombstone_gc_batch(&[reserved])
            .unwrap()
            .tombstoned_chunks
            .is_empty());
    }

    #[test]
    fn abandoned_reservations_expire_deterministically_and_fence_late_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let file = store
            .create(ROOT_INODE, "expiry", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        let first = ChunkId::from_bytes(b"expires-first");
        let second = ChunkId::from_bytes(b"expires-second");
        let first_ticket = WriteTicketId(200);
        let second_ticket = WriteTicketId(201);
        for (ticket, chunk) in [(first_ticket, first), (second_ticket, second)] {
            store
                .with_write_txn(|store, txn| {
                    store.reserve_chunks_in_txn(txn, ticket, file.id, file.generation, &[chunk], 10)
                })
                .unwrap();
        }

        store
            .with_write_txn(|store, txn| {
                assert_eq!(store.expire_reservations_in_txn(txn, 9, 10)?, 0);
                Ok(())
            })
            .unwrap();
        assert!(store
            .tombstone_gc_batch(&[first, second])
            .unwrap()
            .tombstoned_chunks
            .is_empty());

        store
            .with_write_txn(|store, txn| {
                assert_eq!(store.expire_reservations_in_txn(txn, 10, 1)?, 1);
                Ok(())
            })
            .unwrap();
        assert!(store
            .reservation_in_txn(&store.env.read_txn().unwrap(), first_ticket)
            .unwrap()
            .is_none());
        assert!(store
            .reservation_in_txn(&store.env.read_txn().unwrap(), second_ticket)
            .unwrap()
            .is_some());
        assert_eq!(
            store
                .tombstone_gc_batch(&[first, second])
                .unwrap()
                .tombstoned_chunks,
            vec![first]
        );

        let snapshot = store.export_snapshot(&SmAppliedMeta::default()).unwrap();
        let restored_dir = tempfile::tempdir().unwrap();
        let restored = HeedMetaStore::open(restored_dir.path()).unwrap();
        restored.install_snapshot_data(&snapshot).unwrap();
        let restored_reservation = restored
            .reservation_in_txn(&restored.env.read_txn().unwrap(), second_ticket)
            .unwrap()
            .unwrap();
        assert_eq!(restored_reservation.expires_at_unix_ms, 10);
        restored
            .with_write_txn(|store, txn| {
                assert_eq!(store.expire_reservations_in_txn(txn, 10, 10)?, 1);
                Ok(())
            })
            .unwrap();

        let mut next = file.clone();
        next.generation += 1;
        next.head_gen = DataGen(2);
        next.size = 1;
        let manifest = Manifest {
            inode: file.id,
            gen: DataGen(2),
            size: 1,
            extents: vec![Extent::Local {
                offset: 0,
                len: 1,
                chunk: second,
            }],
        };
        assert!(matches!(
            restored.commit_inode_manifest_reserved_with_id(
                RequestOpId::random(),
                second_ticket,
                file.generation,
                &next,
                &manifest,
            ),
            Err(FluxError::Busy)
        ));
        assert_eq!(
            restored
                .tombstone_gc_batch(&[second])
                .unwrap()
                .tombstoned_chunks,
            vec![second]
        );
    }

    #[test]
    fn import_external_atomic_file_and_dir() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let now = now_ms();
        let version = UfsVersion("etag-1".into());
        let template = Inode {
            id: 0,
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: 12,
            mtime_ms: now,
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
                key: "obj.bin".into(),
                size: 12,
                etag: Some("etag-1".into()),
                mtime_ms: Some(now),
            }),
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let manifest = Manifest {
            inode: 0,
            gen: DataGen(0),
            size: 12,
            extents: vec![Extent::UfsRange {
                offset: 0,
                len: 12,
                ufs_key: "obj.bin".into(),
                ufs_version: version,
                offset_in_object: 0,
            }],
        };
        let file = store
            .import_external(ROOT_INODE, "obj.bin", &template, Some(&manifest))
            .unwrap();
        assert_ne!(file.id, 0);
        assert_eq!(file.locality, LocalityLabel::External);
        assert_eq!(file.size, 12);
        let mid = file.manifest_id.expect("manifest");
        let stored = store.get_manifest(mid).unwrap();
        assert_eq!(stored.inode, file.id);
        assert_eq!(store.lookup(ROOT_INODE, "obj.bin").unwrap().id, file.id);

        let dir_template = Inode {
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
                key: "subdir/".into(),
                size: 0,
                etag: None,
                mtime_ms: Some(now),
            }),
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let d = store
            .import_external(ROOT_INODE, "subdir", &dir_template, None)
            .unwrap();
        assert_eq!(d.file_type, FileType::Directory);
        assert!(d.manifest_id.is_none());
        assert!(matches!(
            store
                .import_external(ROOT_INODE, "obj.bin", &template, Some(&manifest))
                .unwrap_err(),
            FluxError::AlreadyExists
        ));
    }

    #[test]
    fn import_external_request_id_retry_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let now = now_ms();
        let op = RequestOpId::random();
        let template = Inode {
            id: 0,
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: 0,
            mtime_ms: now,
            ctime_ms: now,
            atime_ms: now,
            link_count: 1,
            generation: 1,
            head_gen: DataGen(0),
            ufs_gen: DataGen(0),
            ufs_base_version: Some(UfsVersion("empty".into())),
            locality: LocalityLabel::External,
            locality_fields: Some(LocalityFields {
                backing_mode: BackingMode::UfsBacked,
                data_state: DataState::UfsClean,
                op_state: OpState::None,
                origin: Origin::Imported,
            }),
            ufs: Some(UfsObject {
                key: "empty.txt".into(),
                size: 0,
                etag: None,
                mtime_ms: Some(now),
            }),
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let first = store
            .import_external_with_id(op, None, ROOT_INODE, "empty.txt", &template, None)
            .unwrap();
        let second = store
            .import_external_with_id(op, None, ROOT_INODE, "empty.txt", &template, None)
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(store.readdir(ROOT_INODE).unwrap().len(), 1);
    }

    #[test]
    fn directory_generation_cas_guards_create_and_unlink() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let root_gen = store.get_inode(ROOT_INODE).unwrap().generation;
        assert_eq!(root_gen, 1);

        store
            .create_cas(
                Some(root_gen),
                ROOT_INODE,
                "a.txt",
                FileType::Regular,
                0o644,
                0,
                0,
            )
            .unwrap();
        let after_create = store.get_inode(ROOT_INODE).unwrap().generation;
        assert_eq!(after_create, root_gen + 1);

        // Stale parent generation must fail without creating a second name.
        assert_eq!(
            store
                .create_cas(
                    Some(root_gen),
                    ROOT_INODE,
                    "b.txt",
                    FileType::Regular,
                    0o644,
                    0,
                    0,
                )
                .unwrap_err(),
            FluxError::CasFailed {
                expected: root_gen,
                actual: after_create
            }
        );
        assert!(store.lookup(ROOT_INODE, "b.txt").is_err());
        assert_eq!(
            store.get_inode(ROOT_INODE).unwrap().generation,
            after_create
        );

        store
            .create_cas(
                Some(after_create),
                ROOT_INODE,
                "b.txt",
                FileType::Regular,
                0o644,
                0,
                0,
            )
            .unwrap();
        let after_b = store.get_inode(ROOT_INODE).unwrap().generation;

        store
            .unlink_cas(Some(after_b), ROOT_INODE, "a.txt")
            .unwrap();
        let after_unlink = store.get_inode(ROOT_INODE).unwrap().generation;
        assert_eq!(after_unlink, after_b + 1);
        assert_eq!(
            store
                .unlink_cas(Some(after_b), ROOT_INODE, "b.txt")
                .unwrap_err(),
            FluxError::CasFailed {
                expected: after_b,
                actual: after_unlink
            }
        );
        // Compatible path without CAS still bumps generation.
        store.unlink(ROOT_INODE, "b.txt").unwrap();
        assert_eq!(
            store.get_inode(ROOT_INODE).unwrap().generation,
            after_unlink + 1
        );
    }

    #[test]
    fn unlink_drops_inode_so_gc_can_reclaim_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeedMetaStore::open(dir.path()).unwrap();
        let file = store
            .create(ROOT_INODE, "victim.bin", FileType::Regular, 0o644, 0, 0)
            .unwrap();
        let chunk = ChunkId::from_bytes(b"victim-chunk");
        let mut next = file.clone();
        next.size = 4;
        next.generation += 1;
        next.head_gen = DataGen(1);
        let manifest = Manifest {
            inode: file.id,
            gen: DataGen(1),
            size: 4,
            extents: vec![Extent::Local {
                offset: 0,
                len: 4,
                chunk,
            }],
        };
        let ticket = WriteTicketId(11);
        store
            .reserve_chunks(ticket, file.id, file.generation, &[chunk])
            .unwrap();
        let committed = store
            .commit_inode_manifest_reserved_with_id(
                RequestOpId::random(),
                ticket,
                file.generation,
                &next,
                &manifest,
            )
            .unwrap();
        let mid = committed.manifest_id.expect("manifest");

        store.unlink(ROOT_INODE, "victim.bin").unwrap();
        assert!(matches!(store.get_inode(file.id), Err(FluxError::NotFound)));
        assert!(matches!(
            store.lookup(ROOT_INODE, "victim.bin"),
            Err(FluxError::NotFound)
        ));

        // Manifest is no longer referenced by any inode head → GC removes it and
        // tombstones the chunk.
        let batch = store.tombstone_gc_batch(&[chunk]).unwrap();
        assert!(batch.removed_manifests >= 1);
        assert_eq!(batch.tombstoned_chunks, vec![chunk]);
        assert_eq!(store.get_manifest(mid), Err(FluxError::NotFound));
    }
}
