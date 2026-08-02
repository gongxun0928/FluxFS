use crate::raft_types::{MetaRaftRequest, MetaRaftResponse, SmAppliedMeta};
use crate::store::MetaStore;
use fluxfs_types::{
    BackingMode, DataGen, DataState, Dentry, FileType, FluxError, Inode, InodeId, LocalityFields,
    LocalityLabel, Manifest, ManifestId, OpState, Origin, Result, ROOT_INODE,
};
use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};
use openraft::BasicNode;
use openraft::StoredMembership;
use serde::{Deserialize, Serialize};
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

        if let Some(op_id) = req.request_id() {
            if let Some(cached) = self.get_client_request_in_txn(&wtxn, op_id.as_str())? {
                // Still advance SM markers so the Raft log entry is durable.
                self.put_sm_meta_raw(&mut wtxn, sm)?;
                wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
                return Ok(cached);
            }
        }

        let resp = match req {
            MetaRaftRequest::Create {
                parent,
                name,
                file_type,
                mode,
                uid,
                gid,
                ..
            } => {
                match self.create_in_txn(&mut wtxn, *parent, name, *file_type, *mode, *uid, *gid) {
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
            MetaRaftRequest::Unlink { parent, name, .. } => {
                match self.unlink_in_txn(&mut wtxn, *parent, name) {
                    Ok(()) => MetaRaftResponse::Empty,
                    Err(e) => MetaRaftResponse::Err(e),
                }
            }
        };

        if let Some(op_id) = req.request_id() {
            // Retain successes and typed application errors so retries are stable.
            self.put_client_request_in_txn(&mut wtxn, op_id.as_str(), &resp)?;
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
        drop(rtxn);
        Ok(MetaSnapshotData {
            inodes,
            dentries,
            manifests,
            next_inode,
            next_manifest,
            sm: sm.clone(),
            client_requests,
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
        self.put_sm_meta_raw(&mut wtxn, &snap.sm)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn create_in_txn(
        &self,
        wtxn: &mut heed::RwTxn,
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
        if self
            .dentries
            .get(wtxn, &dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_some()
        {
            return Err(FluxError::AlreadyExists);
        }
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
        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        put_inode_raw(&self.inodes, wtxn, &parent_ino)?;
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
        let mid = self.put_manifest_in_txn(wtxn, manifest)?;
        let mut next = inode.clone();
        next.manifest_id = Some(mid);
        put_inode_raw(&self.inodes, wtxn, &next)?;
        Ok(next)
    }

    fn unlink_in_txn(&self, wtxn: &mut heed::RwTxn, parent: InodeId, name: &str) -> Result<()> {
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
        let key = dentry_key(parent, name);
        if self
            .dentries
            .get(wtxn, &key)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_none()
        {
            return Err(FluxError::NotFound);
        }
        self.dentries
            .delete(wtxn, &key)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let now = now_ms();
        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        put_inode_raw(&self.inodes, wtxn, &parent_ino)?;
        Ok(())
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

    fn create(
        &self,
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
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;

        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        let parent_bytes = self
            .inodes
            .get(&wtxn, &inode_key(parent))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let mut parent_ino: Inode =
            serde_json::from_slice(&parent_bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }

        if self
            .dentries
            .get(&wtxn, &dentry_key(parent, name))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_some()
        {
            return Err(FluxError::AlreadyExists);
        }

        let id = self.alloc_inode(&mut wtxn)?;
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
        put_inode_raw(&self.inodes, &mut wtxn, &inode)?;
        self.dentries
            .put(&mut wtxn, &dentry_key(parent, name), &u64_bytes(id))
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        put_inode_raw(&self.inodes, &mut wtxn, &parent_ino)?;

        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(inode)
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
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        if let Some(cached) = self.get_client_request_in_txn(&wtxn, op_id.as_str())? {
            return match cached {
                MetaRaftResponse::Inode(inode) => Ok(*inode),
                MetaRaftResponse::Err(err) => Err(err),
                other => Err(FluxError::Meta(format!(
                    "bad retained commit response: {other:?}"
                ))),
            };
        }
        let next =
            match self.commit_inode_manifest_in_txn(&mut wtxn, expected_generation, inode, manifest)
            {
                Ok(inode) => {
                    self.put_client_request_in_txn(
                        &mut wtxn,
                        op_id.as_str(),
                        &MetaRaftResponse::Inode(Box::new(inode.clone())),
                    )?;
                    inode
                }
                Err(err) => {
                    self.put_client_request_in_txn(
                        &mut wtxn,
                        op_id.as_str(),
                        &MetaRaftResponse::Err(err.clone()),
                    )?;
                    wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
                    return Err(err);
                }
            };
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(next)
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

    fn unlink(&self, parent: InodeId, name: &str) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let parent_bytes = self
            .inodes
            .get(&wtxn, &inode_key(parent))
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .ok_or(FluxError::NotFound)?
            .to_vec();
        let mut parent_ino: Inode =
            serde_json::from_slice(&parent_bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
        if parent_ino.file_type != FileType::Directory {
            return Err(FluxError::NotDirectory);
        }
        let key = dentry_key(parent, name);
        if self
            .dentries
            .get(&wtxn, &key)
            .map_err(|e| FluxError::Meta(e.to_string()))?
            .is_none()
        {
            return Err(FluxError::NotFound);
        }
        self.dentries
            .delete(&mut wtxn, &key)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let now = now_ms();
        parent_ino.mtime_ms = now;
        parent_ino.ctime_ms = now;
        put_inode_raw(&self.inodes, &mut wtxn, &parent_ino)?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }
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
}
