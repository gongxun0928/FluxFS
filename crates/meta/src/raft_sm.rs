//! Raft state machine: apply meta mutations into [`HeedMetaStore`].

use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{Entry, EntryPayload, LogId, SnapshotMeta, StorageError, StoredMembership};
use tokio::sync::RwLock;

use crate::heed_store::HeedMetaStore;
use crate::raft_types::{FluxRaftTypeConfig, MetaRaftRequest, MetaRaftResponse, NodeId};
use crate::store::MetaStore;
use openraft::BasicNode;

#[derive(Debug, Default, Clone)]
struct SmMeta {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// State machine backed by durable Heed for inode/dentry/manifest.
pub struct MetaStateMachine {
    store: Arc<HeedMetaStore>,
    meta: RwLock<SmMeta>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl MetaStateMachine {
    pub fn new(store: Arc<HeedMetaStore>) -> Arc<Self> {
        Arc::new(Self {
            store,
            meta: RwLock::new(SmMeta::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
        })
    }

    fn apply_request(&self, req: &MetaRaftRequest) -> MetaRaftResponse {
        match req {
            MetaRaftRequest::Create {
                parent,
                name,
                file_type,
                mode,
                uid,
                gid,
            } => match self
                .store
                .create(*parent, name, *file_type, *mode, *uid, *gid)
            {
                Ok(inode) => MetaRaftResponse::Inode(Box::new(inode)),
                Err(e) => MetaRaftResponse::Err(e),
            },
            MetaRaftRequest::PutInode { inode } => match self.store.put_inode(inode.as_ref()) {
                Ok(()) => MetaRaftResponse::Empty,
                Err(e) => MetaRaftResponse::Err(e),
            },
            MetaRaftRequest::PutManifest { manifest } => {
                match self.store.put_manifest(manifest.as_ref()) {
                    Ok(id) => MetaRaftResponse::ManifestId(id.0),
                    Err(e) => MetaRaftResponse::Err(e),
                }
            }
            MetaRaftRequest::Unlink { parent, name } => match self.store.unlink(*parent, name) {
                Ok(()) => MetaRaftResponse::Empty,
                Err(e) => MetaRaftResponse::Err(e),
            },
        }
    }
}

impl RaftSnapshotBuilder<FluxRaftTypeConfig> for Arc<MetaStateMachine> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<FluxRaftTypeConfig>, StorageError<NodeId>> {
        let sm = self.meta.read().await;
        // Heed is SoT for app data; snapshot payload is a marker only.
        let data = b"fluxfs-meta-snapshot-v1".to_vec();

        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(sm);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        *current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<FluxRaftTypeConfig> for Arc<MetaStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.meta.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetaRaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<FluxRaftTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut res = Vec::new();
        let mut sm = self.meta.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(MetaRaftResponse::Empty),
                EntryPayload::Normal(ref req) => res.push(self.apply_request(req)),
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(MetaRaftResponse::Empty);
                }
            }
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        // App state stays in Heed; only refresh Raft markers for single-voter.
        let mut sm = self.meta.write().await;
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();
        drop(sm);

        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<FluxRaftTypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(Cursor::new(snapshot.data.clone())),
            })),
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
