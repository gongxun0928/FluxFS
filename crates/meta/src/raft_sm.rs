//! Raft state machine over [`RocksMetaStore`].

#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::BasicNode;
use openraft::{
    Entry, EntryPayload, LogId, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use tokio::sync::RwLock;

use crate::raft_types::{FluxRaftTypeConfig, MetaRaftResponse, NodeId, SmAppliedMeta};
use crate::rocks_store::RocksMetaStore;

#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

pub struct MetaStateMachine {
    store: Arc<RocksMetaStore>,
    meta: RwLock<SmAppliedMeta>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl MetaStateMachine {
    pub fn new(store: Arc<RocksMetaStore>) -> Result<Arc<Self>, StorageError<NodeId>> {
        let sm_meta = store.load_sm_meta().map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::read_state_machine(
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        Ok(Arc::new(Self {
            store,
            meta: RwLock::new(sm_meta),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
        }))
    }
}

impl RaftSnapshotBuilder<FluxRaftTypeConfig> for Arc<MetaStateMachine> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<FluxRaftTypeConfig>, StorageError<NodeId>> {
        let sm = self.meta.read().await;
        let snap = self.store.export_snapshot(&sm).map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::read_state_machine(
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        let data = serde_json::to_vec(&snap).map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::read_state_machine(
                &std::io::Error::other(e.to_string()),
            ))
        })?;

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
                EntryPayload::Blank => {
                    self.store.save_sm_meta_only(&sm).map_err(|e| {
                        StorageError::from(StorageIOError::<NodeId>::write_state_machine(
                            &std::io::Error::other(e.to_string()),
                        ))
                    })?;
                    res.push(MetaRaftResponse::Empty);
                }
                EntryPayload::Normal(ref req) => {
                    let resp = self.store.apply_raft_request(req, &sm).map_err(|e| {
                        StorageError::from(StorageIOError::<NodeId>::write_state_machine(
                            &std::io::Error::other(e.to_string()),
                        ))
                    })?;
                    res.push(resp);
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    self.store.save_sm_meta_only(&sm).map_err(|e| {
                        StorageError::from(StorageIOError::<NodeId>::write_state_machine(
                            &std::io::Error::other(e.to_string()),
                        ))
                    })?;
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
        let data = snapshot.into_inner();
        let snap: crate::rocks_store::MetaSnapshotData =
            serde_json::from_slice(&data).map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::read_snapshot(
                    Some(meta.signature()),
                    &e,
                ))
            })?;
        self.store.install_snapshot_data(&snap).map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                Some(meta.signature()),
                &std::io::Error::other(e.to_string()),
            ))
        })?;

        let mut sm = self.meta.write().await;
        *sm = snap.sm;
        sm.last_applied_log = meta.last_log_id.or(sm.last_applied_log);
        sm.last_membership = meta.last_membership.clone();
        self.store.save_sm_meta_only(&sm).map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::write_state_machine(
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        drop(sm);

        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
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
