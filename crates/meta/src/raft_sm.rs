//! Raft state machine: apply meta mutations into [`HeedMetaStore`].

#![allow(clippy::result_large_err)] // openraft StorageError is large by design

use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::BasicNode;
use openraft::{
    Entry, EntryPayload, LogId, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::heed_store::HeedMetaStore;
use crate::raft_types::{FluxRaftTypeConfig, MetaRaftResponse, NodeId, SmAppliedMeta};

#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    /// On-disk streaming snapshot (not held as a full `Vec<u8>`).
    path: PathBuf,
}

/// State machine backed by durable Heed for inode/dentry/manifest.
///
/// Normal applies persist mutation + `last_applied` in one MetaStore write txn.
pub struct MetaStateMachine {
    store: Arc<HeedMetaStore>,
    meta: RwLock<SmAppliedMeta>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    snapshot_dir: PathBuf,
}

impl MetaStateMachine {
    pub fn new(store: Arc<HeedMetaStore>) -> Result<Arc<Self>, StorageError<NodeId>> {
        let sm_meta = store.load_sm_meta().map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::read_state_machine(
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        let snapshot_dir = store.snapshot_dir();
        std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                None,
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        Ok(Arc::new(Self {
            store,
            meta: RwLock::new(sm_meta),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            snapshot_dir,
        }))
    }

    async fn open_snapshot_file(
        path: &std::path::Path,
    ) -> Result<tokio::fs::File, StorageError<NodeId>> {
        let mut file = tokio::fs::File::open(path).await.map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::read_snapshot(
                None,
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        file.seek(SeekFrom::Start(0)).await.map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::read_snapshot(
                None,
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        Ok(file)
    }
}

impl RaftSnapshotBuilder<FluxRaftTypeConfig> for Arc<MetaStateMachine> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<FluxRaftTypeConfig>, StorageError<NodeId>> {
        let sm = self.meta.read().await;
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = sm.last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };
        let path = self.snapshot_dir.join(format!("{snapshot_id}.snap"));
        self.store
            .export_snapshot_to_path(&sm, &path)
            .map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::read_state_machine(
                    &std::io::Error::other(e.to_string()),
                ))
            })?;

        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        drop(sm);

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            if let Some(prev) = current_snapshot.take() {
                let _ = std::fs::remove_file(prev.path);
            }
            *current_snapshot = Some(StoredSnapshot {
                meta: meta.clone(),
                path: path.clone(),
            });
        }

        let file = MetaStateMachine::open_snapshot_file(&path).await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(file),
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
    ) -> Result<Box<tokio::fs::File>, StorageError<NodeId>> {
        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let path = self.snapshot_dir.join(format!("incoming-{idx}.snap"));
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                    None,
                    &std::io::Error::other(e.to_string()),
                ))
            })?;
        // Keep path discoverable: store as xattr via sidecar name pattern is enough;
        // install_snapshot receives the File handle directly.
        let _ = path;
        Ok(Box::new(file))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        mut snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<NodeId>> {
        snapshot.seek(SeekFrom::Start(0)).await.map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::read_snapshot(
                Some(meta.signature()),
                &e,
            ))
        })?;
        // Install via std::fs for heed's sync reader API: copy to a durable path first.
        let durable = self
            .snapshot_dir
            .join(format!("installed-{}.snap", meta.snapshot_id));
        {
            let mut out = tokio::fs::File::create(&durable).await.map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e.to_string()),
                ))
            })?;
            tokio::io::copy(&mut snapshot, &mut out)
                .await
                .map_err(|e| {
                    StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(e.to_string()),
                    ))
                })?;
            out.flush().await.map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e.to_string()),
                ))
            })?;
        }

        let snap_sm = {
            let mut file = std::fs::File::open(&durable).map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::read_snapshot(
                    Some(meta.signature()),
                    &e,
                ))
            })?;
            self.store
                .install_snapshot_from_reader(&mut file)
                .map_err(|e| {
                    StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(e.to_string()),
                    ))
                })?
        };

        let mut sm = self.meta.write().await;
        *sm = snap_sm;
        sm.last_applied_log = meta.last_log_id.or(sm.last_applied_log);
        sm.last_membership = meta.last_membership.clone();
        self.store.save_sm_meta_only(&sm).map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::write_state_machine(
                &std::io::Error::other(e.to_string()),
            ))
        })?;
        drop(sm);

        let mut current_snapshot = self.current_snapshot.write().await;
        if let Some(prev) = current_snapshot.take() {
            let _ = std::fs::remove_file(prev.path);
        }
        *current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            path: durable,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<FluxRaftTypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let file = MetaStateMachine::open_snapshot_file(&snapshot.path).await?;
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(file),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
