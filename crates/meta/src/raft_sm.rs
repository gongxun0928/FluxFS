//! Raft state machine: apply meta mutations into [`HeedMetaStore`].

#![allow(clippy::result_large_err)] // openraft StorageError is large by design

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
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

fn sync_snapshot_dir(path: &Path) -> std::io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| std::io::Error::other("snapshot path has no parent directory"))?;
    std::fs::File::open(dir)?.sync_all()
}

/// Publish a complete snapshot without ever exposing a half-written final file.
fn publish_snapshot_file(temp: &Path, final_path: &Path) -> std::io::Result<()> {
    std::fs::File::open(temp)?.sync_all()?;
    std::fs::rename(temp, final_path)?;
    sync_snapshot_dir(final_path)
}

fn remove_snapshot_file(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_snapshot_dir(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn managed_snapshot_kind(name: &str) -> Option<&'static str> {
    if name.starts_with("incoming-") && name.ends_with(".snap") {
        Some("incoming")
    } else if name.starts_with("installed-") && name.ends_with(".snap") {
        Some("installed")
    } else if name.starts_with("built-") && name.ends_with(".snap") {
        Some("built")
    } else if name.starts_with(".building-") && name.ends_with(".tmp") {
        Some("building")
    } else if name.starts_with(".installing-") && name.ends_with(".tmp") {
        Some("installing")
    } else if name.ends_with(".snap") {
        // Before #42 locally built snapshots used `{snapshot_id}.snap` without
        // a stable prefix. This directory is private to Raft snapshots, so a
        // leftover `.snap` file is an unreferenced legacy artifact at startup.
        Some("legacy")
    } else {
        None
    }
}

/// Remove files which cannot be referenced after a process restart.
fn prune_snapshot_dir_on_startup(snapshot_dir: &Path) -> std::io::Result<usize> {
    prune_snapshot_dir(snapshot_dir, None, |_| true)
}

/// After a successful install, only receiver/install artifacts are stale. A
/// locally built snapshot may be active concurrently and must not be removed.
fn prune_received_snapshot_files(snapshot_dir: &Path, preserve: &Path) -> std::io::Result<usize> {
    prune_snapshot_dir(snapshot_dir, Some(preserve), |kind| {
        matches!(kind, "incoming" | "installed" | "installing")
    })
}

fn prune_snapshot_dir(
    snapshot_dir: &Path,
    preserve: Option<&Path>,
    should_remove: impl Fn(&str) -> bool,
) -> std::io::Result<usize> {
    let mut removed = 0;
    for entry in std::fs::read_dir(snapshot_dir)? {
        let entry = entry?;
        let path = entry.path();
        if preserve.is_some_and(|keep| keep == path) || !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(kind) = managed_snapshot_kind(&name) else {
            continue;
        };
        if should_remove(kind) {
            match std::fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    // A stale artifact must not make an otherwise healthy Meta
                    // state machine unavailable. Retry on the next startup.
                    tracing::warn!(path = %path.display(), %error, "failed to prune stale snapshot");
                }
            }
        }
    }
    if removed > 0 {
        std::fs::File::open(snapshot_dir)?.sync_all()?;
    }
    Ok(removed)
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
        prune_snapshot_dir_on_startup(&snapshot_dir).map_err(|e| {
            StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                None,
                &std::io::Error::other(format!("prune stale snapshots: {e}")),
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
        let path = self.snapshot_dir.join(format!("built-{snapshot_idx}.snap"));
        let temp = self
            .snapshot_dir
            .join(format!(".building-{snapshot_idx}.tmp"));
        if let Err(error) = self.store.export_snapshot_to_path(&sm, &temp) {
            let _ = std::fs::remove_file(&temp);
            return Err(StorageError::from(
                StorageIOError::<NodeId>::read_state_machine(&std::io::Error::other(
                    error.to_string(),
                )),
            ));
        }
        if let Err(error) = publish_snapshot_file(&temp, &path) {
            let _ = std::fs::remove_file(&temp);
            return Err(StorageError::from(
                StorageIOError::<NodeId>::write_snapshot(
                    None,
                    &std::io::Error::other(error.to_string()),
                ),
            ));
        }

        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        drop(sm);

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let previous = {
            let mut current_snapshot = self.current_snapshot.write().await;
            current_snapshot
                .replace(StoredSnapshot {
                    meta: meta.clone(),
                    path: path.clone(),
                })
                .map(|snapshot| snapshot.path)
        };
        if let Some(previous) = previous {
            if let Err(error) = remove_snapshot_file(&previous) {
                tracing::warn!(
                    path = %previous.display(),
                    %error,
                    "failed to prune superseded snapshot"
                );
            }
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
            .create_new(true)
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
        let install_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let temp = self
            .snapshot_dir
            .join(format!(".installing-{install_idx}.tmp"));
        let durable = self
            .snapshot_dir
            .join(format!("installed-{install_idx}.snap"));
        let copy_result = async {
            let mut out = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp)
                .await
                .map_err(|e| {
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
            out.sync_all().await.map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e.to_string()),
                ))
            })?;
            drop(out);
            tokio::fs::rename(&temp, &durable).await.map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e.to_string()),
                ))
            })?;
            sync_snapshot_dir(&durable).map_err(|e| {
                StorageError::from(StorageIOError::<NodeId>::write_snapshot(
                    Some(meta.signature()),
                    &std::io::Error::other(e.to_string()),
                ))
            })?;
            Ok::<(), StorageError<NodeId>>(())
        }
        .await;
        if let Err(error) = copy_result {
            let _ = std::fs::remove_file(&temp);
            return Err(error);
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

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(StoredSnapshot {
                meta: meta.clone(),
                path: durable.clone(),
            });
        }
        if let Err(error) = prune_received_snapshot_files(&self.snapshot_dir, &durable) {
            tracing::warn!(%error, "failed to prune stale received snapshots");
        }
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn atomic_publish_replaces_final_and_removes_temp() {
        let dir = tempdir().unwrap();
        let temp = dir.path().join(".building-1.tmp");
        let final_path = dir.path().join("built-1.snap");
        std::fs::write(&final_path, b"old").unwrap();
        let mut file = std::fs::File::create(&temp).unwrap();
        file.write_all(b"complete snapshot").unwrap();
        drop(file);

        publish_snapshot_file(&temp, &final_path).unwrap();

        assert!(!temp.exists());
        assert_eq!(std::fs::read(final_path).unwrap(), b"complete snapshot");
    }

    #[test]
    fn startup_prunes_only_managed_snapshot_artifacts() {
        let dir = tempdir().unwrap();
        for name in [
            "incoming-1.snap",
            "installed-2.snap",
            "built-3.snap",
            "1-7-3.snap",
            ".building-4.tmp",
            ".installing-5.tmp",
        ] {
            std::fs::write(dir.path().join(name), b"stale").unwrap();
        }
        std::fs::write(dir.path().join("keep.txt"), b"keep").unwrap();

        assert_eq!(prune_snapshot_dir_on_startup(dir.path()).unwrap(), 6);
        assert!(dir.path().join("keep.txt").exists());
        assert_eq!(prune_snapshot_dir_on_startup(dir.path()).unwrap(), 0);
    }

    #[test]
    fn state_machine_startup_invokes_snapshot_prune() {
        let dir = tempdir().unwrap();
        let store = Arc::new(HeedMetaStore::open(dir.path().join("meta")).unwrap());
        let snapshot_dir = store.snapshot_dir();
        std::fs::create_dir_all(&snapshot_dir).unwrap();
        std::fs::write(snapshot_dir.join("incoming-1.snap"), b"partial").unwrap();
        std::fs::write(snapshot_dir.join("installed-2.snap"), b"stale").unwrap();

        let _state_machine = MetaStateMachine::new(store).unwrap();

        assert!(!snapshot_dir.join("incoming-1.snap").exists());
        assert!(!snapshot_dir.join("installed-2.snap").exists());
    }

    #[test]
    fn installed_prune_preserves_current_and_concurrent_build() {
        let dir = tempdir().unwrap();
        let current = dir.path().join("installed-3.snap");
        for name in [
            "incoming-1.snap",
            "installed-2.snap",
            "installed-3.snap",
            ".installing-4.tmp",
            ".building-5.tmp",
            "built-5.snap",
        ] {
            std::fs::write(dir.path().join(name), b"snapshot").unwrap();
        }

        assert_eq!(
            prune_received_snapshot_files(dir.path(), &current).unwrap(),
            3
        );
        assert!(current.exists());
        assert!(dir.path().join(".building-5.tmp").exists());
        assert!(dir.path().join("built-5.snap").exists());
    }
}
