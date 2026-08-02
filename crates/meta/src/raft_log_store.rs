//! Heed-backed `RaftLogStorage` for MetaMaster.
//!
//! Stores vote / log entries / committed / last_purged, plus state-machine
//! applied markers so a MetaMaster restart can resume without re-initialize wipe.

#![allow(clippy::result_large_err)] // openraft StorageError is large by design
#![allow(clippy::redundant_closure)]

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};

use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::BasicNode;
use openraft::Entry;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;

use crate::raft_types::{FluxRaftTypeConfig, NodeId};
use fluxfs_types::{FluxError, Result as FluxResult};

type LogDb = Database<Bytes, Bytes>;
type MetaDb = Database<Str, Bytes>;

const KEY_VOTE: &str = "vote";
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_SM_LAST_APPLIED: &str = "sm_last_applied";
const KEY_SM_LAST_MEMBERSHIP: &str = "sm_last_membership";

#[derive(Debug, Clone, Default)]
pub struct SmAppliedMeta {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
}

/// Durable Raft log + SM applied markers (separate heed env from inode MetaStore).
pub struct HeedRaftStore {
    env: Env,
    logs: LogDb,
    meta: MetaDb,
    write_lock: Mutex<()>,
}

impl HeedRaftStore {
    pub fn open(path: impl AsRef<Path>) -> FluxResult<Self> {
        std::fs::create_dir_all(path.as_ref()).map_err(|e| FluxError::Io(e.to_string()))?;
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(256 * 1024 * 1024)
                .max_dbs(8)
                .open(path.as_ref())
                .map_err(|e| FluxError::Meta(format!("open raft heed: {e}")))?
        };
        let mut wtxn = env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let logs: LogDb = env
            .create_database(&mut wtxn, Some("raft_logs"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let meta: MetaDb = env
            .create_database(&mut wtxn, Some("raft_meta"))
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(Self {
            env,
            logs,
            meta,
            write_lock: Mutex::new(()),
        })
    }

    pub fn load_sm_meta(&self) -> FluxResult<SmAppliedMeta> {
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
            None => StoredMembership::default(),
        };
        Ok(SmAppliedMeta {
            last_applied_log,
            last_membership,
        })
    }

    pub fn save_sm_meta(&self, sm: &SmAppliedMeta) -> FluxResult<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| FluxError::Meta("raft write lock poisoned".into()))?;
        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let applied =
            serde_json::to_vec(&sm.last_applied_log).map_err(|e| FluxError::Meta(e.to_string()))?;
        self.meta
            .put(&mut wtxn, KEY_SM_LAST_APPLIED, &applied)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        let membership =
            serde_json::to_vec(&sm.last_membership).map_err(|e| FluxError::Meta(e.to_string()))?;
        self.meta
            .put(&mut wtxn, KEY_SM_LAST_MEMBERSHIP, &membership)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        wtxn.commit().map_err(|e| FluxError::Meta(e.to_string()))?;
        Ok(())
    }
}

fn io_logs(e: impl ToString) -> StorageError<NodeId> {
    StorageError::from(StorageIOError::<NodeId>::write_logs(
        &std::io::Error::other(e.to_string()),
    ))
}

fn io_read_logs(e: impl ToString) -> StorageError<NodeId> {
    StorageError::from(StorageIOError::<NodeId>::read_logs(&std::io::Error::other(
        e.to_string(),
    )))
}

fn io_vote(e: impl ToString) -> StorageError<NodeId> {
    StorageError::from(StorageIOError::<NodeId>::write_vote(
        &std::io::Error::other(e.to_string()),
    ))
}

fn index_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

/// Cloneable RaftLogStorage façade over [`HeedRaftStore`].
#[derive(Clone)]
pub struct HeedRaftLogStore {
    inner: Arc<HeedRaftStore>,
}

impl HeedRaftLogStore {
    pub fn new(inner: Arc<HeedRaftStore>) -> Self {
        Self { inner }
    }
}

impl RaftLogReader<FluxRaftTypeConfig> for HeedRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<FluxRaftTypeConfig>>, StorageError<NodeId>> {
        let rtxn = self.inner.env.read_txn().map_err(|e| io_read_logs(e))?;
        let start = match range.start_bound() {
            std::ops::Bound::Included(&i) => i,
            std::ops::Bound::Excluded(&i) => i.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let end_inclusive = match range.end_bound() {
            std::ops::Bound::Included(&i) => Some(i),
            std::ops::Bound::Excluded(&i) => i.checked_sub(1),
            std::ops::Bound::Unbounded => None,
        };

        let mut out = Vec::new();
        let iter = self.inner.logs.iter(&rtxn).map_err(|e| io_read_logs(e))?;
        for item in iter {
            let (k, v) = item.map_err(|e| io_read_logs(e))?;
            if k.len() != 8 {
                continue;
            }
            let idx = u64::from_be_bytes(k.try_into().map_err(|_| io_read_logs("bad log key"))?);
            if idx < start {
                continue;
            }
            if let Some(end) = end_inclusive {
                if idx > end {
                    break;
                }
            }
            let entry: Entry<FluxRaftTypeConfig> =
                serde_json::from_slice(v).map_err(|e| io_read_logs(e))?;
            out.push(entry);
        }
        Ok(out)
    }
}

impl RaftLogStorage<FluxRaftTypeConfig> for HeedRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<FluxRaftTypeConfig>, StorageError<NodeId>> {
        let rtxn = self.inner.env.read_txn().map_err(|e| io_read_logs(e))?;
        let last_purged = match self
            .inner
            .meta
            .get(&rtxn, KEY_LAST_PURGED)
            .map_err(|e| io_read_logs(e))?
        {
            Some(bytes) => Some(serde_json::from_slice(bytes).map_err(|e| io_read_logs(e))?),
            None => None,
        };
        let last = {
            let mut last = None;
            let iter = self.inner.logs.iter(&rtxn).map_err(|e| io_read_logs(e))?;
            for item in iter {
                let (_k, v) = item.map_err(|e| io_read_logs(e))?;
                let entry: Entry<FluxRaftTypeConfig> =
                    serde_json::from_slice(v).map_err(|e| io_read_logs(e))?;
                last = Some(*entry.get_log_id());
            }
            last
        };
        let last_log_id = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let _guard = self
            .inner
            .write_lock
            .lock()
            .map_err(|_| io_logs("write lock poisoned"))?;
        let mut wtxn = self.inner.env.write_txn().map_err(|e| io_logs(e))?;
        let bytes = serde_json::to_vec(&committed).map_err(|e| io_logs(e))?;
        self.inner
            .meta
            .put(&mut wtxn, KEY_COMMITTED, &bytes)
            .map_err(|e| io_logs(e))?;
        wtxn.commit().map_err(|e| io_logs(e))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let rtxn = self.inner.env.read_txn().map_err(|e| io_read_logs(e))?;
        match self
            .inner
            .meta
            .get(&rtxn, KEY_COMMITTED)
            .map_err(|e| io_read_logs(e))?
        {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(bytes).map_err(|e| io_read_logs(e))?,
            )),
            None => Ok(None),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let _guard = self
            .inner
            .write_lock
            .lock()
            .map_err(|_| io_vote("write lock poisoned"))?;
        let mut wtxn = self.inner.env.write_txn().map_err(|e| io_vote(e))?;
        let bytes = serde_json::to_vec(vote).map_err(|e| io_vote(e))?;
        self.inner
            .meta
            .put(&mut wtxn, KEY_VOTE, &bytes)
            .map_err(|e| io_vote(e))?;
        wtxn.commit().map_err(|e| io_vote(e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let rtxn = self.inner.env.read_txn().map_err(|e| io_vote(e))?;
        match self
            .inner
            .meta
            .get(&rtxn, KEY_VOTE)
            .map_err(|e| io_vote(e))?
        {
            Some(bytes) => Ok(Some(serde_json::from_slice(bytes).map_err(|e| io_vote(e))?)),
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<FluxRaftTypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<FluxRaftTypeConfig>>,
    {
        let _guard = self
            .inner
            .write_lock
            .lock()
            .map_err(|_| io_logs("write lock poisoned"))?;
        let mut wtxn = self.inner.env.write_txn().map_err(|e| io_logs(e))?;
        for entry in entries {
            let key = index_key(entry.get_log_id().index);
            let bytes = serde_json::to_vec(&entry).map_err(|e| io_logs(e))?;
            self.inner
                .logs
                .put(&mut wtxn, &key, &bytes)
                .map_err(|e| io_logs(e))?;
        }
        wtxn.commit().map_err(|e| io_logs(e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let _guard = self
            .inner
            .write_lock
            .lock()
            .map_err(|_| io_logs("write lock poisoned"))?;
        let mut wtxn = self.inner.env.write_txn().map_err(|e| io_logs(e))?;
        let keys: Vec<[u8; 8]> = {
            let mut keys = Vec::new();
            let iter = self.inner.logs.iter(&wtxn).map_err(|e| io_logs(e))?;
            for item in iter {
                let (k, _) = item.map_err(|e| io_logs(e))?;
                if k.len() != 8 {
                    continue;
                }
                let idx = u64::from_be_bytes(k.try_into().map_err(|_| io_logs("bad log key"))?);
                if idx >= log_id.index {
                    keys.push(idx.to_be_bytes());
                }
            }
            keys
        };
        for key in keys {
            self.inner
                .logs
                .delete(&mut wtxn, &key)
                .map_err(|e| io_logs(e))?;
        }
        wtxn.commit().map_err(|e| io_logs(e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let _guard = self
            .inner
            .write_lock
            .lock()
            .map_err(|_| io_logs("write lock poisoned"))?;
        let mut wtxn = self.inner.env.write_txn().map_err(|e| io_logs(e))?;
        let purged_bytes = serde_json::to_vec(&log_id).map_err(|e| io_logs(e))?;
        self.inner
            .meta
            .put(&mut wtxn, KEY_LAST_PURGED, &purged_bytes)
            .map_err(|e| io_logs(e))?;
        let keys: Vec<[u8; 8]> = {
            let mut keys = Vec::new();
            let iter = self.inner.logs.iter(&wtxn).map_err(|e| io_logs(e))?;
            for item in iter {
                let (k, _) = item.map_err(|e| io_logs(e))?;
                if k.len() != 8 {
                    continue;
                }
                let idx = u64::from_be_bytes(k.try_into().map_err(|_| io_logs("bad log key"))?);
                if idx <= log_id.index {
                    keys.push(idx.to_be_bytes());
                }
            }
            keys
        };
        for key in keys {
            self.inner
                .logs
                .delete(&mut wtxn, &key)
                .map_err(|e| io_logs(e))?;
        }
        wtxn.commit().map_err(|e| io_logs(e))?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
