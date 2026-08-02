//! RocksDB-backed `RaftLogStorage` (CF `meta` + `logs` on shared DB).

#![allow(clippy::result_large_err)]
#![allow(clippy::redundant_closure)]

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::Entry;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions, DB};

use crate::raft_types::{FluxRaftTypeConfig, NodeId};
use crate::rocks_store::{CF_LOGS, CF_META};

const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_PURGED: &[u8] = b"last_purged";

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

fn sync_write(db: &DB, batch: WriteBatch) -> Result<(), StorageError<NodeId>> {
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    db.write_opt(batch, &wo).map_err(|e| io_logs(e))
}

/// Cloneable RaftLogStorage over the shared RocksDB.
#[derive(Clone)]
pub struct RocksRaftLogStore {
    db: Arc<DB>,
}

impl RocksRaftLogStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    fn cf_meta(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_META).expect("cf meta")
    }

    fn cf_logs(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_LOGS).expect("cf logs")
    }
}

impl RaftLogReader<FluxRaftTypeConfig> for RocksRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<FluxRaftTypeConfig>>, StorageError<NodeId>> {
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
        let cf = self.cf_logs();
        let mut out = Vec::new();
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&index_key(start), Direction::Forward),
        );
        for item in iter {
            let (k, v) = item.map_err(|e| io_read_logs(e))?;
            if k.len() != 8 {
                continue;
            }
            let idx =
                u64::from_be_bytes(k.as_ref().try_into().map_err(|_| io_read_logs("bad key"))?);
            if let Some(end) = end_inclusive {
                if idx > end {
                    break;
                }
            }
            let entry: Entry<FluxRaftTypeConfig> =
                serde_json::from_slice(&v).map_err(|e| io_read_logs(e))?;
            out.push(entry);
        }
        Ok(out)
    }
}

impl RaftLogStorage<FluxRaftTypeConfig> for RocksRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<FluxRaftTypeConfig>, StorageError<NodeId>> {
        let cf_meta = self.cf_meta();
        let last_purged = match self
            .db
            .get_cf(cf_meta, KEY_LAST_PURGED)
            .map_err(|e| io_read_logs(e))?
        {
            Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(|e| io_read_logs(e))?),
            None => None,
        };
        let cf_logs = self.cf_logs();
        let mut last = None;
        let iter = self.db.iterator_cf(cf_logs, IteratorMode::End);
        if let Some(item) = iter.into_iter().next() {
            let (_k, v) = item.map_err(|e| io_read_logs(e))?;
            let entry: Entry<FluxRaftTypeConfig> =
                serde_json::from_slice(&v).map_err(|e| io_read_logs(e))?;
            last = Some(*entry.get_log_id());
        }
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
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf_meta(),
            KEY_COMMITTED,
            serde_json::to_vec(&committed).map_err(|e| io_logs(e))?,
        );
        sync_write(&self.db, batch)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        match self
            .db
            .get_cf(self.cf_meta(), KEY_COMMITTED)
            .map_err(|e| io_read_logs(e))?
        {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(&bytes).map_err(|e| io_read_logs(e))?,
            )),
            None => Ok(None),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf_meta(),
            KEY_VOTE,
            serde_json::to_vec(vote).map_err(|e| io_vote(e))?,
        );
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(batch, &wo).map_err(|e| io_vote(e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        match self
            .db
            .get_cf(self.cf_meta(), KEY_VOTE)
            .map_err(|e| io_vote(e))?
        {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(&bytes).map_err(|e| io_vote(e))?,
            )),
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
        let mut batch = WriteBatch::default();
        let cf = self.cf_logs();
        for entry in entries {
            batch.put_cf(
                cf,
                index_key(entry.get_log_id().index),
                serde_json::to_vec(&entry).map_err(|e| io_logs(e))?,
            );
        }
        sync_write(&self.db, batch)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.cf_logs();
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&index_key(log_id.index), Direction::Forward),
        );
        for item in iter {
            let (k, _) = item.map_err(|e| io_logs(e))?;
            batch.delete_cf(cf, k);
        }
        sync_write(&self.db, batch)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf_meta(),
            KEY_LAST_PURGED,
            serde_json::to_vec(&log_id).map_err(|e| io_logs(e))?,
        );
        let cf = self.cf_logs();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        for item in iter {
            let (k, _) = item.map_err(|e| io_logs(e))?;
            if k.len() != 8 {
                continue;
            }
            let idx = u64::from_be_bytes(k.as_ref().try_into().map_err(|_| io_logs("bad key"))?);
            if idx <= log_id.index {
                batch.delete_cf(cf, k);
            } else {
                break;
            }
        }
        sync_write(&self.db, batch)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
