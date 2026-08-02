//! In-memory `RaftLogStorage` for single-voter MetaMaster.
//!
//! Adapted from openraft's examples/memstore. Durable application state lives in
//! Heed; the Raft log is process-local and re-initialized on MetaMaster restart.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::Vote;
use tokio::sync::Mutex;

#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Debug)]
struct LogStoreInner<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogId<C::NodeId>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogId<C::NodeId>>,
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
where
    C::Entry: Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, val)| val.clone()).collect())
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
where
    C::Entry: Clone,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let inner = self.inner.lock().await;
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, ent)| ent.get_log_id().clone());
        let last_purged = inner.last_purged_log_id.clone();
        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.committed.clone())
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner.vote.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        assert!(inner.last_purged_log_id.as_ref() <= Some(&log_id));
        inner.last_purged_log_id = Some(log_id.clone());
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
