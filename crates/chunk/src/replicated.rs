//! Local RF=N durability coordinator for authoritative chunks.
//!
//! W1 uses independent directories on one machine to validate the replication
//! protocol and restart behavior. W2 replaces each [`DiskChunkStore`] call with
//! a Worker RPC without changing the ACK rule: success is returned only after
//! the configured number of replicas report durable storage.

use crate::{ChunkStore, DiskChunkStore};
use fluxfs_types::{ChunkId, FluxError, Result};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaHealth {
    pub healthy: usize,
    pub required: usize,
    pub total: usize,
}

impl ReplicaHealth {
    pub fn writable(self) -> bool {
        self.healthy >= self.required
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutReceipt {
    pub chunk: ChunkId,
    pub durable_replicas: usize,
}

/// Authoritative content-addressed storage with an explicit durability quorum.
pub struct ReplicatedChunkStore {
    replicas: Vec<DiskChunkStore>,
    required: usize,
}

impl ReplicatedChunkStore {
    pub fn open(replica_paths: Vec<PathBuf>, required: usize) -> Result<Self> {
        if required == 0 || replica_paths.len() < required {
            return Err(FluxError::InvalidArg(format!(
                "replication requires {required} durable replicas, but {} paths were provided",
                replica_paths.len()
            )));
        }
        let replicas = replica_paths
            .into_iter()
            .map(DiskChunkStore::open)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { replicas, required })
    }

    pub fn open_rf2(primary: impl Into<PathBuf>, secondary: impl Into<PathBuf>) -> Result<Self> {
        Self::open(vec![primary.into(), secondary.into()], 2)
    }

    pub fn put_replicated(&self, data: &[u8]) -> Result<PutReceipt> {
        let expected = ChunkId::from_bytes(data);
        let mut durable = 0;
        let mut errors = Vec::new();

        for (index, replica) in self.replicas.iter().enumerate() {
            match replica.put(data) {
                Ok(actual) if actual == expected => durable += 1,
                Ok(actual) => errors.push(format!(
                    "replica {index} returned unexpected chunk {}",
                    actual.to_hex()
                )),
                Err(error) => errors.push(format!("replica {index}: {error}")),
            }
        }

        if durable < self.required {
            return Err(FluxError::Io(format!(
                "chunk {} reached {durable}/{} required durable replicas: {}",
                expected.to_hex(),
                self.required,
                errors.join("; ")
            )));
        }
        Ok(PutReceipt {
            chunk: expected,
            durable_replicas: durable,
        })
    }

    pub fn health(&self, id: &ChunkId) -> ReplicaHealth {
        let healthy = self
            .replicas
            .iter()
            .filter(|replica| replica.get(id).is_ok())
            .count();
        ReplicaHealth {
            healthy,
            required: self.required,
            total: self.replicas.len(),
        }
    }

    /// Recopy a valid replica to every missing or corrupt replica.
    pub fn repair(&self, id: &ChunkId) -> Result<ReplicaHealth> {
        let data = self.get(id)?;
        for replica in &self.replicas {
            if replica.get(id).is_err() {
                let repaired = replica.put(&data)?;
                if repaired != *id {
                    return Err(FluxError::Io("replica repair changed chunk id".into()));
                }
            }
        }
        let health = self.health(id);
        if !health.writable() {
            return Err(FluxError::Io(format!(
                "repair left only {}/{} durable replicas",
                health.healthy, health.required
            )));
        }
        Ok(health)
    }
}

impl ChunkStore for ReplicatedChunkStore {
    fn put(&self, data: &[u8]) -> Result<ChunkId> {
        Ok(self.put_replicated(data)?.chunk)
    }

    fn get(&self, id: &ChunkId) -> Result<Vec<u8>> {
        let mut errors = Vec::new();
        for (index, replica) in self.replicas.iter().enumerate() {
            match replica.get(id) {
                Ok(data) => return Ok(data),
                Err(error) => errors.push(format!("replica {index}: {error}")),
            }
        }
        Err(FluxError::Io(format!(
            "no readable replica for chunk {}: {}",
            id.to_hex(),
            errors.join("; ")
        )))
    }

    fn contains(&self, id: &ChunkId) -> Result<bool> {
        Ok(self.health(id).writable())
    }

    fn list_chunks(&self) -> Result<Vec<ChunkId>> {
        let mut chunks = self
            .replicas
            .iter()
            .map(DiskChunkStore::list_chunks)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        chunks.sort_by_key(ChunkId::to_hex);
        chunks.dedup();
        Ok(chunks)
    }

    fn delete(&self, id: &ChunkId) -> Result<()> {
        for replica in &self.replicas {
            replica.delete(id)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn rf2_put_survives_store_restart() {
        let temp = tempfile::tempdir().unwrap();
        let primary = temp.path().join("worker-a");
        let secondary = temp.path().join("worker-b");
        let id = {
            let store = ReplicatedChunkStore::open_rf2(&primary, &secondary).unwrap();
            let receipt = store.put_replicated(b"survive restart").unwrap();
            assert_eq!(receipt.durable_replicas, 2);
            receipt.chunk
        };

        let reopened = ReplicatedChunkStore::open_rf2(primary, secondary).unwrap();
        assert_eq!(reopened.get(&id).unwrap(), b"survive restart");
        assert_eq!(reopened.health(&id).healthy, 2);
    }

    #[test]
    fn write_does_not_ack_below_rf2() {
        let temp = tempfile::tempdir().unwrap();
        let primary = temp.path().join("worker-a");
        let secondary = temp.path().join("worker-b");
        let store = ReplicatedChunkStore::open_rf2(&primary, &secondary).unwrap();

        fs::remove_dir(secondary.join("objects")).unwrap();
        fs::write(secondary.join("objects"), b"not a directory").unwrap();
        assert!(store.put_replicated(b"must not ack").is_err());
        assert!(!store
            .contains(&ChunkId::from_bytes(b"must not ack"))
            .unwrap());
    }

    #[test]
    fn read_falls_back_then_repair_restores_rf2() {
        let temp = tempfile::tempdir().unwrap();
        let primary = temp.path().join("worker-a");
        let secondary = temp.path().join("worker-b");
        let store = ReplicatedChunkStore::open_rf2(&primary, &secondary).unwrap();
        let id = store.put(b"repair me").unwrap();

        let first = DiskChunkStore::open(&primary).unwrap();
        fs::write(first.object_path(&id), b"corrupt").unwrap();
        assert_eq!(store.get(&id).unwrap(), b"repair me");
        assert_eq!(store.health(&id).healthy, 1);
        assert!(!store.contains(&id).unwrap());

        let health = store.repair(&id).unwrap();
        assert_eq!(health.healthy, 2);
        assert!(health.writable());
        assert!(store.contains(&id).unwrap());
    }
}
