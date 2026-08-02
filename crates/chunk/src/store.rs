use fluxfs_types::{ChunkId, Result, WorkerTargetId};

/// Engine-agnostic chunk put/get. Default RF / replication is a higher-layer concern.
pub trait ChunkStore: Send + Sync {
    fn put(&self, data: &[u8]) -> Result<ChunkId>;
    fn get(&self, id: &ChunkId) -> Result<Vec<u8>>;
    fn contains(&self, id: &ChunkId) -> Result<bool>;

    fn list_chunks(&self) -> Result<Vec<ChunkId>> {
        Err(fluxfs_types::FluxError::Capability(
            "chunk store does not support GC inventory".into(),
        ))
    }

    fn delete(&self, _id: &ChunkId) -> Result<()> {
        Err(fluxfs_types::FluxError::Capability(
            "chunk store does not support GC delete".into(),
        ))
    }

    fn gc_delete_targets(&self) -> Result<Vec<WorkerTargetId>> {
        Ok(vec![WorkerTargetId(0)])
    }

    fn delete_from_target(&self, id: &ChunkId, target: WorkerTargetId) -> Result<()> {
        if target != WorkerTargetId(0) {
            return Err(fluxfs_types::FluxError::InvalidArg(format!(
                "unknown chunk delete target {}",
                target.0
            )));
        }
        self.delete(id)
    }
}
