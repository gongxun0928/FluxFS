use fluxfs_types::{ChunkId, ChunkPage, FluxError, Result, WorkerTargetId};

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

    fn list_chunks_page(&self, cursor: Option<ChunkId>, limit: usize) -> Result<ChunkPage> {
        if limit == 0 {
            return Err(FluxError::InvalidArg(
                "chunk inventory page limit must be non-zero".into(),
            ));
        }
        let mut chunks = self.list_chunks()?;
        chunks.sort_by_key(ChunkId::to_hex);
        chunks.dedup();
        let mut page = chunks
            .into_iter()
            .filter(|chunk| cursor.is_none_or(|cursor| *chunk > cursor))
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        page.truncate(limit);
        let next_cursor = has_more.then(|| *page.last().expect("non-empty page"));
        Ok(ChunkPage {
            chunks: page,
            next_cursor,
        })
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
