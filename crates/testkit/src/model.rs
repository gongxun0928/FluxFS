//! Small executable reference model for write-back durability.

use std::collections::BTreeMap;

pub type Generation = u64;
pub type ChunkId = u64;

pub const AUTHORITATIVE_REPLICATION_FACTOR: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackingMode {
    External,
    Managed,
    Ephemeral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpState {
    Idle,
    Flushing(Generation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkState {
    pub generation: Generation,
    pub durable_replicas: u8,
    pub authoritative: bool,
    pub resident: bool,
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceModel {
    pub backing: BackingMode,
    pub head: Generation,
    pub data_commit: Generation,
    pub acknowledged: Generation,
    pub ufs_commit: Generation,
    pub clean: bool,
    pub op: OpState,
    pub chunks: BTreeMap<ChunkId, ChunkState>,
    next_chunk: ChunkId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    Blocked,
}

impl ReferenceModel {
    pub fn new(backing: BackingMode) -> Self {
        Self {
            backing,
            head: 0,
            data_commit: 0,
            acknowledged: 0,
            ufs_commit: 0,
            clean: backing == BackingMode::External,
            op: OpState::Idle,
            chunks: BTreeMap::new(),
            next_chunk: 1,
        }
    }

    /// Model one write attempt. RF=2 is the restricted-alpha ACK threshold.
    pub fn write(&mut self, durable_replicas: u8) -> Outcome {
        self.head += 1;
        self.clean = false;
        self.op = OpState::Idle;
        let id = self.next_chunk;
        self.next_chunk += 1;
        self.chunks.insert(
            id,
            ChunkState {
                generation: self.head,
                durable_replicas,
                authoritative: false,
                resident: true,
                pinned: false,
            },
        );
        if durable_replicas < AUTHORITATIVE_REPLICATION_FACTOR {
            return Outcome::Blocked;
        }
        // This compact model treats each write as a whole-file replacement.
        // Supersede the old generation only after the new chunk reaches RF=2.
        for chunk in self.chunks.values_mut() {
            chunk.authoritative = chunk.generation == self.head;
            chunk.pinned = chunk.authoritative;
        }
        self.data_commit = self.head;
        self.acknowledged = self.head;
        Outcome::Applied
    }

    pub fn start_flush(&mut self) -> Outcome {
        if self.data_commit != self.head || self.head == 0 {
            return Outcome::Blocked;
        }
        self.op = OpState::Flushing(self.head);
        Outcome::Applied
    }

    /// Record the durable UFS publication for the generation in FLUSH_INTENT.
    pub fn commit_ufs(&mut self) -> Outcome {
        let OpState::Flushing(generation) = self.op else {
            return Outcome::Blocked;
        };
        self.ufs_commit = self.ufs_commit.max(generation);
        Outcome::Applied
    }

    /// CAS-style clean transition: a stale flusher cannot clean a newer head.
    pub fn mark_clean(&mut self) -> Outcome {
        let OpState::Flushing(generation) = self.op else {
            return Outcome::Blocked;
        };
        if generation != self.head || self.ufs_commit != generation {
            self.op = OpState::Idle;
            return Outcome::Blocked;
        }
        self.clean = true;
        self.op = OpState::Idle;
        for chunk in self
            .chunks
            .values_mut()
            .filter(|c| c.generation <= generation)
        {
            chunk.authoritative = false;
            chunk.pinned = false;
        }
        Outcome::Applied
    }

    /// Cache eviction may remove only non-authoritative, unpinned chunks.
    pub fn evict(&mut self, chunk: ChunkId) -> Outcome {
        let Some(state) = self.chunks.get_mut(&chunk) else {
            return Outcome::Blocked;
        };
        if state.authoritative || state.pinned {
            return Outcome::Blocked;
        }
        state.resident = false;
        Outcome::Applied
    }

    /// Remove uncommitted bytes and recover an interrupted flush as Dirty.
    pub fn crash_recover(&mut self) {
        let committed = self.data_commit;
        self.chunks.retain(|_, chunk| chunk.generation <= committed);
        self.head = committed;
        self.acknowledged = self.acknowledged.min(committed);
        self.clean = committed != 0 && self.ufs_commit == committed;
        self.op = OpState::Idle;
    }

    pub fn check_invariants(&self) -> Result<(), &'static str> {
        if self.acknowledged > self.data_commit {
            return Err("acknowledged generation is not metadata-durable");
        }
        if self.data_commit > self.head || self.ufs_commit > self.head {
            return Err("committed generation is ahead of the inode head");
        }
        if self.clean && (self.head == 0 || self.ufs_commit != self.head) {
            return Err("Clean requires UFS_COMMIT for the current head");
        }
        for chunk in self.chunks.values() {
            if chunk.authoritative {
                if !chunk.resident || !chunk.pinned {
                    return Err("authoritative chunk was evicted or unpinned");
                }
                if chunk.generation <= self.data_commit
                    && chunk.durable_replicas < AUTHORITATIVE_REPLICATION_FACTOR
                {
                    return Err("DATA_COMMIT references a chunk below RF=2");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn arbitrary_operation_sequences_preserve_invariants(
            operations in prop::collection::vec((0u8..5, 0u8..4), 0..256)
        ) {
            let mut model = ReferenceModel::new(BackingMode::Managed);
            for (operation, arg) in operations {
                match operation {
                    0 => { model.write(arg); }
                    1 => { model.start_flush(); }
                    2 => { model.commit_ufs(); }
                    3 => { model.mark_clean(); }
                    _ => { model.crash_recover(); }
                }
                prop_assert_eq!(model.check_invariants(), Ok(()));
            }
        }
    }

    #[test]
    fn rf1_write_is_not_acknowledged_and_does_not_survive_crash() {
        let mut model = ReferenceModel::new(BackingMode::Managed);
        assert_eq!(model.write(1), Outcome::Blocked);
        assert_eq!(model.acknowledged, 0);
        model.crash_recover();
        assert!(model.chunks.is_empty());
        assert_eq!(model.check_invariants(), Ok(()));
    }

    #[test]
    fn stale_flusher_cannot_clean_a_newer_generation() {
        let mut model = ReferenceModel::new(BackingMode::Managed);
        assert_eq!(model.write(2), Outcome::Applied);
        assert_eq!(model.start_flush(), Outcome::Applied);
        assert_eq!(model.write(2), Outcome::Applied);
        assert_eq!(model.commit_ufs(), Outcome::Blocked);
        assert_eq!(model.mark_clean(), Outcome::Blocked);
        assert!(!model.clean);
    }

    #[test]
    fn authoritative_chunk_cannot_be_evicted() {
        let mut model = ReferenceModel::new(BackingMode::Ephemeral);
        assert_eq!(model.write(2), Outcome::Applied);
        assert_eq!(model.evict(1), Outcome::Blocked);
        assert!(model.chunks[&1].resident);
    }
}
