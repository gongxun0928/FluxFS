//! Named crash boundaries shared by component and distributed tests.

/// The primary chunk reached stable storage, before the replica did.
pub const AFTER_PRIMARY_CHUNK_DURABLE: &str = "chunk::after_primary_durable";
/// Both RF=2 chunk replicas are durable, before metadata commits the manifest.
pub const AFTER_REPLICA_QUORUM: &str = "chunk::after_replica_quorum";
/// The manifest DATA_COMMIT is durable, before the client receives its ACK.
pub const AFTER_DATA_COMMIT: &str = "meta::after_data_commit";
/// FLUSH_INTENT is durable, before any UFS publish.
pub const AFTER_FLUSH_INTENT: &str = "flush::after_intent";
/// UFS publish completed, before UFS_COMMIT is proposed.
pub const AFTER_UFS_PUBLISH: &str = "flush::after_ufs_publish";
/// UFS_COMMIT is durable, before locality is reported Clean.
pub const AFTER_UFS_COMMIT: &str = "flush::after_ufs_commit";
/// A snapshot was persisted, before log compaction.
pub const AFTER_SNAPSHOT_PERSIST: &str = "raft::after_snapshot_persist";
/// A GC victim was selected, before its bytes are removed.
pub const BEFORE_GC_DELETE: &str = "gc::before_delete";

/// All MVP crash points. The stable names make a failing seed replayable.
pub const ALL: &[&str] = &[
    AFTER_PRIMARY_CHUNK_DURABLE,
    AFTER_REPLICA_QUORUM,
    AFTER_DATA_COMMIT,
    AFTER_FLUSH_INTENT,
    AFTER_UFS_PUBLISH,
    AFTER_UFS_COMMIT,
    AFTER_SNAPSHOT_PERSIST,
    BEFORE_GC_DELETE,
];

/// Enable one process-global `fail` failpoint and remove it on drop.
///
/// Failpoint-driven tests must run serially because the upstream registry is
/// process-global. Deterministic simulation tests should use one scenario per
/// seed and print the seed before execution.
pub struct FailpointGuard {
    name: &'static str,
}

impl FailpointGuard {
    pub fn arm(name: &'static str, action: &str) -> Result<Self, String> {
        fail::cfg(name, action)?;
        Ok(Self { name })
    }
}

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        fail::remove(self.name);
    }
}

/// Execute a callback configured for `name`.
///
/// Production code can call this at an awaitless commit boundary. With the
/// `failpoints` feature disabled, the upstream crate compiles it to no-op.
#[inline]
pub fn hit(name: &'static str) {
    fail::fail_point!(name);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_point_names_are_unique() {
        let unique = ALL
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), ALL.len());
    }

    #[cfg(feature = "failpoints")]
    #[test]
    #[should_panic(expected = "injected crash")]
    fn guard_arms_and_removes_a_failpoint() {
        let _scenario = fail::FailScenario::setup();
        let _guard = FailpointGuard::arm(AFTER_DATA_COMMIT, "panic(injected crash)").unwrap();
        hit(AFTER_DATA_COMMIT);
    }
}
