//! Metadata layer: trait boundary + heed default + remote tonic client + openraft.
//!
//! Engine types must not leak into the public inode/dentry API.

mod heed_store;
mod raft_log_store;
mod raft_meta;
mod raft_network;
mod raft_node;
mod raft_sm;
mod raft_types;
mod remote;
mod store;

#[cfg(test)]
mod proptest_smoke;

pub use heed_store::HeedMetaStore;
pub use raft_meta::RaftMetaStore;
pub use raft_node::{start_single_voter, SINGLE_VOTER_ID};
pub use raft_types::{
    FluxRaft, FluxRaftTypeConfig, MetaRaftRequest, MetaRaftResponse, NodeId, SmAppliedMeta,
};
pub use remote::RemoteMetaStore;
pub use store::MetaStore;

/// A stalled writer is fenced after this deadline. Its late commit is rejected
/// and must retry with a fresh ticket.
pub const WRITE_RESERVATION_TTL_MS: u64 = 15 * 60 * 1_000;

/// Sample wall time before proposing a Raft command, never during apply.
pub fn unix_time_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub fn write_reservation_deadline() -> u64 {
    unix_time_millis().saturating_add(WRITE_RESERVATION_TTL_MS)
}
