//! Metadata layer: MetaStore trait + RocksDB default + remote tonic client + openraft.

mod raft_log_store;
mod raft_network;
mod raft_node;
mod raft_sm;
mod raft_types;
mod remote;
mod rocks_store;
mod store;

#[cfg(test)]
mod proptest_smoke;

pub use raft_node::{start_single_voter, SINGLE_VOTER_ID};
pub use raft_types::{
    FluxRaft, FluxRaftTypeConfig, MetaRaftRequest, MetaRaftResponse, NodeId, SmAppliedMeta,
};
pub use remote::RemoteMetaStore;
pub use rocks_store::RocksMetaStore;
pub use store::MetaStore;
