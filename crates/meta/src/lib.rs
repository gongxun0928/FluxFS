//! Metadata layer: trait boundary + heed default + remote tonic client + openraft.
//!
//! Engine types must not leak into the public inode/dentry API.

mod heed_store;
mod raft_log_store;
mod raft_network;
mod raft_node;
mod raft_sm;
mod raft_types;
mod remote;
mod store;

#[cfg(test)]
mod proptest_smoke;

pub use heed_store::HeedMetaStore;
pub use raft_node::{start_single_voter, SINGLE_VOTER_ID};
pub use raft_types::{FluxRaft, FluxRaftTypeConfig, MetaRaftRequest, MetaRaftResponse, NodeId};
pub use remote::RemoteMetaStore;
pub use store::MetaStore;
