//! Metadata layer: trait boundary + heed default + remote tonic client + openraft stubs.
//!
//! Engine types must not leak into the public inode/dentry API.

mod heed_store;
mod raft_stub;
mod remote;
mod store;

#[cfg(test)]
mod proptest_smoke;

pub use heed_store::HeedMetaStore;
pub use raft_stub::{FluxRaftTypeConfig, NodeId};
pub use remote::RemoteMetaStore;
pub use store::MetaStore;
