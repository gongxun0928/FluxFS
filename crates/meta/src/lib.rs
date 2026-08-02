//! Metadata layer: trait boundary + heed default + openraft type stubs.
//!
//! Engine types must not leak into the public inode/dentry API.

mod heed_store;
mod raft_stub;
mod store;

#[cfg(test)]
mod proptest_smoke;

pub use heed_store::HeedMetaStore;
pub use raft_stub::{FluxRaftTypeConfig, NodeId};
pub use store::MetaStore;
