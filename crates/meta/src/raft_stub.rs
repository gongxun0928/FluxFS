//! openraft type configuration stubs for MetaMaster.
//!
//! W1: declare types and keep Raft wiring behind this module so Storage/StateMachine
//! can be filled without reshaping MetaStore callers. Single-voter bring-up is next.

use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

/// Minimal application request logged through Raft (placeholder).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaRaftRequest {
    /// Opaque serialized MetaStore mutation (create/unlink/rename/put_inode…).
    ApplyBytes(Vec<u8>),
}

/// Minimal application response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaRaftResponse {
    pub ok: bool,
}

declare_raft_types!(
    pub FluxRaftTypeConfig:
        D = MetaRaftRequest,
        R = MetaRaftResponse,
        NodeId = NodeId,
        Node = (),
        Entry = openraft::Entry<FluxRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

use std::io::Cursor;
