//! openraft type configuration for MetaMaster.
//!
//! Single-voter bring-up: mutating MetaStore ops are logged as [`MetaRaftRequest`]
//! and applied into [`crate::HeedMetaStore`]. Reads stay on Heed directly.

use openraft::declare_raft_types;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use fluxfs_types::{FileType, FluxError, Inode, Manifest};

pub type NodeId = u64;

/// Application request logged through Raft (write path only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaRaftRequest {
    Create {
        parent: u64,
        name: String,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    PutInode {
        inode: Box<Inode>,
    },
    PutManifest {
        manifest: Box<Manifest>,
    },
    Unlink {
        parent: u64,
        name: String,
    },
}

/// Application response returned after apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaRaftResponse {
    Empty,
    Inode(Box<Inode>),
    ManifestId(u64),
    Err(FluxError),
}

declare_raft_types!(
    pub FluxRaftTypeConfig:
        D = MetaRaftRequest,
        R = MetaRaftResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<FluxRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type FluxRaft = openraft::Raft<FluxRaftTypeConfig>;
