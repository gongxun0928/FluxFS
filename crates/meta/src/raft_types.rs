//! openraft type configuration for MetaMaster.
//!
//! Single-voter bring-up: mutating MetaStore ops are logged as [`MetaRaftRequest`]
//! and applied into [`crate::HeedMetaStore`]. Reads stay on Heed directly.

use openraft::declare_raft_types;
use openraft::BasicNode;
use openraft::LogId;
use openraft::StoredMembership;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use fluxfs_types::{
    FileType, FlushId, FlushIntent, FluxError, GcLeaseId, GcPlan, Inode, Manifest, RequestOpId,
    UfsObject,
};

pub type NodeId = u64;

/// Durable SM applied markers (stored alongside MetaStore data).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SmAppliedMeta {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
}

/// Application request logged through Raft (write path only).
///
/// `request_id` is optional for forward-compatible replay of older log entries;
/// new writers MUST set it so apply can retain/dedup results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaRaftRequest {
    Create {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        parent: u64,
        name: String,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        /// When set, CAS `parent.generation` before inserting the dentry.
        #[serde(default)]
        expected_parent_generation: Option<u64>,
    },
    PutInode {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        inode: Box<Inode>,
    },
    PutManifest {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        manifest: Box<Manifest>,
    },
    /// Store `manifest`, CAS `inode.generation == expected_generation`, then
    /// publish the updated inode head in the same SM apply / heed write txn.
    CommitInodeManifest {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        expected_generation: u64,
        inode: Box<Inode>,
        manifest: Box<Manifest>,
    },
    BeginFlush {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        expected_generation: u64,
        inode: u64,
        intent: Box<FlushIntent>,
    },
    CommitFlush {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        expected_generation: u64,
        inode: u64,
        flush_id: FlushId,
        published_ufs: Box<UfsObject>,
    },
    FailFlushConflict {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        expected_generation: u64,
        inode: u64,
        flush_id: FlushId,
        error: String,
    },
    BeginGc {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        lease_id: GcLeaseId,
    },
    FinishGc {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        lease_id: GcLeaseId,
    },
    /// Atomically allocate inode id, optional manifest, dentry, and External inode.
    ///
    /// `inode.id` in the template is ignored (server allocates). Manifest extents
    /// that reference inode id are rewritten to the allocated id.
    ImportExternal {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        parent: u64,
        name: String,
        inode: Box<Inode>,
        manifest: Option<Box<Manifest>>,
        #[serde(default)]
        expected_parent_generation: Option<u64>,
    },
    Unlink {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        parent: u64,
        name: String,
        #[serde(default)]
        expected_parent_generation: Option<u64>,
    },
}

impl MetaRaftRequest {
    pub fn request_id(&self) -> Option<&RequestOpId> {
        match self {
            Self::Create { request_id, .. }
            | Self::PutInode { request_id, .. }
            | Self::PutManifest { request_id, .. }
            | Self::CommitInodeManifest { request_id, .. }
            | Self::BeginFlush { request_id, .. }
            | Self::CommitFlush { request_id, .. }
            | Self::FailFlushConflict { request_id, .. }
            | Self::BeginGc { request_id, .. }
            | Self::FinishGc { request_id, .. }
            | Self::ImportExternal { request_id, .. }
            | Self::Unlink { request_id, .. } => request_id.as_ref(),
        }
    }

    pub fn with_request_id(mut self, id: RequestOpId) -> Self {
        match &mut self {
            Self::Create { request_id, .. }
            | Self::PutInode { request_id, .. }
            | Self::PutManifest { request_id, .. }
            | Self::CommitInodeManifest { request_id, .. }
            | Self::BeginFlush { request_id, .. }
            | Self::CommitFlush { request_id, .. }
            | Self::FailFlushConflict { request_id, .. }
            | Self::BeginGc { request_id, .. }
            | Self::FinishGc { request_id, .. }
            | Self::ImportExternal { request_id, .. }
            | Self::Unlink { request_id, .. } => {
                *request_id = Some(id);
            }
        }
        self
    }
}

/// Application response returned after apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaRaftResponse {
    Empty,
    Inode(Box<Inode>),
    ManifestId(u64),
    GcPlan(Box<GcPlan>),
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
