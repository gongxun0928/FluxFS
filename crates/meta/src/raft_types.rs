//! openraft type configuration for MetaMaster.
//!
//! Single-voter bring-up: mutating MetaStore ops are logged as [`MetaRaftRequest`]
//! and applied into [`crate::HeedMetaStore`]. Reads stay on Heed directly.

use fluxfs_types::{
    ChunkId, FileType, FlushId, FlushIntent, FluxError, GcBatch, GcLeaseId, GcPlan, Inode,
    Manifest, RequestOpId, UfsObject, WorkerMembership, WorkerRegistration, WorkerTargetId,
    WriteTicketId,
};
use openraft::declare_raft_types;
use openraft::BasicNode;
use openraft::LogId;
use openraft::StoredMembership;
use serde::{Deserialize, Serialize};

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
    RegisterWorker {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        registration: WorkerRegistration,
    },
    Create {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
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
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        inode: Box<Inode>,
    },
    /// CAS-update inode metadata without allocating/replacing a manifest.
    PutInodeCas {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        expected_generation: u64,
        inode: Box<Inode>,
    },
    PutManifest {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        manifest: Box<Manifest>,
    },
    /// Store `manifest`, CAS `inode.generation == expected_generation`, then
    /// publish the updated inode head in the same SM apply / heed write txn.
    CommitInodeManifest {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        expected_generation: u64,
        inode: Box<Inode>,
        manifest: Box<Manifest>,
    },
    ReserveChunks {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        ticket: WriteTicketId,
        inode: u64,
        expected_generation: u64,
        chunks: Vec<ChunkId>,
        #[serde(default)]
        expires_at_unix_ms: u64,
    },
    AbortChunkReservation {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        ticket: WriteTicketId,
    },
    ExpireChunkReservations {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        cutoff_unix_ms: u64,
        max_to_expire: u64,
    },
    /// Drop expired `client_requests` ledger rows (deterministic cutoff).
    PruneClientRequests {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        cutoff_unix_ms: u64,
        max_to_prune: u64,
    },
    CommitInodeManifestReserved {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        ticket: WriteTicketId,
        expected_generation: u64,
        inode: Box<Inode>,
        manifest: Box<Manifest>,
    },
    TombstoneGcBatch {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        candidates: Vec<ChunkId>,
    },
    FinalizeGcTombstones {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        chunks: Vec<ChunkId>,
    },
    InitializeGcDeleteTargets {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        chunks: Vec<ChunkId>,
        targets: Vec<WorkerTargetId>,
    },
    AcknowledgeGcDeletes {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        deleted: Vec<(ChunkId, WorkerTargetId)>,
    },
    BeginFlush {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        expected_generation: u64,
        inode: u64,
        intent: Box<FlushIntent>,
    },
    CommitFlush {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        expected_generation: u64,
        inode: u64,
        flush_id: FlushId,
        published_ufs: Box<UfsObject>,
    },
    FailFlushConflict {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        expected_generation: u64,
        inode: u64,
        flush_id: FlushId,
        error: String,
    },
    BeginGc {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        lease_id: GcLeaseId,
    },
    FinishGc {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        lease_id: GcLeaseId,
    },
    /// Atomically allocate inode id, optional manifest, dentry, and External inode.
    ///
    /// `inode.id` in the template is ignored (server allocates). Manifest extents
    /// that reference inode id are rewritten to the allocated id.
    ImportExternal {
        #[serde(default)]
        request_id: Option<RequestOpId>,
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
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
        /// Leader-sampled wall time for ledger created/expires (0 = legacy).
        #[serde(default)]
        ledger_now_unix_ms: u64,
        parent: u64,
        name: String,
        #[serde(default)]
        expected_parent_generation: Option<u64>,
    },
}

impl MetaRaftRequest {
    pub fn request_id(&self) -> Option<&RequestOpId> {
        match self {
            Self::RegisterWorker { request_id, .. }
            | Self::Create { request_id, .. }
            | Self::PutInode { request_id, .. }
            | Self::PutInodeCas { request_id, .. }
            | Self::PutManifest { request_id, .. }
            | Self::CommitInodeManifest { request_id, .. }
            | Self::ReserveChunks { request_id, .. }
            | Self::AbortChunkReservation { request_id, .. }
            | Self::ExpireChunkReservations { request_id, .. }
            | Self::PruneClientRequests { request_id, .. }
            | Self::CommitInodeManifestReserved { request_id, .. }
            | Self::TombstoneGcBatch { request_id, .. }
            | Self::FinalizeGcTombstones { request_id, .. }
            | Self::InitializeGcDeleteTargets { request_id, .. }
            | Self::AcknowledgeGcDeletes { request_id, .. }
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
            Self::RegisterWorker { request_id, .. }
            | Self::Create { request_id, .. }
            | Self::PutInode { request_id, .. }
            | Self::PutInodeCas { request_id, .. }
            | Self::PutManifest { request_id, .. }
            | Self::CommitInodeManifest { request_id, .. }
            | Self::ReserveChunks { request_id, .. }
            | Self::AbortChunkReservation { request_id, .. }
            | Self::ExpireChunkReservations { request_id, .. }
            | Self::PruneClientRequests { request_id, .. }
            | Self::CommitInodeManifestReserved { request_id, .. }
            | Self::TombstoneGcBatch { request_id, .. }
            | Self::FinalizeGcTombstones { request_id, .. }
            | Self::InitializeGcDeleteTargets { request_id, .. }
            | Self::AcknowledgeGcDeletes { request_id, .. }
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

    /// Leader-sampled wall time carried in the log entry (0 on pre-stamp legacy).
    pub fn ledger_now_unix_ms(&self) -> u64 {
        match self {
            Self::RegisterWorker {
                ledger_now_unix_ms, ..
            }
            | Self::Create {
                ledger_now_unix_ms, ..
            }
            | Self::PutInode {
                ledger_now_unix_ms, ..
            }
            | Self::PutInodeCas {
                ledger_now_unix_ms, ..
            }
            | Self::PutManifest {
                ledger_now_unix_ms, ..
            }
            | Self::CommitInodeManifest {
                ledger_now_unix_ms, ..
            }
            | Self::ReserveChunks {
                ledger_now_unix_ms, ..
            }
            | Self::AbortChunkReservation {
                ledger_now_unix_ms, ..
            }
            | Self::ExpireChunkReservations {
                ledger_now_unix_ms, ..
            }
            | Self::PruneClientRequests {
                ledger_now_unix_ms, ..
            }
            | Self::CommitInodeManifestReserved {
                ledger_now_unix_ms, ..
            }
            | Self::TombstoneGcBatch {
                ledger_now_unix_ms, ..
            }
            | Self::FinalizeGcTombstones {
                ledger_now_unix_ms, ..
            }
            | Self::InitializeGcDeleteTargets {
                ledger_now_unix_ms, ..
            }
            | Self::AcknowledgeGcDeletes {
                ledger_now_unix_ms, ..
            }
            | Self::BeginFlush {
                ledger_now_unix_ms, ..
            }
            | Self::CommitFlush {
                ledger_now_unix_ms, ..
            }
            | Self::FailFlushConflict {
                ledger_now_unix_ms, ..
            }
            | Self::BeginGc {
                ledger_now_unix_ms, ..
            }
            | Self::FinishGc {
                ledger_now_unix_ms, ..
            }
            | Self::ImportExternal {
                ledger_now_unix_ms, ..
            }
            | Self::Unlink {
                ledger_now_unix_ms, ..
            } => *ledger_now_unix_ms,
        }
    }

    pub fn with_ledger_now(mut self, now_unix_ms: u64) -> Self {
        match &mut self {
            Self::RegisterWorker {
                ledger_now_unix_ms, ..
            }
            | Self::Create {
                ledger_now_unix_ms, ..
            }
            | Self::PutInode {
                ledger_now_unix_ms, ..
            }
            | Self::PutInodeCas {
                ledger_now_unix_ms, ..
            }
            | Self::PutManifest {
                ledger_now_unix_ms, ..
            }
            | Self::CommitInodeManifest {
                ledger_now_unix_ms, ..
            }
            | Self::ReserveChunks {
                ledger_now_unix_ms, ..
            }
            | Self::AbortChunkReservation {
                ledger_now_unix_ms, ..
            }
            | Self::ExpireChunkReservations {
                ledger_now_unix_ms, ..
            }
            | Self::PruneClientRequests {
                ledger_now_unix_ms, ..
            }
            | Self::CommitInodeManifestReserved {
                ledger_now_unix_ms, ..
            }
            | Self::TombstoneGcBatch {
                ledger_now_unix_ms, ..
            }
            | Self::FinalizeGcTombstones {
                ledger_now_unix_ms, ..
            }
            | Self::InitializeGcDeleteTargets {
                ledger_now_unix_ms, ..
            }
            | Self::AcknowledgeGcDeletes {
                ledger_now_unix_ms, ..
            }
            | Self::BeginFlush {
                ledger_now_unix_ms, ..
            }
            | Self::CommitFlush {
                ledger_now_unix_ms, ..
            }
            | Self::FailFlushConflict {
                ledger_now_unix_ms, ..
            }
            | Self::BeginGc {
                ledger_now_unix_ms, ..
            }
            | Self::FinishGc {
                ledger_now_unix_ms, ..
            }
            | Self::ImportExternal {
                ledger_now_unix_ms, ..
            }
            | Self::Unlink {
                ledger_now_unix_ms, ..
            } => {
                *ledger_now_unix_ms = now_unix_ms;
            }
        }
        self
    }

    /// Stable operation label for tracing / metrics (`op=` field).
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::RegisterWorker { .. } => "register_worker",
            Self::Create { .. } => "create",
            Self::PutInode { .. } => "put_inode",
            Self::PutInodeCas { .. } => "put_inode_cas",
            Self::PutManifest { .. } => "put_manifest",
            Self::CommitInodeManifest { .. } => "commit_inode_manifest",
            Self::ReserveChunks { .. } => "reserve_chunks",
            Self::AbortChunkReservation { .. } => "abort_chunk_reservation",
            Self::ExpireChunkReservations { .. } => "expire_chunk_reservations",
            Self::PruneClientRequests { .. } => "prune_client_requests",
            Self::CommitInodeManifestReserved { .. } => "commit_inode_manifest_reserved",
            Self::TombstoneGcBatch { .. } => "tombstone_gc_batch",
            Self::FinalizeGcTombstones { .. } => "finalize_gc_tombstones",
            Self::InitializeGcDeleteTargets { .. } => "initialize_gc_delete_targets",
            Self::AcknowledgeGcDeletes { .. } => "acknowledge_gc_deletes",
            Self::BeginFlush { .. } => "begin_flush",
            Self::CommitFlush { .. } => "commit_flush",
            Self::FailFlushConflict { .. } => "fail_flush_conflict",
            Self::BeginGc { .. } => "begin_gc",
            Self::FinishGc { .. } => "finish_gc",
            Self::ImportExternal { .. } => "import_external",
            Self::Unlink { .. } => "unlink",
        }
    }
}

/// Application response returned after apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaRaftResponse {
    Empty,
    Inode(Box<Inode>),
    ManifestId(u64),
    GcPlan(Box<GcPlan>),
    GcBatch(Box<GcBatch>),
    WorkerMembership(Box<WorkerMembership>),
    Err(FluxError),
}

declare_raft_types!(
    pub FluxRaftTypeConfig:
        D = MetaRaftRequest,
        R = MetaRaftResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<FluxRaftTypeConfig>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type FluxRaft = openraft::Raft<FluxRaftTypeConfig>;
