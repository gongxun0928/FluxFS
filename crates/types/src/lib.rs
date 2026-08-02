//! Shared types for FluxFS MVP.
//!
//! Product labels Dirty/Clean/External/Ephemeral are UX-facing.
//! Implementation tracks BackingMode × data generation × per-extent residency.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

pub type InodeId = u64;
pub type Generation = u64;

/// Content-addressed chunk id (blake3).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId([u8; 32]);

impl ChunkId {
    pub fn from_bytes(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkId({})", &self.to_hex()[..16])
    }
}

/// Minimal hex helper without extra crate dependency on all paths.
mod hex {
    pub fn encode(bytes: [u8; 32]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(64);
        for b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xf) as usize] as char);
        }
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    Directory,
    Regular,
}

/// UX / protocol locality label (not a complete implementation state machine).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalityLabel {
    Dirty,
    Flushing,
    Clean,
    External,
    Ephemeral,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UfsObject {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub mtime_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Inode {
    pub id: InodeId,
    pub file_type: FileType,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_ms: i64,
    pub ctime_ms: i64,
    pub atime_ms: i64,
    pub link_count: u32,
    pub generation: Generation,
    pub locality: LocalityLabel,
    /// Authoritative UFS pointer for Clean/External; absent for Ephemeral/pure Dirty create.
    pub ufs: Option<UfsObject>,
    /// Root of durable Dirty extent map (opaque for W1; later a btree key / blob id).
    pub extent_root: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dentry {
    pub parent: InodeId,
    pub name: String,
    pub child: InodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Extent {
    pub offset: u64,
    pub len: u64,
    pub chunk: ChunkId,
}

#[derive(Debug, Error)]
pub enum FluxError {
    #[error("not found")]
    NotFound,
    #[error("already exists")]
    AlreadyExists,
    #[error("not a directory")]
    NotDirectory,
    #[error("is a directory")]
    IsDirectory,
    #[error("invalid argument: {0}")]
    InvalidArg(String),
    #[error("capability exceeded: {0}")]
    Capability(String),
    #[error("io: {0}")]
    Io(String),
    #[error("meta: {0}")]
    Meta(String),
    #[error("ufs: {0}")]
    Ufs(String),
}

pub type Result<T> = std::result::Result<T, FluxError>;

pub const ROOT_INODE: InodeId = 1;
/// Alpha cap for Dirty/Ephemeral writes and whole-object flush/copy-up (not External reads).
pub const DIRTY_WRITE_CAP_BYTES: u64 = 1 << 30;
