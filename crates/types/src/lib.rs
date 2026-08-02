//! Shared types for FluxFS MVP.
//!
//! Product labels Dirty/Clean/External/Ephemeral are UX-facing (derived).
//! Implementation tracks orthogonal dimensions:
//! `BackingMode × DataState × OpState × Origin` + per-extent residency.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

pub type InodeId = u64;
pub type Generation = u64;

/// Per-inode data generation. `head_gen > ufs_gen` ⇒ Dirty.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct DataGen(pub u64);

/// Pinned UFS version (S3 VersionId when bucket has versioning; ETag otherwise —
/// best-effort under `external-consistency = best-effort`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UfsVersion(pub String);

/// Manifest blob id within MetaStore's manifest keyspace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ManifestId(pub u64);

/// Unique id per flush attempt. Recovery uses (flush_id, target_digest) to
/// determine whether a UFS Put succeeded while the metadata commit was lost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlushId(pub u64);

/// In-flight External lazy-load tracking token.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HydrateToken(pub u64);

/// Logical chunk address within a file. Byte offset = index × CHUNK_SIZE.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct ChunkIndex(pub u32);

/// Content-addressed chunk id (blake3).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId([u8; 32]);

impl ChunkId {
    /// Content-addressed id: blake3(data).
    pub fn from_bytes(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    /// Parse a raw 32-byte chunk id (wire / RPC).
    pub fn from_raw(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl TryFrom<&[u8]> for ChunkId {
    type Error = FluxError;

    fn try_from(bytes: &[u8]) -> std::result::Result<Self, Self::Error> {
        let arr: [u8; 32] = bytes.try_into().map_err(|_| {
            FluxError::InvalidArg(format!("chunk id must be 32 bytes, got {}", bytes.len()))
        })?;
        Ok(ChunkId::from_raw(arr))
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

/// UX / protocol locality label (derived from `LocalityFields`).
///
/// Not the source of truth — call `LocalityLabel::derive(...)` from the
/// orthogonal fields rather than mutating this label directly when the
/// implementation state changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalityLabel {
    Dirty,
    Flushing,
    Clean,
    External,
    Ephemeral,
    /// External UFS version drift detected during Dirty; user intervention required.
    DirtyConflict,
}

impl LocalityLabel {
    /// Derive the UX label from implementation-level orthogonal fields.
    /// See `docs/locality-state.md` for the full state-transition table.
    pub fn derive(f: &LocalityFields, head_gen: DataGen, ufs_gen: DataGen) -> Self {
        if matches!(f.backing_mode, BackingMode::Ephemeral) {
            return LocalityLabel::Ephemeral;
        }
        if matches!(f.op_state, OpState::Flushing { .. }) {
            return LocalityLabel::Flushing;
        }
        if matches!(f.data_state, DataState::DirtyConflict) {
            return LocalityLabel::DirtyConflict;
        }
        match f.data_state {
            DataState::Dirty => LocalityLabel::Dirty,
            DataState::UfsClean if head_gen > ufs_gen => LocalityLabel::Dirty, // defensive
            DataState::UfsClean => {
                if matches!(f.origin, Origin::Imported) {
                    LocalityLabel::External
                } else {
                    LocalityLabel::Clean
                }
            }
            DataState::DirtyConflict => LocalityLabel::DirtyConflict,
            DataState::Ephemeral => LocalityLabel::Ephemeral,
        }
    }
}

// ===== Implementation-level orthogonal state (@ubuntu-gpt56 model) =====

/// Implementation-level orthogonal fields persisted in `Inode`. The product-level
/// `LocalityLabel` is *derived* from these, not stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalityFields {
    pub backing_mode: BackingMode,
    pub data_state: DataState,
    pub op_state: OpState,
    pub origin: Origin,
}

impl Default for LocalityFields {
    fn default() -> Self {
        // Default: a newly FluxFS-created inode with no UFS backing yet.
        // Note: `Ephemeral` mount will override `backing_mode` at create time.
        Self {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::UfsClean,
            op_state: OpState::None,
            origin: Origin::FluxCreated,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackingMode {
    /// FluxFS may flush to UFS; Clean/Dirty/Flushing/Conflict possible.
    UfsBacked,
    /// Mount `--no-ufs`; data lives only in FluxFS write cache.
    Ephemeral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataState {
    /// head_gen == ufs_gen, no pending modifications, no durable flush intent.
    UfsClean,
    /// head_gen > ufs_gen; local chunks hold authoritative bytes.
    Dirty,
    /// External mutation detected while Dirty (ufs_base_version drift).
    DirtyConflict,
    /// Used only when BackingMode=Ephemeral.
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpState {
    /// No background op in flight.
    None,
    /// External lazy-load in progress for a range.
    Hydrating { token: HydrateToken },
    /// Flusher durable-intent recorded; UFS Put in flight or pending commit.
    Flushing { intent: FlushIntent },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Origin {
    /// Created by FluxFS (mkdir/create/rename).
    FluxCreated,
    /// Lazy-imported from UFS on first lookup/readdir (TTL-cache-able, rebuildable).
    Imported,
}

/// Durable flush intent persisted BEFORE UFS Put. Recovery can replay this intent
/// to determine whether a Put succeeded but metadata commit was lost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushIntent {
    pub flush_id: FlushId,
    pub snapshot_gen: DataGen,
    /// Root hash of the snapshot's extent map (content-addressed).
    pub snapshot_manifest_root: ChunkId,
    /// Expected UFS base version for conditional Put (None = first flush).
    pub expected_ufs_version: Option<UfsVersion>,
    /// Hash of the reconstructed UFS object bytes — used for post-Put HEAD verification.
    pub target_digest: ChunkId,
}

// ===== UFS object reference =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UfsObject {
    pub key: String,
    pub size: u64,
    /// ETag for S3; analogous opaque version string for other backends.
    pub etag: Option<String>,
    pub mtime_ms: Option<i64>,
}

// ===== Inode =====

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

    // ===== Generation / versioning =====
    /// Latest manifest generation (incremented on every committed write/truncate).
    pub generation: Generation,

    /// Data-only generation (separate from metadata `generation` to keep
    /// clean/dirty derivation precise). Defaults to 0 = `DataGen(0)` for
    /// backward compatibility with legacy serialized inodes.
    #[serde(default)]
    pub head_gen: DataGen,
    /// Last generation durable to UFS. `head_gen > ufs_gen` ⇒ Dirty.
    #[serde(default)]
    pub ufs_gen: DataGen,
    /// Pinned UFS version for partial-copy-up reads. Required for External,
    /// optional for Dirty (set on first flush).
    #[serde(default)]
    pub ufs_base_version: Option<UfsVersion>,

    // ===== Locality (source of truth = orthogonal fields) =====
    /// Derived UX label — convenience for logs/UI. Use `LocalityLabel::derive`
    /// whenever the underlying fields change.
    pub locality: LocalityLabel,
    /// Implementation-level orthogonal fields. May be missing on legacy
    /// serialized inodes; reconstruct from `locality` in that case.
    #[serde(default)]
    pub locality_fields: Option<LocalityFields>,

    // ===== Manifest =====
    /// Authoritative UFS pointer for Clean/External; absent for Ephemeral/pure Dirty create.
    pub ufs: Option<UfsObject>,
    /// Root of durable Dirty extent map. Opaque for W1; later a btree key / blob id.
    pub extent_root: Option<u64>,
    /// Stable manifest blob id (within MetaStore manifest keyspace).
    /// None for directories / empty files (W1).
    #[serde(default)]
    pub manifest_id: Option<ManifestId>,

    // ===== Flush tracking =====
    #[serde(default)]
    pub flush_intent: Option<FlushIntent>,
    /// Last error from flush/copy-up; surfaces to user on next op.
    #[serde(default)]
    pub last_error: Option<String>,
}

// ===== Dentry =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dentry {
    pub parent: InodeId,
    pub name: String,
    pub child: InodeId,
}

impl Dentry {
    /// Validate POSIX dentry constraints. Returns Err with reason on violation.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(FluxError::InvalidArg("empty dentry name".into()));
        }
        if self.name.contains('/') {
            return Err(FluxError::InvalidArg(format!(
                "dentry name contains '/': {}",
                self.name
            )));
        }
        if self.name == "." || self.name == ".." {
            return Err(FluxError::InvalidArg(format!(
                "reserved dentry name: {}",
                self.name
            )));
        }
        if self.name.len() > 255 {
            return Err(FluxError::InvalidArg(format!(
                "dentry name too long: {} bytes (NAME_MAX=255)",
                self.name.len()
            )));
        }
        Ok(())
    }
}

// ===== Extents =====

/// Extent in a manifest. Dual-form: FluxFS-owned chunks (Dirty/Ephemeral/
/// Clean-cache) vs UFS-referenced bytes (Clean/External unmodified range).
///
/// The UfsRange variant MUST pin a specific `ufs_version` for partial-copy-up
/// correctness — see `external-consistency = best-effort` boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Extent {
    /// FluxFS ChunkStore-owned bytes.
    Local {
        /// Logical offset within file (bytes).
        offset: u64,
        len: u64,
        chunk: ChunkId,
    },
    /// Bytes still referenced from UFS (unmodified range of an External object).
    UfsRange {
        offset: u64,
        len: u64,
        /// S3 object key / UFS path.
        ufs_key: String,
        /// Pinned version (S3 VersionId or ETag).
        ufs_version: UfsVersion,
        /// Byte offset within the UFS object.
        offset_in_object: u64,
    },
}

impl Extent {
    pub fn offset(&self) -> u64 {
        match self {
            Extent::Local { offset, .. } => *offset,
            Extent::UfsRange { offset, .. } => *offset,
        }
    }
    pub fn len(&self) -> u64 {
        match self {
            Extent::Local { len, .. } => *len,
            Extent::UfsRange { len, .. } => *len,
        }
    }
    /// Extents are never zero-length by construction; this exists to satisfy
    /// the `len/is_empty` clippy lint and to support future integrity checks.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ===== Manifest =====

/// Immutable extent-map snapshot, identified by (inode, gen, root_hash).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub inode: InodeId,
    pub gen: DataGen,
    pub size: u64,
    /// Sorted by offset, non-overlapping.
    pub extents: Vec<Extent>,
}

impl Manifest {
    /// Validate that extents are sorted by offset and non-overlapping.
    pub fn validate(&self) -> Result<()> {
        let mut prev_end: Option<u64> = None;
        for e in &self.extents {
            let start = e.offset();
            let end = start + e.len();
            if let Some(pe) = prev_end {
                if start < pe {
                    return Err(FluxError::InvalidArg(format!(
                        "extent overlap at offset {} (prev_end={})",
                        start, pe
                    )));
                }
            }
            prev_end = Some(end);
        }
        Ok(())
    }

    /// Empty manifest for a fresh regular file.
    pub fn empty(inode: InodeId, gen: DataGen) -> Self {
        Self {
            inode,
            gen,
            size: 0,
            extents: Vec::new(),
        }
    }
}

// ===== Errors =====

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
    #[error("CAS failed: expected={expected} actual={actual}")]
    CasFailed { expected: u64, actual: u64 },
    #[error("dirty conflict: external UFS version drift")]
    DirtyConflict,
    #[error("inode busy: op in flight")]
    Busy,
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

/// Default chunk size for MVP. Extents are aligned to this.
pub const CHUNK_SIZE: u64 = 4 * 1024 * 1024; // 4 MiB

/// Default TTL for External lazy-imported dentry cache.
pub const EXTERNAL_CACHE_TTL_SECS: u64 = 60;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_raw_wire_roundtrip() {
        let expected = ChunkId::from_bytes(b"wire roundtrip");
        let decoded = ChunkId::try_from(expected.as_bytes().as_slice()).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn chunk_id_wire_length_is_strict() {
        for len in [0, 31, 33, 64] {
            assert!(ChunkId::try_from(vec![0u8; len].as_slice()).is_err());
        }
        assert!(ChunkId::try_from([0u8; 32].as_slice()).is_ok());
    }

    #[test]
    fn dirty_label_derives_from_data_state() {
        let f = LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::Dirty,
            op_state: OpState::None,
            origin: Origin::FluxCreated,
        };
        assert_eq!(
            LocalityLabel::derive(&f, DataGen(2), DataGen(1)),
            LocalityLabel::Dirty
        );
    }

    #[test]
    fn clean_label_when_gens_equal_and_flux_created() {
        let f = LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::UfsClean,
            op_state: OpState::None,
            origin: Origin::FluxCreated,
        };
        assert_eq!(
            LocalityLabel::derive(&f, DataGen(5), DataGen(5)),
            LocalityLabel::Clean
        );
    }

    #[test]
    fn external_label_when_imported() {
        let f = LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::UfsClean,
            op_state: OpState::None,
            origin: Origin::Imported,
        };
        assert_eq!(
            LocalityLabel::derive(&f, DataGen(5), DataGen(5)),
            LocalityLabel::External
        );
    }

    #[test]
    fn ephemeral_overrides_everything() {
        let f = LocalityFields {
            backing_mode: BackingMode::Ephemeral,
            data_state: DataState::Ephemeral,
            op_state: OpState::None,
            origin: Origin::FluxCreated,
        };
        assert_eq!(
            LocalityLabel::derive(&f, DataGen(1), DataGen(0)),
            LocalityLabel::Ephemeral
        );
    }

    #[test]
    fn flushing_overrides_dirty() {
        let intent = FlushIntent {
            flush_id: FlushId(1),
            snapshot_gen: DataGen(5),
            snapshot_manifest_root: ChunkId([0; 32]),
            expected_ufs_version: None,
            target_digest: ChunkId([0; 32]),
        };
        let f = LocalityFields {
            backing_mode: BackingMode::UfsBacked,
            data_state: DataState::Dirty,
            op_state: OpState::Flushing { intent },
            origin: Origin::FluxCreated,
        };
        assert_eq!(
            LocalityLabel::derive(&f, DataGen(6), DataGen(5)),
            LocalityLabel::Flushing
        );
    }

    #[test]
    fn dentry_validate_rejects_bad_names() {
        let bad = |name: &str| Dentry {
            parent: 1,
            name: name.into(),
            child: 2,
        };
        assert!(bad("").validate().is_err());
        assert!(bad("a/b").validate().is_err());
        assert!(bad(".").validate().is_err());
        assert!(bad("..").validate().is_err());
        assert!(bad(&"a".repeat(256)).validate().is_err());
        assert!(bad("ok").validate().is_ok());
    }

    #[test]
    fn manifest_validate_detects_overlap() {
        let chunk = ChunkId([1; 32]);
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 2 * CHUNK_SIZE,
            extents: vec![
                Extent::Local {
                    offset: 0,
                    len: CHUNK_SIZE,
                    chunk,
                },
                Extent::Local {
                    offset: 0,
                    len: CHUNK_SIZE,
                    chunk,
                },
            ],
        };
        assert!(m.validate().is_err());
    }

    #[test]
    fn manifest_validate_accepts_sorted() {
        let chunk = ChunkId([1; 32]);
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 2 * CHUNK_SIZE,
            extents: vec![
                Extent::Local {
                    offset: 0,
                    len: CHUNK_SIZE,
                    chunk,
                },
                Extent::Local {
                    offset: CHUNK_SIZE,
                    len: CHUNK_SIZE,
                    chunk,
                },
            ],
        };
        assert!(m.validate().is_ok());
    }

    #[test]
    fn legacy_inode_deserialize_without_new_fields() {
        // Legacy JSON: only old fields, no head_gen/ufs_gen/locality_fields/etc.
        let legacy = serde_json::json!({
            "id": 1u64,
            "file_type": "Directory",
            "mode": 0o755,
            "uid": 0,
            "gid": 0,
            "size": 0,
            "mtime_ms": 0,
            "ctime_ms": 0,
            "atime_ms": 0,
            "link_count": 2,
            "generation": 1,
            "locality": "Ephemeral",
            "ufs": null,
            "extent_root": null
        });
        let parsed: Inode = serde_json::from_value(legacy).expect("deserialize legacy");
        assert_eq!(parsed.head_gen, DataGen(0));
        assert_eq!(parsed.ufs_gen, DataGen(0));
        assert!(parsed.locality_fields.is_none());
        assert!(parsed.manifest_id.is_none());
    }
}
