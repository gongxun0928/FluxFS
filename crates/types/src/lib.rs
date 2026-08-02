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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    /// Iterate extents overlapping `[offset, offset+len)`, in sorted order.
    ///
    /// Read-merge path uses this to dispatch per-extent IO: covered `Local`
    /// extents read from ChunkStore, covered `UfsRange` extents do pinned-version
    /// OpenDAL range GETs.
    pub fn extents_in_range(&self, offset: u64, len: u64) -> impl Iterator<Item = &Extent> {
        let end = offset.saturating_add(len);
        self.extents.iter().filter(move |e| {
            let e_end = e.offset().saturating_add(e.len());
            e.offset() < end && offset < e_end
        })
    }

    /// Build a new `Manifest` (gen = `new_gen`) by inserting `new_extent` over
    /// `[new_extent.offset(), new_extent.offset() + new_extent.len())`.
    ///
    /// Splitting rules (designed for Dirty copy-up where new_extent is Local):
    ///
    /// * Existing **UfsRange** partial overlap → split into head/tail, both keep
    ///   the pinned `ufs_version` and adjust `offset_in_object` for the tail.
    /// * Existing **UfsRange** fully covered → dropped.
    /// * Existing **Local** fully covered by `new_extent` → dropped.
    /// * Existing **Local** partially overlapped → `Err`. The caller must RMW
    ///   (read old chunk, overwrite bytes, put new chunk) before calling.
    ///   Sub-chunk-granularity references inside a Local chunk are not modeled
    ///   in W1 (no `internal_offset` field); see production audit.
    ///
    /// The new manifest's `size` grows to `max(old.size, new_end)` if the new
    /// extent extends beyond the current end. The result is validated before
    /// return.
    pub fn replace_range(&self, new_extent: Extent, new_gen: DataGen) -> Result<Manifest> {
        if new_extent.is_empty() {
            return Err(FluxError::InvalidArg(
                "replace_range: new_extent has len=0".into(),
            ));
        }
        let new_start = new_extent.offset();
        let new_end = new_start
            .checked_add(new_extent.len())
            .ok_or_else(|| FluxError::InvalidArg("new_extent len overflow".into()))?;

        let mut result: Vec<Extent> = Vec::with_capacity(self.extents.len() + 2);

        for existing in &self.extents {
            let ex_start = existing.offset();
            let ex_end = ex_start
                .checked_add(existing.len())
                .ok_or_else(|| FluxError::InvalidArg("existing extent len overflow".into()))?;

            // No overlap: existing entirely before or after new range.
            if ex_end <= new_start || ex_start >= new_end {
                result.push(existing.clone());
                continue;
            }

            match existing {
                Extent::UfsRange {
                    ufs_key,
                    ufs_version,
                    offset_in_object,
                    ..
                } => {
                    let obj_base = *offset_in_object;
                    // Head: [ex_start, min(ex_end, new_start)).
                    if ex_start < new_start {
                        let head_len = new_start - ex_start;
                        result.push(Extent::UfsRange {
                            offset: ex_start,
                            len: head_len,
                            ufs_key: ufs_key.clone(),
                            ufs_version: ufs_version.clone(),
                            offset_in_object: obj_base,
                        });
                    }
                    // Tail: [max(ex_start, new_end), ex_end).
                    if ex_end > new_end {
                        let tail_len = ex_end - new_end;
                        let tail_obj_offset = obj_base + (new_end - ex_start);
                        result.push(Extent::UfsRange {
                            offset: new_end,
                            len: tail_len,
                            ufs_key: ufs_key.clone(),
                            ufs_version: ufs_version.clone(),
                            offset_in_object: tail_obj_offset,
                        });
                    }
                    // Middle consumed by new_extent.
                }
                Extent::Local { .. } => {
                    if new_start <= ex_start && new_end >= ex_end {
                        // Fully covered by new; drop.
                    } else {
                        return Err(FluxError::InvalidArg(format!(
                            "Local extent [{ex_start},{ex_end}) partially overlapped by new \
                             [{new_start},{new_end}); caller must RMW to produce a fresh chunk"
                        )));
                    }
                }
            }
        }

        // Insert new_extent in sorted position. binary_search_by_key on offset
        // gives Ok(existing_pos) only when an extent starts exactly at new_start;
        // in that case we replace (drop the old fully-covered one already handled
        // above and re-insert here). Err(insert_pos) is the normal path.
        let insert_pos = result
            .binary_search_by_key(&new_start, Extent::offset)
            .unwrap_or_else(|p| p);
        result.insert(insert_pos, new_extent);

        let new_size = std::cmp::max(self.size, new_end);
        let m = Manifest {
            inode: self.inode,
            gen: new_gen,
            size: new_size,
            extents: result,
        };
        m.validate()?;
        Ok(m)
    }
}

// ===== Errors =====

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
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
    #[error("read-only filesystem")]
    ReadOnly,
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

    fn ufs_range(offset: u64, len: u64) -> Extent {
        Extent::UfsRange {
            offset,
            len,
            ufs_key: "obj".into(),
            ufs_version: UfsVersion("etag-1".into()),
            offset_in_object: offset,
        }
    }

    fn local(offset: u64, len: u64, byte: u8) -> Extent {
        Extent::Local {
            offset,
            len,
            chunk: ChunkId([byte; 32]),
        }
    }

    #[test]
    fn extents_in_range_returns_only_overlapping() {
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 200,
            extents: vec![ufs_range(0, 50), local(50, 50, 1), ufs_range(100, 100)],
        };
        // [40, 90) overlaps extents at 0 and 50, not 100.
        let hits: Vec<&Extent> = m.extents_in_range(40, 50).collect();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].offset(), 0);
        assert_eq!(hits[1].offset(), 50);

        // Adjacent-only range (touching boundary) does not overlap.
        let touches: Vec<&Extent> = m.extents_in_range(50, 0).collect();
        // len=0 → end == start, no extent can strictly overlap a zero-wide window.
        assert_eq!(touches.len(), 0);

        // Range entirely after last extent.
        let after: Vec<&Extent> = m.extents_in_range(300, 10).collect();
        assert_eq!(after.len(), 0);
    }

    #[test]
    fn replace_range_splits_ufs_range_head_and_tail() {
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 100,
            extents: vec![ufs_range(0, 100)],
        };
        let new = local(10, 50, 9);
        let m2 = m.replace_range(new, DataGen(2)).expect("split ok");
        assert_eq!(m2.gen, DataGen(2));
        assert_eq!(m2.size, 100);
        assert_eq!(m2.extents.len(), 3, "{:?}", m2.extents);
        // head [0,10)
        assert_eq!(m2.extents[0].offset(), 0);
        assert_eq!(m2.extents[0].len(), 10);
        // inserted Local [10,60)
        assert_eq!(m2.extents[1].offset(), 10);
        assert_eq!(m2.extents[1].len(), 50);
        // tail [60,100) — ufs_version preserved, offset_in_object adjusted.
        match &m2.extents[2] {
            Extent::UfsRange {
                offset,
                len,
                ufs_version,
                offset_in_object,
                ..
            } => {
                assert_eq!(*offset, 60);
                assert_eq!(*len, 40);
                assert_eq!(ufs_version.0, "etag-1");
                assert_eq!(*offset_in_object, 60);
            }
            _ => panic!("expected UfsRange tail"),
        }
    }

    #[test]
    fn replace_range_drops_fully_covered_ufs_range() {
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 100,
            extents: vec![ufs_range(0, 100)],
        };
        // New covers the entire UfsRange.
        let m2 = m
            .replace_range(local(0, 100, 9), DataGen(2))
            .expect("full cover ok");
        assert_eq!(m2.extents.len(), 1);
        assert!(matches!(m2.extents[0], Extent::Local { .. }));
    }

    #[test]
    fn replace_range_drops_fully_covered_local() {
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 100,
            extents: vec![local(0, 100, 1)],
        };
        let m2 = m
            .replace_range(local(0, 100, 9), DataGen(2))
            .expect("full cover local ok");
        assert_eq!(m2.extents.len(), 1);
        match m2.extents[0] {
            Extent::Local { chunk, .. } => assert_eq!(chunk, ChunkId([9; 32])),
            _ => panic!(),
        }
    }

    #[test]
    fn replace_range_rejects_partial_local_overlap() {
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 100,
            extents: vec![local(0, 100, 1)],
        };
        // Partial overlap on the right side.
        let err = m.replace_range(local(50, 100, 9), DataGen(2));
        assert!(err.is_err());
        // Partial overlap on the left side.
        let err = m.replace_range(local(0, 50, 9), DataGen(2));
        assert!(err.is_err());
        // Inner island.
        let err = m.replace_range(local(25, 50, 9), DataGen(2));
        assert!(err.is_err());
    }

    #[test]
    fn replace_range_keeps_adjacent_extents_without_misjudging_overlap() {
        // Two UfsRanges adjacent: [0,50) and [50,100). Insert Local at [50,100)
        // must NOT split the first one (it's merely adjacent, not overlapping).
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 100,
            extents: vec![ufs_range(0, 50), ufs_range(50, 50)],
        };
        let m2 = m
            .replace_range(local(50, 50, 9), DataGen(2))
            .expect("adjacent not overlap");
        assert_eq!(m2.extents.len(), 2);
        assert_eq!(m2.extents[0].offset(), 0);
        assert_eq!(m2.extents[0].len(), 50);
        assert!(matches!(m2.extents[1], Extent::Local { .. }));
    }

    #[test]
    fn replace_range_extends_size_when_beyond_eof() {
        let m = Manifest {
            inode: 1,
            gen: DataGen(1),
            size: 100,
            extents: vec![ufs_range(0, 100)],
        };
        let m2 = m
            .replace_range(local(50, 200, 9), DataGen(2))
            .expect("extend ok");
        assert_eq!(m2.size, 250);
        // Head [0,50), new Local [50,250).
        assert_eq!(m2.extents.len(), 2);
    }

    #[test]
    fn replace_range_into_empty_manifest_inserts() {
        let m = Manifest::empty(1, DataGen(1));
        let m2 = m
            .replace_range(local(0, 100, 9), DataGen(2))
            .expect("insert ok");
        assert_eq!(m2.extents.len(), 1);
        assert_eq!(m2.size, 100);
    }

    #[test]
    fn replace_range_rejects_zero_len() {
        let m = Manifest::empty(1, DataGen(1));
        let err = m.replace_range(local(0, 0, 9), DataGen(2));
        assert!(err.is_err());
    }
}
