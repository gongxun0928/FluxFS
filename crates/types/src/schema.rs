//! Per-record schema versioning envelope (C4, task #32).
//!
//! FluxFS now has three orthogonal schema-version layers; this module owns the
//! third, codec-level one:
//!
//! 1. **DB-level** (`fluxfs_meta::schema::CURRENT_META_SCHEMA_VERSION`): a
//!    single `u32` stored at `meta_schema_version` in the heed KV, governing
//!    whole-DB layout migrations (e.g. enabling extent-tree manifests). Owned
//!    by B1/B2 — **do not** touch from here.
//! 2. **Per-payload** (e.g. `ExtentTree::WIRE_VERSION`): self-describing
//!    wire form for a single nested structure. B2's `ExtentTree` accepts
//!    both the legacy bare `Vec<Extent>` and the versioned
//!    `{version, entries}` form.
//! 3. **Per-record codec envelope** (THIS module): wraps every top-level
//!    value flowing through `meta_codec::{encode,decode}_*` with
//!    `{schema_version, payload}`. Started at v1.
//!
//! ## Compatibility policy
//!
//! * **New binary reading old data** (legacy bare-JSON, no envelope): the
//!   envelope deserialize fails on the missing `payload` key and we fall
//!   back to direct `T` deserialization, stamping `LEGACY` (= 0). B2's
//!   tolerant deserializers (e.g. `ExtentTree`) make this work transparently
//!   for layout-compatible extensions.
//! * **Old binary reading new data**: serde's default-tolerant structs
//!   absorb unknown fields; the envelope wrapper is invisible to an old
//!   binary only if that binary also uses the same codec. RPC details path
//!   (`tonic::Status`) is unaffected — only the JSON payload contracts
//!   inside `meta_codec` change.
//! * **New binary reading data from an even newer binary**: detected via
//!   `schema_version > CURRENT` and rejected with a clear error rather
//!   than silently corrupting state.
//! * **Breaking layout changes**: override [`Versioned::migrate_from`] to
//!   hand-translate older payload bytes into the current shape.
//!
//! ## Phase 1 scope (this commit)
//!
//! Ships the envelope, trait, encode/decode helpers, per-type impls, test
//! matrix. Wires `meta_codec::{encode,decode}_inode|manifest|flush_intent|
//! dentries` to the envelope. Other codec functions (GC plan/batch/tombstone,
//! chunk-id lists, worker-target lists) migrate incrementally as those
//! subsystems are touched — not blocking this commit. Raw `serde_json::*`
//! callsites in `heed_store` / `raft_log_store` likewise convert
//! incrementally.
//!
//! ## Field-name invariant
//!
//! Types participating in [`Versioned`] MUST NOT declare a field named
//! `payload` of their own, or envelope detection may misfire on bare legacy
//! JSON. None of FluxFS's persisted types currently do; adding such a field
//! requires renaming the envelope key or wrapping the type.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt;

use crate::{
    ChunkId, DedupEntry, Dentry, FlushIntent, FluxError, GcBatch, GcPlan, GcTombstone, Inode,
    InodeId, Manifest, Result, UfsObject, WorkerTargetId,
};

/// Monotonic codec schema stamp. `LEGACY` (= 0) denotes pre-versioning data
/// written before this infrastructure landed; `V1` is the first explicitly
/// versioned codec schema. Independent from the DB-level and per-payload
/// version layers — those have their own constants.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SchemaVersion(pub u32);

impl SchemaVersion {
    /// Pre-envelope data (bare JSON). Decoded via the legacy fallback path.
    pub const LEGACY: Self = Self(0);
    /// First explicitly versioned codec schema. Layout-compatible with
    /// LEGACY for every type whose fields are all `#[serde(default)]`-tolerant
    /// or whose nested types (e.g. `ExtentTree`) accept legacy forms.
    pub const V1: Self = Self(1);

    pub fn is_legacy(self) -> bool {
        self == Self::LEGACY
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// Envelope wrapping a typed payload together with its codec schema version.
///
/// The `payload` key is the discriminator: legacy bare-`T` JSON lacks it, so
/// envelope deserialization fails cleanly and the fallback path takes over.
/// The `schema_version` field defaults to `LEGACY` so a missing key is
/// unambiguously interpreted as v0 rather than v1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VersionedEnvelope<T> {
    #[serde(default)]
    schema_version: SchemaVersion,
    payload: T,
}

/// Trait implemented by every type that participates in the codec envelope.
///
/// Default impl covers the common case: the type's layout has only ever been
/// extended with `#[serde(default)]`-tolerant fields, so migrating from any
/// older version is a no-op (the payload deserializes directly as the
/// current shape). Types with breaking layout changes override
/// [`Versioned::migrate_from`] to hand-translate.
pub trait Versioned: Serialize + DeserializeOwned + Sized {
    /// Codec schema version this binary stamps on encode.
    const CURRENT: SchemaVersion = SchemaVersion::V1;

    /// Reconcile a payload deserialized from bytes stamped `from` with the
    /// current shape. Default rejects future versions and accepts equal/older
    /// ones as-is. Override to add real migration arms.
    ///
    /// `from == LEGACY` (= 0) means the bytes were written before the
    /// envelope existed (no `schema_version` key).
    fn migrate_from(self, from: SchemaVersion) -> Result<Self> {
        if from > Self::CURRENT {
            return Err(FluxError::Meta(format!(
                "codec schema {} newer than current {}; upgrade binary to read this data",
                from,
                Self::CURRENT
            )));
        }
        Ok(self)
    }
}

/// Encode `t` under the codec envelope with `schema_version = T::CURRENT`.
///
/// Use this for every byte boundary (RPC details + persistence) so a future
/// breaking layout change can dispatch off the recorded version.
pub fn encode_versioned<T: Versioned>(t: &T) -> Result<Vec<u8>> {
    let env = VersionedEnvelope {
        schema_version: T::CURRENT,
        payload: t,
    };
    serde_json::to_vec(&env).map_err(|e| FluxError::Meta(format!("encode_versioned: {e}")))
}

/// Decode bytes produced by [`encode_versioned`] OR legacy bare-JSON bytes
/// written before the envelope existed.
///
/// Detection: try envelope first. If envelope deserialization fails (missing
/// `payload` key, malformed JSON, or non-object root), fall back to direct
/// `T` deserialization and stamp `LEGACY`. Any non-trivial decode error
/// surfaces as `FluxError::Meta` — fail closed.
pub fn decode_versioned<T: Versioned>(bytes: &[u8]) -> Result<T> {
    match serde_json::from_slice::<VersionedEnvelope<T>>(bytes) {
        Ok(env) => {
            let from = env.schema_version;
            if from > T::CURRENT {
                return Err(FluxError::Meta(format!(
                    "codec schema {} newer than current {}; upgrade binary to read this data",
                    from,
                    T::CURRENT
                )));
            }
            env.payload.migrate_from(from)
        }
        Err(envelope_err) => {
            // Legacy fallback: bytes are a bare T (pre-versioning JSON) or
            // an envelope whose payload failed to deserialize as current T.
            // The latter (malformed envelope) is reported with both errors
            // so the operator can distinguish "legacy bare JSON" from
            // "newer-binary payload I cannot read".
            let payload: T = serde_json::from_slice(bytes).map_err(|bare_err| {
                FluxError::Meta(format!(
                    "decode_versioned failed: envelope err={envelope_err}; bare-T err={bare_err}"
                ))
            })?;
            payload.migrate_from(SchemaVersion::LEGACY)
        }
    }
}

// ===== Per-type Versioned impls =====
//
// Default impls cover layout-compatible extensions only. Breaking layout
// changes (none today) override `migrate_from`. B2's `ExtentTree` already
// accepts legacy `Vec<Extent>` via its own deserializer, so `Manifest`'s
// default hook handles legacy data without override.

impl Versioned for Inode {}
impl Versioned for Manifest {}
impl Versioned for FlushIntent {}
impl Versioned for DedupEntry {}
impl Versioned for Dentry {}
impl Versioned for UfsObject {}

// GC + Worker topology persisted state. Default impls cover these (only
// layout-compatible field extensions have happened so far).
impl Versioned for GcPlan {}
impl Versioned for GcBatch {}
impl Versioned for GcTombstone {}

// Wire payloads that are themselves the top-level encoded value (readdir
// returns a Vec<Dentry>; GC persistence stores Vec<ChunkId> / tombstone
// lists / flush-intent lists / delete-ack lists). The envelope wraps the
// whole collection.
impl Versioned for Vec<Dentry> {}
impl Versioned for Vec<ChunkId> {}
impl Versioned for Vec<WorkerTargetId> {}
impl Versioned for Vec<GcTombstone> {}
impl Versioned for Vec<(InodeId, FlushIntent)> {}
impl Versioned for Vec<(ChunkId, WorkerTargetId)> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChunkId, DataGen, DedupResult, Extent, ExtentTree, FileType, FlushId, LocalityLabel,
        ManifestId, RequestOpId, UfsVersion,
    };

    fn sample_inode() -> Inode {
        Inode {
            id: 7,
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            size: 4096,
            mtime_ms: 1,
            ctime_ms: 2,
            atime_ms: 3,
            link_count: 1,
            generation: 9,
            head_gen: DataGen(9),
            ufs_gen: DataGen(5),
            ufs_base_version: Some(UfsVersion("etag-1".into())),
            locality: LocalityLabel::Dirty,
            locality_fields: None,
            ufs: None,
            extent_root: None,
            manifest_id: Some(ManifestId(42)),
            flush_intent: None,
            last_error: None,
        }
    }

    fn sample_manifest() -> Manifest {
        let extents = ExtentTree::try_from(vec![Extent::Local {
            offset: 0,
            len: 100,
            chunk: ChunkId([1; 32]),
        }])
        .expect("tree");
        Manifest {
            inode: 7,
            gen: DataGen(9),
            size: 100,
            extents,
        }
    }

    fn sample_flush_intent() -> FlushIntent {
        FlushIntent {
            flush_id: FlushId(1),
            snapshot_gen: DataGen(5),
            snapshot_manifest_root: ChunkId([2; 32]),
            expected_ufs_version: Some(UfsVersion("v1".into())),
            target_digest: ChunkId([3; 32]),
        }
    }

    fn sample_dedup_entry() -> DedupEntry {
        DedupEntry {
            op_id: RequestOpId::from_bytes([0xff; 16]),
            result: DedupResult::Created { inode: 7 },
        }
    }

    fn sample_dentry() -> Dentry {
        Dentry {
            parent: 1,
            name: "x".into(),
            child: 7,
        }
    }

    // ---- 1. Round-trip (all persisted types) ----

    #[test]
    fn inode_roundtrip_through_versioned_envelope() {
        let original = sample_inode();
        let bytes = encode_versioned(&original).expect("encode");
        let decoded: Inode = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.head_gen, original.head_gen);
        assert_eq!(decoded.manifest_id, original.manifest_id);
    }

    #[test]
    fn manifest_roundtrip_through_versioned_envelope() {
        let original = sample_manifest();
        let bytes = encode_versioned(&original).expect("encode");
        let decoded: Manifest = decode_versioned(&bytes).expect("decode");
        decoded.validate().expect("decoded still valid");
        assert_eq!(decoded.extents.len(), original.extents.len());
        assert_eq!(decoded.size, original.size);
    }

    #[test]
    fn flush_intent_roundtrip_through_versioned_envelope() {
        let original = sample_flush_intent();
        let bytes = encode_versioned(&original).expect("encode");
        let decoded: FlushIntent = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.flush_id, original.flush_id);
        assert_eq!(decoded.target_digest, original.target_digest);
    }

    #[test]
    fn dedup_entry_roundtrip_through_versioned_envelope() {
        let original = sample_dedup_entry();
        let bytes = encode_versioned(&original).expect("encode");
        let decoded: DedupEntry = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.op_id, original.op_id);
        assert!(matches!(
            decoded.result,
            crate::DedupResult::Created { inode: 7 }
        ));
    }

    #[test]
    fn dentries_vec_roundtrip_through_versioned_envelope() {
        let v = vec![
            sample_dentry(),
            sample_dentry(),
            Dentry {
                parent: 2,
                name: "y".into(),
                child: 9,
            },
        ];
        let bytes = encode_versioned(&v).expect("encode vec");
        let decoded: Vec<Dentry> = decode_versioned(&bytes).expect("decode vec");
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[2].child, 9);
    }

    // ---- 1b. Round-trip for GC + topology persisted types (C4 incremental) ----

    fn sample_gc_plan() -> GcPlan {
        GcPlan {
            lease_id: crate::GcLeaseId(7),
            live_chunks: vec![ChunkId([1; 32]), ChunkId([2; 32])],
            removed_manifests: 3,
        }
    }

    fn sample_gc_batch() -> GcBatch {
        GcBatch {
            tombstoned_chunks: vec![ChunkId([9; 32])],
            removed_manifests: 1,
        }
    }

    fn sample_gc_tombstone() -> GcTombstone {
        GcTombstone {
            chunk: ChunkId([5; 32]),
            targets_initialized: true,
            pending_targets: vec![WorkerTargetId(1), WorkerTargetId(2)],
        }
    }

    #[test]
    fn gc_plan_roundtrip_through_envelope() {
        let p = sample_gc_plan();
        let bytes = encode_versioned(&p).expect("encode");
        let decoded: GcPlan = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.live_chunks.len(), 2);
        assert_eq!(decoded.removed_manifests, 3);
    }

    #[test]
    fn gc_batch_roundtrip_through_envelope() {
        let b = sample_gc_batch();
        let bytes = encode_versioned(&b).expect("encode");
        let decoded: GcBatch = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.tombstoned_chunks.len(), 1);
    }

    #[test]
    fn gc_tombstone_vec_roundtrip_through_envelope() {
        let v = vec![sample_gc_tombstone(), sample_gc_tombstone()];
        let bytes = encode_versioned(&v).expect("encode");
        let decoded: Vec<GcTombstone> = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].targets_initialized);
    }

    #[test]
    fn chunk_id_list_roundtrip_through_envelope() {
        let v: Vec<ChunkId> = vec![ChunkId([1; 32]), ChunkId([2; 32]), ChunkId([3; 32])];
        let bytes = encode_versioned(&v).expect("encode");
        let decoded: Vec<ChunkId> = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn worker_target_list_roundtrip_through_envelope() {
        let v: Vec<WorkerTargetId> = vec![WorkerTargetId(1), WorkerTargetId(2), WorkerTargetId(3)];
        let bytes = encode_versioned(&v).expect("encode");
        let decoded: Vec<WorkerTargetId> = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn flush_intent_list_roundtrip_through_envelope() {
        let v: Vec<(InodeId, FlushIntent)> =
            vec![(7, sample_flush_intent()), (9, sample_flush_intent())];
        let bytes = encode_versioned(&v).expect("encode");
        let decoded: Vec<(InodeId, FlushIntent)> = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, 7);
    }

    #[test]
    fn gc_delete_ack_list_roundtrip_through_envelope() {
        let v: Vec<(ChunkId, WorkerTargetId)> = vec![
            (ChunkId([1; 32]), WorkerTargetId(1)),
            (ChunkId([2; 32]), WorkerTargetId(2)),
        ];
        let bytes = encode_versioned(&v).expect("encode");
        let decoded: Vec<(ChunkId, WorkerTargetId)> = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.len(), 2);
    }

    #[test]
    fn ufs_object_roundtrip_through_envelope() {
        let o = UfsObject {
            key: "bucket/obj".into(),
            size: 1024,
            etag: Some("abc".into()),
            mtime_ms: Some(42),
        };
        let bytes = encode_versioned(&o).expect("encode");
        let decoded: UfsObject = decode_versioned(&bytes).expect("decode");
        assert_eq!(decoded.key, o.key);
        assert_eq!(decoded.size, 1024);
    }

    // Legacy bare-JSON forward-read for a GC type (proves envelope fallback
    // works for newly-versioned types too).
    #[test]
    fn gc_plan_legacy_bare_json_decodes() {
        let p = sample_gc_plan();
        let legacy = serde_json::to_vec(&p).expect("bare encode");
        let decoded: GcPlan = decode_versioned(&legacy).expect("legacy decode");
        assert_eq!(decoded.live_chunks.len(), p.live_chunks.len());
    }

    // ---- 2. Legacy forward-read (bare JSON, no envelope) ----

    #[test]
    fn inode_legacy_bare_json_decodes_as_v0() {
        let inode = sample_inode();
        let legacy_bytes = serde_json::to_vec(&inode).expect("bare encode");
        let decoded: Inode = decode_versioned(&legacy_bytes).expect("legacy decode");
        assert_eq!(decoded.id, inode.id);
        assert_eq!(decoded.head_gen, inode.head_gen);
    }

    #[test]
    fn manifest_legacy_bare_json_decodes_via_extent_tree_compat() {
        // B2's ExtentTree deserializer accepts the legacy Vec<Extent> form,
        // so a bare-JSON Manifest (pre-envelope) decodes transparently.
        let m = sample_manifest();
        let legacy_bytes = serde_json::to_vec(&m).expect("bare encode");
        let decoded: Manifest = decode_versioned(&legacy_bytes).expect("legacy decode");
        assert_eq!(decoded.extents.len(), m.extents.len());
    }

    #[test]
    fn manifest_legacy_bare_array_extents_decodes_too() {
        // Even older form: Manifest.extents as a bare Vec<Extent> (pre-B2).
        // The envelope fallback deserializes this via ExtentTree's Legacy arm.
        // Construct the legacy JSON by hand to avoid relying on a current-shape
        // serializer (B2 serializes ExtentTree as {version, entries}, not a
        // bare Vec).
        let chunk_array: Vec<u8> = vec![1u8; 32];
        let chunk_json: serde_json::Value = serde_json::Value::Array(
            chunk_array
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        );
        let legacy_json = serde_json::json!({
            "inode": 7u64,
            "gen": 9u64,
            "size": 100u64,
            "extents": [
                serde_json::json!({ "Local": { "offset": 0, "len": 100, "chunk": chunk_json } })
            ]
        });
        let bytes = serde_json::to_vec(&legacy_json).expect("json");
        let decoded: Manifest = decode_versioned(&bytes).expect("legacy vec decode");
        assert_eq!(decoded.extents.len(), 1);
    }

    #[test]
    fn flush_intent_legacy_bare_json_decodes_as_v0() {
        let fi = sample_flush_intent();
        let legacy_bytes = serde_json::to_vec(&fi).expect("bare encode");
        let decoded: FlushIntent = decode_versioned(&legacy_bytes).expect("legacy decode");
        assert_eq!(decoded, fi);
    }

    // ---- 3. Future-incompatible rejection (fail closed) ----

    #[test]
    fn future_schema_version_is_rejected_with_clear_error() {
        let inode = sample_inode();
        let bytes = encode_versioned(&inode).expect("encode");
        // Tamper: bump schema_version past CURRENT (= V1 = 1) to a future value.
        let mut v: serde_json::Value = serde_json::from_slice(&bytes).expect("envelope is json");
        v["schema_version"] = serde_json::json!(99u64);
        let tampered = serde_json::to_vec(&v).expect("re-encode");
        let err = decode_versioned::<Inode>(&tampered).expect_err("future schema must reject");
        match err {
            FluxError::Meta(msg) => {
                assert!(msg.contains("newer"), "unexpected message: {msg}");
                assert!(msg.contains("upgrade"), "unexpected message: {msg}");
            }
            other => panic!("expected FluxError::Meta, got {other:?}"),
        }
    }

    // ---- 4. Malformed envelope fails closed ----

    #[test]
    fn malformed_envelope_fails_closed() {
        // Truncated envelope: starts like an envelope but payload is broken.
        let bad = br#"{"schema_version":1,"payload":{"id":"not-a-u64"}}"#;
        let err = decode_versioned::<Inode>(bad).expect_err("malformed must fail");
        assert!(matches!(err, FluxError::Meta(_)));
    }

    #[test]
    fn empty_bytes_fails_closed() {
        let err = decode_versioned::<Inode>(b"").expect_err("empty must fail");
        assert!(matches!(err, FluxError::Meta(_)));
    }

    #[test]
    fn non_object_json_fails_closed() {
        let err = decode_versioned::<Inode>(b"42").expect_err("scalar must fail");
        assert!(matches!(err, FluxError::Meta(_)));
    }

    #[test]
    fn array_json_fails_closed() {
        let err = decode_versioned::<Inode>(b"[1,2,3]").expect_err("array must fail");
        assert!(matches!(err, FluxError::Meta(_)));
    }

    // ---- 5. Per-type override hook (B2-style integration marker) ----
    //
    // Demonstrates that a custom Versioned impl can intercept older payload
    // bytes via migrate_from, without changing the envelope machinery.

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct SimCurrent {
        new_field: u32,
    }

    impl Versioned for SimCurrent {
        const CURRENT: SchemaVersion = SchemaVersion::V1;
        fn migrate_from(self, from: SchemaVersion) -> Result<Self> {
            assert!(from <= Self::CURRENT, "should not see future version");
            Ok(self)
        }
    }

    #[test]
    fn migrate_from_override_is_invoked() {
        let cur = SimCurrent { new_field: 5 };
        let bytes = encode_versioned(&cur).expect("encode");
        let decoded: SimCurrent = decode_versioned(&bytes).expect("decode via override");
        assert_eq!(decoded, cur);
    }

    // ---- 6. encode stamps schema_version = CURRENT ----

    #[test]
    fn encode_always_stamps_current_version() {
        let bytes = encode_versioned(&sample_inode()).expect("encode");
        let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(v["schema_version"], serde_json::json!(1u64));
    }
}
