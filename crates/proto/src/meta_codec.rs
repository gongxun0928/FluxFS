use crate::meta::v1;
use fluxfs_types::schema::{decode_versioned, Versioned};
use fluxfs_types::{
    ChunkId, Dentry, FileType, FlushIntent, FluxError, GcBatch, GcPlan, GcTombstone, Inode,
    InodeId, Manifest, ManifestId, Result as FluxResult, UfsObject, WorkerMembership,
    WorkerRegistration, WorkerTargetId,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

// ===== Rolling-compatibility codec policy (C4 fix-forward) =====
//
// `encode_*` writes **legacy bare-JSON** so old binaries (pre-envelope) can
// still decode RPC payloads and persisted state during a rolling upgrade.
//
// `decode_*` is bimodal: try bare-T first (covers today's peers and legacy
// persistence), fall back to `decode_versioned` envelope (forwards-compat
// with the brief window where commit 3225ffa wrote envelope on the wire,
// and with future capability-negotiated envelope peers).
//
// The `Versioned` infrastructure in `fluxfs_types::schema` remains in place
// as infrastructure-for-future-use, gated behind a follow-up task that adds
// RPC capability/version negotiation. See task #32 thread + gpt56 review
// (`6b114fa5`) for the rolling-compat constraint.

fn encode_legacy<T: Serialize + ?Sized>(t: &T) -> FluxResult<Vec<u8>> {
    serde_json::to_vec(t).map_err(|e| FluxError::Meta(e.to_string()))
}

fn decode_legacy_or_envelope<T>(bytes: &[u8]) -> FluxResult<T>
where
    T: DeserializeOwned + Versioned,
{
    // Fast path: bare-T (what most peers write today, plus all legacy data).
    if let Ok(value) = serde_json::from_slice::<T>(bytes) {
        return Ok(value);
    }
    // Fallback: envelope form. Either a peer that has already opted in via
    // capability negotiation, or data written during the 3225ffa envelope
    // window before this rolling-compat fix landed.
    decode_versioned(bytes)
}

pub fn encode_worker_registration(value: &WorkerRegistration) -> FluxResult<Vec<u8>> {
    encode_legacy(value)
}

pub fn decode_worker_registration(bytes: &[u8]) -> FluxResult<WorkerRegistration> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_worker_membership(value: &WorkerMembership) -> FluxResult<Vec<u8>> {
    encode_legacy(value)
}

pub fn decode_worker_membership(bytes: &[u8]) -> FluxResult<WorkerMembership> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_inode(inode: &Inode) -> FluxResult<Vec<u8>> {
    encode_legacy(inode)
}

pub fn decode_inode(bytes: &[u8]) -> FluxResult<Inode> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_manifest(m: &Manifest) -> FluxResult<Vec<u8>> {
    encode_legacy(m)
}

pub fn decode_manifest(bytes: &[u8]) -> FluxResult<Manifest> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_flush_intent(intent: &FlushIntent) -> FluxResult<Vec<u8>> {
    encode_legacy(intent)
}

pub fn decode_flush_intent(bytes: &[u8]) -> FluxResult<FlushIntent> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_ufs_object(object: &UfsObject) -> FluxResult<Vec<u8>> {
    encode_legacy(object)
}

pub fn decode_ufs_object(bytes: &[u8]) -> FluxResult<UfsObject> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_flush_intents(intents: &[(InodeId, FlushIntent)]) -> FluxResult<Vec<u8>> {
    encode_legacy(intents)
}

pub fn decode_flush_intents(bytes: &[u8]) -> FluxResult<Vec<(InodeId, FlushIntent)>> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_gc_plan(plan: &GcPlan) -> FluxResult<Vec<u8>> {
    encode_legacy(plan)
}

pub fn decode_gc_plan(bytes: &[u8]) -> FluxResult<GcPlan> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_chunk_ids(chunks: &[ChunkId]) -> FluxResult<Vec<u8>> {
    encode_legacy(chunks)
}

pub fn decode_chunk_ids(bytes: &[u8]) -> FluxResult<Vec<ChunkId>> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_gc_batch(batch: &GcBatch) -> FluxResult<Vec<u8>> {
    encode_legacy(batch)
}

pub fn decode_gc_batch(bytes: &[u8]) -> FluxResult<GcBatch> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_gc_tombstones(value: &[GcTombstone]) -> FluxResult<Vec<u8>> {
    encode_legacy(value)
}

pub fn decode_gc_tombstones(bytes: &[u8]) -> FluxResult<Vec<GcTombstone>> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_worker_targets(value: &[WorkerTargetId]) -> FluxResult<Vec<u8>> {
    encode_legacy(value)
}

pub fn decode_worker_targets(bytes: &[u8]) -> FluxResult<Vec<WorkerTargetId>> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_gc_delete_acks(value: &[(ChunkId, WorkerTargetId)]) -> FluxResult<Vec<u8>> {
    encode_legacy(value)
}

pub fn decode_gc_delete_acks(bytes: &[u8]) -> FluxResult<Vec<(ChunkId, WorkerTargetId)>> {
    decode_legacy_or_envelope(bytes)
}

pub fn encode_dentries(d: &[Dentry]) -> FluxResult<Vec<u8>> {
    encode_legacy(d)
}

pub fn decode_dentries(bytes: &[u8]) -> FluxResult<Vec<Dentry>> {
    decode_legacy_or_envelope(bytes)
}

pub fn file_type_to_wire(ft: FileType) -> u32 {
    match ft {
        FileType::Directory => 0,
        FileType::Regular => 1,
    }
}

pub fn file_type_from_wire(v: u32) -> FluxResult<FileType> {
    match v {
        0 => Ok(FileType::Directory),
        1 => Ok(FileType::Regular),
        _ => Err(FluxError::InvalidArg(format!("bad file_type wire={v}"))),
    }
}

pub fn status_from_flux(err: FluxError) -> tonic::Status {
    use tonic::Code;
    let code = match &err {
        FluxError::NotFound => Code::NotFound,
        FluxError::AlreadyExists => Code::AlreadyExists,
        FluxError::InvalidArg(_) | FluxError::NotDirectory | FluxError::IsDirectory => {
            Code::InvalidArgument
        }
        FluxError::Capability(_) => Code::ResourceExhausted,
        FluxError::Busy => Code::Unavailable,
        FluxError::CasFailed { .. } | FluxError::DirtyConflict | FluxError::ReadOnly => {
            Code::FailedPrecondition
        }
        // C1 auth errors (task #30): distinct codes so clients can branch on
        // "re-present credentials" (Unauthenticated) vs "this principal can
        // never do that" (PermissionDenied). Both are non-retryable without
        // out-of-band action, matching cursor's A7 review (msg 36c528fc).
        FluxError::Unauthenticated(_) => Code::Unauthenticated,
        FluxError::Unauthorized(_) => Code::PermissionDenied,
        _ => Code::Internal,
    };
    // Attach the structured FluxError as tonic::Status details so clients can
    // decode the exact variant + payload (e.g. CasFailed { expected, actual })
    // instead of reverse-parsing the Display string. Generic gRPC clients
    // still see the Code + a human-readable message.
    let details = serde_json::to_vec(&err).unwrap_or_default();
    tonic::Status::with_details(code, err.to_string(), details.into())
}

pub fn flux_from_status(status: tonic::Status) -> FluxError {
    // Preferred path: structured FluxError in Status.details (serde_json).
    if !status.details().is_empty() {
        if let Ok(err) = serde_json::from_slice::<FluxError>(status.details()) {
            return err;
        }
        // Fall through to Code-based reconstruction if details are malformed
        // or carry some other detail schema (forward compatibility).
    }
    // Backward-compatible path for servers that do not yet attach details:
    // reconstruct a best-effort FluxError from the gRPC Code alone. Note this
    // is lossy (no payload) — clients needing structured data must talk to a
    // server that emits details.
    match status.code() {
        tonic::Code::NotFound => FluxError::NotFound,
        tonic::Code::AlreadyExists => FluxError::AlreadyExists,
        tonic::Code::InvalidArgument => FluxError::InvalidArg(status.message().to_string()),
        tonic::Code::ResourceExhausted => FluxError::Capability(status.message().to_string()),
        // Distinguish server-side Busy (round-trip of `Status::unavailable(
        // "chunk worker busy")` emitted by the chunkworker handler) from
        // transport-layer Unavailable (TLS handshake failure, connection
        // reset, etc.). The transport case must preserve the underlying
        // message — otherwise mTLS rejection at the handshake surfaces as a
        // misleading "inode busy: op in flight" and acceptance tests cannot
        // distinguish TLS rejection from server overload (task #30 C1
        // acceptance hardening, gpt56 ac8ef471).
        tonic::Code::Unavailable => {
            let msg = status.message();
            if msg.contains("chunk worker busy") {
                FluxError::Busy
            } else {
                FluxError::Meta(format!("unavailable: {msg}"))
            }
        }
        tonic::Code::FailedPrecondition => FluxError::Meta(status.message().to_string()),
        tonic::Code::Unauthenticated => FluxError::Unauthenticated(status.message().to_string()),
        tonic::Code::PermissionDenied => FluxError::Unauthorized(status.message().to_string()),
        _ => FluxError::Meta(status.to_string()),
    }
}

pub fn inode_response(inode: &Inode) -> FluxResult<v1::GetInodeResponse> {
    Ok(v1::GetInodeResponse {
        inode_json: encode_inode(inode)?,
    })
}

pub fn manifest_id_response(id: ManifestId) -> v1::PutManifestResponse {
    v1::PutManifestResponse { manifest_id: id.0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxfs_types::FluxError;

    fn roundtrip(err: FluxError) -> FluxError {
        flux_from_status(status_from_flux(err))
    }

    #[test]
    fn all_flux_error_variants_roundtrip_through_tonic_status() {
        let cases = vec![
            FluxError::NotFound,
            FluxError::AlreadyExists,
            FluxError::NotDirectory,
            FluxError::IsDirectory,
            FluxError::InvalidArg("bad offset 123".into()),
            FluxError::Capability("over 4MiB".into()),
            FluxError::CasFailed {
                expected: 7,
                actual: 9,
            },
            FluxError::DirtyConflict,
            FluxError::Busy,
            FluxError::ReadOnly,
            FluxError::Io("short read".into()),
            FluxError::Meta("raft apply".into()),
            FluxError::Ufs("s3 403".into()),
            // C1 (task #30) auth variants: structured payload must survive.
            FluxError::Unauthenticated("no client cert".into()),
            FluxError::Unauthorized("worker cannot admin".into()),
        ];
        for err in cases {
            let decoded = roundtrip(err.clone());
            assert_eq!(decoded, err, "round-trip lost data for {err:?}");
        }
    }

    #[test]
    fn cas_failed_payload_survives_wire() {
        // The A7 bug: CasFailed { expected, actual } used to be lost to a
        // Display-string reverse-parse. Verify the structured payload now
        // survives the tonic::Status details round-trip exactly.
        let err = FluxError::CasFailed {
            expected: 42,
            actual: 99,
        };
        let decoded = roundtrip(err.clone());
        match decoded {
            FluxError::CasFailed { expected, actual } => {
                assert_eq!((expected, actual), (42, 99));
            }
            other => panic!("expected CasFailed, got {other:?}"),
        }
    }

    #[test]
    fn backward_compat_path_when_no_details() {
        // Old servers do not attach details. Client must still decode a
        // reasonable FluxError from Code alone (lossy but functional).
        let status = tonic::Status::new(tonic::Code::NotFound, "old server".to_string());
        let decoded = flux_from_status(status);
        assert_eq!(decoded, FluxError::NotFound);
    }

    #[test]
    fn backward_compat_internal_falls_through_to_meta() {
        let status = tonic::Status::new(tonic::Code::Internal, "boom".to_string());
        let decoded = flux_from_status(status);
        assert!(matches!(decoded, FluxError::Meta(_)));
    }

    // ===== C1 auth-variant wire code tests (task #30) =====

    #[test]
    fn unauthenticated_maps_to_tonic_unauthenticated_code() {
        // Code-level contract (cursor msg 36c528fc): clients must see
        // Code::Unauthenticated so they can branch "re-present credentials"
        // vs PermissionDenied's "this principal can never do that".
        let status = status_from_flux(FluxError::Unauthenticated("no client cert".into()));
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn unauthorized_maps_to_tonic_permission_denied_code() {
        let status = status_from_flux(FluxError::Unauthorized("worker cannot admin".into()));
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn auth_codes_survive_lossy_backward_compat_path() {
        // A client talking to an old server that emits only Code (no details)
        // must still reconstruct a recognizable auth FluxError variant.
        let unauth = flux_from_status(tonic::Status::new(
            tonic::Code::Unauthenticated,
            "no cert".to_string(),
        ));
        assert!(matches!(unauth, FluxError::Unauthenticated(_)));
        let denied = flux_from_status(tonic::Status::new(
            tonic::Code::PermissionDenied,
            "no cap".to_string(),
        ));
        assert!(matches!(denied, FluxError::Unauthorized(_)));
    }

    // ===== Rolling-compat codec tests (C4 fix-forward) =====

    #[test]
    fn encode_writes_legacy_bare_json_not_envelope() {
        // Encode must produce bare-T JSON (no envelope wrapper) so old
        // binaries can decode RPC payloads during rolling upgrade.
        let inode = Inode {
            id: 7,
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: 0,
            mtime_ms: 0,
            ctime_ms: 0,
            atime_ms: 0,
            link_count: 1,
            generation: 1,
            head_gen: fluxfs_types::DataGen(1),
            ufs_gen: fluxfs_types::DataGen(0),
            ufs_base_version: None,
            locality: fluxfs_types::LocalityLabel::Dirty,
            locality_fields: None,
            ufs: None,
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let bytes = encode_inode(&inode).expect("encode");
        let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        // Bare-T form has `id` at root and NO `schema_version`/`payload` wrapper.
        assert_eq!(v["id"], serde_json::json!(7u64));
        assert!(v.get("schema_version").is_none());
        assert!(v.get("payload").is_none());
    }

    #[test]
    fn decode_accepts_legacy_bare_json() {
        let inode = Inode {
            id: 99,
            file_type: FileType::Regular,
            mode: 0o600,
            uid: 1,
            gid: 2,
            size: 4096,
            mtime_ms: 1,
            ctime_ms: 2,
            atime_ms: 3,
            link_count: 1,
            generation: 5,
            head_gen: fluxfs_types::DataGen(5),
            ufs_gen: fluxfs_types::DataGen(3),
            ufs_base_version: None,
            locality: fluxfs_types::LocalityLabel::Dirty,
            locality_fields: None,
            ufs: None,
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let legacy = serde_json::to_vec(&inode).expect("bare encode");
        let decoded = decode_inode(&legacy).expect("legacy decode");
        assert_eq!(decoded.id, 99);
    }

    #[test]
    fn worker_membership_codec_keeps_rolling_bare_write_policy() {
        let membership = WorkerMembership {
            epoch: 4,
            workers: vec![WorkerRegistration {
                id: WorkerTargetId(9),
                endpoint: "http://127.0.0.1:50059".into(),
                failure_domain: "rack-a".into(),
                capacity_bytes: 100,
                available_bytes: 80,
                lease_deadline_ms: 500,
            }],
        };
        let bytes = encode_worker_membership(&membership).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["epoch"], serde_json::json!(4));
        assert!(json.get("payload").is_none());
        assert_eq!(decode_worker_membership(&bytes).unwrap(), membership);
    }

    #[test]
    fn decode_accepts_envelope_form_for_forwards_compat() {
        // A peer that wrote envelope form (commit 3225ffa window, or future
        // capability-negotiated peers) must still decode cleanly via the
        // envelope fallback path.
        use fluxfs_types::schema::encode_versioned;
        let inode = Inode {
            id: 42,
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: 0,
            mtime_ms: 0,
            ctime_ms: 0,
            atime_ms: 0,
            link_count: 1,
            generation: 1,
            head_gen: fluxfs_types::DataGen(0),
            ufs_gen: fluxfs_types::DataGen(0),
            ufs_base_version: None,
            locality: fluxfs_types::LocalityLabel::Clean,
            locality_fields: None,
            ufs: None,
            extent_root: None,
            manifest_id: None,
            flush_intent: None,
            last_error: None,
        };
        let envelope = encode_versioned(&inode).expect("envelope encode");
        let decoded = decode_inode(&envelope).expect("envelope decode via fallback");
        assert_eq!(decoded.id, 42);
    }
}
