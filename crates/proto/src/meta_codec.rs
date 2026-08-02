use crate::meta::v1;
use fluxfs_types::{
    Dentry, FileType, FluxError, Inode, Manifest, ManifestId, Result as FluxResult,
};

pub fn encode_inode(inode: &Inode) -> FluxResult<Vec<u8>> {
    serde_json::to_vec(inode).map_err(|e| FluxError::Meta(e.to_string()))
}

pub fn decode_inode(bytes: &[u8]) -> FluxResult<Inode> {
    serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))
}

pub fn encode_manifest(m: &Manifest) -> FluxResult<Vec<u8>> {
    serde_json::to_vec(m).map_err(|e| FluxError::Meta(e.to_string()))
}

pub fn decode_manifest(bytes: &[u8]) -> FluxResult<Manifest> {
    serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))
}

pub fn encode_dentries(d: &[Dentry]) -> FluxResult<Vec<u8>> {
    serde_json::to_vec(d).map_err(|e| FluxError::Meta(e.to_string()))
}

pub fn decode_dentries(bytes: &[u8]) -> FluxResult<Vec<Dentry>> {
    serde_json::from_slice(bytes).map_err(|e| FluxError::Meta(e.to_string()))
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
        tonic::Code::Unavailable => FluxError::Busy,
        tonic::Code::FailedPrecondition => FluxError::Meta(status.message().to_string()),
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
}
