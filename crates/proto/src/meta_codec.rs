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
        FluxError::CasFailed { .. } | FluxError::DirtyConflict => Code::FailedPrecondition,
        _ => Code::Internal,
    };
    tonic::Status::new(code, err.to_string())
}

pub fn flux_from_status(status: tonic::Status) -> FluxError {
    match status.code() {
        tonic::Code::NotFound => FluxError::NotFound,
        tonic::Code::AlreadyExists => FluxError::AlreadyExists,
        tonic::Code::InvalidArgument => FluxError::InvalidArg(status.message().to_string()),
        tonic::Code::ResourceExhausted => FluxError::Capability(status.message().to_string()),
        tonic::Code::Unavailable => FluxError::Busy,
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
