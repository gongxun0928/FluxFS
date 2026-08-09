//! Cross-process request correlation for FluxFS RPCs (#31).
//!
//! Clients inject [`METADATA_KEY`] on every tonic call; servers extract the same
//! value into tracing spans so Meta / Chunk / Client logs share one id.

use std::cell::RefCell;

use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::{Request, Status};
use uuid::Uuid;

/// HTTP/2 / gRPC metadata key (lowercase ASCII).
pub const METADATA_KEY: &str = "x-fluxfs-request-id";

thread_local! {
    static CURRENT_REQUEST_ID: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Run `f` with a pinned correlation id so nested client RPCs reuse it.
pub fn scope_request_id<R>(id: impl Into<String>, f: impl FnOnce() -> R) -> R {
    let id = id.into();
    CURRENT_REQUEST_ID.with(|slot| {
        let previous = slot.replace(Some(id));
        let out = f();
        *slot.borrow_mut() = previous;
        out
    })
}

/// Prefer the scoped id; otherwise mint a new UUID v4.
pub fn current_or_new_request_id() -> String {
    CURRENT_REQUEST_ID.with(|slot| {
        if let Some(id) = slot.borrow().clone() {
            id
        } else {
            Uuid::new_v4().to_string()
        }
    })
}

fn metadata_key() -> AsciiMetadataKey {
    AsciiMetadataKey::from_static(METADATA_KEY)
}

/// Insert / overwrite the correlation id on outbound metadata.
pub fn inject_request_id(metadata: &mut MetadataMap, id: &str) {
    if let Ok(value) = AsciiMetadataValue::try_from(id) {
        metadata.insert(metadata_key(), value);
    }
}

/// Read the correlation id from inbound metadata, or mint one if absent.
pub fn extract_request_id_from_metadata(metadata: &MetadataMap) -> String {
    metadata
        .get(METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

/// Extract from a tonic [`Request`], minting when the client omitted the header.
pub fn extract_request_id<T>(request: &Request<T>) -> String {
    extract_request_id_from_metadata(request.metadata())
}

/// Attach a correlation id to an outbound tonic request (uses scoped/new id).
pub fn attach_request_id<T>(mut request: Request<T>) -> Request<T> {
    let id = current_or_new_request_id();
    inject_request_id(request.metadata_mut(), &id);
    request
}

/// Client interceptor: stamp every RPC with [`METADATA_KEY`].
#[derive(Clone, Default, Debug)]
pub struct RequestIdInterceptor;

impl tonic::service::Interceptor for RequestIdInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let id = current_or_new_request_id();
        inject_request_id(request.metadata_mut(), &id);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::service::Interceptor;

    #[test]
    fn inject_extract_roundtrip() {
        let mut req = Request::new(());
        inject_request_id(req.metadata_mut(), "corr-abc");
        assert_eq!(extract_request_id(&req), "corr-abc");
    }

    #[test]
    fn interceptor_stamps_metadata() {
        let mut interceptor = RequestIdInterceptor;
        let req = interceptor.call(Request::new(())).unwrap();
        let id = extract_request_id(&req);
        assert!(!id.is_empty());
        assert!(id.contains('-')); // uuid form
    }

    #[test]
    fn scope_reuses_id_for_nested_calls() {
        scope_request_id("outer-1", || {
            assert_eq!(current_or_new_request_id(), "outer-1");
            assert_eq!(current_or_new_request_id(), "outer-1");
        });
    }
}
