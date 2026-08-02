//! Workload identity, principals, and authorization primitives for FluxFS
//! RPCs (task #30 C1).
//!
//! # Status: Phase 1 — types only
//!
//! Phase 1 (this module) ships pure types + decision primitives. No TLS or
//! crypto wiring: tonic `ServerTlsConfig` / `ClientTlsConfig` plumbing is
//! Phase 2; per-RPC authz interceptors + mount-token enrollment are Phase 3;
//! cert rotation / CRL / audit is Phase 4.
//!
//! # Trust model
//!
//! An internal cluster CA issues SPIFFE-style workload certificates. The
//! canonical identity string is `spiffe://fluxfs/<role>/<name>` and is carried
//! in the URI SAN of the peer certificate. Transport-layer mTLS establishes
//! [`WorkloadIdentity`]; the application layer resolves [`TenantId`] from a
//! [`MountToken`] carried in gRPC metadata (`x-fluxfs-mount-token`).
//!
//! Roles today: `meta` (MetaMaster), `worker` (ChunkWorker), `client-admin`
//! (privileged cluster tooling). Phase 3 introduces a tenant-scoped client
//! role once mount-token enrollment lands.

use crate::WorkerTargetId;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

// ===== Tenant + mount identifiers =====

/// Tenant identifier (cluster-issued, opaque numeric). Zero is the bootstrap /
/// single-tenant sentinel used until Phase 3 mount-token enrollment ships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TenantId(pub u64);

impl TenantId {
    /// Bootstrap single-tenant sentinel. Phase 3 enrollment assigns nonzero
    /// tenant ids; servers MUST treat nonzero-tenant isolation as not-yet-
    /// enforced until Phase 3 lands.
    pub const BOOTSTRAP: Self = Self(0);
}

impl Default for TenantId {
    fn default() -> Self {
        Self::BOOTSTRAP
    }
}

/// Mount enrollment token (opaque, server-issued). Carried in the
/// `x-fluxfs-mount-token` gRPC metadata header by clients. Never serialized
/// into persisted state — Phase 3 wires server-side enrollment + validation.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MountToken(pub [u8; 32]);

impl MountToken {
    /// Sentinel: anonymous / no token supplied. Phase 3 interceptor must deny
    /// non-bootstrap mutations when the principal presents anonymous.
    pub const ANONYMOUS: Self = Self([0u8; 32]);

    pub fn from_bytes(b: [u8; 32]) -> Self {
        Self(b)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn is_anonymous(&self) -> bool {
        self.0 == [0u8; 32]
    }
}

// No auto-derive for Debug: avoid leaking the secret into logs. Explicit
// redacting Debug shows the first 4 bytes for correlation only.
impl fmt::Debug for MountToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_anonymous() {
            return write!(f, "MountToken(ANONYMOUS)");
        }
        write!(
            f,
            "MountToken({:02x}{:02x}{:02x}{:02x}…REDACTED)",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

// ===== Workload identity (mTLS-established) =====

/// Role encoded in the SPIFFE-style URI SAN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadRole {
    /// MetaMaster service identity (per Raft group / cluster).
    Meta,
    /// ChunkWorker service identity (carries [`WorkerTargetId`]).
    Worker,
    /// Privileged client: cluster admin tooling. Phase 3 adds a tenant-scoped
    /// client role once mount-token enrollment ships.
    ClientAdmin,
}

impl WorkloadRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Worker => "worker",
            Self::ClientAdmin => "client-admin",
        }
    }

    /// Parse the role segment of a SPIFFE URI. Returns `None` on unknown
    /// roles — Phase 2 interceptor fail-closes on `None`.
    pub fn parse_role(s: &str) -> Option<Self> {
        match s {
            "meta" => Some(Self::Meta),
            "worker" => Some(Self::Worker),
            "client-admin" => Some(Self::ClientAdmin),
            _ => None,
        }
    }
}

impl fmt::Display for WorkloadRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Workload identity established by transport-layer mTLS.
///
/// Canonical SPIFFE URI: `spiffe://fluxfs/<role>/<name>`. The `name` is
/// role-scoped: Meta uses the raft-group id, Worker uses the decimal
/// [`WorkerTargetId`], ClientAdmin uses the principal name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkloadIdentity {
    pub role: WorkloadRole,
    pub name: String,
    /// [`WorkerTargetId`] when `role == Worker`, else `None`. Lifted out of
    /// `name` so authz checks don't re-parse strings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_target_id: Option<WorkerTargetId>,
    /// TenantId if the cert carried one in a custom SAN (Phase 3), else
    /// `None` and the application layer must resolve tenant from a
    /// [`MountToken`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
}

impl WorkloadIdentity {
    pub fn spiffe_uri(&self) -> String {
        format!("spiffe://fluxfs/{}/{}", self.role.as_str(), self.name)
    }

    pub fn meta(group: impl Into<String>) -> Self {
        Self {
            role: WorkloadRole::Meta,
            name: group.into(),
            worker_target_id: None,
            tenant_id: None,
        }
    }

    pub fn worker(target_id: WorkerTargetId) -> Self {
        Self {
            role: WorkloadRole::Worker,
            name: target_id.0.to_string(),
            worker_target_id: Some(target_id),
            tenant_id: None,
        }
    }

    pub fn client_admin(name: impl Into<String>) -> Self {
        Self {
            role: WorkloadRole::ClientAdmin,
            name: name.into(),
            worker_target_id: None,
            tenant_id: None,
        }
    }

    /// Parse a SPIFFE URI back into a [`WorkloadIdentity`]. Returns `None` on
    /// unknown role, malformed URI, or unparsable worker id. Phase 2
    /// interceptor fail-closes on `None`.
    pub fn from_spiffe_uri(uri: &str) -> Option<Self> {
        let rest = uri.strip_prefix("spiffe://fluxfs/")?;
        let (role_str, name) = rest.split_once('/')?;
        if name.is_empty() || name.contains('/') {
            return None;
        }
        match WorkloadRole::parse_role(role_str)? {
            WorkloadRole::Meta => Some(Self::meta(name)),
            WorkloadRole::ClientAdmin => Some(Self::client_admin(name)),
            WorkloadRole::Worker => {
                let n: u64 = name.parse().ok()?;
                Some(Self::worker(WorkerTargetId(n)))
            }
        }
    }
}

/// Authenticated principal = mTLS-established [`WorkloadIdentity`] plus the
/// source certificate fingerprint (SHA-256 hex of the DER, 64 lowercase
/// chars) for audit logging and replay diagnostics.
///
/// The fingerprint is NOT consulted for authz decisions — mTLS already
/// validated the chain. Servers persist it only into audit/log records.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Principal {
    pub identity: WorkloadIdentity,
    /// SHA-256 of the client cert DER, lowercase hex (64 chars). Empty in
    /// dev/loopback paths that skip client certs (Phase 2 dev CA still
    /// issues certs; this empty state is reserved for bootstrap).
    #[serde(default)]
    pub cert_fingerprint: String,
}

impl Principal {
    pub fn new(identity: WorkloadIdentity, cert_fingerprint: impl Into<String>) -> Self {
        Self {
            identity,
            cert_fingerprint: cert_fingerprint.into(),
        }
    }

    /// True if this principal's role grants `cap` under the default policy
    /// (see [`RoleCapabilities::default_for`]).
    pub fn has(&self, cap: Capability) -> bool {
        RoleCapabilities::default_for(self.identity.role).contains(cap)
    }
}

// ===== Authorization =====

/// Coarse-grained capability a principal may hold. Per-RPC mapping lives in
/// the Phase 3 interceptor — e.g. `Create` → `MutateMeta`, `DeleteChunk` →
/// `DeleteChunk`, `BeginGc` → `GcControl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Read inodes / readdir / lookup / stat.
    ReadMeta,
    /// Mutate inodes / dentries / manifests / flush state.
    MutateMeta,
    /// Stage chunk data on a worker.
    PutChunk,
    /// Read chunk data from a worker.
    GetChunk,
    /// Delete chunks (GC or reservation abort).
    DeleteChunk,
    /// Drive GC leases / tombstones.
    GcControl,
    /// Cluster admin (placement, topology, cert rotation triggers).
    Admin,
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Stable audit-log string; renaming would break log queries. Keep
        // snake_case to match the serde rename_all on the enum.
        let s = match self {
            Self::ReadMeta => "read_meta",
            Self::MutateMeta => "mutate_meta",
            Self::PutChunk => "put_chunk",
            Self::GetChunk => "get_chunk",
            Self::DeleteChunk => "delete_chunk",
            Self::GcControl => "gc_control",
            Self::Admin => "admin",
        };
        f.write_str(s)
    }
}

/// Default capability set granted to a role. The Phase 3 interceptor consults
/// this when checking `Principal::has(cap)`; Phase 4 may add tenant-scoped
/// overrides persisted in the Meta state machine.
#[derive(Debug, Clone)]
pub struct RoleCapabilities {
    caps: &'static [Capability],
}

impl RoleCapabilities {
    /// Default capability grant for `role`.
    pub const fn default_for(role: WorkloadRole) -> Self {
        use WorkloadRole as R;
        match role {
            // Meta talks to itself for Raft replication; gets full meta +
            // GC + admin (topology/cert triggers route here until Phase 4
            // splits admin into its own role).
            R::Meta => Self {
                caps: &[
                    Capability::ReadMeta,
                    Capability::MutateMeta,
                    Capability::GcControl,
                    Capability::Admin,
                ],
            },
            // Worker stages chunk data, runs GC-driven deletes against its
            // own store, and dials Meta as a client for chunk reservation /
            // commit / GC ACKs — so it needs both chunk-side caps and the
            // meta-client caps used during reservations.
            R::Worker => Self {
                caps: &[
                    Capability::ReadMeta,
                    Capability::MutateMeta,
                    Capability::PutChunk,
                    Capability::GetChunk,
                    Capability::DeleteChunk,
                ],
            },
            // Admin client / FUSE mount: full meta + GC + admin AND direct
            // chunk put/get/delete (the mount dials workers for FUSE data
            // path AND drives GC tombstone sweeps + orphan GC by issuing
            // delete_chunk against workers). All three data-path caps are
            // required by the mount's GC machinery.
            R::ClientAdmin => Self {
                caps: &[
                    Capability::ReadMeta,
                    Capability::MutateMeta,
                    Capability::GcControl,
                    Capability::Admin,
                    Capability::PutChunk,
                    Capability::GetChunk,
                    Capability::DeleteChunk,
                ],
            },
        }
    }

    pub fn contains(&self, cap: Capability) -> bool {
        self.caps.contains(&cap)
    }

    pub fn iter(&self) -> impl Iterator<Item = Capability> {
        self.caps.iter().copied()
    }
}

/// Why an authz check denied. Maps to a tonic `Code` via
/// [`DenyReason::into_flux_error`] + `meta_codec::status_from_flux`.
#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum DenyReason {
    /// No authenticated principal on the call (missing/invalid client cert,
    /// or anonymous mount token on a non-bootstrap mutation).
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// Principal authenticated but lacks the required capability.
    #[error("unauthorized: role {role} lacks {required}: {detail}")]
    Unauthorized {
        role: WorkloadRole,
        required: Capability,
        detail: String,
    },
    /// Tenant/mount token missing or not enrolled (Phase 3).
    #[error("tenant denied: {0}")]
    TenantDenied(String),
}

impl DenyReason {
    pub fn unauthenticated(msg: impl Into<String>) -> Self {
        Self::Unauthenticated(msg.into())
    }

    pub fn unauthorized(
        role: WorkloadRole,
        required: Capability,
        detail: impl Into<String>,
    ) -> Self {
        Self::Unauthorized {
            role,
            required,
            detail: detail.into(),
        }
    }

    pub fn tenant_denied(msg: impl Into<String>) -> Self {
        Self::TenantDenied(msg.into())
    }

    /// Convert to the matching [`crate::FluxError`] variant so the existing
    /// `meta_codec::status_from_flux` path produces the right tonic `Code`.
    pub fn into_flux_error(self) -> crate::FluxError {
        match self {
            Self::Unauthenticated(m) => crate::FluxError::Unauthenticated(m),
            Self::Unauthorized { .. } => crate::FluxError::Unauthorized(self.to_string()),
            Self::TenantDenied(m) => crate::FluxError::Unauthorized(format!("tenant denied: {m}")),
        }
    }
}

/// Outcome of an authz check: `Ok(())` allows the call; `Err(reason)` denies
/// it and the interceptor converts `reason` into a tonic `Status`.
pub type AuthOutcome = Result<(), DenyReason>;

/// Convenience: require `cap` of `principal`, else return the matching deny.
pub fn require(principal: &Principal, cap: Capability) -> AuthOutcome {
    if principal.has(cap) {
        Ok(())
    } else {
        Err(DenyReason::unauthorized(
            principal.identity.role,
            cap,
            format!(
                "principal {} lacks required capability",
                principal.identity.spiffe_uri()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkerTargetId;

    // ----- MountToken redaction -----

    #[test]
    fn mount_token_anonymous_is_none_sentinel() {
        assert!(MountToken::ANONYMOUS.is_anonymous());
        let t = MountToken::from_bytes([0u8; 32]);
        assert!(t.is_anonymous());
    }

    #[test]
    fn mount_token_debug_redacts_secret() {
        let mut b = [0u8; 32];
        b[0] = 0xde;
        b[1] = 0xad;
        b[2] = 0xbe;
        b[3] = 0xef;
        let t = MountToken::from_bytes(b);
        let s = format!("{t:?}");
        assert!(s.contains("deadbeef"));
        assert!(s.contains("REDACTED"));
        // The full 32 bytes must never appear.
        assert!(!s.contains(&hex_encode_full(&b)));
    }

    fn hex_encode_full(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect::<String>()
    }

    #[test]
    fn mount_token_anonymous_debug_is_explicit() {
        let s = format!("{:?}", MountToken::ANONYMOUS);
        assert_eq!(s, "MountToken(ANONYMOUS)");
    }

    // ----- WorkloadIdentity SPIFFE round-trip -----

    #[test]
    fn spiffe_uri_roundtrip_meta() {
        let id = WorkloadIdentity::meta("raft-group-0");
        let uri = id.spiffe_uri();
        assert_eq!(uri, "spiffe://fluxfs/meta/raft-group-0");
        let parsed = WorkloadIdentity::from_spiffe_uri(&uri).expect("parse");
        assert_eq!(parsed, id);
    }

    #[test]
    fn spiffe_uri_roundtrip_worker() {
        let id = WorkloadIdentity::worker(WorkerTargetId(7));
        let uri = id.spiffe_uri();
        assert_eq!(uri, "spiffe://fluxfs/worker/7");
        let parsed = WorkloadIdentity::from_spiffe_uri(&uri).expect("parse");
        assert_eq!(parsed, id);
        assert_eq!(parsed.worker_target_id, Some(WorkerTargetId(7)));
    }

    #[test]
    fn spiffe_uri_roundtrip_client_admin() {
        let id = WorkloadIdentity::client_admin("ops");
        let parsed = WorkloadIdentity::from_spiffe_uri(&id.spiffe_uri()).expect("parse");
        assert_eq!(parsed, id);
    }

    #[test]
    fn from_spiffe_uri_rejects_unknown_role() {
        assert!(WorkloadIdentity::from_spiffe_uri("spiffe://fluxfs/root/0").is_none());
    }

    #[test]
    fn from_spiffe_uri_rejects_wrong_scheme() {
        assert!(WorkloadIdentity::from_spiffe_uri("https://fluxfs/meta/g").is_none());
    }

    #[test]
    fn from_spiffe_uri_rejects_empty_name() {
        assert!(WorkloadIdentity::from_spiffe_uri("spiffe://fluxfs/meta/").is_none());
    }

    #[test]
    fn from_spiffe_uri_rejects_nested_name() {
        // names containing '/' are rejected — prevents path-traversal-style
        // splicing in the SAN.
        assert!(WorkloadIdentity::from_spiffe_uri("spiffe://fluxfs/meta/a/b").is_none());
    }

    #[test]
    fn from_spiffe_uri_rejects_non_numeric_worker() {
        assert!(WorkloadIdentity::from_spiffe_uri("spiffe://fluxfs/worker/abc").is_none());
    }

    // ----- Role → Capability policy -----

    #[test]
    fn meta_role_gets_meta_admin_gc_caps() {
        let caps = RoleCapabilities::default_for(WorkloadRole::Meta);
        assert!(caps.contains(Capability::ReadMeta));
        assert!(caps.contains(Capability::MutateMeta));
        assert!(caps.contains(Capability::GcControl));
        assert!(caps.contains(Capability::Admin));
        // Meta is not a data-path role.
        assert!(!caps.contains(Capability::PutChunk));
        assert!(!caps.contains(Capability::GetChunk));
        assert!(!caps.contains(Capability::DeleteChunk));
    }

    #[test]
    fn worker_role_gets_data_path_and_meta_client_caps() {
        let caps = RoleCapabilities::default_for(WorkloadRole::Worker);
        // Data path on the worker's own store.
        assert!(caps.contains(Capability::PutChunk));
        assert!(caps.contains(Capability::GetChunk));
        assert!(caps.contains(Capability::DeleteChunk));
        // Workers dial Meta for reservation/commit/ACK under the same identity
        // (no separate client cert in Phase 3).
        assert!(caps.contains(Capability::ReadMeta));
        assert!(caps.contains(Capability::MutateMeta));
        // Workers do not drive GC lease control or admin actions.
        assert!(!caps.contains(Capability::Admin));
        assert!(!caps.contains(Capability::GcControl));
    }

    #[test]
    fn client_admin_gets_meta_admin_gc_and_data_path_get_put_delete() {
        let caps = RoleCapabilities::default_for(WorkloadRole::ClientAdmin);
        assert!(caps.contains(Capability::MutateMeta));
        assert!(caps.contains(Capability::Admin));
        // FUSE mount dials workers for data path; admin tooling may stage
        // chunk content via the same identity.
        assert!(caps.contains(Capability::PutChunk));
        assert!(caps.contains(Capability::GetChunk));
        // GC tombstone sweep + orphan GC drive worker delete_chunk via the
        // same client identity.
        assert!(caps.contains(Capability::DeleteChunk));
    }

    // ----- Principal::has + require -----

    #[test]
    fn principal_has_reflects_role_policy() {
        let worker = Principal::new(WorkloadIdentity::worker(WorkerTargetId(1)), "");
        assert!(worker.has(Capability::PutChunk));
        assert!(worker.has(Capability::MutateMeta));
        assert!(!worker.has(Capability::Admin));

        let admin = Principal::new(WorkloadIdentity::client_admin("ops"), "");
        assert!(admin.has(Capability::Admin));
        assert!(admin.has(Capability::PutChunk));
    }

    #[test]
    fn require_allows_when_capable() {
        let p = Principal::new(WorkloadIdentity::worker(WorkerTargetId(2)), "");
        assert!(require(&p, Capability::PutChunk).is_ok());
    }

    #[test]
    fn require_denies_with_structured_reason_when_not_capable() {
        let p = Principal::new(WorkloadIdentity::worker(WorkerTargetId(2)), "");
        let outcome = require(&p, Capability::GcControl);
        match outcome {
            Err(DenyReason::Unauthorized {
                role,
                required,
                detail,
            }) => {
                assert_eq!(role, WorkloadRole::Worker);
                assert_eq!(required, Capability::GcControl);
                assert!(detail.contains("lacks required capability"));
            }
            other => panic!("expected Unauthorized, got {other:?}"),
        }
    }

    #[test]
    fn require_deny_message_includes_spiffe_uri_for_audit() {
        let p = Principal::new(WorkloadIdentity::meta("g0"), "abc123");
        // Meta lacks data-path caps.
        let outcome = require(&p, Capability::DeleteChunk);
        let detail = match outcome {
            Err(DenyReason::Unauthorized { detail, .. }) => detail,
            other => panic!("expected Unauthorized, got {other:?}"),
        };
        assert!(
            detail.contains("spiffe://fluxfs/meta/g0"),
            "detail should include the SPIFFE URI for audit correlation: {detail}"
        );
    }

    // ----- DenyReason → FluxError mapping -----

    #[test]
    fn unauthenticated_maps_to_flux_unauthenticated() {
        let err = DenyReason::unauthenticated("no client cert").into_flux_error();
        assert!(matches!(err, crate::FluxError::Unauthenticated(_)));
    }

    #[test]
    fn unauthorized_maps_to_flux_unauthorized() {
        let err = DenyReason::unauthorized(
            WorkloadRole::Worker,
            Capability::Admin,
            "workers cannot admin",
        )
        .into_flux_error();
        assert!(matches!(err, crate::FluxError::Unauthorized(_)));
    }

    #[test]
    fn tenant_denied_maps_to_flux_unauthorized() {
        // TenantDenied reuses Unauthorized until a dedicated FluxError variant
        // is added (Phase 3 will introduce it if needed).
        let err = DenyReason::tenant_denied("mount token revoked").into_flux_error();
        assert!(matches!(err, crate::FluxError::Unauthorized(_)));
    }

    // ----- Serde round-trip -----

    #[test]
    fn workload_identity_serde_roundtrip() {
        let id = WorkloadIdentity {
            role: WorkloadRole::Worker,
            name: "5".into(),
            worker_target_id: Some(WorkerTargetId(5)),
            tenant_id: Some(TenantId(42)),
        };
        let json = serde_json::to_string(&id).expect("ser");
        let back: WorkloadIdentity = serde_json::from_str(&json).expect("de");
        assert_eq!(back, id);
    }

    #[test]
    fn workload_role_serde_kebab_case() {
        // Wire-stable kebab-case so future roles can be added without
        // breaking camelCase parsers.
        let json = serde_json::to_string(&WorkloadRole::ClientAdmin).expect("ser");
        assert_eq!(json, "\"client-admin\"");
        let back: WorkloadRole = serde_json::from_str("\"client-admin\"").expect("de");
        assert_eq!(back, WorkloadRole::ClientAdmin);
    }

    #[test]
    fn principal_serde_roundtrip_with_fingerprint() {
        let p = Principal::new(WorkloadIdentity::meta("g0"), "abc".repeat(21));
        let json = serde_json::to_string(&p).expect("ser");
        let back: Principal = serde_json::from_str(&json).expect("de");
        assert_eq!(back, p);
    }

    #[test]
    fn tenant_id_default_is_bootstrap() {
        assert_eq!(TenantId::default(), TenantId::BOOTSTRAP);
        assert_eq!(TenantId::default().0, 0);
    }

    #[test]
    fn deny_reason_serde_roundtrip_unauthorized_variant() {
        let r = DenyReason::unauthorized(WorkloadRole::Worker, Capability::Admin, "nope");
        let json = serde_json::to_string(&r).expect("ser");
        let back: DenyReason = serde_json::from_str(&json).expect("de");
        assert_eq!(back, r);
    }
}
