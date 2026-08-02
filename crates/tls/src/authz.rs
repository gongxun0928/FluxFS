//! Phase 3 authz interceptor: extract [`WorkloadIdentity`] from peer client
//! certs and enforce **role-level** admission to a gRPC server (task #30 C1
//! Phase 3).
//!
//! Wiring: metamaster / chunkworker construct one [`AuthzInterceptor`] with
//! the set of [`WorkloadRole`] they accept, then install it via
//! `tonic::transport::Server::interceptor_fn`. The interceptor reads peer
//! certs from `tonic::transport::server::TlsConnectInfo`, parses the SPIFFE
//! URI SAN with the same fail-closed matrix as
//! [`fluxfs_types::auth::WorkloadIdentity::from_spiffe_uri`], and denies on
//! any ambiguity (no certs, malformed SAN, unknown role, role not in allow
//! list).
//!
//! # Why role-level and not per-RPC
//!
//! tonic's `interceptor_fn` runs on a `Request<()>` whose `uri()` has already
//! been stripped — see `tonic::service::interceptor::InterceptedService::call`
//! in tonic 0.14. The interceptor therefore cannot see the gRPC method path,
//! so per-RPC capability tables cannot be enforced there. This module
//! implements the minimal role-level admission gate; per-RPC capability
//! enforcement (a static role→cap map applied at the service-handler layer)
//! is deferred to a follow-up. The [`Capability`] / [`Principal`] /
//! [`fluxfs_types::auth::require`] primitives in `fluxfs-types` already
//! support that follow-up without API churn.
//!
//! # Tenant / mount-token enforcement (Phase 4)
//!
//! This interceptor only enforces transport-layer identity + role. It does
//! NOT consult `MountToken` / `TenantId` — Phase 4 wires mount-token
//! enrollment into the Meta state machine and adds a per-tenant capability
//! override layer.

use fluxfs_types::auth::{Capability, Principal, WorkloadIdentity, WorkloadRole};
use fluxfs_types::FluxError;
use tonic::transport::server::TlsConnectInfo;
use tonic::{Request, Status};

/// OID of the X.509 subjectAltName extension (2.5.29.17).
const OID_SAN: x509_parser::asn1_rs::Oid<'static> = x509_parser::asn1_rs::oid!(2.5.29 .17);

/// OID of the SPIFFE URI otherName entry (1.3.6.1.4.1.52372.1.2).
const OID_SPIFFE: x509_parser::asn1_rs::Oid<'static> =
    x509_parser::asn1_rs::oid!(1.3.6 .1 .4 .1 .52372 .1 .2);

/// Extract the SPIFFE URI (`spiffe://fluxfs/<role>/<name>`) from the peer's
/// end-entity certificate. Returns `None` when:
/// - there are no peer certs (TLS wasn't configured for mTLS, or the peer
///   connected without a client cert and the server is in
///   `allow_no_client_cert` mode),
/// - the cert has no `subjectAltName` extension,
/// - the SAN has no `otherName` entry under the SPIFFE OID
///   (`1.3.6.1.4.1.52372.1.2`),
/// - the otherName payload is not a UTF8 string matching the spiffe scheme.
///
/// All of these are fail-closed: the interceptor denies the call rather than
/// running as anonymous.
pub fn extract_spiffe_uri_from_peer_certs(
    peer_certs: Option<&[rustls::pki_types::CertificateDer<'static>]>,
) -> Option<String> {
    let certs = peer_certs?;
    let end_entity = certs.first()?;
    // x509-parser: parse the end-entity DER and pull the SAN extension.
    let (_, parsed) = x509_parser::parse_x509_certificate(end_entity.as_ref()).ok()?;
    let san = parsed.extensions().iter().find_map(|ext| {
        if ext.oid == OID_SAN {
            match ext.parsed_extension() {
                x509_parser::extensions::ParsedExtension::SubjectAlternativeName(s) => Some(s),
                _ => None,
            }
        } else {
            None
        }
    })?;
    // Iterate otherName entries; find the SPIFFE URI OID.
    for gn in &san.general_names {
        if let x509_parser::extensions::GeneralName::OtherName(oid, value) = gn {
            if *oid == OID_SPIFFE {
                // value is the raw OtherName value payload; SPIFFE stores the
                // URI as a UTF8String. x509-parser hands it back still tagged
                // — pull the inner string. Be tolerant of a leading ASN.1
                // UTF8String tag (0x0C) + length; if neither matches, treat
                // the whole payload as a UTF8 string best-effort.
                if let Some(s) = decode_spiffe_othername(value) {
                    return Some(s);
                }
            }
        }
    }
    None
}

/// SPIFFE otherName payload is `[0] EXPLICIT UTF8String(<uri>)` per the
/// OtherName construction in RFC 5280 (`value [0] EXPLICIT ANY DEFINED BY
/// type-id`). OpenSSL emits the explicit `[0]` wrapper (`0xA0 <len>`) around
/// the inner UTF8String (`0x0C <len> <bytes>`). x509-parser hands us the
/// raw otherName value bytes including that wrapper.
///
/// Accept all three encodings we've seen:
///   1. `A0 <len> 0C <len> <utf8>` — full explicit form (openssl, rustls)
///   2. `0C <len> <utf8>` — pre-peeled UTF8String
///   3. `<utf8 bytes>` — already-unwrapped string body (some decoders)
fn decode_spiffe_othername(bytes: &[u8]) -> Option<String> {
    let mut cur = bytes;
    // Peel the `[0] EXPLICIT` context-constructed wrapper if present.
    if cur.len() >= 2 && (cur[0] & 0xC0) == 0x80 && (cur[0] & 0x1F) == 0 {
        // context-specific constructed tag class (0x80) + tag number 0.
        let len = cur[1] as usize;
        if 2 + len == cur.len() {
            cur = &cur[2..];
        } else {
            return None;
        }
    }
    // Peel the UTF8String tag (0x0C) + short-form length if present.
    let payload = if cur.len() >= 2 && cur[0] == 0x0C {
        let len = cur[1] as usize;
        if 2 + len == cur.len() {
            &cur[2..]
        } else {
            return None;
        }
    } else {
        cur
    };
    let s = std::str::from_utf8(payload).ok()?;
    if s.starts_with("spiffe://") {
        Some(s.to_string())
    } else {
        None
    }
}

/// Resolve the [`Principal`] for an incoming gRPC request.
///
/// Reads peer certs from `TlsConnectInfo` (inserted by tonic when TLS is
/// configured). On any failure (no certs, unparseable SAN, unknown SPIFFE
/// role) returns `Err(DenyReason::Unauthenticated)` so the interceptor can
/// surface a uniform `tonic::Code::Unauthenticated`.
pub fn principal_from_request<T>(
    req: &Request<T>,
) -> Result<Principal, fluxfs_types::auth::DenyReason> {
    use fluxfs_types::auth::DenyReason;

    let tls_info = req
        .extensions()
        .get::<TlsConnectInfo<tonic::transport::server::TcpConnectInfo>>();
    // peer_certs() returns Option<Arc<Vec<CertificateDer>>>; keep the Arc
    // alive in `peer_certs` and project a slice reference for the parser.
    let peer_certs = tls_info.and_then(|info| info.peer_certs());
    let peer_certs_slice: Option<&[rustls::pki_types::CertificateDer<'static>]> =
        peer_certs.as_ref().map(|v| v.as_slice());
    let spiffe = extract_spiffe_uri_from_peer_certs(peer_certs_slice).ok_or_else(|| {
        DenyReason::unauthenticated(
            "no SPIFFE URI SAN in peer client cert (mTLS misconfigured or anonymous dial)",
        )
    })?;
    let identity = WorkloadIdentity::from_spiffe_uri(&spiffe).ok_or_else(|| {
        DenyReason::unauthenticated(format!("unparsable SPIFFE URI SAN: {spiffe}"))
    })?;
    // Fingerprint: SHA-256 of the end-entity DER, lowercase hex. Best-effort;
    // empty string if hashing fails (shouldn't).
    let fingerprint = peer_certs_slice
        .and_then(|cs| cs.first())
        .map(|c| sha256_hex_lower(c.as_ref()))
        .unwrap_or_default();
    Ok(Principal::new(identity, fingerprint))
}

fn sha256_hex_lower(bytes: &[u8]) -> String {
    // Minimal SHA-256 + hex, no extra dep. Used only for audit correlation.
    let h = sha256(bytes);
    let mut out = String::with_capacity(64);
    for b in h {
        out.push(hex_nibble(b >> 4));
        out.push(hex_nibble(b & 0x0F));
    }
    out
}

fn hex_nibble(n: u8) -> char {
    if n < 10 {
        (b'0' + n) as char
    } else {
        (b'a' + (n - 10)) as char
    }
}

fn sha256(data: &[u8]) -> [u8; 32] {
    // Use fluxfs-types' blake3 dep? No — SHA-256 is what X.509 fingerprints
    // are. Inline a compact implementation to avoid pulling sha2.
    let mut h = [
        0x6a09e667u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    let k = [
        0x428a2f98u32,
        0x71374491,
        0xb5c0fbcf,
        0xe9b5dba5,
        0x3956c25b,
        0x59f111f1,
        0x923f82a4,
        0xab1c5ed5,
        0xd807aa98,
        0x12835b01,
        0x243185be,
        0x550c7dc3,
        0x72be5d74,
        0x80deb1fe,
        0x9bdc06a7,
        0xc19bf174,
        0xe49b69c1,
        0xefbe4786,
        0x0fc19dc6,
        0x240ca1cc,
        0x2de92c6f,
        0x4a7484aa,
        0x5cb0a9dc,
        0x76f988da,
        0x983e5152,
        0xa831c66d,
        0xb00327c8,
        0xbf597fc7,
        0xc6e00bf3,
        0xd5a79147,
        0x06ca6351,
        0x14292967,
        0x27b70a85,
        0x2e1b2138,
        0x4d2c6dfc,
        0x53380d13,
        0x650a7354,
        0x766a0abb,
        0x81c2c92e,
        0x92722c85,
        0xa2bfe8a1,
        0xa81a664b,
        0xc24b8b70,
        0xc76c51a3,
        0xd192e819,
        0xd6990624,
        0xf40e3585,
        0x106aa070,
        0x19a4c116,
        0x1e376c08,
        0x2748774c,
        0x34b0bcb5,
        0x391c0cb3,
        0x4ed8aa4a,
        0x5b9cca4f,
        0x682e6ff3,
        0x748f82ee,
        0x78a5636f,
        0x84c87814,
        0x8cc70208,
        0x90befffa,
        0xa4506ceb,
        0xbef9a3f7,
        0xc67178f2,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(k[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let mj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(mj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for (i, w) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
    }
    out
}

/// Role-level authz interceptor. Built once at server startup with the set
/// of [`WorkloadRole`]s that server admits; installed via `interceptor_fn`.
///
/// # Why not per-RPC?
///
/// tonic 0.14's `interceptor_fn` runs on a `Request<()>` whose URI has
/// already been stripped (see `tonic::service::interceptor`), so the
/// interceptor cannot see the gRPC method path. Per-RPC capability tables
/// belong at the service-handler layer; this module will host the minimal
/// role-level gate until that follow-up lands.
pub struct AuthzInterceptor {
    allowed: Vec<WorkloadRole>,
}

impl AuthzInterceptor {
    pub fn new(allowed: Vec<WorkloadRole>) -> Self {
        Self { allowed }
    }

    /// Convenience constructors for the two production servers.
    pub fn for_meta() -> Self {
        // MetaService accepts dials from meta peers, chunkworkers, and
        // client-admin (mount / orphan-gc). All three roles are admitted;
        // tighter per-RPC scoping comes with the service-layer follow-up.
        Self::new(vec![
            WorkloadRole::Meta,
            WorkloadRole::Worker,
            WorkloadRole::ClientAdmin,
        ])
    }

    pub fn for_worker() -> Self {
        // ChunkWorker accepts dials from meta (chunk fetch during recovery)
        // and client-admin (FUSE mount). Peer workers do not dial each
        // other in Phase 3.
        Self::new(vec![WorkloadRole::Meta, WorkloadRole::ClientAdmin])
    }

    /// Run the authz check for a request. Returns the resolved Principal on
    /// success. The interceptor layer (`InterceptorLayer::new(...)`) discards
    /// the return value — for per-method enforcement, use
    /// [`require_in_extensions`] inside each service handler.
    pub fn check<T>(&self, req: &Request<T>) -> Result<Principal, Status> {
        use fluxfs_types::auth::DenyReason;

        let principal = principal_from_request(req).map_err(deny_to_status)?;
        if !self.allowed.contains(&principal.identity.role) {
            // Per-RPC capability tables are enforced per-handler (see
            // require_in_extensions); here we report Admin as the "required"
            // capability (the strongest in the policy lattice) so the deny
            // message is honest about how tight this gate actually is.
            let deny = DenyReason::unauthorized(
                principal.identity.role,
                fluxfs_types::auth::Capability::Admin,
                "role not admitted by this server",
            );
            return Err(deny_to_status(deny));
        }
        Ok(principal)
    }

    /// And-style: same role check, then stash the resolved Principal into
    /// the request's extensions so per-handler `require_in_extensions` can
    /// pull it out for per-method capability enforcement.
    ///
    /// Use this as the interceptor closure body: it returns the modified
    /// `Request<()>` carrying the Principal for downstream handlers.
    pub fn check_and_attach(&self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let principal = self.check(&req)?;
        req.extensions_mut().insert(principal);
        Ok(req)
    }
}

/// Per-handler capability enforcement. Service handlers call this at the top
/// of each RPC method:
///
/// ```ignore
/// async fn create(&self, req: Request<CreateRequest>) -> Result<...> {
///     require_in_extensions(&req, Capability::MutateMeta)?;
///     // ... handler body ...
/// }
/// ```
///
/// Pulls the [`Principal`] previously injected by
/// [`AuthzInterceptor::check_and_attach`] out of request extensions and runs
/// [`fluxfs_types::auth::require`] for the requested capability. Denies map
/// through the same `FluxError → tonic::Status` wire contract as
/// interceptor-level denials.
pub fn require_in_extensions<T>(req: &Request<T>, cap: Capability) -> Result<(), Status> {
    use fluxfs_types::auth::{require, DenyReason};

    let principal = req
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| {
            deny_to_status(DenyReason::unauthenticated(
                "authz: no Principal in request extensions (interceptor not installed?)",
            ))
        })?;
    require(&principal, cap).map_err(deny_to_status)
}

fn deny_to_status(reason: fluxfs_types::auth::DenyReason) -> Status {
    let err: FluxError = reason.into_flux_error();
    fluxfs_proto_status(err)
}

/// Use the meta_codec mapping (FluxError → tonic::Status with structured
/// details). Re-exports the A7 path so authz denies carry the same wire
/// contract as business-logic errors.
fn fluxfs_proto_status(err: FluxError) -> Status {
    // We can't import fluxfs_proto here (would create a cycle: proto depends
    // on types; types must not depend on proto). Inline the structured-detail
    // construction instead — the wire format is owned by meta_codec but the
    // construction is mechanical.
    use tonic::Code;
    let code = match &err {
        FluxError::Unauthenticated(_) => Code::Unauthenticated,
        FluxError::Unauthorized(_) => Code::PermissionDenied,
        _ => Code::Internal,
    };
    let details = serde_json::to_vec(&err).unwrap_or_default();
    Status::with_details(code, err.to_string(), details.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_meta_admits_all_known_roles() {
        let iz = AuthzInterceptor::for_meta();
        assert!(iz.allowed.contains(&WorkloadRole::Meta));
        assert!(iz.allowed.contains(&WorkloadRole::Worker));
        assert!(iz.allowed.contains(&WorkloadRole::ClientAdmin));
    }

    #[test]
    fn for_worker_excludes_peer_workers() {
        let iz = AuthzInterceptor::for_worker();
        assert!(iz.allowed.contains(&WorkloadRole::Meta));
        assert!(iz.allowed.contains(&WorkloadRole::ClientAdmin));
        assert!(!iz.allowed.contains(&WorkloadRole::Worker));
    }

    #[test]
    fn extract_spiffe_returns_none_for_empty_certs() {
        assert!(extract_spiffe_uri_from_peer_certs(None).is_none());
        assert!(extract_spiffe_uri_from_peer_certs(Some(&[])).is_none());
    }

    #[test]
    fn decode_spiffe_only_accepts_spiffe_scheme() {
        // Bare UTF8 bytes.
        assert_eq!(
            decode_spiffe_othername(b"spiffe://fluxfs/meta/g0"),
            Some("spiffe://fluxfs/meta/g0".into())
        );
        // Tagged UTF8String only (0x0C + length).
        assert_eq!(
            decode_spiffe_othername(&{
                let payload = b"spiffe://fluxfs/x/y";
                let mut v = vec![0x0C, payload.len() as u8];
                v.extend_from_slice(payload);
                v
            }),
            Some("spiffe://fluxfs/x/y".into())
        );
        // Full `[0] EXPLICIT UTF8String` form as emitted by OpenSSL for
        // otherName. SPIFFE URI = `spiffe://fluxfs/meta/g0` (20 bytes).
        assert_eq!(
            decode_spiffe_othername(&{
                let uri = b"spiffe://fluxfs/meta/g0";
                // a0 <total_len> 0c <uri_len> <uri>
                let total = 2 + uri.len();
                let mut v = vec![0xA0, total as u8, 0x0C, uri.len() as u8];
                v.extend_from_slice(uri);
                v
            }),
            Some("spiffe://fluxfs/meta/g0".into())
        );
        // Wrong scheme.
        assert!(decode_spiffe_othername(b"https://not-spiffe").is_none());
        // Length-mismatch in inner UTF8String.
        assert!(decode_spiffe_othername(&[0x0C, 99, b'x']).is_none());
        // Length-mismatch in the outer [0] wrapper.
        assert!(decode_spiffe_othername(&[0xA0, 99, 0x0C, 5, b'h', b'e', b'l']).is_none());
    }

    #[test]
    fn sha256_matches_known_vector() {
        // "abc" SHA-256 = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        let h = sha256(b"abc");
        let hex: String = h.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
