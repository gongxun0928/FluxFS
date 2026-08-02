//! Shared mTLS / transport-security helpers for FluxFS RPCs (task #30 C1,
//! Phase 2).
//!
//! Centralizes PEM loading + tonic `ServerTlsConfig` / `ClientTlsConfig`
//! construction so metamaster / chunkworker / meta-client / chunk-client
//! share one fail-closed policy:
//!
//! - If any TLS option is set, all required pieces (CA + identity) must be
//!   set; partial config is an error, not a silent downgrade.
//! - When TLS is enabled, servers require client certs (mTLS). Use
//!   [`ServerTlsOptions::allow_no_client_cert`] only for the dev CA's
//!   bootstrap path; production rejects it.
//! - Plaintext (`http://`) is permitted only when the caller explicitly
//!   opts in via [`InsecureDev::allow`]. The default is fail-closed.
//!
//! # Status: Phase 2 — transport only
//!
//! This module does NOT extract workload identity from peer certs. That
//! lives in Phase 3 (`tonic::service::Interceptor` reading peer certs via
//! `Request::extensions`). See task #30 thread msg `5c77d96b`.

use fluxfs_types::FluxError;
use fluxfs_types::Result as FluxResult;
use std::path::{Path, PathBuf};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// Load a PEM file from disk into bytes.
async fn load_pem(path: &Path) -> FluxResult<Vec<u8>> {
    tokio::fs::read(path)
        .await
        .map_err(|e| FluxError::InvalidArg(format!("tls: read {}: {e}", path.display())))
}

/// Sync variant of [`load_pem`] for callers that build TLS config from a
/// non-async context (e.g. the chunk-client constructor spawned from a
/// sync CLI path).
fn load_pem_blocking(path: &Path) -> FluxResult<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| FluxError::InvalidArg(format!("tls: read {}: {e}", path.display())))
}

/// Server-side TLS options. All paths required when [`Self::enabled`] is true.
///
/// `ca_cert` is the trust anchor for verifying client certs (mTLS). Server
/// presents `server_cert` + `server_key` as its identity.
#[derive(Debug, Clone, Default)]
pub struct ServerTlsOptions {
    pub enabled: bool,
    pub ca_cert: Option<PathBuf>,
    pub server_cert: Option<PathBuf>,
    pub server_key: Option<PathBuf>,
    /// If true, the server accepts connections without a client cert. Use only
    /// for dev/bootstrap paths; production MUST leave this false so mTLS is
    /// enforced.
    pub allow_no_client_cert: bool,
}

impl ServerTlsOptions {
    pub fn from_cli(
        ca_cert: Option<PathBuf>,
        server_cert: Option<PathBuf>,
        server_key: Option<PathBuf>,
        allow_insecure_dev: bool,
    ) -> FluxResult<Self> {
        let server_set = server_cert.is_some() || server_key.is_some();
        if !server_set {
            if allow_insecure_dev {
                // Explicit dev plaintext opt-in.
                return Ok(Self {
                    enabled: false,
                    allow_no_client_cert: false,
                    ..Default::default()
                });
            }
            // Fail-closed default: no TLS config + no explicit dev opt-in.
            return Err(FluxError::InvalidArg(
                "tls: server has no --tls-server-cert; refuse plaintext. Pass --tls-server-cert \
                 (production) or --allow-insecure-dev (tests only)."
                    .into(),
            ));
        }
        // mTLS server requires all three: identity + CA to verify clients.
        let ca_cert = ca_cert.ok_or_else(|| {
            FluxError::InvalidArg(
                "tls: --tls-server-cert set without --tls-ca-cert; mTLS requires a client CA to \
                 verify client certs. Set --tls-ca-cert (production) or --allow-insecure-dev \
                 (tests only)."
                    .into(),
            )
        })?;
        let server_cert = server_cert
            .ok_or_else(|| FluxError::InvalidArg("tls: --tls-server-cert missing".into()))?;
        let server_key = server_key
            .ok_or_else(|| FluxError::InvalidArg("tls: --tls-server-key missing".into()))?;
        Ok(Self {
            enabled: true,
            ca_cert: Some(ca_cert),
            server_cert: Some(server_cert),
            server_key: Some(server_key),
            allow_no_client_cert: false,
        })
    }

    /// Load PEMs and build a [`ServerTlsConfig`]. Returns `Ok(None)` when
    /// TLS is disabled (caller must have already opted into plaintext via
    /// `--allow-insecure-dev`).
    pub async fn build_config(&self) -> FluxResult<Option<ServerTlsConfig>> {
        if !self.enabled {
            return Ok(None);
        }
        let ca_path = self.ca_cert.as_ref().expect("enabled implies ca_cert");
        let cert_path = self
            .server_cert
            .as_ref()
            .expect("enabled implies server_cert");
        let key_path = self
            .server_key
            .as_ref()
            .expect("enabled implies server_key");
        let ca_pem = load_pem(ca_path).await?;
        let cert_pem = load_pem(cert_path).await?;
        let key_pem = load_pem(key_path).await?;
        let mut cfg = ServerTlsConfig::new()
            .identity(Identity::from_pem(cert_pem, key_pem))
            .client_ca_root(Certificate::from_pem(ca_pem));
        if self.allow_no_client_cert {
            cfg = cfg.client_auth_optional(true);
        }
        Ok(Some(cfg))
    }

    /// Sync variant of [`Self::build_config`] for callers that build TLS
    /// config from a non-async context.
    pub fn build_config_blocking(&self) -> FluxResult<Option<ServerTlsConfig>> {
        if !self.enabled {
            return Ok(None);
        }
        let ca_path = self.ca_cert.as_ref().expect("enabled implies ca_cert");
        let cert_path = self
            .server_cert
            .as_ref()
            .expect("enabled implies server_cert");
        let key_path = self
            .server_key
            .as_ref()
            .expect("enabled implies server_key");
        let ca_pem = load_pem_blocking(ca_path)?;
        let cert_pem = load_pem_blocking(cert_path)?;
        let key_pem = load_pem_blocking(key_path)?;
        let mut cfg = ServerTlsConfig::new()
            .identity(Identity::from_pem(cert_pem, key_pem))
            .client_ca_root(Certificate::from_pem(ca_pem));
        if self.allow_no_client_cert {
            cfg = cfg.client_auth_optional(true);
        }
        Ok(Some(cfg))
    }
}

/// Client-side TLS options. `domain` is the SNI / server-name to verify
/// against (defaults to the host part of the endpoint URL when None).
#[derive(Debug, Clone, Default)]
pub struct ClientTlsOptions {
    pub enabled: bool,
    pub ca_cert: Option<PathBuf>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    pub domain: Option<String>,
}

impl ClientTlsOptions {
    /// Construct from CLI flags. When `ca_cert` is set, the client uses TLS
    /// with server verification against that CA. When `client_cert`/`client_key`
    /// are also set, the client presents an identity (mTLS). Both-or-neither
    /// for client_cert/client_key.
    pub fn from_cli(
        ca_cert: Option<PathBuf>,
        client_cert: Option<PathBuf>,
        client_key: Option<PathBuf>,
        domain: Option<String>,
    ) -> FluxResult<Self> {
        let ca_set = ca_cert.is_some();
        let id_set = client_cert.is_some() || client_key.is_some();
        if id_set {
            // Both halves of the client identity are required together.
            let cc = client_cert.clone().ok_or_else(|| {
                FluxError::InvalidArg("tls: --tls-client-key set without --tls-client-cert".into())
            })?;
            let ck = client_key.clone().ok_or_else(|| {
                FluxError::InvalidArg("tls: --tls-client-cert set without --tls-client-key".into())
            })?;
            // Client identity implies a CA must be configured too (we don't
            // fall back to webpki/native roots in production paths; the
            // cluster CA is explicit).
            let ca = ca_cert
                .clone()
                .ok_or_else(|| FluxError::InvalidArg("tls: --tls-client-cert set without --tls-ca-cert; client must pin the cluster CA".into()))?;
            return Ok(Self {
                enabled: true,
                ca_cert: Some(ca),
                client_cert: Some(cc),
                client_key: Some(ck),
                domain,
            });
        }
        if ca_set {
            // TLS with server verification but no client identity — server
            // must be in allow_no_client_cert mode (dev/bootstrap). The CA
            // alone is enough to verify the server.
            return Ok(Self {
                enabled: true,
                ca_cert,
                client_cert: None,
                client_key: None,
                domain,
            });
        }
        // Neither CA nor client identity. Caller must opt into plaintext.
        Ok(Self {
            enabled: false,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            domain,
        })
    }

    pub async fn build_config(&self) -> FluxResult<Option<ClientTlsConfig>> {
        if !self.enabled {
            return Ok(None);
        }
        let ca_path = self.ca_cert.as_ref().expect("enabled implies ca_cert");
        let ca_pem = load_pem(ca_path).await?;
        let mut cfg = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_pem));
        if let Some(cc) = &self.client_cert {
            let ck = self
                .client_key
                .as_ref()
                .expect("client_cert implies client_key");
            let cert_pem = load_pem(cc).await?;
            let key_pem = load_pem(ck).await?;
            cfg = cfg.identity(Identity::from_pem(cert_pem, key_pem));
        }
        if let Some(d) = &self.domain {
            cfg = cfg.domain_name(d.clone());
        }
        Ok(Some(cfg))
    }

    /// Sync variant of [`Self::build_config`].
    pub fn build_config_blocking(&self) -> FluxResult<Option<ClientTlsConfig>> {
        if !self.enabled {
            return Ok(None);
        }
        let ca_path = self.ca_cert.as_ref().expect("enabled implies ca_cert");
        let ca_pem = load_pem_blocking(ca_path)?;
        let mut cfg = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_pem));
        if let Some(cc) = &self.client_cert {
            let ck = self
                .client_key
                .as_ref()
                .expect("client_cert implies client_key");
            let cert_pem = load_pem_blocking(cc)?;
            let key_pem = load_pem_blocking(ck)?;
            cfg = cfg.identity(Identity::from_pem(cert_pem, key_pem));
        }
        if let Some(d) = &self.domain {
            cfg = cfg.domain_name(d.clone());
        }
        Ok(Some(cfg))
    }
}

/// Tracks whether the caller has opted into plaintext (`--allow-insecure-dev`).
///
/// Production defaults to fail-closed: a plaintext endpoint URL (`http://`)
/// without an explicit opt-in is an error, not a silent downgrade.
#[derive(Debug, Clone, Copy, Default)]
pub struct InsecureDev {
    allowed: bool,
}

impl InsecureDev {
    pub fn allow(allowed: bool) -> Self {
        Self { allowed }
    }

    /// Returns Ok(()) iff the caller may use plaintext for this endpoint.
    /// Errors carry the action guidance so the binary can surface it.
    pub fn check_endpoint(&self, endpoint_url: &str) -> FluxResult<()> {
        if endpoint_url.starts_with("https://") {
            return Ok(());
        }
        if endpoint_url.starts_with("http://") {
            if self.allowed {
                return Ok(());
            }
            return Err(FluxError::InvalidArg(format!(
                "tls: plaintext endpoint {endpoint_url} refused without --allow-insecure-dev; \
                 pass --tls-* flags (production) or --allow-insecure-dev (tests only)"
            )));
        }
        // Bare "host:port" — caller normalizes to http(s):// before calling.
        Ok(())
    }

    /// Validate that the URL scheme matches whether TLS is configured. Cursor
    /// review nit (msg 1e5c04e0 #2/#3): if a caller passes `https://` but no
    /// TLS options, that's a misconfiguration (tonic would attempt TLS without
    /// any trust anchor); if they pass `http://` with TLS options, likewise.
    /// Catch both explicitly so the failure is actionable, not a mystery
    /// handshake error from deep in hyper.
    pub fn check_scheme_matches_tls(
        &self,
        endpoint_url: &str,
        tls_enabled: bool,
    ) -> FluxResult<()> {
        let is_https = endpoint_url.starts_with("https://");
        let is_http = endpoint_url.starts_with("http://");
        if is_https && !tls_enabled {
            return Err(FluxError::InvalidArg(format!(
                "tls: https endpoint {endpoint_url} but no --tls-ca-cert configured; pass \
                 --tls-ca-cert (and --tls-client-cert/--tls-client-key for mTLS) or change the \
                 URL to http:// and add --allow-insecure-dev"
            )));
        }
        if is_http && tls_enabled {
            return Err(FluxError::InvalidArg(format!(
                "tls: http endpoint {endpoint_url} with TLS flags set; change the URL to https:// \
                 or drop the TLS flags (and add --allow-insecure-dev for plaintext)"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }

    #[tokio::test]
    async fn server_tls_options_fail_closed_without_any_tls_flags() {
        let err = ServerTlsOptions::from_cli(None, None, None, false).unwrap_err();
        assert!(matches!(err, FluxError::InvalidArg(_)));
        assert!(err.to_string().contains("refuse plaintext"));
    }

    #[tokio::test]
    async fn server_tls_options_allow_insecure_dev_opts_into_plaintext() {
        let opts = ServerTlsOptions::from_cli(None, None, None, true).unwrap();
        assert!(!opts.enabled);
        assert!(opts.build_config().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn server_tls_options_require_ca_when_identity_set() {
        let dir = tmp();
        let cert = dir.join("c.pem");
        let key = dir.join("k.pem");
        tokio::fs::write(&cert, b"x").await.unwrap();
        tokio::fs::write(&key, b"x").await.unwrap();
        let err = ServerTlsOptions::from_cli(None, Some(cert), Some(key), false).unwrap_err();
        assert!(err.to_string().contains("without --tls-ca-cert"));
    }

    #[tokio::test]
    async fn server_tls_options_reject_partial_identity() {
        let dir = tmp();
        let cert = dir.join("c.pem");
        let ca = dir.join("ca.pem");
        tokio::fs::write(&cert, b"x").await.unwrap();
        tokio::fs::write(&ca, b"x").await.unwrap();
        let err = ServerTlsOptions::from_cli(Some(ca), Some(cert), None, false).unwrap_err();
        assert!(err.to_string().contains("--tls-server-key missing"));
    }

    #[tokio::test]
    async fn client_tls_options_require_ca_when_client_identity_set() {
        let dir = tmp();
        let cc = dir.join("cc.pem");
        let ck = dir.join("ck.pem");
        tokio::fs::write(&cc, b"x").await.unwrap();
        tokio::fs::write(&ck, b"x").await.unwrap();
        let err =
            ClientTlsOptions::from_cli(None, Some(cc), Some(ck), Some("meta".into())).unwrap_err();
        assert!(err.to_string().contains("pin the cluster CA"));
    }

    #[tokio::test]
    async fn client_tls_options_reject_partial_identity() {
        let dir = tmp();
        let cc = dir.join("cc.pem");
        let ca = dir.join("ca.pem");
        tokio::fs::write(&cc, b"x").await.unwrap();
        tokio::fs::write(&ca, b"x").await.unwrap();
        let err =
            ClientTlsOptions::from_cli(Some(ca), Some(cc), None, Some("meta".into())).unwrap_err();
        assert!(err
            .to_string()
            .contains("--tls-client-cert set without --tls-client-key"));
    }

    #[tokio::test]
    async fn client_tls_options_ca_only_is_tls_without_client_identity() {
        let dir = tmp();
        let ca = dir.join("ca.pem");
        tokio::fs::write(&ca, b"x").await.unwrap();
        let opts = ClientTlsOptions::from_cli(Some(ca), None, None, Some("meta".into())).unwrap();
        assert!(opts.enabled);
        assert!(opts.client_cert.is_none());
    }

    #[test]
    fn insecure_dev_refuses_plaintext_without_opt_in() {
        let dev = InsecureDev::allow(false);
        let err = dev.check_endpoint("http://meta:50051").unwrap_err();
        assert!(err.to_string().contains("--allow-insecure-dev"));
    }

    #[test]
    fn insecure_dev_accepts_plaintext_with_opt_in() {
        let dev = InsecureDev::allow(true);
        assert!(dev.check_endpoint("http://meta:50051").is_ok());
    }

    #[test]
    fn insecure_dev_accepts_https_regardless() {
        let dev = InsecureDev::allow(false);
        assert!(dev.check_endpoint("https://meta:50051").is_ok());
    }

    #[test]
    fn insecure_dev_accepts_bare_host_port() {
        // Caller is responsible for scheme normalization; bare host:port is
        // left alone so the dialer can prepend http(s):// based on TLS flags.
        let dev = InsecureDev::allow(false);
        assert!(dev.check_endpoint("meta:50051").is_ok());
    }

    #[test]
    fn scheme_check_rejects_https_without_tls() {
        let dev = InsecureDev::allow(false);
        let err = dev
            .check_scheme_matches_tls("https://meta:50051", false)
            .unwrap_err();
        assert!(err.to_string().contains("no --tls-ca-cert configured"));
    }

    #[test]
    fn scheme_check_rejects_http_with_tls() {
        let dev = InsecureDev::allow(true);
        let err = dev
            .check_scheme_matches_tls("http://meta:50051", true)
            .unwrap_err();
        assert!(err.to_string().contains("with TLS flags set"));
    }

    #[test]
    fn scheme_check_accepts_aligned_pairs() {
        let dev = InsecureDev::allow(false);
        assert!(dev
            .check_scheme_matches_tls("https://meta:50051", true)
            .is_ok());
        let dev = InsecureDev::allow(true);
        assert!(dev
            .check_scheme_matches_tls("http://meta:50051", false)
            .is_ok());
        // Bare host:port is left for caller normalization.
        assert!(dev.check_scheme_matches_tls("meta:50051", false).is_ok());
        assert!(dev.check_scheme_matches_tls("meta:50051", true).is_ok());
    }
}
