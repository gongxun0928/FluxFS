//! UFS access via OpenDAL.
//!
//! Alpha: `external-consistency = best-effort`. Prefetch / parallel Range GET
//! are layered on this adapter (patterns inspired by ZeroFS, no full fork).

use fluxfs_types::{FluxError, Result, UfsObject};
use opendal::{services, Operator};
use std::path::Path;

pub struct Ufs {
    op: Operator,
    /// Logical prefix inside the operator root.
    prefix: String,
}

impl Ufs {
    /// Local filesystem UFS for W1 smoke / benches.
    pub fn local(root: impl AsRef<Path>) -> Result<Self> {
        let builder = services::Fs::default().root(
            root.as_ref()
                .to_str()
                .ok_or_else(|| FluxError::Ufs("non-utf8 path".into()))?,
        );
        let op = Operator::new(builder).map_err(|e| FluxError::Ufs(e.to_string()))?;
        Ok(Self {
            op,
            prefix: String::new(),
        })
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn key(&self, rel: &str) -> String {
        if self.prefix.is_empty() {
            rel.to_string()
        } else {
            format!(
                "{}/{}",
                self.prefix.trim_end_matches('/'),
                rel.trim_start_matches('/')
            )
        }
    }

    pub async fn head(&self, rel: &str) -> Result<UfsObject> {
        let key = self.key(rel);
        let meta = self.op.stat(&key).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                FluxError::NotFound
            } else {
                FluxError::Ufs(e.to_string())
            }
        })?;
        Ok(UfsObject {
            key,
            size: meta.content_length(),
            etag: meta.etag().map(|s| s.to_string()),
            mtime_ms: meta
                .last_modified()
                .map(|t| t.into_inner().as_millisecond()),
        })
    }

    pub async fn read_range(&self, rel: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let key = self.key(rel);
        let end = offset.saturating_add(len);
        let buf = self
            .op
            .read_with(&key)
            .range(offset..end)
            .await
            .map_err(|e| {
                if e.kind() == opendal::ErrorKind::NotFound {
                    FluxError::NotFound
                } else {
                    FluxError::Ufs(e.to_string())
                }
            })?;
        Ok(buf.to_vec())
    }

    pub async fn write_full(&self, rel: &str, data: &[u8]) -> Result<UfsObject> {
        let key = self.key(rel);
        self.op
            .write(&key, data.to_vec())
            .await
            .map_err(|e| FluxError::Ufs(e.to_string()))?;
        self.head(rel).await
    }
}
