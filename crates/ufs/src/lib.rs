//! UFS access via OpenDAL.
//!
//! Alpha: `external-consistency = best-effort`. Prefetch / parallel Range GET
//! are layered on this adapter (patterns inspired by ZeroFS, no full fork).

use fluxfs_types::{FluxError, Result, UfsObject};
use futures::future::try_join_all;
use opendal::{services, EntryMode, Operator};
use std::path::Path;

/// Connection parameters for S3-compatible UFS (AWS S3, MinIO, …).
#[derive(Debug, Clone)]
pub struct S3Options {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// Optional key prefix / root inside the bucket.
    pub root: Option<String>,
}

impl S3Options {
    /// Build from `FLUXFS_UFS_*` env vars (used by `fluxfs ufs-check` / scripts).
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            endpoint: env_req("FLUXFS_UFS_ENDPOINT")?,
            bucket: env_req("FLUXFS_UFS_BUCKET")?,
            region: std::env::var("FLUXFS_UFS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access_key: env_req("FLUXFS_UFS_ACCESS_KEY")?,
            secret_key: env_req("FLUXFS_UFS_SECRET_KEY")?,
            root: std::env::var("FLUXFS_UFS_ROOT").ok().filter(|s| !s.is_empty()),
        })
    }
}

fn env_req(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| {
        FluxError::Ufs(format!(
            "missing env {key}; run scripts/dev-minio.sh and export FLUXFS_UFS_*"
        ))
    })
}

pub struct Ufs {
    op: Operator,
    /// Logical prefix inside the operator root.
    prefix: String,
}

/// One entry returned by [`Ufs::list`].
#[derive(Debug, Clone)]
pub struct UfsEntry {
    /// Path relative to the UFS prefix (no leading slash).
    pub path: String,
    pub mode: UfsEntryMode,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UfsEntryMode {
    File,
    Dir,
}

/// A single Range GET request for [`Ufs::read_ranges`].
#[derive(Debug, Clone, Copy)]
pub struct RangeReq {
    pub offset: u64,
    pub len: u64,
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

    /// S3-compatible UFS (MinIO / AWS S3). Path-style by default (MinIO-friendly).
    pub fn s3(opts: &S3Options) -> Result<Self> {
        let mut builder = services::S3::default()
            .endpoint(&opts.endpoint)
            .bucket(&opts.bucket)
            .region(&opts.region)
            .access_key_id(&opts.access_key)
            .secret_access_key(&opts.secret_key);
        if let Some(root) = &opts.root {
            builder = builder.root(root);
        }
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
            rel.trim_start_matches('/').to_string()
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
        let meta = self.op.stat(&key).await.map_err(map_opendal)?;
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
            .map_err(map_opendal)?;
        Ok(buf.to_vec())
    }

    /// Parallel Range GETs for one object (ZeroFS-inspired read fan-out).
    ///
    /// Returns buffers in the same order as `ranges`. Caller owns prefetch
    /// window / coalescing policy; this only executes concurrent OpenDAL reads.
    pub async fn read_ranges(&self, rel: &str, ranges: &[RangeReq]) -> Result<Vec<Vec<u8>>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let key = self.key(rel);
        let futs = ranges.iter().map(|r| {
            let op = self.op.clone();
            let key = key.clone();
            let offset = r.offset;
            let end = r.offset.saturating_add(r.len);
            async move {
                let buf = op
                    .read_with(&key)
                    .range(offset..end)
                    .await
                    .map_err(map_opendal)?;
                Ok::<Vec<u8>, FluxError>(buf.to_vec())
            }
        });
        try_join_all(futs).await
    }

    pub async fn write_full(&self, rel: &str, data: &[u8]) -> Result<UfsObject> {
        let key = self.key(rel);
        self.op
            .write(&key, data.to_vec())
            .await
            .map_err(|e| FluxError::Ufs(e.to_string()))?;
        self.head(rel).await
    }

    /// List one directory level under `rel` (non-recursive). Empty `rel` = prefix root.
    pub async fn list(&self, rel: &str) -> Result<Vec<UfsEntry>> {
        let key = self.key(rel);
        let list_path = if key.is_empty() {
            "/".to_string()
        } else if key.ends_with('/') {
            key
        } else {
            format!("{key}/")
        };
        let entries = self
            .op
            .list(&list_path)
            .await
            .map_err(|e| FluxError::Ufs(e.to_string()))?;
        let mut out = Vec::with_capacity(entries.len());
        for ent in entries {
            let path = ent.path().trim_start_matches('/').to_string();
            // Skip the directory placeholder itself.
            if path.is_empty() || path.trim_end_matches('/') == rel.trim_matches('/') {
                continue;
            }
            let mode = match ent.metadata().mode() {
                EntryMode::FILE => UfsEntryMode::File,
                EntryMode::DIR => UfsEntryMode::Dir,
                _ => continue,
            };
            out.push(UfsEntry {
                path,
                mode,
                size: ent.metadata().content_length(),
            });
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }
}

fn map_opendal(e: opendal::Error) -> FluxError {
    if e.kind() == opendal::ErrorKind::NotFound {
        FluxError::NotFound
    } else {
        FluxError::Ufs(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_roundtrip_and_parallel_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        let payload = b"0123456789abcdef";
        ufs.write_full("obj.bin", payload).await.unwrap();
        let head = ufs.head("obj.bin").await.unwrap();
        assert_eq!(head.size, payload.len() as u64);

        let parts = ufs
            .read_ranges(
                "obj.bin",
                &[
                    RangeReq { offset: 0, len: 4 },
                    RangeReq { offset: 4, len: 4 },
                    RangeReq { offset: 8, len: 8 },
                ],
            )
            .await
            .unwrap();
        assert_eq!(parts[0], b"0123");
        assert_eq!(parts[1], b"4567");
        assert_eq!(parts[2], b"89abcdef");
        assert_eq!(
            [&parts[0][..], &parts[1][..], &parts[2][..]].concat(),
            payload
        );
    }
}
