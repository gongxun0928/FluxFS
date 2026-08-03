//! UFS access via OpenDAL.
//!
//! Alpha: `external-consistency = best-effort`. Prefetch / parallel Range GET
//! are layered on this adapter (patterns inspired by ZeroFS, no full fork).

use fluxfs_types::{ChunkId, FluxError, Result, UfsObject};
use futures::future::try_join_all;
use opendal::{services, EntryMode, Operator, Writer};
use std::path::Path;

mod read_path;

pub use read_path::{ReadPathConfig, ReadPathStats, UfsReadPath};

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
            root: std::env::var("FLUXFS_UFS_ROOT")
                .ok()
                .filter(|s| !s.is_empty()),
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

#[derive(Clone)]
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

/// Result of [`Ufs::probe`] — distinguishes files from directory prefixes.
#[derive(Debug, Clone)]
pub enum UfsProbe {
    File(UfsObject),
    Dir,
}

const FLUXFS_DIGEST_METADATA: &str = "fluxfs-blake3";
/// S3 requires non-final multipart parts to be at least 5 MiB. Eight MiB keeps
/// upload memory bounded while avoiding backend-specific minimum-size edges.
pub const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// A conditional, digest-verifying object publication assembled incrementally.
///
/// OpenDAL selects multipart upload for S3 when the configured chunk fills.
/// Callers must call [`Self::abort`] after any upstream reconstruction error;
/// `finish` aborts automatically when size/digest verification fails.
pub struct VerifiedPublishWriter {
    writer: Writer,
    op: Operator,
    write_key: String,
    key: String,
    rel: String,
    staged_local_publish: bool,
    expected_etag: Option<String>,
    expected_size: u64,
    target_digest: ChunkId,
    digest_hex: String,
    hasher: blake3::Hasher,
    written: u64,
}

impl VerifiedPublishWriter {
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        let next = self
            .written
            .checked_add(data.len() as u64)
            .ok_or_else(|| FluxError::InvalidArg("publish size overflow".into()))?;
        if next > self.expected_size {
            let _ = self.writer.abort().await;
            if self.staged_local_publish {
                let _ = self.op.delete(&self.write_key).await;
            }
            return Err(FluxError::InvalidArg(format!(
                "publish payload exceeds expected size {}",
                self.expected_size
            )));
        }
        if let Err(error) = self.writer.write(data.to_vec()).await {
            let _ = self.writer.abort().await;
            if self.staged_local_publish {
                let _ = self.op.delete(&self.write_key).await;
            }
            return Err(map_publish_error(error));
        }
        self.hasher.update(data);
        self.written = next;
        Ok(())
    }

    pub async fn abort(&mut self) -> Result<()> {
        self.writer.abort().await.map_err(map_opendal)?;
        if self.staged_local_publish {
            self.op.delete(&self.write_key).await.map_err(map_opendal)?;
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<UfsObject> {
        let actual_digest = ChunkId::from_raw(*self.hasher.finalize().as_bytes());
        if self.written != self.expected_size || actual_digest != self.target_digest {
            let _ = self.writer.abort().await;
            if self.staged_local_publish {
                let _ = self.op.delete(&self.write_key).await;
            }
            return Err(FluxError::InvalidArg(format!(
                "publish payload verification failed: size={}/{} digest={}/{}",
                self.written,
                self.expected_size,
                actual_digest.to_hex(),
                self.target_digest.to_hex()
            )));
        }
        if let Err(error) = self.writer.close().await {
            let _ = self.writer.abort().await;
            if self.staged_local_publish {
                let _ = self.op.delete(&self.write_key).await;
            }
            return Err(map_publish_error(error));
        }

        if self.staged_local_publish {
            let destination = self.op.stat(&self.key).await;
            let condition_matches = match (&self.expected_etag, destination) {
                (None, Err(error)) if error.kind() == opendal::ErrorKind::NotFound => true,
                (Some(expected), Ok(metadata)) => metadata.etag() == Some(expected.as_str()),
                _ => false,
            };
            if !condition_matches {
                let _ = self.op.delete(&self.write_key).await;
                return Err(FluxError::DirtyConflict);
            }
            if let Err(error) = self.op.rename(&self.write_key, &self.key).await {
                let _ = self.op.delete(&self.write_key).await;
                return Err(map_publish_error(error));
            }
        }

        let meta = self.op.stat(&self.key).await.map_err(map_opendal)?;
        if meta.content_length() != self.expected_size {
            return Err(FluxError::Io(format!(
                "published UFS size mismatch: want={} got={}",
                self.expected_size,
                meta.content_length()
            )));
        }
        let stored_digest = meta
            .user_metadata()
            .and_then(|metadata| metadata.get(FLUXFS_DIGEST_METADATA));
        if stored_digest != Some(&self.digest_hex) {
            return Err(FluxError::Io(
                "published UFS digest metadata missing or mismatched".into(),
            ));
        }

        Ok(UfsObject {
            key: self.rel,
            size: meta.content_length(),
            etag: meta.etag().map(str::to_owned),
            mtime_ms: meta
                .last_modified()
                .map(|time| time.into_inner().as_millisecond()),
        })
    }
}

/// Backend guarantees required for crash-recoverable conditional publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishCapabilities {
    pub conditional_overwrite: bool,
    pub conditional_create: bool,
    pub verifiable_digest_metadata: bool,
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

    pub fn publish_capabilities(&self) -> PublishCapabilities {
        let caps = self.op.info().capability();
        PublishCapabilities {
            conditional_overwrite: caps.write_with_if_match,
            conditional_create: caps.write_with_if_not_exists,
            verifiable_digest_metadata: caps.write_with_user_metadata,
        }
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
        match self.probe(rel).await? {
            UfsProbe::File(obj) => Ok(obj),
            UfsProbe::Dir => Err(FluxError::Ufs(format!("path is a directory: {rel}"))),
        }
    }

    /// Stat a path and classify it as file or directory.
    pub async fn probe(&self, rel: &str) -> Result<UfsProbe> {
        let key = self.key(rel);
        let meta = self.op.stat(&key).await.map_err(map_opendal)?;
        match meta.mode() {
            EntryMode::DIR => Ok(UfsProbe::Dir),
            EntryMode::FILE => Ok(UfsProbe::File(UfsObject {
                key,
                size: meta.content_length(),
                etag: meta.etag().map(|s| s.to_string()),
                mtime_ms: meta
                    .last_modified()
                    .map(|t| t.into_inner().as_millisecond()),
            })),
            _ => Err(FluxError::Ufs(format!(
                "unsupported UFS entry mode at {rel}"
            ))),
        }
    }

    pub async fn read_range(&self, rel: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.read_range_pinned(rel, offset, len, None).await
    }

    /// Range GET pinned to an object ETag when the backend supports If-Match.
    pub async fn read_range_pinned(
        &self,
        rel: &str,
        offset: u64,
        len: u64,
        etag: Option<&str>,
    ) -> Result<Vec<u8>> {
        let key = self.key(rel);
        let end = offset.saturating_add(len);
        let read = self.op.read_with(&key).range(offset..end);
        let buf = match etag {
            Some(etag) => read.if_match(etag).await,
            None => read.await,
        }
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

    /// Publish one complete object under an optimistic concurrency condition,
    /// then verify its durable size and FluxFS content digest through HEAD.
    ///
    /// `expected_etag=Some` is a compare-and-swap overwrite. `None` is a
    /// create-if-absent operation. Backends that cannot enforce the requested
    /// condition or preserve user metadata are rejected instead of silently
    /// degrading to an unsafe unconditional write.
    pub async fn publish_full_verified(
        &self,
        rel: &str,
        data: &[u8],
        expected_etag: Option<&str>,
        target_digest: &ChunkId,
    ) -> Result<UfsObject> {
        let mut writer = self
            .begin_verified_publish(rel, data.len() as u64, expected_etag, target_digest)
            .await?;
        writer.write(data).await?;
        writer.finish().await
    }

    /// Start a bounded-memory conditional publication. On S3 this uses
    /// multipart upload; the object becomes visible only after `finish` closes
    /// the writer successfully.
    pub async fn begin_verified_publish(
        &self,
        rel: &str,
        expected_size: u64,
        expected_etag: Option<&str>,
        target_digest: &ChunkId,
    ) -> Result<VerifiedPublishWriter> {
        let caps = self.publish_capabilities();
        if !caps.verifiable_digest_metadata {
            return Err(FluxError::Capability(
                "UFS backend cannot persist verification metadata".into(),
            ));
        }
        match expected_etag {
            Some(_) if !caps.conditional_overwrite => {
                return Err(FluxError::Capability(
                    "UFS backend does not support conditional overwrite".into(),
                ));
            }
            None if !caps.conditional_create => {
                return Err(FluxError::Capability(
                    "UFS backend does not support create-if-absent".into(),
                ));
            }
            _ => {}
        }

        let key = self.key(rel);
        // Filesystem writers expose an opened/truncated destination before
        // close. Stage locally and rename only after payload verification so a
        // failed or aborted stream never publishes a partial object. S3 writes
        // directly to the final key because multipart completion is atomic.
        let staged_local_publish = self.op.info().scheme() == "fs";
        let write_key = if staged_local_publish {
            format!("{key}.fluxfs-upload-{}", uuid::Uuid::new_v4())
        } else {
            key.clone()
        };
        let digest_hex = target_digest.to_hex();
        let write = self
            .op
            .writer_with(&write_key)
            .chunk(MULTIPART_CHUNK_BYTES)
            .concurrent(4)
            .user_metadata([(FLUXFS_DIGEST_METADATA.to_string(), digest_hex.clone())]);
        let writer = match (staged_local_publish, expected_etag) {
            (true, _) => write.if_not_exists(true).await,
            (false, Some(etag)) => write.if_match(etag).await,
            (false, None) => write.if_not_exists(true).await,
        }
        .map_err(map_publish_error)?;

        Ok(VerifiedPublishWriter {
            writer,
            op: self.op.clone(),
            write_key,
            key,
            rel: rel.to_string(),
            staged_local_publish,
            expected_etag: expected_etag.map(str::to_owned),
            expected_size,
            target_digest: *target_digest,
            digest_hex,
            hasher: blake3::Hasher::new(),
            written: 0,
        })
    }

    /// Return the published object only when HEAD proves it is exactly the
    /// intended payload. Used after restart to distinguish "Put succeeded,
    /// metadata commit lost" from "Put never happened" without downloading it.
    pub async fn find_verified_publish(
        &self,
        rel: &str,
        expected_size: u64,
        target_digest: &ChunkId,
    ) -> Result<Option<UfsObject>> {
        let key = self.key(rel);
        let meta = match self.op.stat(&key).await {
            Ok(meta) => meta,
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(map_opendal(error)),
        };
        let digest_hex = target_digest.to_hex();
        if meta.content_length() != expected_size
            || meta
                .user_metadata()
                .and_then(|metadata| metadata.get(FLUXFS_DIGEST_METADATA))
                != Some(&digest_hex)
        {
            return Ok(None);
        }
        Ok(Some(UfsObject {
            key: rel.to_string(),
            size: meta.content_length(),
            etag: meta.etag().map(str::to_owned),
            mtime_ms: meta
                .last_modified()
                .map(|time| time.into_inner().as_millisecond()),
        }))
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
    match e.kind() {
        opendal::ErrorKind::NotFound => FluxError::NotFound,
        opendal::ErrorKind::ConditionNotMatch => FluxError::DirtyConflict,
        _ => FluxError::Ufs(e.to_string()),
    }
}

fn map_publish_error(e: opendal::Error) -> FluxError {
    match e.kind() {
        opendal::ErrorKind::ConditionNotMatch | opendal::ErrorKind::AlreadyExists => {
            FluxError::DirtyConflict
        }
        _ => map_opendal(e),
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

    #[tokio::test]
    async fn verified_publish_requires_digest_and_create_condition() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        let payload = b"recoverable-publish";
        let digest = ChunkId::from_bytes(payload);

        let published = ufs
            .publish_full_verified("new.bin", payload, None, &digest)
            .await
            .unwrap();
        assert_eq!(published.size, payload.len() as u64);
        assert_eq!(
            ufs.read_range("new.bin", 0, published.size).await.unwrap(),
            payload
        );

        let conflict = ufs
            .publish_full_verified(
                "new.bin",
                b"replacement",
                None,
                &ChunkId::from_bytes(b"replacement"),
            )
            .await
            .unwrap_err();
        assert_eq!(conflict, FluxError::DirtyConflict);

        let bad_digest = ufs
            .publish_full_verified("bad.bin", payload, None, &ChunkId::from_bytes(b"wrong"))
            .await
            .unwrap_err();
        assert!(matches!(bad_digest, FluxError::InvalidArg(_)));
    }

    #[tokio::test]
    async fn verified_stream_publish_is_incremental_and_aborts_bad_payload() {
        let dir = tempfile::tempdir().unwrap();
        let ufs = Ufs::local(dir.path()).unwrap();
        let pieces = [b"streamed-".as_slice(), b"multipart-", b"payload"];
        let payload = pieces.concat();
        let digest = ChunkId::from_bytes(&payload);
        let mut writer = ufs
            .begin_verified_publish("stream.bin", payload.len() as u64, None, &digest)
            .await
            .unwrap();
        for piece in pieces {
            writer.write(piece).await.unwrap();
        }
        let published = writer.finish().await.unwrap();
        assert_eq!(published.size, payload.len() as u64);
        assert_eq!(
            ufs.read_range("stream.bin", 0, published.size)
                .await
                .unwrap(),
            payload
        );

        let mut incomplete = ufs
            .begin_verified_publish("incomplete.bin", 8, None, &ChunkId::from_bytes(b"12345678"))
            .await
            .unwrap();
        incomplete.write(b"1234").await.unwrap();
        assert!(matches!(
            incomplete.finish().await,
            Err(FluxError::InvalidArg(_))
        ));
        assert_eq!(
            ufs.head("incomplete.bin").await.unwrap_err(),
            FluxError::NotFound
        );
    }

    #[test]
    fn conditional_read_mismatch_is_a_dirty_conflict() {
        let error =
            opendal::Error::new(opendal::ErrorKind::ConditionNotMatch, "pinned ETag changed");
        assert_eq!(map_opendal(error), FluxError::DirtyConflict);
    }
}
