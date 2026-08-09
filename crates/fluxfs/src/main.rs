use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use fluxfs_chunk::{ChunkStore, DiskChunkStore, RemoteReplicatedChunkStore, ReplicatedChunkStore};
use fluxfs_client::FluxClient;
use fluxfs_meta::{start_single_voter, HeedMetaStore, MetaStore, RaftMetaStore, RemoteMetaStore};
use fluxfs_types::{FileType, ROOT_INODE};
use fluxfs_ufs::{RangeReq, ReadPathConfig, S3Options, Ufs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(name = "fluxfs", about = "FluxFS MVP co-located control binary")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

/// Client-side TLS args shared by subcommands that dial Meta/Worker (task #30 C1).
#[derive(Parser, Debug, Clone, Default)]
struct TlsClientArgs {
    /// Cluster CA cert (PEM) for verifying server identity. Required when
    /// --tls-client-cert is set (mTLS); production default.
    #[arg(long)]
    tls_ca_cert: Option<PathBuf>,
    /// Client identity cert (PEM). Setting this enables TLS + mTLS.
    #[arg(long)]
    tls_client_cert: Option<PathBuf>,
    /// Client identity key (PEM). Paired with --tls-client-cert.
    #[arg(long)]
    tls_client_key: Option<PathBuf>,
    /// Explicit plaintext opt-in (tests only). Production MUST pass TLS flags.
    #[arg(long, default_value_t = false)]
    allow_insecure_dev: bool,
}

impl TlsClientArgs {
    /// Build a `ClientTlsOptions` from the parsed flags (None when TLS is
    /// disabled, in which case the caller must have opted into plaintext).
    fn build(
        &self,
        domain: Option<String>,
    ) -> anyhow::Result<Option<fluxfs_tls::ClientTlsOptions>> {
        let opts = fluxfs_tls::ClientTlsOptions::from_cli(
            self.tls_ca_cert.clone(),
            self.tls_client_cert.clone(),
            self.tls_client_key.clone(),
            domain,
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(if opts.enabled { Some(opts) } else { None })
    }
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Meta create/lookup + chunk put/get + local UFS HEAD smoke.
    Smoke {
        #[arg(long, default_value = "/tmp/fluxfs-smoke")]
        data_dir: PathBuf,
    },
    /// Mount FluxFS at a local path. Blocking.
    Mount {
        /// Persistent data directory (local meta if --meta-addr unset; always holds chunks).
        #[arg(long, default_value = "/tmp/fluxfs-data")]
        data_dir: PathBuf,
        /// Mount point (must exist and be empty/usable).
        #[arg(long)]
        mountpoint: PathBuf,
        /// Ephemeral mount (no UFS). Mutually exclusive with `--ufs`.
        #[arg(long, default_value_t = false)]
        no_ufs: bool,
        /// UFS URI for backed mount: `s3://bucket[/prefix]` or `file:///abs/path`.
        /// Requires `FLUXFS_UFS_*` env for S3 credentials/endpoint (see `scripts/dev-minio.sh`).
        #[arg(long)]
        ufs: Option<String>,
        /// Optional remote MetaMaster (`host:port` or `http://host:port`).
        #[arg(long)]
        meta_addr: Option<String>,
        /// Remote ChunkWorker URL. Repeat three times for the v0 multi-process topology.
        /// The first two form the fixed RF=2 replica set; the third is a repair spare.
        #[arg(long = "chunk-worker")]
        chunk_workers: Vec<String>,
        /// Maximum chunk operations waiting in the remote client queue.
        #[arg(long, default_value_t = fluxfs_chunk::DEFAULT_MAX_PENDING_CHUNK_OPS)]
        chunk_max_pending: usize,
        /// Clean/External UFS HybridCache DRAM capacity (task #39).
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        ufs_cache_memory_bytes: usize,
        /// Clean/External UFS HybridCache SSD capacity. `0` disables the disk tier.
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        ufs_cache_disk_bytes: usize,
        /// Directory for the UFS foyer SSD device. Defaults to
        /// `<data-dir>/ufs-foyer-cache` (isolated from Worker `foyer-cache`).
        #[arg(long)]
        ufs_cache_dir: Option<PathBuf>,
        #[command(flatten)]
        tls: TlsClientArgs,
    },
    /// Ping a remote MetaMaster (multi-process smoke).
    MetaPing {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        #[command(flatten)]
        tls: TlsClientArgs,
    },
    /// Probe a remote ChunkWorker via its Health RPC (mTLS / Phase 3 authz).
    ///
    /// Used by test-mtls-acceptance.sh to assert cross-role rejection at the
    /// authz interceptor: a worker cert dialing a worker server hits
    /// for_worker() (admits {Meta, ClientAdmin}) and must be rejected with
    /// PermissionDenied — a real RPC, not Unimplemented from a wrong service.
    ///
    /// `--mode put` switches to a PutChunk probe. Combined with a Meta cert
    /// (admitted by for_worker but lacking the PutChunk capability), this
    /// verifies the per-handler `require(cap)` table fires after the role-
    /// level interceptor passes — gpt56 msg ac8ef471 #2.
    ChunkProbe {
        #[arg(long, default_value = "127.0.0.1:50061")]
        addr: String,
        /// Which RPC to drive: `health` (default) or `put`.
        #[arg(long, default_value = "health")]
        mode: String,
        #[command(flatten)]
        tls: TlsClientArgs,
    },
    /// OpenDAL/S3 smoke against MinIO (or any S3 endpoint via `FLUXFS_UFS_*`).
    UfsCheck {
        /// Object key to write/read (default: fluxfs-ufs-check.bin).
        #[arg(long, default_value = "fluxfs-ufs-check.bin")]
        key: String,
    },
    /// Quiesced orphan GC (stop-the-world). Not run automatically on mount.
    ///
    /// Safe only when no writer can stage chunks that commit after the GC
    /// snapshot. Prefer online reservation/tombstone GC once it lands; this
    /// command remains for admin/tests until then.
    OrphanGc {
        #[arg(long, default_value = "/tmp/fluxfs-data")]
        data_dir: PathBuf,
        #[arg(long)]
        meta_addr: Option<String>,
        #[arg(long = "chunk-worker")]
        chunk_workers: Vec<String>,
        #[arg(long, default_value_t = fluxfs_chunk::DEFAULT_MAX_PENDING_CHUNK_OPS)]
        chunk_max_pending: usize,
        #[command(flatten)]
        tls: TlsClientArgs,
    },
    /// Compact a local Worker pack store (rewrite live chunks, drop hole segments).
    CompactChunks {
        /// Worker data directory (contains `segments/` + `index/`).
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Print stack / topology freeze summary.
    Info,
}

#[tokio::main]
async fn main() -> Result<()> {
    fluxfs_tls::install_crypto_provider();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::CompactChunks { data_dir } => {
            let store = DiskChunkStore::open(&data_dir).context("open chunk store")?;
            let report = store.compact().context("compact chunks")?;
            println!(
                "compact-chunks ok: live={} removed_segments={} data_dir={}",
                report.live_chunks,
                report.removed_segments,
                data_dir.display()
            );
        }
        Cmd::Info => {
            println!("FluxFS MVP");
            println!("  repo: https://github.com/gongxun0928/FluxFS");
            println!("  meta: heed MetaStore; remote via fluxfs-metamaster (tonic)");
            println!("  chunk: ReplicatedChunkStore RF=2; Worker RPC next");
            println!("  ufs: OpenDAL (local FS / S3); parallel Range GET via Ufs::read_ranges");
            println!("  fuse_supported: {}", fluxfs_fuse::mount_supported());
            println!("  multi-process: fluxfs-metamaster --listen 127.0.0.1:50051 --data-dir DIR");
            println!("  mount ephemeral: fluxfs mount --no-ufs --data-dir DIR --mountpoint MNT");
            println!(
                "  mount ufs: fluxfs mount --ufs s3://bucket[/prefix]|file:///path --data-dir DIR --mountpoint MNT"
            );
            println!(
                "  ufs test bed: bash scripts/dev-minio.sh && cargo run -p fluxfs -- ufs-check"
            );
        }
        Cmd::Smoke { data_dir } => {
            run_smoke(data_dir).await?;
        }
        Cmd::Mount {
            data_dir,
            mountpoint,
            no_ufs,
            ufs,
            meta_addr,
            chunk_workers,
            chunk_max_pending,
            ufs_cache_memory_bytes,
            ufs_cache_disk_bytes,
            ufs_cache_dir,
            tls,
        } => match (no_ufs, ufs.as_deref()) {
            (true, None) => run_mount(
                data_dir,
                mountpoint,
                meta_addr,
                chunk_workers,
                chunk_max_pending,
                None,
                tls,
            )?,
            (false, Some(uri)) => {
                let ufs = open_ufs_uri(uri).context("open --ufs")?;
                let read_cfg = ReadPathConfig::for_mount(
                    &data_dir,
                    ufs_cache_memory_bytes,
                    ufs_cache_disk_bytes,
                    ufs_cache_dir,
                );
                run_mount(
                    data_dir,
                    mountpoint,
                    meta_addr,
                    chunk_workers,
                    chunk_max_pending,
                    Some((ufs, read_cfg)),
                    tls,
                )?;
            }
            (true, Some(_)) => bail!("pass either --no-ufs or --ufs, not both"),
            (false, None) => {
                bail!("mount requires --no-ufs (Ephemeral) or --ufs <uri> (UFS-backed)")
            }
        },
        Cmd::MetaPing { addr, tls } => {
            run_meta_ping(&addr, &tls)?;
        }
        Cmd::ChunkProbe { addr, mode, tls } => {
            run_chunk_probe(&addr, &mode, &tls).await?;
        }
        Cmd::UfsCheck { key } => {
            run_ufs_check(&key).await?;
        }
        Cmd::OrphanGc {
            data_dir,
            meta_addr,
            chunk_workers,
            chunk_max_pending,
            tls,
        } => {
            run_orphan_gc_cmd(data_dir, meta_addr, chunk_workers, chunk_max_pending, tls)?;
        }
    }
    Ok(())
}

async fn run_ufs_check(key: &str) -> Result<()> {
    let opts = S3Options::from_env().context("load FLUXFS_UFS_*")?;
    let ufs = Ufs::s3(&opts).context("open S3 UFS")?;
    let payload = b"fluxfs-opendal-minio-ok";
    let written = ufs.write_full(key, payload).await.context("ufs write")?;
    let head = ufs.head(key).await.context("ufs head")?;
    assert_eq!(head.size, payload.len() as u64);
    let ranges = ufs
        .read_ranges(
            key,
            &[
                RangeReq { offset: 0, len: 6 },
                RangeReq {
                    offset: 6,
                    len: (payload.len() as u64).saturating_sub(6),
                },
            ],
        )
        .await
        .context("ufs parallel ranges")?;
    let joined: Vec<u8> = ranges.into_iter().flatten().collect();
    assert_eq!(joined, payload);
    let listed = ufs.list("").await.context("ufs list")?;

    // External lazy namespace path (same stack as `--ufs` mount, without FUSE).
    let tmp = tempfile::tempdir().context("tempdir")?;
    let meta = HeedMetaStore::open(tmp.path().join("meta")).context("meta")?;
    let chunks = DiskChunkStore::open(tmp.path().join("chunks")).context("chunks")?;
    let client = FluxClient::new(meta, chunks)
        .with_ufs(Ufs::s3(&opts).context("ufs for client")?)
        .context("attach ufs")?;
    let imported = client.lookup(ROOT_INODE, key).context("lazy lookup")?;
    assert_eq!(
        imported.locality,
        fluxfs_types::LocalityLabel::External,
        "expected External locality"
    );
    let got = client.read_all(imported.id).context("external read")?;
    assert_eq!(got, payload);

    println!("ufs-check ok");
    println!("  endpoint={}", opts.endpoint);
    println!("  bucket={}", opts.bucket);
    println!(
        "  key={} size={} etag={:?}",
        written.key, written.size, written.etag
    );
    println!("  list_entries={}", listed.len());
    println!(
        "  external_inode={} locality={:?}",
        imported.id, imported.locality
    );
    Ok(())
}

async fn run_smoke(data_dir: PathBuf) -> Result<()> {
    let meta_path = data_dir.join("meta");
    let chunk_path = data_dir.join("chunks");
    let ufs_path = data_dir.join("ufs");
    std::fs::create_dir_all(&ufs_path)?;

    let meta = HeedMetaStore::open(&meta_path).context("open meta")?;
    let chunks = DiskChunkStore::open(&chunk_path).context("open chunks")?;
    let client = FluxClient::new(meta, chunks);

    let dir = client
        .mkdir(ROOT_INODE, "testdir", 0o755, 0, 0)
        .context("mkdir testdir")?;
    let file = client
        .create_file(dir.id, "hello.txt", 0o644, 0, 0)
        .context("create hello.txt")?;
    client
        .write_at(file.id, 0, b"hello fluxfs")
        .context("write")?;
    let got = client.read_all(file.id).context("read")?;
    assert_eq!(got, b"hello fluxfs");
    assert_eq!(file.file_type, FileType::Regular);

    let ufs = Ufs::local(&ufs_path)?;
    let obj = ufs.write_full("obj.bin", b"ufs-bytes").await?;
    let head = ufs.head("obj.bin").await?;
    assert_eq!(head.size, obj.size);

    println!("smoke ok");
    println!("  root={}", client.root());
    println!("  file_inode={} bytes={}", file.id, got.len());
    println!("  ufs_key={} size={}", head.key, head.size);
    println!("  meta_path={}", meta_path.display());
    Ok(())
}

fn open_ufs_uri(uri: &str) -> Result<Ufs> {
    if let Some(path) = uri.strip_prefix("file://") {
        return Ufs::local(path).map_err(Into::into);
    }
    if let Some(rest) = uri.strip_prefix("s3://") {
        let rest = rest.trim_start_matches('/');
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b.to_string(), p.trim_matches('/').to_string()),
            None => (rest.to_string(), String::new()),
        };
        if bucket.is_empty() {
            bail!("s3 URI missing bucket: {uri}");
        }
        let mut opts = S3Options::from_env().context("load FLUXFS_UFS_* for s3:// mount")?;
        opts.bucket = bucket;
        if !prefix.is_empty() {
            opts.root = Some(prefix);
        }
        return Ufs::s3(&opts).map_err(Into::into);
    }
    bail!("unsupported --ufs URI (want file:///path or s3://bucket[/prefix]): {uri}")
}

fn run_mount(
    data_dir: PathBuf,
    mountpoint: PathBuf,
    meta_addr: Option<String>,
    chunk_workers: Vec<String>,
    chunk_max_pending: usize,
    ufs: Option<(Ufs, ReadPathConfig)>,
    tls: TlsClientArgs,
) -> Result<()> {
    if !fluxfs_fuse::mount_supported() {
        bail!("FUSE mount only supported on Linux in this build");
    }
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&mountpoint)?;
    if chunk_workers.is_empty() && meta_addr.is_some() {
        let client_tls = tls.build(None)?;
        let meta = RemoteMetaStore::connect_tls(
            meta_addr.as_deref().expect("checked"),
            client_tls.clone(),
            tls.allow_insecure_dev,
        )
        .context("connect MetaMaster for Worker discovery")?;
        let membership = meta.worker_membership().context("get Worker membership")?;
        let chunks = RemoteReplicatedChunkStore::new_with_membership_discovery_tls(
            membership,
            meta_addr.as_deref().expect("checked").to_string(),
            2,
            chunk_max_pending,
            unix_time_millis(),
            client_tls,
            tls.allow_insecure_dev,
        )
        .context("configure membership-discovered RF=2 chunks")?;
        return mount_remote_chunks(data_dir, mountpoint, meta_addr, chunks, ufs, tls);
    }
    if chunk_workers.is_empty() {
        let chunks = ReplicatedChunkStore::open_rf2(
            data_dir.join("chunks/worker-0"),
            data_dir.join("chunks/worker-1"),
        )
        .context("open local RF=2 chunks")?;
        return mount_with_chunks(
            data_dir,
            mountpoint,
            meta_addr,
            chunks,
            "local RF=2",
            ufs,
            tls,
        );
    }
    if chunk_workers.len() != 3 {
        bail!(
            "multi-process v0 requires exactly three --chunk-worker URLs; got {}",
            chunk_workers.len()
        );
    }
    let chunk_tls = tls.build(None)?;
    let chunks = RemoteReplicatedChunkStore::new_with_max_pending_tls(
        chunk_workers,
        2,
        chunk_max_pending,
        chunk_tls,
        tls.allow_insecure_dev,
    )
    .context("configure remote RF=2 chunks")?;
    mount_remote_chunks(data_dir, mountpoint, meta_addr, chunks, ufs, tls)
}

fn mount_remote_chunks(
    data_dir: PathBuf,
    mountpoint: PathBuf,
    meta_addr: Option<String>,
    chunks: RemoteReplicatedChunkStore,
    ufs: Option<(Ufs, ReadPathConfig)>,
    tls: TlsClientArgs,
) -> Result<()> {
    let available = chunks
        .available_workers()
        .context("probe remote ChunkWorkers")?;
    if available.len() < 2 {
        bail!("RF=2 requires two ready distinct ChunkWorkers; ready={available:?}");
    }
    // Topology catch-up is paginated + background-scrubbed inside the remote
    // chunk store (B5). Mount must not STW on a full inventory sweep.
    println!(
        "remote RF=2: background repair scrub page={} (non-blocking mount)",
        fluxfs_chunk::REPAIR_PAGE_SIZE
    );
    mount_with_chunks(
        data_dir,
        mountpoint,
        meta_addr,
        chunks,
        "remote RF=2",
        ufs,
        tls,
    )
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn mount_with_chunks<C: ChunkStore + 'static>(
    data_dir: PathBuf,
    mountpoint: PathBuf,
    meta_addr: Option<String>,
    chunks: C,
    chunk_mode: &str,
    ufs: Option<(Ufs, ReadPathConfig)>,
    tls: TlsClientArgs,
) -> Result<()> {
    let mode = if ufs.is_some() {
        "UFS External/Dirty write-back"
    } else {
        "Ephemeral"
    };
    if let Some(addr) = meta_addr {
        let meta_tls = tls.build(None)?;
        let meta = RemoteMetaStore::connect_tls(&addr, meta_tls, tls.allow_insecure_dev)
            .context("connect meta")?;
        // Touch root to fail fast if MetaMaster is down.
        meta.get_inode(ROOT_INODE).context("meta get root")?;
        let client = build_client(meta, chunks, ufs)?;
        reconcile_before_mount(&client)?;
        println!(
            "mounting {mode} FluxFS ({chunk_mode}, remote meta={addr}) data_dir={} mountpoint={}",
            data_dir.display(),
            mountpoint.display()
        );
        mount_with_background_gc(client, &mountpoint)?;
    } else {
        // Co-located mount: embed single-voter Raft so mutations share the
        // production write path (request-id ledger + SM apply), not direct Heed.
        let meta_path = data_dir.join("meta");
        let raft_dir = data_dir.join("raft");
        let store = Arc::new(HeedMetaStore::open(&meta_path).context("open meta")?);
        let rt = tokio::runtime::Runtime::new().context("tokio runtime")?;
        // This fn runs inside `#[tokio::main]`'s multi-thread runtime; calling
        // `rt.block_on(...)` directly would nest runtimes and panic. Use
        // `block_in_place` on the AMBIENT runtime to release its worker while
        // the embedded raft runtime drives `start_single_voter` to completion.
        // The `rt` is then handed off to RaftMetaStore, which keeps it alive
        // for the lifetime of the mount.
        let raft_handle = rt.handle().clone();
        let raft = tokio::task::block_in_place(|| {
            raft_handle.block_on(start_single_voter(store.clone(), &raft_dir, "127.0.0.1:0"))
        })
        .context("start embedded openraft")?;
        let meta = RaftMetaStore::new_owned(store, raft, rt);
        let client = build_client(meta, chunks, ufs)?;
        reconcile_before_mount(&client)?;
        println!(
            "mounting {mode} FluxFS ({chunk_mode}, embedded raft) data_dir={} mountpoint={}",
            data_dir.display(),
            mountpoint.display()
        );
        if client.has_ufs() {
            println!(
                "basic check: ls {0} && cat {0}/<ufs-object>",
                mountpoint.display()
            );
        } else {
            println!(
                "basic check: echo hi > {0}/hi.txt && cat {0}/hi.txt",
                mountpoint.display()
            );
        }
        mount_with_background_gc(client, &mountpoint)?;
    }
    Ok(())
}

/// Small-batch concurrent GC while FUSE serves. Does not take a global Meta lease.
const BACKGROUND_GC_BATCH: usize = 32;
const BACKGROUND_GC_IDLE: Duration = Duration::from_secs(5);
const BACKGROUND_GC_BUSY: Duration = Duration::from_millis(200);

fn sleep_interruptible(stop: &AtomicBool, total: Duration) {
    let slice = Duration::from_millis(100);
    let mut left = total;
    while left > Duration::ZERO && !stop.load(Ordering::Relaxed) {
        let step = slice.min(left);
        std::thread::sleep(step);
        left = left.saturating_sub(step);
    }
}

fn spawn_background_gc<M: MetaStore + 'static, C: ChunkStore + 'static>(
    client: Arc<FluxClient<M, C>>,
) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::clone(&stop);
    let handle = std::thread::Builder::new()
        .name("fluxfs-bg-gc".into())
        .spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match client.run_concurrent_gc_pass(BACKGROUND_GC_BATCH) {
                    Ok(report) if report.removed_chunks > 0 || report.removed_manifests > 0 => {
                        sleep_interruptible(&stop_flag, BACKGROUND_GC_BUSY);
                    }
                    Ok(_) => sleep_interruptible(&stop_flag, BACKGROUND_GC_IDLE),
                    Err(err) => {
                        eprintln!("background GC pass failed: {err}");
                        sleep_interruptible(&stop_flag, BACKGROUND_GC_IDLE);
                    }
                }
            }
        })
        .expect("spawn background GC thread");
    (stop, handle)
}

fn mount_with_background_gc<M: MetaStore + 'static, C: ChunkStore + 'static>(
    client: Arc<FluxClient<M, C>>,
    mountpoint: &PathBuf,
) -> Result<()> {
    // So client GC/flush/repair can increment process-local counters when a
    // co-located mount does not run metamaster/chunkworker metrics endpoints.
    fluxfs_metrics::install_process_metrics(fluxfs_metrics::FluxMetrics::new());
    let (gc_stop, gc_handle) = spawn_background_gc(Arc::clone(&client));
    println!(
        "background GC: batch={BACKGROUND_GC_BATCH} idle={}s (non-blocking)",
        BACKGROUND_GC_IDLE.as_secs()
    );
    let mount_result = fluxfs_fuse::mount_ephemeral(client, mountpoint).context("fuse mount");
    gc_stop.store(true, Ordering::Relaxed);
    let _ = gc_handle.join();
    mount_result
}

fn run_orphan_gc_cmd(
    data_dir: PathBuf,
    meta_addr: Option<String>,
    chunk_workers: Vec<String>,
    chunk_max_pending: usize,
    tls: TlsClientArgs,
) -> Result<()> {
    std::fs::create_dir_all(&data_dir)?;
    if chunk_workers.is_empty() && meta_addr.is_some() {
        let client_tls = tls.build(None)?;
        let meta = RemoteMetaStore::connect_tls(
            meta_addr.as_deref().expect("checked"),
            client_tls.clone(),
            tls.allow_insecure_dev,
        )
        .context("connect MetaMaster for Worker discovery")?;
        let membership = meta.worker_membership().context("get Worker membership")?;
        let chunks = RemoteReplicatedChunkStore::new_with_membership_discovery_tls(
            membership,
            meta_addr.as_deref().expect("checked").to_string(),
            2,
            chunk_max_pending,
            unix_time_millis(),
            client_tls,
            tls.allow_insecure_dev,
        )
        .context("configure membership-discovered RF=2 chunks")?;
        run_orphan_gc_with_chunks(data_dir, meta_addr, chunks, &tls)
    } else if chunk_workers.is_empty() {
        let chunks = ReplicatedChunkStore::open_rf2(
            data_dir.join("chunks/worker-0"),
            data_dir.join("chunks/worker-1"),
        )
        .context("open local RF=2 chunks")?;
        run_orphan_gc_with_chunks(data_dir, meta_addr, chunks, &tls)
    } else {
        if chunk_workers.len() != 3 {
            bail!(
                "multi-process v0 requires exactly three --chunk-worker URLs; got {}",
                chunk_workers.len()
            );
        }
        let chunk_tls = tls.build(None)?;
        let chunks = RemoteReplicatedChunkStore::new_with_max_pending_tls(
            chunk_workers,
            2,
            chunk_max_pending,
            chunk_tls,
            tls.allow_insecure_dev,
        )
        .context("configure remote RF=2 chunks")?;
        run_orphan_gc_with_chunks(data_dir, meta_addr, chunks, &tls)
    }
}

fn run_orphan_gc_with_chunks<C: ChunkStore>(
    data_dir: PathBuf,
    meta_addr: Option<String>,
    chunks: C,
    tls: &TlsClientArgs,
) -> Result<()> {
    if let Some(addr) = meta_addr {
        let meta_tls = tls.build(None)?;
        let meta = RemoteMetaStore::connect_tls(&addr, meta_tls, tls.allow_insecure_dev)
            .context("connect meta")?;
        let client = FluxClient::new(meta, chunks);
        let report = client.run_orphan_gc().context("orphan gc")?;
        println!(
            "orphan-gc ok: manifests={} chunks={}",
            report.removed_manifests, report.removed_chunks
        );
    } else {
        let meta_path = data_dir.join("meta");
        let raft_dir = data_dir.join("raft");
        let store = Arc::new(HeedMetaStore::open(&meta_path).context("open meta")?);
        let rt = tokio::runtime::Runtime::new().context("tokio runtime")?;
        let raft = rt
            .block_on(start_single_voter(store.clone(), &raft_dir, "127.0.0.1:0"))
            .context("start embedded openraft")?;
        let meta = RaftMetaStore::new_owned(store, raft, rt);
        let client = FluxClient::new(meta, chunks);
        let report = client.run_orphan_gc().context("orphan gc")?;
        println!(
            "orphan-gc ok: manifests={} chunks={}",
            report.removed_manifests, report.removed_chunks
        );
    }
    Ok(())
}

fn reconcile_before_mount<M: MetaStore, C: ChunkStore>(client: &FluxClient<M, C>) -> Result<()> {
    // Do not run stop-the-world orphan GC on the mount critical path: it stalls
    // FUSE bring-up and holds a Meta Busy lease across the whole sweep.
    // Release any interrupted lease so writers are not stuck Busy after mount;
    // physical reclaim waits for reservation/tombstone background GC.
    if client
        .release_interrupted_gc_lease()
        .context("release interrupted orphan GC lease")?
    {
        println!("orphan GC: released interrupted lease (deferred background reclaim)");
    }
    if client.has_ufs() {
        let report = client
            .reconcile_flushes()
            .context("reconcile durable flush intents")?;
        if report.completed + report.conflicts + report.pending > 0 {
            println!(
                "flush recovery: completed={} conflicts={} pending={}",
                report.completed, report.conflicts, report.pending
            );
        }
    }
    Ok(())
}

fn build_client<M: MetaStore + 'static, C: ChunkStore + 'static>(
    meta: M,
    chunks: C,
    ufs: Option<(Ufs, ReadPathConfig)>,
) -> Result<Arc<FluxClient<M, C>>> {
    let client = FluxClient::new(meta, chunks);
    let client = if let Some((ufs, read_cfg)) = ufs {
        if read_cfg.disk_capacity_bytes > 0 {
            let dir = read_cfg
                .cache_dir
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<unset>".into());
            println!(
                "ufs clean cache: memory_parts={} disk_bytes={} dir={}",
                read_cfg.max_cached_parts, read_cfg.disk_capacity_bytes, dir
            );
        }
        client
            .with_ufs_config(ufs, read_cfg)
            .context("attach UFS")?
    } else {
        client
    };
    Ok(Arc::new(client))
}

fn run_meta_ping(addr: &str, tls: &TlsClientArgs) -> Result<()> {
    let meta_tls = tls.build(None)?;
    let meta =
        RemoteMetaStore::connect_tls(addr, meta_tls, tls.allow_insecure_dev).context("connect")?;
    let root = meta.get_inode(ROOT_INODE).context("get root")?;
    println!(
        "meta-ping ok addr={addr} root_inode={} locality={:?}",
        root.id, root.locality
    );
    Ok(())
}

async fn run_chunk_probe(addr: &str, mode: &str, tls: &TlsClientArgs) -> Result<()> {
    use fluxfs_proto::ChunkWorkerClient;
    use tonic::transport::Endpoint;

    let chunk_tls = tls.build(None)?;
    let tls_enabled = chunk_tls.is_some();
    let url = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else if tls_enabled {
        format!("https://{addr}")
    } else {
        format!("http://{addr}")
    };
    let gate = fluxfs_tls::InsecureDev::allow(tls.allow_insecure_dev);
    gate.check_endpoint(&url).context("tls endpoint")?;
    gate.check_scheme_matches_tls(&url, tls_enabled)
        .context("tls scheme")?;
    let mut endpoint = Endpoint::from_shared(url.clone()).context("endpoint")?;
    if let Some(opts) = chunk_tls.as_ref() {
        let cfg = opts
            .build_config_blocking()
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        if let Some(cfg) = cfg {
            endpoint = endpoint
                .tls_config(cfg)
                .map_err(|e| anyhow::anyhow!("tls_config: {e}"))?;
        }
    }
    // Connection is lazy; the chosen RPC drives the TLS handshake + Phase 3
    // authz interceptor + per-handler capability evaluation.
    let mut client = ChunkWorkerClient::new(endpoint.connect_lazy());
    match mode {
        "health" => {
            let resp = client
                .health(tonic::Request::new(
                    fluxfs_proto::chunk::v1::HealthRequest::default(),
                ))
                .await
                .context("chunk Health RPC")?;
            let body = resp.into_inner();
            println!(
                "chunk-probe ok mode=health addr={addr} worker_id={} ready={}",
                body.worker_id, body.ready
            );
        }
        "put" => {
            // 1-byte payload whose ChunkId is derived from the data. We do NOT
            // expect this to succeed against a real worker when probing a
            // principal that lacks PutChunk; we surface the RPC error so the
            // caller (test-mtls-acceptance.sh) can assert on the status code.
            let payload = vec![0x42u8; 1];
            let resp = client
                .put_chunk(tonic::Request::new(
                    fluxfs_proto::chunk::v1::PutChunkRequest { data: payload },
                ))
                .await
                .context("chunk PutChunk RPC")?;
            let body = resp.into_inner();
            println!(
                "chunk-probe ok mode=put addr={addr} chunk_id_len={} worker_id={}",
                body.chunk_id.len(),
                body.worker_id
            );
        }
        other => bail!("--mode must be health|put, got {other:?}"),
    }
    Ok(())
}
