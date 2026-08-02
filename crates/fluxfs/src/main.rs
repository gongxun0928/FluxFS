use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use fluxfs_chunk::{ChunkStore, DiskChunkStore, RemoteReplicatedChunkStore, ReplicatedChunkStore};
use fluxfs_client::FluxClient;
use fluxfs_meta::{MetaStore, RemoteMetaStore, RocksMetaStore};
use fluxfs_types::{FileType, ROOT_INODE};
use fluxfs_ufs::Ufs;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "fluxfs", about = "FluxFS MVP co-located control binary")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Meta create/lookup + chunk put/get + local UFS HEAD smoke.
    Smoke {
        #[arg(long, default_value = "/tmp/fluxfs-smoke")]
        data_dir: PathBuf,
    },
    /// Mount Ephemeral FluxFS (`--no-ufs`) at a local path. Blocking.
    Mount {
        /// Persistent data directory (local meta if --meta-addr unset; always holds chunks).
        #[arg(long, default_value = "/tmp/fluxfs-data")]
        data_dir: PathBuf,
        /// Mount point (must exist and be empty/usable).
        #[arg(long)]
        mountpoint: PathBuf,
        /// Required for v0.1 mount path (UFS-backed mount lands later).
        #[arg(long, default_value_t = true)]
        no_ufs: bool,
        /// Optional remote MetaMaster (`host:port` or `http://host:port`).
        #[arg(long)]
        meta_addr: Option<String>,
        /// Remote ChunkWorker URL. Repeat three times for the v0 multi-process topology.
        /// The first two form the fixed RF=2 replica set; the third is a repair spare.
        #[arg(long = "chunk-worker")]
        chunk_workers: Vec<String>,
    },
    /// Ping a remote MetaMaster (multi-process smoke).
    MetaPing {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
    /// Print stack / topology freeze summary.
    Info,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Info => {
            println!("FluxFS MVP");
            println!("  repo: https://github.com/gongxun0928/FluxFS");
            println!("  meta: RocksDB MetaStore; remote via fluxfs-metamaster (tonic)");
            println!("  chunk: ReplicatedChunkStore RF=2; Worker RPC next");
            println!("  ufs: OpenDAL (local FS / S3)");
            println!("  fuse_supported: {}", fluxfs_fuse::mount_supported());
            println!("  multi-process: fluxfs-metamaster --listen 127.0.0.1:50051 --data-dir DIR");
            println!(
                "  mount: fluxfs mount --no-ufs --data-dir DIR --mountpoint MNT [--meta-addr HOST:PORT]"
            );
        }
        Cmd::Smoke { data_dir } => {
            run_smoke(data_dir).await?;
        }
        Cmd::Mount {
            data_dir,
            mountpoint,
            no_ufs,
            meta_addr,
            chunk_workers,
        } => {
            if !no_ufs {
                bail!("v0.1 only supports --no-ufs (Ephemeral); UFS-backed mount is next");
            }
            run_mount(data_dir, mountpoint, meta_addr, chunk_workers)?;
        }
        Cmd::MetaPing { addr } => {
            run_meta_ping(&addr)?;
        }
    }
    Ok(())
}

async fn run_smoke(data_dir: PathBuf) -> Result<()> {
    let meta_path = data_dir.join("meta");
    let chunk_path = data_dir.join("chunks");
    let ufs_path = data_dir.join("ufs");
    std::fs::create_dir_all(&ufs_path)?;

    let meta = RocksMetaStore::open(&meta_path).context("open meta")?;
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

fn run_mount(
    data_dir: PathBuf,
    mountpoint: PathBuf,
    meta_addr: Option<String>,
    chunk_workers: Vec<String>,
) -> Result<()> {
    if !fluxfs_fuse::mount_supported() {
        bail!("FUSE mount only supported on Linux in this build");
    }
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&mountpoint)?;
    if chunk_workers.is_empty() {
        let chunks = ReplicatedChunkStore::open_rf2(
            data_dir.join("chunks/worker-0"),
            data_dir.join("chunks/worker-1"),
        )
        .context("open local RF=2 chunks")?;
        return mount_with_chunks(data_dir, mountpoint, meta_addr, chunks, "local RF=2");
    }
    if chunk_workers.len() != 3 {
        bail!(
            "multi-process v0 requires exactly three --chunk-worker URLs; got {}",
            chunk_workers.len()
        );
    }
    let chunks = RemoteReplicatedChunkStore::new(chunk_workers, 2)
        .context("configure remote RF=2 chunks")?;
    let available = chunks
        .available_workers()
        .context("probe remote ChunkWorkers")?;
    if available.len() < 2 {
        bail!("RF=2 requires two ready distinct ChunkWorkers; ready={available:?}");
    }
    chunks
        .repair()
        .context("repair remote RF=2 chunks before mount")?;
    mount_with_chunks(data_dir, mountpoint, meta_addr, chunks, "remote RF=2")
}

fn mount_with_chunks<C: ChunkStore + 'static>(
    data_dir: PathBuf,
    mountpoint: PathBuf,
    meta_addr: Option<String>,
    chunks: C,
    chunk_mode: &str,
) -> Result<()> {
    if let Some(addr) = meta_addr {
        let meta = RemoteMetaStore::connect(&addr).context("connect meta")?;
        // Touch root to fail fast if MetaMaster is down.
        meta.get_inode(ROOT_INODE).context("meta get root")?;
        let client = Arc::new(FluxClient::new(meta, chunks));
        println!(
            "mounting Ephemeral FluxFS ({chunk_mode}, remote meta={addr}) data_dir={} mountpoint={}",
            data_dir.display(),
            mountpoint.display()
        );
        fluxfs_fuse::mount_ephemeral(client, &mountpoint).context("fuse mount")?;
    } else {
        let meta = RocksMetaStore::open(data_dir.join("meta")).context("open meta")?;
        let client = Arc::new(FluxClient::new(meta, chunks));
        println!(
            "mounting Ephemeral FluxFS ({chunk_mode}) data_dir={} mountpoint={}",
            data_dir.display(),
            mountpoint.display()
        );
        println!(
            "basic check: echo hi > {0}/hi.txt && cat {0}/hi.txt",
            mountpoint.display()
        );
        fluxfs_fuse::mount_ephemeral(client, &mountpoint).context("fuse mount")?;
    }
    Ok(())
}

fn run_meta_ping(addr: &str) -> Result<()> {
    let meta = RemoteMetaStore::connect(addr).context("connect")?;
    let root = meta.get_inode(ROOT_INODE).context("get root")?;
    println!(
        "meta-ping ok addr={addr} root_inode={} locality={:?}",
        root.id, root.locality
    );
    Ok(())
}
