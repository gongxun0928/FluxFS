use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use fluxfs_chunk::{DiskChunkStore, FoyerChunkStore};
use fluxfs_client::FluxClient;
use fluxfs_meta::HeedMetaStore;
use fluxfs_types::{FileType, ROOT_INODE};
use fluxfs_ufs::Ufs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "fluxfs", about = "FluxFS MVP co-located control binary")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// W1 smoke: meta create/lookup + chunk put/get (+ optional local UFS HEAD).
    Smoke {
        #[arg(long, default_value = "/tmp/fluxfs-smoke")]
        data_dir: PathBuf,
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
            println!("FluxFS MVP W1");
            println!("  repo: https://github.com/gongxun0928/FluxFS");
            println!("  meta: openraft types + heed MetaStore (single-voter Raft next)");
            println!("  chunk: DiskChunkStore + FoyerChunkStore hybrid facade");
            println!("  ufs: OpenDAL (local FS / S3)");
            println!("  client: internal API; FUSE crate skeleton (linux)");
            println!("  fuse_supported: {}", fluxfs_fuse::mount_supported());
            println!("  topology: crate-split Master/Worker, co-located binary");
        }
        Cmd::Smoke { data_dir } => {
            run_smoke(data_dir).await?;
        }
    }
    Ok(())
}

async fn run_smoke(data_dir: PathBuf) -> Result<()> {
    let meta_path = data_dir.join("meta");
    let chunk_path = data_dir.join("chunks");
    let ufs_path = data_dir.join("ufs");
    std::fs::create_dir_all(&ufs_path)?;

    let meta = HeedMetaStore::open(&meta_path).context("open meta")?;
    let chunks = FoyerChunkStore::open(&chunk_path, 1024).context("open chunks")?;
    // Keep disk store path exercised too.
    let _disk = DiskChunkStore::open(data_dir.join("chunks-disk")).context("open disk chunks")?;

    let client = FluxClient::new(meta, chunks);

    let dir = client
        .mkdir(ROOT_INODE, "testdir")
        .context("mkdir testdir")?;
    let file = client
        .create_file(dir.id, "hello.txt")
        .context("create hello.txt")?;
    let looked = client
        .lookup(dir.id, "hello.txt")
        .context("lookup hello.txt")?;
    assert_eq!(looked.id, file.id);
    assert_eq!(looked.file_type, FileType::Regular);

    let cid = client.put_chunk(b"hello fluxfs")?;
    let data = client.get_chunk(&cid)?;
    assert_eq!(data, b"hello fluxfs");

    let ufs = Ufs::local(&ufs_path)?;
    let obj = ufs.write_full("obj.bin", b"ufs-bytes").await?;
    let head = ufs.head("obj.bin").await?;
    assert_eq!(head.size, obj.size);
    assert_eq!(head.size, 9);

    println!("smoke ok");
    println!("  root={}", client.root());
    println!("  file_inode={}", file.id);
    println!("  chunk={}", cid.to_hex());
    println!("  ufs_key={} size={}", head.key, head.size);
    println!("  meta_path={}", meta_path.display());
    Ok(())
}
