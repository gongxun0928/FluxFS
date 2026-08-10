use super::TlsClientArgs;
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use fluxfs_chunk::{RemoteReplicatedChunkStore, DEFAULT_MAX_PENDING_CHUNK_OPS};
use fluxfs_client::{FluxClient, InodeSetAttr};
use fluxfs_meta::{MetaStore, RemoteMetaStore};
use fluxfs_types::{
    FileType, FluxError, Inode, RequestOpId, WorkerRegistration, XattrSetMode, CHUNK_SIZE,
    ROOT_INODE,
};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

type RemoteClient = FluxClient<RemoteMetaStore, RemoteReplicatedChunkStore>;

#[derive(Args, Debug)]
pub(crate) struct FsArgs {
    /// MetaMaster address (`host:port` or URL).
    #[arg(long, default_value = "127.0.0.1:50051")]
    meta_addr: String,
    /// Maximum chunk operations waiting in the remote client queue.
    #[arg(long, default_value_t = DEFAULT_MAX_PENDING_CHUNK_OPS)]
    chunk_max_pending: usize,
    #[command(flatten)]
    tls: TlsClientArgs,
    #[command(subcommand)]
    command: FsCommand,
}

#[derive(Subcommand, Debug)]
enum FsCommand {
    /// List a directory or print one file entry.
    Ls {
        #[arg(default_value = "/")]
        path: String,
    },
    /// Print detailed inode metadata.
    Stat { path: String },
    /// Print metadata without following the final symbolic link.
    Lstat { path: String },
    /// Print a symbolic link target.
    Readlink { path: String },
    /// Stream a file to stdout.
    Cat { path: String },
    /// Stream a FluxFS file to a local file.
    Get { source: String, local: PathBuf },
    /// Stream a local file into FluxFS.
    Put {
        local: PathBuf,
        destination: String,
        #[arg(long, short = 'f')]
        overwrite: bool,
        #[arg(long, default_value = "0644", value_parser = parse_mode)]
        mode: u32,
    },
    /// Create one directory.
    Mkdir {
        path: String,
        #[arg(long, default_value = "0755", value_parser = parse_mode)]
        mode: u32,
    },
    /// Remove one regular file. Imported/UFS namespace entries fail closed.
    Rm { path: String },
    /// Create a hard link, or a symbolic link with `--symbolic`.
    Ln {
        source: String,
        destination: String,
        #[arg(long, short = 's')]
        symbolic: bool,
    },
    /// Remove one empty directory.
    Rmdir { path: String },
    /// Atomically rename or move one path.
    Mv {
        source: String,
        destination: String,
        #[arg(long)]
        no_replace: bool,
    },
    /// Set POSIX permission bits (octal, e.g. 0640).
    Chmod {
        #[arg(value_parser = parse_mode)]
        mode: u32,
        path: String,
    },
    /// Set numeric owner as UID or UID:GID.
    Chown { owner: String, path: String },
    /// Update atime/mtime to now, creating an empty file when absent.
    Touch {
        path: String,
        #[arg(long, default_value = "0644", value_parser = parse_mode)]
        mode: u32,
    },
    /// Set the logical file size (sparse growth is supported).
    Truncate { size: u64, path: String },
    /// Read an extended attribute (raw bytes to stdout or `--output`).
    Getxattr {
        path: String,
        name: String,
        #[arg(long, short = 'o')]
        output: Option<PathBuf>,
        #[arg(long)]
        no_follow: bool,
    },
    /// List extended attribute names.
    Listxattr {
        path: String,
        #[arg(long)]
        no_follow: bool,
    },
    /// Set an extended attribute from a UTF-8 value or `--value-file`.
    Setxattr {
        path: String,
        name: String,
        value: Option<String>,
        #[arg(long)]
        value_file: Option<PathBuf>,
        #[arg(long)]
        create: bool,
        #[arg(long)]
        replace: bool,
        #[arg(long)]
        no_follow: bool,
    },
    /// Remove an extended attribute.
    Removexattr {
        path: String,
        name: String,
        #[arg(long)]
        no_follow: bool,
    },
}

#[derive(Args, Debug)]
pub(crate) struct AdminArgs {
    /// MetaMaster address (`host:port` or URL).
    #[arg(long, default_value = "127.0.0.1:50051")]
    meta_addr: String,
    #[command(flatten)]
    tls: TlsClientArgs,
    #[command(subcommand)]
    command: AdminCommand,
}

#[derive(Subcommand, Debug)]
enum AdminCommand {
    /// Print a compact Meta/Worker health summary.
    Status,
    /// List registered Worker identities, endpoints, leases, and capacity.
    Workers,
}

impl FsArgs {
    pub(crate) fn run(self) -> Result<()> {
        let client = connect_fs_client(&self.meta_addr, self.chunk_max_pending, &self.tls)?;
        match self.command {
            FsCommand::Ls { path } => ls(&client, &path),
            FsCommand::Stat { path } => {
                print_inode(&path, &client.lookup_path(&path)?);
                Ok(())
            }
            FsCommand::Lstat { path } => {
                print_inode(&path, &client.lstat_path(&path)?);
                Ok(())
            }
            FsCommand::Readlink { path } => {
                println!("{}", client.readlink_path(&path)?);
                Ok(())
            }
            FsCommand::Cat { path } => {
                let inode = client.lookup_path(&path)?;
                let stdout = io::stdout();
                let mut output = stdout.lock();
                client.read_to_writer(inode.id, &mut output)?;
                output.flush().context("flush stdout")
            }
            FsCommand::Get { source, local } => {
                let inode = client.lookup_path(&source)?;
                let file = File::create(&local)
                    .with_context(|| format!("create local file {}", local.display()))?;
                let mut output = BufWriter::new(file);
                let bytes = client.read_to_writer(inode.id, &mut output)?;
                output.flush().context("flush local file")?;
                println!("get ok: {source} -> {} bytes={bytes}", local.display());
                Ok(())
            }
            FsCommand::Put {
                local,
                destination,
                overwrite,
                mode,
            } => {
                let bytes = put_atomic(&client, &local, &destination, overwrite, mode)?;
                println!("put ok: {} -> {destination} bytes={bytes}", local.display());
                Ok(())
            }
            FsCommand::Mkdir { path, mode } => {
                let inode = client.mkdir_path(&path, mode, current_uid(), current_gid())?;
                println!("mkdir ok: {path} inode={}", inode.id);
                Ok(())
            }
            FsCommand::Rm { path } => {
                client.unlink_path(&path)?;
                println!("rm ok: {path}");
                Ok(())
            }
            FsCommand::Ln {
                source,
                destination,
                symbolic,
            } => {
                let inode = if symbolic {
                    client.symlink_path(&source, &destination, current_uid(), current_gid())?
                } else {
                    client.link_path(&source, &destination)?
                };
                println!(
                    "ln ok: {source} -> {destination} inode={} symbolic={symbolic}",
                    inode.id
                );
                Ok(())
            }
            FsCommand::Rmdir { path } => {
                client.rmdir_path(&path)?;
                println!("rmdir ok: {path}");
                Ok(())
            }
            FsCommand::Mv {
                source,
                destination,
                no_replace,
            } => {
                let inode = client.rename_path(&source, &destination, no_replace)?;
                println!("mv ok: {source} -> {destination} inode={}", inode.id);
                Ok(())
            }
            FsCommand::Chmod { mode, path } => {
                let inode = client.setattr_path(
                    &path,
                    InodeSetAttr {
                        mode: Some(mode),
                        ..InodeSetAttr::default()
                    },
                )?;
                println!("chmod ok: {path} mode={:04o}", inode.mode);
                Ok(())
            }
            FsCommand::Chown { owner, path } => {
                let (uid, gid) = parse_owner(&owner)?;
                let inode = client.setattr_path(
                    &path,
                    InodeSetAttr {
                        uid: Some(uid),
                        gid,
                        ..InodeSetAttr::default()
                    },
                )?;
                println!("chown ok: {path} uid={} gid={}", inode.uid, inode.gid);
                Ok(())
            }
            FsCommand::Touch { path, mode } => {
                let inode = match client.lookup_path(&path) {
                    Ok(inode) => inode,
                    Err(FluxError::NotFound) => {
                        client.create_file_path(&path, mode, current_uid(), current_gid())?
                    }
                    Err(error) => return Err(error.into()),
                };
                let now = now_ms();
                let inode = client.setattr(
                    inode.id,
                    InodeSetAttr {
                        atime_ms: Some(now),
                        mtime_ms: Some(now),
                        ..InodeSetAttr::default()
                    },
                )?;
                println!("touch ok: {path} mtime_ms={}", inode.mtime_ms);
                Ok(())
            }
            FsCommand::Truncate { size, path } => {
                let inode = client.setattr_path(
                    &path,
                    InodeSetAttr {
                        size: Some(size),
                        ..InodeSetAttr::default()
                    },
                )?;
                println!("truncate ok: {path} size={}", inode.size);
                Ok(())
            }
            FsCommand::Getxattr {
                path,
                name,
                output,
                no_follow,
            } => {
                let value = if no_follow {
                    client.lget_xattr_path(&path, &name)?
                } else {
                    client.get_xattr_path(&path, &name)?
                };
                if let Some(output) = output {
                    std::fs::write(&output, &value)
                        .with_context(|| format!("write {}", output.display()))?;
                } else {
                    let stdout = io::stdout();
                    let mut stdout = stdout.lock();
                    stdout.write_all(&value).context("write xattr to stdout")?;
                    stdout.flush().context("flush stdout")?;
                }
                Ok(())
            }
            FsCommand::Listxattr { path, no_follow } => {
                let names = if no_follow {
                    client.llist_xattrs_path(&path)?
                } else {
                    client.list_xattrs_path(&path)?
                };
                for name in names {
                    println!("{name}");
                }
                Ok(())
            }
            FsCommand::Setxattr {
                path,
                name,
                value,
                value_file,
                create,
                replace,
                no_follow,
            } => {
                if create && replace {
                    anyhow::bail!("--create and --replace are mutually exclusive");
                }
                let value = match (value, value_file) {
                    (Some(value), None) => value.into_bytes(),
                    (None, Some(path)) => {
                        std::fs::read(&path).with_context(|| format!("read {}", path.display()))?
                    }
                    (Some(_), Some(_)) => {
                        anyhow::bail!("provide either VALUE or --value-file, not both")
                    }
                    (None, None) => anyhow::bail!("provide VALUE or --value-file"),
                };
                let mode = if create {
                    XattrSetMode::Create
                } else if replace {
                    XattrSetMode::Replace
                } else {
                    XattrSetMode::Upsert
                };
                if no_follow {
                    client.lset_xattr_path(&path, &name, &value, mode)?;
                } else {
                    client.set_xattr_path(&path, &name, &value, mode)?;
                }
                println!("setxattr ok: {path} {name} bytes={}", value.len());
                Ok(())
            }
            FsCommand::Removexattr {
                path,
                name,
                no_follow,
            } => {
                if no_follow {
                    client.lremove_xattr_path(&path, &name)?;
                } else {
                    client.remove_xattr_path(&path, &name)?;
                }
                println!("removexattr ok: {path} {name}");
                Ok(())
            }
        }
    }
}

impl AdminArgs {
    pub(crate) fn run(self) -> Result<()> {
        let meta = connect_meta(&self.meta_addr, &self.tls)?;
        let membership = meta.worker_membership().context("read Worker membership")?;
        match self.command {
            AdminCommand::Status => {
                let root = meta.get_inode(ROOT_INODE).context("read root inode")?;
                let now = now_ms_u64();
                let live = membership
                    .workers
                    .iter()
                    .filter(|registration| registration.lease_deadline_ms > now)
                    .count();
                let available = membership
                    .workers
                    .iter()
                    .filter(|registration| registration.lease_deadline_ms > now)
                    .map(|registration| registration.available_bytes)
                    .sum::<u64>();
                let state = if live >= 2 { "ok" } else { "degraded" };
                println!(
                    "cluster {state}: root_generation={} membership_epoch={} workers_live={}/{} available_bytes={available}",
                    root.generation,
                    membership.epoch,
                    live,
                    membership.workers.len(),
                );
                Ok(())
            }
            AdminCommand::Workers => {
                let now = now_ms_u64();
                let mut workers = membership.workers.iter().collect::<Vec<_>>();
                workers.sort_by_key(|registration| registration.id.0);
                println!("ID\tSTATE\tENDPOINT\tFAILURE_DOMAIN\tAVAILABLE\tLEASE_EXPIRES_MS");
                for registration in workers {
                    print_worker(registration, now);
                }
                Ok(())
            }
        }
    }
}

fn put_atomic(
    client: &RemoteClient,
    local: &PathBuf,
    destination: &str,
    overwrite: bool,
    mode: u32,
) -> Result<u64> {
    match client.lookup_path(destination) {
        Ok(_) if !overwrite => return Err(FluxError::AlreadyExists.into()),
        Ok(inode) if inode.file_type != FileType::Regular => {
            return Err(FluxError::IsDirectory.into());
        }
        Ok(_) | Err(FluxError::NotFound) => {}
        Err(error) => return Err(error.into()),
    }

    let file = File::open(local).with_context(|| format!("open local file {}", local.display()))?;
    let (parent, destination_name) = client.resolve_parent(destination)?;
    let temporary_name = format!(".fluxfs-put-{}", RequestOpId::random().to_hex());
    let temporary = client.create_file(
        parent.id,
        &temporary_name,
        mode,
        current_uid(),
        current_gid(),
    )?;

    let result: std::result::Result<u64, FluxError> = (|| {
        let mut input = BufReader::with_capacity(CHUNK_SIZE as usize, file);
        let bytes = client.write_from_reader(temporary.id, &mut input)?;
        client.rename(
            parent.id,
            &temporary_name,
            parent.id,
            &destination_name,
            !overwrite,
        )?;
        Ok(bytes)
    })();
    if result.is_err() {
        let _ = client.unlink(parent.id, &temporary_name);
    }
    result.map_err(Into::into)
}

fn connect_meta(addr: &str, tls: &TlsClientArgs) -> Result<RemoteMetaStore> {
    RemoteMetaStore::connect_tls(addr, tls.build(None)?, tls.allow_insecure_dev)
        .with_context(|| format!("connect MetaMaster {addr}"))
}

fn connect_fs_client(
    addr: &str,
    chunk_max_pending: usize,
    tls: &TlsClientArgs,
) -> Result<RemoteClient> {
    let client_tls = tls.build(None)?;
    let meta = RemoteMetaStore::connect_tls(addr, client_tls.clone(), tls.allow_insecure_dev)
        .with_context(|| format!("connect MetaMaster {addr}"))?;
    let membership = meta.worker_membership().context("discover ChunkWorkers")?;
    let chunks = RemoteReplicatedChunkStore::new_with_membership_discovery_tls(
        membership,
        addr.to_string(),
        2,
        chunk_max_pending,
        now_ms_u64(),
        client_tls,
        tls.allow_insecure_dev,
    )
    .context("configure membership-discovered RF=2 chunks")?;
    Ok(FluxClient::new(meta, chunks))
}

fn ls(client: &RemoteClient, path: &str) -> Result<()> {
    let inode = client.lookup_path(path)?;
    if inode.file_type == FileType::Regular {
        print_inode(path, &inode);
        return Ok(());
    }
    let mut entries = client
        .readdir(inode.id)?
        .into_iter()
        .map(|dentry| {
            let child = client.get_inode(dentry.child)?;
            Ok((dentry.name, child))
        })
        .collect::<std::result::Result<Vec<_>, FluxError>>()?;
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, child) in entries {
        println!(
            "{} {:04o} {:>10} {:>6}:{:<6} {}",
            file_type_char(child.file_type),
            child.mode & 0o7777,
            child.size,
            child.uid,
            child.gid,
            name,
        );
    }
    Ok(())
}

fn print_inode(path: &str, inode: &Inode) {
    println!("path={path}");
    println!("inode={}", inode.id);
    println!("type={:?}", inode.file_type);
    println!("mode={:04o}", inode.mode & 0o7777);
    println!("uid={}", inode.uid);
    println!("gid={}", inode.gid);
    println!("size={}", inode.size);
    println!("generation={}", inode.generation);
    println!("locality={:?}", inode.locality);
    println!("atime_ms={}", inode.atime_ms);
    println!("mtime_ms={}", inode.mtime_ms);
    println!("ctime_ms={}", inode.ctime_ms);
}

fn print_worker(registration: &WorkerRegistration, now: u64) {
    let state = if registration.lease_deadline_ms > now {
        "live"
    } else {
        "expired"
    };
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        registration.id.0,
        state,
        registration.endpoint,
        registration.failure_domain,
        registration.available_bytes,
        registration.lease_deadline_ms,
    );
}

fn file_type_char(file_type: FileType) -> char {
    match file_type {
        FileType::Directory => 'd',
        FileType::Regular => '-',
        FileType::Symlink => 'l',
    }
}

fn parse_mode(value: &str) -> std::result::Result<u32, String> {
    let value = value.strip_prefix("0o").unwrap_or(value);
    u32::from_str_radix(value, 8)
        .map(|mode| mode & 0o7777)
        .map_err(|error| format!("invalid octal mode {value:?}: {error}"))
}

fn parse_owner(value: &str) -> Result<(u32, Option<u32>)> {
    let (uid, gid) = match value.split_once(':') {
        Some((uid, gid)) => (uid, Some(gid)),
        None => (value, None),
    };
    let uid = uid
        .parse::<u32>()
        .with_context(|| format!("invalid numeric uid {uid:?}"))?;
    let gid = gid
        .map(|gid| {
            gid.parse::<u32>()
                .with_context(|| format!("invalid numeric gid {gid:?}"))
        })
        .transpose()?;
    Ok((uid, gid))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn now_ms_u64() -> u64 {
    now_ms().max(0) as u64
}

fn current_uid() -> u32 {
    // UID mapping from authenticated CLI principals is not implemented yet.
    // Match the existing bootstrap CLI/FUSE smoke convention explicitly.
    0
}

fn current_gid() -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_modes_and_numeric_owner() {
        assert_eq!(parse_mode("0640").unwrap(), 0o640);
        assert_eq!(parse_mode("0o4755").unwrap(), 0o4755);
        assert!(parse_mode("999").is_err());
        assert_eq!(parse_owner("1000").unwrap(), (1000, None));
        assert_eq!(parse_owner("1000:1001").unwrap(), (1000, Some(1001)));
        assert!(parse_owner("alice").is_err());
    }
}
