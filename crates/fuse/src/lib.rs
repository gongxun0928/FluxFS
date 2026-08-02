//! Minimal FUSE filesystem for Ephemeral (`--no-ufs`) MVP mounts.

use fluxfs_chunk::ChunkStore;
use fluxfs_client::FluxClient;
use fluxfs_meta::MetaStore;
use fluxfs_types::{FileType as FluxFileType, FluxError, Inode};
use fuser::{
    mount, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, SessionACL, TimeOrNow,
}; // SessionACL used in mount_ephemeral
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

const TTL: Duration = Duration::from_secs(1);

pub struct FluxFs<M: MetaStore + 'static, C: ChunkStore + 'static> {
    client: Arc<FluxClient<M, C>>,
}

impl<M: MetaStore + 'static, C: ChunkStore + 'static> FluxFs<M, C> {
    pub fn new(client: Arc<FluxClient<M, C>>) -> Self {
        Self { client }
    }

    fn attr(&self, ino: &Inode) -> FileAttr {
        let kind = match ino.file_type {
            FluxFileType::Directory => FileType::Directory,
            FluxFileType::Regular => FileType::RegularFile,
        };
        let atime = ms_to_systime(ino.atime_ms);
        let mtime = ms_to_systime(ino.mtime_ms);
        let ctime = ms_to_systime(ino.ctime_ms);
        FileAttr {
            ino: INodeNo(ino.id),
            size: ino.size,
            blocks: ino.size.div_ceil(512),
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind,
            perm: (ino.mode & 0o7777) as u16,
            nlink: ino.link_count,
            uid: ino.uid,
            gid: ino.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

impl<M: MetaStore + 'static, C: ChunkStore + 'static> Filesystem for FluxFs<M, C> {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.client.lookup(parent.0, name) {
            Ok(ino) => reply.entry(&TTL, &self.attr(&ino), Generation(0)),
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.client.get_inode(ino.0) {
            Ok(inode) => reply.attr(&TTL, &self.attr(&inode)),
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        if self.client.has_ufs() {
            reply.error(Errno::EROFS);
            return;
        }
        if let Some(sz) = size {
            if let Err(e) = self.client.truncate(ino.0, sz) {
                reply.error(map_err(e));
                return;
            }
        }
        match self.client.get_inode(ino.0) {
            Ok(mut inode) => {
                if let Some(m) = mode {
                    inode.mode = m;
                }
                if let Some(u) = uid {
                    inode.uid = u;
                }
                if let Some(g) = gid {
                    inode.gid = g;
                }
                if mode.is_some() || uid.is_some() || gid.is_some() {
                    if let Err(e) = self.client.meta.put_inode(&inode) {
                        reply.error(map_err(e));
                        return;
                    }
                }
                reply.attr(&TTL, &self.attr(&inode));
            }
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self
            .client
            .mkdir(parent.0, name, mode, req.uid(), req.gid())
        {
            Ok(ino) => reply.entry(&TTL, &self.attr(&ino), Generation(0)),
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.client.unlink(parent.0, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: fuser::OpenFlags, reply: ReplyOpen) {
        match self.client.get_inode(ino.0) {
            Ok(inode) if inode.file_type == FluxFileType::Regular => {
                reply.opened(FileHandle(0), FopenFlags::empty());
            }
            Ok(_) => reply.error(Errno::EISDIR),
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self
            .client
            .create_file(parent.0, name, mode, req.uid(), req.gid())
        {
            Ok(ino) => {
                reply.created(
                    &TTL,
                    &self.attr(&ino),
                    Generation(0),
                    FileHandle(0),
                    FopenFlags::empty(),
                );
            }
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        match self.client.read_at(ino.0, offset, size) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.client.write_at(ino.0, offset, data) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(map_err(e)),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.client.flush_inode(ino.0) {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.client.readdir(ino.0) {
            Ok(e) => e,
            Err(e) => {
                reply.error(map_err(e));
                return;
            }
        };

        // offset cookie: 1='.', 2='..', then children starting at 3
        if offset < 1 && reply.add(INodeNo(ino.0), 1, FileType::Directory, ".") {
            reply.ok();
            return;
        }
        if offset < 2 && reply.add(INodeNo(ino.0), 2, FileType::Directory, "..") {
            reply.ok();
            return;
        }
        for (i, d) in entries.into_iter().enumerate() {
            let next = (i as u64) + 3;
            if next <= offset {
                continue;
            }
            let child = match self.client.get_inode(d.child) {
                Ok(c) => c,
                Err(e) => {
                    warn!("readdir get_inode {}: {e}", d.child);
                    continue;
                }
            };
            let kind = match child.file_type {
                FluxFileType::Directory => FileType::Directory,
                FluxFileType::Regular => FileType::RegularFile,
            };
            if reply.add(INodeNo(d.child), next, kind, &d.name) {
                break;
            }
        }
        reply.ok();
    }
}

/// Mount FluxFS at `mountpoint` (blocking). Intended for Ephemeral `--no-ufs`.
pub fn mount_ephemeral<M: MetaStore + 'static, C: ChunkStore + 'static>(
    client: Arc<FluxClient<M, C>>,
    mountpoint: impl AsRef<Path>,
) -> std::io::Result<()> {
    let fs = FluxFs::new(client);
    let mut config = Config::default();
    // Avoid AutoUnmount/allow_other — many hosts lack user_allow_other in fuse.conf.
    // Unmount with: fusermount3 -u <mountpoint>
    config.mount_options = vec![MountOption::FSName("fluxfs".into())];
    config.acl = SessionACL::Owner;
    mount(fs, mountpoint.as_ref(), &config)
}

pub fn mount_supported() -> bool {
    cfg!(target_os = "linux")
}

fn map_err(e: FluxError) -> Errno {
    match e {
        FluxError::NotFound => Errno::ENOENT,
        FluxError::AlreadyExists => Errno::EEXIST,
        FluxError::NotDirectory => Errno::ENOTDIR,
        FluxError::IsDirectory => Errno::EISDIR,
        FluxError::Capability(_) => Errno::ENOSPC,
        FluxError::Busy => Errno::EAGAIN,
        FluxError::ReadOnly => Errno::EROFS,
        FluxError::InvalidArg(_) => Errno::EPERM,
        _ => Errno::EIO,
    }
}

fn ms_to_systime(ms: i64) -> SystemTime {
    if ms <= 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_is_exposed_as_retryable_eagain() {
        assert_eq!(map_err(FluxError::Busy), Errno::EAGAIN);
    }
}
