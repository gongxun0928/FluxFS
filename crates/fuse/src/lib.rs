//! Minimal FUSE filesystem for Ephemeral (`--no-ufs`) and UFS-backed MVP mounts.

use fluxfs_chunk::ChunkStore;
use fluxfs_client::{FluxClient, InodeSetAttr};
use fluxfs_meta::MetaStore;
use fluxfs_types::{FileType as FluxFileType, FluxError, Inode, XattrSetMode};
use fuser::{
    mount, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, LockOwner, MountOption, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request, SessionACL,
    TimeOrNow,
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
            FluxFileType::Symlink => FileType::Symlink,
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
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let attrs = InodeSetAttr {
            mode,
            uid,
            gid,
            size,
            atime_ms: atime.map(time_or_now_to_ms),
            mtime_ms: mtime.map(time_or_now_to_ms),
        };
        match self.client.setattr(ino.0, attrs) {
            Ok(inode) => reply.attr(&TTL, &self.attr(&inode)),
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

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.client.rmdir(parent.0, name) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let (Some(link_name), Some(target)) = (link_name.to_str(), target.to_str()) else {
            reply.error(Errno::EINVAL);
            return;
        };
        match self
            .client
            .symlink(parent.0, link_name, target, req.uid(), req.gid())
        {
            Ok(inode) => reply.entry(&TTL, &self.attr(&inode), Generation(0)),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.client.readlink(ino.0) {
            Ok(target) => reply.data(target.as_bytes()),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let supported = RenameFlags::RENAME_NOREPLACE;
        if !(flags - supported).is_empty() {
            reply.error(Errno::EOPNOTSUPP);
            return;
        }
        match self.client.rename(
            parent.0,
            name,
            newparent.0,
            newname,
            flags.contains(RenameFlags::RENAME_NOREPLACE),
        ) {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let Some(newname) = newname.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.client.link(ino.0, newparent.0, newname) {
            Ok(inode) => reply.entry(&TTL, &self.attr(&inode), Generation(0)),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        if position != 0 || flags & !0x3 != 0 || flags == 0x3 {
            reply.error(Errno::EINVAL);
            return;
        }
        let mode = match flags {
            0x1 => XattrSetMode::Create,
            0x2 => XattrSetMode::Replace,
            _ => XattrSetMode::Upsert,
        };
        match self.client.set_xattr(ino.0, name, value, mode) {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        match self.client.get_xattr(ino.0, name) {
            Ok(value) if size == 0 => reply.size(value.len() as u32),
            Ok(value) if value.len() <= size as usize => reply.data(&value),
            Ok(_) => reply.error(Errno::ERANGE),
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        match self.client.list_xattrs(ino.0) {
            Ok(names) => {
                let mut value = Vec::new();
                for name in names {
                    value.extend_from_slice(name.as_bytes());
                    value.push(0);
                }
                if size == 0 {
                    reply.size(value.len() as u32);
                } else if value.len() <= size as usize {
                    reply.data(&value);
                } else {
                    reply.error(Errno::ERANGE);
                }
            }
            Err(error) => reply.error(map_err(error)),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        match self.client.remove_xattr(ino.0, name) {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(map_err(error)),
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

    /// close()/dup paths call flush; do not force UFS publish here.
    /// Authoritative write-back remains [`Filesystem::fsync`].
    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
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
                FluxFileType::Symlink => FileType::Symlink,
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
    // Reads intentionally do not issue metadata writes. Advertise noatime so
    // the kernel-visible contract matches the persisted inode behavior; an
    // explicit utimens/setattr request is still stored normally.
    config.mount_options = vec![MountOption::FSName("fluxfs".into()), MountOption::NoAtime];
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
        FluxError::NotEmpty => Errno::ENOTEMPTY,
        FluxError::NoData => Errno::ENODATA,
        FluxError::NoSpace => Errno::ENOSPC,
        FluxError::NotPermitted => Errno::EPERM,
        FluxError::Capability(_) => Errno::EOPNOTSUPP,
        FluxError::Busy => Errno::EAGAIN,
        FluxError::CasFailed { .. } => Errno::EAGAIN,
        FluxError::ReadOnly => Errno::EROFS,
        FluxError::InvalidArg(_) => Errno::EINVAL,
        FluxError::Unauthenticated(_) | FluxError::Unauthorized(_) => Errno::EACCES,
        _ => Errno::EIO,
    }
}

fn ms_to_systime(ms: i64) -> SystemTime {
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_millis(ms.unsigned_abs()))
            .unwrap_or(UNIX_EPOCH)
    }
}

fn time_or_now_to_ms(t: TimeOrNow) -> i64 {
    match t {
        TimeOrNow::Now => system_time_to_ms(SystemTime::now()),
        TimeOrNow::SpecificTime(st) => system_time_to_ms(st),
    }
}

fn system_time_to_ms(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(error) => {
            let millis = i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX);
            -millis
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_is_exposed_as_retryable_eagain() {
        assert_eq!(map_err(FluxError::Busy), Errno::EAGAIN);
    }

    #[test]
    fn readonly_is_erofs() {
        assert_eq!(map_err(FluxError::ReadOnly), Errno::EROFS);
    }

    #[test]
    fn invalid_and_capability_errors_have_posix_errno() {
        assert_eq!(
            map_err(FluxError::InvalidArg("bad offset".into())),
            Errno::EINVAL
        );
        assert_eq!(
            map_err(FluxError::Capability("unsupported".into())),
            Errno::EOPNOTSUPP
        );
        assert_eq!(map_err(FluxError::NotEmpty), Errno::ENOTEMPTY);
    }

    #[test]
    fn timestamps_round_trip_before_and_after_epoch() {
        for millis in [-1_234_i64, 0, 9_876] {
            assert_eq!(system_time_to_ms(ms_to_systime(millis)), millis);
        }
    }
}
