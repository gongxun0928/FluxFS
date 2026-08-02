//! FUSE mount skeleton.
//!
//! W1 ships the crate + `fuser` dependency; full `Filesystem` impl lands when
//! MetaStore/ChunkStore smoke is green (see docs/mvp-v0.1.md W5).

use fuser::Filesystem;

/// Placeholder FUSE FS — methods default to ENOSYS via fuser defaults until wired.
#[derive(Debug, Default)]
pub struct FluxFuse;

impl Filesystem for FluxFuse {}

pub fn mount_supported() -> bool {
    // Presence of fuser means we can attempt mount on Linux with fusermount.
    cfg!(target_os = "linux")
}
