//! Streaming Meta Raft snapshot wire format.
//!
//! Records are length-prefixed JSON objects written to a seekable file so OpenRaft
//! can chunk-transfer without holding the whole state machine in a `Vec<u8>`.

use std::io::{Read, Seek, SeekFrom, Write};

use fluxfs_types::{
    ChunkReservation, Dentry, FluxError, GcLeaseId, GcTombstone, Inode, Manifest, Result,
    WorkerMembership,
};
use serde::{Deserialize, Serialize};

use crate::raft_types::{MetaRaftResponse, SmAppliedMeta};

/// File magic (includes trailing newline for easy `file(1)` / hexdump checks).
pub const SNAPSHOT_MAGIC: &[u8] = b"fluxfs-meta-snapshot\n";
pub const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFormat {
    StreamingV1,
    /// Pre-B3 monolithic `MetaSnapshotData` JSON blob.
    LegacyJson,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum SnapshotRecord {
    Header {
        next_inode: u64,
        next_manifest: u64,
        sm: SmAppliedMeta,
        gc_lease: Option<GcLeaseId>,
        /// Added compatibly: old readers ignore this unknown header field;
        /// new readers default it when installing pre-B4 snapshots.
        #[serde(default)]
        worker_membership: WorkerMembership,
    },
    Inode(Box<Inode>),
    Dentry(Dentry),
    Manifest {
        id: u64,
        manifest: Box<Manifest>,
    },
    ClientRequest {
        id: String,
        resp: MetaRaftResponse,
        /// Absolute expiry for the dedup ledger (#13). `0` on legacy snapshots.
        #[serde(default)]
        expires_at_unix_ms: u64,
        #[serde(default)]
        created_at_unix_ms: u64,
    },
    Reservation(ChunkReservation),
    DeleteTombstone(GcTombstone),
    End,
}

pub fn write_magic_and_version(w: &mut impl Write) -> Result<()> {
    w.write_all(SNAPSHOT_MAGIC)
        .map_err(|e| FluxError::Io(e.to_string()))?;
    w.write_all(&SNAPSHOT_VERSION.to_le_bytes())
        .map_err(|e| FluxError::Io(e.to_string()))?;
    Ok(())
}

pub fn write_record(w: &mut impl Write, record: &SnapshotRecord) -> Result<()> {
    let bytes = serde_json::to_vec(record).map_err(|e| FluxError::Meta(e.to_string()))?;
    let len = u32::try_from(bytes.len())
        .map_err(|_| FluxError::Meta("snapshot record too large".into()))?;
    w.write_all(&len.to_le_bytes())
        .map_err(|e| FluxError::Io(e.to_string()))?;
    w.write_all(&bytes)
        .map_err(|e| FluxError::Io(e.to_string()))?;
    Ok(())
}

/// Detect format. On [`SnapshotFormat::StreamingV1`] the reader is left after
/// magic+version. On [`SnapshotFormat::LegacyJson`] the reader is rewound to 0.
pub fn detect_format(r: &mut (impl Read + Seek)) -> Result<SnapshotFormat> {
    r.seek(SeekFrom::Start(0))
        .map_err(|e| FluxError::Io(e.to_string()))?;
    let mut magic = vec![0u8; SNAPSHOT_MAGIC.len()];
    match r.read_exact(&mut magic) {
        Ok(()) if magic.as_slice() == SNAPSHOT_MAGIC => {}
        Ok(()) => {
            r.seek(SeekFrom::Start(0))
                .map_err(|e| FluxError::Io(e.to_string()))?;
            return Ok(SnapshotFormat::LegacyJson);
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            r.seek(SeekFrom::Start(0))
                .map_err(|e| FluxError::Io(e.to_string()))?;
            return Ok(SnapshotFormat::LegacyJson);
        }
        Err(e) => return Err(FluxError::Io(e.to_string())),
    }
    let mut ver = [0u8; 4];
    r.read_exact(&mut ver)
        .map_err(|e| FluxError::Io(e.to_string()))?;
    let version = u32::from_le_bytes(ver);
    if version != SNAPSHOT_VERSION {
        return Err(FluxError::Meta(format!(
            "unsupported snapshot version {version}"
        )));
    }
    Ok(SnapshotFormat::StreamingV1)
}

pub fn read_record(r: &mut impl Read) -> Result<Option<SnapshotRecord>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(FluxError::Io(e.to_string())),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut bytes = vec![0u8; len];
    r.read_exact(&mut bytes)
        .map_err(|e| FluxError::Io(e.to_string()))?;
    let record: SnapshotRecord =
        serde_json::from_slice(&bytes).map_err(|e| FluxError::Meta(e.to_string()))?;
    Ok(Some(record))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn round_trip_records() {
        let mut buf = Vec::new();
        write_magic_and_version(&mut buf).unwrap();
        write_record(
            &mut buf,
            &SnapshotRecord::Header {
                next_inode: 3,
                next_manifest: 2,
                sm: SmAppliedMeta::default(),
                gc_lease: None,
                worker_membership: WorkerMembership::default(),
            },
        )
        .unwrap();
        write_record(&mut buf, &SnapshotRecord::End).unwrap();

        assert!(buf.starts_with(SNAPSHOT_MAGIC));
        let mut cur = Cursor::new(buf);
        assert_eq!(
            detect_format(&mut cur).unwrap(),
            SnapshotFormat::StreamingV1
        );
        match read_record(&mut cur).unwrap() {
            Some(SnapshotRecord::Header { next_inode, .. }) => assert_eq!(next_inode, 3),
            other => panic!("unexpected {other:?}"),
        }
        match read_record(&mut cur).unwrap() {
            Some(SnapshotRecord::End) => {}
            other => panic!("unexpected {other:?}"),
        }
        assert!(read_record(&mut cur).unwrap().is_none());
    }

    #[test]
    fn detects_legacy_json() {
        let mut cur = Cursor::new(br#"{"inodes":[]}"#.to_vec());
        assert_eq!(detect_format(&mut cur).unwrap(), SnapshotFormat::LegacyJson);
        assert_eq!(cur.position(), 0);
    }
}
