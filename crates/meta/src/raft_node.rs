//! Bootstrap a single-voter MetaMaster Raft instance.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::BasicNode;
use openraft::Config;

use crate::heed_store::HeedMetaStore;
use crate::raft_log_store::{HeedRaftLogStore, HeedRaftStore};
use crate::raft_network::StubNetwork;
use crate::raft_sm::MetaStateMachine;
use crate::raft_types::{FluxRaft, NodeId};
use fluxfs_types::{FluxError, Result};

/// Default single-voter node id.
pub const SINGLE_VOTER_ID: NodeId = 1;

/// Start openraft with one voter, wait until this node is leader.
///
/// Raft vote/log live under `raft_dir` (heed). SM applied markers and inode
/// state live in [`HeedMetaStore`] (same write txn on normal apply).
pub async fn start_single_voter(
    store: Arc<HeedMetaStore>,
    raft_dir: impl AsRef<Path>,
    advertise: &str,
) -> Result<FluxRaft> {
    let mut config = Config {
        cluster_name: "fluxfs-meta".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Default::default()
    };
    config.enable_tick = true;
    config.enable_heartbeat = true;
    config.enable_elect = true;
    let config = Arc::new(
        config
            .validate()
            .map_err(|e| FluxError::Meta(format!("raft config: {e}")))?,
    );

    let raft_store = Arc::new(HeedRaftStore::open(raft_dir)?);
    let log_store = HeedRaftLogStore::new(raft_store);
    let state_machine =
        MetaStateMachine::new(store).map_err(|e| FluxError::Meta(format!("raft sm: {e}")))?;
    let network = StubNetwork;

    let raft = FluxRaft::new(SINGLE_VOTER_ID, config, network, log_store, state_machine)
        .await
        .map_err(|e| FluxError::Meta(format!("raft new: {e}")))?;

    let initialized = raft
        .is_initialized()
        .await
        .map_err(|e| FluxError::Meta(format!("raft is_initialized: {e}")))?;

    if !initialized {
        let mut members = BTreeMap::new();
        members.insert(SINGLE_VOTER_ID, BasicNode::new(advertise));
        match raft.initialize(members).await {
            Ok(()) => {}
            Err(RaftError::APIError(InitializeError::NotAllowed { .. })) => {}
            Err(RaftError::APIError(InitializeError::NotInMembers { .. })) => {
                return Err(FluxError::Meta(
                    "raft initialize: local node not in members".into(),
                ));
            }
            Err(e) => return Err(FluxError::Meta(format!("raft initialize: {e}"))),
        }
    }

    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(SINGLE_VOTER_ID, "single-voter elect")
        .await
        .map_err(|e| FluxError::Meta(format!("wait leader: {e}")))?;

    Ok(raft)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_types::{MetaRaftRequest, MetaRaftResponse};
    use crate::store::MetaStore;
    use fluxfs_types::{FileType, ROOT_INODE};
    use tempfile::tempdir;

    #[tokio::test]
    async fn single_voter_create_via_raft() {
        let dir = tempdir().unwrap();
        let store = Arc::new(HeedMetaStore::open(dir.path().join("meta")).unwrap());
        let raft = start_single_voter(store.clone(), dir.path().join("raft"), "127.0.0.1:0")
            .await
            .expect("start raft");

        let resp = raft
            .client_write(MetaRaftRequest::Create {
                request_id: None,
                parent: ROOT_INODE,
                name: "via-raft.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
                expected_parent_generation: None,
            })
            .await
            .expect("client_write");

        match resp.data {
            MetaRaftResponse::Inode(inode) => {
                assert_eq!(inode.file_type, FileType::Regular);
                let looked = store.lookup(ROOT_INODE, "via-raft.txt").unwrap();
                assert_eq!(looked.id, inode.id);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        let duplicate = raft
            .client_write(MetaRaftRequest::Create {
                request_id: None,
                parent: ROOT_INODE,
                name: "via-raft.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
                expected_parent_generation: None,
            })
            .await
            .expect("duplicate reaches state machine");
        assert!(matches!(
            duplicate.data,
            MetaRaftResponse::Err(FluxError::AlreadyExists)
        ));
    }

    #[tokio::test]
    async fn single_voter_survives_process_restart() {
        let dir = tempdir().unwrap();
        let meta_path = dir.path().join("meta");
        let raft_path = dir.path().join("raft");

        {
            let store = Arc::new(HeedMetaStore::open(&meta_path).unwrap());
            let raft = start_single_voter(store.clone(), &raft_path, "127.0.0.1:0")
                .await
                .expect("start raft");
            raft.client_write(MetaRaftRequest::Create {
                request_id: None,
                parent: ROOT_INODE,
                name: "persist.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
                expected_parent_generation: None,
            })
            .await
            .expect("create");
            // Explicit shutdown so heed env releases the map.
            raft.shutdown().await.expect("shutdown");
        }

        let store = Arc::new(HeedMetaStore::open(&meta_path).unwrap());
        let looked = store
            .lookup(ROOT_INODE, "persist.txt")
            .expect("heed durable");
        assert_eq!(looked.file_type, FileType::Regular);

        let raft = start_single_voter(store.clone(), &raft_path, "127.0.0.1:0")
            .await
            .expect("restart raft from durable log");
        assert!(raft.is_initialized().await.expect("initialized"));

        let resp = raft
            .client_write(MetaRaftRequest::Create {
                request_id: None,
                parent: ROOT_INODE,
                name: "after-restart.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
                expected_parent_generation: None,
            })
            .await
            .expect("write after restart");
        assert!(matches!(resp.data, MetaRaftResponse::Inode(_)));
        store
            .lookup(ROOT_INODE, "after-restart.txt")
            .expect("visible after restart write");
    }

    #[tokio::test]
    async fn duplicate_request_id_does_not_double_create() {
        use fluxfs_types::RequestOpId;

        let dir = tempdir().unwrap();
        let store = Arc::new(HeedMetaStore::open(dir.path().join("meta")).unwrap());
        let raft = start_single_voter(store.clone(), dir.path().join("raft"), "127.0.0.1:0")
            .await
            .expect("start raft");

        let op_id = RequestOpId::random();
        let req = MetaRaftRequest::Create {
            request_id: Some(op_id),
            parent: ROOT_INODE,
            name: "once.txt".into(),
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
            expected_parent_generation: None,
        };
        let first = raft
            .client_write(req.clone())
            .await
            .expect("first write")
            .data;
        let second = raft.client_write(req).await.expect("retry write").data;
        let MetaRaftResponse::Inode(a) = first else {
            panic!("expected inode");
        };
        let MetaRaftResponse::Inode(b) = second else {
            panic!("expected replay inode");
        };
        assert_eq!(a.id, b.id);
        // Only one dentry — retry must not allocate a second inode name collision.
        let entries = store.readdir(ROOT_INODE).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "once.txt");
        assert_eq!(entries[0].child, a.id);
    }

    #[tokio::test]
    async fn commit_inode_manifest_via_raft_cas() {
        use fluxfs_types::{DataGen, Manifest};

        let dir = tempdir().unwrap();
        let store = Arc::new(HeedMetaStore::open(dir.path().join("meta")).unwrap());
        let raft = start_single_voter(store.clone(), dir.path().join("raft"), "127.0.0.1:0")
            .await
            .expect("start raft");

        let created = match raft
            .client_write(MetaRaftRequest::Create {
                request_id: None,
                parent: ROOT_INODE,
                name: "cas.bin".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
                expected_parent_generation: None,
            })
            .await
            .expect("create")
            .data
        {
            MetaRaftResponse::Inode(inode) => *inode,
            other => panic!("unexpected create: {other:?}"),
        };

        let base_gen = created.generation;
        let mut next = created.clone();
        next.generation = base_gen.saturating_add(1);
        next.size = 8;
        next.head_gen = DataGen(1);
        let manifest = Manifest {
            inode: created.id,
            gen: DataGen(1),
            size: 8,
            extents: fluxfs_types::ExtentTree::default(),
        };
        let committed = match raft
            .client_write(MetaRaftRequest::CommitInodeManifest {
                request_id: None,
                expected_generation: base_gen,
                inode: Box::new(next.clone()),
                manifest: Box::new(manifest.clone()),
            })
            .await
            .expect("commit")
            .data
        {
            MetaRaftResponse::Inode(inode) => *inode,
            other => panic!("unexpected commit: {other:?}"),
        };
        assert_eq!(committed.generation, base_gen + 1);
        assert!(committed.manifest_id.is_some());

        let cas_fail = raft
            .client_write(MetaRaftRequest::CommitInodeManifest {
                request_id: None,
                expected_generation: base_gen,
                inode: Box::new(next),
                manifest: Box::new(manifest),
            })
            .await
            .expect("cas reaches sm");
        match cas_fail.data {
            MetaRaftResponse::Err(FluxError::CasFailed { expected, actual }) => {
                assert_eq!(expected, base_gen);
                assert_eq!(actual, base_gen + 1);
            }
            other => panic!("expected CasFailed, got {other:?}"),
        }
        assert_eq!(
            store.get_inode(created.id).unwrap().generation,
            base_gen + 1
        );
    }

    #[tokio::test]
    async fn snapshot_roundtrip_restores_inodes() {
        let dir = tempdir().unwrap();
        let store = Arc::new(HeedMetaStore::open(dir.path().join("meta")).unwrap());
        let raft = start_single_voter(store.clone(), dir.path().join("raft"), "127.0.0.1:0")
            .await
            .expect("start raft");
        raft.client_write(MetaRaftRequest::Create {
            request_id: None,
            parent: ROOT_INODE,
            name: "snap.txt".into(),
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
            expected_parent_generation: None,
        })
        .await
        .expect("create");

        let sm = MetaStateMachine::new(store.clone()).expect("sm");
        let mut snapshot = {
            use openraft::storage::RaftSnapshotBuilder;
            let mut builder = sm.clone();
            builder.build_snapshot().await.expect("build snapshot")
        };
        use tokio::io::AsyncReadExt;
        let expected = crate::snapshot_stream::SNAPSHOT_MAGIC;
        let mut magic = vec![0u8; expected.len()];
        snapshot
            .snapshot
            .read_exact(&mut magic)
            .await
            .expect("read snapshot magic");
        assert_eq!(magic.as_slice(), expected);
        // Rewind for install_snapshot.
        use std::io::SeekFrom;
        use tokio::io::AsyncSeekExt;
        snapshot
            .snapshot
            .seek(SeekFrom::Start(0))
            .await
            .expect("rewind snapshot");

        // Wipe app state then install snapshot.
        let empty = Arc::new(HeedMetaStore::open(dir.path().join("meta2")).unwrap());
        let mut sm2 = MetaStateMachine::new(empty.clone()).expect("sm2");
        use openraft::storage::RaftStateMachine;
        sm2.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install");
        empty
            .lookup(ROOT_INODE, "snap.txt")
            .expect("restored from snapshot");
        let _ = sm; // keep first sm alive until after build
        raft.shutdown().await.expect("shutdown");
    }
}
