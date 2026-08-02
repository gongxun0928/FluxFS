//! Bootstrap a single-voter MetaMaster Raft instance on RocksDB.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::BasicNode;
use openraft::Config;

use crate::raft_log_store::RocksRaftLogStore;
use crate::raft_network::StubNetwork;
use crate::raft_sm::MetaStateMachine;
use crate::raft_types::{FluxRaft, NodeId};
use crate::rocks_store::RocksMetaStore;
use fluxfs_types::{FluxError, Result};

pub const SINGLE_VOTER_ID: NodeId = 1;

/// Start openraft with one voter on the shared RocksDB behind `store`.
pub async fn start_single_voter(store: Arc<RocksMetaStore>, advertise: &str) -> Result<FluxRaft> {
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

    let log_store = RocksRaftLogStore::new(store.db());
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
    use crate::raft_sm::MetaStateMachine;
    use crate::raft_types::{MetaRaftRequest, MetaRaftResponse};
    use crate::store::MetaStore;
    use fluxfs_types::{FileType, ROOT_INODE};
    use tempfile::tempdir;

    #[tokio::test]
    async fn single_voter_create_via_raft() {
        let dir = tempdir().unwrap();
        let store = Arc::new(RocksMetaStore::open(dir.path()).unwrap());
        let raft = start_single_voter(store.clone(), "127.0.0.1:0")
            .await
            .expect("start raft");

        let resp = raft
            .client_write(MetaRaftRequest::Create {
                parent: ROOT_INODE,
                name: "via-raft.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
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
                parent: ROOT_INODE,
                name: "via-raft.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
            })
            .await
            .expect("duplicate reaches state machine");
        assert!(matches!(
            duplicate.data,
            MetaRaftResponse::Err(FluxError::AlreadyExists)
        ));
        raft.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn single_voter_survives_process_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta");

        {
            let store = Arc::new(RocksMetaStore::open(&path).unwrap());
            let raft = start_single_voter(store.clone(), "127.0.0.1:0")
                .await
                .expect("start raft");
            raft.client_write(MetaRaftRequest::Create {
                parent: ROOT_INODE,
                name: "persist.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
            })
            .await
            .expect("create");
            raft.shutdown().await.expect("shutdown");
        }

        let store = Arc::new(RocksMetaStore::open(&path).unwrap());
        let looked = store.lookup(ROOT_INODE, "persist.txt").expect("durable");
        assert_eq!(looked.file_type, FileType::Regular);

        let raft = start_single_voter(store.clone(), "127.0.0.1:0")
            .await
            .expect("restart raft");
        assert!(raft.is_initialized().await.expect("initialized"));

        let resp = raft
            .client_write(MetaRaftRequest::Create {
                parent: ROOT_INODE,
                name: "after-restart.txt".into(),
                file_type: FileType::Regular,
                mode: 0o644,
                uid: 0,
                gid: 0,
            })
            .await
            .expect("write after restart");
        assert!(matches!(resp.data, MetaRaftResponse::Inode(_)));
        store
            .lookup(ROOT_INODE, "after-restart.txt")
            .expect("visible after restart write");
        raft.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn snapshot_roundtrip_restores_inodes() {
        let dir = tempdir().unwrap();
        let store = Arc::new(RocksMetaStore::open(dir.path().join("meta")).unwrap());
        let raft = start_single_voter(store.clone(), "127.0.0.1:0")
            .await
            .expect("start raft");
        raft.client_write(MetaRaftRequest::Create {
            parent: ROOT_INODE,
            name: "snap.txt".into(),
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 0,
            gid: 0,
        })
        .await
        .expect("create");

        let sm = MetaStateMachine::new(store.clone()).expect("sm");
        let snapshot = {
            use openraft::storage::RaftSnapshotBuilder;
            let mut builder = sm.clone();
            builder.build_snapshot().await.expect("build snapshot")
        };
        assert!(snapshot.snapshot.get_ref().len() > 20);

        let empty = Arc::new(RocksMetaStore::open(dir.path().join("meta2")).unwrap());
        let mut sm2 = MetaStateMachine::new(empty.clone()).expect("sm2");
        use openraft::storage::RaftStateMachine;
        sm2.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install");
        empty
            .lookup(ROOT_INODE, "snap.txt")
            .expect("restored from snapshot");
        raft.shutdown().await.expect("shutdown");
    }
}
