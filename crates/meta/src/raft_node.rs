//! Bootstrap a single-voter MetaMaster Raft instance.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::BasicNode;
use openraft::Config;

use crate::heed_store::HeedMetaStore;
use crate::raft_log_store::LogStore;
use crate::raft_network::StubNetwork;
use crate::raft_sm::MetaStateMachine;
use crate::raft_types::{FluxRaft, FluxRaftTypeConfig, NodeId};
use fluxfs_types::{FluxError, Result};

/// Default single-voter node id.
pub const SINGLE_VOTER_ID: NodeId = 1;

/// Start openraft with one voter, wait until this node is leader.
///
/// Raft log is in-memory; Heed remains the durable MetaStore. On MetaMaster
/// process restart the Raft log is re-initialized while Heed state is retained.
pub async fn start_single_voter(store: Arc<HeedMetaStore>, advertise: &str) -> Result<FluxRaft> {
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

    let log_store = LogStore::<FluxRaftTypeConfig>::default();
    let state_machine = MetaStateMachine::new(store);
    let network = StubNetwork;

    let raft = FluxRaft::new(SINGLE_VOTER_ID, config, network, log_store, state_machine)
        .await
        .map_err(|e| FluxError::Meta(format!("raft new: {e}")))?;

    let mut members = BTreeMap::new();
    members.insert(SINGLE_VOTER_ID, BasicNode::new(advertise));

    match raft.initialize(members).await {
        Ok(()) => {}
        Err(RaftError::APIError(InitializeError::NotAllowed { .. })) => {
            // Already formed in this process — fine.
        }
        Err(RaftError::APIError(InitializeError::NotInMembers { .. })) => {
            return Err(FluxError::Meta(
                "raft initialize: local node not in members".into(),
            ));
        }
        Err(e) => return Err(FluxError::Meta(format!("raft initialize: {e}"))),
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
        let store = Arc::new(HeedMetaStore::open(dir.path()).unwrap());
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
    }
}
