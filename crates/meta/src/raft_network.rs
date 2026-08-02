//! Single-voter network stub: never talks to remote peers.

use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;

use crate::raft_types::{FluxRaftTypeConfig, NodeId};

#[derive(Clone, Debug, Default)]
pub struct StubNetwork;

#[derive(Clone, Debug)]
pub struct StubNetworkConnection {
    target: NodeId,
}

fn unreachable<E>(target: NodeId) -> RPCError<NodeId, BasicNode, E>
where
    E: std::error::Error,
{
    let err = std::io::Error::other(format!("single-voter MetaMaster: no remote peer {target}"));
    RPCError::Unreachable(Unreachable::new(&err))
}

impl RaftNetworkFactory<FluxRaftTypeConfig> for StubNetwork {
    type Network = StubNetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        StubNetworkConnection { target }
    }
}

impl RaftNetwork<FluxRaftTypeConfig> for StubNetworkConnection {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<FluxRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable(self.target))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<FluxRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(unreachable(self.target))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable(self.target))
    }
}
