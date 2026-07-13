"""Wires together a RaftNode + RaftNodeRuntime + GrpcRaftTransport + a real
gRPC server listening on a TCP port -- one complete, independently
runnable Raft node. Used both by tests (multiple nodes on localhost) and
by the Docker entrypoint (docker/raft_node_entrypoint.py); the only
difference between "simulated distributed" and "actually distributed" is
whether peer_addresses points at other localhost ports or other containers.
"""

from __future__ import annotations

from concurrent import futures

import grpc

from coordinator.grpc import raft_pb2_grpc
from coordinator.grpc.raft_service import RaftGrpcServicer
from coordinator.grpc.raft_transport import GrpcRaftTransport
from coordinator.raft_runtime import RaftNodeRuntime
from raft.node import RaftNode
from raft.state_machine import StateMachine
from raft.storage import Storage


class GrpcRaftNode:
    def __init__(
        self,
        node_id: str,
        listen_port: int,
        peer_addresses: dict[str, str],
        storage: Storage,
        state_machine: StateMachine,
        election_timeout_min: float = 0.3,
        election_timeout_max: float = 0.6,
        heartbeat_interval: float = 0.1,
        tick_interval: float = 0.02,
    ):
        self.node_id = node_id
        self.listen_port = listen_port
        self.transport = GrpcRaftTransport(peer_addresses)
        self.node = RaftNode(
            node_id,
            list(peer_addresses),
            storage,
            self.transport,
            state_machine,
            election_timeout_min,
            election_timeout_max,
            heartbeat_interval,
        )
        self.runtime = RaftNodeRuntime(self.node, self.transport, tick_interval)
        self.transport.bind_runtime(self.runtime)

        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftGrpcServicer(self.runtime), self._server)
        self._server.add_insecure_port(f"0.0.0.0:{listen_port}")

    def start(self) -> None:
        self._server.start()
        self.runtime.start()

    def stop(self) -> None:
        self.runtime.stop()
        self._server.stop(grace=1.0).wait()
        self.transport.close()
