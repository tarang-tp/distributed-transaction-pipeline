"""Client-facing handle for a Raft cluster running as separate gRPC
services (e.g. one per Docker container) -- the network counterpart to
coordinator.raft_cluster.RaftCluster's in-process one. Same propose()
contract (raises TimeoutError on failure, otherwise returns a committed-or-
committing LogEntry), so SettlementSubmitter works with either unmodified.

Follows leader redirects the same way a real Raft client has to: try the
last known leader first, fall back to asking every node in turn, and
follow leader_hint when a node says "not me, try this one" (proto/raft.proto's
ProposeReply.leader_hint).
"""

from __future__ import annotations

import pickle
import time

import grpc

from coordinator.grpc import raft_pb2, raft_pb2_grpc
from raft.types import LogEntry


class GrpcRaftClusterClient:
    def __init__(self, addresses: dict[str, str], call_timeout: float = 1.0):
        self._addresses = addresses
        self._call_timeout = call_timeout
        self._stubs: dict[str, raft_pb2_grpc.RaftServiceStub] = {}
        self._last_known_leader: str | None = None

    def _stub_for(self, node_id: str) -> raft_pb2_grpc.RaftServiceStub:
        stub = self._stubs.get(node_id)
        if stub is None:
            channel = grpc.insecure_channel(self._addresses[node_id])
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            self._stubs[node_id] = stub
        return stub

    def propose(self, command, timeout: float = 2.0) -> LogEntry:
        deadline = time.monotonic() + timeout
        ordered = [self._last_known_leader] if self._last_known_leader else []
        ordered += [nid for nid in self._addresses if nid not in ordered]

        while time.monotonic() < deadline:
            for node_id in list(ordered):
                if node_id is None or node_id not in self._addresses:
                    continue
                try:
                    stub = self._stub_for(node_id)
                    reply = stub.Propose(
                        raft_pb2.ProposeRequest(command=pickle.dumps(command)), timeout=self._call_timeout
                    )
                except grpc.RpcError:
                    continue  # this node is unreachable; try the next candidate
                if reply.accepted:
                    self._last_known_leader = node_id
                    return LogEntry(term=reply.term, index=reply.index, command=command)
                if reply.leader_hint and reply.leader_hint in self._addresses:
                    ordered.insert(0, reply.leader_hint)
            time.sleep(0.05)
        raise TimeoutError("no leader accepted the proposal within timeout")
