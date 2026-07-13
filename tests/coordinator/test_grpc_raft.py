"""Proves the Raft cluster works over real gRPC sockets between separate
process-equivalents (real TCP servers on localhost), not just the
in-process InMemoryTransport everything else in this suite uses. This is
what the raft/tests/harness.Cluster and coordinator/raft_cluster.RaftCluster
correctness already proven still holds once the wire is real gRPC.
"""

from __future__ import annotations

import socket
import time

import pytest

from coordinator.grpc.node_bootstrap import GrpcRaftNode
from raft.state_machine import RecordingStateMachine
from raft.storage import InMemoryStorage


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def grpc_cluster():
    node_ids = ["n1", "n2", "n3"]
    ports = {nid: free_port() for nid in node_ids}
    addresses = {nid: f"127.0.0.1:{port}" for nid, port in ports.items()}
    state_machines = {nid: RecordingStateMachine() for nid in node_ids}

    nodes: dict[str, GrpcRaftNode] = {}
    for nid in node_ids:
        peer_addresses = {other: addr for other, addr in addresses.items() if other != nid}
        nodes[nid] = GrpcRaftNode(nid, ports[nid], peer_addresses, InMemoryStorage(), state_machines[nid])
    for node in nodes.values():
        node.start()

    yield nodes, state_machines

    for node in nodes.values():
        node.stop()


def find_leader(nodes: dict[str, GrpcRaftNode]) -> GrpcRaftNode | None:
    leaders = [n for n in nodes.values() if n.runtime.is_leader]
    return max(leaders, key=lambda n: n.runtime.current_term) if leaders else None


def test_grpc_cluster_elects_exactly_one_leader(grpc_cluster):
    nodes, _ = grpc_cluster
    assert wait_until(lambda: find_leader(nodes) is not None)
    leaders = [n for n in nodes.values() if n.runtime.is_leader]
    assert len(leaders) == 1


def test_grpc_cluster_replicates_and_commits_across_real_sockets(grpc_cluster):
    nodes, state_machines = grpc_cluster
    assert wait_until(lambda: find_leader(nodes) is not None)
    leader = find_leader(nodes)

    entry = leader.runtime.propose("cmd1")
    assert entry is not None

    assert wait_until(lambda: all(sm.applied == ["cmd1"] for sm in state_machines.values()))


def test_grpc_cluster_survives_leader_kill_and_keeps_accepting_proposals(grpc_cluster):
    nodes, state_machines = grpc_cluster
    assert wait_until(lambda: find_leader(nodes) is not None)
    leader = find_leader(nodes)
    old_term = leader.runtime.current_term
    old_leader_id = leader.node_id

    leader.stop()  # real process-equivalent death: thread AND server both stop
    remaining = {nid: n for nid, n in nodes.items() if nid != old_leader_id}

    assert wait_until(
        lambda: any(n.runtime.is_leader and n.runtime.current_term > old_term for n in remaining.values()),
        timeout=5.0,
    )
    new_leader = next(n for n in remaining.values() if n.runtime.is_leader)

    entry = new_leader.runtime.propose("cmd-after-kill")
    assert entry is not None
    assert wait_until(
        lambda: all(state_machines[nid].applied[-1:] == ["cmd-after-kill"] for nid in remaining)
    )
