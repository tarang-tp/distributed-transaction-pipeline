"""Proves GrpcRaftClusterClient -- the handle a coordinator running in its
own container would use to talk to a Raft cluster running as separate
containers -- can find the leader, propose, and follow a redirect after a
leader change, all over real gRPC sockets.
"""

from __future__ import annotations

import socket
import time

import pytest

from coordinator.grpc.node_bootstrap import GrpcRaftNode
from coordinator.grpc.raft_cluster_client import GrpcRaftClusterClient
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

    yield nodes, state_machines, addresses

    for node in nodes.values():
        node.stop()


def test_client_finds_leader_and_proposes(grpc_cluster):
    nodes, state_machines, addresses = grpc_cluster
    assert wait_until(lambda: any(n.runtime.is_leader for n in nodes.values()))

    client = GrpcRaftClusterClient(addresses)
    entry = client.propose("cmd1", timeout=3.0)
    assert entry.command == "cmd1"

    assert wait_until(lambda: all(sm.applied == ["cmd1"] for sm in state_machines.values()))


def test_client_survives_leader_kill_and_finds_the_new_one(grpc_cluster):
    nodes, state_machines, addresses = grpc_cluster
    assert wait_until(lambda: any(n.runtime.is_leader for n in nodes.values()))

    client = GrpcRaftClusterClient(addresses)
    client.propose("cmd1", timeout=3.0)
    assert wait_until(lambda: all(sm.applied == ["cmd1"] for sm in state_machines.values()))

    leader = next(n for n in nodes.values() if n.runtime.is_leader)
    old_term = leader.runtime.current_term
    leader.stop()
    remaining = {nid: n for nid, n in nodes.items() if nid != leader.node_id}

    assert wait_until(
        lambda: any(n.runtime.is_leader and n.runtime.current_term > old_term for n in remaining.values()),
        timeout=5.0,
    )

    entry = client.propose("cmd2", timeout=5.0)  # must discover the new leader itself
    assert entry.command == "cmd2"
    assert wait_until(lambda: all(state_machines[nid].applied == ["cmd1", "cmd2"] for nid in remaining))
