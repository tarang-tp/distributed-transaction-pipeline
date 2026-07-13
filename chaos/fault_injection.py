"""Thin, named wrappers around the kill/revive primitives Coordinator and
RaftCluster already expose (coordinator/coordinator.py, coordinator/raft_cluster.py).

Kept deliberately small: the actual fault-tolerance behavior lives in
raft/node.py (PreVote + election safety) and streaming/group.py
(consumer-group rebalancing) and is proven in their own test suites. This
module exists so demo scripts read as a narrative ("kill the leader", "kill
a fraud worker") instead of reaching into coordinator internals directly.
"""

from __future__ import annotations

import time

from coordinator.coordinator import Coordinator
from coordinator.raft_runtime import RaftNodeRuntime


def kill_raft_leader(coordinator: Coordinator, timeout: float = 2.0) -> str:
    """Kill whichever Raft node is currently leader. Returns its node_id."""
    leader = coordinator.raft_cluster.find_leader(timeout=timeout)
    if leader is None:
        raise TimeoutError("no leader to kill")
    coordinator.raft_cluster.kill(leader.node_id)
    return leader.node_id


def wait_for_new_leader(coordinator: Coordinator, exclude_term: int, timeout: float = 3.0) -> RaftNodeRuntime:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        candidate = coordinator.raft_cluster.find_leader(timeout=0.2)
        if candidate is not None and candidate.current_term > exclude_term:
            return candidate
    raise TimeoutError("no new leader elected within timeout")


def kill_fraud_worker(coordinator: Coordinator, member_id: str) -> None:
    coordinator.kill_fraud_worker(member_id)


def revive_fraud_worker(coordinator: Coordinator, member_id: str) -> None:
    coordinator.revive_fraud_worker(member_id)
