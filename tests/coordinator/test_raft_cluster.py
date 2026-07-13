import time

import pytest

from coordinator.raft_cluster import RaftCluster
from raft.state_machine import RecordingStateMachine


@pytest.fixture
def cluster():
    c = RaftCluster(["n1", "n2", "n3"], state_machine_factory=RecordingStateMachine)
    c.start()
    yield c
    c.stop()


def test_cluster_elects_a_leader(cluster):
    leader = cluster.find_leader(timeout=2.0)
    assert leader is not None
    assert leader.is_leader


def test_propose_commits_and_applies_across_replicas(cluster):
    entry = cluster.propose("cmd1", timeout=2.0)
    assert entry.command == "cmd1"

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        applied = [sm.applied for nid, sm in cluster.state_machines.items() if cluster.is_alive(nid)]
        if all(a == ["cmd1"] for a in applied):
            return
        time.sleep(0.02)
    pytest.fail("cmd1 was not applied on all replicas within timeout")


def test_kill_leader_new_leader_elected_and_still_accepts_proposals(cluster):
    first_leader = cluster.find_leader(timeout=2.0)
    assert first_leader is not None
    first_leader_id = first_leader.node_id
    first_term = first_leader.current_term

    cluster.kill(first_leader_id)

    deadline = time.monotonic() + 3.0
    new_leader = None
    while time.monotonic() < deadline:
        candidate = cluster.find_leader(timeout=0.5)
        if (
            candidate is not None
            and candidate.node_id != first_leader_id
            and candidate.current_term > first_term
        ):
            new_leader = candidate
            break
    assert new_leader is not None, "no new leader elected after killing the original leader"

    entry = cluster.propose("cmd-after-kill", timeout=2.0)
    assert entry.command == "cmd-after-kill"
