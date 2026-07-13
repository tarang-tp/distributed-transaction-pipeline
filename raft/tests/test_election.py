import pytest

from raft.state_machine import RecordingStateMachine
from raft.storage import InMemoryStorage
from raft.tests.harness import Cluster
from raft.transport import InMemoryTransport
from raft.types import LogEntry, RequestVoteArgs


def test_single_node_cluster_becomes_leader_immediately():
    cluster = Cluster(["n1"], seed=1)
    leader = cluster.elect_leader()
    assert leader.node_id == "n1"


def test_cluster_of_three_elects_exactly_one_leader():
    cluster = Cluster(["n1", "n2", "n3"], seed=1)
    leader = cluster.elect_leader()
    assert leader is not None
    others = [n for nid, n in cluster.nodes.items() if nid != leader.node_id]
    assert all(not n.is_leader for n in others)
    cluster.assert_election_safety()


def test_minority_partition_cannot_elect_leader():
    cluster = Cluster(["n1", "n2", "n3"], seed=2)
    cluster.partition("n3")
    leader = cluster.elect_leader()
    assert leader.node_id in ("n1", "n2")
    assert cluster.nodes["n3"].is_leader is False


def test_partitioned_leader_steps_down_after_rejoining_to_higher_term():
    cluster = Cluster(["n1", "n2", "n3"], seed=3)
    leader = cluster.elect_leader()
    original_leader_id = leader.node_id

    cluster.partition(original_leader_id)
    # min_term matters here: the partitioned original leader still believes
    # it's LEADER from tick one, so without it elect_leader() would return
    # immediately instead of waiting for the survivors to elect someone new
    new_leader = cluster.elect_leader(dt=10.0, max_steps=200, min_term=leader.current_term)
    assert new_leader.node_id != original_leader_id
    assert new_leader.current_term > cluster.nodes[original_leader_id].current_term

    cluster.heal()
    cluster.settle()

    assert cluster.nodes[original_leader_id].is_leader is False
    assert cluster.nodes[original_leader_id].current_term >= new_leader.current_term
    cluster.assert_election_safety()


def test_vote_denied_for_less_up_to_date_candidate_log():
    storage = InMemoryStorage()
    storage.append([LogEntry(term=1, index=1, command="x"), LogEntry(term=2, index=2, command="y")])
    storage.set_current_term(2)

    from raft.node import RaftNode

    node = RaftNode("A", ["B"], storage, InMemoryTransport(), RecordingStateMachine())
    node.start(0.0)

    # B claims a higher term (3) but a shorter, staler log (last_log_term=1, matching
    # only A's first entry) -- A must adopt the higher term but still refuse the vote.
    reply = node.handle_request_vote(
        RequestVoteArgs(term=3, candidate_id="B", last_log_index=1, last_log_term=1), now=0.0
    )

    assert reply.vote_granted is False
    assert node.current_term == 3  # higher term is always adopted...
    assert storage.get_voted_for() is None  # ...but no vote was cast


def test_identical_election_timeouts_can_livelock_without_randomization():
    # min == max removes randomization entirely: every candidate re-times-out at
    # exactly the same simulated instant forever, so a 3-way split vote never
    # resolves. This demonstrates *why* randomized timeouts are required, not a bug.
    cluster = Cluster(["n1", "n2", "n3"], election_timeout_min=150.0, election_timeout_max=150.0, seed=4)
    with pytest.raises(TimeoutError):
        cluster.elect_leader(dt=10.0, max_steps=20)


def test_randomized_timeouts_recover_from_repeated_split_vote_in_larger_cluster():
    cluster = Cluster(["n1", "n2", "n3", "n4", "n5"], seed=5)
    leader = cluster.elect_leader()
    assert leader is not None
    cluster.assert_election_safety()
