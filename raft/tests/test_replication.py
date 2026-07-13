from raft.tests.harness import Cluster


def test_propose_replicates_commits_and_applies_on_all_nodes():
    cluster = Cluster(["n1", "n2", "n3"], seed=1)
    cluster.propose("cmd1")
    cluster.settle()

    leader = cluster.leader()
    assert leader is not None
    assert leader.commit_index == 1
    for node_id in cluster.live_node_ids():
        assert cluster.applied(node_id) == ["cmd1"]


def test_follower_far_behind_catches_up_after_partition_heals():
    cluster = Cluster(["n1", "n2", "n3"], seed=6)
    leader = cluster.elect_leader()
    behind = next(nid for nid in cluster.live_node_ids() if nid != leader.node_id)

    cluster.partition(behind)
    for i in range(5):
        cluster.propose(f"cmd{i}")
        cluster.settle()  # commits via leader + the one remaining connected follower

    cluster.heal()
    cluster.settle()

    expected = [f"cmd{i}" for i in range(5)]
    assert [e.command for e in cluster.nodes[leader.node_id].log] == expected
    assert [e.command for e in cluster.nodes[behind].log] == expected
    assert cluster.applied(behind) == expected


def test_uncommitted_entry_can_be_overwritten_by_a_new_leader():
    """A minority-replicated (uncommitted) entry is not durable -- a new
    leader elected without ever seeing it is allowed to overwrite it on the
    follower that did have it. This is correct Raft behavior (only
    majority-committed entries are guaranteed to survive), not a bug.
    """
    cluster = Cluster(["n1", "n2", "n3", "n4", "n5"], seed=2)
    leader = cluster.elect_leader()
    leader_id = leader.node_id
    others = [nid for nid in cluster.live_node_ids() if nid != leader_id]
    lucky_follower, stale_followers = others[0], others[1:]

    cluster.partition(*stale_followers)  # cut off before the proposal goes out
    cluster.propose("cmd-A")
    cluster.advance(10.0)  # lucky_follower appends cmd-A, but only 2/5 replicas have it

    assert cluster.nodes[leader_id].commit_index == 0
    assert any(e.command == "cmd-A" for e in cluster.nodes[lucky_follower].log)

    cluster.kill(leader_id)
    cluster.heal()
    cluster.partition(lucky_follower)  # force the next election to exclude the only node with cmd-A

    new_leader = cluster.elect_leader()
    assert new_leader.node_id in stale_followers
    assert all(e.command != "cmd-A" for e in new_leader.log)

    cluster.propose("cmd-B")
    cluster.heal()
    cluster.settle()

    assert all(e.command != "cmd-A" for e in cluster.nodes[lucky_follower].log)
    assert any(e.command == "cmd-B" for e in cluster.nodes[lucky_follower].log)
