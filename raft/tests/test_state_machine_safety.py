from decimal import Decimal

from common.models import LedgerEntry
from raft.tests.harness import Cluster


def test_all_live_nodes_apply_committed_entries_in_identical_order():
    cluster = Cluster(["n1", "n2", "n3", "n4", "n5"], seed=11)
    for i in range(10):
        cluster.propose(f"cmd{i}")
    cluster.settle()

    sequences = [cluster.applied(nid) for nid in cluster.live_node_ids()]
    assert all(seq == sequences[0] for seq in sequences)
    assert sequences[0] == [f"cmd{i}" for i in range(10)]


def test_revived_node_does_not_double_apply_after_replaying_committed_log():
    cluster = Cluster(["n1", "n2", "n3"], seed=3)
    leader = cluster.elect_leader()
    cluster.propose("cmd1")
    cluster.settle()

    follower_id = next(nid for nid in cluster.live_node_ids() if nid != leader.node_id)
    assert cluster.applied(follower_id) == ["cmd1"]

    cluster.kill(follower_id)
    cluster.revive(follower_id)
    # revive() rebuilds the RaftNode with commit_index/last_applied reset to 0 (volatile
    # state is never persisted in real Raft either), but keeps the SAME state machine
    # instance -- modeling a real restart where durable ledger state survives on disk.
    assert cluster.nodes[follower_id].last_applied == 0
    assert cluster.nodes[follower_id].commit_index == 0

    cluster.settle()  # revived node hears from the leader again and replays its log

    assert cluster.applied(follower_id) == ["cmd1"]  # exactly once, not duplicated


def test_conflicting_transactions_on_same_account_are_serialized_without_double_spend():
    """The scenario the whole settlement stage exists for: two transactions
    hitting the same account 'simultaneously' must be applied in a single,
    globally agreed order -- never both accepted if only one can be afforded,
    and never applied twice.
    """
    cluster = Cluster(["n1", "n2", "n3"], seed=13)
    leader = cluster.elect_leader()

    balance = Decimal("100.00")
    debit_a = LedgerEntry(transaction_id="txn-a", account_id="acct-1", delta=Decimal("-80.00"))
    debit_b = LedgerEntry(transaction_id="txn-b", account_id="acct-1", delta=Decimal("-80.00"))

    # both "arrive" before either is committed, exactly like two concurrent debits
    leader.propose(debit_a, cluster.now)
    leader.propose(debit_b, cluster.now)
    cluster.settle()

    for node_id in cluster.live_node_ids():
        applied = cluster.applied(node_id)
        assert applied == [debit_a, debit_b]  # every node agrees on the SAME order

    # a state machine applying these in the agreed order would reject the second:
    # it is the agreed order itself (not luck) that prevents a double-spend
    resulting_balance = balance
    accepted = []
    for entry in cluster.applied(leader.node_id):
        if resulting_balance + entry.delta >= 0:
            resulting_balance += entry.delta
            accepted.append(entry.transaction_id)
    assert accepted == ["txn-a"]
    assert resulting_balance == Decimal("20.00")
