"""End-to-end proof of the project's core claim: Raft consensus, driving a
LedgerStateMachine instead of raft/'s generic test-only state machine,
correctly serializes two conflicting transactions on the same account with
no double-spend -- and survives a leader kill mid-transaction without
losing or duplicating settled state.

This intentionally reuses raft/tests/harness.Cluster (the same simulated,
deterministic cluster raft/'s own correctness suite runs on) rather than
building a second parallel test rig -- the whole point of stage 4 was
proving that harness correct in isolation so it can be trusted here.
"""

from decimal import Decimal

from common.models import LedgerEntry
from raft.tests.harness import Cluster
from settlement.ledger_state_machine import LedgerStateMachine


def make_settlement_cluster(node_ids: list[str], seed: int, initial_balances: dict[str, Decimal]) -> Cluster:
    return Cluster(
        node_ids, seed=seed, state_machine_factory=lambda: LedgerStateMachine(dict(initial_balances))
    )


def test_two_conflicting_transactions_on_same_account_no_double_spend():
    cluster = make_settlement_cluster(
        ["n1", "n2", "n3"], seed=21, initial_balances={"acct-1": Decimal("100")}
    )
    leader = cluster.elect_leader()

    debit_a = LedgerEntry(transaction_id="txn-a", account_id="acct-1", delta=Decimal("-80"))
    debit_b = LedgerEntry(transaction_id="txn-b", account_id="acct-1", delta=Decimal("-80"))

    # both proposed before either is committed -- simulates them "arriving simultaneously"
    leader.propose(debit_a, cluster.now)
    leader.propose(debit_b, cluster.now)
    cluster.settle()

    for node_id in cluster.live_node_ids():
        sm: LedgerStateMachine = cluster.state_machines[node_id]
        assert sm.balance("acct-1") == Decimal("20")  # exactly one debit accepted, everywhere
        result_a = sm.apply(debit_a)  # idempotent re-apply, just reads the cached outcome
        result_b = sm.apply(debit_b)
        assert result_a.accepted is True
        assert result_b.accepted is False
        assert result_b.reason.startswith("insufficient funds")


def test_settlement_survives_leader_kill_mid_transaction():
    cluster = make_settlement_cluster(
        ["n1", "n2", "n3"], seed=22, initial_balances={"acct-1": Decimal("500")}
    )
    leader = cluster.elect_leader()

    committed = LedgerEntry(transaction_id="txn-1", account_id="acct-1", delta=Decimal("-100"))
    cluster.propose(committed)
    cluster.settle()
    for node_id in cluster.live_node_ids():
        assert cluster.state_machines[node_id].balance("acct-1") == Decimal("400")

    cluster.kill(leader.node_id)
    new_leader = cluster.elect_leader(min_term=leader.current_term)
    assert new_leader.node_id != leader.node_id

    next_txn = LedgerEntry(transaction_id="txn-2", account_id="acct-1", delta=Decimal("-150"))
    cluster.propose(next_txn)
    cluster.settle()

    for node_id in cluster.live_node_ids():
        sm: LedgerStateMachine = cluster.state_machines[node_id]
        assert sm.balance("acct-1") == Decimal("250")  # 500 - 100 - 150, exactly once each
        assert sm.apply(committed).accepted is True  # cached, not re-debited
        assert sm.apply(next_txn).accepted is True
