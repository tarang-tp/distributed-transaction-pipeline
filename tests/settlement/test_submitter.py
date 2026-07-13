import queue
import time
from decimal import Decimal

import pytest

from common.models import FraudScore, ScoredTransaction, Transaction, TransactionType
from coordinator.raft_cluster import RaftCluster
from settlement.ledger_state_machine import LedgerStateMachine
from settlement.submitter import SettlementSubmitter


def make_scored(
    account_id: str, amount: str, is_fraud: bool, txn_type=TransactionType.DEBIT
) -> ScoredTransaction:
    txn = Transaction(account_id=account_id, transaction_type=txn_type, amount=Decimal(amount))
    score = FraudScore(
        transaction_id=txn.transaction_id, worker_id="w1", score=1.0 if is_fraud else 0.0, is_fraud=is_fraud
    )
    return ScoredTransaction(transaction=txn, score=score)


@pytest.fixture
def raft_cluster():
    c = RaftCluster(
        ["n1", "n2", "n3"], state_machine_factory=lambda: LedgerStateMachine({"acct-1": Decimal("1000")})
    )
    c.start()
    yield c
    c.stop()


def wait_until(predicate, timeout=2.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_cleared_transaction_is_settled_on_ledger(raft_cluster):
    input_queue: "queue.Queue[ScoredTransaction]" = queue.Queue()
    submitter = SettlementSubmitter(input_queue, raft_cluster)
    submitter.start()
    try:
        scored = make_scored("acct-1", "100", is_fraud=False)
        input_queue.put(scored)

        assert wait_until(lambda: submitter.submitted_count == 1)
        leader_sm = raft_cluster.state_machines[raft_cluster.find_leader().node_id]
        assert wait_until(lambda: leader_sm.balance("acct-1") == Decimal("900"))
    finally:
        submitter.stop()


def test_fraud_flagged_transaction_is_never_settled(raft_cluster):
    input_queue: "queue.Queue[ScoredTransaction]" = queue.Queue()
    submitter = SettlementSubmitter(input_queue, raft_cluster)
    submitter.start()
    try:
        scored = make_scored("acct-1", "500", is_fraud=True)
        input_queue.put(scored)

        assert wait_until(lambda: submitter.fraud_flagged_count == 1)
        time.sleep(0.2)  # give it a moment to (incorrectly) settle if there's a bug
        assert submitter.submitted_count == 0
        leader_sm = raft_cluster.state_machines[raft_cluster.find_leader().node_id]
        assert leader_sm.balance("acct-1") == Decimal("1000")
    finally:
        submitter.stop()


def test_conflicting_debits_through_the_real_submitter_no_double_spend(raft_cluster):
    input_queue: "queue.Queue[ScoredTransaction]" = queue.Queue()
    submitter = SettlementSubmitter(input_queue, raft_cluster)
    submitter.start()
    try:
        debit_a = make_scored("acct-1", "800", is_fraud=False)
        debit_b = make_scored("acct-1", "800", is_fraud=False)
        input_queue.put(debit_a)
        input_queue.put(debit_b)

        assert wait_until(lambda: submitter.submitted_count + len(submitter.failed_transaction_ids) == 2)

        for node_id in raft_cluster.runtimes:
            if raft_cluster.is_alive(node_id):
                sm = raft_cluster.state_machines[node_id]
                assert wait_until(lambda sm=sm: sm.balance("acct-1") == Decimal("200"))
    finally:
        submitter.stop()
