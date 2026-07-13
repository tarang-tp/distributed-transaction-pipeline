import time
from decimal import Decimal

import pytest

from common.models import Transaction, TransactionType
from coordinator.coordinator import Coordinator
from fraud.scorer import ScorerConfig


def wait_until(predicate, timeout=3.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def make_txn(account_id: str, amount: str, txn_type=TransactionType.DEBIT, **metadata) -> Transaction:
    return Transaction(
        account_id=account_id, transaction_type=txn_type, amount=Decimal(amount), metadata=metadata
    )


@pytest.fixture
def coordinator():
    c = Coordinator(
        num_partitions=4,
        num_fraud_workers=2,
        initial_balances={"acct-1": Decimal("1000"), "acct-2": Decimal("1000")},
        fraud_session_timeout=0.3,
        health_check_interval=0.05,
        scorer_config=ScorerConfig(large_amount_threshold=Decimal("10000")),
    )
    c.start()
    yield c
    c.stop()


def test_clean_transaction_flows_end_to_end_and_settles(coordinator):
    coordinator.submit_transaction(make_txn("acct-1", "150"))
    assert wait_until(lambda: coordinator.submitter.submitted_count == 1)
    assert wait_until(lambda: coordinator.balance("acct-1") == Decimal("850"))


def test_fraudulent_transaction_is_flagged_and_never_settled(coordinator):
    coordinator.submit_transaction(make_txn("acct-1", "50000"))  # over the large-amount threshold
    assert wait_until(lambda: coordinator.submitter.fraud_flagged_count == 1)
    time.sleep(0.2)
    assert coordinator.submitter.submitted_count == 0
    assert coordinator.balance("acct-1") == Decimal("1000")


def test_kill_fraud_worker_mid_stream_reassigns_and_nothing_is_lost(coordinator):
    # distinct accounts (and small $10 amounts) so this exercises rebalancing,
    # not the velocity fraud rule -- ten rapid debits on the SAME account
    # would legitimately get some of them flagged, which isn't what's under
    # test here
    # CREDIT (not DEBIT) so these unseeded accounts (balance 0) don't get
    # rejected by the ledger for going negative -- that would be a settlement-
    # level rejection unrelated to what this test is actually checking
    account_ids = [f"acct-worker-{i}" for i in range(10)]
    for account_id in account_ids:
        coordinator.submit_transaction(make_txn(account_id, "10", txn_type=TransactionType.CREDIT))

    coordinator.kill_fraud_worker("fraud-0")

    # session timeout (0.3s) + health check interval must both elapse for
    # the group to notice and reassign fraud-0's partitions to fraud-1
    assert wait_until(
        lambda: coordinator.submitter.submitted_count + coordinator.submitter.fraud_flagged_count == 10,
        timeout=5.0,
    )
    assert coordinator.submitter.submitted_count == 10  # none of these should trip fraud rules
    for account_id in account_ids:
        assert wait_until(lambda a=account_id: coordinator.balance(a) == Decimal("10"), timeout=3.0)


def test_kill_raft_leader_mid_transaction_recovers_without_loss_or_duplication(coordinator):
    coordinator.submit_transaction(make_txn("acct-1", "100"))
    assert wait_until(lambda: coordinator.submitter.submitted_count == 1)
    assert wait_until(lambda: coordinator.balance("acct-1") == Decimal("900"))

    leader = coordinator.raft_cluster.find_leader()
    assert leader is not None
    old_leader_id, old_term = leader.node_id, leader.current_term
    coordinator.raft_cluster.kill(old_leader_id)

    coordinator.submit_transaction(make_txn("acct-1", "200"))
    assert wait_until(
        lambda: (
            (coordinator.raft_cluster.find_leader() is not None)
            and coordinator.raft_cluster.find_leader().current_term > old_term
        ),
        timeout=3.0,
    )
    assert wait_until(lambda: coordinator.submitter.submitted_count == 2, timeout=3.0)
    assert wait_until(lambda: coordinator.balance("acct-1") == Decimal("700"), timeout=3.0)
