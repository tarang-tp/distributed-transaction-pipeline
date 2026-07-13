"""Automated coverage for demo scenario 1 (scripts/demo_fraud_burst.py):
a burst of fraud-shaped transactions under load should be mostly caught,
while normal traffic passes through clean.
"""

import time
from decimal import Decimal

from common.models import Transaction, TransactionType
from coordinator.coordinator import Coordinator
from fraud.scorer import ScorerConfig


def wait_until(predicate, timeout=8.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_fraud_burst_is_mostly_caught_while_normal_traffic_settles_clean():
    num_accounts = 20
    legit_per_account = 3
    velocity_burst_count = 25
    large_amount_count = 25

    coordinator = Coordinator(
        num_partitions=4,
        num_fraud_workers=3,
        initial_balances={f"acct-{i}": Decimal("5000") for i in range(num_accounts)},
        scorer_config=ScorerConfig(
            large_amount_threshold=Decimal("10000"),
            velocity_max_transactions=5,
            velocity_window_seconds=5.0,
        ),
    )
    coordinator.start()
    try:
        legit_count = num_accounts * legit_per_account
        for i in range(legit_count):
            coordinator.submit_transaction(
                Transaction(
                    account_id=f"acct-{i % num_accounts}",
                    transaction_type=TransactionType.DEBIT,
                    amount=Decimal("25"),
                )
            )

        fraud_shaped = velocity_burst_count + large_amount_count
        for _ in range(velocity_burst_count):
            coordinator.submit_transaction(
                Transaction(
                    account_id="acct-victim", transaction_type=TransactionType.DEBIT, amount=Decimal("15")
                )
            )
        for i in range(large_amount_count):
            coordinator.submit_transaction(
                Transaction(
                    account_id=f"acct-{i % num_accounts}",
                    transaction_type=TransactionType.DEBIT,
                    amount=Decimal("25000"),
                )
            )

        total = legit_count + fraud_shaped
        assert wait_until(
            lambda: coordinator.submitter.submitted_count + coordinator.submitter.fraud_flagged_count >= total
        )

        status = coordinator.status()
        # every legit transaction should settle clean -- none should trip fraud rules
        assert status["submitted_count"] >= legit_count
        # the large-amount half of the burst (25) is caught deterministically every time;
        # the velocity half only trips after the 5th transaction on that account, so at
        # least 20 of those 25 are always caught too
        assert status["fraud_flagged_count"] >= 45
        catch_rate = status["fraud_flagged_count"] / fraud_shaped
        assert catch_rate >= 0.85
    finally:
        coordinator.stop()
