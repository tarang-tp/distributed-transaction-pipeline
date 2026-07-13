"""Demo 1: a burst of fraudulent transactions under load -- real-time catch
rate as the fraud-scoring stage sees it.

Run: python scripts/demo_fraud_burst.py
"""

from __future__ import annotations

import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import Transaction, TransactionType  # noqa: E402
from coordinator.coordinator import Coordinator  # noqa: E402
from fraud.scorer import ScorerConfig  # noqa: E402


def main() -> None:
    num_accounts = 20
    legit_per_account = 3
    velocity_burst_count = 25  # rapid debits on ONE account -- trips the velocity rule
    large_amount_count = 25  # spread across accounts -- trips the large-amount rule

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
        print(f"Submitting {legit_count} normal transactions across {num_accounts} accounts...")
        for i in range(legit_count):
            account = f"acct-{i % num_accounts}"
            coordinator.submit_transaction(
                Transaction(account_id=account, transaction_type=TransactionType.DEBIT, amount=Decimal("25"))
            )

        fraud_shaped = velocity_burst_count + large_amount_count
        print(f"Firing a burst of {fraud_shaped} fraud-shaped transactions...")
        start = time.monotonic()
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
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            status = coordinator.status()
            if status["submitted_count"] + status["fraud_flagged_count"] >= total:
                break
            time.sleep(0.02)
        elapsed = time.monotonic() - start

        status = coordinator.status()
        flagged = status["fraud_flagged_count"]
        processed = status["submitted_count"] + flagged
        print()
        print(
            f"Processed {processed}/{total} transactions in {elapsed:.2f}s ({processed / elapsed:.0f} txn/s)"
        )
        print(f"Settled clean:   {status['submitted_count']}")
        print(f"Flagged fraud:   {flagged}  (of ~{fraud_shaped} fraud-shaped transactions injected)")
        print(f"Approx catch rate: {min(flagged, fraud_shaped) / fraud_shaped:.0%}")
    finally:
        coordinator.stop()


if __name__ == "__main__":
    main()
