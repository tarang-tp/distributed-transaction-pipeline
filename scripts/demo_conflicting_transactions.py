"""Demo 2: two conflicting transactions hit the same account "simultaneously"
-- Raft consensus forces them into one agreed order, so only one is ever
accepted. No double-spend.

Run: python scripts/demo_conflicting_transactions.py
"""

from __future__ import annotations

import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import Transaction, TransactionType  # noqa: E402
from coordinator.coordinator import Coordinator  # noqa: E402


def main() -> None:
    coordinator = Coordinator(
        num_partitions=2, num_fraud_workers=2, initial_balances={"acct-1": Decimal("100")}
    )
    coordinator.start()
    try:
        print("Starting balance for acct-1: $100")
        txn_a = Transaction(account_id="acct-1", transaction_type=TransactionType.DEBIT, amount=Decimal("80"))
        txn_b = Transaction(account_id="acct-1", transaction_type=TransactionType.DEBIT, amount=Decimal("80"))
        print(f"Firing two concurrent $80 debits: {txn_a.transaction_id[:8]} and {txn_b.transaction_id[:8]}")

        barrier = threading.Barrier(2)

        def fire(txn: Transaction) -> None:
            barrier.wait()  # both threads submit as close to simultaneously as possible
            coordinator.submit_transaction(txn)

        t1 = threading.Thread(target=fire, args=(txn_a,))
        t2 = threading.Thread(target=fire, args=(txn_b,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # submitted_count only means "accepted into the leader's log" -- commit
        # and apply happen asynchronously afterward, so poll for the actual
        # settled balance rather than checking once right after submission
        deadline = time.monotonic() + 5.0
        final_balance = coordinator.balance("acct-1")
        while time.monotonic() < deadline and final_balance == Decimal("100"):
            time.sleep(0.02)
            final_balance = coordinator.balance("acct-1")

        print(f"Final balance: ${final_balance}")
        if final_balance == Decimal("20"):
            print("PASS: exactly one $80 debit was accepted -- no double-spend.")
        else:
            print(f"UNEXPECTED final balance: {final_balance} (expected $20)")
    finally:
        coordinator.stop()


if __name__ == "__main__":
    main()
