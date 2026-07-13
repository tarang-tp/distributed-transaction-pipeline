"""Demo 3: kill the Raft leader mid-transaction -- the cluster elects a new
leader and recovers with no lost or duplicated data.

Run: python scripts/demo_kill_leader.py
"""

from __future__ import annotations

import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chaos.fault_injection import kill_raft_leader, wait_for_new_leader  # noqa: E402
from common.models import Transaction, TransactionType  # noqa: E402
from coordinator.coordinator import Coordinator  # noqa: E402


def main() -> None:
    coordinator = Coordinator(
        num_partitions=2,
        num_fraud_workers=2,
        initial_balances={"acct-1": Decimal("1000")},
        raft_node_ids=["raft-1", "raft-2", "raft-3", "raft-4", "raft-5"],
    )
    coordinator.start()
    try:
        print("Settling one transaction before the kill...")
        coordinator.submit_transaction(
            Transaction(account_id="acct-1", transaction_type=TransactionType.DEBIT, amount=Decimal("100"))
        )
        deadline = time.monotonic() + 3.0
        balance = coordinator.balance("acct-1")
        while time.monotonic() < deadline and balance != Decimal("900"):
            time.sleep(0.02)
            balance = coordinator.balance("acct-1")
        print(f"Balance after txn1: ${balance}")

        old_leader = coordinator.raft_cluster.find_leader()
        assert old_leader is not None
        old_term = old_leader.current_term
        killed_id = kill_raft_leader(coordinator)
        print(f"Killed leader {killed_id} (term {old_term})")

        print("Submitting a second transaction immediately (mid-failover)...")
        coordinator.submit_transaction(
            Transaction(account_id="acct-1", transaction_type=TransactionType.DEBIT, amount=Decimal("200"))
        )

        new_leader = wait_for_new_leader(coordinator, exclude_term=old_term)
        print(f"New leader elected: {new_leader.node_id} (term {new_leader.current_term})")

        expected = Decimal("700")
        deadline = time.monotonic() + 3.0
        final_balance = coordinator.balance("acct-1")
        while time.monotonic() < deadline and final_balance != expected:
            time.sleep(0.02)
            final_balance = coordinator.balance("acct-1")

        print(f"Final balance: ${final_balance} (expected $700: 1000 - 100 - 200)")
        if final_balance == Decimal("700"):
            print("PASS: no transaction lost or double-applied across the leader failover.")
        else:
            print(f"UNEXPECTED final balance: {final_balance}")
    finally:
        coordinator.stop()


if __name__ == "__main__":
    main()
