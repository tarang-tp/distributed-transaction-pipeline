"""Bridges fraud-cleared transactions into Raft proposals.

Reads ScoredTransaction off the shared fraud-output queue. Fraud-flagged
transactions are counted and dropped here -- they never reach the ledger.
Cleared ones become a LedgerEntry proposed to the Raft cluster; idempotent
apply on LedgerStateMachine (keyed on entry_id) is what makes a retried or
duplicated proposal after a leader failover safe to just resubmit.

Only DEBIT/CREDIT are settled as a single-account entry. TRANSFER's
counterparty leg (crediting a second account atomically) isn't implemented
-- that needs a multi-entry command committed as one Raft entry, which none
of the three demo scenarios require, so it's out of scope for this pass.
"""

from __future__ import annotations

import queue
import threading
from decimal import Decimal

from common.models import LedgerEntry, ScoredTransaction, Transaction, TransactionType
from coordinator.raft_cluster import RaftCluster


def delta_for(transaction: Transaction) -> Decimal:
    if transaction.transaction_type == TransactionType.CREDIT:
        return transaction.amount
    return -transaction.amount  # DEBIT and TRANSFER both draw down the source account


class SettlementSubmitter:
    def __init__(
        self,
        input_queue: "queue.Queue[ScoredTransaction]",
        raft_cluster: RaftCluster,
        poll_interval: float = 0.02,
        propose_timeout: float = 2.0,
    ):
        self._input_queue = input_queue
        self._raft_cluster = raft_cluster
        self._poll_interval = poll_interval
        self._propose_timeout = propose_timeout
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self.submitted_count = 0
        self.fraud_flagged_count = 0
        self.failed_transaction_ids: list[str] = []

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="settlement-submitter", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                scored = self._input_queue.get(timeout=self._poll_interval)
            except queue.Empty:
                continue
            self._handle(scored)

    def _handle(self, scored: ScoredTransaction) -> None:
        if scored.score.is_fraud:
            with self._lock:
                self.fraud_flagged_count += 1
            return

        entry = LedgerEntry(
            transaction_id=scored.transaction.transaction_id,
            account_id=scored.transaction.account_id,
            delta=delta_for(scored.transaction),
        )
        try:
            self._raft_cluster.propose(entry, timeout=self._propose_timeout)
            with self._lock:
                self.submitted_count += 1
        except TimeoutError:
            with self._lock:
                self.failed_transaction_ids.append(scored.transaction.transaction_id)
