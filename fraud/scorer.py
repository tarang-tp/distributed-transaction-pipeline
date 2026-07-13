"""Rules-based fraud scorer.

Keeps per-account velocity state (recent transaction timestamps) in memory.
This is safe to do per-worker, unshared, only because Transaction.partition_key
is account_id: every transaction for a given account is guaranteed to land
on the same partition and therefore be scored by the same worker, so no
account's velocity window is ever split across two scorer instances.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from threading import RLock
from typing import Callable

from common.models import FraudScore, Transaction, utc_now


@dataclass(frozen=True, slots=True)
class ScorerConfig:
    large_amount_threshold: Decimal = Decimal("10000")
    velocity_window_seconds: float = 60.0
    velocity_max_transactions: int = 5
    device_flag_weight: float = 0.6
    large_amount_weight: float = 0.5
    velocity_weight: float = 0.6
    fraud_threshold: float = 0.5


class RuleBasedScorer:
    def __init__(
        self,
        worker_id: str,
        config: ScorerConfig | None = None,
        now_fn: Callable[[], datetime] = utc_now,
    ):
        self._worker_id = worker_id
        self._config = config or ScorerConfig()
        self._now = now_fn
        self._recent_by_account: dict[str, deque[datetime]] = defaultdict(deque)
        self._lock = RLock()

    def score(self, txn: Transaction) -> FraudScore:
        config = self._config
        weight = 0.0
        reasons: list[str] = []

        if txn.amount >= config.large_amount_threshold:
            weight += config.large_amount_weight
            reasons.append(f"amount {txn.amount} >= large-amount threshold {config.large_amount_threshold}")

        if txn.metadata.get("device_flagged") == "true":
            weight += config.device_flag_weight
            reasons.append("device flagged")

        velocity = self._record_and_count(txn)
        if velocity > config.velocity_max_transactions:
            weight += config.velocity_weight
            reasons.append(
                f"velocity {velocity} transactions for {txn.account_id} "
                f"within {config.velocity_window_seconds}s window"
            )

        score = min(weight, 1.0)
        return FraudScore(
            transaction_id=txn.transaction_id,
            worker_id=self._worker_id,
            score=score,
            is_fraud=score >= config.fraud_threshold,
            reasons=tuple(reasons),
        )

    def _record_and_count(self, txn: Transaction) -> int:
        with self._lock:
            window = self._recent_by_account[txn.account_id]
            now = self._now()
            window.append(now)
            cutoff = now - timedelta(seconds=self._config.velocity_window_seconds)
            while window and window[0] < cutoff:
                window.popleft()
            return len(window)
