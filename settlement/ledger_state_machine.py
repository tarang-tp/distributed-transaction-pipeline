"""The ledger state machine applied to Raft's committed log.

This is what raft/'s generic StateMachine protocol gets filled in with once
Raft is wired into settlement. Two properties matter more than anything
else here, because they're what the whole project is actually about:

1. Idempotent apply on LedgerEntry.entry_id -- required because Raft's
   commitIndex/lastApplied are volatile (raft/node.py, raft/state_machine.py),
   so a node that crashes and restarts replays its committed log from
   scratch. Replaying an already-applied entry must be a no-op that returns
   the SAME result, not a silent skip and not a re-debit.

2. Deterministic rejection on insufficient funds. Two conflicting debits on
   the same account only avoid a double-spend because Raft has already
   forced them into one agreed total order in the log before either is
   applied -- every replica applies them in that same order and therefore
   rejects the same one, deterministically, with no coordination beyond
   "replay the log in order".
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from threading import RLock

from common.models import LedgerEntry


@dataclass(frozen=True, slots=True)
class ApplyResult:
    entry_id: str
    accepted: bool
    resulting_balance: Decimal
    reason: str = ""


class LedgerStateMachine:
    def __init__(self, initial_balances: dict[str, Decimal] | None = None):
        self._balances: dict[str, Decimal] = dict(initial_balances or {})
        self._results: dict[str, ApplyResult] = {}
        self._lock = RLock()

    def balance(self, account_id: str) -> Decimal:
        with self._lock:
            return self._balances.get(account_id, Decimal("0"))

    def apply(self, command: LedgerEntry) -> ApplyResult:
        with self._lock:
            cached = self._results.get(command.entry_id)
            if cached is not None:
                return cached  # idempotent replay: same result, not reapplied

            current = self._balances.get(command.account_id, Decimal("0"))
            new_balance = current + command.delta
            if new_balance < 0:
                result = ApplyResult(
                    entry_id=command.entry_id,
                    accepted=False,
                    resulting_balance=current,
                    reason=f"insufficient funds: balance {current} + delta {command.delta} < 0",
                )
            else:
                self._balances[command.account_id] = new_balance
                result = ApplyResult(entry_id=command.entry_id, accepted=True, resulting_balance=new_balance)

            self._results[command.entry_id] = result
            return result
