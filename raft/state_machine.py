"""State machine interface applied to committed log entries.

Because commitIndex/lastApplied are volatile (never persisted -- only
currentTerm, votedFor, and the log itself are, per the Raft paper), a node
that crashes and restarts replays already-committed entries from the start
once it rejoins and learns the leader's commit index again. apply() must
therefore be idempotent on the command's own identity, not rely on being
called exactly once. settlement/'s ledger state machine will follow the
same pattern using LedgerEntry.entry_id; RecordingStateMachine here is the
test-only stand-in that proves RaftNode holds up its end of that contract.
"""

from __future__ import annotations

from typing import Any, Protocol


class StateMachine(Protocol):
    def apply(self, command: Any) -> Any: ...


class RecordingStateMachine:
    def __init__(self) -> None:
        self.applied: list[Any] = []
        self._applied_ids: set[Any] = set()

    def apply(self, command: Any) -> Any:
        key = getattr(command, "entry_id", command)
        if key in self._applied_ids:
            return None
        self._applied_ids.add(key)
        self.applied.append(command)
        return command
