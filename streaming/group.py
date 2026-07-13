"""Kafka-style consumer group: membership, heartbeats, and partition
rebalancing when a member dies.

Rebalancing is driven explicitly via check_expired_members() rather than a
background thread, so the algorithm is deterministic and testable with a
fake clock -- the same reasoning as keeping Raft's tests off wall-clock
sleeps. A real worker process calls heartbeat() on a timer and periodically
calls check_expired_members() (or a coordinator does it centrally); either
way the assignment algorithm itself doesn't care who's driving it.

Assignment strategy is plain round-robin over sorted member ids: simple and
deterministic, not sticky (a rebalance can reassign partitions that didn't
need to move). Good enough for this project; a real system would use a
sticky assignor to avoid unnecessary churn.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from threading import RLock
from typing import Callable


@dataclass
class MemberState:
    member_id: str
    last_heartbeat: float


class ConsumerGroup:
    def __init__(
        self,
        group_id: str,
        num_partitions: int,
        session_timeout_seconds: float = 10.0,
        now_fn: Callable[[], float] = time.monotonic,
    ):
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")
        self.group_id = group_id
        self.num_partitions = num_partitions
        self.session_timeout_seconds = session_timeout_seconds
        self._now = now_fn
        self._members: dict[str, MemberState] = {}
        self._assignment: dict[str, list[int]] = {}
        self._lock = RLock()

    def join(self, member_id: str) -> list[int]:
        with self._lock:
            self._members[member_id] = MemberState(member_id, self._now())
            self._rebalance()
            return list(self._assignment[member_id])

    def heartbeat(self, member_id: str) -> None:
        with self._lock:
            if member_id not in self._members:
                raise ValueError(f"unknown member {member_id!r}; call join() first")
            self._members[member_id].last_heartbeat = self._now()

    def leave(self, member_id: str) -> None:
        with self._lock:
            if self._members.pop(member_id, None) is not None:
                self._rebalance()

    def assignment_for(self, member_id: str) -> list[int]:
        with self._lock:
            return list(self._assignment.get(member_id, []))

    def members(self) -> list[str]:
        with self._lock:
            return sorted(self._members)

    def check_expired_members(self) -> list[str]:
        """Evict members that missed their heartbeat window and rebalance
        their partitions onto the survivors. Returns the evicted member ids.
        """
        with self._lock:
            now = self._now()
            expired = [
                member_id
                for member_id, state in self._members.items()
                if now - state.last_heartbeat > self.session_timeout_seconds
            ]
            for member_id in expired:
                del self._members[member_id]
            if expired:
                self._rebalance()
            return expired

    def _rebalance(self) -> None:
        members = sorted(self._members)
        assignment: dict[str, list[int]] = {member_id: [] for member_id in members}
        for partition in range(self.num_partitions):
            if not members:
                break
            owner = members[partition % len(members)]
            assignment[owner].append(partition)
        self._assignment = assignment
