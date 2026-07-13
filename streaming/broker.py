"""In-process partitioned log.

This is the first-pass "dummy" bus: everything lives in one process, in
memory, with no network transport. It exists to prove the partitioning and
offset semantics are correct before stage 3 (fraud workers) turns each
partition's reader into a real out-of-process gRPC consumer, and before the
coordinator spreads producers/consumers across containers.

Each partition is an append-only list of LogRecord. Offsets are the index
into that list, so "commit offset N" always means "everything before N has
been consumed" -- the same semantics a real log-structured broker gives you.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import RLock
from typing import Any

from common.models import utc_now


@dataclass(frozen=True, slots=True)
class LogRecord:
    partition: int
    offset: int
    key: str
    value: Any
    produced_at: datetime = field(default_factory=utc_now)


class PartitionedLog:
    def __init__(self, topic: str, num_partitions: int):
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")
        self.topic = topic
        self._partitions: list[list[LogRecord]] = [[] for _ in range(num_partitions)]
        self._locks: list[RLock] = [RLock() for _ in range(num_partitions)]
        self._committed_offsets: dict[tuple[str, int], int] = {}
        self._offsets_lock = RLock()

    @property
    def num_partitions(self) -> int:
        return len(self._partitions)

    def append(self, partition: int, key: str, value: Any) -> LogRecord:
        self._check_partition(partition)
        with self._locks[partition]:
            offset = len(self._partitions[partition])
            record = LogRecord(partition=partition, offset=offset, key=key, value=value)
            self._partitions[partition].append(record)
            return record

    def read(self, partition: int, from_offset: int, max_records: int = 100) -> list[LogRecord]:
        self._check_partition(partition)
        if from_offset < 0:
            raise ValueError("from_offset must be non-negative")
        with self._locks[partition]:
            return list(self._partitions[partition][from_offset : from_offset + max_records])

    def latest_offset(self, partition: int) -> int:
        self._check_partition(partition)
        with self._locks[partition]:
            return len(self._partitions[partition])

    def commit_offset(self, group_id: str, partition: int, offset: int) -> None:
        """Persist a consumer group's progress on a partition.

        Committing to the broker (rather than in the consumer's own memory)
        is what lets a partition move to a different group member after a
        rebalance and resume from where the last owner left off, instead of
        replaying from the start or losing track of what was processed.
        """
        self._check_partition(partition)
        with self._offsets_lock:
            key = (group_id, partition)
            self._committed_offsets[key] = max(self._committed_offsets.get(key, 0), offset)

    def committed_offset(self, group_id: str, partition: int) -> int:
        self._check_partition(partition)
        with self._offsets_lock:
            return self._committed_offsets.get((group_id, partition), 0)

    def _check_partition(self, partition: int) -> None:
        if not 0 <= partition < self.num_partitions:
            raise ValueError(f"partition {partition} out of range [0, {self.num_partitions})")
