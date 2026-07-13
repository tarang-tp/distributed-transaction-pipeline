"""Single-consumer reader over a PartitionedLog.

Deliberately dumb for this first pass: one consumer, statically assigned to
a fixed set of partitions, in-memory offsets. Consumer-group membership,
partition rebalancing on worker death, and offset persistence belong to
stage 3 (fraud workers), which will build on top of this.
"""

from __future__ import annotations

from streaming.broker import LogRecord, PartitionedLog
from streaming.group import ConsumerGroup


class Consumer:
    def __init__(self, log: PartitionedLog, partitions: list[int] | None = None):
        self._log = log
        self._partitions = partitions if partitions is not None else list(range(log.num_partitions))
        self._offsets: dict[int, int] = {p: 0 for p in self._partitions}

    def poll(self, max_records_per_partition: int = 100) -> list[LogRecord]:
        batch: list[LogRecord] = []
        for partition in self._partitions:
            records = self._log.read(partition, self._offsets[partition], max_records_per_partition)
            batch.extend(records)
        return batch

    def commit(self, record: LogRecord) -> None:
        self._offsets[record.partition] = max(self._offsets[record.partition], record.offset + 1)

    def offset(self, partition: int) -> int:
        return self._offsets[partition]


class GroupConsumer:
    """A consumer-group-aware reader.

    Partition ownership comes from a ConsumerGroup (which handles join,
    heartbeat, and rebalancing on member death); offsets are committed to
    the broker itself, so when a partition is reassigned after a rebalance
    the new owner resumes from the last commit rather than from scratch or
    from wherever the old owner's in-memory offset happened to be.
    """

    def __init__(self, log: PartitionedLog, group: ConsumerGroup, member_id: str):
        self._log = log
        self._group = group
        self._member_id = member_id

    def join(self) -> list[int]:
        return self._group.join(self._member_id)

    def heartbeat(self) -> None:
        self._group.heartbeat(self._member_id)

    @property
    def assigned_partitions(self) -> list[int]:
        return self._group.assignment_for(self._member_id)

    def poll_partition(self, partition: int, max_records: int) -> list[LogRecord]:
        offset = self._log.committed_offset(self._group.group_id, partition)
        return self._log.read(partition, offset, max_records)

    def commit(self, record: LogRecord) -> None:
        self._log.commit_offset(self._group.group_id, record.partition, record.offset + 1)
