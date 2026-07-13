from __future__ import annotations

from typing import Any

from streaming.broker import LogRecord, PartitionedLog
from streaming.partitioner import key_to_partition


class Producer:
    def __init__(self, log: PartitionedLog):
        self._log = log

    def produce(self, key: str, value: Any) -> LogRecord:
        partition = key_to_partition(key, self._log.num_partitions)
        return self._log.append(partition, key, value)
