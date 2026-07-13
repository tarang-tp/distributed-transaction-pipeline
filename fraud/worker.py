"""FraudWorker: a consumer-group member that scores transactions.

Delivery guarantee: at-least-once. Offsets are committed only after a score
has been produced and handed to the output queue, so a worker that dies
between scoring and commit causes its partitions to be reprocessed by
whichever member picks them up on rebalance -- never silently dropped.
Reprocessing is safe because transaction_id is a stable idempotency key
every downstream consumer of FraudScore can dedupe on, which is what turns
at-least-once + idempotent processing into effectively-once.

Backpressure: each worker pulls only as many records as there is room for
in its bounded output queue. If the queue is full (downstream can't keep
up), the worker produces nothing this cycle rather than buffering
unboundedly -- the slowdown propagates upstream to the consumer instead of
growing memory without bound.
"""

from __future__ import annotations

import queue

from common.models import ScoredTransaction
from fraud.scorer import RuleBasedScorer
from streaming.broker import PartitionedLog
from streaming.consumer import GroupConsumer
from streaming.group import ConsumerGroup


class FraudWorker:
    def __init__(
        self,
        member_id: str,
        log: PartitionedLog,
        group: ConsumerGroup,
        scorer: RuleBasedScorer,
        output_queue: "queue.Queue[ScoredTransaction]",
        max_batch: int = 50,
    ):
        self.member_id = member_id
        self._consumer = GroupConsumer(log, group, member_id)
        self._scorer = scorer
        self._output_queue = output_queue
        self._max_batch = max_batch

    def join(self) -> list[int]:
        return self._consumer.join()

    def heartbeat(self) -> None:
        self._consumer.heartbeat()

    def run_once(self) -> list[ScoredTransaction]:
        capacity = self._available_capacity()
        if capacity <= 0:
            return []  # backpressure: downstream queue is full, pull nothing this cycle

        produced: list[ScoredTransaction] = []
        for partition in self._consumer.assigned_partitions:
            if capacity <= 0:
                break
            records = self._consumer.poll_partition(partition, capacity)
            for record in records:
                transaction = record.value
                score = self._scorer.score(transaction)
                scored = ScoredTransaction(transaction=transaction, score=score)
                self._output_queue.put_nowait(scored)
                self._consumer.commit(record)
                produced.append(scored)
                capacity -= 1
        return produced

    def _available_capacity(self) -> int:
        if self._output_queue.maxsize <= 0:
            return self._max_batch
        room = self._output_queue.maxsize - self._output_queue.qsize()
        return max(0, min(self._max_batch, room))
