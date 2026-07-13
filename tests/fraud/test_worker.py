import queue
from decimal import Decimal

from common.models import Transaction, TransactionType
from fraud.scorer import RuleBasedScorer
from fraud.worker import FraudWorker
from streaming.broker import PartitionedLog
from streaming.group import ConsumerGroup


def make_clock():
    state = {"t": 0.0}

    def now():
        return state["t"]

    def advance(seconds):
        state["t"] += seconds

    return now, advance


def make_txn(account_id="acct-1"):
    return Transaction(account_id=account_id, transaction_type=TransactionType.DEBIT, amount=Decimal("1.00"))


def test_worker_stops_pulling_when_output_queue_is_full():
    now, _ = make_clock()
    log = PartitionedLog("txns", num_partitions=1)
    for _ in range(5):
        log.append(0, key="acct-1", value=make_txn())

    group = ConsumerGroup("g1", num_partitions=1, now_fn=now)
    out_queue: "queue.Queue" = queue.Queue(maxsize=2)
    worker = FraudWorker("w1", log, group, RuleBasedScorer("w1"), out_queue, max_batch=10)
    worker.join()

    first = worker.run_once()
    assert len(first) == 2  # bounded by queue capacity, not max_batch
    assert out_queue.full()

    second = worker.run_once()
    assert second == []  # backpressure: queue still full, nothing pulled

    out_queue.get()  # downstream drains one item
    third = worker.run_once()
    assert len(third) == 1


def test_worker_survives_peer_death_all_transactions_still_scored():
    now, advance = make_clock()
    log = PartitionedLog("txns", num_partitions=2)
    partition0_txns = [make_txn("acct-a") for _ in range(3)]
    partition1_txns = [make_txn("acct-b") for _ in range(3)]
    for t in partition0_txns:
        log.append(0, key="acct-a", value=t)
    for t in partition1_txns:
        log.append(1, key="acct-b", value=t)

    group = ConsumerGroup("g1", num_partitions=2, session_timeout_seconds=10.0, now_fn=now)
    out_queue: "queue.Queue" = queue.Queue()
    worker1 = FraudWorker("w1", log, group, RuleBasedScorer("w1"), out_queue, max_batch=100)
    worker2 = FraudWorker("w2", log, group, RuleBasedScorer("w2"), out_queue, max_batch=100)

    worker1.join()
    worker2.join()
    assert group.assignment_for("w1") == [0]
    assert group.assignment_for("w2") == [1]

    worker1.run_once()  # fully drains and commits partition 0

    advance(5)
    worker1.heartbeat()  # w1 stays alive; w2 never heartbeats again -- simulates a dead worker
    advance(6)  # w2's last heartbeat is now 11s stale, past the 10s timeout
    expired = group.check_expired_members()
    assert expired == ["w2"]
    assert group.assignment_for("w1") == [0, 1]

    worker1.run_once()  # now also owns partition 1, resumes from offset 0 there

    all_scored = list(out_queue.queue)
    scored_ids = {st.score.transaction_id for st in all_scored}
    expected_ids = {t.transaction_id for t in partition0_txns + partition1_txns}

    assert scored_ids == expected_ids
    assert len(all_scored) == 6  # no duplicate processing since w2 never touched partition 1
