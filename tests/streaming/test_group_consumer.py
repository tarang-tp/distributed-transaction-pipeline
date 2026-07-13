from streaming.broker import PartitionedLog
from streaming.consumer import GroupConsumer
from streaming.group import ConsumerGroup


def make_clock():
    state = {"t": 0.0}

    def now():
        return state["t"]

    def advance(seconds):
        state["t"] += seconds

    return now, advance


def test_uncommitted_records_are_redelivered_to_new_owner_after_rebalance():
    """Proves the at-least-once contract at the transport level: if a
    consumer reads records but crashes before committing, the partition's
    new owner re-reads from the last *committed* offset, not from wherever
    the dead consumer's read cursor happened to be -- so nothing is lost.
    """
    now, advance = make_clock()
    log = PartitionedLog("txns", num_partitions=1)
    for i in range(3):
        log.append(0, key="acct-1", value=f"txn-{i}")

    group = ConsumerGroup("g1", num_partitions=1, session_timeout_seconds=10.0, now_fn=now)

    consumer1 = GroupConsumer(log, group, "c1")
    consumer1.join()
    first_read = consumer1.poll_partition(0, max_records=10)
    assert [r.value for r in first_read] == ["txn-0", "txn-1", "txn-2"]
    # crash: no consumer1.commit() call

    advance(11)  # past session_timeout without a heartbeat from c1
    expired = group.check_expired_members()
    assert expired == ["c1"]

    consumer2 = GroupConsumer(log, group, "c2")
    consumer2.join()
    redelivered = consumer2.poll_partition(0, max_records=10)

    assert [r.value for r in redelivered] == ["txn-0", "txn-1", "txn-2"]


def test_committed_records_are_not_redelivered():
    now, advance = make_clock()
    log = PartitionedLog("txns", num_partitions=1)
    log.append(0, key="acct-1", value="txn-0")

    group = ConsumerGroup("g1", num_partitions=1, session_timeout_seconds=10.0, now_fn=now)
    consumer1 = GroupConsumer(log, group, "c1")
    consumer1.join()
    for record in consumer1.poll_partition(0, max_records=10):
        consumer1.commit(record)

    advance(11)
    group.check_expired_members()

    consumer2 = GroupConsumer(log, group, "c2")
    consumer2.join()
    assert consumer2.poll_partition(0, max_records=10) == []
