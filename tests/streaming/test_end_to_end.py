from decimal import Decimal

from common.models import Transaction, TransactionType
from streaming.broker import PartitionedLog
from streaming.consumer import Consumer
from streaming.partitioner import key_to_partition
from streaming.producer import Producer


def _make_txn(account_id: str, amount: str) -> Transaction:
    return Transaction(
        account_id=account_id,
        transaction_type=TransactionType.DEBIT,
        amount=Decimal(amount),
    )


def test_events_flow_end_to_end_and_all_are_consumed():
    log = PartitionedLog("txns", num_partitions=4)
    producer = Producer(log)
    consumer = Consumer(log)

    txns = [_make_txn(f"acct-{i % 6}", "10.00") for i in range(30)]
    for txn in txns:
        producer.produce(txn.partition_key, txn)

    consumed = consumer.poll()
    consumed_ids = {record.value.transaction_id for record in consumed}

    assert len(consumed) == 30
    assert consumed_ids == {t.transaction_id for t in txns}


def test_same_account_transactions_land_on_same_partition_in_order():
    log = PartitionedLog("txns", num_partitions=4)
    producer = Producer(log)

    txns = [_make_txn("acct-shared", str(i + 1)) for i in range(10)]
    for txn in txns:
        producer.produce(txn.partition_key, txn)

    expected_partition = key_to_partition("acct-shared", 4)
    records = log.read(expected_partition, from_offset=0, max_records=100)

    assert [r.value.transaction_id for r in records] == [t.transaction_id for t in txns]


def test_consumer_commit_advances_offset_and_poll_is_incremental():
    log = PartitionedLog("txns", num_partitions=1)
    producer = Producer(log)
    consumer = Consumer(log)

    for i in range(3):
        producer.produce("acct-1", _make_txn("acct-1", "1.00"))

    first_batch = consumer.poll()
    assert len(first_batch) == 3
    for record in first_batch:
        consumer.commit(record)

    assert consumer.offset(0) == 3
    assert consumer.poll() == []

    producer.produce("acct-1", _make_txn("acct-1", "2.00"))
    second_batch = consumer.poll()
    assert len(second_batch) == 1
