import pytest

from streaming.broker import PartitionedLog


def test_append_assigns_sequential_offsets_per_partition():
    log = PartitionedLog("txns", num_partitions=2)
    r0 = log.append(0, key="a", value="first")
    r1 = log.append(0, key="a", value="second")
    r2 = log.append(1, key="b", value="third")

    assert (r0.partition, r0.offset) == (0, 0)
    assert (r1.partition, r1.offset) == (0, 1)
    assert (r2.partition, r2.offset) == (1, 0)


def test_read_returns_records_from_offset():
    log = PartitionedLog("txns", num_partitions=1)
    for i in range(5):
        log.append(0, key="a", value=i)

    records = log.read(0, from_offset=2, max_records=2)
    assert [r.value for r in records] == [2, 3]


def test_latest_offset_tracks_append_count():
    log = PartitionedLog("txns", num_partitions=1)
    assert log.latest_offset(0) == 0
    log.append(0, key="a", value=1)
    assert log.latest_offset(0) == 1


def test_out_of_range_partition_raises():
    log = PartitionedLog("txns", num_partitions=2)
    with pytest.raises(ValueError):
        log.append(2, key="a", value=1)
    with pytest.raises(ValueError):
        log.read(-1, from_offset=0)
