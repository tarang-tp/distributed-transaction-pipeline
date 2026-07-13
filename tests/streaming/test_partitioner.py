from streaming.partitioner import key_to_partition


def test_same_key_always_same_partition():
    partitions = {key_to_partition("acct-42", 8) for _ in range(50)}
    assert len(partitions) == 1


def test_partition_within_range():
    for key in ["a", "acct-1", "acct-2", "region-us-east"]:
        p = key_to_partition(key, 4)
        assert 0 <= p < 4


def test_rejects_non_positive_partitions():
    import pytest

    with pytest.raises(ValueError):
        key_to_partition("k", 0)
