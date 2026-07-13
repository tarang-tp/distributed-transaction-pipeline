import pytest

from streaming.group import ConsumerGroup


def make_clock():
    state = {"t": 0.0}

    def now():
        return state["t"]

    def advance(seconds):
        state["t"] += seconds

    return now, advance


def test_single_member_gets_all_partitions():
    now, _ = make_clock()
    group = ConsumerGroup("g1", num_partitions=4, now_fn=now)
    assigned = group.join("w1")
    assert assigned == [0, 1, 2, 3]


def test_partitions_split_round_robin_across_members():
    now, _ = make_clock()
    group = ConsumerGroup("g1", num_partitions=4, now_fn=now)
    group.join("w1")
    group.join("w2")
    assert group.assignment_for("w1") == [0, 2]
    assert group.assignment_for("w2") == [1, 3]


def test_heartbeat_requires_prior_join():
    now, _ = make_clock()
    group = ConsumerGroup("g1", num_partitions=2, now_fn=now)
    with pytest.raises(ValueError):
        group.heartbeat("ghost")


def test_expired_member_triggers_rebalance_onto_survivor():
    now, advance = make_clock()
    group = ConsumerGroup("g1", num_partitions=4, session_timeout_seconds=10.0, now_fn=now)
    group.join("w1")
    group.join("w2")
    assert group.assignment_for("w1") == [0, 2]

    advance(5)
    group.heartbeat("w2")  # w1 stops heartbeating (simulates a dead worker)
    advance(6)  # w1's last heartbeat is now 11s stale, past the 10s timeout

    expired = group.check_expired_members()

    assert expired == ["w1"]
    assert group.assignment_for("w1") == []
    assert group.assignment_for("w2") == [0, 1, 2, 3]


def test_no_expiry_when_all_members_heartbeat_in_time():
    now, advance = make_clock()
    group = ConsumerGroup("g1", num_partitions=2, session_timeout_seconds=10.0, now_fn=now)
    group.join("w1")
    advance(5)
    group.heartbeat("w1")
    advance(5)

    assert group.check_expired_members() == []
    assert group.assignment_for("w1") == [0, 1]


def test_leave_rebalances_remaining_members():
    now, _ = make_clock()
    group = ConsumerGroup("g1", num_partitions=2, now_fn=now)
    group.join("w1")
    group.join("w2")
    group.leave("w2")
    assert group.assignment_for("w1") == [0, 1]
