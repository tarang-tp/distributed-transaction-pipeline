from decimal import Decimal

from common.models import Transaction, TransactionType
from fraud.scorer import RuleBasedScorer, ScorerConfig


def make_clock(start=0.0):
    import datetime

    state = {"t": start}

    def now():
        return datetime.datetime.fromtimestamp(state["t"], tz=datetime.timezone.utc)

    def advance(seconds):
        state["t"] += seconds

    return now, advance


def make_txn(account_id="acct-1", amount="10.00", **metadata):
    return Transaction(
        account_id=account_id,
        transaction_type=TransactionType.DEBIT,
        amount=Decimal(amount),
        metadata=metadata,
    )


def test_clean_low_amount_transaction_scores_low():
    scorer = RuleBasedScorer("w1")
    result = scorer.score(make_txn(amount="25.00"))
    assert result.score < 0.5
    assert result.is_fraud is False


def test_large_amount_is_flagged():
    scorer = RuleBasedScorer("w1")
    result = scorer.score(make_txn(amount="15000.00"))
    assert result.is_fraud is True
    assert any("large-amount" in r for r in result.reasons)


def test_device_flagged_metadata_is_flagged():
    scorer = RuleBasedScorer("w1")
    result = scorer.score(make_txn(amount="10.00", device_flagged="true"))
    assert result.is_fraud is True
    assert "device flagged" in result.reasons


def test_velocity_burst_on_same_account_is_flagged():
    now, _ = make_clock()
    config = ScorerConfig(velocity_max_transactions=3, velocity_window_seconds=60.0)
    scorer = RuleBasedScorer("w1", config=config, now_fn=now)

    results = [scorer.score(make_txn(account_id="acct-burst", amount="5.00")) for _ in range(6)]

    assert results[-1].is_fraud is True
    assert any("velocity" in r for r in results[-1].reasons)


def test_velocity_window_expires_old_transactions():
    now, advance = make_clock()
    config = ScorerConfig(velocity_max_transactions=2, velocity_window_seconds=10.0)
    scorer = RuleBasedScorer("w1", config=config, now_fn=now)

    for _ in range(3):
        scorer.score(make_txn(account_id="acct-1", amount="5.00"))

    advance(15)  # past the velocity window, history should reset

    result = scorer.score(make_txn(account_id="acct-1", amount="5.00"))
    assert not any("velocity" in r for r in result.reasons)


def test_different_accounts_have_independent_velocity_windows():
    now, _ = make_clock()
    config = ScorerConfig(velocity_max_transactions=1, velocity_window_seconds=60.0)
    scorer = RuleBasedScorer("w1", config=config, now_fn=now)

    scorer.score(make_txn(account_id="acct-a", amount="5.00"))
    scorer.score(make_txn(account_id="acct-a", amount="5.00"))
    result_b = scorer.score(make_txn(account_id="acct-b", amount="5.00"))

    assert not any("velocity" in r for r in result_b.reasons)
