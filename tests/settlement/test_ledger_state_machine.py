from decimal import Decimal

from common.models import LedgerEntry
from settlement.ledger_state_machine import LedgerStateMachine


def test_credit_increases_balance():
    sm = LedgerStateMachine({"acct-1": Decimal("100")})
    result = sm.apply(LedgerEntry(transaction_id="t1", account_id="acct-1", delta=Decimal("50")))
    assert result.accepted is True
    assert result.resulting_balance == Decimal("150")
    assert sm.balance("acct-1") == Decimal("150")


def test_debit_decreases_balance():
    sm = LedgerStateMachine({"acct-1": Decimal("100")})
    result = sm.apply(LedgerEntry(transaction_id="t1", account_id="acct-1", delta=Decimal("-30")))
    assert result.accepted is True
    assert result.resulting_balance == Decimal("70")


def test_debit_rejected_on_insufficient_funds_and_balance_unchanged():
    sm = LedgerStateMachine({"acct-1": Decimal("50")})
    result = sm.apply(LedgerEntry(transaction_id="t1", account_id="acct-1", delta=Decimal("-80")))
    assert result.accepted is False
    assert result.resulting_balance == Decimal("50")
    assert sm.balance("acct-1") == Decimal("50")


def test_idempotent_replay_returns_cached_result_without_reapplying():
    sm = LedgerStateMachine({"acct-1": Decimal("100")})
    entry = LedgerEntry(transaction_id="t1", account_id="acct-1", delta=Decimal("-30"))

    first = sm.apply(entry)
    second = sm.apply(entry)  # simulates replay after a crash/restart

    assert first == second
    assert sm.balance("acct-1") == Decimal("70")  # not debited twice


def test_conflicting_debits_on_same_account_are_serialized_by_apply_order():
    sm = LedgerStateMachine({"acct-1": Decimal("100")})
    debit_a = LedgerEntry(transaction_id="txn-a", account_id="acct-1", delta=Decimal("-80"))
    debit_b = LedgerEntry(transaction_id="txn-b", account_id="acct-1", delta=Decimal("-80"))

    result_a = sm.apply(debit_a)
    result_b = sm.apply(debit_b)

    assert result_a.accepted is True
    assert result_b.accepted is False
    assert sm.balance("acct-1") == Decimal("20")  # never went negative


def test_default_balance_for_unseen_account_is_zero():
    sm = LedgerStateMachine()
    assert sm.balance("never-seen") == Decimal("0")
