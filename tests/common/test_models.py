from decimal import Decimal

import pytest

from common.models import (
    Account,
    FraudScore,
    LedgerEntry,
    Transaction,
    TransactionType,
)


def test_transaction_defaults_and_partition_key():
    t = Transaction(account_id="acct-1", transaction_type=TransactionType.DEBIT, amount=Decimal("42.50"))
    assert t.transaction_id
    assert t.status.value == "PENDING"
    assert t.partition_key == "acct-1"


def test_transaction_rejects_non_positive_amount():
    with pytest.raises(ValueError):
        Transaction(account_id="acct-1", transaction_type=TransactionType.DEBIT, amount=Decimal("-5"))


def test_transaction_transfer_requires_counterparty():
    with pytest.raises(ValueError):
        Transaction(account_id="acct-1", transaction_type=TransactionType.TRANSFER, amount=Decimal("5"))


def test_transaction_is_immutable():
    t = Transaction(account_id="acct-1", transaction_type=TransactionType.DEBIT, amount=Decimal("5"))
    with pytest.raises(AttributeError):
        t.amount = Decimal("10")  # type: ignore[misc]


def test_ledger_entry_defaults_entry_id_to_transaction_id():
    entry = LedgerEntry(transaction_id="tx-1", account_id="acct-1", delta=Decimal("-5"))
    assert entry.entry_id == "tx-1"


def test_ledger_entry_rejects_zero_delta():
    with pytest.raises(ValueError):
        LedgerEntry(transaction_id="tx-1", account_id="acct-1", delta=Decimal("0"))


def test_fraud_score_rejects_out_of_range_score():
    with pytest.raises(ValueError):
        FraudScore(transaction_id="tx-1", worker_id="w1", score=1.5, is_fraud=True)


def test_account_rejects_empty_account_id():
    with pytest.raises(ValueError):
        Account(account_id="", balance=Decimal("0"))
