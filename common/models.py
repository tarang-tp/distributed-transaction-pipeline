"""Shared domain models for the distributed transaction processing pipeline.

These are plain, stdlib-only dataclasses with no gRPC/proto or networking
dependencies, so every stage (streaming, fraud, raft, settlement,
coordinator) can import them without pulling in wire-format or transport
code. Proto <-> dataclass conversion will live next to the generated stubs
in proto/, not here.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum


def new_id() -> str:
    return str(uuid.uuid4())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TransactionType(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"
    TRANSFER = "TRANSFER"


class TransactionStatus(str, Enum):
    PENDING = "PENDING"  # produced onto the stream, not yet scored
    SCORING = "SCORING"  # picked up by a fraud worker
    FRAUD_FLAGGED = "FRAUD_FLAGGED"  # scorer rejected it, stops here
    CLEARED = "CLEARED"  # passed fraud scoring, headed to Raft
    SETTLED = "SETTLED"  # committed to the ledger state machine
    REJECTED = "REJECTED"  # cleared fraud but settlement rejected it (e.g. insufficient funds)


@dataclass(frozen=True, slots=True)
class Transaction:
    """An immutable transaction event as it flows through the stream.

    transaction_id doubles as the idempotency key used end-to-end (stream
    dedup, fraud scoring, and the Raft log entry derived from it), so every
    stage can detect redelivery/replay without a separate dedup id.
    """

    account_id: str
    transaction_type: TransactionType
    amount: Decimal
    counterparty_account_id: str | None = None
    currency: str = "USD"
    region: str = "us-east"
    status: TransactionStatus = TransactionStatus.PENDING
    transaction_id: str = field(default_factory=new_id)
    created_at: datetime = field(default_factory=utc_now)
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.account_id:
            raise ValueError("account_id must not be empty")
        if self.amount <= 0:
            raise ValueError("amount must be positive; direction is carried by transaction_type")
        if self.transaction_type == TransactionType.TRANSFER and not self.counterparty_account_id:
            raise ValueError("TRANSFER transactions require a counterparty_account_id")

    @property
    def partition_key(self) -> str:
        """All events for a given account must land on the same partition.

        This is what lets a single fraud worker (and later, log replication
        order) see conflicting transactions against the same account in a
        consistent order -- required for stage 2's double-spend guarantee.
        """
        return self.account_id


@dataclass(frozen=True, slots=True)
class FraudScore:
    """Immutable scoring result emitted by a fraud worker for one transaction."""

    transaction_id: str
    worker_id: str
    score: float  # 0.0 (clean) - 1.0 (certain fraud)
    is_fraud: bool
    reasons: tuple[str, ...] = ()
    scored_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not 0.0 <= self.score <= 1.0:
            raise ValueError("score must be between 0.0 and 1.0")


@dataclass(frozen=True, slots=True)
class ScoredTransaction:
    """A transaction paired with its fraud verdict -- what a fraud worker
    hands downstream. The settlement stage needs both: the score to decide
    whether to submit it at all, and the original transaction because
    that's where account_id/amount/type actually live (FraudScore alone
    only carries the verdict, keyed by transaction_id).
    """

    transaction: Transaction
    score: FraudScore


@dataclass(slots=True)
class Account:
    """A point-in-time snapshot of account state.

    The authoritative, mutable balance lives inside the Raft-replicated
    ledger state machine (settlement/); this type is a value object for
    seeding initial state and returning balances to callers, not the
    state machine's internal storage.
    """

    account_id: str
    balance: Decimal
    region: str = "us-east"

    def __post_init__(self) -> None:
        if not self.account_id:
            raise ValueError("account_id must not be empty")


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    """The settlement command applied to the ledger state machine.

    This is the payload Raft's own log entry (term, index, command) wraps
    once consensus is reached -- it is not itself a Raft log entry. `entry_id`
    defaults to the source transaction_id so the state machine can dedupe
    re-applied entries after leader failover (exactly-once apply).
    """

    transaction_id: str
    account_id: str
    delta: Decimal  # signed: negative for debit, positive for credit
    entry_id: str = ""
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.entry_id:
            object.__setattr__(self, "entry_id", self.transaction_id)
        if self.delta == 0:
            raise ValueError("delta must be non-zero")
