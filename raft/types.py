"""Core Raft types: log entries, node states, and RPC messages.

Deliberately domain-agnostic -- LogEntry.command is an opaque payload. The
settlement stage will later put LedgerEntry values in there, but raft/ has
no idea what a ledger is; it only knows how to replicate and commit an
ordered log.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass(frozen=True, slots=True)
class LogEntry:
    term: int
    index: int
    command: Any


@dataclass(frozen=True, slots=True)
class PreVoteArgs:
    """Phase 0 of election: 'would you vote for me if I actually ran?'

    Sent at term current_term+1 WITHOUT persisting anything or leaving
    Follower state. This is what stops a node that was partitioned (and so
    kept incrementing its own term in a futile, un-winnable election loop)
    from disrupting a healthy cluster the moment it reconnects: peers that
    are happily following a live leader simply refuse to engage, so the
    stale node never gets to bump its real term and force everyone else to
    step down.
    """

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True, slots=True)
class PreVoteReply:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass(frozen=True, slots=True)
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True, slots=True)
class RequestVoteReply:
    term: int
    vote_granted: bool
    voter_id: str


@dataclass(frozen=True, slots=True)
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True, slots=True)
class AppendEntriesReply:
    term: int
    success: bool
    responder_id: str
    # On success: prev_log_index + len(entries) from the request this replies
    # to, i.e. the last index the follower now has matching the leader. Self
    # describing on purpose, so the leader doesn't need to correlate replies
    # with in-flight requests to know how far match_index should advance.
    match_index: int = 0
    # Fast backtracking (conflict optimization from the Raft paper, ch 5.3):
    # lets the leader jump nextIndex back to the start of the conflicting
    # term instead of decrementing one index per rejected AppendEntries.
    conflict_index: int = 0
    conflict_term: int = -1
