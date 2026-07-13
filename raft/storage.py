"""Persistent Raft state: currentTerm, votedFor, and the log.

Log indices are 1-based, matching the Raft paper: index 0 is an implicit
"nothing here yet" entry with term 0, which is what lets prevLogIndex=0 /
prevLogTerm=0 work as the boundary case for a brand-new log without special
casing every call site.

InMemoryStorage is what the test harness uses -- "restarting" a node in a
test just means constructing a new RaftNode around the same InMemoryStorage
instance, which is a faithful simulation of a real process restarting and
reloading its persisted state from disk. A real deployment (stage 5/6,
gRPC + Docker) will get a disk-backed Storage implementation with the same
interface; nothing in RaftNode needs to change for that swap.
"""

from __future__ import annotations

from threading import RLock
from typing import Protocol

from raft.types import LogEntry


class Storage(Protocol):
    def get_current_term(self) -> int: ...
    def set_current_term(self, term: int) -> None: ...
    def get_voted_for(self) -> str | None: ...
    def set_voted_for(self, candidate_id: str | None) -> None: ...
    def last_index(self) -> int: ...
    def last_term(self) -> int: ...
    def term_at(self, index: int) -> int: ...
    def get(self, index: int) -> LogEntry | None: ...
    def entries_from(self, index: int) -> list[LogEntry]: ...
    def append(self, entries: list[LogEntry]) -> None: ...
    def truncate_from(self, index: int) -> None: ...


class InMemoryStorage:
    def __init__(self) -> None:
        self._current_term = 0
        self._voted_for: str | None = None
        self._log: list[LogEntry] = []  # self._log[i] has index i + 1
        self._lock = RLock()

    def get_current_term(self) -> int:
        with self._lock:
            return self._current_term

    def set_current_term(self, term: int) -> None:
        with self._lock:
            self._current_term = term

    def get_voted_for(self) -> str | None:
        with self._lock:
            return self._voted_for

    def set_voted_for(self, candidate_id: str | None) -> None:
        with self._lock:
            self._voted_for = candidate_id

    def last_index(self) -> int:
        with self._lock:
            return len(self._log)

    def last_term(self) -> int:
        with self._lock:
            return self._log[-1].term if self._log else 0

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        with self._lock:
            if 1 <= index <= len(self._log):
                return self._log[index - 1].term
            return -1  # no such entry

    def get(self, index: int) -> LogEntry | None:
        if index <= 0:
            return None
        with self._lock:
            if index > len(self._log):
                return None
            return self._log[index - 1]

    def entries_from(self, index: int) -> list[LogEntry]:
        if index <= 0:
            index = 1
        with self._lock:
            return list(self._log[index - 1 :])

    def append(self, entries: list[LogEntry]) -> None:
        with self._lock:
            self._log.extend(entries)

    def truncate_from(self, index: int) -> None:
        if index <= 0:
            raise ValueError("index must be positive")
        with self._lock:
            del self._log[index - 1 :]
