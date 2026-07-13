"""A fully controllable fake network for deterministic Raft tests.

Messages are queued per recipient and only become visible to a node when
the harness calls deliver_to(node_id, handler) -- nothing is delivered
automatically or concurrently. This lets tests script exact interleavings
(who hears about an election first, whether a heartbeat beats a competing
RequestVote, etc.) without any real threading or timing.

A real deployment swaps this for a gRPC-backed transport with the same
send() signature; RaftNode never imports this module's internals directly.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Protocol, Union

from raft.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    PreVoteArgs,
    PreVoteReply,
    RequestVoteArgs,
    RequestVoteReply,
)

Message = Union[
    RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply, PreVoteArgs, PreVoteReply
]


@dataclass(frozen=True, slots=True)
class Envelope:
    sender_id: str
    recipient_id: str
    message: Message


class Transport(Protocol):
    """What RaftNode's driver (test harness or a real runtime) needs from a
    network. InMemoryTransport is the only implementation today; a gRPC-backed
    one can implement the same three methods without RaftNode or the runtime
    changing at all.
    """

    def register(self, node_id: str) -> None: ...
    def send(self, sender_id: str, recipient_id: str, message: Message) -> None: ...
    def deliver_to(self, node_id: str, handler: Callable[[Envelope], None]) -> int: ...


class InMemoryTransport:
    def __init__(self) -> None:
        self._inboxes: dict[str, deque[Envelope]] = defaultdict(deque)
        self._partitioned: set[str] = set()

    def register(self, node_id: str) -> None:
        self._inboxes.setdefault(node_id, deque())

    def send(self, sender_id: str, recipient_id: str, message: Message) -> None:
        if sender_id in self._partitioned or recipient_id in self._partitioned:
            return  # dropped silently, exactly like a real network partition
        self._inboxes[recipient_id].append(Envelope(sender_id, recipient_id, message))

    def partition(self, *node_ids: str) -> None:
        self._partitioned.update(node_ids)

    def heal(self) -> None:
        self._partitioned.clear()

    def rejoin(self, *node_ids: str) -> None:
        self._partitioned.difference_update(node_ids)

    def is_partitioned(self, node_id: str) -> bool:
        return node_id in self._partitioned

    def inbox_size(self, node_id: str) -> int:
        return len(self._inboxes[node_id])

    def deliver_to(self, node_id: str, handler: Callable[[Envelope], None]) -> int:
        inbox = self._inboxes[node_id]
        delivered = 0
        while inbox:
            envelope = inbox.popleft()
            handler(envelope)
            delivered += 1
        return delivered
