"""Drives one RaftNode for real: a background thread, real wall-clock time,
and a real (or in-process fake) Transport.

raft/node.py itself holds no lock and assumes single-threaded, externally
sequenced access -- that's exactly what makes raft/tests/harness.Cluster
able to single-step it deterministically. Running for real introduces a
genuinely concurrent caller: something outside the driver thread (a
settlement submitter) wants to call propose(). RaftNodeRuntime is what
supplies the missing serialization, via one lock shared between the driver
loop's own tick()/receive() calls and any external propose() call. The core
algorithm in raft/node.py does not change or gain any locking of its own.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from raft.node import RaftNode
from raft.transport import Envelope, Transport
from raft.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    PreVoteArgs,
    PreVoteReply,
    RequestVoteArgs,
    RequestVoteReply,
)


class RaftNodeRuntime:
    def __init__(self, node: RaftNode, transport: Transport, tick_interval: float = 0.01):
        self.node = node
        self._transport = transport
        self._tick_interval = tick_interval
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def node_id(self) -> str:
        return self.node.node_id

    def start(self) -> None:
        with self._lock:
            self.node.start(time.monotonic())
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=f"raft-{self.node.node_id}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def propose(self, command: Any) -> LogEntry | None:
        with self._lock:
            return self.node.propose(command, time.monotonic())

    # -- thread-safe passthroughs for a real (e.g. gRPC) transport --------------------
    #
    # A gRPC unary RPC handler needs to call directly into the node and return its
    # reply synchronously as the RPC response (see coordinator/grpc/raft_service.py);
    # it can't go through node.receive()/Transport.send(), which model *outbound*
    # fire-and-forget messaging, not a request a server is currently answering. These
    # give that caller the same lock-serialized access the driver loop uses.

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        with self._lock:
            return self.node.handle_request_vote(args, time.monotonic())

    def handle_prevote(self, args: PreVoteArgs) -> PreVoteReply:
        with self._lock:
            return self.node.handle_prevote(args, time.monotonic())

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        with self._lock:
            return self.node.handle_append_entries(args, time.monotonic())

    def receive(self, envelope: Envelope) -> None:
        """Inject an asynchronously-arrived message (typically a reply to an
        outbound call this node made) from outside the driver loop."""
        with self._lock:
            self.node.receive(envelope, time.monotonic())

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self.node.is_leader

    @property
    def current_term(self) -> int:
        with self._lock:
            return self.node.current_term

    @property
    def leader_id(self) -> str | None:
        with self._lock:
            return self.node.leader_id

    @property
    def commit_index(self) -> int:
        with self._lock:
            return self.node.commit_index

    def _run(self) -> None:
        while not self._stop_event.is_set():
            now = time.monotonic()
            with self._lock:
                self.node.tick(now)
                self._transport.deliver_to(self.node.node_id, lambda env: self.node.receive(env, now))
            time.sleep(self._tick_interval)
