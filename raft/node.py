"""RaftNode: a deterministic, side-effect-free-except-for-storage Raft core.

No threads, no sleeps, no sockets in this file. All timing is driven by an
externally supplied `now` (a plain float; the harness or a production timer
thread owns the clock) and all message delivery goes through an injected
Transport. This is what makes exhaustive, fast, non-flaky testing of
election safety and log replication possible: a test harness single-steps
the whole cluster's clock and message delivery deterministically. The same
node logic runs in production behind a real gRPC server and a timer thread;
nothing here changes for that swap.
"""

from __future__ import annotations

import random
from typing import Any, Callable

from raft.state_machine import StateMachine
from raft.storage import Storage
from raft.transport import Envelope, InMemoryTransport
from raft.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    NodeState,
    PreVoteArgs,
    PreVoteReply,
    RequestVoteArgs,
    RequestVoteReply,
)


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        storage: Storage,
        transport: InMemoryTransport,
        state_machine: StateMachine,
        election_timeout_min: float = 150.0,
        election_timeout_max: float = 300.0,
        heartbeat_interval: float = 50.0,
        random_fn: Callable[[float, float], float] = random.uniform,
    ):
        self.node_id = node_id
        self.peer_ids = list(peer_ids)
        self._storage = storage
        self._transport = transport
        self._state_machine = state_machine
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval
        self._random = random_fn

        self.state = NodeState.FOLLOWER
        self.leader_id: str | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._votes_received: set[str] = set()
        self._prevotes_received: set[str] = set()
        self._prevote_target_term = -1
        self._election_deadline = 0.0
        self._next_heartbeat_deadline = float("inf")
        # Tracks when we last heard a valid AppendEntries from a recognized
        # leader -- deliberately separate from _election_deadline, which
        # _start_prevote() itself resets for retry backoff. Conflating the
        # two would mean the first node to notice a dead leader immediately
        # blocks every other node's prevote too.
        self._last_leader_contact = float("-inf")

        transport.register(node_id)

    # -- public read-only helpers -------------------------------------------------

    @property
    def current_term(self) -> int:
        return self._storage.get_current_term()

    @property
    def is_leader(self) -> bool:
        return self.state == NodeState.LEADER

    @property
    def log(self) -> list[LogEntry]:
        return self._storage.entries_from(1)

    def start(self, now: float) -> None:
        """Arm the initial election timeout. Call once before the first tick."""
        self._reset_election_deadline(now)

    # -- driving the node: time and messages ---------------------------------------

    def tick(self, now: float) -> None:
        if self.state == NodeState.LEADER:
            if now >= self._next_heartbeat_deadline:
                self._broadcast_append_entries(now)
        else:
            if now >= self._election_deadline:
                self._start_prevote(now)

    def receive(self, envelope: Envelope, now: float) -> None:
        message = envelope.message
        sender_id = envelope.sender_id
        if isinstance(message, RequestVoteArgs):
            reply = self.handle_request_vote(message, now)
            self._transport.send(self.node_id, sender_id, reply)
        elif isinstance(message, RequestVoteReply):
            self._handle_request_vote_reply(message, now)
        elif isinstance(message, AppendEntriesArgs):
            reply = self.handle_append_entries(message, now)
            self._transport.send(self.node_id, sender_id, reply)
        elif isinstance(message, AppendEntriesReply):
            self._handle_append_entries_reply(sender_id, message, now)
        elif isinstance(message, PreVoteArgs):
            reply = self.handle_prevote(message, now)
            self._transport.send(self.node_id, sender_id, reply)
        elif isinstance(message, PreVoteReply):
            self._handle_prevote_reply(message, now)
        else:
            raise TypeError(f"unknown message type: {type(message)!r}")

    # -- client-facing API (leader only) --------------------------------------------

    def propose(self, command: Any, now: float) -> LogEntry | None:
        if self.state != NodeState.LEADER:
            return None
        entry = LogEntry(term=self.current_term, index=self._storage.last_index() + 1, command=command)
        self._storage.append([entry])
        self._broadcast_append_entries(now)
        return entry

    # -- RPC handlers ----------------------------------------------------------------

    def handle_prevote(self, args: PreVoteArgs, now: float) -> PreVoteReply:
        current_term = self._storage.get_current_term()
        # Refuse outright if we believe a leader is currently active (we
        # haven't hit our own election timeout since last hearing from one).
        # This is the crux of the fix: a node that was partitioned and spun
        # its term up while isolated can never get past this check against
        # peers still happily following the real leader, so it can't force
        # a disruptive step-down merely by reconnecting.
        if self.leader_id is not None and (now - self._last_leader_contact) < self._election_timeout_min:
            return PreVoteReply(term=current_term, vote_granted=False, voter_id=self.node_id)
        if args.term <= current_term:
            return PreVoteReply(term=current_term, vote_granted=False, voter_id=self.node_id)
        if not self._log_up_to_date(args.last_log_index, args.last_log_term):
            return PreVoteReply(term=current_term, vote_granted=False, voter_id=self.node_id)
        return PreVoteReply(term=current_term, vote_granted=True, voter_id=self.node_id)

    def handle_request_vote(self, args: RequestVoteArgs, now: float) -> RequestVoteReply:
        current_term = self._storage.get_current_term()
        if args.term > current_term:
            self._become_follower(args.term, now)
            current_term = args.term
        if args.term < current_term:
            return RequestVoteReply(term=current_term, vote_granted=False, voter_id=self.node_id)

        voted_for = self._storage.get_voted_for()
        can_vote = voted_for is None or voted_for == args.candidate_id
        log_ok = self._log_up_to_date(args.last_log_index, args.last_log_term)
        if can_vote and log_ok:
            self._storage.set_voted_for(args.candidate_id)
            self._reset_election_deadline(now)
            return RequestVoteReply(term=current_term, vote_granted=True, voter_id=self.node_id)
        return RequestVoteReply(term=current_term, vote_granted=False, voter_id=self.node_id)

    def handle_append_entries(self, args: AppendEntriesArgs, now: float) -> AppendEntriesReply:
        current_term = self._storage.get_current_term()
        if args.term > current_term:
            self._become_follower(args.term, now)
            current_term = args.term
        if args.term < current_term:
            return AppendEntriesReply(term=current_term, success=False, responder_id=self.node_id)

        # args.term == current_term: this is a valid leader for our current term
        self.state = NodeState.FOLLOWER
        self.leader_id = args.leader_id
        self._last_leader_contact = now
        self._reset_election_deadline(now)

        if args.prev_log_index > 0:
            term_at_prev = self._storage.term_at(args.prev_log_index)
            if term_at_prev == -1:
                return AppendEntriesReply(
                    term=current_term,
                    success=False,
                    responder_id=self.node_id,
                    conflict_index=self._storage.last_index() + 1,
                    conflict_term=-1,
                )
            if term_at_prev != args.prev_log_term:
                conflict_index = self._first_index_of_term(term_at_prev, args.prev_log_index)
                return AppendEntriesReply(
                    term=current_term,
                    success=False,
                    responder_id=self.node_id,
                    conflict_index=conflict_index,
                    conflict_term=term_at_prev,
                )

        insert_at = args.prev_log_index + 1
        for offset, entry in enumerate(args.entries):
            idx = insert_at + offset
            existing_term = self._storage.term_at(idx)
            if existing_term == -1:
                self._storage.append(list(args.entries[offset:]))
                break
            if existing_term != entry.term:
                self._storage.truncate_from(idx)
                self._storage.append(list(args.entries[offset:]))
                break
        # if the loop completes without breaking, every sent entry was already
        # present and matched -- a duplicate/retried heartbeat, nothing to do

        if args.leader_commit > self.commit_index:
            self.commit_index = min(args.leader_commit, self._storage.last_index())
            self._apply_committed()

        match_index = args.prev_log_index + len(args.entries)
        return AppendEntriesReply(
            term=current_term, success=True, responder_id=self.node_id, match_index=match_index
        )

    # -- reply handlers (leader/candidate side) ---------------------------------------

    def _handle_prevote_reply(self, reply: PreVoteReply, now: float) -> None:
        if reply.term > self._storage.get_current_term():
            self._become_follower(reply.term, now)
            return
        # Only count replies for the prevote round we're still actually in;
        # this naturally goes stale (and is ignored) the moment we become a
        # real candidate, hear from a leader, or start a new prevote round,
        # since self._prevote_target_term stops equalling current_term + 1.
        if self.state != NodeState.FOLLOWER or self._prevote_target_term != self.current_term + 1:
            return
        if reply.vote_granted:
            self._prevotes_received.add(reply.voter_id)
            self._maybe_win_prevote(now)

    def _handle_request_vote_reply(self, reply: RequestVoteReply, now: float) -> None:
        if reply.term > self._storage.get_current_term():
            self._become_follower(reply.term, now)
            return
        if self.state != NodeState.CANDIDATE or reply.term != self._storage.get_current_term():
            return  # stale reply from a past term or a since-abandoned election
        if reply.vote_granted:
            self._votes_received.add(reply.voter_id)
            self._maybe_become_leader_from_votes(now)

    def _handle_append_entries_reply(self, sender_id: str, reply: AppendEntriesReply, now: float) -> None:
        if reply.term > self._storage.get_current_term():
            self._become_follower(reply.term, now)
            return
        if self.state != NodeState.LEADER or reply.term != self._storage.get_current_term():
            return  # stale reply

        if reply.success:
            if reply.match_index > self.match_index.get(sender_id, 0):
                self.match_index[sender_id] = reply.match_index
            self.next_index[sender_id] = self.match_index[sender_id] + 1
            self._advance_commit_index()
        else:
            if reply.conflict_term != -1:
                last_idx = self._last_index_of_term(reply.conflict_term)
                self.next_index[sender_id] = last_idx + 1 if last_idx > 0 else reply.conflict_index
            else:
                self.next_index[sender_id] = max(1, reply.conflict_index)
            self._send_append_entries_to(sender_id, now)  # retry immediately with corrected nextIndex

    # -- state transitions --------------------------------------------------------------

    def _become_follower(self, term: int, now: float) -> None:
        self._storage.set_current_term(term)
        self._storage.set_voted_for(None)
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self._votes_received.clear()
        self._reset_election_deadline(now)

    def _start_prevote(self, now: float) -> None:
        # Pre-candidate phase is represented as FOLLOWER (no dedicated enum
        # value) so a retry from a failed real candidacy must explicitly
        # step back down -- otherwise _handle_prevote_reply's own state
        # guard rejects the replies to this very round and the retry can
        # never graduate to a real election again.
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self._prevote_target_term = self.current_term + 1
        self._prevotes_received = {self.node_id}
        self._reset_election_deadline(now)  # avoid re-firing prevote every tick while replies are pending
        args = PreVoteArgs(
            term=self._prevote_target_term,
            candidate_id=self.node_id,
            last_log_index=self._storage.last_index(),
            last_log_term=self._storage.last_term(),
        )
        for peer in self.peer_ids:
            self._transport.send(self.node_id, peer, args)
        self._maybe_win_prevote(now)  # handles single-node clusters (majority of 1)

    def _maybe_win_prevote(self, now: float) -> None:
        majority = (len(self.peer_ids) + 1) // 2 + 1
        if len(self._prevotes_received) >= majority:
            self._become_candidate(now)  # graduate to a real election: bump term for real

    def _become_candidate(self, now: float) -> None:
        new_term = self._storage.get_current_term() + 1
        self._storage.set_current_term(new_term)
        self._storage.set_voted_for(self.node_id)
        self.state = NodeState.CANDIDATE
        self.leader_id = None
        self._votes_received = {self.node_id}
        self._reset_election_deadline(now)
        self._broadcast_request_vote(now)
        self._maybe_become_leader_from_votes(now)  # handles single-node clusters

    def _become_leader(self, now: float) -> None:
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        last_index = self._storage.last_index()
        self.next_index = {peer: last_index + 1 for peer in self.peer_ids}
        self.match_index = {peer: 0 for peer in self.peer_ids}
        self._broadcast_append_entries(now)  # authority-establishing heartbeat

    def _maybe_become_leader_from_votes(self, now: float) -> None:
        if self.state != NodeState.CANDIDATE:
            return
        majority = (len(self.peer_ids) + 1) // 2 + 1
        if len(self._votes_received) >= majority:
            self._become_leader(now)

    # -- commit index / apply -----------------------------------------------------------

    def _advance_commit_index(self) -> None:
        match_indexes = sorted([self._storage.last_index(), *self.match_index.values()], reverse=True)
        majority = (len(self.peer_ids) + 1) // 2 + 1
        candidate_n = match_indexes[majority - 1]
        # Safety (Raft paper 5.4.2): a leader only commits by counting
        # replicas for entries from its OWN current term. Committing an
        # earlier-term entry just because it's now on a majority of nodes is
        # unsafe -- it can be overwritten by a future leader that never saw
        # it get "committed". Such entries become committed indirectly once
        # a later same-term entry commits on top of them.
        if candidate_n > self.commit_index and self._storage.term_at(candidate_n) == self.current_term:
            self.commit_index = candidate_n
            self._apply_committed()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self._storage.get(self.last_applied)
            assert entry is not None
            self._state_machine.apply(entry.command)

    # -- broadcasting --------------------------------------------------------------------

    def _broadcast_request_vote(self, now: float) -> None:
        args = RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self._storage.last_index(),
            last_log_term=self._storage.last_term(),
        )
        for peer in self.peer_ids:
            self._transport.send(self.node_id, peer, args)

    def _broadcast_append_entries(self, now: float) -> None:
        self._next_heartbeat_deadline = now + self._heartbeat_interval
        for peer in self.peer_ids:
            self._send_append_entries_to(peer, now)

    def _send_append_entries_to(self, peer: str, now: float) -> None:
        next_idx = self.next_index.get(peer, self._storage.last_index() + 1)
        prev_log_index = next_idx - 1
        prev_log_term = self._storage.term_at(prev_log_index)
        entries = tuple(self._storage.entries_from(next_idx))
        args = AppendEntriesArgs(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        self._transport.send(self.node_id, peer, args)

    # -- log helpers -----------------------------------------------------------------------

    def _log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        my_last_term = self._storage.last_term()
        my_last_index = self._storage.last_index()
        if last_log_term != my_last_term:
            return last_log_term > my_last_term
        return last_log_index >= my_last_index

    def _first_index_of_term(self, term: int, from_index: int) -> int:
        index = from_index
        while index > 0 and self._storage.term_at(index) == term:
            index -= 1
        return index + 1

    def _last_index_of_term(self, term: int) -> int:
        index = self._storage.last_index()
        while index > 0:
            t = self._storage.term_at(index)
            if t == term:
                return index
            if t < term:
                return 0
            index -= 1
        return 0

    def _reset_election_deadline(self, now: float) -> None:
        timeout = self._random(self._election_timeout_min, self._election_timeout_max)
        self._election_deadline = now + timeout
