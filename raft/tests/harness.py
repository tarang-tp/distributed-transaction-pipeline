"""Deterministic simulated Raft cluster for tests.

Owns a single logical clock and drives every node's tick() and message
delivery in lockstep, so tests can script exact scenarios (kill this node
mid-replication, partition these two away, heal and observe recovery)
without real threads, real sleeps, or any flakiness.
"""

from __future__ import annotations

import random
from typing import Any, Callable

from raft.node import RaftNode
from raft.state_machine import RecordingStateMachine, StateMachine
from raft.storage import InMemoryStorage
from raft.transport import InMemoryTransport
from raft.types import LogEntry


class Cluster:
    def __init__(
        self,
        node_ids: list[str],
        election_timeout_min: float = 150.0,
        election_timeout_max: float = 300.0,
        heartbeat_interval: float = 50.0,
        seed: int = 0,
        state_machine_factory: Callable[[], StateMachine] = RecordingStateMachine,
    ):
        self.transport = InMemoryTransport()
        self._rng = random.Random(seed)
        self.now = 0.0
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval

        self.storages: dict[str, InMemoryStorage] = {nid: InMemoryStorage() for nid in node_ids}
        # A fresh factory call per node, not one shared instance -- otherwise
        # every "replica" would silently be the same object, masking bugs
        # where nodes disagree (e.g. after a partition heals incorrectly).
        self.state_machines: dict[str, StateMachine] = {nid: state_machine_factory() for nid in node_ids}
        self.nodes: dict[str, RaftNode] = {}
        self._killed: set[str] = set()

        for nid in node_ids:
            self.nodes[nid] = self._build_node(nid)
            self.nodes[nid].start(self.now)

    def _build_node(self, node_id: str) -> RaftNode:
        peers = [nid for nid in self.storages if nid != node_id]
        return RaftNode(
            node_id,
            peers,
            self.storages[node_id],
            self.transport,
            self.state_machines[node_id],
            self._election_timeout_min,
            self._election_timeout_max,
            self._heartbeat_interval,
            random_fn=self._rng.uniform,
        )

    # -- driving the simulation ----------------------------------------------------

    def advance(self, dt: float, steps: int = 1) -> None:
        for _ in range(steps):
            self.now += dt
            self._pump()

    def _pump(self) -> None:
        for nid, node in self.nodes.items():
            if nid in self._killed:
                continue
            node.tick(self.now)
        self._drain_messages()

    def _drain_messages(self) -> None:
        for _ in range(1000):
            delivered_any = False
            for nid, node in self.nodes.items():
                if nid in self._killed:
                    continue
                delivered = self.transport.deliver_to(nid, lambda env, n=node: n.receive(env, self.now))
                delivered_any = delivered_any or delivered > 0
            if not delivered_any:
                return
        raise RuntimeError("message delivery did not quiesce -- likely an infinite reply loop")

    # -- fault injection --------------------------------------------------------------

    def kill(self, node_id: str) -> None:
        self._killed.add(node_id)
        self.transport.partition(node_id)

    def revive(self, node_id: str) -> None:
        """Simulate a process restart: a fresh RaftNode over the SAME
        persisted storage. Volatile state (commit_index, leader_id, votes)
        resets to zero, exactly matching real Raft semantics -- only
        currentTerm, votedFor, and the log survive a crash.
        """
        self._killed.discard(node_id)
        self.transport.rejoin(node_id)
        self.nodes[node_id] = self._build_node(node_id)
        self.nodes[node_id].start(self.now)

    def partition(self, *node_ids: str) -> None:
        self.transport.partition(*node_ids)

    def heal(self) -> None:
        self.transport.heal()

    # -- assertions / queries -----------------------------------------------------

    def assert_election_safety(self) -> None:
        by_term: dict[int, list[str]] = {}
        for nid, node in self.nodes.items():
            if nid in self._killed:
                continue
            if node.is_leader:
                by_term.setdefault(node.current_term, []).append(nid)
        for term, leaders in by_term.items():
            assert len(leaders) <= 1, f"election safety violated: multiple leaders in term {term}: {leaders}"

    def leader(self) -> RaftNode | None:
        self.assert_election_safety()
        live_leaders = [n for nid, n in self.nodes.items() if nid not in self._killed and n.is_leader]
        if not live_leaders:
            return None
        return max(live_leaders, key=lambda n: n.current_term)

    def elect_leader(self, dt: float = 10.0, max_steps: int = 100, min_term: int = 0) -> RaftNode:
        """Advance until a leader exists (and, if min_term is given, until
        its term exceeds min_term). min_term matters whenever a stale
        leader might still be present but unreachable -- e.g. a partitioned
        former leader still believes it's LEADER from tick one, so "a
        leader exists" alone would return it immediately instead of waiting
        for the survivors to actually elect someone new.
        """
        for _ in range(max_steps):
            self.advance(dt)
            leader = self.leader()
            if leader is not None and leader.current_term > min_term:
                return leader
        raise TimeoutError("no leader elected within simulated time budget")

    def propose(self, command: Any) -> LogEntry:
        leader = self.leader() or self.elect_leader()
        entry = leader.propose(command, self.now)
        assert entry is not None, "leader() returned a node that isn't actually the leader"
        return entry

    def settle(self, dt: float = 10.0, rounds: int = 20) -> None:
        """Advance enough rounds for in-flight replication/heartbeats to
        fully propagate to a quiescent state."""
        self.advance(dt, steps=rounds)

    def applied(self, node_id: str) -> list[Any]:
        return list(self.state_machines[node_id].applied)

    def live_node_ids(self) -> list[str]:
        return [nid for nid in self.nodes if nid not in self._killed]
