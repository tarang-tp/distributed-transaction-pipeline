"""A real, running Raft cluster: N RaftNodeRuntimes sharing one transport,
plus a client-facing propose() that finds the current leader and retries
across leadership changes. This is the production counterpart to
raft/tests/harness.Cluster -- same RaftNode class, real threads and
wall-clock time instead of a simulated clock, and InMemoryTransport used as
an in-process network rather than a hand-steppable fake.
"""

from __future__ import annotations

import time
from typing import Any, Callable

from coordinator.raft_runtime import RaftNodeRuntime
from raft.node import RaftNode
from raft.state_machine import StateMachine
from raft.storage import InMemoryStorage
from raft.transport import InMemoryTransport
from raft.types import LogEntry


class RaftCluster:
    def __init__(
        self,
        node_ids: list[str],
        state_machine_factory: Callable[[], StateMachine],
        election_timeout_min: float = 0.15,
        election_timeout_max: float = 0.3,
        heartbeat_interval: float = 0.05,
        tick_interval: float = 0.01,
    ):
        self.transport = InMemoryTransport()
        self.storages: dict[str, InMemoryStorage] = {nid: InMemoryStorage() for nid in node_ids}
        self.state_machines: dict[str, StateMachine] = {nid: state_machine_factory() for nid in node_ids}
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval
        self._tick_interval = tick_interval
        self.runtimes: dict[str, RaftNodeRuntime] = {nid: self._build_runtime(nid) for nid in node_ids}
        self._last_known_leader: str | None = None

    def _build_runtime(self, node_id: str) -> RaftNodeRuntime:
        peers = [nid for nid in self.storages if nid != node_id]
        node = RaftNode(
            node_id,
            peers,
            self.storages[node_id],
            self.transport,
            self.state_machines[node_id],
            self._election_timeout_min,
            self._election_timeout_max,
            self._heartbeat_interval,
        )
        return RaftNodeRuntime(node, self.transport, self._tick_interval)

    def start(self) -> None:
        for runtime in self.runtimes.values():
            runtime.start()

    def stop(self) -> None:
        for runtime in self.runtimes.values():
            runtime.stop()

    def kill(self, node_id: str) -> None:
        self.runtimes[node_id].stop()
        self.transport.partition(node_id)

    def revive(self, node_id: str) -> None:
        """Simulate a process restart: same persisted storage and state
        machine (both survive a real crash on disk), fresh RaftNodeRuntime.
        """
        self.transport.rejoin(node_id)
        self.runtimes[node_id] = self._build_runtime(node_id)
        self.runtimes[node_id].start()

    def is_alive(self, node_id: str) -> bool:
        return not self.transport.is_partitioned(node_id)

    def find_leader(self, timeout: float = 2.0, poll_interval: float = 0.02) -> RaftNodeRuntime | None:
        deadline = time.monotonic() + timeout
        while True:
            if self._last_known_leader is not None:
                runtime = self.runtimes.get(self._last_known_leader)
                if runtime is not None and self.is_alive(self._last_known_leader) and runtime.is_leader:
                    return runtime
            for node_id, runtime in self.runtimes.items():
                if self.is_alive(node_id) and runtime.is_leader:
                    self._last_known_leader = node_id
                    return runtime
            if time.monotonic() >= deadline:
                return None
            time.sleep(poll_interval)

    def propose(self, command: Any, timeout: float = 2.0) -> LogEntry:
        """Client-facing propose with leader discovery and retry, mirroring
        how a real Raft client handles a 'not the leader' response -- just
        in-process, since caller and cluster share an address space here.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leader = self.find_leader(timeout=max(0.0, deadline - time.monotonic()))
            if leader is None:
                break
            entry = leader.propose(command)
            if entry is not None:
                return entry
            self._last_known_leader = None  # stale cache: leadership changed between find and propose
        raise TimeoutError("no leader available to accept the proposal within timeout")

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {
            node_id: {"is_leader": rt.is_leader, "term": rt.current_term, "leader_id": rt.leader_id}
            for node_id, rt in self.runtimes.items()
            if self.is_alive(node_id)
        }
