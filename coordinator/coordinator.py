"""Top-level orchestrator: spins up fraud workers and a Raft cluster, wires
fraud clearance into settlement, and periodically checks fraud-worker
liveness to trigger consumer-group rebalancing on failure.

Raft node liveness is a different story on purpose: Raft already self-heals
via leader election (raft/node.py's PreVote + election safety, proven in
raft/tests/), so there's no "reassignment" step for it here -- the
coordinator just exposes cluster health via status().

By default this owns an in-process RaftCluster (RaftNodeRuntimes sharing an
InMemoryTransport in this same process) -- what every demo script and most
tests use. Passing raft_cluster explicitly (e.g. a GrpcRaftClusterClient
pointed at separate raft-node containers, see coordinator/grpc/) switches
to a real distributed cluster the coordinator doesn't own the lifecycle of;
SettlementSubmitter only needs propose(), so it works unmodified either
way. balance()/full status() are in-process-only conveniences -- a remote
cluster has no local replica state for this process to read directly.
"""

from __future__ import annotations

import queue
import threading
import time
from decimal import Decimal
from typing import Any

from common.models import ScoredTransaction, Transaction
from coordinator.fraud_runtime import FraudWorkerRuntime
from coordinator.raft_cluster import RaftCluster
from fraud.scorer import RuleBasedScorer, ScorerConfig
from fraud.worker import FraudWorker
from settlement.ledger_state_machine import LedgerStateMachine
from settlement.submitter import SettlementSubmitter
from streaming.broker import PartitionedLog
from streaming.group import ConsumerGroup
from streaming.producer import Producer


class Coordinator:
    def __init__(
        self,
        num_partitions: int = 4,
        num_fraud_workers: int = 3,
        raft_node_ids: list[str] | None = None,
        initial_balances: dict[str, Decimal] | None = None,
        fraud_session_timeout: float = 1.0,
        health_check_interval: float = 0.2,
        scorer_config: ScorerConfig | None = None,
        raft_cluster: Any | None = None,
    ):
        self.log = PartitionedLog("transactions", num_partitions=num_partitions)
        self.producer = Producer(self.log)
        self.group = ConsumerGroup(
            "fraud-workers", num_partitions=num_partitions, session_timeout_seconds=fraud_session_timeout
        )
        self.output_queue: "queue.Queue[ScoredTransaction]" = queue.Queue(maxsize=1000)
        self._scorer_config = scorer_config or ScorerConfig()

        self.fraud_runtimes: dict[str, FraudWorkerRuntime] = {}
        for i in range(num_fraud_workers):
            self._spawn_fraud_worker(f"fraud-{i}")

        self._owns_raft_cluster = raft_cluster is None
        if raft_cluster is None:
            raft_node_ids = raft_node_ids or ["raft-1", "raft-2", "raft-3"]
            self._initial_balances = dict(initial_balances or {})
            self.raft_cluster = RaftCluster(
                raft_node_ids,
                state_machine_factory=lambda: LedgerStateMachine(dict(self._initial_balances)),
            )
        else:
            self.raft_cluster = raft_cluster
        self.submitter = SettlementSubmitter(self.output_queue, self.raft_cluster)

        self._health_check_interval = health_check_interval
        self._stop_event = threading.Event()
        self._health_thread: threading.Thread | None = None

    def _spawn_fraud_worker(self, member_id: str) -> None:
        scorer = RuleBasedScorer(member_id, config=self._scorer_config)
        worker = FraudWorker(member_id, self.log, self.group, scorer, self.output_queue)
        self.fraud_runtimes[member_id] = FraudWorkerRuntime(worker)

    def start(self) -> None:
        if self._owns_raft_cluster:
            self.raft_cluster.start()
        self.submitter.start()
        for runtime in self.fraud_runtimes.values():
            runtime.start()
        self._stop_event.clear()
        self._health_thread = threading.Thread(
            target=self._health_loop, name="coordinator-health", daemon=True
        )
        self._health_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._health_thread is not None:
            self._health_thread.join(timeout=2.0)
            self._health_thread = None
        for runtime in self.fraud_runtimes.values():
            runtime.stop()
        self.submitter.stop()
        if self._owns_raft_cluster:
            self.raft_cluster.stop()

    def submit_transaction(self, transaction: Transaction) -> None:
        self.producer.produce(transaction.partition_key, transaction)

    def kill_fraud_worker(self, member_id: str) -> None:
        # No graceful group.leave(): this is meant to look like a crash, so
        # the group only notices via a missed heartbeat, same as a real dead
        # process would.
        self.fraud_runtimes[member_id].stop()

    def revive_fraud_worker(self, member_id: str) -> None:
        self._spawn_fraud_worker(member_id)
        self.fraud_runtimes[member_id].start()

    def balance(self, account_id: str, timeout: float = 2.0) -> Decimal:
        if not self._owns_raft_cluster:
            raise NotImplementedError(
                "balance() reads local replica state and only works against an "
                "in-process RaftCluster; a remote cluster has no state this "
                "process can read directly -- query a raft node's own status "
                "endpoint instead."
            )
        leader = self.raft_cluster.find_leader(timeout=timeout)
        if leader is None:
            raise TimeoutError("no Raft leader available")
        sm: LedgerStateMachine = self.raft_cluster.state_machines[leader.node_id]
        return sm.balance(account_id)

    def status(self) -> dict:
        return {
            "raft": self.raft_cluster.snapshot() if self._owns_raft_cluster else {"mode": "remote"},
            "fraud_partition_assignment": {m: self.group.assignment_for(m) for m in self.fraud_runtimes},
            "submitted_count": self.submitter.submitted_count,
            "fraud_flagged_count": self.submitter.fraud_flagged_count,
            "failed_transaction_ids": list(self.submitter.failed_transaction_ids),
        }

    def _health_loop(self) -> None:
        while not self._stop_event.is_set():
            self.group.check_expired_members()
            time.sleep(self._health_check_interval)
