"""Container entrypoint for the coordinator: runs the stream + fraud +
settlement pipeline in this container, talking to a Raft cluster running as
SEPARATE containers over real gRPC (coordinator/grpc/raft_cluster_client.py).

Env vars:
  RAFT_ADDRESSES     -- "node_id=host:port,node_id=host:port,..." for every
                        raft node container
  NUM_PARTITIONS      -- stream partition count (default 4)
  NUM_FRAUD_WORKERS    -- in-process fraud worker count (default 3)

Continuously submits a light stream of sample transactions (mostly clean,
occasionally fraud-shaped) so `docker logs` shows real end-to-end activity:
transactions flowing through the stream, getting scored, and settling
through the real distributed Raft cluster.

Run from the repo root: python docker/coordinator_entrypoint.py
"""

from __future__ import annotations

import os
import random
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.models import Transaction, TransactionType  # noqa: E402
from coordinator.coordinator import Coordinator  # noqa: E402
from coordinator.grpc.raft_cluster_client import GrpcRaftClusterClient  # noqa: E402


def parse_address_map(value: str) -> dict[str, str]:
    addresses: dict[str, str] = {}
    for pair in value.split(","):
        pair = pair.strip()
        if not pair:
            continue
        node_id, _, address = pair.partition("=")
        if not node_id or not address:
            raise ValueError(f"malformed address entry {pair!r}; expected node_id=host:port")
        addresses[node_id] = address
    if not addresses:
        raise ValueError(f"no raft addresses parsed from {value!r}")
    return addresses


def main() -> None:
    raft_addresses = parse_address_map(os.environ["RAFT_ADDRESSES"])
    num_partitions = int(os.environ.get("NUM_PARTITIONS", "4"))
    num_fraud_workers = int(os.environ.get("NUM_FRAUD_WORKERS", "3"))

    raft_client = GrpcRaftClusterClient(raft_addresses)
    accounts = [f"acct-{i}" for i in range(10)]

    coordinator = Coordinator(
        num_partitions=num_partitions,
        num_fraud_workers=num_fraud_workers,
        raft_cluster=raft_client,
    )
    print(f"[coordinator] starting, raft peers={list(raft_addresses)}", flush=True)
    coordinator.start()

    stop = {"flag": False}

    def handle_signal(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        while not stop["flag"]:
            account = random.choice(accounts)
            is_fraud_shaped = random.random() < 0.1
            amount = Decimal("25000") if is_fraud_shaped else Decimal(str(random.randint(5, 200)))
            coordinator.submit_transaction(
                Transaction(account_id=account, transaction_type=TransactionType.DEBIT, amount=amount)
            )
            time.sleep(0.5)
            print(f"[coordinator] status={coordinator.status()}", flush=True)
    finally:
        print("[coordinator] shutting down", flush=True)
        coordinator.stop()


if __name__ == "__main__":
    main()
