"""Container entrypoint for one Raft node.

Env vars:
  NODE_ID           -- this node's id, e.g. "raft-1"
  LISTEN_PORT        -- port to bind the gRPC server on, e.g. "50051"
  PEER_ADDRESSES      -- "node_id=host:port,node_id=host:port,..." for every
                         OTHER node (in Compose, host is the service name)
  INITIAL_BALANCES    -- optional "account_id=amount,account_id=amount,..."
                         seed balances for this node's ledger state machine

Run from the repo root: python docker/raft_node_entrypoint.py
"""

from __future__ import annotations

import os
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from coordinator.grpc.node_bootstrap import GrpcRaftNode  # noqa: E402
from raft.storage import InMemoryStorage  # noqa: E402
from settlement.ledger_state_machine import LedgerStateMachine  # noqa: E402


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
        raise ValueError(f"no peer addresses parsed from {value!r}")
    return addresses


def parse_balances(value: str) -> dict[str, Decimal]:
    balances: dict[str, Decimal] = {}
    for pair in value.split(","):
        pair = pair.strip()
        if not pair:
            continue
        account_id, _, amount = pair.partition("=")
        balances[account_id] = Decimal(amount)
    return balances


def main() -> None:
    node_id = os.environ["NODE_ID"]
    listen_port = int(os.environ["LISTEN_PORT"])
    peer_addresses = parse_address_map(os.environ["PEER_ADDRESSES"])
    initial_balances = parse_balances(os.environ.get("INITIAL_BALANCES", ""))

    node = GrpcRaftNode(
        node_id,
        listen_port,
        peer_addresses,
        InMemoryStorage(),
        LedgerStateMachine(initial_balances),
    )
    print(f"[{node_id}] starting on port {listen_port}, peers={list(peer_addresses)}", flush=True)
    node.start()

    stop = {"flag": False}

    def handle_signal(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        while not stop["flag"]:
            time.sleep(1.0)
            print(
                f"[{node_id}] term={node.runtime.current_term} "
                f"leader={node.runtime.is_leader} leader_id={node.runtime.leader_id} "
                f"commit_index={node.runtime.commit_index}",
                flush=True,
            )
    finally:
        print(f"[{node_id}] shutting down", flush=True)
        node.stop()


if __name__ == "__main__":
    main()
