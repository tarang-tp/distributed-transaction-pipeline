# Distributed Transaction Processing Pipeline

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Tests](https://img.shields.io/badge/tests-74%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A distributed system combining real-time fraud detection with consensus-based transaction settlement. Transactions flow through a partitioned stream, get scored for fraud in parallel by worker nodes, and clean transactions are settled through a Raft-replicated ledger that prevents double-spending, including across leader failures.

Built from scratch: the partitioned log, the consumer-group rebalancing protocol, and the Raft consensus algorithm (including a PreVote extension beyond the base paper). No Kafka, no `raft` library, the goal was to understand and prove correct the actual mechanics behind those tools, not just call an API.

**Stack:** Python, gRPC + Protocol Buffers, pytest, Docker / Docker Compose

## Quickstart

```bash
git clone https://github.com/tarang-tp/distributed-transaction-pipeline.git
cd distributed-transaction-pipeline
pip install -r requirements-dev.txt
pytest                              # ~74 tests, in-process + real gRPC-over-localhost
python scripts/demo_kill_leader.py  # kill the Raft leader mid-transaction, watch it recover
```

## Architecture

```
                    ┌─────────────────────┐
  Transaction  ───▶ │  Partitioned Log     │   partition key = account_id
  (producer)        │  (streaming/)         │   (same account always → same partition,
                    └──────────┬───────────┘    same worker, ordered)
                               │
                    ┌──────────▼───────────┐
                    │  Fraud Worker Pool    │   consumer group: rebalances
                    │  (fraud/)              │   partitions automatically if
                    │  rules-based scorer    │   a worker dies (missed heartbeat)
                    └──────────┬───────────┘
                               │ cleared transactions only
                    ┌──────────▼───────────┐
                    │  Settlement Submitter │
                    │  (settlement/)         │
                    └──────────┬───────────┘
                               │ propose(LedgerEntry)
                    ┌──────────▼───────────┐
                    │   Raft Cluster        │   leader election, log replication,
                    │   (raft/)              │   idempotent apply to ledger state
                    │   3–5 nodes            │   machine (account balances)
                    └───────────────────────┘

     Coordinator (coordinator/) spins up fraud workers + Raft nodes,
     monitors health via heartbeats, triggers rebalancing on failure.
```

## Key properties

- **Leader election safety**: at most one leader per term, verified across 200 randomized seeds in a simulated test harness.
- **No double-spend**: two conflicting transactions on the same account are forced into one agreed order by consensus; the state machine deterministically accepts one and rejects the other on every replica.
- **Leader-kill recovery**: killing the Raft leader mid-transaction triggers a new election and outstanding work settles correctly, no lost or duplicated funds. `commitIndex`/`lastApplied` are never persisted, and the ledger's `apply()` is idempotent on `entry_id`, so replaying a node's committed log after a crash never double-counts.
- **Fraud-worker rebalancing**: killing a fraud worker mid-stream causes the consumer group to detect the missed heartbeat and reassign its partitions; no transaction is dropped.
- **PreVote extension**: the base Raft paper allows a partitioned node to spin its term up indefinitely while isolated, which disrupts a healthy cluster the moment it reconnects. Fixed with a PreVote phase (`raft/node.py`), the same approach etcd/raft uses in production.

## Repo layout

```
common/        Shared domain models (Transaction, Account, LedgerEntry, FraudScore) — stdlib only
streaming/     Self-built partitioned log: partitioner, broker, producer/consumer, consumer groups
fraud/         Rules-based fraud scorer + backpressure-aware worker
raft/          Raft core (election, replication, PreVote) + its own deterministic test harness
settlement/    Ledger state machine (idempotent apply, insufficient-funds rejection) + submitter
coordinator/   Real-time orchestration: threaded runtimes, health monitoring, gRPC transport/servicer
proto/         raft.proto — the real gRPC Raft RPC definitions
docker/        Per-node Dockerfiles + entrypoints + docker-compose.yml (5 raft nodes + coordinator)
chaos/         Fault-injection helpers used by the demo scripts
scripts/       The three runnable demo scenarios
tests/         ~70 tests across every layer
```

## Demo scenarios

```bash
python scripts/demo_fraud_burst.py               # burst of fraud-shaped transactions under load, catch rate
python scripts/demo_conflicting_transactions.py   # two concurrent debits, same account, no double-spend
python scripts/demo_kill_leader.py                # kill the Raft leader mid-transaction, verify recovery
```

## Running it

```bash
pip install -r requirements-dev.txt
pytest                              # ~74 tests, in-process + real gRPC-over-localhost
python scripts/demo_kill_leader.py  # etc.
```

Multi-container deployment (5 Raft nodes and a coordinator, each its own container, over real gRPC):

```bash
docker compose up --build
```

## Design notes

**Raft core is deterministic and side-effect-free.** `raft/node.py` has no threads, sleeps, or sockets, just `tick(now)` and `handle_*(args, now)`, driven externally. This allows election safety and failure recovery to be tested deterministically (`raft/tests/harness.py` single-steps a whole simulated cluster) before any real concurrency is introduced. The same `RaftNode` class runs in-process (threads + in-memory transport) and over real gRPC (`coordinator/grpc/`) with no changes to the algorithm.

**Partitioning by `account_id`** guarantees every transaction for a given account is seen by exactly one fraud worker, in order, so per-account velocity checks need no shared state. It also determines the Raft log ordering that prevents double-spends.

**At-least-once delivery + idempotent processing = effectively-once**, applied at every hop. Fraud workers commit stream offsets only after scoring (crash → safe reprocessing, deduped by `transaction_id`). The ledger state machine dedupes on `LedgerEntry.entry_id` (crash → safe log replay).

## Known limitations

- Only the Raft layer is fully containerized. Fraud workers and the stream broker run in-process, via threads, inside the coordinator container.
- `TRANSFER`-type transactions only settle the source account's leg; the counterparty credit (atomic multi-account commit) isn't implemented.
