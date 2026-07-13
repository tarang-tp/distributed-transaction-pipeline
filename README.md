# Distributed Transaction Processing Pipeline

A from-scratch distributed system that combines real-time fraud detection (stream processing) with consensus-based transaction settlement (Raft). Transactions flow through a partitioned stream, get scored for fraud in parallel by worker nodes, and clean transactions are settled through a Raft-replicated ledger that prevents double-spending on conflicting transactions — even across leader failures.

Everything here is built from scratch: the partitioned log, the consumer-group rebalancing protocol, and the Raft consensus algorithm (including a PreVote extension beyond the base paper). No Kafka, no `raft` library.

## Why this exists

This is a systems-design deep dive, not a CRUD app. The goal was to build and prove correct, under real failure injection, the two hardest primitives in a transaction-processing system: **exactly-once-ish delivery under partition rebalancing** and **linearizable settlement under leader failure**.

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

## What's proven, not just implemented

- **Raft leader election safety** — at most one leader per term, verified across 200 randomized seeds in the simulated test harness, not just the handful of hand-picked scenarios in the test files.
- **No double-spend** — two conflicting transactions on the same account are forced into one agreed order by consensus; the state machine deterministically accepts one and rejects the other on every replica.
- **Leader-kill recovery** — killing the Raft leader mid-transaction elects a new leader and settles outstanding work with no lost or duplicated funds, because `commitIndex`/`lastApplied` are (correctly) never persisted and the ledger state machine's `apply()` is idempotent on `entry_id` — replaying a node's committed log after a crash never double-counts.
- **Fraud-worker rebalancing** — killing a fraud worker mid-stream causes the consumer group to detect the missed heartbeat and reassign its partitions; no transaction is silently dropped.
- **A liveness bug that only showed up under real time** — the base Raft paper's algorithm let a partitioned node that spun its term up while isolated disrupt a healthy cluster the instant it reconnected. Fixed with a PreVote phase (`raft/node.py`), the same fix etcd/raft ships in production.

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
python scripts/demo_fraud_burst.py              # burst of fraud-shaped transactions under load, catch rate
python scripts/demo_conflicting_transactions.py  # two concurrent debits, same account, no double-spend
python scripts/demo_kill_leader.py               # kill the Raft leader mid-transaction, verify recovery
```

## Running it

```bash
pip install -r requirements-dev.txt
pytest                          # ~70 tests, in-process + real gRPC-over-localhost
python scripts/demo_kill_leader.py   # etc.
```

**Real multi-container deployment** (5 Raft nodes + a coordinator, each its own container, talking over real gRPC):

```bash
docker compose up --build
```

## Notable design decisions

- **Raft's core is deterministic and side-effect-free** (`raft/node.py`): no threads, no sleeps, no sockets — just `tick(now)` and `handle_*(args, now)`, driven externally. That's what makes it possible to test election safety and failure recovery deterministically (`raft/tests/harness.py` single-steps a whole simulated cluster) before ever touching a real thread or socket. The exact same `RaftNode` class runs in-process (threads + `InMemoryTransport`) and over real gRPC (`coordinator/grpc/`) with zero changes to the algorithm.
- **Partitioning by `account_id`** isn't just a load-balancing choice — it's what guarantees every transaction for a given account is seen by exactly one fraud worker in order (so per-account velocity fraud checks don't need shared state) and, later, lands in Raft entries whose ordering is what actually prevents the double-spend.
- **At-least-once delivery + idempotent processing = effectively-once**, applied consistently at every hop: fraud scoring commits offsets only after scoring (worker crash → safe reprocessing, deduped downstream by `transaction_id`), and the ledger state machine dedupes on `LedgerEntry.entry_id` (crash → safe log replay).

## Known limitations

- Only the Raft layer is fully containerized. Fraud workers and the stream broker still run in-process via threads inside the coordinator container — that layer's correctness (partitioning, rebalancing, backpressure) is already fully proven without needing real process/network boundaries the way "kill the leader for real" does for Raft.
- `TRANSFER`-type transactions settle only the source account's leg; the counterparty credit (atomic multi-account commit) isn't implemented, since none of the three demo scenarios need it.
