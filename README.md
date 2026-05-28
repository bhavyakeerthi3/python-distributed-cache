# Distributed Key-Value Store

A fault-tolerant in-memory key-value store built from scratch in Python. It
demonstrates distributed-systems fundamentals that are useful for backend SDE
roles: consistent hashing, replication, TCP networking, TTL expiry, concurrent
request handling, and latency benchmarking.

The project uses only the Python standard library, so it is easy to run and
easy to reason about in interviews.

## Why This Project Stands Out

- Implements consistent hashing with virtual nodes for balanced key placement.
- Replicates each write to multiple nodes so reads can survive a node failure.
- Exposes a simple TCP wire protocol that can be tested with `telnet` or `nc`.
- Reuses per-node TCP connections from the cluster client for low latency.
- Tracks node-level stats such as key count, uptime, operations, and hit rate.
- Includes a benchmark script and a unit test suite.
- Keeps the code compact enough to explain clearly in a 30-minute interview.

## Architecture

```text
Client / Demo / Benchmark
        |
        v
KVCluster
        |
        +-- ConsistentHashRing
        |      maps keys to primary and replica nodes
        |
        +-- KVNode node1  TCP 127.0.0.1:6001
        +-- KVNode node2  TCP 127.0.0.1:6002
        +-- KVNode node3  TCP 127.0.0.1:6003
               |
               +-- in-memory dict + TTL expiry + thread-safe locks
```

## Design Decisions

| Area | Choice | Reason |
| --- | --- | --- |
| Partitioning | Consistent hashing | Limits key movement when nodes join or leave |
| Load balance | 150 virtual nodes per physical node | Smooths distribution across small clusters |
| Replication | Primary plus clockwise replicas | Keeps reads available after a single-node failure |
| Network protocol | Newline-terminated TCP text commands | Simple, inspectable, and dependency-free |
| Client networking | Reused TCP connections per node | Avoids reconnect overhead during benchmarks |
| Concurrency | Thread per client connection | Good fit for a local educational store |
| Expiry | Lazy TTL eviction | Removes expired keys without a background sweeper |

## Project Structure

```text
.
|-- main.py                # End-to-end demo
|-- cluster.py             # Cluster membership, routing, replication
|-- node.py                # TCP storage node and command protocol
|-- consistent_hash.py     # Consistent hash ring with virtual nodes
|-- benchmark.py           # Latency and throughput benchmark
|-- tests/
|   `-- test_kv_store.py   # Unit tests for ring, node, and cluster behavior
|-- requirements.txt       # No external dependencies
|-- LICENSE                # MIT license
`-- README.md
```

## Quick Start

Requires Python 3.10+.

```bash
python main.py
```

The demo starts a 3-node local cluster, runs basic operations, verifies TTL
expiry, prints key distribution, simulates primary node failure, and shows
cluster stats.

## Run Tests

```bash
python -m unittest discover -s tests
```

## Run Benchmark

```bash
python benchmark.py
```

Example run on a local Windows machine:

```text
SET      500 ops | Avg: 0.34ms | P99: 0.58ms | ~2950 ops/s | Success: 500/500
GET      500 ops | Avg: 0.15ms | P99: 0.28ms | ~6597 ops/s | Success: 500/500
DELETE   500 ops | Avg: 0.26ms | P99: 0.47ms | ~3774 ops/s | Success: 500/500
```

Your exact numbers will depend on your machine.

## Wire Protocol

Each node accepts newline-terminated TCP commands.

| Command | Example | Response |
| --- | --- | --- |
| `PING` | `PING` | `PONG` |
| `SET key value [ttl]` | `SET name Bhavya` | `OK` |
| `GET key` | `GET name` | `VALUE Bhavya` or `NOT_FOUND` |
| `DELETE key` | `DELETE name` | `DELETED` or `NOT_FOUND` |
| `KEYS` | `KEYS` | `KEYS key1,key2` |
| `INFO` | `INFO` | JSON node stats |

Manual TCP test:

```bash
python node.py node1 6001
```

In another terminal:

```bash
telnet 127.0.0.1 6001
SET language Python
GET language
```

## Interview Talking Points

- How consistent hashing reduces remapping compared with modulo hashing.
- Why virtual nodes improve distribution in small clusters.
- What consistency tradeoff this design makes by accepting a write if at least
  one replica succeeds.
- Why connection reuse matters for network services and benchmarks.
- Why TTL is implemented lazily and when a background compactor would be useful.
- How the design could evolve toward production with persistence, read repair,
  quorum reads/writes, and leader election.

## Future Improvements

- Add write-ahead logging for crash recovery.
- Add quorum-based reads and writes.
- Add read repair when replicas diverge.
- Add Docker Compose for multi-process demos.
- Add a small HTTP API on top of the TCP node protocol.

## Resume Bullet

Built a distributed in-memory key-value store in Python using consistent hashing,
replication, reusable TCP connections, TTL expiry, and concurrent request
handling; benchmarked local reads at roughly 6.5K ops/sec and added unit tests
for distribution, protocol, and fault-tolerance behavior.
