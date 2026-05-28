"""
Latency and throughput benchmark for the distributed key-value store.

Run:
    python benchmark.py
"""

import statistics
import time

from cluster import KVCluster


NUM_OPS = 500


def benchmark_operation(name: str, fn, keys: list[str]) -> list[float]:
    latencies = []
    success = 0

    for key in keys:
        start = time.perf_counter()
        result = fn(key)
        elapsed_ms = (time.perf_counter() - start) * 1000

        latencies.append(elapsed_ms)
        if result is not None and result is not False:
            success += 1

    avg = statistics.mean(latencies)
    p99_index = min(int(len(latencies) * 0.99), len(latencies) - 1)
    p99 = sorted(latencies)[p99_index]
    throughput = 1000 / avg

    print(
        f"  {name:<8} {len(keys)} ops | "
        f"Avg: {avg:.2f}ms | "
        f"P99: {p99:.2f}ms | "
        f"~{throughput:.0f} ops/s | "
        f"Success: {success}/{len(keys)}"
    )

    return latencies


def main():
    print("\n" + "=" * 58)
    print("  Distributed KV Store Benchmark")
    print("=" * 58)

    cluster = KVCluster(replication_factor=2)
    try:
        cluster.add_node("bench1", "127.0.0.1", 7001)
        cluster.add_node("bench2", "127.0.0.1", 7002)
        cluster.add_node("bench3", "127.0.0.1", 7003)

        keys = [f"bench:key:{i}" for i in range(NUM_OPS)]
        print(f"\nRunning {NUM_OPS} operations per command...\n")

        benchmark_operation("SET", lambda key: cluster.set(key, f"value_{key}"), keys)
        benchmark_operation("GET", lambda key: cluster.get(key) or "NOT_FOUND", keys)
        benchmark_operation("DELETE", lambda key: cluster.delete(key), keys)

        print("\nDone. Record your own machine's numbers in the README or resume.")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
