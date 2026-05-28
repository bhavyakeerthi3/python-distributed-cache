"""
Distributed key-value store demo.

Run:
    python main.py
"""

import time

from cluster import KVCluster


def print_section(title: str):
    print(f"\n{'=' * 56}")
    print(f"  {title}")
    print("=" * 56)


def main():
    cluster = KVCluster(replication_factor=2)

    try:
        print_section("Starting 3-node distributed KV store")
        cluster.add_node("node1", "127.0.0.1", 6001)
        cluster.add_node("node2", "127.0.0.1", 6002)
        cluster.add_node("node3", "127.0.0.1", 6003)

        print_section("Basic operations")
        cluster.set("user:1001", "Bhavya Keerthi")
        cluster.set("user:1002", "Amazon SDE")
        cluster.set("config:timeout", "30")
        cluster.set("session:abc", "active")

        print(f"GET user:1001       -> {cluster.get('user:1001')}")
        print(f"GET user:1002       -> {cluster.get('user:1002')}")
        print(f"GET config:timeout  -> {cluster.get('config:timeout')}")
        print(f"GET missing_key     -> {cluster.get('missing_key')}")

        cluster.delete("session:abc")
        print(f"DELETE session:abc  -> GET returns {cluster.get('session:abc')}")

        print_section("TTL expiry")
        cluster.set("temp:token", "xyz789", ttl=2.0)
        print(f"GET temp:token before expiry -> {cluster.get('temp:token')}")
        print("Waiting 3 seconds...")
        time.sleep(3)
        print(f"GET temp:token after expiry  -> {cluster.get('temp:token')}")

        print_section("Consistent hashing distribution")
        test_keys = [f"key:{i}" for i in range(30)]
        for key in test_keys:
            cluster.set(key, f"value_{key}")

        distribution = cluster.ring.get_distribution(test_keys)
        for node, count in sorted(distribution.items()):
            bar = "#" * count
            print(f"  {node}: {bar} ({count} keys)")

        print_section("Fault tolerance simulation")
        cluster.set("critical:data", "must_not_lose_this")
        print(f"Before failure -> {cluster.get('critical:data')}")

        replicas = cluster._get_replicas("critical:data")
        failed_node = replicas[0]
        print(f"Simulating failure of primary replica: {failed_node}")
        cluster.remove_node(failed_node)

        result = cluster.get("critical:data")
        print(f"After failure  -> {result}")
        print("PASS: data survived node failure" if result else "FAIL: data was unavailable")

        print_section("Cluster info")
        for node_id, stats in cluster.cluster_info().items():
            print(f"\n[{node_id}]")
            for key, value in stats.items():
                print(f"  {key}: {value}")

        print_section("Demo complete")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
