import socket
import time
import unittest

from cluster import KVCluster
from consistent_hash import ConsistentHashRing
from node import KVNode


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ConsistentHashRingTest(unittest.TestCase):
    def test_returns_distinct_replicas(self):
        ring = ConsistentHashRing(virtual_nodes=20)
        for node_id in ["node1", "node2", "node3"]:
            ring.add_node(node_id)

        replicas = ring.get_nodes("customer:42", 2)

        self.assertEqual(len(replicas), 2)
        self.assertEqual(len(set(replicas)), 2)

    def test_removing_node_keeps_other_keys_addressable(self):
        ring = ConsistentHashRing(virtual_nodes=20)
        for node_id in ["node1", "node2", "node3"]:
            ring.add_node(node_id)

        ring.remove_node("node2")

        self.assertIn(ring.get_node("order:123"), {"node1", "node3"})


class KVNodeTest(unittest.TestCase):
    def test_ttl_expires_key(self):
        node = KVNode("test", "127.0.0.1", free_port())
        node.set("temp", "value", ttl=0.05)

        self.assertEqual(node.get("temp"), "value")
        time.sleep(0.08)
        self.assertIsNone(node.get("temp"))

    def test_text_protocol(self):
        node = KVNode("test", "127.0.0.1", free_port())

        self.assertEqual(node._handle_command("SET name Bhavya"), "OK")
        self.assertEqual(node._handle_command("GET name"), "VALUE Bhavya")
        self.assertEqual(node._handle_command("DELETE name"), "DELETED")
        self.assertEqual(node._handle_command("GET name"), "NOT_FOUND")


class KVClusterTest(unittest.TestCase):
    def test_cluster_survives_primary_node_failure(self):
        ports = [free_port(), free_port(), free_port()]
        cluster = KVCluster(replication_factor=2)

        try:
            cluster.add_node("node1", "127.0.0.1", ports[0])
            cluster.add_node("node2", "127.0.0.1", ports[1])
            cluster.add_node("node3", "127.0.0.1", ports[2])

            key = "critical:data"
            self.assertTrue(cluster.set(key, "must_not_lose_this"))
            failed_primary = cluster._get_replicas(key)[0]

            cluster.remove_node(failed_primary)

            self.assertEqual(cluster.get(key), "must_not_lose_this")
        finally:
            cluster.shutdown()


if __name__ == "__main__":
    unittest.main()
