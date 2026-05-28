"""
Cluster coordinator for the distributed key-value store.

The cluster owns membership, consistent-hash placement, replication, and TCP
communication with storage nodes.
"""

import json
import socket
import threading
import time

from consistent_hash import ConsistentHashRing
from node import KVNode


class KVCluster:
    def __init__(self, replication_factor: int = 2):
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")

        self.ring = ConsistentHashRing(virtual_nodes=150)
        self.nodes: dict[str, dict] = {}
        self.replication_factor = replication_factor
        self._lock = threading.Lock()
        self._connections: dict[str, socket.socket] = {}
        self._connection_locks: dict[str, threading.Lock] = {}

    def add_node(self, node_id: str, host: str, port: int):
        """Add and start a node in the cluster."""
        node = KVNode(node_id=node_id, host=host, port=port)
        node.start()
        time.sleep(0.1)

        with self._lock:
            self.nodes[node_id] = {"host": host, "port": port, "node": node}
            self._connection_locks[node_id] = threading.Lock()
            self.ring.add_node(node_id)

        print(f"[Cluster] Node '{node_id}' joined the cluster.")

    def remove_node(self, node_id: str, stop: bool = True):
        """Remove a node from membership and optionally stop its TCP server."""
        with self._lock:
            node_info = self.nodes.pop(node_id, None)
            self._connection_locks.pop(node_id, None)
            if node_info:
                self.ring.remove_node(node_id)

        self._close_connection(node_id)
        if node_info and stop:
            node_info["node"].stop()
            print(f"[Cluster] Node '{node_id}' removed from cluster.")

    def shutdown(self):
        """Stop every node owned by this cluster."""
        for node_id in list(self.nodes):
            self.remove_node(node_id)

    def _get_replicas(self, key: str) -> list[str]:
        """Return primary plus replica node IDs for a key."""
        with self._lock:
            replica_count = min(self.replication_factor, len(self.nodes))
            return self.ring.get_nodes(key, replica_count)

    def _send_command(self, node_id: str, command: str) -> str | None:
        """Send a raw TCP command to a node."""
        node_info = self.nodes.get(node_id)
        if not node_info:
            return None

        lock = self._connection_locks.setdefault(node_id, threading.Lock())
        with lock:
            response = self._send_with_reused_connection(node_id, node_info, command)
            if response is not None:
                return response

            self._close_connection(node_id)
            return self._send_with_reused_connection(node_id, node_info, command)

    def _send_with_reused_connection(
        self,
        node_id: str,
        node_info: dict,
        command: str,
    ) -> str | None:
        """Send a command over an existing socket, reconnecting when needed."""
        try:
            sock = self._connections.get(node_id)
            if sock is None:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((node_info["host"], node_info["port"]))
                self._connections[node_id] = sock

            sock.sendall((command + "\n").encode())
            return sock.recv(4096).decode().strip()
        except (socket.timeout, ConnectionRefusedError, OSError):
            print(f"[Cluster] Node '{node_id}' unreachable; skipping.")
            return None

    def _close_connection(self, node_id: str):
        """Close and forget a cached node connection."""
        sock = self._connections.pop(node_id, None)
        if sock:
            try:
                sock.close()
            except OSError:
                pass

    def set(self, key: str, value: str, ttl: float | None = None) -> bool:
        """Write a key-value pair to all replica nodes."""
        replicas = self._get_replicas(key)
        if not replicas:
            print("[Cluster] ERROR: No nodes available.")
            return False

        command = f"SET {key} {value}" + (f" {ttl}" if ttl else "")
        successes = sum(1 for node_id in replicas if self._send_command(node_id, command) == "OK")
        return successes > 0

    def get(self, key: str) -> str | None:
        """Read from the primary node, then fall back to replicas."""
        for node_id in self._get_replicas(key):
            response = self._send_command(node_id, f"GET {key}")
            if response is None:
                continue
            if response.startswith("VALUE "):
                return response[6:]
            if response == "NOT_FOUND":
                return None
        return None

    def delete(self, key: str) -> bool:
        """Delete a key from all replica nodes."""
        deleted = False
        for node_id in self._get_replicas(key):
            if self._send_command(node_id, f"DELETE {key}") == "DELETED":
                deleted = True
        return deleted

    def cluster_info(self) -> dict:
        """Return INFO responses from all reachable nodes."""
        info = {}
        for node_id in self.nodes:
            response = self._send_command(node_id, "INFO")
            if not response:
                info[node_id] = {"status": "unreachable"}
                continue
            try:
                info[node_id] = json.loads(response)
            except json.JSONDecodeError:
                info[node_id] = {"raw": response}
        return info
