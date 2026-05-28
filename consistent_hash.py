"""
Consistent hashing ring.

The ring maps keys to nodes with virtual nodes so data is distributed evenly
and only a small slice of keys move when membership changes.
"""

import bisect
import hashlib


class ConsistentHashRing:
    def __init__(self, virtual_nodes: int = 100):
        """
        virtual_nodes: number of positions per real node on the ring.
        More virtual nodes generally means a smoother key distribution.
        """
        self.virtual_nodes = virtual_nodes
        self.ring: dict[int, str] = {}
        self.sorted_keys: list[int] = []

    def _hash(self, key: str) -> int:
        """Return a stable 32-bit integer hash for a string key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)

    def add_node(self, node_id: str):
        """Add a node and its virtual positions to the ring."""
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:vnode:{i}"
            position = self._hash(virtual_key)
            self.ring[position] = node_id
            bisect.insort(self.sorted_keys, position)

        print(f"[Ring] Node '{node_id}' added ({self.virtual_nodes} virtual nodes)")

    def remove_node(self, node_id: str):
        """Remove a node and all of its virtual positions from the ring."""
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:vnode:{i}"
            position = self._hash(virtual_key)
            if position in self.ring:
                del self.ring[position]
                index = bisect.bisect_left(self.sorted_keys, position)
                if index < len(self.sorted_keys) and self.sorted_keys[index] == position:
                    self.sorted_keys.pop(index)

        print(f"[Ring] Node '{node_id}' removed")

    def get_node(self, key: str) -> str | None:
        """Return the primary node responsible for a key."""
        nodes = self.get_nodes(key, 1)
        return nodes[0] if nodes else None

    def get_nodes(self, key: str, count: int) -> list[str]:
        """
        Return up to ``count`` distinct nodes clockwise from the key position.

        The cluster uses this for primary-plus-replica placement.
        """
        if not self.ring or count <= 0:
            return []

        key_hash = self._hash(key)
        index = bisect.bisect_left(self.sorted_keys, key_hash)
        if index == len(self.sorted_keys):
            index = 0

        selected = []
        seen = set()
        total_nodes = len(set(self.ring.values()))

        while len(selected) < count and len(seen) < total_nodes:
            position = self.sorted_keys[index]
            node_id = self.ring[position]
            if node_id not in seen:
                seen.add(node_id)
                selected.append(node_id)
            index = (index + 1) % len(self.sorted_keys)

        return selected

    def get_distribution(self, keys: list[str]) -> dict[str | None, int]:
        """Return a node-to-key-count distribution for a sample of keys."""
        distribution: dict[str | None, int] = {}
        for key in keys:
            node = self.get_node(key)
            distribution[node] = distribution.get(node, 0) + 1
        return distribution
