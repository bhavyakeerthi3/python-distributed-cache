"""
Single storage node for the distributed key-value store.

Each node is an independent TCP server with an in-memory dictionary, optional
TTL expiry, basic statistics, and a small newline-terminated text protocol.
"""

import json
import socket
import sys
import threading
import time


class KVNode:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port

        self._store: dict[str, dict] = {}
        self._lock = threading.Lock()

        self._get_count = 0
        self._set_count = 0
        self._delete_count = 0
        self._hit_count = 0
        self._miss_count = 0
        self._start_time = time.time()

        self._server: socket.socket | None = None
        self._server_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def set(self, key: str, value: str, ttl: float | None = None) -> str:
        """Store a key-value pair with an optional TTL in seconds."""
        expires_at = time.time() + ttl if ttl else None
        with self._lock:
            self._store[key] = {"value": value, "expires_at": expires_at}
            self._set_count += 1
        return "OK"

    def get(self, key: str) -> str | None:
        """Retrieve a value, returning None when missing or expired."""
        with self._lock:
            self._get_count += 1
            entry = self._store.get(key)

            if entry is None:
                self._miss_count += 1
                return None

            if entry["expires_at"] and time.time() > entry["expires_at"]:
                del self._store[key]
                self._miss_count += 1
                return None

            self._hit_count += 1
            return entry["value"]

    def delete(self, key: str) -> bool:
        """Delete a key if present."""
        with self._lock:
            self._delete_count += 1
            if key in self._store:
                del self._store[key]
                return True
            return False

    def keys(self) -> list[str]:
        """Return all non-expired keys."""
        now = time.time()
        with self._lock:
            return [
                key
                for key, value in self._store.items()
                if not value["expires_at"] or now <= value["expires_at"]
            ]

    def info(self) -> dict:
        """Return operational stats for this node."""
        uptime = time.time() - self._start_time
        hit_rate = (self._hit_count / self._get_count * 100) if self._get_count else 0

        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "keys_stored": len(self.keys()),
            "uptime_secs": round(uptime, 2),
            "gets": self._get_count,
            "sets": self._set_count,
            "deletes": self._delete_count,
            "hit_rate": f"{hit_rate:.1f}%",
        }

    def _handle_command(self, raw: str) -> str:
        """Parse and execute one protocol command."""
        command = raw.strip()
        if not command:
            return "ERROR empty command"

        parts = command.split(" ", 3)
        verb = parts[0].upper()

        if verb == "PING":
            return "PONG"

        if verb == "SET":
            if len(parts) < 3:
                return "ERROR usage: SET key value [ttl]"

            key = parts[1]
            value = parts[2]
            ttl = None

            if len(parts) == 4:
                try:
                    ttl = float(parts[3])
                except ValueError:
                    value = f"{parts[2]} {parts[3]}"

            return self.set(key, value, ttl)

        if verb == "GET":
            if len(parts) < 2:
                return "ERROR usage: GET key"
            result = self.get(parts[1])
            return f"VALUE {result}" if result is not None else "NOT_FOUND"

        if verb == "DELETE":
            if len(parts) < 2:
                return "ERROR usage: DELETE key"
            return "DELETED" if self.delete(parts[1]) else "NOT_FOUND"

        if verb == "KEYS":
            return f"KEYS {','.join(self.keys())}"

        if verb == "INFO":
            return json.dumps(self.info())

        return f"ERROR unknown command '{verb}'"

    def _handle_client(self, conn: socket.socket, _addr):
        """Handle one client connection."""
        with conn:
            while not self._stop_event.is_set():
                try:
                    data = conn.recv(4096).decode().strip()
                    if not data:
                        break
                    response = self._handle_command(data)
                    conn.sendall((response + "\n").encode())
                except (ConnectionResetError, BrokenPipeError, OSError):
                    break

    def start(self):
        """Start the TCP server in a background thread."""
        if self._server is not None:
            return

        self._stop_event.clear()
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(50)
        self._server.settimeout(0.2)

        print(f"[Node {self.node_id}] Listening on {self.host}:{self.port}")

        def serve():
            while not self._stop_event.is_set():
                try:
                    conn, addr = self._server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                client = threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True,
                )
                client.start()

        self._server_thread = threading.Thread(target=serve, daemon=True)
        self._server_thread.start()

    def stop(self):
        """Stop the TCP server."""
        self._stop_event.set()
        if self._server:
            try:
                self._server.close()
            except OSError:
                pass
            self._server = None
        if self._server_thread:
            self._server_thread.join(timeout=1.0)
            self._server_thread = None


if __name__ == "__main__":
    node_id = sys.argv[1] if len(sys.argv) > 1 else "node1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5001

    node = KVNode(node_id=node_id, host="127.0.0.1", port=port)
    node.start()

    print(f"[Node {node_id}] Running on port {port}. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()
        print(f"\n[Node {node_id}] Shutting down.")
