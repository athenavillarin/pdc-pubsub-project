"""Publisher client for the pub-sub system.

Scope in this file:
- Connect to the broker over TCP
- Send messages with a topic label
"""

from __future__ import annotations

import argparse
import json
import socket
import sys


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000


class Publisher:
    """Connects to the broker and publishes messages to a topic."""

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
        self.host = host
        self.port = port
        self._sock: socket.socket | None = None
        self._reader = None

    # ── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Open a TCP connection to the broker."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._reader = self._sock.makefile("r", encoding="utf-8")
        print(f"[Publisher] Connected to broker at {self.host}:{self.port}")

    def disconnect(self) -> None:
        """Close the connection."""
        if self._reader:
            try:
                self._reader.close()
            except OSError:
                pass
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        print("[Publisher] Disconnected.")

    # ── Internal helpers ────────────────────────────────────────────────────

    def _send(self, payload: dict) -> None:
        """Send a JSON line to the broker."""
        data = (json.dumps(payload) + "\n").encode("utf-8")
        assert self._sock is not None, "Not connected"
        self._sock.sendall(data)

    def _recv(self) -> dict:
        """Read one JSON line from the broker."""
        assert self._reader is not None, "Not connected"
        line = self._reader.readline()
        if not line:
            raise ConnectionError("Broker closed the connection")
        return json.loads(line.strip())

    # ── Public API ──────────────────────────────────────────────────────────

    def publish(self, topic: str, message: str) -> int:
        """Publish a message to a topic. Returns number of subscribers routed to."""
        self._send({"cmd": "publish", "topic": topic, "payload": message})
        response = self._recv()
        if response.get("type") == "ok":
            routed = response.get("routed", 0)
            print(f"[Publisher] Published to '{topic}': {message!r} → routed to {routed} subscriber(s)")
            return routed
        else:
            print(f"[Publisher] Error: {response.get('message')}")
            return 0


# ── CLI entry point ─────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Pub-Sub Publisher Client")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Broker host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Broker port")
    parser.add_argument("--topic", required=True, help="Topic to publish to")
    parser.add_argument("--message", required=True, help="Message to publish")
    args = parser.parse_args()

    pub = Publisher(host=args.host, port=args.port)
    try:
        pub.connect()
        pub.publish(args.topic, args.message)
    except ConnectionRefusedError:
        print(f"[Publisher] Could not connect to broker at {args.host}:{args.port}")
        sys.exit(1)
    except Exception as e:
        print(f"[Publisher] Unexpected error: {e}")
        sys.exit(1)
    finally:
        pub.disconnect()


if __name__ == "__main__":
    main()