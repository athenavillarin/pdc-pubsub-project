"""Subscriber client for the pub-sub system.

Scope in this file:
- Connect to the broker over TCP
- Register topic subscriptions
- Receive delivered messages
- Send heartbeats to stay online
- Auto-reconnect on disconnection
- Flush persistent queue on reconnection (at-least-once delivery)
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import threading
import time

from queues.persistent_queue import PersistentQueue


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000
HEARTBEAT_INTERVAL = 2.0   # seconds — must be less than broker's heartbeat_timeout (5s)
RECONNECT_DELAY = 3.0      # seconds to wait before reconnecting


class Subscriber:
    """Connects to the broker, subscribes to topics, and receives messages.

    Features:
    - Sends periodic heartbeats so the broker keeps this subscriber online
    - Auto-reconnects if the connection drops
    - Flushes the persistent queue on reconnection (at-least-once delivery)
    """

    def __init__(
        self,
        subscriber_id: str,
        topics: list[str],
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        queue: PersistentQueue | None = None,
    ) -> None:
        self.subscriber_id = subscriber_id
        self.topics = topics
        self.host = host
        self.port = port
        self.queue = queue or PersistentQueue()

        self._sock: socket.socket | None = None
        self._reader = None
        self._running = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # ── Connection ──────────────────────────────────────────────────────────

    def _connect(self) -> bool:
        """Try to open a TCP connection and subscribe. Returns True on success."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
            reader = sock.makefile("r", encoding="utf-8")

            with self._lock:
                self._sock = sock
                self._reader = reader

            # Register subscriptions
            self._send({"cmd": "subscribe", "subscriber_id": self.subscriber_id, "topics": self.topics})
            response = self._recv()
            if response.get("type") != "ok":
                print(f"[{self.subscriber_id}] Subscribe failed: {response}")
                return False

            print(f"[{self.subscriber_id}] Connected and subscribed to: {self.topics}")

            # Flush any queued messages from when we were offline
            self._flush_queue()
            return True

        except (ConnectionRefusedError, OSError) as e:
            print(f"[{self.subscriber_id}] Connection failed: {e}")
            return False

    def _disconnect(self) -> None:
        """Close the current connection silently."""
        with self._lock:
            reader, self._reader = self._reader, None
            sock, self._sock = self._sock, None

        if reader:
            try:
                reader.close()
            except OSError:
                pass
        if sock:
            try:
                sock.close()
            except OSError:
                pass

    # ── Internal helpers ────────────────────────────────────────────────────

    def _send(self, payload: dict) -> None:
        """Send a JSON line to the broker."""
        with self._lock:
            sock = self._sock
        if sock is None:
            raise ConnectionError("Not connected")
        data = (json.dumps(payload) + "\n").encode("utf-8")
        sock.sendall(data)

    def _recv(self) -> dict:
        """Read one JSON line from the broker."""
        with self._lock:
            reader = self._reader
        if reader is None:
            raise ConnectionError("Not connected")
        line = reader.readline()
        if not line:
            raise ConnectionError("Broker closed the connection")
        return json.loads(line.strip())

    def _flush_queue(self) -> None:
        """Deliver any messages queued while offline, then clear the queue."""
        messages = self.queue.flush(self.subscriber_id)
        if messages:
            print(f"[{self.subscriber_id}] Flushing {len(messages)} queued message(s) from disk:")
            for msg in messages:
                self._on_message(msg.topic, msg.payload, from_queue=True)

    def _on_message(self, topic: str, payload: str, from_queue: bool = False) -> None:
        """Handle a delivered message. Override or extend this for custom logic."""
        source = "QUEUE" if from_queue else "LIVE "
        print(f"[{self.subscriber_id}] [{source}] topic={topic!r} payload={payload!r}")

    # ── Heartbeat ───────────────────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        """Send heartbeats to the broker every HEARTBEAT_INTERVAL seconds."""
        while self._running.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            if not self._running.is_set():
                break
            try:
                self._send({"cmd": "heartbeat", "subscriber_id": self.subscriber_id})
            except (ConnectionError, OSError):
                pass  # Receive loop will handle reconnection

    # ── Main receive loop ───────────────────────────────────────────────────

    def _receive_loop(self) -> None:
        """Receive messages from the broker. Auto-reconnects on disconnection."""
        while self._running.is_set():
            if self._sock is None:
                print(f"[{self.subscriber_id}] Reconnecting in {RECONNECT_DELAY}s...")
                time.sleep(RECONNECT_DELAY)
                self._connect()
                continue

            try:
                message = self._recv()
            except (ConnectionError, OSError, json.JSONDecodeError) as e:
                if self._running.is_set():
                    print(f"[{self.subscriber_id}] Disconnected: {e}. Will reconnect...")
                    self._disconnect()
                continue

            msg_type = message.get("type")

            if msg_type == "deliver":
                topic = message.get("topic", "")
                payload = message.get("payload", "")

                # Queue the message first (at-least-once: write before processing)
                self.queue.enqueue(self.subscriber_id, topic, payload)
                self._on_message(topic, payload)
                # Clear from queue after successful processing
                self.queue.flush(self.subscriber_id)

            elif msg_type == "error":
                print(f"[{self.subscriber_id}] Broker error: {message.get('message')}")

            elif msg_type == "ok":
                pass  # Heartbeat ack or other confirmations — ignore

    # ── Public API ──────────────────────────────────────────────────────────

    def start(self) -> None:
        """Connect to the broker and start listening in the background."""
        self._running.set()

        connected = self._connect()
        if not connected:
            print(f"[{self.subscriber_id}] Initial connection failed. Will retry in background.")

        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        # Run receive loop in the foreground (blocking)
        self._receive_loop()

    def stop(self) -> None:
        """Stop the subscriber gracefully."""
        self._running.clear()
        self._disconnect()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2)
        print(f"[{self.subscriber_id}] Stopped.")


# ── CLI entry point ─────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Pub-Sub Subscriber Client")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Broker host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Broker port")
    parser.add_argument("--id", required=True, dest="subscriber_id", help="Unique subscriber ID")
    parser.add_argument("--topics", required=True, nargs="+", help="Topics to subscribe to")
    args = parser.parse_args()

    sub = Subscriber(
        subscriber_id=args.subscriber_id,
        topics=args.topics,
        host=args.host,
        port=args.port,
    )

    try:
        sub.start()
    except KeyboardInterrupt:
        sub.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()