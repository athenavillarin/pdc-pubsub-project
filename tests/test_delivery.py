"""Tests for at-least-once delivery guarantees."""
from __future__ import annotations

import json
import shutil
import socket
import sys
import time
import uuid
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from broker.broker import BrokerServer
import queues.persistent_queue as persistent_queue


def _send_json(conn: socket.socket, payload: dict) -> None:
    conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))


def _recv_json(conn: socket.socket, timeout: float = 2.0) -> dict | None:
    conn.settimeout(timeout)
    data = b""
    while b"\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            return None
        data += chunk
    line, _, _ = data.partition(b"\n")
    return json.loads(line.decode("utf-8"))


def _clear_queues() -> None:
    """Use an isolated temporary queue directory for this test run."""
    temp_dir = tempfile.mkdtemp(prefix="pdc-pubsub-tests-")
    persistent_queue.QUEUE_DIR = temp_dir
    return temp_dir


def _restore_queues(queue_dir: str) -> None:
    persistent_queue.QUEUE_DIR = str(ROOT / "queues")
    shutil.rmtree(queue_dir, ignore_errors=True)


def test_ack_required_for_delivery() -> None:
    """Verify that messages include message_id and broker waits for ACK."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, ack_timeout=0.5, heartbeat_timeout=2.0, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    sub = socket.create_connection((broker.host, broker.port), timeout=2)
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # Subscribe
        _send_json(sub, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub)["type"] == "ok"

        # Publish a message
        _send_json(pub, {"cmd": "publish", "topic": "test", "payload": "Hello"})
        pub_resp = _recv_json(pub)
        assert pub_resp["type"] == "ok"

        # Receive the delivery
        delivery = _recv_json(sub)
        assert delivery["type"] == "deliver"
        assert delivery["topic"] == "test"
        assert delivery["payload"] == "Hello"
        message_id = delivery.get("message_id")
        assert message_id is not None, "Message should include message_id"

        # ACK the message
        _send_json(sub, {"cmd": "ack", "subscriber_id": sub_id, "message_id": message_id})
        ack_resp = _recv_json(sub)
        assert ack_resp["type"] == "ok"
        assert ack_resp["message_id"] == message_id

    finally:
        for conn in [sub, pub]:
            try:
                conn.close()
            except OSError:
                pass
        broker.stop()
        _restore_queues(queue_dir)


def test_persisted_queue_on_offline() -> None:
    """Verify messages are queued when subscriber is offline."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, ack_timeout=0.5, heartbeat_timeout=0.3, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    sub = socket.create_connection((broker.host, broker.port), timeout=2)
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # Subscribe to a topic
        _send_json(sub, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub)["type"] == "ok"

        # Let the subscriber go offline by not sending heartbeat
        time.sleep(0.5)  # Wait for heartbeat timeout
        sub.close()

        # Verify subscriber is offline
        time.sleep(0.3)
        assert broker.is_subscriber_online(sub_id) is False

        # Publish while offline
        _send_json(pub, {"cmd": "publish", "topic": "test", "payload": "Offline message"})
        pub_resp = _recv_json(pub)
        assert pub_resp["type"] == "ok"
        assert pub_resp["routed"] == 0  # No active subscribers

        # Check that queue has the message
        queued = broker.queue.fetch_pending(sub_id)
        assert len(queued) > 0, "Message should be persisted for offline subscriber"
        assert queued[0].payload == "Offline message"

    finally:
        try:
            pub.close()
        except OSError:
            pass
        broker.stop()
        _restore_queues(queue_dir)


def test_message_replay_on_reconnection() -> None:
    """Verify persisted messages are replayed when subscriber reconnects."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, ack_timeout=0.5, heartbeat_timeout=0.3, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # First subscriber connects and subscribes
        sub1 = socket.create_connection((broker.host, broker.port), timeout=2)
        _send_json(sub1, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub1)["type"] == "ok"

        # Go offline
        time.sleep(0.4)
        sub1.close()

        # Publish while offline
        _send_json(pub, {"cmd": "publish", "topic": "test", "payload": "Message 1"})
        _ = _recv_json(pub)

        _send_json(pub, {"cmd": "publish", "topic": "test", "payload": "Message 2"})
        _ = _recv_json(pub)

        time.sleep(0.2)

        # Reconnect with a new connection; queued deliveries arrive before the subscribe OK.
        sub2 = socket.create_connection((broker.host, broker.port), timeout=2)
        _send_json(sub2, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        # Should receive the persisted messages on reconnection
        msg1 = _recv_json(sub2, timeout=1.0)
        assert msg1["type"] == "deliver"
        assert msg1["payload"] == "Message 1"

        msg2 = _recv_json(sub2, timeout=1.0)
        assert msg2["type"] == "deliver"
        assert msg2["payload"] == "Message 2"

        subscribe_resp = _recv_json(sub2, timeout=1.0)
        assert subscribe_resp["type"] == "ok"

        sub2.close()

    finally:
        try:
            pub.close()
        except OSError:
            pass
        broker.stop()
        persistent_queue.QUEUE_DIR = str(ROOT / "queues")
        try:
            Path(queue_dir).rmdir()
        except OSError:
            pass


def test_duplicate_flag_on_redelivery() -> None:
    """Verify that redelivered messages are marked as duplicates."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, ack_timeout=0.5, heartbeat_timeout=2.0, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    sub = socket.create_connection((broker.host, broker.port), timeout=2)
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # Subscribe
        _send_json(sub, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub)["type"] == "ok"

        # Publish
        _send_json(pub, {"cmd": "publish", "topic": "test", "payload": "Test"})
        _ = _recv_json(pub)

        # Receive delivery
        delivery1 = _recv_json(sub)
        assert delivery1["type"] == "deliver"
        msg_id = delivery1["message_id"]
        duplicate_flag = delivery1.get("duplicate", False)
        assert duplicate_flag is False, "First delivery should not be marked as duplicate"

        # Don't ACK - wait for timeout and redelivery
        time.sleep(0.7)

        # Should receive redelivery
        delivery2 = _recv_json(sub, timeout=1.0)
        assert delivery2["type"] == "deliver"
        assert delivery2["message_id"] == msg_id
        assert delivery2.get("duplicate", False) is True, "Redelivery should be marked as duplicate"

    finally:
        for conn in [sub, pub]:
            try:
                conn.close()
            except OSError:
                pass
        broker.stop()
        _restore_queues(queue_dir)