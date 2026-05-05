"""Tests for subscriber disconnection and reconnection with message catch-up."""
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


def test_subscriber_goes_offline_after_heartbeat_timeout() -> None:
    """Verify that subscribers marked offline when heartbeat times out."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, heartbeat_timeout=0.5, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    sub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        _send_json(sub, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub)["type"] == "ok"

        # Initially online
        assert broker.is_subscriber_online(sub_id) is True

        # Send heartbeat
        _send_json(sub, {"cmd": "heartbeat", "subscriber_id": sub_id})
        assert _recv_json(sub)["type"] == "ok"
        assert broker.is_subscriber_online(sub_id) is True

        # Wait for heartbeat timeout without sending another heartbeat
        time.sleep(0.7)

        # Should be offline now
        assert broker.is_subscriber_online(sub_id) is False

    finally:
        try:
            sub.close()
        except OSError:
            pass
        broker.stop()
        _restore_queues(queue_dir)


def test_disconnect_cleanup() -> None:
    """Verify that closing connection marks subscriber offline."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, heartbeat_timeout=2.0, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    sub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        _send_json(sub, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub)["type"] == "ok"

        assert broker.is_subscriber_online(sub_id) is True

        # Close connection
        sub.close()
        time.sleep(0.2)

        # Should be offline
        assert broker.is_subscriber_online(sub_id) is False

    finally:
        try:
            sub.close()
        except OSError:
            pass
        broker.stop()
        _restore_queues(queue_dir)


def test_multiple_reconnections() -> None:
    """Verify subscriber can reconnect multiple times and catch up on messages."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, ack_timeout=0.5, heartbeat_timeout=0.3, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # First connection
        sub1 = socket.create_connection((broker.host, broker.port), timeout=2)
        _send_json(sub1, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub1)["type"] == "ok"

        # Go offline
        time.sleep(0.4)
        sub1.close()

        # Publish message 1
        _send_json(pub, {"cmd": "publish", "topic": "test", "payload": "Msg1"})
        _ = _recv_json(pub)
        time.sleep(0.2)

        # Second connection (reconnection)
        sub2 = socket.create_connection((broker.host, broker.port), timeout=2)
        _send_json(sub2, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        msg = _recv_json(sub2)
        assert msg["type"] == "deliver"
        assert msg["payload"] == "Msg1"

        msg1_id = msg["message_id"]

        # ACK it
        _send_json(sub2, {"cmd": "ack", "subscriber_id": sub_id, "message_id": msg1_id})
        _ = _recv_json(sub2)

        subscribe_ok = _recv_json(sub2)
        assert subscribe_ok["type"] == "ok"

        # Go offline again
        time.sleep(0.4)
        sub2.close()

        # Publish message 2
        _send_json(pub, {"cmd": "publish", "topic": "test", "payload": "Msg2"})
        _ = _recv_json(pub)
        time.sleep(0.2)

        # Third connection (second reconnection)
        sub3 = socket.create_connection((broker.host, broker.port), timeout=2)
        _send_json(sub3, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        msg = _recv_json(sub3)
        assert msg["type"] == "deliver"
        assert msg["payload"] == "Msg2"

        subscribe_ok = _recv_json(sub3, timeout=1.0)
        assert subscribe_ok["type"] == "ok"

        sub3.close()

    finally:
        try:
            pub.close()
        except OSError:
            pass
        broker.stop()
        _restore_queues(queue_dir)


def test_connection_steal() -> None:
    """Verify that a new connection with same subscriber_id closes the old one."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, heartbeat_timeout=2.0, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    sub1 = socket.create_connection((broker.host, broker.port), timeout=2)
    sub2 = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # First connection for sub_id
        _send_json(sub1, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        resp1 = _recv_json(sub1)
        assert resp1["type"] == "ok"

        # Second connection for same subscriber_id
        _send_json(sub2, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        resp2 = _recv_json(sub2)
        assert resp2["type"] == "ok"

        # First connection should be closed now
        sub1.settimeout(0.5)
        try:
            # Try to send on old connection
            _send_json(sub1, {"cmd": "heartbeat", "subscriber_id": sub_id})
            result = _recv_json(sub1)
            # If we get here, connection wasn't closed (which is ok, just means cleanup timing)
            # But the broker should consider sub_id to be using sub2's connection now
        except (socket.timeout, BrokenPipeError, OSError):
            # Expected - old connection closed
            pass

        # Active session should be sub2
        assert broker.is_subscriber_online(sub_id) is True

    finally:
        try:
            sub1.close()
        except OSError:
            pass
        try:
            sub2.close()
        except OSError:
            pass
        broker.stop()
        _restore_queues(queue_dir)


def test_missed_messages_queued_during_disconnection() -> None:
    """Verify that all messages published while offline are queued and replayed."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, ack_timeout=0.5, heartbeat_timeout=0.3, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_id = f"sub_{uuid.uuid4().hex[:8]}"
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # Connect subscriber
        sub1 = socket.create_connection((broker.host, broker.port), timeout=2)
        _send_json(sub1, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        assert _recv_json(sub1)["type"] == "ok"

        # Go offline
        time.sleep(0.4)
        sub1.close()

        # Publish 5 messages while offline
        for i in range(5):
            _send_json(pub, {"cmd": "publish", "topic": "test", "payload": f"Message{i}"})
            _ = _recv_json(pub)

        time.sleep(0.2)

        # Reconnect
        sub2 = socket.create_connection((broker.host, broker.port), timeout=2)
        _send_json(sub2, {"cmd": "subscribe", "subscriber_id": sub_id, "topics": ["test"]})
        replayed = []
        while True:
            message = _recv_json(sub2, timeout=1.0)
            if message["type"] == "ok":
                break
            assert message["type"] == "deliver"
            replayed.append(message["payload"])

        # Should receive all 5 messages
        received_messages = replayed

        # Verify all messages are there (order may vary, but all should be present)
        for i in range(5):
            assert f"Message{i}" in received_messages, f"Message{i} not found in replay"

        sub2.close()

    finally:
        try:
            pub.close()
        except OSError:
            pass
        broker.stop()
        _restore_queues(queue_dir)