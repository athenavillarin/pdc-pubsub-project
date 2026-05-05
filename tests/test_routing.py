"""Tests for topic-based message routing."""
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


def test_multiple_topics_routing() -> None:
    """Verify that messages are routed only to subscribers of matching topics."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, heartbeat_timeout=5.0, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    # Create subscribers for different topics with unique IDs
    sub_tech_id = f"sub_tech_{uuid.uuid4().hex[:8]}"
    sub_bank_id = f"sub_bank_{uuid.uuid4().hex[:8]}"
    sub_both_id = f"sub_both_{uuid.uuid4().hex[:8]}"
    
    sub_tech = socket.create_connection((broker.host, broker.port), timeout=2)
    sub_bank = socket.create_connection((broker.host, broker.port), timeout=2)
    sub_both = socket.create_connection((broker.host, broker.port), timeout=2)
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # sub_tech subscribes to stocks.tech only
        _send_json(sub_tech, {"cmd": "subscribe", "subscriber_id": sub_tech_id, "topics": ["stocks.tech"]})
        assert _recv_json(sub_tech)["type"] == "ok"

        # sub_bank subscribes to stocks.bank only
        _send_json(sub_bank, {"cmd": "subscribe", "subscriber_id": sub_bank_id, "topics": ["stocks.bank"]})
        assert _recv_json(sub_bank)["type"] == "ok"

        # sub_both subscribes to both topics
        _send_json(sub_both, {"cmd": "subscribe", "subscriber_id": sub_both_id, "topics": ["stocks.tech", "stocks.bank"]})
        assert _recv_json(sub_both)["type"] == "ok"

        # Publish to stocks.tech
        _send_json(pub, {"cmd": "publish", "topic": "stocks.tech", "payload": "AAPL=150"})
        resp = _recv_json(pub)
        assert resp["type"] == "ok"
        assert resp["routed"] == 2  # sub_tech and sub_both

        # sub_tech receives it
        msg = _recv_json(sub_tech)
        assert msg["type"] == "deliver"
        assert msg["topic"] == "stocks.tech"
        assert msg["payload"] == "AAPL=150"

        # sub_both receives it
        msg = _recv_json(sub_both)
        assert msg["type"] == "deliver"
        assert msg["topic"] == "stocks.tech"

        # sub_bank should NOT receive it (within timeout)
        sub_bank.settimeout(0.3)
        try:
            sub_bank.recv(1)
            assert False, "sub_bank should not receive tech message"
        except socket.timeout:
            pass

        # Now publish to stocks.bank
        _send_json(pub, {"cmd": "publish", "topic": "stocks.bank", "payload": "ICBC=10"})
        resp = _recv_json(pub)
        assert resp["type"] == "ok"
        assert resp["routed"] == 2  # sub_bank and sub_both

        # sub_bank receives it
        msg = _recv_json(sub_bank)
        assert msg["type"] == "deliver"
        assert msg["topic"] == "stocks.bank"
        assert msg["payload"] == "ICBC=10"

        # sub_both receives it
        msg = _recv_json(sub_both)
        assert msg["type"] == "deliver"
        assert msg["topic"] == "stocks.bank"

        # sub_tech should NOT receive it
        sub_tech.settimeout(0.3)
        try:
            sub_tech.recv(1)
            assert False, "sub_tech should not receive bank message"
        except socket.timeout:
            pass

    finally:
        for conn in [sub_tech, sub_bank, sub_both, pub]:
            try:
                conn.close()
            except OSError:
                pass
        broker.stop()
        _restore_queues(queue_dir)


def test_wildcard_topic_routing() -> None:
    """Verify exact topic matching (no wildcard support in this scope)."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, heartbeat_timeout=5.0, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub1_id = f"sub1_{uuid.uuid4().hex[:8]}"
    sub2_id = f"sub2_{uuid.uuid4().hex[:8]}"
    
    sub1 = socket.create_connection((broker.host, broker.port), timeout=2)
    sub2 = socket.create_connection((broker.host, broker.port), timeout=2)
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        # sub1 subscribes to "stocks.tech"
        _send_json(sub1, {"cmd": "subscribe", "subscriber_id": sub1_id, "topics": ["stocks.tech"]})
        assert _recv_json(sub1)["type"] == "ok"

        # sub2 subscribes to "stocks" (different topic)
        _send_json(sub2, {"cmd": "subscribe", "subscriber_id": sub2_id, "topics": ["stocks"]})
        assert _recv_json(sub2)["type"] == "ok"

        # Publish to "stocks.tech"
        _send_json(pub, {"cmd": "publish", "topic": "stocks.tech", "payload": "MSFT=400"})
        resp = _recv_json(pub)
        assert resp["type"] == "ok"
        assert resp["routed"] == 1  # Only sub1

        msg = _recv_json(sub1)
        assert msg["type"] == "deliver"
        assert msg["topic"] == "stocks.tech"

        # sub2 should NOT receive (exact match required)
        sub2.settimeout(0.3)
        try:
            sub2.recv(1)
            assert False, "sub2 should not receive (topic mismatch)"
        except socket.timeout:
            pass

    finally:
        for conn in [sub1, sub2, pub]:
            try:
                conn.close()
            except OSError:
                pass
        broker.stop()
        _restore_queues(queue_dir)


def test_no_message_leak_across_topics() -> None:
    """Verify messages don't leak between different topic trees."""
    queue_dir = _clear_queues()
    broker = BrokerServer(port=0, heartbeat_timeout=5.0, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub_news_id = f"sub_news_{uuid.uuid4().hex[:8]}"
    sub_alerts_id = f"sub_alerts_{uuid.uuid4().hex[:8]}"
    
    sub_news = socket.create_connection((broker.host, broker.port), timeout=2)
    sub_alerts = socket.create_connection((broker.host, broker.port), timeout=2)
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        _send_json(sub_news, {"cmd": "subscribe", "subscriber_id": sub_news_id, "topics": ["news.market"]})
        assert _recv_json(sub_news)["type"] == "ok"

        _send_json(sub_alerts, {"cmd": "subscribe", "subscriber_id": sub_alerts_id, "topics": ["alerts.price"]})
        assert _recv_json(sub_alerts)["type"] == "ok"

        # Publish multiple messages to different topics
        for i in range(5):
            _send_json(pub, {"cmd": "publish", "topic": "news.market", "payload": f"Market update {i}"})
            _recv_json(pub)  # consume response

        # sub_news should receive 5 messages
        for i in range(5):
            msg = _recv_json(sub_news)
            assert msg["type"] == "deliver"
            assert msg["topic"] == "news.market"

        # sub_alerts should have received nothing
        try:
            unexpected = _recv_json(sub_alerts, timeout=0.3)
            assert unexpected is None, f"sub_alerts should not receive news messages: {unexpected}"
        except socket.timeout:
            pass

    finally:
        for conn in [sub_news, sub_alerts, pub]:
            try:
                conn.close()
            except OSError:
                pass
        broker.stop()
        _restore_queues(queue_dir)