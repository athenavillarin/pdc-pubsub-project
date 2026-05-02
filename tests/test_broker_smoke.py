from __future__ import annotations

import json
import socket
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from broker.broker import BrokerServer


def _send_json(conn: socket.socket, payload: dict[str, object]) -> None:
    conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))


def _recv_json(conn: socket.socket, timeout: float = 2.0) -> dict[str, object] | None:
    conn.settimeout(timeout)
    data = b""
    while b"\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            return None
        data += chunk
    line, _, _ = data.partition(b"\n")
    return json.loads(line.decode("utf-8"))


def test_broker_scope_smoke() -> None:
    broker = BrokerServer(port=0, heartbeat_timeout=0.8, maintenance_interval=0.1)
    broker.start()
    time.sleep(0.1)

    sub1 = socket.create_connection((broker.host, broker.port), timeout=2)
    sub2 = socket.create_connection((broker.host, broker.port), timeout=2)
    pub = socket.create_connection((broker.host, broker.port), timeout=2)

    try:
        _send_json(sub1, {"cmd": "subscribe", "subscriber_id": "sub1", "topics": ["stocks.tech"]})
        assert _recv_json(sub1)["type"] == "ok"

        _send_json(sub2, {"cmd": "subscribe", "subscriber_id": "sub2", "topics": ["stocks.bank"]})
        assert _recv_json(sub2)["type"] == "ok"

        _send_json(pub, {"cmd": "publish", "topic": "stocks.tech", "payload": "AAPL=205"})
        publish_response = _recv_json(pub)
        assert publish_response is not None
        assert publish_response["type"] == "ok"
        assert publish_response["routed"] == 1

        deliver = _recv_json(sub1)
        assert deliver is not None
        assert deliver["type"] == "deliver"
        assert deliver["topic"] == "stocks.tech"
        assert deliver["payload"] == "AAPL=205"

        sub2.settimeout(0.4)
        no_cross_topic = True
        try:
            sub2.recv(1)
            no_cross_topic = False
        except Exception:
            no_cross_topic = True
        assert no_cross_topic is True

        _send_json(sub1, {"cmd": "heartbeat", "subscriber_id": "sub1"})
        assert _recv_json(sub1)["type"] == "ok"

        time.sleep(0.4)
        assert broker.is_subscriber_online("sub1") is True

        time.sleep(1.0)
        assert broker.is_subscriber_online("sub2") is False

        sub1.close()
        time.sleep(0.3)
        assert broker.is_subscriber_online("sub1") is False
    finally:
        try:
            sub1.close()
        except OSError:
            pass
        try:
            sub2.close()
        except OSError:
            pass
        try:
            pub.close()
        except OSError:
            pass
        broker.stop()
