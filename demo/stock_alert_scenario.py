#!/usr/bin/env python3
"""Demo scenario — stock alert system with 2 publishers and 3 subscribers across different topics.

This demo shows:
- Multiple publishers (market data, price alerts)
- Multiple subscribers (traders, risk managers, analysts)
- Topic-based routing (stocks.tech, stocks.bank, market.news)
- At-least-once delivery with persistence
- Heartbeats to maintain connection
- Graceful handling of offline periods

Run this script from the project root.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from broker.broker import BrokerServer
from clients.publisher import Publisher
from clients.subscriber import Subscriber


def _send_json(conn: socket.socket, payload: dict) -> None:
    """Helper to send JSON over socket."""
    conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))


def _recv_json(conn: socket.socket, timeout: float = 2.0) -> dict | None:
    """Helper to receive JSON from socket."""
    conn.settimeout(timeout)
    data = b""
    while b"\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            return None
        data += chunk
    line, _, _ = data.partition(b"\n")
    return json.loads(line.decode("utf-8"))


def demo_scenario(broker_host: str = "127.0.0.1", broker_port: int = 9000) -> None:
    """Run the stock alert demo.
    
    Participants:
    - Publisher 1: Market data feed (stocks.tech, stocks.bank)
    - Publisher 2: Price alerts (alerts.high, alerts.low)
    - Subscriber 1: Tech trader (stocks.tech, alerts.high)
    - Subscriber 2: Bank analyst (stocks.bank)
    - Subscriber 3: Risk manager (alerts.high, alerts.low)
    """
    print("\n" + "=" * 70)
    print("STOCK ALERT SYSTEM DEMO")
    print("=" * 70)
    print()

    # Start broker
    print("[BROKER] Starting broker...")
    broker = BrokerServer(host=broker_host, port=broker_port, 
                         heartbeat_timeout=5.0, maintenance_interval=0.2)
    broker.start()
    time.sleep(0.2)
    print(f"[BROKER] Listening on {broker.host}:{broker.port}\n")

    # Create publishers
    print("[PUBLISHERS] Creating publishers...")
    market_pub = Publisher(host=broker_host, port=broker.port)
    alert_pub = Publisher(host=broker_host, port=broker.port)
    
    try:
        market_pub.connect()
        alert_pub.connect()
    except ConnectionRefusedError as e:
        print(f"[ERROR] Could not connect to broker: {e}")
        alert_pub.disconnect()
        market_pub.disconnect()
        broker.stop()
        return

    print()

    # Create and start subscribers in background threads
    print("[SUBSCRIBERS] Starting subscribers...")
    
    subscribers = {
        "trader": Subscriber("trader", ["stocks.tech", "alerts.high"],
                              host=broker_host, port=broker.port),
        "analyst": Subscriber("analyst", ["stocks.bank"],
                               host=broker_host, port=broker.port),
        "risk_mgr": Subscriber("risk_mgr", ["alerts.high", "alerts.low"],
                                host=broker_host, port=broker.port),
    }

    sub_threads = {}
    for name, sub in subscribers.items():
        thread = threading.Thread(target=_run_subscriber_loop, args=(sub, name), daemon=True)
        thread.start()
        sub_threads[name] = thread
        time.sleep(0.1)

    print()

    # Run publishing sequence
    print("[DEMO] Publishing messages...\n")
    
    # Market data publishers
    print("--- MARKET DATA UPDATES ---")
    market_pub.publish("stocks.tech", "AAPL: $180.50 (+2.1%)")
    time.sleep(0.3)
    market_pub.publish("stocks.tech", "MSFT: $420.30 (+1.5%)")
    time.sleep(0.3)
    market_pub.publish("stocks.bank", "JPM: $175.80 (+0.8%)")
    time.sleep(0.3)
    market_pub.publish("stocks.bank", "ICBC: $8.90 (-1.2%)")
    time.sleep(0.3)

    print("\n--- PRICE ALERTS ---")
    alert_pub.publish("alerts.high", "⚠️  AAPL reached $180 - HIGH ALERT")
    time.sleep(0.3)
    alert_pub.publish("alerts.low", "⚠️  ICBC dropped to $8.90 - LOW ALERT")
    time.sleep(0.3)

    print("\n--- MORE MARKET UPDATES ---")
    market_pub.publish("stocks.tech", "GOOGL: $140.20 (-0.5%)")
    time.sleep(0.3)
    alert_pub.publish("alerts.high", "⚠️  MSFT reached $420 - HIGH ALERT")
    time.sleep(0.3)

    # Let subscribers process messages
    time.sleep(1.0)

    # Simulate subscriber going offline
    print("\n" + "=" * 70)
    print("[DEMO] Simulating analyst going offline...")
    print("=" * 70)
    subscribers["analyst"].stop()
    time.sleep(0.5)

    print("\n[DEMO] Publishing while analyst is offline...")
    market_pub.publish("stocks.bank", "HSBC: $45.30 (+2.0%)")
    time.sleep(0.3)
    market_pub.publish("stocks.bank", "BAIDU: $125.10 (-3.2%)")
    time.sleep(0.3)

    # Reconnect analyst
    print("\n[DEMO] Analyst reconnecting (will receive offline messages)...\n")
    time.sleep(0.5)
    subscribers["analyst"] = Subscriber("analyst", ["stocks.bank"],
                                         host=broker_host, port=broker.port)
    thread = threading.Thread(target=_run_subscriber_loop, args=(subscribers["analyst"], "analyst"), daemon=True)
    thread.start()
    time.sleep(1.0)

    # Final messages
    print("--- FINAL MARKET UPDATES ---")
    market_pub.publish("stocks.tech", "NVDA: $900.00 (NEW HIGH!)")
    time.sleep(0.3)
    alert_pub.publish("alerts.high", "🎉 NVDA NEW HIGH - $900.00")
    time.sleep(0.3)

    market_pub.publish("stocks.bank", "WFC: $65.40 (+1.1%)")
    time.sleep(0.3)

    # Let everything settle
    time.sleep(1.0)

    print("\n" + "=" * 70)
    print("[DEMO] Demo complete!")
    print("=" * 70)
    print()

    # Cleanup
    print("[CLEANUP] Disconnecting publishers...")
    market_pub.disconnect()
    alert_pub.disconnect()

    print("[CLEANUP] Disconnecting subscribers...")
    for sub in subscribers.values():
        sub.stop()

    print("[CLEANUP] Stopping broker...")
    broker.stop()
    print("[CLEANUP] Done.\n")


def _run_subscriber_loop(subscriber: Subscriber, name: str) -> None:
    """Run subscriber in a background loop.
    
    Handles:
    - Initial connection and subscription
    - Message reception
    - Auto-reconnection on disconnect
    - Heartbeats to stay online
    """
    subscriber.start()


def main() -> None:
    """Run the demo with optional arguments."""
    parser = argparse.ArgumentParser(description="Stock Alert System Demo")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=9000, help="Broker port")
    args = parser.parse_args()

    try:
        demo_scenario(broker_host=args.host, broker_port=args.port)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Demo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Demo failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()