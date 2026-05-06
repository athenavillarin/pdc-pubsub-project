#!/usr/bin/env python3
"""Publisher console for the stock alert system.

This script provides an interactive prompt for publishing messages
to the broker. It's used for live demonstrations of the pub/sub system.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from dataclasses import dataclass

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from clients.publisher import Publisher


@dataclass(frozen=True)
class PresetMessage:
    label: str
    publisher: str
    topic: str
    message: str


PRESET_MESSAGES: tuple[PresetMessage, ...] = (
    PresetMessage(
        label="Tech: GCash to the Moon",
        publisher="market",
        topic="stocks.tech",
        message="GCash valuation hits new record! Everyone in Binondo is buying.",
    ),
    PresetMessage(
        label="Bank: BDO 'Finds Ways'",
        publisher="market",
        topic="stocks.bank",
        message="BDO 'Finds Ways' to increase service fees. Stock price up +1.5%.",
    ),
    PresetMessage(
        label="Alert: GCash Cash-In DOWN",
        publisher="alert",
        topic="alerts.high",
        message="RED ALERT: 7-Eleven GCash Cash-in is DOWN. Repeat: No G-Xchange today!",
    ),
    PresetMessage(
        label="Alert: BPI Maintenance",
        publisher="alert",
        topic="alerts.low",
        message="System Note: BPI Maintenance at 10 PM. Move your 'Sahod' now.",
    ),
)


def _publish_message(publisher: Publisher, topic: str, message: str) -> None:
    """Publish a single message and keep the demo output simple."""
    return publisher.publish(topic, message)


def _print_banner(host: str, port: int) -> None:
    print()
    print("╔" + "═" * 68 + "╗")
    print("║" + " STOCK ALERT CONSOLE".ljust(68) + "║")
    print("║" + f" Connected broker: {host}:{port}".ljust(68) + "║")
    print("║" + " Use the menu below to send market updates or alert messages.".ljust(68) + "║")
    print("╚" + "═" * 68 + "╝")
    print()


def _print_menu() -> None:
    print("Available actions")
    print("  1) Send market update")
    print("  2) Send alert")
    print("  3) Send preset message")
    print("  4) Send custom message")
    print("  5) Quit")
    print()


def _choose_preset() -> PresetMessage | None:
    print("Preset messages")
    for index, preset in enumerate(PRESET_MESSAGES, start=1):
        print(f"  {index}) {preset.label} [{preset.topic}]")
    print()

    selection = input(f"Choose a preset [1-{len(PRESET_MESSAGES)}]: ").strip()
    if not selection.isdigit():
        return None

    index = int(selection)
    if 1 <= index <= len(PRESET_MESSAGES):
        return PRESET_MESSAGES[index - 1]
    return None


def _prompt_non_empty(prompt: str, default: str | None = None) -> str:
    while True:
        value = input(prompt).strip()
        if value:
            return value
        if default is not None:
            return default
        print("Please enter a value.")


def _publish_with_status(publisher: Publisher, topic: str, message: str) -> None:
    print(f"[SYSTEM] Sending message to {topic!r}...")
    routed = _publish_message(publisher, topic, message)
    print(f"[SYSTEM] Delivery confirmed. Routed to {routed} subscriber(s).")


def publisher_console(broker_host: str = "127.0.0.1", broker_port: int = 9000) -> None:
    """Run a dedicated publisher terminal for live presentations."""
    _print_banner(broker_host, broker_port)

    market_pub = Publisher(host=broker_host, port=broker_port)
    alert_pub = Publisher(host=broker_host, port=broker_port)

    try:
        while True:
            try:
                market_pub.connect()
                alert_pub.connect()
                break
            except ConnectionRefusedError:
                print("[SYSTEM] Broker is not ready yet. Retrying in 1 second...")
                time.sleep(1.0)

        print("[SYSTEM] Ready. This console can publish live market and alert messages.")
        print("[SYSTEM] Presets are tuned for a clean demo flow; custom messages are still available.")
        print()

        while True:
            _print_menu()
            choice = input("Select an action [1-5]: ").strip()

            if choice == "1":
                topic = _prompt_non_empty("Topic [stocks.tech/stocks.bank] [stocks.tech]: ", "stocks.tech")
                message = _prompt_non_empty("Market message: ")
                _publish_with_status(market_pub, topic, message)

            elif choice == "2":
                topic = _prompt_non_empty("Topic [alerts.high/alerts.low] [alerts.high]: ", "alerts.high")
                message = _prompt_non_empty("Alert message: ")
                _publish_with_status(alert_pub, topic, message)

            elif choice == "3":
                preset = _choose_preset()
                if preset is None:
                    print("[SYSTEM] Invalid preset selection.")
                else:
                    publisher = alert_pub if preset.publisher == "alert" else market_pub
                    _publish_with_status(publisher, preset.topic, preset.message)

            elif choice == "4":
                publisher_choice = _prompt_non_empty("Publisher [1 market / 2 alert] [1]: ", "1")
                publisher = alert_pub if publisher_choice == "2" else market_pub
                topic = _prompt_non_empty("Topic: ")
                message = _prompt_non_empty("Message: ")
                _publish_with_status(publisher, topic, message)

            elif choice == "5":
                print("[SYSTEM] Closing publisher console.")
                break

            else:
                print("[SYSTEM] Please choose 1, 2, 3, 4, or 5.")

            print()

    finally:
        market_pub.disconnect()
        alert_pub.disconnect()


def main() -> None:
    """Run the demo with optional arguments."""
    parser = argparse.ArgumentParser(description="Stock Alert System Publisher Console")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=9000, help="Broker port")
    args = parser.parse_args()

    try:
        publisher_console(broker_host=args.host, broker_port=args.port)
    except KeyboardInterrupt:
        print("\n[SYSTEM] Publisher interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[SYSTEM] Publisher failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()