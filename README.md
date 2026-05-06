# PDC Pub-Sub Project

Topic-based publish/subscribe system built in Python. The system has three main parts:

1. A TCP broker that routes messages by topic.
2. A publisher client that sends messages into the broker.
3. A subscriber client that receives topic deliveries, sends heartbeats, and reconnects automatically.

The broker also persists messages for offline subscribers so they can be replayed after reconnection.

## Overview

The broker speaks newline-delimited JSON over TCP. Clients connect to the broker, send commands such as `subscribe`, `publish`, `heartbeat`, and `ack`, and receive `deliver` events for matching topics.

The routing model is exact-match only: a subscriber must subscribe to the exact topic string that a publisher uses. For example, `stocks.tech` and `stocks.bank` are separate topics.

## Project Layout

- [broker/broker.py](broker/broker.py) contains the broker server, topic registry, heartbeat tracking, ACK handling, and retry loop.
- [clients/publisher.py](clients/publisher.py) contains the publisher CLI/client.
- [clients/subscriber.py](clients/subscriber.py) contains the subscriber CLI/client with reconnect and heartbeat logic.
- [queues/persistent_queue.py](queues/persistent_queue.py) stores offline deliveries in JSONL files under the `queues/` directory.
- [demo/stock_alert_scenario.py](demo/stock_alert_scenario.py) runs a stock-alert walkthrough with multiple publishers and subscribers.
- [tests/](tests) contains smoke, routing, delivery, and reconnection coverage.

## Core Features

### Broker

The broker supports:

- TCP server lifecycle management: start, accept, handle requests, and stop cleanly.
- Topic subscription tracking through a topic-to-subscriber registry and a subscriber-to-topic registry.
- Exact topic routing for `publish` commands.
- Subscriber online/offline tracking using heartbeats and disconnect cleanup.
- Per-message ACK tracking with `message_id` values.
- Retries for unacknowledged live deliveries.
- Persistent queue replay for messages published while a subscriber is offline.

### Publisher

The publisher is a thin client that connects to the broker and sends a topic plus message payload. It waits for the broker response and prints how many subscribers the message was routed to.

### Subscriber

The subscriber:

- Connects to the broker and subscribes to one or more topics.
- Receives `deliver` events and prints them.
- Sends `ack` messages back to the broker after each delivery.
- Sends periodic heartbeats to stay marked online.
- Reconnects automatically if the connection drops.
- Replays queued messages on reconnection.

### Persistence

Offline messages are written to per-subscriber JSONL files in the `queues/` folder. The queue survives process restarts because it is file-backed.

## Broker Protocol

Transport:

- TCP
- newline-delimited JSON

Supported client commands:

- `subscribe`
- `unsubscribe`
- `publish`
- `heartbeat`
- `ack`

Broker responses and events:

- `ok` for successful commands
- `error` for invalid requests
- `deliver` for routed messages

### Command Shapes

Subscribe:

```json
{"cmd":"subscribe","subscriber_id":"sub1","topics":["stocks.tech","alerts.high"]}
```

Unsubscribe:

```json
{"cmd":"unsubscribe","subscriber_id":"sub1","topics":["stocks.tech"]}
```

Publish:

```json
{"cmd":"publish","topic":"stocks.tech","payload":"AAPL: $180.50"}
```

Heartbeat:

```json
{"cmd":"heartbeat","subscriber_id":"sub1"}
```

ACK:

```json
{"cmd":"ack","subscriber_id":"sub1","message_id":"<uuid>"}
```

### Delivery Shape

Delivered messages look like this:

```json
{
  "type": "deliver",
  "message_id": "<uuid>",
  "topic": "stocks.tech",
  "payload": "AAPL: $180.50",
  "duplicate": false,
  "from_queue": false
}
```

Fields:

- `message_id` identifies the delivery for ACK tracking.
- `duplicate` becomes `true` when the broker redelivers an unacked message.
- `from_queue` becomes `true` when the delivery came from the offline persistence queue.

## Delivery Semantics

The system provides at-least-once delivery:

- Every routed message gets a unique `message_id`.
- The broker stores pending deliveries until the subscriber ACKs them.
- If an ACK does not arrive before `ack_timeout`, the broker retries the delivery.
- If a subscriber is offline, published messages are written to the persistent queue and replayed on the next subscription.

This means subscribers should be prepared to handle duplicates.

## Running The System

### Start The Broker

From the project root:

```bash
python broker/broker.py
```

The broker prints the host and port it is listening on. By default it uses `127.0.0.1:9000` when started from the CLI.

### Run A Publisher

```bash
python clients/publisher.py --topic stocks.tech --message "AAPL: $180.50"
```

Optional flags:

- `--host` to point at a different broker host.
- `--port` to point at a different broker port.

### Run A Subscriber

```bash
python clients/subscriber.py --id trader --topics stocks.tech alerts.high
```

Optional flags:

- `--host` to point at a different broker host.
- `--port` to point at a different broker port.
- `--topics` accepts one or more topic names.

## Demo Scenario

The demo script launches a broker, two publishers, and three subscribers, then publishes a stock-alert sequence that shows routing, offline queueing, and reconnection replay.

Run it from the project root:

```bash
python demo/stock_alert_scenario.py
```

You can also override the broker host and port:

```bash
python demo/stock_alert_scenario.py --host 127.0.0.1 --port 9000
```

## Tests

Run the test suite with:

```bash
python -m pytest
```

Coverage includes:

- Broker smoke behavior.
- Topic routing.
- ACK handling and redelivery.
- Offline persistence and replay.
- Subscriber reconnection behavior.

## Notes On The Queue Files

The broker writes subscriber queue files into the `queues/` directory. These files are part of runtime state and may change when the system runs. If you want a clean test/demo run, clear or isolate that directory first.

## Configuration Defaults

- Broker host: `127.0.0.1`
- Broker port: `9000` for the CLI clients and demo
- Heartbeat interval: `2.0` seconds in the subscriber client
- Heartbeat timeout: `5.0` seconds in the broker by default
- ACK timeout: `1.0` second in the broker by default

## Example Flow

1. Start the broker.
2. Start a subscriber for `stocks.tech`.
3. Publish a message to `stocks.tech`.
4. The broker delivers the message to the subscriber.
5. The subscriber ACKs the delivery.
6. If the subscriber goes offline, the broker queues future messages and replays them after reconnect.

## Troubleshooting

- If a client cannot connect, verify the broker is running and the host/port match.
- If a subscriber appears offline too quickly, make sure heartbeats are still being sent and that the subscriber process is not blocked.
- If messages are replayed more than once, that is expected until the ACK reaches the broker.

