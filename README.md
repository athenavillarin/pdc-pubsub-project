# PDC Pub-Sub Project (Broker Scope)

This README documents only implemented in this branch:

- Broker server
- Subscription registry
- Topic routing
- Online/offline tracking

## Implemented 

The broker is implemented in broker/broker.py with these features:

- TCP server lifecycle: start, accept client connections, handle requests, graceful stop.
- Subscription registry:
	- topic_subscribers map for topic-to-subscriber lookups.
	- subscriber_topics map for subscriber-to-topic tracking.
- Topic routing:
	- publish command routes message only to subscribers of matching topic.
	- unsubscribe removes routing entries.
- Online/offline tracking:
	- subscriber sessions tracked by subscriber_id.
	- heartbeat updates last_seen timestamp.
	- maintenance loop marks inactive subscribers offline when heartbeat timeout is exceeded.
	- disconnect cleanup marks subscriber offline and closes session.

## Broker Protocol 

Transport: newline-delimited JSON over TCP.

Supported commands in this scope:

- subscribe
- unsubscribe
- publish
- heartbeat

Delivery events sent by broker:

- deliver with topic and payload


## Run Broker

From project root:

python broker/broker.py

The broker will print its listening host and port.

## Message IDs and Acknowledgements

- The broker assigns a unique `message_id` (UUID) to every routed delivery.
- Each `deliver` event sent to a subscriber includes `message_id` and a `deliver` type.
- Subscribers MUST acknowledge receipt by sending an `ack` command with the same
	`subscriber_id` and `message_id`.
- The broker keeps `pending_acks` for in-flight deliveries and treats messages as
	delivered only after a valid `ack` is received (at-least-once semantics).
- For offline subscribers the broker persists deliveries in the per-subscriber
	persistent queue; persisted items include `message_id` and attempt counters.
- A maintenance loop retries unacked deliveries after an `ack_timeout`; each retry
	increments the attempt counter. Persisted messages remain until acknowledged.

This design provides at-least-once delivery: the broker may redeliver messages
until a subscriber acknowledges them, and persisted messages survive subscriber
disconnects for later replay.
