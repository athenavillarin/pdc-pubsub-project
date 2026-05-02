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
