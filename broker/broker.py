"""Broker server for topic-based pub-sub.

Scope in this file:
- Broker server lifecycle
- Subscription registry
- Topic routing
- Online/offline tracking
"""

from __future__ import annotations

import json
import socket
import sys
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from queues.persistent_queue import PersistentQueue


@dataclass
class ClientSession:
	conn: socket.socket
	addr: tuple[str, int]
	subscriber_id: str | None = None
	lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class PendingDelivery:
	subscriber_id: str
	message_id: str
	topic: str
	payload: str
	deadline: float
	attempts: int = 1
	persisted: bool = False


class BrokerServer:
	"""Broker that accepts subscribe/publish/heartbeat commands over TCP JSON lines."""

	def __init__(
		self,
		host: str = "127.0.0.1",
		port: int = 0,
		ack_timeout: float = 5.0,
		heartbeat_timeout: float = 5.0,
		maintenance_interval: float = 0.2,
	) -> None:
		self.host = host
		self.port = port
		self.ack_timeout = ack_timeout
		self.heartbeat_timeout = heartbeat_timeout
		self.maintenance_interval = maintenance_interval

		self._lock = threading.RLock()
		self._running = threading.Event()
		self._socket: socket.socket | None = None
		self._accept_thread: threading.Thread | None = None
		self._maintenance_thread: threading.Thread | None = None
		self._client_threads: set[threading.Thread] = set()
		self.queue = PersistentQueue()

		# Subscription registry
		self.topic_subscribers: dict[str, set[str]] = defaultdict(set)
		self.subscriber_topics: dict[str, set[str]] = defaultdict(set)

		# Online/offline tracking
		self.subscriber_sessions: dict[str, ClientSession] = {}
		self.online_status: dict[str, dict[str, Any]] = {}
		self.conn_to_subscriber: dict[socket.socket, str] = {}
		self.pending_acks: dict[tuple[str, str], PendingDelivery] = {}

	def start(self) -> None:
		if self._running.is_set():
			return
		self._running.set()

		self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self._socket.bind((self.host, self.port))
		self._socket.listen(100)
		self._socket.settimeout(0.5)
		self.port = self._socket.getsockname()[1]

		self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
		self._maintenance_thread = threading.Thread(target=self._maintenance_loop, daemon=True)
		self._accept_thread.start()
		self._maintenance_thread.start()

	def stop(self) -> None:
		self._running.clear()
		if self._socket:
			try:
				self._socket.close()
			except OSError:
				pass

		with self._lock:
			sessions = list(self.subscriber_sessions.values())
		for session in sessions:
			try:
				session.conn.shutdown(socket.SHUT_RDWR)
			except OSError:
				pass
			try:
				session.conn.close()
			except OSError:
				pass

		if self._accept_thread:
			self._accept_thread.join(timeout=2)
		if self._maintenance_thread:
			self._maintenance_thread.join(timeout=2)

		for thread in list(self._client_threads):
			thread.join(timeout=1)

	def _accept_loop(self) -> None:
		assert self._socket is not None
		while self._running.is_set():
			try:
				conn, addr = self._socket.accept()
			except socket.timeout:
				continue
			except OSError:
				break

			thread = threading.Thread(target=self._handle_connection, args=(conn, addr), daemon=True)
			self._client_threads.add(thread)
			thread.start()

	def _handle_connection(self, conn: socket.socket, addr: tuple[str, int]) -> None:
		print(f"[BROKER] Connection from {addr}")
		conn.settimeout(None)  # Set to blocking mode
		buffer = b""
		try:
			while self._running.is_set():
				try:
					# Read available data from socket
					print(f"[BROKER] Waiting for data from {addr}...", flush=True)
					data = conn.recv(4096)
					print(f"[BROKER] Received {len(data)} bytes from {addr}: {data[:100]}", flush=True)
					if not data:
						# Connection closed by client
						print(f"[BROKER] EOF from {addr}", flush=True)
						break
					buffer += data
				except socket.timeout:
					continue
				except (OSError, ConnectionResetError) as e:
					print(f"[BROKER] Socket error from {addr}: {e}", flush=True)
					break

				# Process all complete lines in buffer
				while b"\n" in buffer:
					line, buffer = buffer.split(b"\n", 1)
					line = line.strip()
					if not line:
						continue
					
					try:
						message = json.loads(line.decode("utf-8"))
						print(f"[BROKER] Parsed message from {addr}: {message}", flush=True)
					except (json.JSONDecodeError, UnicodeDecodeError) as e:
						print(f"[BROKER] JSON error from {addr}: {e}", flush=True)
						self._send_json_raw(conn, {"type": "error", "message": "invalid-json"})
						continue

					print(f"[BROKER] Dispatching message from {addr}", flush=True)
					response = self._dispatch(message, conn, addr)
					if response is not None:
						self._send_json_raw(conn, response)
		except Exception as e:
			print(f"[BROKER] Exception in _handle_connection: {e}", flush=True)
		finally:
			print(f"[BROKER] Closing connection from {addr}", flush=True)
			self._cleanup_connection(conn)
			try:
				conn.close()
			except OSError:
				pass

	def _dispatch(self, message: dict[str, Any], conn: socket.socket, addr: tuple[str, int]) -> dict[str, Any] | None:
		cmd = message.get("cmd")

		if cmd == "subscribe":
			subscriber_id = str(message.get("subscriber_id", "")).strip()
			topics = message.get("topics") or []
			if not subscriber_id:
				return {"type": "error", "message": "missing-subscriber-id"}
			if not isinstance(topics, list):
				return {"type": "error", "message": "topics-must-be-list"}
			return self._handle_subscribe(conn, addr, subscriber_id, [str(t) for t in topics])

		if cmd == "unsubscribe":
			subscriber_id = str(message.get("subscriber_id", "")).strip()
			topics = message.get("topics") or []
			if not subscriber_id:
				return {"type": "error", "message": "missing-subscriber-id"}
			return self._handle_unsubscribe(subscriber_id, [str(t) for t in topics])

		if cmd == "publish":
			topic = str(message.get("topic", "")).strip()
			payload = str(message.get("payload", ""))
			if not topic:
				return {"type": "error", "message": "missing-topic"}
			routed = self._route_message(topic, payload)
			return {"type": "ok", "cmd": "publish", "routed": routed}

		if cmd == "heartbeat":
			subscriber_id = str(message.get("subscriber_id", "")).strip()
			if not subscriber_id:
				return {"type": "error", "message": "missing-subscriber-id"}
			self._touch_subscriber(subscriber_id)
			return {"type": "ok", "cmd": "heartbeat"}

		if cmd == "ack":
			subscriber_id = str(message.get("subscriber_id", "")).strip()
			message_id = str(message.get("message_id", "")).strip()
			if not subscriber_id or not message_id:
				return {"type": "error", "message": "missing-ack-fields"}
			self._handle_ack(subscriber_id, message_id)
			return {"type": "ok", "cmd": "ack", "message_id": message_id}

		return {"type": "error", "message": "unknown-command"}

	def _handle_subscribe(
		self,
		conn: socket.socket,
		addr: tuple[str, int],
		subscriber_id: str,
		topics: list[str],
	) -> dict[str, Any]:
		clean_topics = {topic for topic in topics if topic}
		with self._lock:
			existing = self.subscriber_sessions.get(subscriber_id)
			if existing and existing.conn is not conn:
				try:
					existing.conn.shutdown(socket.SHUT_RDWR)
				except OSError:
					pass
				try:
					existing.conn.close()
				except OSError:
					pass

			session = ClientSession(conn=conn, addr=addr, subscriber_id=subscriber_id)
			self.subscriber_sessions[subscriber_id] = session
			self.conn_to_subscriber[conn] = subscriber_id

			for topic in clean_topics:
				self.topic_subscribers[topic].add(subscriber_id)
				self.subscriber_topics[subscriber_id].add(topic)

			self.online_status[subscriber_id] = {"online": True, "last_seen": time.time()}

		return {
			"type": "ok",
			"cmd": "subscribe",
			"subscriber_id": subscriber_id,
			"topics": sorted(list(self.subscriber_topics.get(subscriber_id, set()))),
		}

	def _handle_unsubscribe(self, subscriber_id: str, topics: list[str]) -> dict[str, Any]:
		removed = 0
		with self._lock:
			if subscriber_id not in self.subscriber_topics:
				return {"type": "ok", "cmd": "unsubscribe", "removed": removed}

			if not topics:
				topics = list(self.subscriber_topics[subscriber_id])

			for topic in topics:
				if topic in self.subscriber_topics[subscriber_id]:
					self.subscriber_topics[subscriber_id].discard(topic)
					self.topic_subscribers[topic].discard(subscriber_id)
					removed += 1

		return {"type": "ok", "cmd": "unsubscribe", "removed": removed}

	def _route_message(self, topic: str, payload: str) -> int:
		with self._lock:
			subscribers = list(self.topic_subscribers.get(topic, set()))
		message_id = str(uuid.uuid4())

		routed = 0
		for subscriber_id in subscribers:
			if self.is_subscriber_online(subscriber_id):
				ok = self._send_to_subscriber(subscriber_id, topic, payload, message_id=message_id, persisted=False)
				if ok:
					routed += 1
			else:
				self.queue.enqueue(subscriber_id, topic, payload, message_id=message_id)
		return routed

	def _send_to_subscriber(
		self,
		subscriber_id: str,
		topic: str,
		payload: str,
		message_id: str,
		persisted: bool,
		duplicate: bool = False,
	) -> bool:
		with self._lock:
			session = self.subscriber_sessions.get(subscriber_id)
			if session is None:
				return False

		envelope = {
			"type": "deliver",
			"message_id": message_id,
			"topic": topic,
			"payload": payload,
			"duplicate": duplicate,
			"from_queue": persisted,
		}
		ok = self._send_json_session(session, envelope)
		if not ok:
			self._mark_offline(subscriber_id)
			return False

		with self._lock:
			self.pending_acks[(subscriber_id, message_id)] = PendingDelivery(
				subscriber_id=subscriber_id,
				message_id=message_id,
				topic=topic,
				payload=payload,
				deadline=time.time() + self.ack_timeout,
				persisted=persisted,
			)
		if persisted:
			self.queue.increment_attempt(subscriber_id, message_id)
		return True

	def _handle_ack(self, subscriber_id: str, message_id: str) -> None:
		with self._lock:
			pending = self.pending_acks.pop((subscriber_id, message_id), None)
		if pending and pending.persisted:
			self.queue.mark_acked(subscriber_id, message_id)

	def _touch_subscriber(self, subscriber_id: str) -> None:
		with self._lock:
			state = self.online_status.setdefault(subscriber_id, {"online": True, "last_seen": 0.0})
			state["online"] = True
			state["last_seen"] = time.time()

	def _cleanup_connection(self, conn: socket.socket) -> None:
		with self._lock:
			subscriber_id = self.conn_to_subscriber.pop(conn, None)
			if not subscriber_id:
				return

			# Avoid race condition: only clear session if it's the current one
			session = self.subscriber_sessions.get(subscriber_id)
			if session and session.conn is conn:
				self.subscriber_sessions.pop(subscriber_id, None)
				self.online_status[subscriber_id] = {"online": False, "last_seen": time.time()}

	def _mark_offline(self, subscriber_id: str) -> None:
		with self._lock:
			state = self.online_status.setdefault(subscriber_id, {"online": False, "last_seen": 0.0})
			state["online"] = False
			state["last_seen"] = time.time()

			session = self.subscriber_sessions.pop(subscriber_id, None)
			if session is not None:
				self.conn_to_subscriber.pop(session.conn, None)

			pending_keys = [key for key in self.pending_acks if key[0] == subscriber_id]
			for key in pending_keys:
				pending = self.pending_acks.pop(key)
				if not pending.persisted:
					self.queue.enqueue(
						subscriber_id,
						pending.topic,
						pending.payload,
						message_id=pending.message_id,
					)

		if session is not None:
			try:
				session.conn.shutdown(socket.SHUT_RDWR)
			except OSError:
				pass
			try:
				session.conn.close()
			except OSError:
				pass

	def _maintenance_loop(self) -> None:
		while self._running.is_set():
			self._expire_heartbeats()
			self._retry_expired_acks()
			time.sleep(self.maintenance_interval)

	def _expire_heartbeats(self) -> None:
		now = time.time()
		to_offline: list[str] = []
		with self._lock:
			for subscriber_id, state in self.online_status.items():
				if state.get("online") and now - float(state.get("last_seen", 0.0)) > self.heartbeat_timeout:
					to_offline.append(subscriber_id)
		for subscriber_id in to_offline:
			self._mark_offline(subscriber_id)

	def _retry_expired_acks(self) -> None:
		now = time.time()
		expired: list[PendingDelivery] = []
		with self._lock:
			for pending in self.pending_acks.values():
				if pending.deadline <= now:
					expired.append(pending)

		for pending in expired:
			with self._lock:
				still_pending = self.pending_acks.get((pending.subscriber_id, pending.message_id))
				if still_pending is None:
					continue
			if not self.is_subscriber_online(pending.subscriber_id):
				continue

			delivered = self._send_to_subscriber(
				pending.subscriber_id,
				pending.topic,
				pending.payload,
				message_id=pending.message_id,
				persisted=pending.persisted,
				duplicate=True,
			)
			if delivered:
				with self._lock:
					entry = self.pending_acks.get((pending.subscriber_id, pending.message_id))
					if entry is not None:
						entry.attempts += 1
						entry.deadline = time.time() + self.ack_timeout

	def _send_json_raw(self, conn: socket.socket, payload: dict[str, Any]) -> bool:
		data = (json.dumps(payload) + "\n").encode("utf-8")
		try:
			print(f"[BROKER] Sending: {payload}", flush=True)
			conn.sendall(data)
			print(f"[BROKER] Sent: {payload}", flush=True)
			return True
		except OSError as e:
			print(f"[BROKER] Send failed: {e}", flush=True)
			return False

	def _send_json_session(self, session: ClientSession, payload: dict[str, Any]) -> bool:
		with session.lock:
			return self._send_json_raw(session.conn, payload)

	def is_subscriber_online(self, subscriber_id: str) -> bool:
		with self._lock:
			state = self.online_status.get(subscriber_id)
			return bool(state and state.get("online"))


def main() -> None:
	"""Run the broker server."""
	server = BrokerServer(host="127.0.0.1", port=9000)
	server.start()
	print(f"Broker started on {server.host}:{server.port}")

	try:
		# Keep the main thread alive
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		print("Broker shutting down...")
	finally:
		server.stop()
		print("Broker stopped.")


if __name__ == "__main__":
	main()
