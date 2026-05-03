"""Persistent queue for offline subscribers.

Scope in this file:
- File-based message queue (survives restart)
- Enqueue messages for offline subscribers
- Flush queued messages on reconnection
"""

from __future__ import annotations

import uuid
import json
import os
import threading
from dataclasses import dataclass
from typing import Any


QUEUE_DIR = "queues"


@dataclass
class QueuedMessage:
    message_id: str
    topic: str
    payload: str
    attempts: int = 0
    acked: bool = False


class PersistentQueue:
    """File-based per-subscriber message queue.

    Each subscriber gets its own JSON-lines file under the QUEUE_DIR folder.
    Messages survive process restarts because they are written to disk.
    """

    def __init__(self, queue_dir: str = QUEUE_DIR) -> None:
        self.queue_dir = queue_dir
        self._lock = threading.Lock()
        os.makedirs(self.queue_dir, exist_ok=True)

    # ── Internal helpers ────────────────────────────────────────────────────

    def _path(self, subscriber_id: str) -> str:
        """Return the queue file path for a given subscriber."""
        safe_id = subscriber_id.replace("/", "_").replace("\\", "_")
        return os.path.join(self.queue_dir, f"{safe_id}.jsonl")

    def _read_all(self, subscriber_id: str) -> list[QueuedMessage]:
        """Read all queued messages from disk. Returns empty list if none."""
        path = self._path(subscriber_id)
        if not os.path.exists(path):
            return []
        messages: list[QueuedMessage] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    messages.append(QueuedMessage(
                        message_id=str(data.get("message_id", "")),
                        topic=data["topic"],
                        payload=data["payload"],
                        attempts=int(data.get("attempts", 0)),
                        acked=bool(data.get("acked", False)),
                    ))
                except (json.JSONDecodeError, KeyError):
                    continue
        return messages

    def _write_all(self, subscriber_id: str, messages: list[QueuedMessage]) -> None:
        """Overwrite the queue file with the given list of messages."""
        path = self._path(subscriber_id)
        with open(path, "w", encoding="utf-8") as f:
            for msg in messages:
                f.write(
                    json.dumps(
                        {
                            "message_id": msg.message_id,
                            "topic": msg.topic,
                            "payload": msg.payload,
                            "attempts": msg.attempts,
                            "acked": msg.acked,
                        }
                    )
                    + "\n"
                )

    # ── Public API ──────────────────────────────────────────────────────────

    def enqueue(
        self,
        subscriber_id: str,
        topic: str,
        payload: str,
        message_id: str | None = None,
        attempts: int = 0,
    ) -> str:
        """Append a message to the subscriber's queue file and return its message id."""
        message_id = message_id or str(uuid.uuid4())
        with self._lock:
            path = self._path(subscriber_id)
            with open(path, "a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "message_id": message_id,
                            "topic": topic,
                            "payload": payload,
                            "attempts": attempts,
                            "acked": False,
                        }
                    )
                    + "\n"
                )
        return message_id

    def flush(self, subscriber_id: str) -> list[QueuedMessage]:
        """Return all queued messages and clear the queue file."""
        with self._lock:
            messages = self._read_all(subscriber_id)
            self._write_all(subscriber_id, [])
        return messages

    def fetch_pending(self, subscriber_id: str) -> list[QueuedMessage]:
        """Return queued messages that have not been acknowledged yet."""
        with self._lock:
            return [message for message in self._read_all(subscriber_id) if not message.acked]

    def mark_acked(self, subscriber_id: str, message_id: str) -> None:
        """Mark a queued message acknowledged and remove it from the queue file."""
        with self._lock:
            messages = [message for message in self._read_all(subscriber_id) if message.message_id != message_id]
            self._write_all(subscriber_id, messages)

    def increment_attempt(self, subscriber_id: str, message_id: str) -> None:
        """Increment the delivery attempts counter for a queued message."""
        with self._lock:
            messages = self._read_all(subscriber_id)
            for message in messages:
                if message.message_id == message_id:
                    message.attempts += 1
                    break
            self._write_all(subscriber_id, messages)

    def peek(self, subscriber_id: str) -> list[QueuedMessage]:
        """Return all queued messages without clearing the queue."""
        with self._lock:
            return self._read_all(subscriber_id)

    def size(self, subscriber_id: str) -> int:
        """Return the number of queued messages for a subscriber."""
        with self._lock:
            return len(self._read_all(subscriber_id))

    def clear(self, subscriber_id: str) -> None:
        """Delete all queued messages for a subscriber."""
        with self._lock:
            path = self._path(subscriber_id)
            if os.path.exists(path):
                os.remove(path)