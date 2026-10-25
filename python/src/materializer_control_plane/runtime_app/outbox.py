"""Messages a member has for its committer, until the broker has them (spec §7.4).

The writer adds schema change requests and the pipeline adds file announcements; the pipeline task
drains the outbox every pass through a `topicSend` to `<runtime>-commits`. The task's map returns a
plain generator over the outbox: Kaspr sends each message it yields - with `ack: true`, waiting for
the broker - before it asks for the next one, so a message leaves the outbox only once it has been
sent. A failed send ends the task's pass with the message still here, and the next pass sends it
again. Nothing is lost by a lost message anyway: members announce every uncommitted file again
until a commit covers it.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Iterator
import threading
from typing import Any

#: Messages kept at most; beyond this the oldest schema requests go first (announcements are
#: re-created by the pipeline, requests by the writer's next record).
OUTBOX_LIMIT = 10_000


class Outbox:
    def __init__(self) -> None:
        self._messages: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._lock = threading.Lock()
        self.sent = 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._messages)

    def put(self, key: str, message: dict[str, Any]) -> None:
        """Queue `message` under `key`, replacing any message still queued under that key."""
        with self._lock:
            self._messages.pop(key, None)
            self._messages[key] = message
            while len(self._messages) > OUTBOX_LIMIT:
                self._messages.popitem(last=False)

    def discard(self, predicate: Callable[[str, dict[str, Any]], bool]) -> int:
        with self._lock:
            keys = [key for key, message in self._messages.items() if predicate(key, message)]
            for key in keys:
                del self._messages[key]
            return len(keys)

    def keys(self) -> list[str]:
        with self._lock:
            return list(self._messages)

    def drain(self) -> Iterator[dict[str, Any]]:
        """Yield queued messages oldest first, removing each after the consumer asks for the next.

        Taken as a snapshot: what is queued while it drains waits for the next pass.
        """
        with self._lock:
            snapshot = list(self._messages.items())
        for key, message in snapshot:
            yield message
            # The caller came back for more, so the message above was sent.
            with self._lock:
                if self._messages.get(key) is message:
                    del self._messages[key]
                    self.sent += 1

    def clear(self) -> None:
        with self._lock:
            self._messages.clear()


#: The process's outbox.
outbox = Outbox()
