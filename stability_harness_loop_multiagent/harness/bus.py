"""EventBus — the only cross-engine communication seam.

In-process async implementation. Standard library only.
- publish(topic, msg): fire-and-forget; handlers scheduled via create_task.
- publish_and_wait(topic, msg): awaits all handlers (sync guarantee).
- subscribe(topic, handler): handler may be sync or async. Returns an
  unsubscribe callable. Trailing '#' wildcard (e.g. "a/#" matches "a", "a/b").
- request(topic, msg, timeout): publish and await the first reply correlated
  by req_id. Responder calls bus.reply(req_id, response). Timeout -> TimeoutError.
- Handler exceptions are logged, never propagated to the publisher.
"""

import asyncio
import logging
import secrets
import time
from typing import Any, Callable, Dict, List, Optional


def _match(sub_topic: str, pub_topic: str) -> bool:
    """Return True if a subscription topic matches a published topic."""
    if sub_topic == pub_topic:
        return True
    if sub_topic.endswith("#"):
        prefix = sub_topic[:-1]  # "a/#" -> "a/"
        if prefix == "":
            return True  # bare "#" matches everything
        if pub_topic == prefix.rstrip("/"):
            return True
        return pub_topic.startswith(prefix)
    return False


class EventBus:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._handlers: Dict[str, List[Callable]] = {}
        self._reply_waiters: Dict[str, "asyncio.Future"] = {}
        self._log = logger or logging.getLogger("stability_harness_loop_multiagent.bus")

    # ---- subscription -------------------------------------------------
    def subscribe(self, topic: str, handler: Callable) -> Callable[[], None]:
        """Register handler for topic. Returns an unsubscribe callable."""
        self._handlers.setdefault(topic, []).append(handler)

        def unsubscribe() -> None:
            lst = self._handlers.get(topic)
            if lst and handler in lst:
                lst.remove(handler)

        return unsubscribe

    def unsubscribe(self, topic: str, handler: Callable) -> None:
        lst = self._handlers.get(topic)
        if lst and handler in lst:
            lst.remove(handler)

    # ---- reply correlation -------------------------------------------
    def reply(self, req_id: str, message: Any) -> None:
        """Resolve a pending request future identified by req_id."""
        fut = self._reply_waiters.get(req_id)
        if fut is not None and not fut.done():
            fut.set_result(message)

    # ---- publish ------------------------------------------------------
    def publish(self, topic: str, message: Any = None) -> None:
        """Fire-and-forget broadcast to all matching handlers."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        for handler in self._matching(topic):
            loop.create_task(self._run_handler(handler, topic, message))

    async def publish_and_wait(self, topic: str, message: Any = None) -> None:
        """Broadcast and await completion of every handler."""
        tasks = [
            self._run_handler(handler, topic, message)
            for handler in self._matching(topic)
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def request(
        self, topic: str, message: Any = None, timeout: float = 1.0
    ) -> Any:
        """Publish and await the first reply correlated by req_id."""
        req_id = secrets.token_hex(8)
        if isinstance(message, dict):
            envelope = dict(message)
        else:
            envelope = {"payload": message}
        envelope["req_id"] = req_id

        fut: "asyncio.Future" = asyncio.get_event_loop().create_future()
        self._reply_waiters[req_id] = fut
        self.publish(topic, envelope)
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._reply_waiters.pop(req_id, None)

    # ---- internals ----------------------------------------------------
    def _matching(self, topic: str) -> List[Callable]:
        out: List[Callable] = []
        for sub_topic, handlers in self._handlers.items():
            if _match(sub_topic, topic):
                out.extend(handlers)
        return out

    async def _run_handler(self, handler: Callable, topic: str, message: Any) -> None:
        try:
            result = handler(topic, message)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 - isolation: never leak to publisher
            self._log.exception("bus handler error on topic=%r", topic)


__all__ = ["EventBus"]
