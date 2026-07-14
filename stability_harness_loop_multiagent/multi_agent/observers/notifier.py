"""NotifierAgent — observer with pluggable notify channels.

Consumes generic notification topics (``agent/incident``, ``loop/abort``,
``notify``) and dispatches each to a channel. Channels:

    * ``print``   (default) — pretty-prints the event to stdout.
    * ``webhook`` — stub. ``_send_webhook`` is intentionally inert (no-op) unless
      a ``webhook_url`` is supplied, in which case it logs intent. This keeps the
      framework standard-library-only while leaving a clear extension point.

No decision authority: it only reports what it observes. Drop in additional
channels by overriding ``_dispatch_channel``.
"""

import json
import logging
import time

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from .base import ObserverAgent


class NotifierAgent(ObserverAgent):
    DEFAULT_SUBSCRIPTIONS = ("agent/incident", "loop/abort", "notify")

    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        *,
        channel: str = "print",
        webhook_url: str = None,
    ) -> None:
        for needed in self.DEFAULT_SUBSCRIPTIONS:
            if needed not in spec.subscriptions:
                spec.subscriptions.append(needed)
        super().__init__(bus, spec)
        self.channel = channel
        self.webhook_url = webhook_url
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.observer.{self.role}")

    # ---- notify -------------------------------------------------------
    def on_event(self, topic: str, message) -> None:
        msg = message if isinstance(message, dict) else {"payload": message}
        record = {"topic": topic, "message": msg, "ts": time.time()}
        if self.channel == "webhook":
            self._send_webhook(record)
        else:
            self._print(record)

    # ---- channels -----------------------------------------------------
    def _print(self, record: dict) -> None:
        try:
            text = json.dumps(record, ensure_ascii=False, default=str)
        except Exception:  # noqa: BLE001
            text = str(record)
        print(f"[notify:{self.role}] {text}")

    def _send_webhook(self, record: dict) -> None:
        # Stub: a real deployment wires webhook_url + an HTTP transport here.
        # Kept inert so the framework stays standard-library-only; never raises.
        if self.webhook_url:
            self._log.info("webhook notify -> %s (stub): %r", self.webhook_url, record)
        else:
            self._log.debug("webhook channel selected but no webhook_url; skipping")


__all__ = ["NotifierAgent"]
