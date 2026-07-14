"""Watchdog — liveness / staleness / deadlock detector mounted OUTSIDE the
execution engines (loop/multi_agent). It watches loop progress and heartbeats; when the
control loop stalls (no activity within ``stall_timeout``) it publishes
``harness/abort``. Because it is independent of loop/multi_agent, it can still inject an
abort even if the loop is deadlocked.
"""

import asyncio
import logging
import time
from typing import List

from .agent import Agent, AgentSpec
from .bus import EventBus


class Watchdog(Agent):
    def __init__(
        self,
        bus: EventBus,
        *,
        stall_timeout: float = 120.0,
        check_interval: float = 5.0,
        activity_topics: List[str] = None,
        liveness_topic: str = "harness/liveness/heartbeat",
    ) -> None:
        subs = activity_topics or ["loop/done", "loop/tick", "agent/#"]
        super().__init__(
            bus,
            AgentSpec(
                id="watchdog",
                role="watchdog",
                capabilities={"liveness", "deadlock-detection"},
                subscriptions=subs,
            ),
        )
        self.stall_timeout = stall_timeout
        self.check_interval = check_interval
        self.liveness_topic = liveness_topic
        self._last_activity = time.monotonic()
        self._log = logging.getLogger("stability_harness_loop_multiagent.watchdog")

    async def handle(self, topic: str, message) -> None:  # every watched topic
        self._last_activity = time.monotonic()

    async def run(self) -> None:
        while self._running:
            await asyncio.sleep(self.check_interval)
            idle = time.monotonic() - self._last_activity
            if idle >= self.stall_timeout:
                reason = f"stall: no activity for {idle:.1f}s (>= {self.stall_timeout}s)"
                self._log.warning("watchdog abort: %s", reason)
                self.bus.publish(
                    "harness/abort", {"reason": reason, "idle": idle}
                )
                self._last_activity = time.monotonic()  # avoid spamming
            else:
                self.bus.publish(
                    self.liveness_topic,
                    {"idle": idle, "ts": time.monotonic()},
                )


__all__ = ["Watchdog"]
