"""Watchdog —— 部署在执行引擎（loop/multi_agent）之外的存活 / 停滞 / 死锁探测器。

它监视 Loop 进度与心跳；当 ControlLoop 停滞（在 ``stall_timeout`` 内无任何活动）时，
它发布 ``harness/abort``。由于它独立于 loop/multi_agent，即使 Loop 已死锁，
它仍能注入一个中止信号。
"""

import asyncio
import logging
import time
from typing import List

from ..core.agent import Agent, AgentSpec
from ..core.bus import EventBus


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

    async def handle(self, topic: str, message) -> None:  # 每个被监视的主题
        self._last_activity = time.monotonic()

    async def run(self) -> None:
        while self._running:
            await asyncio.sleep(self.check_interval)
            idle = time.monotonic() - self._last_activity
            if idle >= self.stall_timeout:
                reason = f"stall: no activity for {idle:.1f}s (>= {self.stall_timeout}s)"
                self._log.warning("watchdog 中止: %s", reason)
                self.bus.publish(
                    "harness/abort", {"reason": reason, "idle": idle}
                )
                self._last_activity = time.monotonic()  # 避免重复刷屏
            else:
                self.bus.publish(
                    self.liveness_topic,
                    {"idle": idle, "ts": time.monotonic()},
                )


__all__ = ["Watchdog"]
