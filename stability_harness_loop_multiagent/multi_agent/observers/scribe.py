"""ScribeAgent —— 保有私有时间线 + 摘要的 Observer。

订阅轮次 / 事件 / 中止事件，并保有它*自己的*时间线；它绝不触及共享的 Loop
上下文，也绝不裁决任何东西。它还能通过发布 ``scribe/summary`` 来回应
``scribe/summary/request``，附带一份聚合摘要（决策分布、风险统计、事件计数）。

纯观察：可安全地添加或移除，而不会影响 Loop 行为。
"""

import logging
import time
from collections import Counter

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from .base import ObserverAgent


class ScribeAgent(ObserverAgent):
    # 本 Observer 关心的主题（若尚未存在则自动接入）
    DEFAULT_SUBSCRIPTIONS = (
        "loop/done",
        "agent/incident",
        "loop/abort",
        "scribe/summary/request",
    )

    def __init__(self, bus: EventBus, spec: AgentSpec) -> None:
        for needed in self.DEFAULT_SUBSCRIPTIONS:
            if needed not in spec.subscriptions:
                spec.subscriptions.append(needed)
        super().__init__(bus, spec)
        self._timeline: list = []
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.observer.{self.role}")

    # ---- 记录 -------------------------------------------------------
    def on_event(self, topic: str, message) -> None:
        msg = message if isinstance(message, dict) else {"payload": message}
        self._timeline.append({"topic": topic, "message": msg, "ts": time.time()})
        if topic == "scribe/summary/request":
            self.publish(
                "scribe/summary",
                {"summary": self.summary(), "req_id": msg.get("req_id")},
            )

    # ---- 摘要 -------------------------------------------------------
    def summary(self) -> dict:
        rounds = [e["message"] for e in self._timeline if e["topic"] == "loop/done"]
        incidents = [e["message"] for e in self._timeline if e["topic"] == "agent/incident"]
        aborts = [e["message"] for e in self._timeline if e["topic"] == "loop/abort"]
        decisions = Counter(r.get("verdict", "unknown") for r in rounds)
        risks = [float(r.get("risk", 0.0)) for r in rounds]
        return {
            "rounds": len(rounds),
            "decisions": dict(decisions),
            "risk_avg": (sum(risks) / len(risks)) if risks else None,
            "risk_max": max(risks) if risks else None,
            "incidents": len(incidents),
            "critical_incidents": sum(
                1 for i in incidents if i.get("severity") == "critical"
            ),
            "aborted": bool(aborts),
            "abort_reason": aborts[-1].get("reason") if aborts else None,
            "timeline_len": len(self._timeline),
        }

    @property
    def timeline(self) -> list:
        """私有事件时间线的只读副本。"""
        return list(self._timeline)


__all__ = ["ScribeAgent"]
