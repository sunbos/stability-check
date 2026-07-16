"""AdvisorAgent —— MAS 中的建议性监控/分析角色。

订阅 ``loop/done``，在每轮投票（风险、置信度），并可能主动提出事件。
仅具建议性：它绝不裁决通过/失败。循环通过 ``loop/vote/request`` ->
``agent/vote/reply`` 收集投票，并通过 ``agent/incident/ack`` 对事件进行 ack。
"""

import time

from ...harness.agent import Agent, AgentSpec
from ...harness.bus import EventBus


class AdvisorAgent(Agent):
    def __init__(
        self, bus: EventBus, spec: AgentSpec, *, weight: float = 1.0
    ) -> None:
        for needed in ("loop/done", "loop/vote/request"):
            if needed not in spec.subscriptions:
                spec.subscriptions.append(needed)
        super().__init__(bus, spec)
        self.weight = weight
        self._private_window: list = []  # 自包含状态；绝不读取 ctx

    async def handle(self, topic: str, message) -> None:
        if topic == "loop/done":
            self.on_round(message or {})
        elif topic == "loop/vote/request":
            self._maybe_vote(message or {})

    def _maybe_vote(self, round_info: dict) -> None:
        risk, conf = self.vote()
        self.publish(
            "agent/vote/reply",
            {
                "role": self.role,
                "risk": risk,
                "confidence": conf,
                "weight": self.weight,
                "round": round_info.get("round"),
            },
        )

    def on_round(self, round_info: dict) -> None:
        """覆盖：根据 round_info 更新私有窗口。"""
        self._private_window.append(round_info.get("risk"))
        if len(self._private_window) > 100:
            self._private_window.pop(0)

    def vote(self) -> tuple:
        """覆盖：返回 (risk_score, confidence)。默认弃权。"""
        return (50.0, 0.0)

    def raise_incident(self, severity: str, detail) -> None:
        self.publish(
            "agent/incident",
            {
                "role": self.role,
                "severity": severity,
                "detail": detail,
                "ts": time.time(),
            },
        )


__all__ = ["AdvisorAgent"]
