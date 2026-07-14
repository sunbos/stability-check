"""AdvisorAgent — advisory monitoring/analysis role of the MAS.

Subscribes to ``loop/done``, votes on each round (risk, confidence), and may
proactively raise incidents. Advisory ONLY: it never decides pass/fail. The
loop collects votes via ``loop/vote/request`` -> ``agent/vote/reply`` and acks
incidents via ``agent/incident/ack``.
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
        self._private_window: list = []  # self-contained state; never reads ctx

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
        """Override: update private window from round_info."""
        self._private_window.append(round_info.get("risk"))
        if len(self._private_window) > 100:
            self._private_window.pop(0)

    def vote(self) -> tuple:
        """Override: return (risk_score, confidence). Default abstains."""
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
