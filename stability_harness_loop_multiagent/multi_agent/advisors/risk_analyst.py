"""RiskAnalyst — advisory risk scorer (autonomous L3-style role).

Accumulates a PRIVATE risk history from ``loop/done``, votes a weighted
``(risk, confidence)`` on ``loop/vote/request``, and runs a *proactive* timer
(every ``check_interval`` seconds, default 45) that re-scans its risk history
and may raise incidents:

    * 3 consecutive high-risk rounds (risk > high_risk_threshold) -> critical
    * a single extreme-risk round (risk >= extreme_risk_threshold)  -> warn

The proactive re-scan exists so a critical trend is surfaced even if no new
round arrives in time to be voted on — the agent watches on its own clock.

ADVISORY ONLY: it never decides pass/fail — it only suggests via vote + incident.
"""

import asyncio
import logging
import time

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from .base import AdvisorAgent


class RiskAnalyst(AdvisorAgent):
    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        *,
        weight: float = 0.5,
        check_interval: float = 45.0,
        high_risk_threshold: float = 80.0,
        extreme_risk_threshold: float = 90.0,
        high_streak: int = 3,
        min_samples: int = 3,
    ) -> None:
        super().__init__(bus, spec, weight=weight)
        self.check_interval = check_interval
        self.high_risk_threshold = high_risk_threshold
        self.extreme_risk_threshold = extreme_risk_threshold
        self.high_streak = high_streak
        self.min_samples = min_samples

        self._risks: list = []            # private risk history
        self._last_round_ts: float = 0.0
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.advisor.{self.role}")

    # ---- reactive: round intake (loop/done) --------------------------
    def on_round(self, round_info: dict) -> None:
        r = round_info or {}
        self._risks.append(float(r.get("risk", 50.0)))
        if len(self._risks) > 200:
            self._risks.pop(0)
        self._last_round_ts = time.time()

    # ---- proactive timer loop ----------------------------------------
    async def run(self) -> None:
        while self._running:
            await asyncio.sleep(self.check_interval)
            try:
                self._proactive_check()
            except Exception:  # noqa: BLE001 - isolation: never kill the agent
                self._log.exception("proactive check error")

    def _proactive_check(self) -> None:
        if not self._risks:
            return

        # consecutive high-risk streak, counted from the end
        streak = 0
        for v in reversed(self._risks):
            if v > self.high_risk_threshold:
                streak += 1
            else:
                break
        if streak >= self.high_streak:
            self.raise_incident(
                "critical", {"kind": "high_risk_streak", "streak": streak}
            )
            return

        # single extreme risk reading -> warn
        if self._risks[-1] >= self.extreme_risk_threshold:
            self.raise_incident(
                "warn", {"kind": "extreme_risk", "value": self._risks[-1]}
            )

    # ---- vote (risk, confidence) -------------------------------------
    def vote(self) -> tuple:
        if not self._risks:
            return (50.0, 0.0)  # abstain until we have data
        # conservative: the recent peak dominates the vote
        recent = self._risks[-self.min_samples:]
        risk = max(recent)
        conf = min(1.0, len(self._risks) / self.min_samples)
        return (float(risk), float(conf))


__all__ = ["RiskAnalyst"]
