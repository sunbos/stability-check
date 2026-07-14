"""TrendSupervisorAgent — advisory trend monitor (autonomous L3-style role).

Keeps a PRIVATE window of round outcomes gathered from ``loop/done`` — it never
reads the shared loop context directly (self-contained autonomous state). On each
round it appends to its window; a *proactive* timer (every ``check_interval``
seconds, default 30) re-scans the window for harmful trends and may raise an
incident. It also responds to ``loop/vote/request`` with a weighted
``(risk, confidence)`` vote.

ADVISORY ONLY: it never decides pass/fail. The loop's DecisionAuthority is the
sole arbiter. Its risk score only *annotates* (warn/recheck), and a ``critical``
incident forces the loop to recheck. This mirrors the autonomy principle:
agents watch independently and alert, they do not rule.
"""

import asyncio
import logging
import time

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from .base import AdvisorAgent


class TrendSupervisorAgent(AdvisorAgent):
    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        *,
        weight: float = 0.5,
        check_interval: float = 30.0,
        warn_streak: int = 3,
        critical_streak: int = 5,
        fail_rate_threshold: float = 0.30,
        min_samples: int = 5,
        spike_factor: float = 2.0,
        stale_timeout: float = 300.0,
    ) -> None:
        super().__init__(bus, spec, weight=weight)
        self.check_interval = check_interval
        self.warn_streak = warn_streak
        self.critical_streak = critical_streak
        self.fail_rate_threshold = fail_rate_threshold
        self.min_samples = min_samples
        self.spike_factor = spike_factor
        self.stale_timeout = stale_timeout

        self._window: list = []            # private: list of round dicts
        self._last_round_ts: float = 0.0
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.advisor.{self.role}")

    # ---- reactive: round intake (loop/done) --------------------------
    def on_round(self, round_info: dict) -> None:
        self._window.append(round_info or {})
        if len(self._window) > 200:
            self._window.pop(0)
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
        # stale-data alert: no new round for a while -> warn
        if self._last_round_ts and (time.time() - self._last_round_ts) > self.stale_timeout:
            self.raise_incident(
                "warn",
                {
                    "kind": "stale",
                    "seconds_since_round": time.time() - self._last_round_ts,
                },
            )

        inc = self._detect_trend()
        if inc is not None:
            self.raise_incident(inc["severity"], inc["detail"])

    # ---- trend detection (private window) ----------------------------
    def _detect_trend(self):
        if len(self._window) < 2:
            return None
        risks = [float(r.get("risk", 50.0)) for r in self._window]
        verdicts = [r.get("verdict", "") for r in self._window]

        # consecutive strictly-increasing risk streak, counted from the end
        inc_streak = 0
        for a, b in zip(reversed(risks[1:]), reversed(risks[:-1])):
            if b > a:
                inc_streak += 1
            else:
                break

        if inc_streak >= self.critical_streak:
            return {
                "severity": "critical",
                "detail": {"kind": "increasing_risk", "streak": inc_streak},
            }
        if inc_streak >= self.warn_streak:
            return {
                "severity": "warn",
                "detail": {"kind": "increasing_risk", "streak": inc_streak},
            }

        # failure-rate crossing upward through the threshold
        if len(self._window) >= self.min_samples:
            fails = sum(1 for v in verdicts if v in ("fail", "abort"))
            rate = fails / len(self._window)
            if rate > self.fail_rate_threshold:
                return {"severity": "warn", "detail": {"kind": "fail_rate", "rate": rate}}

        # risk spike > spike_factor x the recent mean
        recent = risks[-self.min_samples:]
        mean = sum(recent) / len(recent)
        if mean > 0 and risks[-1] > self.spike_factor * mean:
            return {
                "severity": "warn",
                "detail": {"kind": "risk_spike", "value": risks[-1], "mean": mean},
            }
        return None

    # ---- vote (risk, confidence) -------------------------------------
    def vote(self) -> tuple:
        if len(self._window) < 2:
            return (20.0, 0.3)  # thin data -> low risk, low confidence
        risks = [float(r.get("risk", 50.0)) for r in self._window]
        verdicts = [r.get("verdict", "") for r in self._window]

        inc_streak = 0
        for a, b in zip(reversed(risks[1:]), reversed(risks[:-1])):
            if b > a:
                inc_streak += 1
            else:
                break

        risk = 20.0
        if inc_streak >= self.critical_streak:
            risk = 85.0
        elif inc_streak >= self.warn_streak:
            risk = 65.0

        if len(self._window) >= self.min_samples:
            rate = sum(1 for v in verdicts if v in ("fail", "abort")) / len(self._window)
            if rate > self.fail_rate_threshold:
                risk = max(risk, 75.0)

        conf = min(1.0, len(self._window) / self.min_samples)
        return (float(risk), float(conf))


__all__ = ["TrendSupervisorAgent"]
