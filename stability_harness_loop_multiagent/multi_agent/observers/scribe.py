"""ScribeAgent — observer that records a private timeline + summary.

Subscribes to round / incident / abort events and keeps its OWN timeline; it
never touches the shared loop context and never decides anything. It can also
answer a ``scribe/summary/request`` by publishing ``scribe/summary`` with an
aggregated summary (decision distribution, risk stats, incident counts).

Pure observation: safe to add or remove without affecting loop behaviour.
"""

import logging
import time
from collections import Counter

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from .base import ObserverAgent


class ScribeAgent(ObserverAgent):
    # topics this observer cares about (auto-wired if not already present)
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

    # ---- record -------------------------------------------------------
    def on_event(self, topic: str, message) -> None:
        msg = message if isinstance(message, dict) else {"payload": message}
        self._timeline.append({"topic": topic, "message": msg, "ts": time.time()})
        if topic == "scribe/summary/request":
            self.publish(
                "scribe/summary",
                {"summary": self.summary(), "req_id": msg.get("req_id")},
            )

    # ---- summary ------------------------------------------------------
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
        """Read-only copy of the private event timeline."""
        return list(self._timeline)


__all__ = ["ScribeAgent"]
