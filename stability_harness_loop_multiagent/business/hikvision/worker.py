"""HikvisionWorker: door-open test execution + event-chain assertion + self-heal.

Pipeline (inherited, customized):
  do_work(tick)  -> remote_open_door via adapter
  recover(tick)  -> async: parallel query 3 events via asyncio.to_thread;
                    if missing + time skew > threshold, run LLM diagnostic
                    kernel -> time_sync heal.
  check(tick)    -> sync: assert 3-event chain facts from cached query results.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from ...multi_agent.workers.base import WorkerAgent
from .adapter import HikvisionAdapter
from .diagnostic import DiagnosticKernel, HEAL_TIME_SYNC, HEAL_ABORT
from .event_codes import HikEventCode


def _now_iso() -> str:
    t = datetime.now(timezone(timedelta(hours=8)))
    return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")


class HikvisionWorker(WorkerAgent):
    def __init__(self, bus: EventBus, spec: AgentSpec, adapter: HikvisionAdapter,
                 client, time_skew_threshold: float = 3.0,
                 diagnostic: DiagnosticKernel = None) -> None:
        super().__init__(bus, spec, adapter)
        self._client = client
        self._time_skew_threshold = time_skew_threshold
        self._diagnostic = diagnostic
        self._last_events: Dict[str, list] = {"trigger": [], "opened": [], "closed": []}
        self._healed: Any = None

    def do_work(self, tick: dict) -> Any:
        op = tick.get("operation") or {"op": "remote_open_door"}
        return self.adapter.act(op)

    async def recover(self, tick: dict) -> bool:
        start = tick.get("window_start", _now_iso())
        # Use current time as end to cover events generated during do_work
        end = _now_iso()
        # Parallel query across majors via asyncio.to_thread (adapter/client are sync)
        trigger, opened, closed = await asyncio.gather(
            asyncio.to_thread(self._client.query_events,
                              *HikEventCode.REMOTE_OPEN, start, end),
            asyncio.to_thread(self._client.query_events,
                              *HikEventCode.LOCK_OPEN, start, end),
            asyncio.to_thread(self._client.query_events,
                              *HikEventCode.LOCK_CLOSE, start, end),
        )
        self._last_events = {"trigger": trigger, "opened": opened, "closed": closed}

        # Self-heal: time skew if trigger missing and skew exceeds threshold
        if not trigger and self._diagnostic is not None:
            skew = self._measure_time_skew()
            env = {"time_skew_seconds": skew,
                   "missing": self._missing_names(trigger, opened, closed),
                   "http_error": None}
            decision = self._diagnostic.diagnose(env)
            if decision == HEAL_TIME_SYNC and skew > self._time_skew_threshold:
                self._client.set_time(_now_iso())
                # Re-query trigger after heal
                self._last_events["trigger"] = await asyncio.to_thread(
                    self._client.query_events,
                    *HikEventCode.REMOTE_OPEN, start, end)
                self._healed = "time_sync"
                return True
            self._healed = None
            if decision == HEAL_ABORT:
                return False
        self._healed = None
        return True

    def check(self, tick: dict) -> dict:
        ev = self._last_events
        facts = {
            "remote_open_triggered": len(ev["trigger"]) > 0,
            "lock_opened": len(ev["opened"]) > 0,
            "lock_closed": len(ev["closed"]) > 0,
        }
        if getattr(self, "_healed", None):
            facts["self_healed"] = self._healed  # non-bool truthy, won't fail
        return facts

    def _measure_time_skew(self) -> float:
        try:
            dev = self._client.get_time()["Time"]["localTime"]
            dev_t = datetime.fromisoformat(dev)
            host_t = datetime.now(dev_t.tzinfo)
            return abs((dev_t - host_t).total_seconds())
        except Exception:  # noqa: BLE001
            return 0.0

    @staticmethod
    def _missing_names(trigger, opened, closed) -> list:
        missing = []
        if not trigger:
            missing.append("remote_open")
        if not opened:
            missing.append("lock_open")
        if not closed:
            missing.append("lock_close")
        return missing


__all__ = ["HikvisionWorker"]
