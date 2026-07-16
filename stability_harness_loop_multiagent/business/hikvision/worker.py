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


def _iso_seconds_ago(seconds: int) -> str:
    """ISO timestamp N seconds ago (second precision, no microseconds)."""
    t = datetime.now(timezone(timedelta(hours=8))) - timedelta(seconds=seconds)
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
        # Use a 30-second lookback window to reliably catch events generated
        # during do_work. do_work + digest auth handshake can take >500ms, so
        # a zero-width window [now, now] misses events from the same second.
        # 30s is safe: round interval is 2s, and serialNo can correlate cycles.
        start = _iso_seconds_ago(30)
        end = _now_iso()
        # Parallel query across majors via asyncio.to_thread (adapter/client are sync).
        # Client is internally serialized via threading.Lock (digest auth not thread-safe),
        # so the 3 calls run sequentially under the lock but don't block the event loop.
        try:
            trigger, opened, closed = await asyncio.gather(
                asyncio.to_thread(self._client.query_events,
                                  *HikEventCode.REMOTE_OPEN, start, end),
                asyncio.to_thread(self._client.query_events,
                                  *HikEventCode.LOCK_OPEN, start, end),
                asyncio.to_thread(self._client.query_events,
                                  *HikEventCode.LOCK_CLOSE, start, end),
            )
        except Exception as exc:  # noqa: BLE001
            # Network/HTTP error: record empty events so check() returns False
            # facts (fact dictatorship -> fail verdict). Don't crash the loop.
            self._last_events = {"trigger": [], "opened": [], "closed": []}
            self._recover_error = str(exc)
            self._healed = None
            return True
        self._last_events = {"trigger": trigger, "opened": opened, "closed": closed}

        # Self-heal: time skew if trigger missing and skew exceeds threshold
        if not trigger and self._diagnostic is not None:
            skew = self._measure_time_skew()
            env = {"time_skew_seconds": skew,
                   "missing": self._missing_names(trigger, opened, closed),
                   "http_error": None}
            decision = self._diagnostic.diagnose(env)
            if decision == HEAL_TIME_SYNC and skew > self._time_skew_threshold:
                try:
                    self._client.set_time(_now_iso())
                    # Re-query trigger after heal
                    self._last_events["trigger"] = await asyncio.to_thread(
                        self._client.query_events,
                        *HikEventCode.REMOTE_OPEN, start, end)
                    self._healed = "time_sync"
                except Exception:  # noqa: BLE001
                    self._healed = None
                return True
            self._healed = None
            if decision == HEAL_ABORT:
                return False
        self._healed = None
        return True

    def check(self, tick: dict) -> dict:
        ev = self._last_events
        # Hard facts (bool): any False -> fail verdict (fact dictatorship).
        # remote_open_triggered + lock_opened are the strong event-chain facts.
        facts = {
            "remote_open_triggered": len(ev["trigger"]) > 0,
            "lock_opened": len(ev["opened"]) > 0,
        }
        # Soft fact (non-bool truthy): lock_closed is a passive event (door
        # auto-closes), may not happen within the query window. Per spec §2.1,
        # it does NOT participate in fact dictatorship; Advisor raises risk
        # instead. Stored as dict metadata so it's visible but won't force fail.
        closed_count = len(ev["closed"])
        if closed_count > 0:
            facts["lock_closed"] = {"found": True, "count": closed_count}
        else:
            facts["lock_closed"] = {"found": False, "count": 0}
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
