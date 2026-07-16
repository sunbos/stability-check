"""HikvisionWorker: door-open test execution + event-chain assertion + self-heal.

Pipeline (inherited, customized):
  do_work(tick)  -> [reboot -> wait_online -> warmup] -> remote_open_door
                    (reboot phases only when run_reboot=True and plan.skip_reboot=False)
  recover(tick)  -> async: parallel query 3 events via asyncio.to_thread;
                    if missing + time skew > threshold, run LLM diagnostic
                    kernel -> time_sync heal.
  check(tick)    -> sync: assert 3-event chain facts from cached query results.

spec §3.1.3 long-IO rule: do_work may block 60-180s (reboot + probe + warmup),
so act() wraps it with asyncio.to_thread to avoid stalling the event loop.
"""

import asyncio
import time
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
                 diagnostic: DiagnosticKernel = None,
                 *,
                 run_reboot: bool = True,
                 probe_interval: float = 5.0,
                 probe_confirm_count: int = 2,
                 warmup_time: float = 60.0,
                 max_recover_timeout: float = 180.0) -> None:
        super().__init__(bus, spec, adapter)
        self._client = client
        self._time_skew_threshold = time_skew_threshold
        self._diagnostic = diagnostic
        self._last_events: Dict[str, list] = {"trigger": [], "opened": [], "closed": []}
        self._healed: Any = None
        # Reboot + probe + warmup config (spec §4.1, §4.2, §6 worker.*)
        self._run_reboot = run_reboot
        self._probe_interval = probe_interval
        self._probe_confirm_count = probe_confirm_count
        self._warmup_time = warmup_time
        self._max_recover_timeout = max_recover_timeout
        # Track last do_work outcome for observability
        self._last_work_stages: Dict[str, Any] = {}

    async def act(self, tick: dict) -> None:
        """Override base act() to run do_work in a thread.

        do_work may block 60-180s (reboot + probe + warmup). Per spec §3.1.3,
        long IO must be wrapped with asyncio.to_thread to avoid stalling the
        event loop and disabling ControlLoop's timeout safety net.
        """
        result = await asyncio.to_thread(self.do_work, tick)
        self.publish(
            "target/acted",
            {"role": self.role, "round": tick.get("round"), "result": result},
        )
        recovered = await self.recover(tick)
        self.publish(
            "target/recovered",
            {"role": self.role, "round": tick.get("round"), "recovered": recovered},
        )
        facts = self.check(tick)
        self.publish(
            "target/checked",
            {"role": self.role, "round": tick.get("round"), "facts": facts},
        )
        self.publish("agent/" + self.role + "/done", {"round": tick.get("round")})

    def do_work(self, tick: dict) -> Any:
        """Execute the per-round test operation.

        When run_reboot=True and plan.skip_reboot=False (default), the full
        door-restart stability flow runs:
          1. reboot device (PUT /ISAPI/System/reboot)
          2. wait_online: poll get_work_status until probe_confirm_count
             consecutive successes (spec §4.1) or max_recover_timeout
          3. warmup: sleep warmup_time seconds (spec §6 worker.warmup_time)
          4. remote_open_door (PUT /ISAPI/AccessControl/RemoteControl/door/N)
        """
        plan = self.state.get("plan", {}) or {}
        skip_reboot = bool(plan.get("skip_reboot", False))
        op = tick.get("operation") or {"op": "remote_open_door"}
        stages: Dict[str, Any] = {"op": op.get("op"), "skip_reboot": skip_reboot}

        # Phase 1: reboot -> wait_online -> warmup
        if self._run_reboot and not skip_reboot:
            stages["reboot_started"] = True
            reboot_res = self.adapter.act({"op": "reboot"})
            if not reboot_res.ok:
                stages["reboot_ok"] = False
                stages["error"] = reboot_res.error
                self._last_work_stages = stages
                return reboot_res
            stages["reboot_ok"] = True

            online = self._wait_online()
            stages["online"] = online
            if not online:
                from ...multi_agent.adapter import Result
                self._last_work_stages = stages
                return Result(ok=False, error="device did not come online "
                                f"within {self._max_recover_timeout}s")
            # warmup
            time.sleep(self._warmup_time)
            stages["warmup_done"] = True

        # Phase 2: remote_open_door (or whatever op was requested)
        result = self.adapter.act(op)
        stages["act_ok"] = result.ok
        self._last_work_stages = stages
        return result

    def _wait_online(self) -> bool:
        """Poll device until probe_confirm_count consecutive successes.

        spec §4.1: two consecutive HTTP 200 from /AcsWorkStatus confirms online
        (avoids false positive from transient TCP bind during reboot).
        """
        deadline = time.monotonic() + self._max_recover_timeout
        consecutive = 0
        while time.monotonic() < deadline:
            try:
                self._client.get_work_status()
                consecutive += 1
                if consecutive >= self._probe_confirm_count:
                    return True
            except Exception:  # noqa: BLE001
                consecutive = 0
            time.sleep(self._probe_interval)
        return False

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
