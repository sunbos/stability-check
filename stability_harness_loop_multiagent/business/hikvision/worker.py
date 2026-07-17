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
from typing import Any, Dict, List

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from ...multi_agent.workers.base import WorkerAgent
from .adapter import HikvisionAdapter
from .diagnostic import DiagnosticKernel, HEAL_TIME_SYNC, HEAL_ABORT
from .event_codes import HikEventCode


def _now_iso() -> str:
    t = datetime.now(timezone(timedelta(hours=8)))
    return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")


def _iso_seconds_before(ref_iso: str, seconds: int) -> str:
    """ISO timestamp N seconds before a reference ISO time (second precision).

    Used to compute device-time-based query windows (spec §2.1 A.2): device
    event log stores events with device time, so the window must be in device
    time. After a reboot, device time may drift (NTP not synced, factory
    default), making host-time windows miss events recorded at drifted time.
    """
    ref = datetime.fromisoformat(ref_iso)
    t = ref - timedelta(seconds=seconds)
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
        # Per-round timeline of stage executions for observability.
        # Each entry: {"stage": <name>, "ts": <iso>, "t": <seconds_since_round_start>, **extra}
        # Reset at the start of each do_work(); appended by recover()/check() too.
        self._timeline: List[Dict[str, Any]] = []
        self._t0: float = 0.0  # monotonic start of current round
        # Baseline recorded before the FIRST reboot: stores the latest serialNo
        # of each event type so post-reboot queries can filter out pre-existing
        # events and only count new ones from this round's remote_open_door.
        # This solves the "trigger=2" issue where the 300s lookback window
        # contains residual events from previous rounds.
        self._baseline: Dict[str, Any] = {}
        self._baseline_recorded: bool = False

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

    def _mark(self, stage: str, **extra: Any) -> None:
        """Append a stage entry to the per-round timeline.

        Thread-safe: list.append is atomic under CPython GIL, so calls
        from do_work (running in a worker thread) and recover/check
        (running in the event loop) can both append safely.
        """
        entry = {"stage": stage, "ts": _now_iso(),
                 "t": round(time.monotonic() - self._t0, 2)}
        entry.update(extra)
        self._timeline.append(entry)

    def _record_baseline(self) -> None:
        """Record device baseline before the FIRST reboot.

        Captures device time and the latest serialNo of each event type
        (remote_open, lock_open, lock_close) so that post-reboot queries
        can filter out pre-existing events. Only the first reboot records
        baseline; subsequent rounds use the updated serialNo from the
        previous round's recover() as the new baseline.

        This is essential for the reboot stability test: without it, the
        300s lookback window would contain residual events from previous
        rounds, inflating counts (e.g., trigger=2 instead of 1).
        """
        try:
            device_time = self._client.get_time()["Time"]["localTime"]
        except Exception:  # noqa: BLE001
            device_time = _now_iso()
        # Query recent events (5 min lookback) to find the latest serialNo
        start = _iso_seconds_before(device_time, 300)
        serials: Dict[str, int] = {}
        for name, code in (("trigger", HikEventCode.REMOTE_OPEN),
                           ("opened", HikEventCode.LOCK_OPEN),
                           ("closed", HikEventCode.LOCK_CLOSE)):
            try:
                events = self._client.query_events(*code, start, device_time)
                serials[name] = max(
                    (int(e.get("serialNo", 0)) for e in events),
                    default=0)
            except Exception:  # noqa: BLE001
                serials[name] = 0
        self._baseline = {"device_time": device_time, "serialNos": serials}
        self._baseline_recorded = True

    def _update_baseline_from_events(self) -> None:
        """Update baseline serialNos from this round's recovered events.

        Called at the end of recover() so the next round's filter uses the
        latest known serialNo as its baseline. This ensures each round only
        counts events produced AFTER the previous round.
        """
        if not self._baseline_recorded:
            return
        serials = self._baseline.setdefault("serialNos", {})
        for name in ("trigger", "opened", "closed"):
            events = self._last_events.get(name, [])
            if events:
                current_max = max(int(e.get("serialNo", 0)) for e in events)
                if current_max > serials.get(name, 0):
                    serials[name] = current_max

    def do_work(self, tick: dict) -> Any:
        """Execute the per-round test operation.

        When run_reboot=True and plan.skip_reboot=False (default), the full
        door-restart stability flow runs:
          1. (first round only) record baseline: device time + last serialNo
          2. reboot device (PUT /ISAPI/System/reboot)
          3. wait_online: wait for device to go offline (reboot takes effect),
             then come back, then confirm with probe_confirm_count successes
          4. warmup: sleep warmup_time seconds (spec §6 worker.warmup_time)
          5. remote_open_door (PUT /ISAPI/AccessControl/RemoteControl/door/N)
        """
        plan = self.state.get("plan", {}) or {}
        skip_reboot = bool(plan.get("skip_reboot", False))
        op = tick.get("operation") or {"op": "remote_open_door"}
        # Reset per-round timeline at the start of do_work.
        self._timeline = []
        self._t0 = time.monotonic()
        stages: Dict[str, Any] = {"op": op.get("op"), "skip_reboot": skip_reboot,
                                  "timeline": self._timeline}

        # Phase 1: baseline (first round) -> reboot -> wait_online -> warmup
        if self._run_reboot and not skip_reboot:
            # Record baseline before first reboot (spec: first-reboot baseline)
            if not self._baseline_recorded:
                self._mark("baseline_start")
                self._record_baseline()
                self._mark("baseline_done", **self._baseline)

            self._mark("reboot_start")
            reboot_res = self.adapter.act({"op": "reboot"})
            self._mark("reboot_done", ok=reboot_res.ok,
                       error=None if reboot_res.ok else reboot_res.error)
            if not reboot_res.ok:
                stages["reboot_ok"] = False
                stages["error"] = reboot_res.error
                self._last_work_stages = stages
                return reboot_res
            stages["reboot_ok"] = True

            self._mark("probe_start", interval=self._probe_interval,
                       confirm=self._probe_confirm_count)
            online = self._wait_online()
            self._mark("probe_done", online=online)
            stages["online"] = online
            if not online:
                from ...multi_agent.adapter import Result
                self._last_work_stages = stages
                return Result(ok=False, error="device did not come online "
                                f"within {self._max_recover_timeout}s")
            # warmup
            self._mark("warmup_start", seconds=self._warmup_time)
            time.sleep(self._warmup_time)
            self._mark("warmup_done")
            stages["warmup_done"] = True

        # Phase 2: remote_open_door (or whatever op was requested)
        self._mark("act_start", op=op.get("op"))
        result = self.adapter.act(op)
        self._mark("act_done", ok=result.ok,
                   error=None if result.ok else result.error)
        stages["act_ok"] = result.ok
        if not result.ok:
            stages["act_error"] = result.error
        self._last_work_stages = stages
        return result

    def _wait_online(self) -> bool:
        """Wait for device to reboot and come back online (spec §4.1).

        Three phases (the old code skipped phase 1, falsely detecting
        "online" in ~2s because the device keeps serving HTTP for a few
        seconds after the reboot PUT returns 200, before actually
        restarting):

        1. Wait for device to go OFFLINE: the reboot PUT returns 200
           immediately, but the device keeps serving HTTP for a few seconds
           before actually restarting. We detect reboot-take-effect by the
           first get_work_status failure.
        2. Wait for device to come BACK: poll until get_work_status succeeds
           again (device finished rebooting, HTTP service restored).
        3. Confirm: require probe_confirm_count consecutive successes to
           rule out transient TCP bind during early boot (spec §4.1).

        Real device reboot takes 30-60s; phase 1 (~2-5s) + phase 2 (~30-60s)
        + phase 3 (~probe_interval*confirm) should fit within max_recover_timeout.
        """
        deadline = time.monotonic() + self._max_recover_timeout
        consecutive = 0
        offline_seen = False
        while time.monotonic() < deadline:
            try:
                self._client.get_work_status()
                if not offline_seen:
                    # Phase 1: device still up, reboot hasn't taken effect.
                    # Keep polling until we see a failure (device going down).
                    time.sleep(self._probe_interval)
                    continue
                # Phase 3: confirm online with consecutive successes
                consecutive += 1
                if consecutive == 1:
                    self._mark("probe_back_online")
                if consecutive >= self._probe_confirm_count:
                    return True
            except Exception:  # noqa: BLE001
                if not offline_seen:
                    # Phase 1 complete: reboot took effect, device going down
                    offline_seen = True
                    self._mark("probe_offline_seen")
                consecutive = 0
            time.sleep(self._probe_interval)
        return False

    async def recover(self, tick: dict) -> bool:
        # Query window: when run_reboot=True, device reboots (~60s) + warmup
        # (60s) means remote_open_door events may be 120s+ old by the time we
        # query. Use a wide 300s lookback to cover the full reboot cycle.
        # When run_reboot=False, 30s is ample (do_work is just a quick open).
        lookback = 300 if self._run_reboot else 30
        self._mark("recover_start", lookback=lookback)
        # Compute query window in DEVICE time (spec §2.1 A.2): device event
        # log stores events with device time. After a reboot, device time may
        # drift (NTP not synced, factory default), so a host-time window would
        # miss events recorded at drifted device time. Fetch device_now and
        # compute [device_now - lookback, device_now]. Fallback to host time
        # only if get_time() fails (device unreachable mid-round).
        try:
            device_time = await asyncio.to_thread(self._client.get_time)
            end = device_time["Time"]["localTime"]
        except Exception:  # noqa: BLE001
            end = _now_iso()
        start = _iso_seconds_before(end, lookback)
        self._mark("recover_query_start", start=start, end=end)
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
            self._mark("recover_query_done", error=str(exc))
            self._mark("recover_done", recovered=True)
            return True
        # Filter out pre-baseline events: only count events with serialNo
        # greater than the baseline (recorded before first reboot / updated
        # from previous round). This ensures each round only counts NEW
        # events produced by THIS round's remote_open_door, not residual
        # events from previous rounds still within the 300s lookback window.
        raw_counts = {"trigger": len(trigger), "opened": len(opened),
                      "closed": len(closed)}
        if self._baseline_recorded:
            base_serials = self._baseline.get("serialNos", {})
            trigger = [e for e in trigger
                       if int(e.get("serialNo", 0)) > base_serials.get("trigger", 0)]
            opened = [e for e in opened
                      if int(e.get("serialNo", 0)) > base_serials.get("opened", 0)]
            closed = [e for e in closed
                      if int(e.get("serialNo", 0)) > base_serials.get("closed", 0)]
        self._last_events = {"trigger": trigger, "opened": opened, "closed": closed}
        self._mark("recover_query_done",
                   raw=raw_counts,
                   filtered={"trigger": len(trigger), "opened": len(opened),
                             "closed": len(closed)})

        # Self-heal: time skew if trigger missing and skew exceeds threshold.
        # NOTE: with device-time query window above, trigger is rarely missing
        # due to time drift alone. This branch handles genuine missing events
        # (e.g., device firmware bug, event log cleared on reboot).
        if not trigger and self._diagnostic is not None:
            self._mark("heal_diagnose_start")
            skew = self._measure_time_skew()
            env = {"time_skew_seconds": skew,
                   "missing": self._missing_names(trigger, opened, closed),
                   "http_error": None}
            decision = self._diagnostic.diagnose(env)
            self._mark("heal_diagnose_done", decision=decision, skew=round(skew, 2))
            if decision == HEAL_TIME_SYNC and skew > self._time_skew_threshold:
                try:
                    self._client.set_time(_now_iso())
                    # Re-query trigger after heal using the SAME device-time
                    # window (event was recorded at pre-sync device time D1,
                    # which is within [device_now - lookback, device_now]).
                    retrigger = await asyncio.to_thread(
                        self._client.query_events,
                        *HikEventCode.REMOTE_OPEN, start, end)
                    # Apply same baseline filter to re-queried trigger
                    if self._baseline_recorded:
                        base_t = self._baseline.get("serialNos", {}).get("trigger", 0)
                        retrigger = [e for e in retrigger
                                     if int(e.get("serialNo", 0)) > base_t]
                    self._last_events["trigger"] = retrigger
                    self._healed = "time_sync"
                    self._mark("heal_time_sync_done",
                               trigger=len(self._last_events["trigger"]))
                except Exception:  # noqa: BLE001
                    self._healed = None
                self._update_baseline_from_events()
                self._mark("recover_done", recovered=True, healed=self._healed)
                return True
            self._healed = None
            if decision == HEAL_ABORT:
                self._mark("recover_done", recovered=False, reason="heal_abort")
                return False
        self._healed = None
        self._update_baseline_from_events()
        self._mark("recover_done", recovered=True)
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
        self._mark("check_done",
                   remote_open_triggered=facts["remote_open_triggered"],
                   lock_opened=facts["lock_opened"],
                   lock_closed_found=facts["lock_closed"]["found"])
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
