"""HikvisionWorker: door-open test execution + event-chain assertion + self-heal.

Pipeline aligned with the manual inspection logic:
  pre_loop_setup()  -> record initial baseline + baseline reboot + record
                       baseline_reboot_duration (offline -> back -> confirm).
                       Called once before the loop starts.
  do_work(tick)     -> remote_open_door -> sleep(event_check_delay) ->
                       query events (while device online) -> reboot ->
                       wait(baseline_reboot_duration) -> verify online.
                       (reboot phase only when run_reboot=True and
                        plan.skip_reboot=False)
  recover(tick)     -> async: optional self-heal if events missing; update
                       baseline serialNos from this round's events.
  check(tick)       -> sync: assert 3-event chain facts from cached results.

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
                 max_recover_timeout: float = 180.0,
                 event_check_delay: float = 3.0) -> None:
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
        # Delay between remote_open_door PUT and event-log query (spec §6
        # worker.event_check_delay). Real device writes events to log 2-3s
        # after the protocol completes; querying too early misses them.
        self._event_check_delay = event_check_delay
        # Track last do_work outcome for observability
        self._last_work_stages: Dict[str, Any] = {}
        # Per-round timeline of stage executions for observability.
        # Each entry: {"stage": <name>, "ts": <iso>, "t": <seconds_since_round_start>, **extra}
        # Reset at the start of each do_work(); appended by recover()/check() too.
        self._timeline: List[Dict[str, Any]] = []
        self._t0: float = 0.0  # monotonic start of current round
        # Baseline recorded during pre_loop_setup(): stores the latest serialNo
        # of each event type (remote_open, lock_open, lock_close) so that
        # post-reboot queries can filter out pre-existing events and only count
        # new ones from this round's remote_open_door.
        # This solves the "trigger=2" issue where the lookback window contains
        # residual events from previous rounds.
        self._baseline: Dict[str, Any] = {}
        self._baseline_recorded: bool = False
        # Measured reboot duration from pre_loop_setup() baseline reboot.
        # Subsequent per-round reboots wait this long (instead of running
        # the full 3-phase probe each time), since the device's reboot
        # duration is roughly constant. If setup was skipped/not run, falls
        # back to running _wait_online() per round.
        self._baseline_reboot_duration: float = 0.0
        self._setup_done: bool = False
        # Cached facts from do_work(): events are queried BEFORE the per-round
        # reboot while device is online; recover() runs after reboot, so it
        # cannot query events itself. check() reads these cached facts.
        self._round_online_ok: bool = False

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
        """Record device baseline: device time + latest serialNo of each event.

        Captures the latest serialNo of each event type (remote_open,
        lock_open, lock_close) so that subsequent queries can filter out
        pre-existing events. Called during pre_loop_setup() before the
        baseline reboot, and the serialNos are updated after each round.

        Without this, the lookback window would contain residual events
        from previous rounds, inflating counts (e.g., trigger=2 instead
        of 1).
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

    def pre_loop_setup(self) -> Dict[str, Any]:
        """One-time setup before the loop starts (manual inspection step 1-3).

        Mirrors the manual pre-loop checks:
          1. Check and record device initial state (baseline serialNos).
          2. Trigger a baseline reboot.
          3. Measure the reboot duration (offline -> back -> confirm).
          4. Mark setup done; subsequent rounds wait this duration instead
             of running the full 3-phase probe.

        Returns a dict with baseline + duration for observability. If a
        step fails, duration is left at 0.0 so per-round reboots fall back
        to the full _wait_online() probe.
        """
        self._timeline = []
        self._t0 = time.monotonic()
        self._mark("setup_start")
        # Step 1: record initial baseline (device time + serialNos).
        self._mark("setup_baseline_start")
        self._record_baseline()
        self._mark("setup_baseline_done", **self._baseline)

        duration = 0.0
        if self._run_reboot:
            # Step 2: trigger baseline reboot.
            self._mark("setup_reboot_start")
            reboot_res = self.adapter.act({"op": "reboot"})
            self._mark("setup_reboot_done", ok=reboot_res.ok,
                       error=None if reboot_res.ok else reboot_res.error)
            if reboot_res.ok:
                # Step 3: measure reboot duration via 3-phase probe.
                self._mark("setup_probe_start",
                           interval=self._probe_interval,
                           confirm=self._probe_confirm_count)
                t_start = time.monotonic()
                online = self._wait_online()
                duration = time.monotonic() - t_start
                self._mark("setup_probe_done",
                           online=online, duration=round(duration, 2))
                if online:
                    # Optional warmup so subsequent rounds start from a
                    # fully stable device state (event log flushed, etc.).
                    self._mark("setup_warmup_start", seconds=self._warmup_time)
                    time.sleep(self._warmup_time)
                    self._mark("setup_warmup_done")
        self._baseline_reboot_duration = duration
        self._setup_done = True
        self._mark("setup_done",
                   baseline_reboot_duration=round(duration, 2))
        return {"baseline": dict(self._baseline),
                "baseline_reboot_duration": duration,
                "setup_done": True}

    def do_work(self, tick: dict) -> Any:
        """Execute the per-round test (manual inspection step 4 onwards).

        Flow per round:
          1. remote_open_door (PUT /ISAPI/AccessControl/RemoteControl/door/N)
          2. sleep event_check_delay (2-3s) for the device to write events
             to its event log.
          3. Query the 3-event chain (REMOTE_OPEN + LOCK_OPEN + LOCK_CLOSE)
             using a device-time window while the device is still online.
          4. (if run_reboot and not skip_reboot) reboot device
          5. Wait baseline_reboot_duration (measured in pre_loop_setup) OR
             fall back to _wait_online() 3-phase probe if no baseline.
          6. Verify device is online (get_work_status).

        Events are queried in step 3 (BEFORE the per-round reboot) so we
        capture them while the device is reachable; recover() cannot query
        after the reboot because the device may still be booting.
        """
        plan = self.state.get("plan", {}) or {}
        skip_reboot = bool(plan.get("skip_reboot", False))
        # event_check_delay_adjust lets the LLM plan extend the delay when
        # cold-start propagation is slow (default 0 = no adjustment).
        delay_adjust = float(plan.get("event_check_delay_adjust", 0) or 0)
        op = tick.get("operation") or {"op": "remote_open_door"}
        # Reset per-round timeline at the start of do_work.
        self._timeline = []
        self._t0 = time.monotonic()
        stages: Dict[str, Any] = {"op": op.get("op"), "skip_reboot": skip_reboot,
                                  "timeline": self._timeline}
        self._round_online_ok = False

        # Step 1: remote_open_door (the protocol trigger under test).
        self._mark("act_start", op=op.get("op"))
        result = self.adapter.act(op)
        self._mark("act_done", ok=result.ok,
                   error=None if result.ok else result.error)
        stages["act_ok"] = result.ok
        if not result.ok:
            stages["act_error"] = result.error
            self._last_work_stages = stages
            return result

        # Step 2: wait for device to write events to its log (spec §6
        # worker.event_check_delay). Without this delay, query_events may
        # return empty because the device hasn't flushed the just-generated
        # events to its log yet.
        delay = self._event_check_delay + delay_adjust
        self._mark("event_delay_start", delay=delay)
        time.sleep(delay)
        self._mark("event_delay_done")

        # Step 3: query events BEFORE reboot (device is online, reachable).
        # Use device-time window so we match the device's event-log clock
        # (spec §2.1 A.2). Lookback covers the delay plus a small buffer.
        lookback = max(30, int(delay) + 10)
        self._query_events_pre_reboot(lookback)

        # Step 4-6: reboot + wait + verify (only when run_reboot enabled).
        if self._run_reboot and not skip_reboot:
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

            # Step 5: wait for device to come back. If we have a measured
            # baseline_reboot_duration from pre_loop_setup, sleep that long
            # (device reboot time is roughly constant) and then verify.
            # Otherwise, fall back to the full 3-phase _wait_online probe.
            if self._baseline_reboot_duration > 0:
                wait = self._baseline_reboot_duration
                self._mark("reboot_wait_start", wait=round(wait, 2),
                           mode="baseline_duration")
                time.sleep(wait)
                self._mark("reboot_wait_done")
            else:
                self._mark("probe_start", interval=self._probe_interval,
                           confirm=self._probe_confirm_count,
                           mode="3phase_probe")
                online = self._wait_online()
                self._mark("probe_done", online=online)
                if not online:
                    from ...multi_agent.adapter import Result
                    stages["online"] = False
                    self._last_work_stages = stages
                    return Result(ok=False, error="device did not come online "
                                  f"within {self._max_recover_timeout}s")

            # Step 6: verify device online after the wait.
            self._mark("verify_online_start")
            try:
                self._client.get_work_status()
                online_ok = True
            except Exception:  # noqa: BLE001
                online_ok = False
            self._mark("verify_online_done", online=online_ok)
            stages["online"] = online_ok
            self._round_online_ok = online_ok
        else:
            # No reboot this round; device is still online after step 3.
            self._round_online_ok = True

        self._last_work_stages = stages
        return result

    def _query_events_pre_reboot(self, lookback: int) -> None:
        """Query the 3-event chain using a device-time window, BEFORE reboot.

        Called from do_work() while the device is still online. Applies the
        baseline serialNo filter so only NEW events from this round's
        remote_open_door are counted. Results stored in self._last_events
        for check() to read.

        lookback -- device-time window in seconds (>= 30).
        """
        self._mark("query_events_start", lookback=lookback)
        try:
            device_time = self._client.get_time()["Time"]["localTime"]
        except Exception:  # noqa: BLE001
            device_time = _now_iso()
        start = _iso_seconds_before(device_time, lookback)
        end = device_time
        self._mark("query_window", start=start, end=end)
        try:
            trigger = self._client.query_events(*HikEventCode.REMOTE_OPEN, start, end)
            opened = self._client.query_events(*HikEventCode.LOCK_OPEN, start, end)
            closed = self._client.query_events(*HikEventCode.LOCK_CLOSE, start, end)
        except Exception as exc:  # noqa: BLE001
            self._last_events = {"trigger": [], "opened": [], "closed": []}
            self._recover_error = str(exc)
            self._mark("query_events_done", error=str(exc))
            return
        raw_counts = {"trigger": len(trigger), "opened": len(opened),
                      "closed": len(closed)}
        # Filter out pre-baseline events: only count events with serialNo
        # greater than the baseline so residual events from previous rounds
        # don't inflate counts.
        if self._baseline_recorded:
            base_serials = self._baseline.get("serialNos", {})
            trigger = [e for e in trigger
                       if int(e.get("serialNo", 0)) > base_serials.get("trigger", 0)]
            opened = [e for e in opened
                      if int(e.get("serialNo", 0)) > base_serials.get("opened", 0)]
            closed = [e for e in closed
                      if int(e.get("serialNo", 0)) > base_serials.get("closed", 0)]
        self._last_events = {"trigger": trigger, "opened": opened, "closed": closed}
        self._mark("query_events_done",
                   raw=raw_counts,
                   filtered={"trigger": len(trigger), "opened": len(opened),
                             "closed": len(closed)})

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

        Used by pre_loop_setup() for the baseline measurement; do_work()
        uses baseline_reboot_duration sleep instead when available.
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
        """Post-do_work self-heal + baseline update.

        Events were already queried in do_work() BEFORE the per-round reboot
        (while device was online). This method:
          - Runs the LLM diagnostic self-heal if events are missing and
            time skew exceeds threshold (rare with device-time query window).
          - Updates baseline serialNos from this round's events so the next
            round's filter uses them.
          - Returns the round's online verification status from do_work().
        """
        self._mark("recover_start")
        trigger = self._last_events.get("trigger", [])
        opened = self._last_events.get("opened", [])
        closed = self._last_events.get("closed", [])

        # Self-heal: only when trigger missing AND we have a diagnostic kernel.
        # With device-time query window + event_check_delay, trigger is rarely
        # missing due to drift alone. This branch handles genuine missing
        # events (firmware bug, event log cleared on reboot).
        if not trigger and self._diagnostic is not None:
            self._mark("heal_diagnose_start")
            skew = self._measure_time_skew()
            env = {"time_skew_seconds": skew,
                   "missing": self._missing_names(trigger, opened, closed),
                   "http_error": getattr(self, "_recover_error", None)}
            decision = self._diagnostic.diagnose(env)
            self._mark("heal_diagnose_done", decision=decision,
                       skew=round(skew, 2))
            if decision == HEAL_TIME_SYNC and skew > self._time_skew_threshold:
                try:
                    self._client.set_time(_now_iso())
                    self._healed = "time_sync"
                    self._mark("heal_time_sync_done")
                except Exception:  # noqa: BLE001
                    self._healed = None
                self._update_baseline_from_events()
                self._mark("recover_done",
                           recovered=self._round_online_ok,
                           healed=self._healed)
                return self._round_online_ok
            self._healed = None
            if decision == HEAL_ABORT:
                self._mark("recover_done", recovered=False, reason="heal_abort")
                return False
        self._healed = None
        self._update_baseline_from_events()
        self._mark("recover_done", recovered=self._round_online_ok)
        return self._round_online_ok

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
