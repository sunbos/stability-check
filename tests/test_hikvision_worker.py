# tests/test_hikvision_worker.py
import pytest
from stability_harness_loop_multiagent.business.hikvision.worker import HikvisionWorker
from stability_harness_loop_multiagent.business.hikvision.event_codes import HikEventCode
from stability_harness_loop_multiagent.business.hikvision.adapter import HikvisionAdapter
from stability_harness_loop_multiagent.business.hikvision.diagnostic import (
    DiagnosticKernel, HEAL_TIME_SYNC, HEAL_RETRIGGER,
)
from stability_harness_loop_multiagent.harness.bus import EventBus
from stability_harness_loop_multiagent.harness.agent import AgentSpec
from tests.fakes.fake_hikvision import FakeHikvisionClient


def _make_worker(client=None, time_skew_threshold=3.0, with_diagnostic=False,
                 run_reboot=False):
    bus = EventBus()
    spec = AgentSpec(id="w1", role="hik", capabilities={"act"})
    client = client or FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    diagnostic = None
    if with_diagnostic:
        def default_decide(env: dict) -> str:
            if env.get("time_skew_seconds", 0) > time_skew_threshold:
                return HEAL_TIME_SYNC
            return HEAL_RETRIGGER
        diagnostic = DiagnosticKernel(llm_decide=default_decide,
                                      whitelist=[HEAL_TIME_SYNC, HEAL_RETRIGGER])
    worker = HikvisionWorker(bus, spec, adapter, client,
                             time_skew_threshold=time_skew_threshold,
                             diagnostic=diagnostic,
                             run_reboot=run_reboot,
                             probe_interval=0.01,
                             probe_confirm_count=2,
                             warmup_time=0.0,
                             max_recover_timeout=1.0,
                             event_check_delay=0.0)
    return bus, worker, client


@pytest.mark.asyncio
async def test_worker_check_all_events_present_passes():
    """run_reboot=False: do_work opens door, queries events, all 3 present."""
    bus, worker, client = _make_worker()
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    facts = worker.check(tick)
    assert facts["remote_open_triggered"] is True
    assert facts["lock_opened"] is True
    # lock_closed is a soft fact (non-bool dict), per spec §2.1
    assert facts["lock_closed"]["found"] is True


@pytest.mark.asyncio
async def test_worker_check_lock_open_missing_fails_fact():
    """Suppressing lock_open after do_work -> check() reports lock_opened=False."""
    bus, worker, client = _make_worker()
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    # Suppress lock-open event after act, re-query, update cache
    client.suppress_event(*HikEventCode.LOCK_OPEN)
    worker._last_events["opened"] = client.query_events(
        *HikEventCode.LOCK_OPEN, client._win_start, client._win_end)
    facts = worker.check(tick)
    assert facts["lock_opened"] is False  # fact dictatorship -> fail


@pytest.mark.asyncio
async def test_worker_self_heals_time_skew():
    """recover() triggers time_sync heal when trigger missing + skew > threshold.

    Without calling do_work first, _last_events is empty (default), so trigger
    is missing. recover() runs the diagnostic kernel -> time_sync heal.
    """
    client = FakeHikvisionClient(time_skew_seconds=10.0)
    bus, worker, client = _make_worker(client=client, with_diagnostic=True)
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.recover(tick)
    assert client._skew == 0.0  # set_time cleared skew
    facts = worker.check(tick)
    assert facts.get("self_healed") == "time_sync"


@pytest.mark.asyncio
async def test_worker_pre_loop_setup_records_baseline_and_duration():
    """pre_loop_setup() records baseline serialNos + measures reboot duration."""
    bus, worker, client = _make_worker(run_reboot=True)
    info = worker.pre_loop_setup()
    assert info["setup_done"] is True
    assert info["baseline_reboot_duration"] > 0.0  # 3-phase probe completed
    assert worker._baseline_recorded is True
    assert "serialNos" in worker._baseline
    assert worker._setup_done is True
    # reboot was called during setup
    assert client._reboot_called is True


@pytest.mark.asyncio
async def test_worker_reboot_flow_open_then_reboot_then_verify():
    """run_reboot=True with baseline: do_work opens -> queries -> reboots -> verifies.

    New flow (manual inspection logic):
      1. remote_open_door
      2. wait event_check_delay
      3. query events (BEFORE reboot, device online)
      4. reboot
      5. wait baseline_reboot_duration (measured in pre_loop_setup)
      6. verify device online
    """
    bus, worker, client = _make_worker(run_reboot=True)
    # Pre-loop setup must run first to measure baseline_reboot_duration;
    # otherwise do_work falls back to 3-phase probe (also works but slower).
    worker.pre_loop_setup()
    assert worker._baseline_reboot_duration > 0.0
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    # Reboot called during do_work (separate from the setup reboot).
    assert client._reboot_called is True
    # Stages recorded for observability.
    assert worker._last_work_stages["act_ok"] is True
    assert worker._last_work_stages.get("reboot_ok") is True
    assert worker._last_work_stages["online"] is True
    # Events generated by remote_open_door and queried before reboot.
    facts = worker.check(tick)
    assert facts["remote_open_triggered"] is True
    assert facts["lock_opened"] is True


@pytest.mark.asyncio
async def test_worker_skip_reboot_via_plan():
    """plan.skip_reboot=True bypasses reboot phase even when run_reboot=True."""
    bus, worker, client = _make_worker(run_reboot=True)
    # Simulate Advisor publishing skip_reboot plan
    worker.state["plan"] = {"skip_reboot": True}
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    # No reboot in do_work (pre_loop_setup not called either).
    assert client._reboot_called is False
    # Door still opened, events queried.
    facts = worker.check(tick)
    assert facts["remote_open_triggered"] is True
    assert facts["lock_opened"] is True
    # No reboot stage recorded.
    assert "reboot_ok" not in worker._last_work_stages


@pytest.mark.asyncio
async def test_worker_baseline_filter_excludes_pre_existing_events():
    """Baseline serialNo filter excludes events from previous rounds.

    Without the filter, the lookback window would contain residual events
    from previous rounds, inflating counts (e.g., trigger=2 instead of 1).
    """
    bus, worker, client = _make_worker(run_reboot=False)
    # Pre-populate events by calling remote_open_door directly (simulating
    # a previous round). Then record baseline, which should capture these
    # serialNos. A subsequent do_work should only count NEW events.
    client.remote_open_door(1)
    worker.pre_loop_setup()  # records baseline with serialNos from above
    pre_baseline_serials = dict(worker._baseline["serialNos"])
    assert pre_baseline_serials["trigger"] > 0

    # do_work triggers another remote_open_door and queries events.
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    facts = worker.check(tick)
    # Even though the lookback window contains both old and new events,
    # the baseline filter should exclude the old ones -> count == 1.
    assert facts["remote_open_triggered"] is True
    assert facts["lock_opened"] is True
    # Verify only NEW events are in _last_events (1 each, not 2).
    assert len(worker._last_events["trigger"]) == 1
    assert len(worker._last_events["opened"]) == 1


@pytest.mark.asyncio
async def test_worker_timeline_records_pre_reboot_query_stage():
    """Timeline should include query_events stage (events queried before reboot)."""
    bus, worker, client = _make_worker(run_reboot=False)
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    stages = [e["stage"] for e in worker._timeline]
    # New flow stages.
    assert "act_start" in stages
    assert "event_delay_start" in stages
    assert "event_delay_done" in stages
    assert "query_events_start" in stages
    assert "query_events_done" in stages
    assert "check_done" in stages
