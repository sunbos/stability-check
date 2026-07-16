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


def _make_worker(client=None, time_skew_threshold=3.0, with_diagnostic=False):
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
                             diagnostic=diagnostic)
    return bus, worker, client


@pytest.mark.asyncio
async def test_worker_check_all_events_present_passes():
    bus, worker, client = _make_worker()
    tick = {"round": 1, "window_start": client._win_start,
            "window_end": client._win_end, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    facts = worker.check(tick)
    assert facts["remote_open_triggered"] is True
    assert facts["lock_opened"] is True
    # lock_closed is a soft fact (non-bool dict), per spec §2.1
    assert facts["lock_closed"]["found"] is True


@pytest.mark.asyncio
async def test_worker_check_lock_open_missing_fails_fact():
    bus, worker, client = _make_worker()
    tick = {"round": 1, "window_start": client._win_start,
            "window_end": client._win_end, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    # Suppress lock-open event after act, re-query, update cache
    client.suppress_event(*HikEventCode.LOCK_OPEN)
    worker._last_events["opened"] = client.query_events(
        *HikEventCode.LOCK_OPEN, client._win_start, client._win_end)
    facts = worker.check(tick)
    assert facts["lock_opened"] is False  # fact dictatorship -> fail


@pytest.mark.asyncio
async def test_worker_self_heals_time_skew():
    client = FakeHikvisionClient(time_skew_seconds=10.0)
    bus, worker, client = _make_worker(client=client, with_diagnostic=True)
    # Directly call recover (no act -> no events -> trigger missing -> heal)
    tick = {"round": 1, "window_start": client._win_start,
            "window_end": client._win_end, "operation": {"op": "remote_open_door"}}
    await worker.recover(tick)
    assert client._skew == 0.0  # set_time cleared skew
    facts = worker.check(tick)
    assert facts.get("self_healed") == "time_sync"
