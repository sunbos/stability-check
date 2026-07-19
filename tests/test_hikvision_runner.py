# tests/test_hikvision_runner.py
import pytest
from stability_harness_loop_multiagent.business.hikvision.runner import run_hikvision_stability
from tests.fakes.fake_hikvision import FakeHikvisionClient


@pytest.mark.asyncio
async def test_runner_completes_with_fake_client():
    """run_reboot=False: loop completes 3 rounds within timeout.

    Using run_reboot=False + event_check_delay=0.0 avoids the ~60s baseline
    reboot probe in pre_loop_setup. ControlLoop hard-waits
    max(recover_timeout, check_timeout) per round (driver.py); with the door
    close-poll timeout formula (max(event_check_delay, open_duration*3+5)+3)
    each round is ~14s, so run_timeout must cover 3 rounds (~42s) plus margin.
    The reboot flow is covered by test_hikvision_worker.py.
    """
    result = await run_hikvision_stability(
        client=FakeHikvisionClient(),
        max_rounds=3,
        run_timeout=60.0,
        run_reboot=False,
        event_check_delay=0.0,
    )
    ctx = result["ctx"]
    assert ctx.round_count == 3
    assert ctx.aborted
    # Architecture invariant: verdict produced
    assert result["loop"].verdict is not None


@pytest.mark.asyncio
async def test_runner_worker_subscribes_plan():
    """Worker caches hikvision/plan published by Advisor during start()."""
    result = await run_hikvision_stability(
        client=FakeHikvisionClient(),
        max_rounds=1,
        run_timeout=30.0,
        instruction="skip reboot",
        run_reboot=False,
        event_check_delay=0.0,
    )
    worker = result["worker"]
    # Worker should have cached plan from advisor
    assert "plan" in worker.state
