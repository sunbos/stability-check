# tests/test_hikvision_runner.py
import pytest
from stability_harness_loop_multiagent.business.hikvision.runner import run_hikvision_stability
from tests.fakes.fake_hikvision import FakeHikvisionClient


@pytest.mark.asyncio
async def test_runner_completes_with_fake_client():
    result = await run_hikvision_stability(
        client=FakeHikvisionClient(),
        max_rounds=3,
        run_timeout=10.0,
    )
    ctx = result["ctx"]
    assert ctx.round_count == 3
    assert ctx.aborted
    # Architecture invariant: verdict produced
    assert result["loop"].verdict is not None


@pytest.mark.asyncio
async def test_runner_worker_subscribes_plan():
    result = await run_hikvision_stability(
        client=FakeHikvisionClient(),
        max_rounds=1,
        run_timeout=10.0,
        instruction="skip reboot",
    )
    worker = result["worker"]
    # Worker should have cached plan from advisor
    assert "plan" in worker.state
