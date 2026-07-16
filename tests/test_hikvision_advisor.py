# tests/test_hikvision_advisor.py
import pytest
from stability_harness_loop_multiagent.business.hikvision.advisor import HikvisionAdvisor
from stability_harness_loop_multiagent.harness.bus import EventBus
from stability_harness_loop_multiagent.harness.agent import AgentSpec


@pytest.mark.asyncio
async def test_advisor_parses_instruction_and_publishes_plan():
    bus = EventBus()
    received = []
    bus.subscribe("hikvision/plan", lambda t, m: received.append(m))

    def fake_parse(instruction: str) -> dict:
        return {"skip_reboot": True, "event_check_delay_adjust": 2,
                "trigger_interval_adjust": 0, "diagnose_whitelist": ["time_sync"]}

    advisor = HikvisionAdvisor(
        bus, AgentSpec(id="a1", role="risk"),
        instruction="skip reboot, add 2s event wait",
        llm_parse=fake_parse,
    )
    await advisor.start()
    await advisor.stop()
    assert len(received) == 1
    plan = received[0]
    assert plan["skip_reboot"] is True
    assert plan["diagnose_whitelist"] == ["time_sync"]


def test_advisor_vote_returns_trend_risk():
    bus = EventBus()
    advisor = HikvisionAdvisor(
        bus, AgentSpec(id="a1", role="risk"),
        instruction="",
        llm_parse=lambda s: {},
    )
    risk, conf = advisor.vote()
    assert 0 <= risk <= 100
    assert 0 <= conf <= 1
