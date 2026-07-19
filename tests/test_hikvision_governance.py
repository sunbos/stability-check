"""Hikvision worker 治理闸门 opt-in 集成测试。

验证 P1-3：worker.act() 在 to_thread(do_work) 之前经总线发一次
harness/govern/request（operation="round"）；允许则执行、拒绝则跳过并上报
denied 事实。纯总线契约，不影响既有未启用治理的行为。
"""

import pytest

from stability_harness_loop_multiagent.business.hikvision.adapter import HikvisionAdapter
from stability_harness_loop_multiagent.business.hikvision.worker import HikvisionWorker
from stability_harness_loop_multiagent.core.agent import AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.governance import (
    AccessControl,
    Governance,
    GovernanceAgent,
)
from tests.fakes.fake_hikvision import FakeHikvisionClient


def _make(bus, client, governance=None):
    spec = AgentSpec(id="w1", role="hik", capabilities={"act"})
    adapter = HikvisionAdapter(client)
    worker = HikvisionWorker(
        bus, spec, adapter, client,
        run_reboot=False, probe_interval=0.01, probe_confirm_count=2,
        warmup_time=0.0, max_recover_timeout=1.0, event_check_delay=0.0,
        enable_governance=(governance is not None),
    )
    return worker


@pytest.mark.asyncio
async def test_gate_allows_when_policy_permits():
    bus = EventBus()
    client = FakeHikvisionClient()
    gov = Governance(access=AccessControl(policy={"hik": {"door-test": "*"}}))
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    worker = _make(bus, client, governance=gov)
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    # 闸门放行 -> do_work 执行（rounds 计数 +1，无 denied 标记）
    assert worker.get_chain_stats()["rounds"] == 1
    stages = [e["stage"] for e in worker._timeline]
    assert "governance_denied" not in stages
    await agent.stop()


@pytest.mark.asyncio
async def test_gate_denies_blocks_do_work():
    bus = EventBus()
    client = FakeHikvisionClient()
    # 策略只放行 "other"，拒绝本 worker 发出的 operation="round"
    gov = Governance(access=AccessControl(policy={"hik": {"door-test": ["other"]}}))
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    worker = _make(bus, client, governance=gov)
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    # 闸门拒绝 -> do_work 被跳过（rounds 仍为 0）
    assert worker.get_chain_stats()["rounds"] == 0
    assert "governance_denied" in [e["stage"] for e in worker._timeline]
    assert client._reboot_called is False
    await agent.stop()
