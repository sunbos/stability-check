"""HikvisionAdvisor 校验闸门集成测试（P1-b）。

验证：解析出的计划在采纳前经 harness/verify/request 做 fail-closed 护栏；
被拒/超时则丢弃计划（不发布 hikvision/plan），由规则兜底接管。纯总线契约。
"""

import asyncio

import pytest

from stability_harness_loop_multiagent.business.hikvision.advisor import (
    HikvisionAdvisor,
)
from stability_harness_loop_multiagent.core.agent import AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.verify import (
    VerificationAgent,
    Verifier,
)


def _capture(bus, topic):
    msgs = []
    bus.subscribe(topic, lambda _t, m: msgs.append(m))
    return msgs


@pytest.mark.asyncio
async def test_advisor_publishes_plan_when_verify_allows():
    bus = EventBus()
    verifier = Verifier()  # 无护栏 -> 放行
    agent = VerificationAgent(bus, verifier)
    await agent.start()
    captured = _capture(bus, "hikvision/plan")
    advisor = HikvisionAdvisor(
        bus, AgentSpec(id="a1", role="risk"), instruction="",
        llm_parse=lambda s: {"skip_reboot": True},
        enable_verify=True,
    )
    await advisor.start()
    # 总线发布为 fire-and-forget：让出事件循环，使订阅者 handler 执行。
    await asyncio.sleep(0)
    assert captured, "校验放行后计划应被发布"
    assert captured[0].get("skip_reboot") is True
    await agent.stop()


@pytest.mark.asyncio
async def test_advisor_discards_plan_when_verify_denies():
    bus = EventBus()
    verifier = Verifier().add_input_guardrail(
        "deny", lambda item: (False, "rejected"))
    agent = VerificationAgent(bus, verifier)
    await agent.start()
    captured = _capture(bus, "hikvision/plan")
    advisor = HikvisionAdvisor(
        bus, AgentSpec(id="a1", role="risk"), instruction="",
        llm_parse=lambda s: {"skip_reboot": True},
        enable_verify=True,
    )
    await advisor.start()
    await asyncio.sleep(0)
    assert captured == [], "被拒计划不应发布 hikvision/plan"
    await agent.stop()
