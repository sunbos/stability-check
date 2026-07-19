"""Verifier / 校验引擎的单测。

锁定校验能力的行为：
  - 输入护栏 fail-closed 抛 VerifyError；非 fail-closed 则返回 VerifyResult
  - run_eval 聚合评分（EvalResult / 数值 / 元组）
  - VerificationAgent 经总线 request/reply 护栏

纯单元测试，无外部依赖。
"""

import pytest

from stability_harness_loop_multiagent.core.agent import AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.verify import (
    EvalResult,
    VerificationAgent,
    VerifyError,
    Verifier,
)


def test_input_guardrail_pass():
    v = Verifier()
    v.add_input_guardrail("g1", lambda item: (True, "ok"))
    res = v.validate_input({"x": 1})
    assert res.ok is True


def test_input_guardrail_fail_closed_raises():
    v = Verifier(fail_closed=True)
    v.add_input_guardrail("g2", lambda item: (False, "bad"))
    with pytest.raises(VerifyError) as exc:
        v.validate_input({})
    assert exc.value.reason == "bad"
    assert exc.value.hook == "g2"


def test_input_guardrail_soft_returns_result():
    v = Verifier(fail_closed=False)
    v.add_input_guardrail("g3", lambda item: (False, "soft"))
    res = v.validate_input({})
    assert res.ok is False
    assert res.reason == "soft"


def test_run_eval_aggregation():
    v = Verifier()
    v.add_eval_hook("e1", lambda rec: EvalResult(name="e1", ok=True, score=0.8))
    v.add_eval_hook("e2", lambda rec: 0.5)  # 数值 -> ok=True, score=0.5
    report = v.run_eval({"foo": 1})
    assert report.passed is True
    assert abs(report.score - 0.65) < 1e-9

    v2 = Verifier()
    v2.add_eval_hook(
        "e3", lambda rec: EvalResult(name="e3", ok=False, score=0.0, reason="nope")
    )
    report2 = v2.run_eval({})
    assert report2.passed is False


@pytest.mark.asyncio
async def test_verification_agent_bus_allows():
    bus = EventBus()
    v = Verifier()
    v.add_input_guardrail("g1", lambda item: (True, "ok"))
    va = VerificationAgent(bus, v)
    await va.start()
    reply = await bus.request(
        "harness/verify/request", {"stage": "input", "item": {"x": 1}}, timeout=1.0
    )
    assert reply["allowed"] is True
    await va.stop()


@pytest.mark.asyncio
async def test_verification_agent_bus_denies():
    bus = EventBus()
    v = Verifier()
    v.add_input_guardrail("g2", lambda item: (False, "bad"))
    va = VerificationAgent(bus, v)
    await va.start()
    reply = await bus.request(
        "harness/verify/request", {"stage": "input", "item": {}}, timeout=1.0
    )
    assert reply["allowed"] is False
    assert reply["reason"] == "bad"
    assert reply["hook"] == "g2"
    await va.stop()
