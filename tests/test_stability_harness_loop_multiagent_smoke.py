"""针对通用 stability_harness_loop_multiagent 框架的集成冒烟测试。

纯粹通过 EventBus 将三个引擎连接起来（没有具体场景），并断言以下核心不变量：

  1. 循环会终止（到达其轮次上限；无死锁 / 挂起）。
  2. 每轮都由权威的 DecisionAuthority 产生裁决。
  3. Observer 会收到事件（总线扇出端到端可用）。
  4. 事实独裁：一个被注入的失败事实会强制产生 'fail' 裁决，尽管 Advisor 以高
     置信度投出了低风险。

仅使用标准库。运行方式：
    python stability_harness_loop_multiagent/examples/smoke.py   # 独立断言
    pytest tests/test_stability_harness_loop_multiagent_smoke.py # 本文件
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from stability_harness_loop_multiagent import DecisionAuthority
from stability_harness_loop_multiagent.examples.smoke import (
    FakeTargetAdapter,
    assert_failing_fact,
    assert_healthy,
    run_smoke,
)


@pytest.mark.asyncio
async def test_loop_terminates_and_produces_verdicts():
    result = await run_smoke(fail=False, max_rounds=5)
    # 不变量 1 + 2 + 3：终止、产生了裁决、Observer 看到了事件。
    assert_healthy(result)
    # 无死锁：本次运行已返回（asyncio.wait_for 本会超时）。
    assert result["ctx"].round_count == 5
    assert result["loop"].verdict is not None


@pytest.mark.asyncio
async def test_fact_dictatorship_failing_fact_forces_fail():
    result = await run_smoke(fail=True, max_rounds=5)
    # 不变量 4：事实独裁覆盖了高置信度的低风险 Advisor 投票。
    assert_failing_fact(result)


def test_decision_authority_fact_dictatorship_unit():
    """对安全底线的纯单元测试：任何 falsy 事实 => fail，句号。"""
    dec = DecisionAuthority()
    # 低风险 + 一个 False 事实 => fail（风险无法升级一个已破损的事实）。
    v = dec.decide({"acted": True, "state_ok": False}, risk_score=30.0)
    assert v.decision == "fail"
    assert v.reason.startswith("事实未满足")
    # 所有事实满足 + 低风险 => pass。
    assert dec.decide({"acted": True, "state_ok": True}, risk_score=30.0).decision == "pass"
    # 空事实字典是“没有 falsy 事实” => pass（当没有 Worker 上报时，Loop 会注入
    # 一个失败的 'checks_received' 事实，见 ControlLoop._merge_facts）。
    assert dec.decide({}, risk_score=10.0).decision == "pass"


@pytest.mark.asyncio
async def test_fake_adapter_is_structural_target_adapter():
    """假适配器表现得像一个 TargetAdapter，而无需子类化它。"""
    from stability_harness_loop_multiagent.multi_agent.adapter import TargetAdapter

    a = FakeTargetAdapter()
    assert isinstance(a, TargetAdapter)  # runtime_checkable 协议
    r = a.act("ping")
    assert r.ok and r.data["counter"] == 1
    assert a.observe().snapshot["up"] is True
    a.fail = True
    assert a.observe().snapshot["up"] is False
    assert any(e.kind == "degraded" for e in a.events(0.0))
