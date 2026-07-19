"""Governance / 治理引擎的单测。

锁定治理能力的行为，使其不再是"死代码"：
  - AccessControl 放行/拒绝/默认拒绝/通配
  - Quota / Budget 滚动与累计上限
  - CircuitBreaker 状态机
  - Governance.evaluate 两阶段：允许时提交配额/预算、拒绝时**不突变**（P1-1 修复核心）
  - GovernanceAgent 经总线 request/reply 闸门
  - gate_allowed 异步闸门 fail-closed

全部为纯单元/集成测试，使用标准库 + MemorySink，无外部依赖。
"""

import pytest

from stability_harness_loop_multiagent.core.agent import Agent, AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.governance import (
    AccessControl,
    Budget,
    CircuitBreaker,
    Governance,
    GovernanceAgent,
    Quota,
    gate_allowed,
)


# ---- AccessControl -------------------------------------------------------

def test_accesscontrol_default_deny():
    ac = AccessControl(default_deny=True)
    ok, _ = ac.allow("hik", "door", "open")
    assert ok is False


def test_accesscontrol_explicit_allow():
    ac = AccessControl(policy={"hik": {"door": ["open", "reboot"]}})
    assert ac.allow("hik", "door", "open")[0] is True
    assert ac.allow("hik", "door", "reboot")[0] is True
    # 同一 capability 下未列出的操作 -> 拒绝
    assert ac.allow("hik", "door", "close")[0] is False


def test_accesscontrol_wildcard_capability():
    ac = AccessControl(policy={"hik": {"door": "*"}})
    assert ac.allow("hik", "door", "anything")[0] is True


def test_accesscontrol_wildcard_role():
    ac = AccessControl(policy={"*": {"*": "*"}})
    assert ac.allow("any", "any", "any")[0] is True


# ---- Quota ---------------------------------------------------------------

def test_quota_consume_limit():
    q = Quota(limit=2)
    assert q.consume("k")[0] is True
    assert q.consume("k")[0] is True
    ok, reason = q.consume("k")
    assert ok is False
    assert "exceeded" in reason


def test_quota_can_consume_non_mutating():
    q = Quota(limit=1)
    ok, _ = q.can_consume("k")
    assert ok is True
    assert q._counts == {}  # 探测不突变
    # 真正消费后才突变
    q.consume("k")
    assert q._counts.get("k") == 1
    # 超过上限的探测仍不突变
    ok2, _ = q.can_consume("k")
    assert ok2 is False
    assert q._counts.get("k") == 1


def test_quota_windowed_roll():
    q = Quota(limit=1, window=10.0)
    assert q.consume("k")[0] is True
    # 窗口内再消费 -> 拒绝
    assert q.can_consume("k")[0] is False
    # 模拟窗口过期后回绕
    q._first["k"] = 0.0
    assert q.can_consume("k")[0] is True


# ---- Budget --------------------------------------------------------------

def test_budget_spend_limit():
    b = Budget(limit=10.0)
    assert b.spend(6.0)[0] is True
    assert b.spend(3.0)[0] is True
    ok, reason = b.spend(5.0)
    assert ok is False
    assert "exceeded" in reason


def test_budget_can_spend_non_mutating():
    b = Budget(limit=5.0)
    assert b.can_spend(5.0)[0] is True
    assert b._spent == 0.0  # 探测不突变
    b.spend(5.0)
    assert b._spent == 5.0
    ok2, _ = b.can_spend(1.0)
    assert ok2 is False
    assert b._spent == 5.0  # 仍不突变


# ---- CircuitBreaker ------------------------------------------------------

def test_circuit_breaker_state_machine():
    cb = CircuitBreaker(failure_threshold=1, cooldown=10.0, half_open_probes=1)
    assert cb.state == "closed"
    assert cb.allow() is True
    cb.record_failure()  # 达阈值 -> 打开
    assert cb.state == "open"
    assert cb.allow() is False  # 冷却未过
    cb._opened_at = 0.0  # 强制冷却过期
    assert cb.allow() is True  # 打开 -> 半开
    assert cb.allow() is True  # 半开：探针数未达上限
    cb.record_success()  # 半开探针成功 -> 关闭
    assert cb.state == "closed"


# ---- Governance.evaluate 两阶段（P1-1 修复核心） ------------------------

def test_evaluate_allowed_commits_quota_and_budget():
    gov = Governance(
        access=AccessControl(policy={"hik": {"door": "*"}}),
        quotas={"q": Quota(limit=10)},
        budgets={"b": Budget(limit=100.0)},
    )
    allowed, _, _ = gov.evaluate(
        {"role": "hik", "capability": "door", "operation": "open", "cost": 5.0}
    )
    assert allowed is True
    assert gov.quotas["q"]._counts.get("hik") == 1  # quota_key 默认 = role
    assert gov.budgets["b"]._spent == 5.0


def test_evaluate_denied_by_access_does_not_consume():
    gov = Governance(
        access=AccessControl(policy={"hik": {"door": ["open"]}}),  # 拒绝 reboot
        quotas={"q": Quota(limit=10)},
        budgets={"b": Budget(limit=100.0)},
    )
    allowed, _, breaches = gov.evaluate(
        {"role": "hik", "capability": "door", "operation": "reboot", "cost": 5.0}
    )
    assert allowed is False
    assert ("access",) in [(n,) for n, _ in breaches]
    # 关键：被访问控制在第一步拒绝，配额/预算不得被扣减
    assert gov.quotas["q"]._counts == {}
    assert gov.budgets["b"]._spent == 0.0


def test_evaluate_denied_by_quota_does_not_spend_budget():
    gov = Governance(
        access=AccessControl(policy={"hik": {"door": "*"}}),
        quotas={"q": Quota(limit=1)},
        budgets={"b": Budget(limit=100.0)},
    )
    # 第一次：允许，消耗 1 配额 + 花费 10
    a1, _, _ = gov.evaluate(
        {"role": "hik", "capability": "door", "operation": "x", "cost": 10.0}
    )
    assert a1 is True
    assert gov.budgets["b"]._spent == 10.0
    # 配额已满（limit=1）。第二次：访问通过但配额拒绝 -> 整体拒绝，预算不得突变
    a2, _, breaches = gov.evaluate(
        {"role": "hik", "capability": "door", "operation": "x", "cost": 50.0}
    )
    assert a2 is False
    assert any(n.startswith("quota:") for n, _ in breaches)
    assert gov.budgets["b"]._spent == 10.0  # 不变
    assert gov.quotas["q"]._counts.get("hik") == 1  # consume 未被调用


# ---- GovernanceAgent 总线网关 -------------------------------------------

@pytest.mark.asyncio
async def test_governance_agent_bus_gate_and_no_abort_when_disabled():
    bus = EventBus()
    gov = Governance(
        access=AccessControl(policy={"hik": {"door": "*"}}),
        quotas={"q": Quota(limit=2)},
        emit_abort=False,  # 网关纯回复模式（最佳实践）
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()

    aborts = []
    bus.subscribe("harness/abort", lambda t, m: aborts.append(m))

    r1 = await bus.request(
        "harness/govern/request",
        {"role": "hik", "capability": "door", "operation": "open"},
        timeout=1.0,
    )
    assert r1["allowed"] is True

    r2 = await bus.request(
        "harness/govern/request",
        {"role": "hik", "capability": "door", "operation": "open"},
        timeout=1.0,
    )
    assert r2["allowed"] is True  # 配额满 2 之前

    r3 = await bus.request(
        "harness/govern/request",
        {"role": "hik", "capability": "door", "operation": "open"},
        timeout=1.0,
    )
    assert r3["allowed"] is False  # 超出配额
    # emit_abort=False：不应发布 harness/abort
    assert aborts == []

    await agent.stop()


@pytest.mark.asyncio
async def test_governance_agent_emits_abort_when_enabled():
    bus = EventBus()
    gov = Governance(
        access=AccessControl(policy={"hik": {"door": ["open"]}}),  # 拒绝 reboot
        emit_abort=True,
        bus=bus,
    )
    agent = GovernanceAgent(bus, gov)
    aborts = []
    bus.subscribe("harness/abort", lambda t, m: aborts.append(m))
    await agent.start()

    reply = await bus.request(
        "harness/govern/request",
        {"role": "hik", "capability": "door", "operation": "reboot"},
        timeout=1.0,
    )
    assert reply["allowed"] is False
    assert len(aborts) == 1  # 仅硬违例（emit_abort=True）才 halt

    await agent.stop()


# ---- gate_allowed 异步闸门（fail-closed） -------------------------------

@pytest.mark.asyncio
async def test_gate_allowed_permits_when_agent_allows():
    bus = EventBus()
    gov = Governance(access=AccessControl(policy={"hik": {"door": "*"}}))
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    dummy = Agent(bus, AgentSpec(id="d", role="d"))
    allowed = await gate_allowed(
        dummy,
        {"role": "hik", "capability": "door", "operation": "open"},
        timeout=1.0,
    )
    assert allowed is True
    await agent.stop()


@pytest.mark.asyncio
async def test_gate_allowed_fail_closed_on_timeout():
    # 总线上没有任何 GovernanceAgent 响应 -> 超时 -> fail-closed 返回 False
    bus = EventBus()
    dummy = Agent(bus, AgentSpec(id="d", role="d"))
    allowed = await gate_allowed(dummy, {"role": "hik"}, timeout=0.1)
    assert allowed is False
