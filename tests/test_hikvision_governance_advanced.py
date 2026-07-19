"""Hikvision worker 熔断器 + 按操作鉴权测试（P1-c / P1-d）。

P1-c：破坏性外部操作（开门/重启）经 _guarded_adapter_act 受 CircuitBreaker 保护；
      连续失败达阈值后熔断器打开，后续调用被跳过（不再打设备）。
P1-d：治理按 operations 返回 denied_ops；worker 跳过被拒操作（如 reboot）但
      仍执行其余操作（如 remote_open_door）。
"""

import asyncio

import pytest

from stability_harness_loop_multiagent.business.hikvision.adapter import (
    HikvisionAdapter,
)
from stability_harness_loop_multiagent.business.hikvision.worker import (
    HikvisionWorker,
)
from stability_harness_loop_multiagent.core.agent import AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.governance import (
    AccessControl,
    CircuitBreaker,
    DeniedOp,
    Governance,
    GovernanceAgent,
)
from stability_harness_loop_multiagent.harness.telemetry import (
    MemorySink,
    Telemetry,
)
from stability_harness_loop_multiagent.multi_agent.observers.gov_panel import (
    GovernancePanelAgent,
)
from stability_harness_loop_multiagent.multi_agent.adapter import Result
from tests.fakes.fake_hikvision import FakeHikvisionClient


class _FailAdapter:
    """可控制前 N 次调用失败的假适配器。"""

    def __init__(self, fail_n: int) -> None:
        self.fail_n = fail_n
        self.calls = 0

    def act(self, op):
        self.calls += 1
        if self.calls <= self.fail_n:
            return Result(ok=False, error="boom")
        return Result(ok=True)


def _worker(gov=None):
    bus = EventBus()
    client = FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    w = HikvisionWorker(
        bus, AgentSpec(id="w1", role="hik", capabilities={"act"}),
        adapter, client, governance=gov,
    )
    return w


# ---- P1-c：熔断器 ----

def test_breaker_trips_and_skips_adapter():
    gov = Governance(breakers={"hikvision-api": CircuitBreaker(
        failure_threshold=2, cooldown=1000)})
    w = _worker(gov)
    w.adapter = _FailAdapter(fail_n=5)
    r1 = w._guarded_adapter_act({"op": "reboot"})
    r2 = w._guarded_adapter_act({"op": "reboot"})
    assert not r1.ok and not r2.ok
    assert gov.breakers["hikvision-api"].state == "open"
    # 熔断器打开后，调用被跳过，适配器不再被调用
    calls_before = w.adapter.calls
    r3 = w._guarded_adapter_act({"op": "reboot"})
    assert not r3.ok and r3.error == "circuit breaker open"
    assert w.adapter.calls == calls_before


def test_breaker_records_success_keeps_closed():
    gov = Governance(breakers={"hikvision-api": CircuitBreaker(
        failure_threshold=2, cooldown=1000)})
    w = _worker(gov)
    w.adapter = _FailAdapter(fail_n=0)
    r = w._guarded_adapter_act({"op": "reboot"})
    assert r.ok
    assert gov.breakers["hikvision-api"].state == "closed"


def test_no_governance_adapter_act_passthrough():
    w = _worker(None)
    w.adapter = _FailAdapter(fail_n=99)
    r = w._guarded_adapter_act({"op": "reboot"})
    assert not r.ok
    assert w.adapter.calls == 1  # 无熔断器时直接调用，不拦截


# ---- P1-d：按操作鉴权 ----

@pytest.mark.asyncio
async def test_denied_ops_skips_reboot_but_runs_open():
    bus = EventBus()
    client = FakeHikvisionClient()
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={"reboot"},
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    adapter = HikvisionAdapter(client)
    w = HikvisionWorker(
        bus, AgentSpec(id="w1", role="hik", capabilities={"act"}),
        adapter, client, run_reboot=True,
        probe_interval=0.01, probe_confirm_count=2,
        warmup_time=0.0, max_recover_timeout=1.0, event_check_delay=0.0,
        enable_governance=True,
    )
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await w.act(tick)
    stages = [e["stage"] for e in w._timeline]
    assert "op_denied" in stages, "reboot 应被按操作拒绝"
    assert "reboot_start" not in stages, "被拒的 reboot 不应执行"
    assert client._reboot_called is False
    assert w.get_chain_stats()["rounds"] == 1, "开门操作仍应执行"
    await agent.stop()


# ---- P1-d（增强）：按 role / capability 维度声明 DeniedOp ----

def _worker_on(bus, gov, **kwargs):
    client = FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    w = HikvisionWorker(
        bus, AgentSpec(id="w1", role=kwargs.pop("role", "hik"),
                       capabilities={"act"}),
        adapter, client, run_reboot=True,
        probe_interval=0.01, probe_confirm_count=2,
        warmup_time=0.0, max_recover_timeout=1.0, event_check_delay=0.0,
        governance=gov, enable_governance=True,
        **kwargs,
    )
    return w


@pytest.mark.asyncio
async def test_denied_op_by_role_does_not_deny_other_role():
    bus = EventBus()
    # 仅禁止 role="risk" 的 reboot；hik worker 不受影响。
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="reboot", role="risk")},
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    w = _worker_on(bus, gov)
    decision = await w._governance_decision({"round": 1})
    assert decision["allowed"] is True
    assert "reboot" not in decision["denied_ops"], "hik 角色的 reboot 不应被按角色规则拒绝"
    await agent.stop()


@pytest.mark.asyncio
async def test_denied_op_by_role_denies_matching_role():
    bus = EventBus()
    gov = Governance(
        access=AccessControl(policy={"risk": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="reboot", role="risk")},
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    w = _worker_on(bus, gov, role="risk")
    decision = await w._governance_decision({"round": 1})
    assert "reboot" in decision["denied_ops"], "risk 角色的 reboot 应被按角色规则拒绝"
    await agent.stop()


@pytest.mark.asyncio
async def test_denied_op_by_capability():
    bus = EventBus()
    # 仅禁止 capability="door-test" 的 reboot；与 role 无关。
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="reboot", capability="door-test")},
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    w = _worker_on(bus, gov)
    decision = await w._governance_decision({"round": 1})
    assert "reboot" in decision["denied_ops"], "capability=door-test 的 reboot 应被拒"
    await agent.stop()


@pytest.mark.asyncio
async def test_denied_op_global_string_still_works():
    bus = EventBus()
    # 向后兼容：字符串配置项应等价于 DeniedOp(op=...)。
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={"reboot"},
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    w = _worker_on(bus, gov)
    decision = await w._governance_decision({"round": 1})
    assert "reboot" in decision["denied_ops"], "全局字符串拒绝应仍生效"
    await agent.stop()


# ---- 治理结构化事实上报（governance.decision fact） ----

@pytest.mark.asyncio
async def test_governance_decision_emits_fact():
    bus = EventBus()
    sink = MemorySink()
    tel = Telemetry(sinks=[sink])
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="reboot", role="hik")},
        telemetry=tel,
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    w = _worker_on(bus, gov)
    await w._governance_decision({"round": 7})
    facts = sink.get(kind="fact", name="governance.decision")
    assert facts, "治理决策应发结构化事实"
    f = facts[-1]
    assert f["allowed"] is True
    assert "reboot" in f["denied_ops"], "被拒操作应记录在事实里"
    assert f["round"] == 7 and f["role"] == "hik"
    await agent.stop()


@pytest.mark.asyncio
async def test_governance_decision_timeout_emits_fact():
    bus = EventBus()
    sink = MemorySink()
    tel = Telemetry(sinks=[sink])
    # 治理实例带 telemetry，但总线未挂载 GovernanceAgent -> 请求超时（fail-closed）。
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        telemetry=tel,
    )
    w = _worker_on(bus, gov, governance_timeout=0.05)
    await w._governance_decision({"round": 3})
    facts = sink.get(kind="fact", name="governance.decision")
    assert facts, "超时路径应发结构化事实"
    f = facts[-1]
    assert f["allowed"] is False
    assert "fail-closed" in f["reason"], "应标明为超时/fail-closed"
    assert f["round"] == 3


# ---- DeniedOp 通配 role / capability = "*" ----

@pytest.mark.asyncio
async def test_denied_op_wildcard_role_denies_any_role():
    bus = EventBus()
    # role="*" 等价于全局禁止 reboot（无论 worker 角色）。
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="reboot", role="*")},
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    w = _worker_on(bus, gov)
    decision = await w._governance_decision({"round": 1})
    assert "reboot" in decision["denied_ops"], "role='*' 应拒绝任意角色的 reboot"
    await agent.stop()


@pytest.mark.asyncio
async def test_denied_op_wildcard_capability_denies_any_capability():
    bus = EventBus()
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="reboot", capability="*")},
    )
    agent = GovernanceAgent(bus, gov)
    await agent.start()
    w = _worker_on(bus, gov)
    decision = await w._governance_decision({"round": 1})
    assert "reboot" in decision["denied_ops"], "capability='*' 应拒绝任意能力的 reboot"
    await agent.stop()


# ---- 治理观测面板 Observer（消费 governance.decision 事实） ----

@pytest.mark.asyncio
async def test_gov_panel_consumes_facts_and_aggregates():
    bus = EventBus()
    panel = GovernancePanelAgent(
        bus, AgentSpec(id="o2", role="gov-panel",
                       subscriptions=["harness/fact/governance.decision"]),
    )
    await panel.start()
    # 确定性投递：publish_and_wait 等所有处理器完成后再返回。
    await bus.publish_and_wait("harness/fact/governance.decision", {
        "allowed": True, "reason": "ok", "denied_ops": [],
        "role": "hik", "capability": "door-test", "operation": "round", "round": 1,
    })
    await bus.publish_and_wait("harness/fact/governance.decision", {
        "allowed": False, "reason": "denied-op", "denied_ops": ["reboot"],
        "role": "risk", "capability": "door-test", "operation": "round", "round": 2,
    })
    await bus.publish_and_wait("harness/fact/governance.decision", {
        "allowed": False, "reason": "timeout/fail-closed", "denied_ops": [],
        "role": "hik", "capability": "door-test", "operation": "round", "round": 3,
    })
    p = panel.panel()
    assert p["total"] == 3
    assert p["allowed"] == 1 and p["denied"] == 2
    assert p["fail_closed"] == 1
    assert p["by_role"] == {"hik": 2, "risk": 1}
    assert p["denied_ops_by_op"] == {"reboot": 1}
    assert p["rounds_covered"] == 3
    text = panel.render()
    assert "治理观测面板" in text and "reboot" in text
    await panel.stop()


@pytest.mark.asyncio
async def test_gov_panel_receives_from_gateway_via_bus():
    bus = EventBus()
    tel = Telemetry(bus=bus, sinks=[MemorySink()])  # 与总线相连 -> 事实发到总线
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="reboot", role="hik")},
        telemetry=tel,
    )
    gate = GovernanceAgent(bus, gov)
    await gate.start()
    panel = GovernancePanelAgent(
        bus, AgentSpec(id="o2", role="gov-panel",
                       subscriptions=["harness/fact/governance.decision"]),
    )
    await panel.start()
    w = _worker_on(bus, gov)
    await w._governance_decision({"round": 9})
    # 让总线已调度的面板任务跑完（publish 经 create_task 异步派发）。
    await asyncio.sleep(0.02)
    p = panel.panel()
    assert p["total"] == 1, "面板应经总线收到网关发出的治理决策事实"
    assert p["rounds_observed"] == [9]
    assert p["allowed"] == 1
    assert p["denied_ops_by_op"] == {"reboot": 1}
    await gate.stop()
    await panel.stop()


# ---- DeniedOp 操作名匹配维度：prefix / regex ----

def test_denied_op_prefix_match():
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="diag_", match="prefix")},
    )
    assert gov.matches_denied_op("diag_health", "hik", "door-test") is True
    assert gov.matches_denied_op("diag_net", "hik", "door-test") is True
    assert gov.matches_denied_op("reboot", "hik", "door-test") is False
    # 通配角色 + 前缀组合
    gov2 = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="diag_", match="prefix", role="*")},
    )
    assert gov2.matches_denied_op("diag_x", "risk", "door-test") is True


def test_denied_op_regex_match():
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op=r"force_.*_now", match="regex")},
    )
    assert gov.matches_denied_op("force_reboot_now", "hik", "door-test") is True
    # 正则全匹配：结尾不符则不命中
    assert gov.matches_denied_op("force_reboot_later", "hik", "door-test") is False
    # 非法正则按"未命中"处理，绝不抛出
    gov2 = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="(bad", match="regex")},
    )
    assert gov2.matches_denied_op("anything", "hik", "door-test") is False


def test_denied_op_suffix_match():
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="_now", match="suffix")},
    )
    assert gov.matches_denied_op("force_reboot_now", "hik", "door-test") is True
    assert gov.matches_denied_op("diag_now", "hik", "door-test") is True
    assert gov.matches_denied_op("force_reboot_later", "hik", "door-test") is False
    # 后缀 + 通配角色组合
    gov2 = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="_now", match="suffix", role="*")},
    )
    assert gov2.matches_denied_op("x_now", "risk", "door-test") is True


def test_denied_op_contains_match():
    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="temp", match="contains")},
    )
    assert gov.matches_denied_op("read_temp", "hik", "door-test") is True
    assert gov.matches_denied_op("temp_flush", "hik", "door-test") is True
    assert gov.matches_denied_op("reboot", "hik", "door-test") is False
    # 子串 + 通配能力组合
    gov2 = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"}}),
        denied_operations={DeniedOp(op="temp", match="contains", capability="*")},
    )
    assert gov2.matches_denied_op("flush_temp", "hik", "cam-test") is True


# ---- 治理面板真实拉取（governance/panel/request -> governance/panel） ----

@pytest.mark.asyncio
async def test_gov_panel_pull_via_request_reply():
    bus = EventBus()
    panel = GovernancePanelAgent(
        bus, AgentSpec(id="o2", role="gov-panel",
                       subscriptions=["harness/fact/governance.decision",
                                      "governance/panel/request"]),
    )
    await panel.start()
    # 先喂入若干治理决策事实
    await bus.publish_and_wait("harness/fact/governance.decision", {
        "allowed": True, "reason": "ok", "denied_ops": ["reboot"],
        "role": "hik", "capability": "door-test", "operation": "round", "round": 1,
    })
    # 真实拉取：订阅回复主题并发布请求，面板回发 governance/panel
    pulled = []
    bus.subscribe("governance/panel", lambda _t, m: pulled.append(m))
    await bus.publish_and_wait("governance/panel/request", {"req_id": "q1"})
    await asyncio.sleep(0.02)  # 让面板回发的 governance/panel 派发到订阅者
    assert pulled, "面板应经总线回发 governance/panel"
    assert pulled[-1].get("req_id") == "q1", "回复应回带 req_id"
    p = pulled[-1]["panel"]
    assert p["total"] == 1
    assert p["denied_ops_by_op"] == {"reboot": 1}
    # 面板回复应同时携带经总线拉取的时间序列（供趋势图/报告消费）。
    ts = pulled[-1].get("timeseries")
    assert ts is not None, "面板回复应携带 timeseries"
    assert ts["step"] == [1]
    assert ts["cum_allowed"] == [1]
    assert ts["cum_denied"] == [0]
    await panel.stop()
