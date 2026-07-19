"""海康门禁稳定性测试的组装入口。

对应 examples/smoke.py 的接线方式：EventBus + Telemetry + SharedContext +
DecisionAuthority + RunConfig -> ControlLoop，外加 HikvisionWorker（订阅
hikvision/plan）+ HikvisionAdvisor + ScribeAgent + Watchdog。opt-in 治理时还会
挂载 GovernanceAgent 网关 + GovernancePanelAgent 治理观测面板。

LLM 自动探测：若设置了 LLM_API_KEY / OPENROUTER_API_KEY（或加载了 .env），
advisor/diagnostic 使用真实的 OpenRouter tencent/hy3:free；否则回退到确定性的
规则兜底可调用对象（测试即使用这些）。
"""

import asyncio
import json
from typing import Any, Callable, Dict

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from ...harness.governance import Governance, GovernanceAgent
from ...harness.telemetry import MemorySink, Telemetry
from ...harness.verify import VerificationAgent, Verifier
from ...harness.watchdog import Watchdog
from ...loop.context import SharedContext
from ...loop.decision import DecisionAuthority
from ...loop.driver import ControlLoop, RunConfig
from ...loop.scheduler import Scheduler
from ...multi_agent.observers.gov_panel import GovernancePanelAgent
from ...multi_agent.observers.scribe import ScribeAgent
from .adapter import HikvisionAdapter
from .advisor import HikvisionAdvisor
from .diagnostic import DiagnosticKernel, HEAL_TIME_SYNC, HEAL_RETRIGGER
from .llm import get_client
from .worker import HikvisionWorker


def _default_parse(instruction: str) -> Dict[str, Any]:
    """无 LLM 时可用的确定性兜底解析。"""
    return {"skip_reboot": False, "event_check_delay_adjust": 0,
            "trigger_interval_adjust": 0,
            "diagnose_whitelist": [HEAL_TIME_SYNC, HEAL_RETRIGGER]}


def _default_llm_decide(env: dict) -> str:
    """无 LLM 时可用的确定性兜底决策。"""
    if env.get("time_skew_seconds", 0) > 3.0:
        return HEAL_TIME_SYNC
    return HEAL_RETRIGGER


def _make_llm_parse() -> Callable[[str], Dict] | None:
    """若配置了 API 密钥，返回真实 LLM 解析可调用对象，否则返回 None。"""
    client = get_client()
    if client is None:
        return None

    system_prompt = (
        "你是一名海康门禁稳定性测试规划器。"
        "请将用户指令解析为 JSON。")

    def _parse(instruction: str) -> Dict[str, Any]:
        if not instruction:
            return _default_parse(instruction)
        result = client.chat_json(system_prompt, instruction)
        if not isinstance(result, dict):
            return _default_parse(instruction)
        return result
    return _parse


def _make_llm_decide() -> Callable[[dict], str] | None:
    """若配置了 API 密钥，返回真实 LLM 决策可调用对象，否则返回 None。"""
    client = get_client()
    if client is None:
        return None

    system_prompt = (
        "你是一名海康门禁稳定性自愈诊断器。"
        "根据环境事实，选择一种自愈子流程："
        "time_sync、wait_network、retrigger、abort。"
        '请返回 JSON {"decision": "<名称>"}。')

    def _decide(env: dict) -> str:
        result = client.chat_json(system_prompt, json.dumps(env))
        if not isinstance(result, dict):
            return HEAL_RETRIGGER
        decision = result.get("decision", HEAL_RETRIGGER)
        return decision
    return _decide


def _patch_worker_plan_handler(worker: HikvisionWorker) -> None:
    """覆写 worker.handle，将 hikvision/plan 缓存进 worker.state。

    基类 Agent._dispatch 在调用时查找 self.handle，因此在 start() 之前替换
    实例属性即足够。原始 handle 仍保留用于非 plan 话题（如 loop/tick）。
    """
    original_handle = worker.handle

    async def handle_with_plan(topic: str, message) -> None:
        if topic == "hikvision/plan":
            worker.state["plan"] = message or {}
            return
        await original_handle(topic, message)
    worker.handle = handle_with_plan  # type: ignore[assignment]


async def run_hikvision_stability(
    client,
    max_rounds: int = 5,
    run_timeout: float = 30.0,
    instruction: str = "",
    llm_parse: Callable[[str], Dict] | None = None,
    llm_decide: Callable[[dict], str] | None = None,
    *,
    run_reboot: bool = True,
    probe_interval: float = 5.0,
    probe_confirm_count: int = 2,
    warmup_time: float = 60.0,
    max_recover_timeout: float = 180.0,
    event_check_delay: float = 3.0,
    open_duration: float | None = None,
    device_writes: Dict[str, Any] | None = None,
    required_serial_mode: str | None = None,
    serial_port: int = 1,
    governance: "Governance | None" = None,
    verifier: "Verifier | None" = None,
    governance_timeout: float = 1.0,
) -> dict:
    """端到端运行一次海康门禁稳定性测试会话。

    若环境变量 / .env 提供了密钥则自动探测真实 LLM；显式参数优先。
    重启 / 探测 / 预热配置（spec §4.1、§4.2、§6 worker.*）转发给
    HikvisionWorker。``pre_loop_setup()`` 在 worker 启动之后（以便其能发布）
    但 Loop 启动之前调用，从而使基准重启耗时只测量一次并在每轮复用。返回包含
    ctx/loop/worker/advisor/telemetry/config 的字典。
    """
    if llm_parse is None:
        llm_parse = _make_llm_parse() or _default_parse
    if llm_decide is None:
        llm_decide = _make_llm_decide() or _default_llm_decide

    bus = EventBus()
    mem = MemorySink()
    tel = Telemetry(bus=bus, sinks=[mem])
    ctx = SharedContext(baseline={"kind": "hikvision"}, strategy_text=instruction)
    decision = DecisionAuthority()
    # ControlLoop 每轮在收齐 target/recovered + target/checked 后立即返回
    # （事件驱动，见 driver.py §round），超时仅作兜底。当 run_reboot=True 时，
    # 重启+探测+预热可能耗时 max_recover_timeout + warmup_time，需额外预留
    # 探测 + 开门 + 查事件的缓冲。当 run_reboot=False 时，do_work 即
    # remote_open_door + 等待门关闭 + 事件查询；查询延迟须 ≥ openDuration+余量+1，
    # 因此超时按生效的开启保持时间计算（多留 5s 用于事件查询本身）。
    if run_reboot:
        round_act_timeout = max_recover_timeout + warmup_time + 30.0
    else:
        if device_writes and "openDuration" in device_writes:
            effective_open = float(device_writes["openDuration"])
        else:
            effective_open = open_duration if open_duration else 2.0
        # 轮询等待门关闭的最坏情况为 openDuration*3+5（门不关闭时），
        # 加上查询本身开销，故超时须覆盖该上限。
        round_act_timeout = max(event_check_delay, effective_open * 3.0 + 5.0) + 3.0
    cfg = RunConfig(max_rounds=max_rounds, max_duration=0.0, fail_threshold=10_000,
                    vote_timeout=0.1, vote_settle=0.05, recover_timeout=round_act_timeout,
                    check_timeout=round_act_timeout, recheck_limit=0)
    term = cfg.build_termination()
    loop = ControlLoop(
        bus, ctx, decision, term,
        vote_timeout=cfg.vote_timeout,
        vote_settle=cfg.vote_settle,
        recover_timeout=cfg.recover_timeout,
        check_timeout=cfg.check_timeout,
        recheck_limit=cfg.recheck_limit,
        # k=0.0 关闭自适应冷却：每轮的 round_act_timeout 已经等待了完整的
        # 「重启+探测+预热+开门」周期。若不关闭，恢复时间（约 180s）乘以
        # k（1.5）= 每轮额外睡眠 270s，5 轮重启测试将耗时 40 分钟以上。
        scheduler=Scheduler(base=0.0, k=0.0, min_interval=0.0),
        telemetry=tel,
    )

    adapter = HikvisionAdapter(client)
    diagnostic = DiagnosticKernel(
        llm_decide=llm_decide,
        whitelist=[HEAL_TIME_SYNC, HEAL_RETRIGGER],
    )
    worker = HikvisionWorker(
        bus,
        AgentSpec(id="w1", role="hik",
                  subscriptions=["hikvision/plan"]),
        adapter, client, time_skew_threshold=3.0,
        diagnostic=diagnostic,
        run_reboot=run_reboot,
        probe_interval=probe_interval,
        probe_confirm_count=probe_confirm_count,
        warmup_time=warmup_time,
        max_recover_timeout=max_recover_timeout,
        event_check_delay=event_check_delay,
        open_duration=open_duration,
        device_writes=device_writes,
        required_serial_mode=required_serial_mode,
        serial_port=serial_port,
        governance=governance,
        enable_governance=(governance is not None),
        governance_timeout=governance_timeout,
    )
    # 在 start() 之前打补丁，使 _dispatch 能取到新属性
    _patch_worker_plan_handler(worker)

    advisor = HikvisionAdvisor(
        bus, AgentSpec(id="a1", role="risk"),
        instruction=instruction,
        llm_parse=llm_parse,
        enable_verify=(verifier is not None),
    )
    scribe = ScribeAgent(
        bus, AgentSpec(id="o1", role="scribe",
                       subscriptions=["loop/done", "agent/incident", "target/#"]),
    )
    # 看门狗的 stall_timeout 必须超过 max_recover_timeout + warmup_time
    #（重启阶段会在工作线程中阻塞 do_work 这么久）。
    stall_timeout = max(300.0, max_recover_timeout + warmup_time + 60.0)
    dog = Watchdog(bus, stall_timeout=stall_timeout, check_interval=0.05)

    # opt-in 治理/校验网关：仅当调用方显式传入才挂载（与 Watchdog 同模式）。
    # 不传则 worker 闸门自动放行，既有行为完全不变。
    extra_agents = []
    gov_panel = None
    if governance is not None:
        # 把运行器的 Telemetry（同一总线）接到治理实例，使治理决策事实
        # （governance.decision）真正发到总线上，供治理观测面板消费。
        governance.telemetry = tel
        gov_agent = GovernanceAgent(bus, governance)  # 网关纯回复（emit_abort 由 governance 决定）
        extra_agents.append(gov_agent)
        # 治理观测面板（Observer）：订阅 harness/fact/governance.decision，
        # 聚合为可读 dashboard。opt-in，与治理网关同生命周期。
        gov_panel = GovernancePanelAgent(
            bus, AgentSpec(id="o2", role="gov-panel",
                           subscriptions=["harness/fact/governance.decision",
                                          "governance/panel/request"]),
        )
        extra_agents.append(gov_panel)
    if verifier is not None:
        extra_agents.append(VerificationAgent(bus, verifier))

    for a in (worker, advisor, scribe, dog, *extra_agents):
        await a.start()
    # Loop 前准备：记录基线 + 基准重启 + 测量耗时。
    # 在 worker.start() 之后（以便其能发布）但 loop.start() 之前执行，
    # 这样测量到的 baseline_reboot_duration 才能供 do_work() 使用。
    # 放在线程中运行，因为 pre_loop_setup 在基准重启探测时可能阻塞 60-180s；
    # 这绝不能阻塞事件循环。
    await asyncio.to_thread(worker.pre_loop_setup)
    # Advisor 在 start() 期间发布 hikvision/plan；worker 会将其缓存。
    # 在 advisor 之后启动循环，使计划已入队。
    await loop.start()
    try:
        await asyncio.wait_for(loop._task, run_timeout)
    finally:
        for a in (worker, advisor, scribe, dog, *extra_agents):
            await a.stop()
    result = {"ctx": ctx, "loop": loop, "worker": worker,
              "advisor": advisor, "telemetry": tel, "config": cfg}
    if governance is not None:
        result["gov_agent"] = gov_agent
        result["gov_panel"] = gov_panel
    if verifier is not None:
        result["verify_agent"] = extra_agents[-1]
    return result


__all__ = ["run_hikvision_stability"]
