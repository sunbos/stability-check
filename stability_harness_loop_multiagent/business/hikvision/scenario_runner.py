"""run_scenario —— 把一份 Scenario 组装成完整的稳定性控制循环。

复用框架既有引擎（harness 运行时 / loop 确定性循环 / multi_agent 角色），
不新增跨引擎耦合：本模块属于 business（领域）层，负责把领域数据
（Scenario）+ 领域适配器（ScenarioISAPIAdapter）接线进 ControlLoop。

运行模式：传入真实 HikvisionClient（自动用 target 连接信息构造），或显式传入
TargetAdapter（测试可脚本化探测结果）。

返回一份结构化汇总（轮数、裁决分布、NA 计数、中止原因、遥测），便于上层
生成报告或接治理面板。
"""

import asyncio
from collections import Counter
from typing import Any, Dict, List, Optional

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from ...harness.telemetry import MemorySink, Telemetry
from ...loop.context import SharedContext
from ...loop.decision import DecisionAuthority
from ...loop.driver import ControlLoop, RunConfig
from ...loop.scheduler import Scheduler
from ...loop.termination import (
    CountStop,
    DurationStop,
    ExternalAbortStop,
    TerminationPolicy,
)
from ...multi_agent.observers.base import ObserverAgent
from ...multi_agent.observers.scribe import ScribeAgent
from ...multi_agent.adapter import TargetAdapter
from .client import HikvisionClient
from .scenario_adapter import ScenarioISAPIAdapter
from .scenario_schema import Scenario
from .scenario_worker import ScenarioWorker


class ScenarioLiveReporter(ObserverAgent):
    """实时逐轮打印观察者：订阅 ``loop/done`` 等事件，每轮裁决出来即打印一行。

    纯观察（与 ScribeAgent 同契约），绝不裁决或改动循环状态。打印全部
    ``flush=True``，因此即便 stdout 被块缓冲（非 TTY / 管道），也能实时流出，
    便于长时间真实设备回归时边跑边看。
    """

    def __init__(self, bus, spec, total_rounds: int, scenario_id: str) -> None:
        spec.subscriptions = list(spec.subscriptions or []) + [
            "loop/done", "agent/incident", "loop/abort", "harness/abort",
        ]
        super().__init__(bus, spec)
        self._total = total_rounds
        self._sid = scenario_id
        self._seen = 0

    def on_event(self, topic: str, message: Any) -> None:
        if topic == "loop/done":
            self._report_round(message)
        elif topic in ("agent/incident", "loop/abort", "harness/abort"):
            if topic == "agent/incident":
                sev = (message or {}).get("severity", "warn")
                extra = (message or {}).get("reason") or (message or {}).get("detail") or ""
                print(f"  [事件] severity={sev} {extra}", flush=True)
            else:
                reason = (message or {}).get("reason") if isinstance(message, dict) else message
                print(f"  [中止] {reason}", flush=True)

    def _report_round(self, msg: Any) -> None:
        self._seen += 1
        if not isinstance(msg, dict):
            return
        r = msg.get("round", self._seen)
        verdict = msg.get("verdict", "?")
        risk = msg.get("risk")
        facts = msg.get("facts", {}) or {}
        ok = facts.get("probe_ok")
        val = facts.get("probe_value")
        na = facts.get("probe_na")
        rt = msg.get("recover_time")
        if na:
            tag = "NA "
        elif ok:
            tag = "OK "
        else:
            tag = "FAIL"
        line = (f"  [轮 {r}/{self._total}] verdict={verdict} risk={risk} "
                f"probe={tag} value={val}")
        if rt is not None:
            try:
                line += f" recover={float(rt):.1f}s"
            except (TypeError, ValueError):
                pass
        print(line, flush=True)


def _recover_timeout_for(scenario: Scenario) -> float:
    st = scenario.stress
    if st.type in ("reboot", "upgrade") and st.reboot_after:
        # wait_online 含设备掉线 + HTTP 恢复 + 401 重置 DigestAuth 重试。
        # client.py _request 在 401 时重置 DigestAuth 实例并立即重试，新实例
        # 从空状态重新协商 challenge（~1 次额外往返），所以 wait_online 实际
        # 在设备 HTTP 服务恢复（~43s）后立即返回 True，无需等设备端 Digest Auth
        # 认证服务完全就绪（~420s+）。+60s buffer 覆盖 probe_interval + 边界抖动。
        return st.wait_online_timeout + 60.0
    return 10.0


async def run_scenario(
    scenario: Scenario,
    *,
    run_timeout: Optional[float] = None,
    client: Optional[HikvisionClient] = None,
    adapter: Optional[TargetAdapter] = None,
    bus: Optional[EventBus] = None,
    telemetry: Optional[Telemetry] = None,
    scribe: bool = True,
    live: bool = False,
) -> Dict[str, Any]:
    """端到端运行一份场景，返回汇总字典。

    ``adapter`` 显式传入时直接使用（测试可脚本化探测结果）；否则按 scenario.target
    自动构造 HikvisionClient + ScenarioISAPIAdapter（连接真实设备）。

    ``live=True`` 时挂载 ``ScenarioLiveReporter``，每轮裁决出来即打印一行（实时
    流式），便于长时间真实设备回归时边跑边看；库默认 ``False``（避免测试污染输出）。
    """
    bus = bus or EventBus()
    mem = MemorySink()
    tel = telemetry or Telemetry(bus=bus, sinks=[mem])
    ctx = SharedContext(
        baseline={"kind": "scenario", "id": scenario.id},
        strategy_text=scenario.name,
    )
    decision = DecisionAuthority()
    recover_timeout = _recover_timeout_for(scenario)

    cfg = RunConfig(
        max_rounds=scenario.loop.max_rounds,
        max_duration=scenario.loop.max_duration or 0.0,
        fail_threshold=scenario.loop.fail_threshold or 0,
        vote_timeout=0.1, vote_settle=0.05,
        recover_timeout=recover_timeout, check_timeout=recover_timeout,
        recheck_limit=0,
    )
    term_conds = [CountStop(scenario.loop.max_rounds), ExternalAbortStop(bus)]
    if scenario.loop.max_duration:
        term_conds.append(DurationStop(scenario.loop.max_duration))
    term = TerminationPolicy(term_conds)

    loop = ControlLoop(
        bus, ctx, decision, term,
        vote_timeout=cfg.vote_timeout, vote_settle=cfg.vote_settle,
        recover_timeout=cfg.recover_timeout, check_timeout=cfg.check_timeout,
        recheck_limit=cfg.recheck_limit,
        # k=0 关闭自适应冷却：轮间间隔由 scenario.loop.interval_seconds 固定给出。
        scheduler=Scheduler(base=scenario.loop.interval_seconds, k=0.0,
                             min_interval=0.0),
        telemetry=tel,
    )

    if adapter is not None:
        used_adapter = adapter
    else:
        if client is None:
            t = scenario.target
            client = HikvisionClient(host=t.host, port=t.port,
                                     username=t.username, password=t.password,
                                     http_timeout=t.http_timeout)
        used_adapter = ScenarioISAPIAdapter(client, scenario)

    worker = ScenarioWorker(
        bus, AgentSpec(id="sw1", role="scenario"), used_adapter, scenario,
        recover_timeout=recover_timeout,
    )
    agents: List[Any] = [worker]
    if scribe:
        agents.append(ScribeAgent(
            bus, AgentSpec(id="o1", role="scribe",
                           subscriptions=["loop/done", "agent/incident",
                                          "target/#"]),
        ))
    if live:
        agents.append(ScenarioLiveReporter(
            bus, AgentSpec(id="o2", role="live"),
            total_rounds=scenario.loop.max_rounds, scenario_id=scenario.id,
        ))

    for a in agents:
        await a.start()
    # 循环前执行 preconditions（DeviceOnline/SerialMode/BaselineRecord）
    if hasattr(worker, "pre_loop_setup"):
        if not worker.pre_loop_setup():
            reason = "pre_loop_setup failed"
            for a in agents:
                await a.stop()
            return {
                "summary": {
                    "scenario_id": scenario.id,
                    "scenario_name": scenario.name,
                    "rounds": 0,
                    "verdicts": {},
                    "pass": 0, "fail": 0, "na": 0, "stress_fail": 0,
                    "aborted": True,
                    "abort_reason": reason,
                    "stop_reason": reason,
                },
                "ctx": ctx, "loop": loop, "worker": worker,
                "telemetry": tel, "config": cfg,
            }
    await loop.start()
    try:
        await asyncio.wait_for(loop._task, run_timeout)
    finally:
        for a in agents:
            await a.stop()

    # ---- 汇总 ----------------------------------------------------------
    snap_ctx = ctx.snapshot()
    history = snap_ctx.round_history
    verdicts = Counter(r.verdict for r in history)
    chain = worker.get_chain_stats()
    summary = {
        "scenario_id": scenario.id,
        "scenario_name": scenario.name,
        "rounds": ctx.round_count,
        "verdicts": dict(verdicts),
        "pass": chain.get("pass", 0),
        "fail": chain.get("fail", 0),
        "na": chain.get("na", 0),
        "stress_fail": chain.get("stress_fail", 0),
        "aborted": snap_ctx.aborted,
        "abort_reason": snap_ctx.abort_reason or worker.stop_reason,
        "stop_reason": worker.stop_reason,
    }
    result: Dict[str, Any] = {
        "summary": summary,
        "ctx": ctx,
        "loop": loop,
        "worker": worker,
        "telemetry": tel,
        "config": cfg,
        "client": client,
    }
    return result


__all__ = ["run_scenario"]
