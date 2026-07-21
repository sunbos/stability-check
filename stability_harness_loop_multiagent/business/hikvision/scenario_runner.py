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
import time
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

# rich 为可选依赖（pyproject.toml [examples] extras），缺失时回退标准库 print。
# spec §6.6 要求用 rich.console + rich.table 替代手写 print 拼接，实现：
#   - 引擎/角色完整名称（Loop/MAS/Harness，Worker/Scribe/Advisor 等，不简写）
#   - verdict 颜色（pass=绿/fail=红/warn=黄/na=灰）
#   - 汇总表格 + ASCII bar 裁决分布
try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    _HAS_RICH = True
except ImportError:  # pragma: no cover - 可选依赖
    _HAS_RICH = False


class ScenarioLiveReporter(ObserverAgent):
    """实时逐轮打印观察者：订阅 ``loop/done`` 等事件，每轮裁决出来即打印一行。

    纯观察（与 ScribeAgent 同契约），绝不裁决或改动循环状态。打印全部
    ``flush=True``，因此即便 stdout 被块缓冲（非 TTY / 管道），也能实时流出，
    便于长时间真实设备回归时边跑边看。

    额外订阅 ``target/acted`` 以缓存最近一轮 stress 操作的耗时细分（如
    ``reboot_put`` / ``wait_online``），与 ``loop/done`` 的 ``recover_time``
    一起展示，便于排查"设备掉线多久、wait_online 多久"。

    ``verbose=True`` 时订阅总线全部话题（"#"通配），按时间顺序实时打印每个
    引擎的动作 trace，让三引擎（Loop/MAS/Harness）的协作过程显式可见。

    引擎/角色均用完整名称（不简写），让读者无需查图例即可理解：

        [t=  0.1s] Loop    Loop        loop/tick       轮 1 开始
        [t= 64.2s] MAS     Worker      target/acted    执行 stress (reboot_put=0.2s, wait_online=63.8s)
        [t= 64.2s] MAS     Worker      target/recovered 设备恢复 recovered=True
        [t= 64.2s] MAS     Worker      target/checked  facts=['probe_ok', 'probe_value']
        [t= 64.2s] Loop    Loop        loop/done       裁决 pass risk=50.0(none) recover=64.2s

    rich 可用时自动启用颜色（verdict pass=绿/fail=红/warn=黄/na=灰）与表格汇总；
    缺失时回退标准库 print（零依赖安全网）。
    """

    # 引擎完整名称（不简写，与 EngineBusTracer 归类一致）
    # 颜色映射仅在 rich 可用时生效
    _ENGINE_COLOR = {
        "Loop": "cyan",
        "MAS": "magenta",
        "Harness": "green",
        "Other": "white",
    }

    # verdict -> 颜色（rich 样式）
    _VERDICT_COLOR = {
        "pass": "green",
        "fail": "red",
        "warn": "yellow",
        "na": "dim",
        "recheck": "yellow",
    }

    # 角色 -> 完整名称（不简写）
    _ROLE_NAME = {
        "scenario": "Worker",
        "scribe": "Scribe",
        "live": "Reporter",
        "advisor": "Advisor",
        "verifier": "Verifier",
        "governance": "Governance",
    }

    # verbose 模式下跳过的高频/低价值话题（避免刷屏）
    _SKIP_TOPICS = {"harness/liveness/heartbeat"}

    def __init__(self, bus, spec, total_rounds: int, scenario_id: str,
                 *, verbose: bool = False) -> None:
        if verbose:
            # verbose 模式：订阅全部话题，实时打印 trace
            spec.subscriptions = list(spec.subscriptions or []) + ["#"]
        else:
            spec.subscriptions = list(spec.subscriptions or []) + [
                "loop/done", "target/acted",
                "agent/incident", "loop/abort", "harness/abort",
            ]
        super().__init__(bus, spec)
        self._total = total_rounds
        self._sid = scenario_id
        self._seen = 0
        self._verbose = verbose
        self._t0 = time.monotonic()
        # rich Console 单例（无 rich 时为 None，回退 print）
        self._console = Console() if _HAS_RICH else None
        # round_no -> {"timing": {...}, "stress_ok": bool, "error": str?}
        # 缓存 target/acted 的结果，在 loop/done 时合并展示
        self._acted: dict[int, dict] = {}

    def on_event(self, topic: str, message: Any) -> None:
        if self._verbose:
            self._on_event_verbose(topic, message)
            return
        # 简洁模式：只打印 loop/done + 关键事件
        if topic == "loop/done":
            self._report_round(message)
        elif topic == "target/acted":
            self._on_acted(message)
        elif topic in ("agent/incident", "loop/abort", "harness/abort"):
            if topic == "agent/incident":
                sev = (message or {}).get("severity", "warn")
                extra = (message or {}).get("reason") or (message or {}).get("detail") or ""
                self._emit(f"  [事件] severity={sev} {extra}")
            else:
                reason = (message or {}).get("reason") if isinstance(message, dict) else message
                self._emit(f"  [中止] {reason}")

    # ---- 输出层：rich / print 统一出口 --------------------------------
    def _emit(self, text: str, *, style: str | None = None) -> None:
        """统一输出：rich 可用时带样式打印，否则回退 print。"""
        if self._console:
            if style:
                self._console.print(text, style=style)
            else:
                self._console.print(text)
        else:
            print(text, flush=True)

    def _emit_round_line(self, r: int, msg: dict) -> None:
        """打印 loop/done 的汇总行（带 verdict 颜色）。"""
        line, verdict = self._build_round_line(r, msg)
        if self._console:
            # verdict 颜色单独染色（rich Text 不能在 plain str 上做局部样式，
            # 所以整行用 verdict 颜色；简洁模式下整行染色可读性更好）
            color = self._VERDICT_COLOR.get(verdict, "white")
            self._console.print(line, style=color)
        else:
            print(line, flush=True)

    # ---- verbose 模式：实时打印三引擎 trace ---------------------------
    def _on_event_verbose(self, topic: str, message: Any) -> None:
        """verbose 模式：按时间顺序实时打印每条事件的引擎/角色/摘要。"""
        # 缓存 target/acted 的 timing（loop/done 时合并展示）
        if topic == "target/acted":
            self._on_acted(message)
        # loop/done 用汇总行（含 timing）替代 trace 行
        if topic == "loop/done":
            self._report_round_verbose(message)
            return
        # 跳过高频/低价值话题
        if topic in self._SKIP_TOPICS:
            return
        if topic.startswith("harness/metric/"):
            return
        if topic == "agent/incident/ack":
            return
        engine = self._engine_of(topic)
        role = self._role_of(topic, message)
        summary = self._summarize(topic, message)
        t = time.monotonic() - self._t0
        if self._console:
            # rich 模式：引擎用颜色，角色加粗，时间/话题 dim
            # 注意：[t=xxx] 会被 rich 误解析为样式标签，改用 t=xxx 不带方括号
            eng_color = self._ENGINE_COLOR.get(engine, "white")
            self._console.print(
                f"  [dim]t={t:6.1f}s[/dim] "
                f"[{eng_color}]{engine:<7}[/{eng_color}] "
                f"[bold]{role:<10}[/bold] "
                f"[dim]{topic:<28}[/dim] "
                f"{summary}"
            )
        else:
            # 回退 print：保持列对齐
            print(f"  t={t:6.1f}s {engine:<7} {role:<10} {topic:<28} {summary}",
                  flush=True)

    @staticmethod
    def _engine_of(topic: str) -> str:
        """根据话题前缀判定事件所属引擎（与 EngineBusTracer 一致）。"""
        for prefix, engine in (
            ("loop/", "Loop"),
            ("agent/", "MAS"),
            ("hikvision/", "MAS"),
            ("target/", "MAS"),
            ("harness/", "Harness"),
        ):
            if topic.startswith(prefix):
                return engine
        return "Other"

    @classmethod
    def _role_of(cls, topic: str, msg: Any) -> str:
        """从话题或消息中提取角色完整名称（Worker/Scribe/Advisor 等）。"""
        if not isinstance(msg, dict):
            msg = {}
        role = msg.get("role")
        if role:
            return cls._ROLE_NAME.get(role, role.capitalize() if role else "")
        # agent/<role>/done
        if topic.startswith("agent/"):
            parts = topic.split("/")
            if len(parts) >= 2:
                r = parts[1]
                return cls._ROLE_NAME.get(r, r.capitalize())
        # target/* 都是 worker
        if topic.startswith("target/"):
            return "Worker"
        # loop/* 是 Loop driver
        if topic.startswith("loop/"):
            return "Loop"
        # harness/* 看子话题
        if topic.startswith("harness/verify"):
            return "Verifier"
        if topic.startswith("harness/govern"):
            return "Governance"
        if topic.startswith("harness/liveness"):
            return "Watchdog"
        if topic.startswith("harness/"):
            return "Harness"
        return ""

    def _summarize(self, topic: str, msg: Any) -> str:
        """把一条总线事件转成人类可读摘要（精简版，复用 EngineBusTracer 思路）。"""
        if not isinstance(msg, dict):
            msg = {}
        if topic == "loop/tick":
            return f"轮 {msg.get('round')} 开始"
        if topic == "loop/vote/request":
            return "请求 Advisor 投票"
        if topic == "agent/vote/reply":
            return (f"投票 risk={msg.get('risk')} conf={msg.get('confidence')}")
        if topic == "agent/incident":
            return f"事件 sev={msg.get('severity')} {msg.get('reason') or msg.get('detail') or ''}"
        if topic == "loop/abort":
            return f"循环中止 {msg.get('reason')}"
        if topic == "harness/abort":
            return f"看门狗中止 {msg.get('reason')}"
        if topic == "loop/recheck":
            return "recheck 发布"
        if topic == "agent/hik/done" or (
                topic.startswith("agent/") and topic.endswith("/done")):
            return f"Worker 完成 (round={msg.get('round')})"
        if topic == "hikvision/plan":
            return "Advisor 发布计划"
        if topic == "target/acted":
            result = msg.get("result") or {}
            data = result.get("data") or {}
            timing = data.get("timing") or {}
            if timing:
                return (f"执行 stress "
                        f"(reboot_put={timing.get('reboot_put')}, "
                        f"wait_online={timing.get('wait_online')})")
            err = result.get("error")
            if err:
                return f"stress 失败: {err[:60]}"
            return "执行 stress"
        if topic == "target/recovered":
            return f"设备恢复 recovered={msg.get('recovered')}"
        if topic == "target/checked":
            facts = msg.get("facts") or {}
            keys = [k for k in facts.keys() if not k.startswith("recovered")]
            return f"facts={keys}"
        if topic == "harness/verify/request":
            return "校验请求 (LLM 护栏)"
        if topic == "harness/verify/response":
            return f"校验响应 allowed={msg.get('allowed')}"
        if topic == "harness/govern/request":
            return "治理请求"
        if topic.startswith("harness/fact/"):
            name = topic.split("/", 2)[-1]
            return f"事实 {name}"
        if topic.startswith("harness/liveness/"):
            return f"看门狗 {msg.get('reason') or topic}"
        return topic

    def _on_acted(self, msg: Any) -> None:
        """缓存 target/acted 的 timing / stress_ok / error，等 loop/done 时合并展示。"""
        if not isinstance(msg, dict):
            return
        r = msg.get("round")
        if r is None:
            return
        result = msg.get("result") or {}
        self._acted[r] = {
            "timing": (result.get("data") or {}).get("timing") or {},
            "stress_ok": result.get("stress_ok", False),
            "error": result.get("error"),
        }

    def _report_round_verbose(self, msg: Any) -> None:
        """verbose 模式的 loop/done 汇总行。"""
        self._seen += 1
        if not isinstance(msg, dict):
            return
        r = msg.get("round", self._seen)
        self._emit_round_line(r, msg)
        if r < self._total:
            self._emit("  " + "-" * 78, style="dim")

    def _report_round(self, msg: Any) -> None:
        """简洁模式的 loop/done 汇总行。"""
        self._seen += 1
        if not isinstance(msg, dict):
            return
        r = msg.get("round", self._seen)
        self._emit_round_line(r, msg)
        if r < self._total:
            self._emit("  " + "-" * 60, style="dim")

    def _build_round_line(self, r: int, msg: dict) -> tuple[str, str]:
        """构造 loop/done 的汇总行（简洁/verbose 共用）。

        Returns:
            (line, verdict): line 是要打印的字符串，verdict 用于染色。
        """
        verdict = msg.get("verdict", "?")
        risk = msg.get("risk")
        facts = msg.get("facts", {}) or {}
        ok = facts.get("probe_ok")
        val = facts.get("probe_value")
        na = facts.get("probe_na")
        rt = msg.get("recover_time")
        # risk 来源标注：无 Advisor 投票时 risk=50 是 NEUTRAL_RISK 默认值
        risk_tag = f"{risk}"
        if risk is not None:
            try:
                if abs(float(risk) - 50.0) < 1e-6:
                    risk_tag = f"{risk}(none)"
            except (TypeError, ValueError):
                pass
        if na:
            tag = "NA "
        elif ok:
            tag = "OK "
        else:
            tag = "FAIL"
        prefix = (f"  [轮 {r}/{self._total}] verdict={verdict} risk={risk_tag} "
                  f"probe={tag} value={val}")
        # 耗时细分：优先用 target/acted 缓存的 timing（reboot_put / wait_online），
        # 再附 loop/done 的 recover_time（含 wait_online 之后的 check 耗时）
        acted = self._acted.get(r, {})
        timing = acted.get("timing") or {}
        parts: list[str] = []
        if timing:
            for k in ("reboot_put", "wait_online"):
                if k in timing:
                    parts.append(f"{k}={float(timing[k]):.1f}s")
        if rt is not None:
            try:
                parts.append(f"recover={float(rt):.1f}s")
            except (TypeError, ValueError):
                pass
        if parts:
            prefix += "  " + " ".join(parts)
        if not acted.get("stress_ok", True) and acted.get("error"):
            prefix += f"  err={acted['error'][:80]}"
        return prefix, str(verdict)


def _render_summary_rich(summary: dict, console: "Console") -> None:
    """用 rich 表格渲染运行汇总（含 ASCII bar 裁决分布）。

    spec §6.6 要求用 rich.table 替代 print 拼接，本函数实现 PR5 的汇总部分：
      - 轮数/裁决/统计/早停/结论 用 Table 对齐
      - 裁决分布加 ASCII bar（pass ███████ 3 / fail ▏0），一眼看出分布
      - 结论行按 fail/success 染色（绿/红）
    """
    v = summary.get("verdicts") or {}
    pass_n = v.get("pass", 0)
    warn_n = v.get("warn", 0)
    fail_n = v.get("fail", 0)
    na_n = v.get("na", 0)
    total_v = pass_n + warn_n + fail_n + na_n

    # ASCII bar（每格代表 1 票，最多 50 格避免过长）
    def _bar(n: int, color: str) -> str:
        if n == 0 or total_v == 0:
            return ""
        max_bar = 50
        width = max(1, int(n / total_v * max_bar))
        return f"[{color}]{'█' * width}[/] {n}"

    table = Table(show_header=True, header_style="bold cyan", show_lines=False,
                  box=None, padding=(0, 1))
    table.add_column("指标", style="dim", width=8)
    table.add_column("值")
    table.add_row("轮数", str(summary.get("rounds", 0)))
    # 裁决分布：ASCII bar + 计数
    verdict_lines = []
    if pass_n:
        verdict_lines.append(f"pass {_bar(pass_n, 'green')}")
    if warn_n:
        verdict_lines.append(f"warn {_bar(warn_n, 'yellow')}")
    if fail_n:
        verdict_lines.append(f"fail {_bar(fail_n, 'red')}")
    if na_n:
        verdict_lines.append(f"na   {_bar(na_n, 'dim')}")
    table.add_row("裁决", "\n".join(verdict_lines) if verdict_lines else "无")
    table.add_row("统计",
                  f"pass={summary.get('pass', 0)} fail={summary.get('fail', 0)} "
                  f"na={summary.get('na', 0)} stress_fail={summary.get('stress_fail', 0)}")
    table.add_row("早停", summary.get("stop_reason") or "无（正常完成）")
    console.print(table)

    # 结论行：按 fail 染色
    ok = summary.get("fail", 0) == 0
    stop_reason = summary.get("stop_reason")
    if ok:
        conclusion = "通过"
        if stop_reason:
            conclusion += f"（含早停: {stop_reason}）"
        console.print(f"  结论: [bold green]{conclusion}[/]")
    else:
        conclusion = "未通过"
        if stop_reason:
            conclusion += f"（含早停: {stop_reason}）"
        console.print(f"  结论: [bold red]{conclusion}[/]")


def _render_summary_plain(summary: dict) -> None:
    """无 rich 时回退到标准库 print（与原汇总格式一致）。"""
    v = summary.get("verdicts") or {}
    verdict_str = " ".join(f"{k}:{v.get(k, 0)}" for k in ("pass", "warn", "fail", "na")
                            if v.get(k, 0) > 0) or "无"
    print("\n===== 运行汇总 =====")
    print(f"  轮数    : {summary.get('rounds', 0)}")
    print(f"  裁决    : {verdict_str}")
    print(f"  统计    : pass={summary.get('pass', 0)} fail={summary.get('fail', 0)} "
          f"na={summary.get('na', 0)} stress_fail={summary.get('stress_fail', 0)}")
    print(f"  早停    : {summary.get('stop_reason') or '无（正常完成）'}")
    ok = summary.get("fail", 0) == 0
    conclusion = "通过" if ok else "未通过"
    if summary.get("stop_reason"):
        conclusion += f"（含早停: {summary['stop_reason']}）"
    print(f"  结论    : {conclusion}")


def render_summary(summary: dict) -> None:
    """运行汇总的统一出口：rich 可用时用表格，否则回退 print。"""
    if _HAS_RICH:
        console = Console()
        console.print("\n[bold]===== 运行汇总 =====[/]")
        _render_summary_rich(summary, console)
    else:
        _render_summary_plain(summary)


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
    verbose: bool = False,
) -> Dict[str, Any]:
    """端到端运行一份场景，返回汇总字典。

    ``adapter`` 显式传入时直接使用（测试可脚本化探测结果）；否则按 scenario.target
    自动构造 HikvisionClient + ScenarioISAPIAdapter（连接真实设备）。

    ``live=True`` 时挂载 ``ScenarioLiveReporter``，每轮裁决出来即打印一行（实时
    流式），便于长时间真实设备回归时边跑边看；库默认 ``False``（避免测试污染输出）。

    ``verbose=True``（且 ``live=True``）时 Reporter 额外订阅总线全部话题，按时间
    顺序实时打印三引擎（Loop/MAS/Harness）的每一步动作 trace，让框架协作过程
    显式可见。默认 ``False``（简洁模式，只打印每轮汇总行）。
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
            verbose=verbose,
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
