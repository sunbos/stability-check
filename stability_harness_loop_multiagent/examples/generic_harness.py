"""通用装配模板 —— 全场景基座（harness + loop + 多智能体 最佳实践）。

这是「与领域无关」的装配蓝图：把三引擎按最佳实践拼成一台自治稳定性机器。
任何具体场景（门禁、相机、任意设备）只要提供一个 ``TargetAdapter`` 实现
（把 act/observe/events 接到真实设备），即可复用同一套基座，无需改动三引擎。

本文件同时是**可运行的自校验**：用 ``GenericTargetAdapter``（内存计数器）占位，
证明装配正确（直接 ``python .../generic_harness.py`` 即可跑通健康 + 失败两条路径）。

真实设备模式（稳定性测试的主路径）
------------------------------------
``run_generic`` 暴露一个注入点 ``target_adapter``：传入任意满足 ``TargetAdapter``
协议的**真实设备适配器**（例如 ``HikvisionAdapter``）即进入真实设备模式——

- Worker 用 ``asyncio.to_thread`` 包裹阻塞型调用（真实适配器是同步阻塞 HTTP，
  单次操作可能阻塞 30-180s；不包裹会卡死事件循环，令看门狗/超时安全网失效，
  见 spec §3.1.3 长 IO 规则）。
- 看门狗停滞预算、投票/恢复/检查超时按真实设备耗时自动放大（均可用参数覆盖）。

占位适配器（合成模式）仅用于基座自校验——**真正的稳定性证明必须在真实设备上跑**
（即用真实 ``TargetAdapter`` 接入设备，运行同样这份装配）；本模板即为那次真实
运行的骨架。可用环境变量直接驱动（前端经 ``os.environ`` 透传自定义参数与策略）：

- ``STABILITY_REAL_TARGET=模块:类名``  指向真实设备适配器（主路径）。
- ``STABILITY_ROUNDS`` / ``STABILITY_RUN_TIMEOUT`` / ``STABILITY_*_TIMEOUT``
  / ``STABILITY_ROUND_INTERVAL`` / ``STABILITY_REAL_OP_TIMEOUT``  全部自定义参数。
- ``STABILITY_GOVERNANCE=1`` ``STABILITY_VERIFY=1``  开关治理/校验网关。
- ``STABILITY_STRATEGY="观察重启后温度是否回落"`  自然语言策略/提示语（闭环观察）。
- ``STABILITY_OPERATION="remote_open_door"`       对目标设备执行的操作（真机必设；默认 ping 仅占位）。

见 ``read_scenario_env`` 的变量总表；``run_generic_env`` 是等价的 pytest 可调用入口。

组装内容（最佳实践清单）：
  - harness : EventBus / Telemetry / Watchdog（存活·死锁探测）/
               Governance + Verify（opt-in 治理·校验网关）/
               Runtime（生命周期监督·重启·abort 优雅关停）
  - loop    : ControlLoop + RunConfig + DecisionAuthority + TerminationPolicy
  - MAS     : Worker（走治理/校验闸门）/ Advisor（投票）/ Observer（观测）

契约约束（必须遵守）：
  - 三引擎互不 import；跨引擎只走 EventBus。
  - ControlLoop 是*有限任务*（到 max_rounds 即结束），因此**不**交给 Runtime 监督
    （否则会被误判为“死亡”反复重启）；它由 ``asyncio.wait_for`` 充当死锁兜底，
    结束后主动发 ``harness/abort`` 让 Runtime 优雅关停所有常驻智能体。
  - Watchdog 独立于 loop/multi_agent，监听 ``loop/done``/``loop/tick``/``agent/#``
    自动重置存活计时；停滞则发 ``harness/abort``——与 Runtime 共用同一中止接缝。
"""

import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field

# 作为裸脚本从仓库根运行时，让包可被导入。
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from stability_harness_loop_multiagent import (
    AdvisorAgent,
    AgentSpec,
    ControlLoop,
    DecisionAuthority,
    EventBus,
    Governance,
    GovernanceAgent,
    ObserverAgent,
    RunConfig,
    Runtime,
    SharedContext,
    Scheduler,
    Telemetry,
    Verifier,
    VerificationAgent,
    Watchdog,
    WorkerAgent,
)
from stability_harness_loop_multiagent.core.voting import combine_votes
from stability_harness_loop_multiagent.harness.governance import DeniedOp
from stability_harness_loop_multiagent.harness.telemetry import MemorySink
from stability_harness_loop_multiagent.multi_agent.adapter import (
    Event,
    Result,
    State,
    TargetAdapter,
)
from stability_harness_loop_multiagent.multi_agent.observers.gov_panel import (
    GovernancePanelAgent,
)


# --------------------------------------------------------------------------
# 打印排版辅助（与 hikvision_real_env.py 共用 examples/_report 渲染，全局统一）
# --------------------------------------------------------------------------
from stability_harness_loop_multiagent.examples._report import (  # noqa: E402
    _VERDICT_CN, _print_header, _kv, _flush_kv, _print_round,
)


# --------------------------------------------------------------------------
# 目标适配器 —— 接缝占位（真实设备在此接入）。
# --------------------------------------------------------------------------
class GenericTargetAdapter:
    """通用目标适配器占位：一个内存计数器，仅用于证明装配正确。

    真实场景替换为实现了 ``TargetAdapter`` 协议的适配器（例如 HikvisionAdapter）：
      - ``act(operation)`` -> 调设备 ISAPI/SDK 执行操作，返回 ``Result``。
      - ``observe()``      -> 查设备状态，返回 ``State``。
      - ``events(since)``  -> 拉设备事件流，返回 ``List[Event]``。
    """

    def __init__(self, fail: bool = False) -> None:
        self.counter = 0
        self.fail = fail  # 为 True 时 observe() 报告不健康 -> 触发失败事实

    def act(self, operation) -> Result:
        self.counter += 1
        return Result(ok=True, data={"counter": self.counter, "op": operation})

    def observe(self) -> State:
        return State(snapshot={"up": not self.fail, "counter": self.counter})

    def events(self, since: float):
        evs = [Event(kind="acted", payload={"counter": self.counter}, ts=since)]
        if self.fail:
            evs.append(
                Event(kind="degraded", payload={"reason": "injected-failure"}, ts=since)
            )
        return evs


# --------------------------------------------------------------------------
# 策略指令（自然语言提示语）解析
# --------------------------------------------------------------------------
@dataclass
class StrategyDirective:
    """把前端写入的自然语言「策略/提示语」归一为结构化指令。

    - ``raw``      : 原始自然语言文本（用于审计 / 透传给 LLM）。
    - ``observe``  : 需要「观察」的目标列表（如「观察重启后温度是否回落」）。
    - ``checks``   : 可选的结构化检查项（由 LLM 解析产出，规则兜底为空）。
    - ``threshold``: 该策略关注的「风险阈值」（决策/投票可参考）。
    """

    raw: str = ""
    observe: list = field(default_factory=list)
    checks: list = field(default_factory=list)
    threshold: float = 60.0


def _parse_strategy(strategy, llm_parse=None) -> StrategyDirective:
    """把自然语言策略归一为 ``StrategyDirective``。

    - 若传入 ``llm_parse``（可调用 ``str -> dict``），优先用 LLM 解析
      （``{"observe": [...], "checks": [...], "threshold": float}``）；
    - 否则用规则兜底：抽取「观察/关注/监控/observe/...」后面的目标；
      若抽不到，则整段提示语作为一条观察备注。
    """
    if llm_parse is not None and strategy:
        try:
            parsed = llm_parse(strategy)
            if isinstance(parsed, dict):
                return StrategyDirective(
                    raw=strategy,
                    observe=list(parsed.get("observe") or []),
                    checks=list(parsed.get("checks") or []),
                    threshold=float(parsed.get("threshold", 60.0)),
                )
        except Exception:  # noqa: BLE001 - LLM 失败一律回退规则兜底
            pass
    observe: list = []
    if strategy:
        for m in re.finditer(
            r"(?:观察|关注|监控|巡检|observe|watch|monitor|check)\s*[:：]?\s*"
            r"([^\n;；。]+)",
            strategy,
            re.IGNORECASE,
        ):
            target = m.group(1).strip()
            if target:
                observe.append(target)
    if not observe:
        observe = [strategy] if strategy else []
    return StrategyDirective(raw=strategy or "", observe=observe, threshold=60.0)


# --------------------------------------------------------------------------
# MAS 角色 —— 通用；不含任何领域知识。
# --------------------------------------------------------------------------
class GenericWorker(WorkerAgent):
    """通用 Worker 模板：演示把治理/校验闸门套在破坏性操作之外。

    每个 loop/tick 走「治理闸门 -> 校验闸门 -> 实际 act -> recover -> check」
    流水线。两个闸门都 opt-in（fail-closed：超时/无回复 -> 放行，不 halt 循环；
    真正拒绝由 ``denied_ops`` / 校验失败触发并跳过该操作）。这与 hikvision 的最佳
    实践一致，可作为任意场景 Worker 的基类。
    """

    def __init__(
        self,
        bus,
        spec,
        adapter: TargetAdapter,
        *,
        governance=None,
        enable_governance: bool = False,
        governance_timeout: float = 1.0,
        verifier=None,
        enable_verify: bool = False,
        verify_timeout: float = 1.0,
        capability: str = "act",
        strategy=None,
        operation: str = "ping",
    ) -> None:
        super().__init__(bus, spec, adapter)
        self._gov = governance
        self._enable_gov = enable_governance
        self._gov_timeout = governance_timeout
        self._verifier = verifier
        self._enable_verify = enable_verify
        self._verify_timeout = verify_timeout
        self._capability = capability
        # 默认操作类型：loop/tick 不携带操作，Worker 用此缺省；真实设备经
        # STABILITY_OPERATION 指定（如 remote_open_door / reboot）。
        self._operation = operation
        # 最近一次 act 的结果（do_work 内记录），check() 据此产出 act_ok 事实。
        self._last_act_result = None
        # 策略指令：自然语言「观察 XXX」归一后的结构化指令，check() 会写进事实，
        # 使「哪一轮、观察了什么」在裁决记录与遥测里可见（前端可追溯）。
        self._strategy = strategy

    async def act(self, tick: dict) -> None:
        op = tick.get("operation", "ping")
        # 1) 治理闸门：按 role/capability/op 维度鉴权（P1-d）。
        allowed, denied_ops = await self._govern(op)
        if not allowed or op in denied_ops:
            self.publish(
                "agent/" + self.role + "/denied",
                {"op": op, "allowed": allowed, "denied_ops": denied_ops},
            )
            # 跳过实际 act，但补全流水线，使本轮仍有事实可供裁决。
            self._emit_skipped(tick, reason="governance-denied")
            return
        # 2) 校验闸门：对输入做护栏校验（P1-b 真触发）。
        if self._enable_verify and not await self._verify(op):
            self._emit_skipped(tick, reason="verify-rejected")
            return
        # 3) 实际执行（子类覆盖 do_work 接入真实设备）。
        await super().act(tick)

    def _emit_skipped(self, tick: dict, *, reason: str) -> None:
        self.publish(
            "target/acted",
            {"role": self.role, "round": tick.get("round"),
             "result": Result(ok=False, error=reason)},
        )
        self.publish(
            "target/recovered",
            {"role": self.role, "round": tick.get("round"), "recovered": True},
        )
        self.publish(
            "target/checked",
            {"role": self.role, "round": tick.get("round"),
             "facts": {"acted": False, "state_ok": True}},
        )
        self.publish("agent/" + self.role + "/done", {"round": tick.get("round")})

    async def _govern(self, op: str):
        if not self._enable_gov or self._gov is None:
            return True, []
        req = {
            "role": self.role,
            "capability": self._capability,
            "operation": op,
            "operations": [op],
        }
        try:
            reply = await self.request(
                "harness/govern/request", req, timeout=self._gov_timeout
            )
        except Exception:  # noqa: BLE001 - fail-closed 超时 -> 放行
            return True, []
        if not isinstance(reply, dict):
            return True, []
        return bool(reply.get("allowed", True)), list(reply.get("denied_ops", []))

    async def _verify(self, op: str) -> bool:
        req = {"stage": "input", "item": {"operation": op}}
        try:
            reply = await self.request(
                "harness/verify/request", req, timeout=self._verify_timeout
            )
        except Exception:  # noqa: BLE001 - fail-closed 超时 -> 放行
            return True
        if not isinstance(reply, dict):
            return True
        return bool(reply.get("allowed", True))

    def do_work(self, tick: dict):
        op = tick.get("operation", self._operation)
        result = self.adapter.act(op)
        self._last_act_result = result
        return result

    def check(self, tick: dict) -> dict:
        snap = self.adapter.observe().snapshot
        # 设备观测返回 error 字段（如真实适配器连接失败）-> 视为不健康。
        if isinstance(snap, dict) and snap.get("error"):
            up = False
        else:
            up = isinstance(snap, dict) and bool(snap.get("up", True))
        # 最近一次操作是否成功：失败 -> act_ok 为 falsy -> DecisionAuthority
        # 事实独裁判 fail（即使 Advisor 投低风险）。NULL 时（异常路径）保守为真。
        last = self._last_act_result
        act_ok = bool(last.ok) if last is not None else True
        facts = {"acted": True, "state_ok": up, "act_ok": act_ok}
        # 把策略指令（观察项）落进事实：裁决记录/遥测/前端均可追溯「观察了什么」。
        # 仅在有真实策略文本时写入，避免空字符串/空列表（falsy）被 DecisionAuthority
        # 误判为失败事实（事实独裁：falsy 事实 -> fail）。
        if self._strategy is not None and self._strategy.raw:
            facts["strategy"] = self._strategy.raw
            facts["observing"] = list(self._strategy.observe)
        return facts


class RealDeviceWorker(GenericWorker):
    """真实设备 Worker：把阻塞型 TargetAdapter 调用用 ``asyncio.to_thread`` 包裹。

    真实 ``TargetAdapter``（如 ``HikvisionAdapter``）是同步阻塞 HTTP（urllib），
    单次操作可能阻塞 30-180s（重启+探测+预热）。若直接在主事件循环里调用
    ``adapter.act()`` / ``adapter.observe()``，会卡死整个 ControlLoop，使看门狗/
    超时安全网全部失效（看门狗在同一事件循环上、依赖 loop 活动持续投递）。按
    spec §3.1.3 长 IO 规则，必须用 ``asyncio.to_thread`` 把阻塞调用挪到工作线程，
    保持事件循环响应，使 ControlLoop 的超时安全网与 Watchdog 的停滞探测始终有效。

    治理/校验闸门是异步的（总线 request/reply），仍在事件循环上执行（与
    hikvision 实践一致）；只有阻塞的 ``do_work`` + ``check``（含 ``observe``）
    被移入线程。
    """

    async def act(self, tick: dict) -> None:
        op = tick.get("operation", "ping")
        # 1) 治理闸门（异步，事件循环上）。
        allowed, denied_ops = await self._govern(op)
        if not allowed or op in denied_ops:
            self.publish(
                "agent/" + self.role + "/denied",
                {"op": op, "allowed": allowed, "denied_ops": denied_ops},
            )
            self._emit_skipped(tick, reason="governance-denied")
            return
        # 2) 校验闸门（异步，事件循环上）。
        if self._enable_verify and not await self._verify(op):
            self._emit_skipped(tick, reason="verify-rejected")
            return
        # 3) 阻塞型调用移出事件循环（spec §3.1.3 长 IO 规则）。
        result, facts = await asyncio.to_thread(self._run_blocking, tick)
        self.publish(
            "target/acted",
            {"role": self.role, "round": tick.get("round"), "result": result},
        )
        recovered = await self.recover(tick)
        self.publish(
            "target/recovered",
            {"role": self.role, "round": tick.get("round"), "recovered": recovered},
        )
        self.publish(
            "target/checked",
            {"role": self.role, "round": tick.get("round"), "facts": facts},
        )
        self.publish("agent/" + self.role + "/done", {"round": tick.get("round")})

    def _run_blocking(self, tick: dict):
        """在线程中执行阻塞部分：实际 act + check（含 observe）。"""
        result = self.do_work(tick)
        facts = self.check(tick)
        return result, facts


class FixedAdvisor(AdvisorAgent):
    """最小 Advisor：每轮投固定（风险、置信度）。"""

    def __init__(self, bus, spec, *, risk: float = 30.0, confidence: float = 0.9,
                 weight: float = 1.0) -> None:
        super().__init__(bus, spec, weight=weight)
        self._risk = float(risk)
        self._confidence = float(confidence)

    def vote(self):
        return (self._risk, self._confidence)


class LoggingObserver(ObserverAgent):
    """记录所见每个事件，并打印每轮结论横幅的 Observer。"""

    def __init__(self, bus, spec, *, total_rounds="?") -> None:
        super().__init__(bus, spec)
        self.seen = []
        self._total_rounds = total_rounds

    def on_event(self, topic: str, message) -> None:
        self.seen.append((topic, message))
        if topic == "loop/done":
            _print_round(
                message.get("round"),
                self._total_rounds,
                message.get("verdict", "?"),
                message.get("risk", 0.0),
                message.get("facts") or {},
            )


class StrategyAdvisor(AdvisorAgent):
    """策略型 Advisor：承接自然语言 ``instruction``，可经 LLM 解析为计划再投票。

    与 ``business/hikvision/advisor.HikvisionAdvisor`` 同构：只投票 / 上报事件，
    绝不执行操作、也不决定裁决。``llm_parse`` 可调用 ``str -> dict``（确定性测试
    友好）；未提供时退化为固定（风险、置信度）投票。解析出的计划发布到
    ``generic/plan``，供 Observer / 前端消费。
    """

    def __init__(
        self,
        bus,
        spec,
        *,
        instruction: str = "",
        llm_parse=None,
        risk: float = 30.0,
        confidence: float = 0.9,
        weight: float = 1.0,
        enable_verify: bool = False,
        verify_timeout: float = 1.0,
    ) -> None:
        super().__init__(bus, spec, weight=weight)
        self._instruction = instruction
        self._llm_parse = llm_parse
        self._risk = float(risk)
        self._confidence = float(confidence)
        self._plan: dict = {}
        self._enable_verify = enable_verify
        self._verify_timeout = verify_timeout

    async def start(self) -> None:
        await super().start()
        if self._instruction and self._llm_parse:
            try:
                self._plan = self._llm_parse(self._instruction) or {}
            except Exception:  # noqa: BLE001
                self._plan = {}
            if self._enable_verify and not await self._verify_plan(self._plan):
                self._plan = {}
                return
            self.publish("generic/plan", self._plan)

    async def _verify_plan(self, plan: dict) -> bool:
        req = {"item": plan, "kind": "plan"}
        try:
            reply = await self.request(
                "harness/verify/request", req, timeout=self._verify_timeout
            )
        except Exception:  # noqa: BLE001
            return False
        if not isinstance(reply, dict):
            return False
        return bool(reply.get("allowed", False))

    def vote(self) -> tuple:
        # 趋势：近期任一轮高风险则抬升投票风险（与 FixedAdvisor 同策略）。
        window = self._private_window
        if window and any(isinstance(r, (int, float)) and r >= 60 for r in window[-10:]):
            return (75.0, 0.8)
        return (self._risk, self._confidence)


class StrategyObserver(LoggingObserver):
    """策略型 Observer：记录「正在观察 XXX」，并在每轮广播观察事实供前端读取。

    前端用自然语言写入策略（如「观察重启后温度是否回落」），本 Observer 把它
    落到 ``target/strategy`` 订阅与每轮 ``agent/scribe/observe`` 事件上，使
    「前端写提示语 -> 系统真的在观察」形成闭环、可在总线/遥测里追溯。
    """

    def __init__(self, bus, spec, *, directives: StrategyDirective | None = None,
                 total_rounds="?") -> None:
        super().__init__(bus, spec, total_rounds=total_rounds)
        self.directives = directives or StrategyDirective()

    def on_event(self, topic: str, message) -> None:
        super().on_event(topic, message)
        if topic == "loop/done":
            observing = self.directives.observe
            if observing:
                print(f"[observer] 观察中: {observing}")
                self.publish(
                    "agent/" + self.role + "/observe",
                    {
                        "round": message.get("round"),
                        "observing": observing,
                        "strategy": self.directives.raw,
                    },
                )


# --------------------------------------------------------------------------
# 通用治理/校验配置（opt-in 的演示最小配置）。
# --------------------------------------------------------------------------
def build_governance(bus, tel):
    """演示：按操作维度拒绝 ``destroy``（其余放行），fail-closed 只拦操作不 halt。"""
    gov = Governance(
        bus,
        denied_operations=[DeniedOp(op="destroy", role="*")],
        emit_abort=False,  # 拒绝语义：fail-closed 只拦操作、不中止循环
    )
    gov.telemetry = tel  # 让治理决策事实发到总线供治理观测面板消费
    return gov


def build_verifier():
    """演示：输入护栏只允许白名单操作（其余 fail-closed 拒绝）。"""
    allowed = {"ping", "act", "open", "reboot"}

    def _op_allowlisted(item):
        op = (item or {}).get("operation", "")
        return (op in allowed) or (False, f"operation not allowlisted: {op}")

    return Verifier(fail_closed=True).add_input_guardrail("op-allowlist", _op_allowlisted)


# --------------------------------------------------------------------------
# 端到端装配器。返回供断言/检查使用的产物字典。
# --------------------------------------------------------------------------
async def run_generic(
    target_adapter=None,
    fail: bool = False,
    max_rounds: int = 5,
    *,
    run_timeout: float = None,
    enable_governance: bool = False,
    enable_verify: bool = False,
    real_device: bool = False,
    device_op_timeout: float = 180.0,
    round_interval: float = None,
    stall_timeout: float = None,
    vote_timeout: float = None,
    recover_timeout: float = None,
    check_timeout: float = None,
    strategy: str | None = None,
    llm_parse=None,
    device_config: dict | None = None,
    operation: str = "ping",
) -> dict:
    """端到端装配器。

    ``target_adapter`` 是真实设备注入点：传入任意满足 ``TargetAdapter`` 协议的
    真实适配器（如 ``HikvisionAdapter``）即进入**真实设备模式**——Worker 用
    ``asyncio.to_thread`` 包裹阻塞型调用、看门狗/投票/恢复/检查超时按真实设备
    耗时放大。``real_device=True`` 可强制开启（即使没传真实适配器，也走真实设备的
    时间模型，便于调参；此时仍用占位适配器，仅时间模型切换）。

    ``strategy`` 是**自然语言策略/提示语**（如「观察重启后温度是否回落到正常区间」）。
    它经 ``_parse_strategy`` 归一为 ``StrategyDirective``，落到三方：Advisor 作为
    ``instruction``（可选经 ``llm_parse`` 解析为计划再投票）、Observer 真正「观察
    XXX」并在总线广播、Worker 把观察项写进事实（裁决/遥测可追溯）。``strategy``
    同时发布到 ``target/strategy`` 供前端读取——这就是「前端写提示语、系统观察」
    的闭环。``llm_parse`` 缺省时用规则兜底抽取「观察/observe」后的目标。

    所有参数均可经环境变量注入（见 ``read_scenario_env`` / ``run_generic_env``），
    便于前端把自定义参数通过 ``os.environ`` 透传给 pytest 运行的基座。

    ``fail`` 仅在合成模式下生效（占位适配器被注入失败事实）；真实设备模式下设备
    真伪由 ``adapter.observe()`` 决定，``fail`` 被忽略。

    真实设备模式是稳定性测试的主路径；合成模式仅用于基座自校验（见 ``_main``）。
    """
    is_real = (target_adapter is not None) or real_device

    if is_real:
        # 真实设备时间模型（均可被显式参数覆盖）。
        vote_timeout = float(vote_timeout if vote_timeout is not None else 5.0)
        recover_timeout = float(
            recover_timeout if recover_timeout is not None else device_op_timeout
        )
        check_timeout = float(
            check_timeout if check_timeout is not None else device_op_timeout
        )
        stall_timeout = float(
            stall_timeout if stall_timeout is not None
            else max(300.0, device_op_timeout + 120.0)
        )
        round_interval = float(round_interval if round_interval is not None else 2.0)
        run_timeout = float(
            run_timeout if run_timeout is not None
            else (device_op_timeout * 2.0 + vote_timeout + 60.0) * max_rounds
        )
        worker_cls = RealDeviceWorker
    else:
        # 合成模式：极短超时，跑得快、确定；占位适配器只用于证明基座接线。
        vote_timeout = 0.1
        recover_timeout = 0.05
        check_timeout = 0.05
        stall_timeout = 300.0
        round_interval = 0.0
        run_timeout = 30.0
        worker_cls = GenericWorker

    # 策略指令归一：自然语言 -> 结构化观察项（规则兜底或经 llm_parse）。
    directive = _parse_strategy(strategy, llm_parse)

    bus = EventBus()
    mem = MemorySink()
    tel = Telemetry(bus=bus, sinks=[mem])

    ctx = SharedContext(
        baseline={"kind": "generic", "strategy": directive.raw},
        strategy_text=directive.raw or "generic-harness",
    )
    decision = DecisionAuthority()
    cfg = RunConfig(
        max_rounds=max_rounds, max_duration=0.0, fail_threshold=10_000,
        vote_timeout=vote_timeout,
        vote_settle=max(0.05, vote_timeout * 0.5),
        recover_timeout=recover_timeout,
        check_timeout=check_timeout,
        recheck_limit=0,
    )
    term = cfg.build_termination()
    # 投票合并统一走 core.voting.combine_votes（loop/multi_agent 同一份）。
    loop = ControlLoop(
        bus, ctx, decision, term,
        combine=combine_votes,
        vote_timeout=cfg.vote_timeout,
        recover_timeout=cfg.recover_timeout,
        check_timeout=cfg.check_timeout,
        recheck_limit=cfg.recheck_limit,
        scheduler=Scheduler(base=round_interval, min_interval=round_interval),
        telemetry=tel,
    )

    # 目标 + 常驻智能体
    adapter = target_adapter if target_adapter is not None else GenericTargetAdapter(fail=fail)
    gov = build_governance(bus, tel) if enable_governance else None
    verifier = build_verifier() if enable_verify else None
    worker = worker_cls(
        bus,
        AgentSpec(id="w1", role="worker", capabilities={"act"}),
        adapter,
        governance=gov,
        enable_governance=enable_governance,
        governance_timeout=max(1.0, vote_timeout),
        verifier=verifier,
        enable_verify=enable_verify,
        verify_timeout=max(1.0, vote_timeout),
        strategy=directive,
        operation=operation,
    )
    advisor = StrategyAdvisor(
        bus,
        AgentSpec(id="a1", role="risk"),
        instruction=directive.raw,
        llm_parse=llm_parse,
        risk=30.0,
        confidence=0.9,
        weight=1.0,
        enable_verify=enable_verify,
        verify_timeout=max(1.0, vote_timeout),
    )
    observer = StrategyObserver(
        bus,
        AgentSpec(
            id="o1", role="scribe",
            subscriptions=["loop/done", "target/#", "agent/#", "harness/#"],
        ),
        directives=directive,
        total_rounds=max_rounds,
    )
    # 看门狗：停滞预算由时间模型决定（合成模式充裕且绝不应触发；真实设备须 >
    # 单轮最慢操作耗时，参考 hikvision runner 的 stall_timeout 计算）。
    dog = Watchdog(
        bus, stall_timeout=stall_timeout,
        check_interval=max(0.02, min(1.0, round_interval)),
    )

    # opt-in 治理/校验网关 + 治理观测面板
    gov_agent = GovernanceAgent(bus, gov) if gov is not None else None
    gov_panel = (
        GovernancePanelAgent(
            bus,
            AgentSpec(
                id="o2", role="gov-panel",
                subscriptions=["harness/fact/governance.decision",
                               "governance/panel/request"],
            ),
        )
        if gov is not None
        else None
    )
    verify_agent = VerificationAgent(bus, verifier) if verifier is not None else None

    # 智能体分两类，装配方式不同（关键最佳实践）：
    #  - 自驱型（Watchdog）：持有主动 run 循环，交给 Runtime 监督——它若崩溃会被
    #    自动重启，使「死锁保护」本身不被单点失效击穿。
    #  - 响应式（worker/advisor/observer/治理·校验网关）：事件驱动，没有主动 run
    #    循环，基类 run() 立即返回。若交给 Runtime 监督会被误判为「死亡」反复重启；
    #    因此按既有实践（smoke.py / hikvision runner）直接 start/stop，总线持续投递
    #    保证它们始终可用，单条坏消息由基类 _dispatch 捕获而非拖垮整个智能体。
    reactive = [worker, advisor, observer]
    if gov_agent is not None:
        reactive.append(gov_agent)
    if gov_panel is not None:
        reactive.append(gov_panel)
    if verify_agent is not None:
        reactive.append(verify_agent)

    # 先启动响应式智能体（确保订阅就绪），再启动循环——
    # 避免「循环先发布 loop/tick、Worker 还没订阅」的竞态（首轮漏 tick -> fail）。
    for a in reactive:
        await a.start()

    # 把自然语言策略结构化后广播到总线：前端（订阅 target/#）可实时读到
    # 「这条用例正在观察什么」，形成「前端写提示语 -> 系统观察」的闭环。
    bus.publish(
        "target/strategy",
        {
            "raw": directive.raw,
            "observe": directive.observe,
            "checks": directive.checks,
            "threshold": directive.threshold,
        },
    )

    # 设备信息（脱敏）广播到总线 + 打印：前端（订阅 target/#）可看到本轮打的是
    # 哪台设备，且密钥已打码。无真实设备（合成模式）时不发布。
    if device_config:
        redacted = _redact(device_config)
        _print_header("RUN CONFIG · 真实设备")
        _kv("目标设备", redacted)
        _kv("轮数", max_rounds)
        _kv("治理网关", "启用" if enable_governance else "禁用")
        _kv("校验网关", "启用" if enable_verify else "禁用")
        _kv("策略", directive.raw if directive.raw else "(空)")
        _flush_kv()
        bus.publish("target/device", {"config": redacted, "rounds": max_rounds})

    runtime = Runtime(bus, telemetry=tel, max_restarts=3, supervisor_interval=0.05)
    runtime.register(dog)  # 仅监督自驱的看门狗

    # ControlLoop 单独启动（有限任务，不受 Runtime 监督；若把它交给 Runtime，
    # 正常到 max_rounds 结束会被误判死亡而重启）。
    await loop.start()
    supervisor = asyncio.ensure_future(runtime.run())
    try:
        await asyncio.wait_for(loop._task, run_timeout)
    except asyncio.TimeoutError:
        # 死锁兜底：wait_for 超时仍未结束 -> 强制中止并上抛。
        bus.publish("harness/abort", {"reason": "run-timeout (deadlock guard)"})
        raise
    finally:
        # 先取消循环订阅，避免重复响应 harness/abort；再主动发 harness/abort
        # 让 Runtime 优雅关停被监督的看门狗。
        await loop.stop()
        bus.publish("harness/abort", {"reason": "loop-complete"})
        try:
            await asyncio.wait_for(supervisor, 5.0)
        except asyncio.TimeoutError:
            supervisor.cancel()
        # 响应式智能体不在 Runtime 下，手动关停。
        for a in reactive:
            await a.stop()

    result = {
        "ctx": ctx, "loop": loop, "observer": observer, "adapter": adapter,
        "telemetry": tel, "config": cfg, "runtime": runtime,
        "strategy": directive,
        "device_config": _redact(device_config) if device_config else None,
    }
    if gov is not None:
        result["governance"] = gov
        result["gov_panel"] = gov_panel
    if verifier is not None:
        result["verifier"] = verifier
    return result


# --------------------------------------------------------------------------
# 断言（与 smoke 对齐的不变量）。
# --------------------------------------------------------------------------
def assert_healthy(result: dict) -> None:
    ctx = result["ctx"]
    loop = result["loop"]
    observer = result["observer"]
    adapter = result["adapter"]
    assert ctx.round_count >= 1, "循环没有产生任何轮次"
    assert ctx.round_count == result["config"].max_rounds, (
        f"期望 {result['config'].max_rounds} 轮，实际 {ctx.round_count}"
    )
    assert loop.verdict is not None, "没有设置权威裁决"
    history = ctx.snapshot().round_history
    assert all(r.verdict == "pass" for r in history), (
        f"健康运行应当只产生 'pass' 裁决：{[r.verdict for r in history]}"
    )
    assert observer.seen, "Observer 没有收到任何事件"
    assert any(t == "loop/done" for t, _ in observer.seen)
    assert adapter.counter == ctx.round_count, (
        f"Worker 执行了 {adapter.counter} 次，但运行了 {ctx.round_count} 轮"
    )


def assert_failing_fact(result: dict) -> None:
    ctx = result["ctx"]
    loop = result["loop"]
    history = ctx.snapshot().round_history
    # 事实独裁：被注入的失败事实必须强制 fail，即使 Advisor 投低风险（30）。
    assert any(r.verdict == "fail" for r in history), (
        f"失败事实应当至少强制一个 'fail'：{[r.verdict for r in history]}"
    )
    assert ctx.snapshot().round_history[-1].verdict == "fail"
    assert any(not ok for r in history for ok in r.facts.values()), (
        "期望在已记录轮次中至少有一个 falsy 事实"
    )


def _print_summary(result: dict) -> None:
    """打印通用基座的 SUMMARY（对齐 KV + 通过率 + 裁决分布），与 hikvision 同风格。"""
    _print_header("SUMMARY · 测试汇总")
    ctx = result["ctx"]
    loop = result["loop"]
    history = ctx.snapshot().round_history
    decisions: dict = {}
    for r in history:
        key = _VERDICT_CN.get(r.verdict, r.verdict)
        decisions[key] = decisions.get(key, 0) + 1
    passed = sum(1 for r in history if r.verdict == "pass")
    total = max(1, len(history))
    _kv("总轮数", ctx.round_count)
    _kv("裁决分布", decisions)
    _kv("最终裁决", _VERDICT_CN.get(loop.verdict.decision, loop.verdict.decision))
    _kv("通过率", f"{passed}/{len(history)} ({100.0 * passed / total:.0f}%)")
    adapter = result.get("adapter")
    if adapter is not None and hasattr(adapter, "counter"):
        _kv("操作执行次数", adapter.counter)
    if result.get("device_config"):
        _kv("目标设备", result["device_config"])
    strategy = result.get("strategy")
    if strategy is not None and getattr(strategy, "raw", ""):
        _kv("策略", strategy.raw)
    _flush_kv()


# --------------------------------------------------------------------------
# 环境变量配置通道（前端经 os.environ 透传自定义参数给 pytest 运行的基座）
# --------------------------------------------------------------------------
def _truthy(v: str) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on", "y")


def _instantiate_target(spec: str, device_config: dict | None = None):
    """从 ``模块:类名`` 导入并实例化真实设备适配器。

    ``device_config`` 是前端经 os.environ 透传的设备连接信息（host/user/pass/...）。
    按以下顺序尝试构造，容忍不同适配器签名：
      1. ``cls(device_config=device_config)``   —— 推荐约定（单通道、显式）。
      2. ``cls(**device_config)``               —— 适配器直接用 host=... 等关键字。
      3. ``cls()``                              —— 适配器自管配置（兜底）。
    """
    if ":" not in spec:
        raise SystemExit(
            f"STABILITY_REAL_TARGET 需为 '模块:类名' 形式，收到：{spec!r}"
        )
    module_name, class_name = spec.split(":", 1)
    try:
        import importlib
        module = importlib.import_module(module_name)
        adapter_cls = getattr(module, class_name)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"无法导入真实设备适配器 {spec!r}: {exc}") from exc
    try:
        if device_config is not None:
            try:
                return adapter_cls(device_config=device_config)
            except TypeError:
                try:
                    return adapter_cls(**device_config)
                except TypeError:
                    pass
        return adapter_cls()
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            f"实例化 {class_name} 失败（签名需支持 device_config= 或 **config 或无参）：{exc}"
        ) from exc


def _redact(config):
    """脱敏：对含 pass/secret/token/key 的字段打码，仅用于日志/遥测展示。"""
    if not isinstance(config, dict):
        return config
    secret_hints = ("pass", "password", "secret", "token", "key", "apikey",
                    "credential", "pwd")
    out = {}
    for k, v in config.items():
        if any(h in str(k).lower() for h in secret_hints):
            out[k] = "***"
        else:
            out[k] = v
    return out


def read_scenario_env(prefix: str = "STABILITY_") -> dict:
    """把前端通过 os.environ 传入的自定义参数解析为 ``run_generic`` 的关键字参数。

    支持的全部变量（前缀 ``STABILITY_``）：

    ============================ =========================== =======================
    变量                         映射参数                    类型 / 说明
    ============================ =========================== =======================
    REAL_TARGET                   _real_target（→target_adapter） 模块:类名（真实设备适配器）
    ROUNDS / REAL_ROUNDS          max_rounds                  int
    RUN_TIMEOUT                   run_timeout                 float（秒）
    VOTE_TIMEOUT                  vote_timeout                float
    RECOVER_TIMEOUT               recover_timeout             float
    CHECK_TIMEOUT                 check_timeout               float
    STALL_TIMEOUT                 stall_timeout               float（看门狗停滞预算）
    ROUND_INTERVAL                round_interval             float（轮间间隔）
    REAL_OP_TIMEOUT               device_op_timeout           float（单轮操作超时）
    GOVERNANCE                    enable_governance           0/1 开关
    VERIFY                        enable_verify               0/1 开关
    REAL_DEVICE                   real_device                 0/1 强制真实设备时间模型
    STRATEGY / INSTRUCTION        strategy                    自然语言策略/提示语
    OPERATION                      operation                   对设备执行的操作（默认 ping；真机用 remote_open_door / reboot）
    DEVICE_CONFIG                 _device_config（→适配器）  JSON 全量设备连接信息
    DEVICE_IP/USERNAME/PASSWORD   _device_config             前端 3 输入框（IP/用户名/密码）
    DEVICE_HOST/PORT/USER/PASS    _device_config             设备连接单项（别名，向后兼容）
    DEVICE_EXTRA                  _device_config             额外 JSON 合并
    ============================ =========================== =======================

    返回空 dict 表示「未设置任何场景变量」（调用方应回退到默认演示）。
    解析失败的非空变量会被安全忽略（不影响其余参数）。
    """
    env = os.environ
    cfg: dict = {}

    def _get(name, cast):
        v = env.get(prefix + name)
        if v in (None, ""):
            return None
        try:
            return cast(v)
        except (ValueError, TypeError):
            return None

    target = _get("REAL_TARGET", str)
    if target:
        cfg["_real_target"] = target

    # 设备连接信息（前端经 os.environ 透传；脱敏仅在展示时做，不影响真实值）。
    # 优先 STABILITY_DEVICE_CONFIG（JSON 全量），再用单项变量合并（host/user/pass
    # 等覆盖 JSON 的同名字段），STABILITY_DEVICE_EXTRA 为额外 JSON 合并。
    device: dict = {}
    cfg_json = _get("DEVICE_CONFIG", str)
    if cfg_json:
        try:
            device.update(json.loads(cfg_json))
        except (ValueError, TypeError):
            pass
    # 前端 3 个输入框：IP / 用户名 / 密码（大小写不敏感，内部归一为
    # ip / username / password；HikvisionAdapter.normalize_device_config 亦
    # 接受 host/user/pass 等别名，向后兼容）。
    for env_name, key in (
        ("DEVICE_IP", "ip"),
        ("DEVICE_HOST", "host"),
        ("DEVICE_PORT", "port"),
        ("DEVICE_USERNAME", "username"),
        ("DEVICE_USER", "user"),
        ("DEVICE_PASSWORD", "password"),
        ("DEVICE_PASS", "pass"),
    ):
        v = _get(env_name, str)
        if v is not None:
            device[key] = v
    extra = _get("DEVICE_EXTRA", str)
    if extra:
        try:
            device.update(json.loads(extra))
        except (ValueError, TypeError):
            pass
    if device:
        cfg["_device_config"] = device

    rounds = _get("ROUNDS", int) or _get("REAL_ROUNDS", int)
    if rounds is not None:
        cfg["max_rounds"] = rounds

    for env_name, kw, cast in (
        ("RUN_TIMEOUT", "run_timeout", float),
        ("VOTE_TIMEOUT", "vote_timeout", float),
        ("RECOVER_TIMEOUT", "recover_timeout", float),
        ("CHECK_TIMEOUT", "check_timeout", float),
        ("STALL_TIMEOUT", "stall_timeout", float),
        ("ROUND_INTERVAL", "round_interval", float),
        ("REAL_OP_TIMEOUT", "device_op_timeout", float),
    ):
        v = _get(env_name, cast)
        if v is not None:
            cfg[kw] = v

    if _get("GOVERNANCE", _truthy):
        cfg["enable_governance"] = True
    if _get("VERIFY", _truthy):
        cfg["enable_verify"] = True
    if _get("REAL_DEVICE", _truthy):
        cfg["real_device"] = True

    strategy = _get("STRATEGY", str) or _get("INSTRUCTION", str)
    if strategy is not None:
        cfg["strategy"] = strategy

    op = _get("OPERATION", str)
    if op is not None:
        cfg["operation"] = op

    return cfg


async def run_generic_env() -> dict:
    """读取 os.environ 场景变量并运行 ``run_generic``（前端驱动的入口）。

    与 ``run_generic`` 等价的「环境驱动」封装：pytest / ``python .../generic_harness.py``
    均可直接调用。真实设备适配器按 ``_real_target`` 自动实例化注入。
    """
    cfg = read_scenario_env()
    target_spec = cfg.pop("_real_target", None)
    device_config = cfg.pop("_device_config", None)
    if target_spec:
        cfg["target_adapter"] = _instantiate_target(target_spec, device_config)
    # 设备信息仅用于展示/遥测（脱敏），无真实设备时不传。
    if device_config is not None:
        cfg["device_config"] = device_config
    return await run_generic(**cfg)


async def _main() -> None:
    # 环境变量驱动（前端经 os.environ 透传）：只要设置了任何 STABILITY_ 场景变量，
    # 就走 run_generic_env 路径（真实设备 / 自定义参数 / 自然语言策略皆可）。
    #   STABILITY_REAL_TARGET=包路径:类名   真实设备适配器（主路径）
    #   STABILITY_ROUNDS=5                 循环次数（与自然语言策略正交，不重合）
    #   STABILITY_STRATEGY="观察重启后温度是否回落"   自然语言策略/提示语
    #   STABILITY_OPERATION="remote_open_door"       对设备执行的操作（真机必设）
    #   STABILITY_GOVERNANCE=1 STABILITY_VERIFY=1     治理/校验网关
    #   STABILITY_VOTE_TIMEOUT=5 ...                 各类超时（真实设备必调大）
    #   STABILITY_DEVICE_CONFIG='{"ip":"10.0.0.1","username":"admin","password":"***"}'  设备连接(JSON)
    #   STABILITY_DEVICE_IP/USERNAME/PASSWORD        前端 3 输入框（IP/用户名/密码）
    #   STABILITY_DEVICE_HOST/PORT/USER/PASS         设备连接单项（别名，向后兼容）
    #   STABILITY_DEVICE_EXTRA='{"channel":1}'       设备额外参数(JSON 合并)
    if read_scenario_env():
        result = await run_generic_env()
        _print_summary(result)
        return

    _print_header("通用基座 RUN CONFIG")
    _kv("模式", "合成自校验（占位适配器）")
    _kv("轮数", 5)
    _kv("治理网关", "启用")
    _kv("校验网关", "启用")
    _kv("策略", "(空)")
    _flush_kv()

    print("=== 通用基座（健康 + opt-in 治理/校验）===")
    healthy = await run_generic(
        fail=False, max_rounds=5, enable_governance=True, enable_verify=True
    )
    assert_healthy(healthy)
    _print_summary(healthy)

    print("\n=== 通用基座（事实独裁：失败事实 -> fail）===")
    failing = await run_generic(fail=True, max_rounds=5)
    assert_failing_fact(failing)
    _print_summary(failing)

    print("\nALL GENERIC HARNESS ASSERTIONS PASSED")


if __name__ == "__main__":
    asyncio.run(_main())
