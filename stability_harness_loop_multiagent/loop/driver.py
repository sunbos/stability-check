"""ControlLoop —— 确定性的循环引擎（感测->规划->执行->检查->裁决->停止）。

完全通过事件总线驱动（无直接的引擎 import）。它会：
  - 发布 ``loop/tick``（触发工作者的感测/规划/执行），
  - 收集 ``target/recovered`` 与 ``target/checked`` 事实（带超时），
  - 通过 ``loop/vote/request`` 请求投票并收集 ``agent/vote/reply``，
  - 应用 DecisionAuthority，向 SharedContext 追加一条 RoundRecord，
  - 发布 ``loop/done``；若为 recheck 则发布 ``loop/recheck``（最多一次）；
  - 对其他人提出的事件进行 ack，并在终止/harness/abort 时停止。

持有权威裁决结果。风险合并是一个本地的确定性辅助函数（引擎隔离：规范的
``combine_votes`` 位于 multi_agent/protocols，用于 MAS 侧的聚合；本循环保留
自己的实现以避免导入 multi_agent）。
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from ..harness.agent import Agent, AgentSpec
from ..harness.bus import EventBus
from .context import RoundRecord, SharedContext
from .decision import (
    CONSERVATIVE_RISK,
    NEUTRAL_RISK,
    DecisionAuthority,
    Verdict,
)
from .scheduler import RetryBudget, Scheduler
from .termination import (
    CountStop,
    DurationStop,
    FailThresholdStop,
    StopCondition,
    TerminationPolicy,
)


class ControlLoop(Agent):
    def __init__(
        self,
        bus: EventBus,
        ctx: SharedContext,
        decision: DecisionAuthority,
        termination: TerminationPolicy,
        *,
        vote_timeout: float = 1.0,
        recover_timeout: float = 30.0,
        check_timeout: float = 30.0,
        recheck_limit: int = 1,
        combine: Callable[[List[Dict[str, Any]]], float] = None,
        scheduler: Optional[Scheduler] = None,
        telemetry=None,
    ) -> None:
        super().__init__(
            bus,
            AgentSpec(
                id="control-loop",
                role="coordinator",
                capabilities={"decision", "orchestration"},
                subscriptions=["loop/recheck", "agent/incident", "harness/abort"],
            ),
        )
        self.ctx = ctx
        self.decision = decision
        self.termination = termination
        self.vote_timeout = vote_timeout
        self.recover_timeout = recover_timeout
        self.check_timeout = check_timeout
        self.recheck_limit = recheck_limit
        self.combine = combine or self._default_combine
        # 节奏/退避/重试预算引擎。默认值让循环保持安全：1 秒的基础间隔避免了
        # 忙等待自旋，自适应公式会为较慢的目标自动拉长冷却时间。
        self.scheduler = scheduler or Scheduler()
        self.telemetry = telemetry

        self._verdict: Optional[Verdict] = None
        self._has_critical = False
        self._incidents: List[Dict[str, Any]] = []
        self._recheck_pending = 0
        self._round_no = 0
        self._stop = False
        self._log = logging.getLogger("stability_harness_loop_multiagent.loop")

    # ---- 权威裁决结果 ------------------------------------------------
    @property
    def verdict(self) -> Optional[Verdict]:
        return self._verdict

    # ---- 主循环 ------------------------------------------------------
    async def run(self) -> None:
        while not self._stop:
            halt, reason = self.termination.should_halt(self.ctx.snapshot())
            if halt:
                self._halt(reason)
                break
            recover_time = await self._run_round()
            if self.ctx.aborted:
                break
            # 使用调度器为循环进入下一轮定速。较慢的目标（较大的 recover_time）
            # 会自动获得更长的冷却时间；较快的目标则保持紧凑。这是循环唯一的
            # 轮间等待，因此一个卡住的调度器无法让循环死锁。
            interval = self.scheduler.interval(recover_time)
            if interval and interval > 0:
                await asyncio.sleep(interval)

    # ---- 单次迭代 ----------------------------------------------------
    async def _run_round(self) -> Optional[float]:
        self._round_no += 1
        if self.telemetry:
            self.telemetry.metric("loop.round", self._round_no)

        # 在发布 loop/tick 之前同步订阅，这样工作者发送即忘的 target/* 发布
        # 绝不会被遗漏。
        recover_time: Optional[float] = None
        rec_start = time.time()

        def _on_recover(msg: Any) -> None:
            nonlocal recover_time
            if recover_time is None and isinstance(msg, dict) and msg.get("recovered"):
                recover_time = time.time() - rec_start

        rec_unsub, rec_buf = self._start_collect("target/recovered", on_first=_on_recover)
        chk_unsub, chk_buf = self._start_collect("target/checked")
        self.bus.publish("loop/tick", {"round": self._round_no})
        await asyncio.sleep(max(self.recover_timeout, self.check_timeout))
        rec_unsub()
        chk_unsub()
        recovered_list = rec_buf
        facts_list = chk_buf

        facts = self._merge_facts(facts_list, recovered_list)
        votes, risk, critical, verdict = [], NEUTRAL_RISK, False, None
        try:
            votes = await self._collect_votes()
            risk = self.combine(votes)
            critical = self._has_critical
            verdict = self.decision.decide(facts, risk, critical)
        except Exception:  # noqa: BLE001
            # 保守回退：绝不采用乐观的通过。投票超时 / 全部弃权已经通过 combine
            # 得到 NEUTRAL_RISK=50；这里我们对整条 decide 路径做保护并回退到
            # warn(60)。
            self._log.exception("决策错误 -> 保守 warn(60)")
            try:
                verdict = self.decision.decide(
                    facts, NEUTRAL_RISK, self._has_critical, error=True
                )
                risk = CONSERVATIVE_RISK
            except Exception:  # noqa: BLE001
                verdict = Verdict(
                    "warn", risk_score=CONSERVATIVE_RISK,
                    critical=self._has_critical,
                    reason="决策错误 -> 保守 warn(60)",
                )
                risk = CONSERVATIVE_RISK
        self._verdict = verdict

        record = RoundRecord(
            round_no=self._round_no,
            facts=facts,
            verdict=verdict.decision,
            risk_score=risk,
            critical=critical,
            timestamp=time.time(),
        )
        self.ctx.append_round(record)

        self.bus.publish(
            "loop/done",
            {
                "round": self._round_no,
                "verdict": verdict.decision,
                "risk": risk,
                "critical": critical,
                "facts": facts,
                "recover_time": recover_time,
            },
        )
        if self.telemetry:
            self.telemetry.metric(
                "loop.verdict",
                1.0,
                decision=verdict.decision,
                risk=risk,
                round=self._round_no,
            )

        # recheck（有界）
        if verdict.decision == "recheck" and self._recheck_pending < self.recheck_limit:
            self._recheck_pending += 1
            self.bus.publish("loop/recheck", {"round": self._round_no})
        else:
            self._recheck_pending = 0

        # 对其他人（绝不自己）提出的事件进行 ack
        for inc in self._incidents:
            self.bus.publish(
                "agent/incident/ack",
                {"req_id": inc.get("req_id"), "incident": inc},
            )
        self._incidents.clear()
        self._has_critical = False
        return recover_time

    # ---- 入站事件 ----------------------------------------------------
    async def handle(self, topic: str, message: Any) -> None:
        if topic == "agent/incident":
            sev = (message or {}).get("severity", "warn")
            if sev == "critical":
                self._has_critical = True
            self._incidents.append(message or {})
        elif topic == "harness/abort":
            reason = (message or {}).get("reason", "watchdog abort")
            self._halt(reason)
        elif topic == "loop/recheck":
            pass  # recheck 由 _run_round 内部驱动

    # ---- 辅助方法 ----------------------------------------------------
    def _halt(self, reason: str) -> None:
        self._stop = True
        self.ctx.mark_aborted(reason)
        self.bus.publish("loop/abort", {"reason": reason})

    def _start_collect(self, reply_topic: str, on_first=None):
        """同步订阅一个收集器；返回 (unsub, buffer)。

        ``on_first``（可选的可调用对象(msg)）会在首条匹配消息到达时被调用一次
        —— 用于对目标恢复打时间戳，而不将收集器缓冲区与计时逻辑耦合。
        """
        buf: List[Any] = []
        seen = {"first": False}

        def handler(_t, msg):
            buf.append(msg)
            if on_first is not None and not seen["first"]:
                seen["first"] = True
                on_first(msg)

        unsub = self.bus.subscribe(reply_topic, handler)
        return unsub, buf

    async def _gather(self, reply_topic: str, timeout: float) -> List[Any]:
        unsub, buf = self._start_collect(reply_topic)
        try:
            await asyncio.sleep(timeout)
        finally:
            unsub()
        return buf

    async def _collect_votes(self) -> List[Dict[str, Any]]:
        unsub, replies = self._start_collect("agent/vote/reply")
        self.bus.publish("loop/vote/request", {"round": self._round_no})
        try:
            await asyncio.sleep(self.vote_timeout)
        finally:
            unsub()
        return [m for m in replies if isinstance(m, dict)]

    def _merge_facts(self, facts_list, recovered_list) -> Dict[str, Any]:
        facts: Dict[str, Any] = {}
        for msg in facts_list:
            if isinstance(msg, dict):
                facts.update(msg.get("facts", {}) or {})
        if not facts:
            facts["checks_received"] = False  # 没有工作者检查 -> fail
        # 恢复情况：任意 False（或无回复）=> 未恢复
        if recovered_list:
            recovered = all(
                (m.get("recovered") if isinstance(m, dict) else bool(m))
                for m in recovered_list
            )
        else:
            recovered = False
        facts["recovered"] = recovered
        return facts

    @staticmethod
    def _default_combine(votes: List[Dict[str, Any]]) -> float:
        # 快速路径：任意 risk >= 90 立即胜出
        for v in votes:
            if v.get("risk", 0) >= 90:
                return float(v["risk"])
        num = 0.0
        den = 0.0
        for v in votes:
            risk = float(v.get("risk", 50.0))
            conf = float(v.get("confidence", 0.0))
            w = float(v.get("weight", 1.0))
            if conf <= 0:  # 弃权
                continue
            num += risk * w * conf
            den += w * conf
        if den == 0:
            return 50.0  # 全部弃权时的中性默认值
        return num / den


@dataclass
class RunConfig:
    """声明式的控制循环运行参数（通用词汇表）。

    将高层旋钮映射到 ``TerminationPolicy`` + ``ControlLoop`` 的超时。
    不内置任何具体场景 —— 由调用方提供工作者 / 顾问 / 观察者 / 适配器。
    ``max_duration=0``（默认）会禁用“时长停止”；``fail_threshold=0`` 会禁用
    “失败停止”；二者都与 ``max_rounds`` 以 OR 方式组合（只要 > 0 就始终生效）。
    """

    max_rounds: int = 10
    max_duration: float = 0.0
    fail_threshold: int = 0
    fail_consecutive: int = 0
    vote_timeout: float = 0.5
    recover_timeout: float = 1.0
    check_timeout: float = 1.0
    recheck_limit: int = 1

    def build_termination(self) -> TerminationPolicy:
        conds: List[StopCondition] = []
        if self.max_rounds:
            conds.append(CountStop(self.max_rounds))
        if self.max_duration:
            conds.append(DurationStop(self.max_duration))
        if self.fail_threshold or self.fail_consecutive:
            conds.append(
                FailThresholdStop(
                    cumulative=self.fail_threshold,
                    consecutive=self.fail_consecutive,
                )
            )
        return TerminationPolicy(conds)


__all__ = ["ControlLoop", "RunConfig"]
