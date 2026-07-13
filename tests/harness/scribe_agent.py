"""ScribeAgent：记录员（仅使用标准库）。

职责
----
* 作为总线观察者，把各 agent 的关键消息整理成**面向人**的叙事（narrative），
  并维护私有 timeline 累积每轮记录。
* 订阅：round/done、incident/raise、analyst/decision、analyst/report、coord/abort。
* summary() 从私有 timeline 计算，不读 ctx.round_history。

设计说明
--------
Scribe 不发起任何设备请求，也不做决策，只"记录"。它把总线上的分散信号
连成一条连贯的时间线。Phase 3 起，Scribe 维护私有 timeline 和 _aborted 标志，
不再依赖 ctx 的可变状态，符合"私有状态"原则。

仅依赖标准库 + 同仓 bus / agent / context，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from agent import Agent  # noqa: E402


class ScribeAgent(Agent):
    """记录员：私有 timeline 累积 + 叙事，summary() 不读 ctx。"""

    SUMMARY_TOPIC = "scribe/summary"
    TOPICS = (
        "round/done",
        "incident/raise",
        "analyst/decision",
        "analyst/report",
        "coord/abort",
    )

    def __init__(self, spec, bus, ctx, cfg=None) -> None:
        super().__init__(spec, bus, ctx)
        self.cfg = cfg if cfg is not None else getattr(ctx, "cfg", None)
        self.narrative: list = []
        self.timeline: list = []          # private round records
        self._aborted: bool = False       # private abort flag
        self._abort_reason: str = ""

    # ------------------------------------------------------------------ #
    # 叙事记录
    # ------------------------------------------------------------------ #
    def _line(self, text: str) -> None:
        ts = time.strftime("%H:%M:%S", time.localtime())
        entry = f"[{ts}] {text}"
        self.narrative.append(entry)
        self.ctx.append_log(f"记录员: {text}")
        print(f"[{ts}] [记录员] {text}")

    async def _on_round_done(self, m: dict) -> None:
        record = dict(m)
        self.timeline.append(record)
        r = m.get("round")
        tag = "通过" if m.get("passed") else "失败"
        rt = m.get("recover_time")
        rt_str = f"{rt:.1f}秒" if isinstance(rt, (int, float)) else "NA"
        self._line(
            f"第 {r} 轮 {tag}：事件={m.get('found')} 状态偏移={m.get('changed')} "
            f"恢复耗时={rt_str}"
        )

    async def _on_incident(self, m: dict) -> None:
        inc = m.get("incident", m)
        self._line(f"事故：{inc}")

    async def _on_decision(self, m: dict) -> None:
        cont = m.get("continue")
        src = m.get("source")
        self._line(
            f"分析决策(来源={src})：{'继续' if cont else '停止'} —— {m.get('reason')}"
        )

    async def _on_report(self, m: dict) -> None:
        if m.get("failed"):
            self._line(
                f"稳定性评分={m.get('stability_score')} 失败={m.get('failed')}/"
                f"{m.get('total')} 建议={m.get('recommendation')}"
            )

    async def _on_abort(self, m: dict) -> None:
        self._aborted = True
        self._abort_reason = m.get("reason", "unknown")
        self._line(f"拷机中止：{m.get('reason')}")
        await self._emit_summary()

    async def _emit_summary(self) -> None:
        summary = self.summary()
        await self.publish(self.SUMMARY_TOPIC, summary)

    # ------------------------------------------------------------------ #
    # 摘要（从私有 timeline 计算，不读 ctx）
    # ------------------------------------------------------------------ #
    def summary(self) -> dict:
        """从私有 timeline 计算汇总（不读 ctx）。"""
        total = len(self.timeline)
        passed = sum(1 for r in self.timeline if r.get("passed"))
        failed = total - passed
        recover_times = [
            r.get("recover_time")
            for r in self.timeline
            if r.get("recover_time") is not None
        ]
        avg_recover_time = (
            sum(recover_times) / len(recover_times) if recover_times else None
        )
        max_recover_time = max(recover_times) if recover_times else None
        return {
            "narrative": list(self.narrative),
            "total": total,
            "passed": passed,
            "failed": failed,
            "aborted": self._aborted,
            "reason": self._abort_reason,
            "avg_recover_time": round(avg_recover_time, 1) if avg_recover_time is not None else None,
            "max_recover_time": max_recover_time,
            "rounds": total,
        }

    async def _on_summary_request(self, m: dict) -> None:
        await self._emit_summary()

    async def run(self) -> None:
        self.subscribe("round/done", self._on_round_done)
        self.subscribe("incident/raise", self._on_incident)
        self.subscribe("analyst/decision", self._on_decision)
        self.subscribe("analyst/report", self._on_report)
        self.subscribe("coord/abort", self._on_abort)
        self.subscribe("scribe/summary/request", self._on_summary_request)
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    from bus import EventBus
    from context import RunContext
    from agent import AgentSpec
    from config import load_config_from_env

    cfg = load_config_from_env()
    bus = EventBus()
    ctx = RunContext()
    ctx.cfg = cfg
    spec = AgentSpec("scribe", "scribe", "", cfg.user, cfg.password, cfg.host)
    agent = ScribeAgent(spec, bus, ctx, cfg=cfg)

    async def _demo():
        async def _on_summary(m):
            print("[记录员/摘要]", m)

        bus.subscribe("scribe/summary", _on_summary)
        await bus.publish(
            "round/done", {"round": 1, "passed": True, "found": True, "changed": False, "recover_time": 62.1}
        )
        await bus.publish(
            "analyst/decision", {"continue": False, "source": "rule", "reason": "断电"}
        )
        await bus.publish("coord/abort", {"reason": "power loss"})
        await bus.request("scribe/summary/request", {}, timeout=5)

    asyncio.run(_demo())
