"""ScribeAgent：小组白板“书记员”（仅使用标准库）。

职责
----
* 作为总线观察者，把各 agent 的关键消息整理成**面向人**的叙事（narrative），
  写入 ctx.log 并维护自身 narrative 列表，便于事后复盘。
* 订阅：round/done、incident/raise、analyst/decision、analyst/report、coord/abort。
* 在 coord/abort 或 scribe/summary 请求时，广播一份整体摘要（scribe/summary）。

设计说明
--------
Scribe 不发起任何设备请求，也不做决策，只“记录”。它把总线上的分散信号
（重启、恢复、事件核对、状态核对、分析决策）连成一条连贯的时间线，等价于
“人不在场时，有一个书记员在实时记录拷机现场”。

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
    """书记员：把总线信号整理为连贯叙事，供复盘与通知。"""

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

    # ------------------------------------------------------------------ #
    # 叙事记录
    # ------------------------------------------------------------------ #
    def _line(self, text: str) -> None:
        ts = time.strftime("%H:%M:%S", time.localtime())
        entry = f"[{ts}] {text}"
        self.narrative.append(entry)
        self.ctx.append_log(f"scribe: {text}")
        print(f"[scribe] {text}")

    async def _on_round_done(self, m: dict) -> None:
        r = m.get("round")
        tag = "OK" if m.get("passed") else "FAIL"
        self._line(
            f"第 {r} 轮 {tag}：事件={m.get('found')} 状态偏移={m.get('changed')} "
            f"恢复耗时={m.get('recover_time')}"
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
        # 多角度分析较频繁，仅在稳定性评分下降或失败时记录，避免刷屏。
        if m.get("failed"):
            self._line(
                f"稳定性评分={m.get('stability_score')} 失败={m.get('failed')}/"
                f"{m.get('total')} 建议={m.get('recommendation')}"
            )

    async def _on_abort(self, m: dict) -> None:
        self._line(f"拷机中止：{m.get('reason')}")
        # 中止时主动产出整体摘要。
        await self._emit_summary()

    async def _emit_summary(self) -> None:
        summary = self.summary()
        await self.publish(self.SUMMARY_TOPIC, summary)

    # ------------------------------------------------------------------ #
    # 摘要
    # ------------------------------------------------------------------ #
    def summary(self) -> dict:
        """产出书记员视角的整体摘要。"""
        return {
            "narrative": list(self.narrative),
            "rounds": len(self.ctx.round_history),
            "aborted": getattr(self.ctx, "aborted", False),
        }

    # ------------------------------------------------------------------ #
    # 总线处理
    # ------------------------------------------------------------------ #
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
            print("[scribe/summary]", m)

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
