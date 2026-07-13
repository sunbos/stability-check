"""ReporterAgent：总线观察者，纯消费消息做记录与汇总（仅标准库）。

设计原则
--------
* 作为 EventBus 的观察者，只订阅而不主动发起任何设备请求。
* 复用 tests/agents/report.py 的 Reporter 类做结果汇总与告警。
* 订阅 'check/event'、'check/status'、'round/done'、'coord/abort'：
  - 'check/event' / 'check/status'：累积事件/状态，必要时落日志。
  - 'round/done'：把一轮结果写入 ctx.round_history 并调用 Reporter.record。
  - 'coord/abort'：调用 Reporter.abort(reason) 并告警（预留 webhook）。

可单独运行：订阅总线后静默记录，直到被取消。

仅依赖标准库 + 同仓的 bus / agent / context / report，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

from agent import Agent

# 让本模块能 `from report import Reporter`：report.py 位于 harness 的上级 agents/。
_AGENTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "agents")
if _AGENTS_DIR not in sys.path:
    sys.path.insert(0, _AGENTS_DIR)

from report import Reporter  # noqa: E402


class ReporterAgent(Agent):
    """总线观察者：消费检查/轮次/中止消息，做记录、汇总与告警。"""

    # 订阅的主题
    TOPICS = ("check/event", "check/status", "round/done", "coord/abort")

    def __init__(self, spec, bus, ctx) -> None:
        super().__init__(spec, bus, ctx)
        # 内部汇总器（复用 tests/agents/report.py 的 Reporter）
        self.reporter = Reporter()
        # 累积的离散事件与状态，便于在轮次或最终汇总时引用
        self.events: list = []
        self.statuses: list = []

    # ------------------------------------------------------------------ #
    # 订阅注册（在 run() 内调用，便于独立运行）
    # ------------------------------------------------------------------ #
    def _subscribe_all(self) -> None:
        """订阅所有关心的主题，绑定到对应处理器。

        bus.publish 不会把主题名注入消息体，因此为每个主题绑定一个
        携带主题信息的闭包处理器，避免运行时再猜测 topic。
        """
        self.subscribe("check/event", self._on_event)
        self.subscribe("check/status", self._on_status)
        self.subscribe("round/done", self._on_round_done)
        self.subscribe("coord/abort", self._on_abort)

    # ------------------------------------------------------------------ #
    # 各类消息处理
    # ------------------------------------------------------------------ #
    async def _on_event(self, message: dict) -> None:
        """处理 check/event：累积事件并落日志。"""
        self.events.append(message)
        ts = time.strftime("%H:%M:%S", time.localtime())
        print(
            f"[{ts}] [汇报] [事件] "
            f"轮次={message.get('round_no')} "
            f"found={message.get('found')} "
            f"error={message.get('error')}"
        )
        self.ctx.append_log(f"汇报/事件: {message}")
        if message.get("error") or message.get("abnormal"):
            self.reporter.alert(f"事件异常: {message}")

    async def _on_status(self, message: dict) -> None:
        """处理 check/status：累积状态并落日志。"""
        self.statuses.append(message)
        ts = time.strftime("%H:%M:%S", time.localtime())
        print(
            f"[{ts}] [汇报] [状态] "
            f"轮次={message.get('round_no')} "
            f"changed={message.get('changed')} "
            f"diff={message.get('diff')} "
            f"error={message.get('error')}"
        )
        self.ctx.append_log(f"汇报/状态: {message}")
        if message.get("error") or message.get("abnormal"):
            self.reporter.alert(f"状态异常: {message}")

    async def _on_event_or_status(self, message: dict) -> None:
        """处理 check/event 与 check/status：累积并落日志。"""
        # 区分事件与状态（消息可能在两种主题间复用结构）
        if message.get("event") is not None or "event_type" in message:
            self.events.append(message)
            bucket = "事件"
        else:
            self.statuses.append(message)
            bucket = "状态"

        line = f"[{bucket}] {message}"
        print(f"[汇报] {line}")
        self.ctx.append_log(f"汇报/{bucket}: {message}")
        # 仅在显式标记异常时告警，不主动做任何设备请求
        if message.get("error") or message.get("abnormal"):
            self.reporter.alert(f"{bucket}异常: {message}")

    async def _on_round_done(self, message: dict) -> None:
        """处理 round/done：记录一轮结果到 ctx 与内部 Reporter。"""
        result = dict(message)
        # 把本轮累积的事件/状态作为上下文附带进轮次记录（可选，便于复盘）
        result.setdefault("events", list(self.events))
        result.setdefault("statuses", list(self.statuses))

        # 写入共享上下文（小组白板）
        self.ctx.append_round(result)
        # 写入内部 Reporter 做汇总
        self.reporter.record(result)

        ts = result.get("timestamp") or time.time()
        # 注：不在控制台重复打印轮次汇总——[拷机] 行已是核心结论，
        # [分析]/[记录员] 各有补充视角；此处仅落日志供事后复盘。
        self.ctx.append_log(
            f"汇报/轮次: 轮次={result.get('round')} "
            f"通过={result.get('passed')} 时间戳={ts}"
        )
        # 复位本轮累积，避免跨轮混淆
        self.events.clear()
        self.statuses.clear()

    async def _on_abort(self, message: dict) -> None:
        """处理 coord/abort：标记中断并告警（预留 webhook）。"""
        reason = message.get("reason", "unknown")
        self.reporter.abort(reason)
        self.ctx.append_log(f"汇报/中止: {reason}")
        # 告警（Reporter.alert 内部预留 webhook 钩子）
        self.reporter.alert(f"协调者中止: {reason}")
        ts = time.strftime("%H:%M:%S", time.localtime())
        print(f"[{ts}] [汇报] 已中止: {reason}")

    # ------------------------------------------------------------------ #
    # 业务 step（保留基类契约，但本 agent 不主动处理输入/设备请求）
    # ------------------------------------------------------------------ #
    async def step(self, message: dict) -> dict:
        """基类契约：用于被协调者串行驱动时也能记录。

        按消息内容路由到对应处理器，不主动发起任何设备请求。
        """
        if "reason" in message:
            await self._on_abort(message)
        elif "passed" in message or "round" in message:
            await self._on_round_done(message)
        elif message.get("event") is not None or "event_type" in message:
            await self._on_event(message)
        elif "status" in message or "device" in message:
            await self._on_status(message)
        else:
            await self._on_status(message)
        return self.reporter.summary()

    # ------------------------------------------------------------------ #
    # 独立主循环：订阅总线后静默记录，直到被取消
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        """独立运行：订阅全部主题，静默消费并落日志，直到被取消。"""
        self._subscribe_all()
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    # 简单的独立运行入口：构造最小 spec/bus/ctx 后静默记录。
    from bus import EventBus
    from context import RunContext
    from agent import AgentSpec

    async def _main():
        bus = EventBus()
        ctx = RunContext()
        spec = AgentSpec(
            name="reporter",
            role="reporter",
            endpoint="",
            user="",
            password="",
            host="",
        )
        agent = ReporterAgent(spec, bus, ctx)
        # 先同步注册订阅（run() 内也会再注册，subscribe 幂等），再发布消息
        agent._subscribe_all()
        run_task = asyncio.create_task(agent.run())
        # 让出一次控制权，确保 run() 已初始化 _stop 事件后再发布/停止
        await asyncio.sleep(0)

        # 演示：发布几条消息让 reporter 消费
        await bus.publish("check/event", {"event": "device_online", "device": "door1"})
        await bus.publish("check/status", {"device": "door1", "status": "ok"})
        await bus.publish(
            "round/done",
            {"round": 1, "passed": True, "recover_time": 2.5, "timestamp": time.time()},
        )
        await bus.publish("coord/abort", {"reason": "manual stop for demo"})

        print("汇总:", agent.reporter.summary())
        agent.stop()
        await run_task

    asyncio.run(_main())
