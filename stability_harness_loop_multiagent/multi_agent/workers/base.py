"""WorkerAgent —— MAS 中的执行角色。

通过 TargetAdapter 执行领域操作。响应 ``loop/tick``，随后发布 ``target/acted``、
``target/recovered``、``target/checked`` 以及 ``agent/<role>/done``。子类化并
覆盖 ``do_work`` / ``recover`` / ``check``。
"""

import asyncio
import time

from ...harness.agent import Agent, AgentSpec
from ...harness.bus import EventBus
from ..adapter import TargetAdapter


class WorkerAgent(Agent):
    def __init__(self, bus: EventBus, spec: AgentSpec, adapter: TargetAdapter) -> None:
        if "loop/tick" not in spec.subscriptions:
            spec.subscriptions.append("loop/tick")  # 工作者在 tick 上行动
        super().__init__(bus, spec)
        self.adapter = adapter

    async def handle(self, topic: str, message) -> None:
        if topic == "loop/tick":
            await self.act(message or {})

    async def act(self, tick: dict) -> None:
        """默认流水线：act -> recover -> check。针对具体场景覆盖。"""
        result = self.do_work(tick)
        self.publish(
            "target/acted",
            {"role": self.role, "round": tick.get("round"), "result": result},
        )
        recovered = await self.recover(tick)
        self.publish(
            "target/recovered",
            {"role": self.role, "round": tick.get("round"), "recovered": recovered},
        )
        facts = self.check(tick)
        self.publish(
            "target/checked",
            {"role": self.role, "round": tick.get("round"), "facts": facts},
        )
        self.publish("agent/" + self.role + "/done", {"round": tick.get("round")})

    def do_work(self, tick: dict):
        """覆盖：调用 adapter.act(operation)。返回任意值（会被记录）。"""
        return self.adapter.act(tick.get("operation"))

    async def recover(self, tick: dict) -> bool:
        """覆盖：轮询 adapter.observe() 直到稳定；默认返回 True。"""
        return True

    def check(self, tick: dict) -> dict:
        """覆盖：为 DecisionAuthority 返回 {fact_name: bool, ...}。"""
        return {}


__all__ = ["WorkerAgent"]
