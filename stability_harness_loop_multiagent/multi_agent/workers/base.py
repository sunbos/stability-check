"""WorkerAgent — execution role of the MAS.

Performs domain operations through a TargetAdapter. Reacts to ``loop/tick``,
then publishes ``target/acted``, ``target/recovered``, ``target/checked`` and
``agent/<role>/done``. Subclass and override ``do_work`` / ``recover`` / ``check``.
"""

import asyncio
import time

from ...harness.agent import Agent, AgentSpec
from ...harness.bus import EventBus
from ..adapter import TargetAdapter


class WorkerAgent(Agent):
    def __init__(self, bus: EventBus, spec: AgentSpec, adapter: TargetAdapter) -> None:
        if "loop/tick" not in spec.subscriptions:
            spec.subscriptions.append("loop/tick")  # workers act on ticks
        super().__init__(bus, spec)
        self.adapter = adapter

    async def handle(self, topic: str, message) -> None:
        if topic == "loop/tick":
            await self.act(message or {})

    async def act(self, tick: dict) -> None:
        """Default pipeline: act -> recover -> check. Override for specifics."""
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
        """Override: invoke adapter.act(operation). Return anything (logged)."""
        return self.adapter.act(tick.get("operation"))

    async def recover(self, tick: dict) -> bool:
        """Override: poll adapter.observe() until stable; default True."""
        return True

    def check(self, tick: dict) -> dict:
        """Override: return {fact_name: bool, ...} for the DecisionAuthority."""
        return {}


__all__ = ["WorkerAgent"]
