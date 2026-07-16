"""HikvisionAdvisor: parses BURNIN_STRATEGY -> plan, publishes hikvision/plan.

LLM parse is injected as a callable for deterministic tests. Advisor only
votes / raises incidents; it NEVER executes operations or decides verdict.
"""

from typing import Callable, Dict

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from ...multi_agent.advisors.base import AdvisorAgent


class HikvisionAdvisor(AdvisorAgent):
    def __init__(self, bus: EventBus, spec: AgentSpec,
                 instruction: str,
                 llm_parse: Callable[[str], Dict],
                 *, weight: float = 1.0) -> None:
        super().__init__(bus, spec, weight=weight)
        self._instruction = instruction
        self._llm_parse = llm_parse
        self._plan: Dict = {}

    async def start(self) -> None:
        await super().start()
        # Parse instruction and publish plan once at startup
        self._plan = self._llm_parse(self._instruction)
        self.publish("hikvision/plan", self._plan)

    def on_round(self, round_info: dict) -> None:
        super().on_round(round_info)
        # Track risk trend in private window (inherited)

    def vote(self) -> tuple:
        # Simple trend: if any recent round failed, raise risk
        window = self._private_window
        if window and any(isinstance(r, (int, float)) and r >= 60 for r in window[-10:]):
            return (75.0, 0.8)
        return (30.0, 0.7)


__all__ = ["HikvisionAdvisor"]
