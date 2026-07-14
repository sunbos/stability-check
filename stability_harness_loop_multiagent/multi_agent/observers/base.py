"""ObserverAgent — reporting/notification role of the MAS.

Consumes events and reports/notifies. Never decides. Subscribe to event topics
in the spec and implement ``on_event``.
"""

from ...harness.agent import Agent, AgentSpec
from ...harness.bus import EventBus


class ObserverAgent(Agent):
    def __init__(self, bus: EventBus, spec: AgentSpec) -> None:
        super().__init__(bus, spec)

    async def handle(self, topic: str, message) -> None:
        self.on_event(topic, message)

    def on_event(self, topic: str, message) -> None:
        """Override: record or notify. Never alters loop state/decisions."""
        return None


__all__ = ["ObserverAgent"]
