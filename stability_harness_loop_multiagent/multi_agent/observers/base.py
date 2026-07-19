"""ObserverAgent —— MAS 中的上报/通知角色。

消费事件并上报/通知。绝不裁决。在 spec 中订阅事件主题并实现 ``on_event``。
"""

from ...core.agent import Agent, AgentSpec
from ...core.bus import EventBus


class ObserverAgent(Agent):
    def __init__(self, bus: EventBus, spec: AgentSpec) -> None:
        super().__init__(bus, spec)

    async def handle(self, topic: str, message) -> None:
        self.on_event(topic, message)

    def on_event(self, topic: str, message) -> None:
        """覆盖：记录或通知。绝不改动循环状态/决策。"""
        return None


__all__ = ["ObserverAgent"]
