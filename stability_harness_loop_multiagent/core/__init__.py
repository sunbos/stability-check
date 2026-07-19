"""core —— 跨引擎共享的契约内核（事件总线 + 智能体基类）。

本包只放真正跨引擎共享的契约，使 harness / loop / multi_agent 三引擎对等、
边界可在模块层强制。不包含任何治理 / 运行时实现。
"""

from .bus import EventBus
from .agent import Agent, AgentSpec
from .voting import combine_votes

__all__ = ["EventBus", "Agent", "AgentSpec", "combine_votes"]
