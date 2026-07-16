"""MAS 工作者角色（驱动 TargetAdapter 的执行智能体）。"""

from .base import WorkerAgent
from .example import ExampleWorkerAgent

__all__ = ["WorkerAgent", "ExampleWorkerAgent"]
