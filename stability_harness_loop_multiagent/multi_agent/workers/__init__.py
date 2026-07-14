"""MAS worker roles (execution agents driving a TargetAdapter)."""

from .base import WorkerAgent
from .example import ExampleWorkerAgent

__all__ = ["WorkerAgent", "ExampleWorkerAgent"]
