"""能力原子包 —— actions + probes + preconditions。

工厂函数 create_action/create_probe/create_precondition 按 type 路由到具体实现。
Task 4a.1 只定义骨架,Task 4a.2 起逐步填充实现。
"""
from typing import Any

from .actions.base import ActionBase, ActionResult
from .probes.base import ProbeBase
from .preconditions.base import PreconditionBase

__all__ = [
    "ActionBase", "ActionResult", "ProbeBase", "PreconditionBase",
    "create_action", "create_probe", "create_precondition",
]


def create_action(spec: Any) -> ActionBase:
    """根据 ActionSpec 创建 Action 实例(暂未实现,Task 4a.2 起逐步填充)。"""
    raise NotImplementedError(f"Action type {getattr(spec, 'type', '?')} 暂未实现")


def create_probe(spec: Any) -> ProbeBase:
    """根据 ProbeSpec 创建 Probe 实例(暂未实现,Task 4a.3 起逐步填充)。"""
    raise NotImplementedError(f"Probe type {getattr(spec, 'type', '?')} 暂未实现")


def create_precondition(spec: Any) -> PreconditionBase:
    """根据 PreconditionSpec 创建 Precondition 实例(暂未实现,Task 4a.6 填充)。"""
    raise NotImplementedError(f"Precondition type {getattr(spec, 'type', '?')} 暂未实现")
