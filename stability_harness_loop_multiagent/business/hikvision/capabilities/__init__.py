"""能力原子包 —— actions + probes + preconditions。

工厂函数 create_action/create_probe/create_precondition 按 type 路由到具体实现。
Task 4a.1 定义骨架,Task 4a.2 起逐步填充实现。
"""
from typing import Any

from .actions.base import ActionBase, ActionResult
from .actions.sleep import SleepAction
from .actions.noop import NoopAction
from .probes.base import ProbeBase
from .preconditions.base import PreconditionBase

__all__ = [
    "ActionBase", "ActionResult", "ProbeBase", "PreconditionBase",
    "SleepAction", "NoopAction",
    "create_action", "create_probe", "create_precondition",
]

# Action 类型注册表:type -> class
_ACTION_REGISTRY = {
    "sleep": SleepAction,
    "noop": NoopAction,
}


def create_action(spec: Any) -> ActionBase:
    """根据 ActionSpec.type 路由到具体 Action 实现。

    Args:
        spec: ActionSpec 实例(含 .type 和 .params)

    Returns:
        ActionBase 实现实例

    Raises:
        NotImplementedError: 未注册的 type
    """
    cls = _ACTION_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Action type {spec.type} 暂未实现")
    return cls(**(spec.params or {}))


def create_probe(spec: Any) -> ProbeBase:
    """根据 ProbeSpec 创建 Probe 实例(暂未实现,Task 4a.3 起逐步填充)。"""
    raise NotImplementedError(f"Probe type {getattr(spec, 'type', '?')} 暂未实现")


def create_precondition(spec: Any) -> PreconditionBase:
    """根据 PreconditionSpec 创建 Precondition 实例(暂未实现,Task 4a.6 填充)。"""
    raise NotImplementedError(f"Precondition type {getattr(spec, 'type', '?')} 暂未实现")
