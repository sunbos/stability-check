"""NoopAction —— 显式无操作占位(对应原 stress.type=none 路径)。

当某个 stress 阶段不需要执行任何操作时使用,保持流水线结构一致。
"""
from typing import Any

from .base import ActionResult


class NoopAction:
    """noop Action:不执行任何操作,直接返回 ok=True。"""

    def __init__(self, **_: Any) -> None:
        pass

    def execute(self, ctx: Any) -> ActionResult:
        """返回 ok=True 的空结果。"""
        return ActionResult(ok=True)


__all__ = ["NoopAction"]
