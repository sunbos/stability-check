"""Precondition 基类 —— 前置条件协议(用例开始前检查/设置)。

Precondition 是 ScenarioWorker 在 pre_loop_setup 阶段执行的前置单元,
如 device_online / serial_mode / baseline_record。
每个 Precondition 接收 ctx,返回 bool(True=通过,False=中止用例)。
"""
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class PreconditionBase(Protocol):
    """Precondition 协议:setup(ctx) -> bool(True=通过,False=中止)。

    ctx 是 SimpleNamespace,至少包含:
    - client: HikvisionClient 实例
    - events: list[dict](初始为空)
    - baseline: dict(初始为空,BaselineRecordPrecondition 会填充)
    """
    def setup(self, ctx: Any) -> bool:
        ...


__all__ = ["PreconditionBase"]
