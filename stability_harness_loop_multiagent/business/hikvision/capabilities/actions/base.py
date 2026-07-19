"""Action 基类 —— 操作能力协议(改变设备状态)。

Action 是 ScenarioWorker 在 do_work 阶段执行的操作单元,如 reboot / remote_open / sleep。
每个 Action 接收 ctx(SimpleNamespace,含 client/events/baseline 等),返回 ActionResult。
"""
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class ActionResult:
    """Action 执行结果。

    ok=True 表示成功,ok=False 表示失败(附 error 信息)。
    data 携带额外产出(如 reboot 的 target、query_events 的 count)。
    """
    ok: bool = True
    error: str = ""
    data: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class ActionBase(Protocol):
    """Action 协议:execute(ctx) -> ActionResult。

    ctx 是 SimpleNamespace,至少包含:
    - client: HikvisionClient 实例
    - events: list[dict](QueryEventsAction 产出,EventChainProbe 消费)
    - baseline: dict(BaselineRecordPrecondition 产出)
    """
    def execute(self, ctx: Any) -> ActionResult:
        ...


__all__ = ["ActionResult", "ActionBase"]
