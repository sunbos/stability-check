"""TargetAdapter —— MAS 所操作对象的契约。

通用、与领域无关。一个具体场景实现该协议（例如某个设备、服务或资源适配器），
并注册驱动它的 WorkerAgent。Loop 引擎从不导入它；由各 Worker 导入。
"""

from dataclasses import dataclass, field
from typing import Any, List, Protocol, runtime_checkable


@dataclass
class Event:
    """从目标观测到的一个领域事件。"""

    kind: str
    payload: Any = None
    ts: float = field(default=0.0)


@dataclass
class Result:
    """一次 act() 操作的结果。"""

    ok: bool
    data: Any = None
    error: str = ""


@dataclass
class State:
    """目标在某一时刻的观测快照。"""

    snapshot: Any = None


@runtime_checkable
class TargetAdapter(Protocol):
    def act(self, operation: Any) -> Result:
        """在目标上执行一个操作。返回一个 Result。"""
        ...

    def observe(self) -> State:
        """观测目标当前的状态。"""
        ...

    def events(self, since: float) -> List[Event]:
        """返回在 ``since``（epoch 秒）及其之后发生的事件。"""
        ...


__all__ = ["TargetAdapter", "Event", "Result", "State"]
