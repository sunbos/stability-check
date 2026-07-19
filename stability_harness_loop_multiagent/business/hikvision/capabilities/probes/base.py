"""Probe 基类 —— 探测能力协议(读取状态,不改变)。

Probe 是 ScenarioWorker 在 check 阶段执行的断言单元,如 field / online / event_chain。
每个 Probe 接收 snapshot(设备状态快照或 ctx),返回事实字典(facts dict)。
事实字典中的 key 即 fact name,value 即 fact value(布尔)。
"""
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ProbeBase(Protocol):
    """Probe 协议:check(snapshot) -> dict[str, bool](事实字典)。

    snapshot 可以是:
    - 设备状态快照 dict(如 get_work_status() 返回值)—— field/online/count probe
    - ctx(SimpleNamespace,含 events)—— event_chain probe
    """
    def check(self, snapshot: Any) -> dict[str, bool]:
        ...


__all__ = ["ProbeBase"]
