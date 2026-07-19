"""能力原子包 —— actions + probes + preconditions。

工厂函数 create_action/create_probe/create_precondition 按 type 路由到具体实现。
"""
from typing import Any

from .actions.base import ActionBase, ActionResult
from .actions.sleep import SleepAction
from .actions.noop import NoopAction
from .actions.reboot import RebootAction
from .actions.upgrade import UpgradeAction
from .actions.remote_open import RemoteOpenAction
from .actions.dispatch import DispatchAction
from .actions.switch_serial import SwitchSerialAction
from .actions.query_events import QueryEventsAction
from .probes.base import ProbeBase
from .probes.field import FieldProbe
from .probes.online import OnlineProbe
from .probes.count import CountProbe
from .probes.event_chain import EventChainProbe
from .preconditions.base import PreconditionBase
from .preconditions.device_online import DeviceOnlinePrecondition
from .preconditions.serial_mode import SerialModePrecondition
from .preconditions.baseline_record import BaselineRecordPrecondition

__all__ = [
    "ActionBase", "ActionResult", "ProbeBase", "PreconditionBase",
    "SleepAction", "NoopAction", "RebootAction", "UpgradeAction",
    "RemoteOpenAction", "DispatchAction", "SwitchSerialAction",
    "QueryEventsAction",
    "FieldProbe", "OnlineProbe", "CountProbe", "EventChainProbe",
    "DeviceOnlinePrecondition", "SerialModePrecondition", "BaselineRecordPrecondition",
    "create_action", "create_probe", "create_precondition",
]

# Action 类型注册表:type -> class
_ACTION_REGISTRY = {
    "sleep": SleepAction,
    "noop": NoopAction,
    "reboot": RebootAction,
    "upgrade": UpgradeAction,
    "remote_open": RemoteOpenAction,
    "dispatch": DispatchAction,
    "switch_serial": SwitchSerialAction,
    "query_events": QueryEventsAction,
}

# Probe 类型注册表:type -> class
_PROBE_REGISTRY = {
    "field": FieldProbe,
    "online": OnlineProbe,
    "count": CountProbe,
    "event_chain": EventChainProbe,
}

# Precondition 类型注册表:type -> class
_PRECONDITION_REGISTRY = {
    "device_online": DeviceOnlinePrecondition,
    "serial_mode": SerialModePrecondition,
    "baseline_record": BaselineRecordPrecondition,
}


def create_action(spec: Any) -> ActionBase:
    """根据 ActionSpec.type 路由到具体 Action 实现。"""
    cls = _ACTION_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Action type {spec.type} 暂未实现")
    return cls(**(spec.params or {}))


def create_probe(spec: Any) -> ProbeBase:
    """根据 ProbeSpec.type 路由到具体 Probe 实现。"""
    cls = _PROBE_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Probe type {spec.type} 暂未实现")
    return cls(**(spec.params or {}))


def create_precondition(spec: Any) -> PreconditionBase:
    """根据 PreconditionSpec.type 路由到具体 Precondition 实现。"""
    cls = _PRECONDITION_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Precondition type {spec.type} 暂未实现")
    return cls(**(spec.params or {}))
