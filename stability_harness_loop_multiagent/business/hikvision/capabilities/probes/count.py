# business/hikvision/capabilities/probes/count.py
"""CountProbe —— 数值字段比对(子设备在线数量等)。

支持 expect_equals / expect_in 两种断言,复用 scenario_schema.compare_probe
做弱类型转换(如 "1" vs 1)。
"""
from dataclasses import dataclass
from typing import Any

from ...scenario_schema import resolve_field, compare_probe, _SENTINEL


@dataclass
class _ProbeParams:
    """compare_probe 鸭子类型适配器。"""
    expect_equals: Any = None
    expect_in: list[Any] | None = None


class CountProbe:
    """数值字段比对 Probe。

    Args:
        field: 字段路径(如 AcsWorkStatus.childDeviceOnlineCount)
        expect_equals: 期望数值(等于则 probe_ok=True)
        expect_in: 期望数值列表(值在其中则 probe_ok=True)
    """

    def __init__(self, field: str,
                 expect_equals: Any = None,
                 expect_in: list[Any] | None = None,
                 **_: Any) -> None:
        self._field = field
        self._expect_equals = expect_equals
        self._expect_in = expect_in

    def check(self, snapshot: Any) -> dict[str, bool]:
        """对 snapshot 执行数值断言。"""
        value = resolve_field(snapshot, self._field, _SENTINEL)
        if value is _SENTINEL:
            return {"probe_ok": False}
        probe_obj = _ProbeParams(expect_equals=self._expect_equals,
                                  expect_in=self._expect_in)
        ok = compare_probe(value, probe_obj)
        return {"probe_ok": bool(ok)}


__all__ = ["CountProbe"]
