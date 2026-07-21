"""FieldProbe —— 字段值断言(从 scenario_schema.py 迁出)。

支持:
- expect_equals: 等于期望值(类型弱转换,复用 scenario_schema.compare_probe)
- expect_in: 值在列表中
- na_if_absent: 存在性字段缺失 → NA(不强制失败,只标记)
"""
from dataclasses import dataclass
from typing import Any

from ...scenario_schema import resolve_field, compare_probe, _SENTINEL


@dataclass
class _ProbeParams:
    """compare_probe 鸭子类型适配器(避免触碰 scenario_schema 私有 _coerce_eq)。"""
    expect_equals: Any = None
    expect_in: list[Any] | None = None


class FieldProbe:
    """字段值断言 Probe。

    Args:
        field: 字段路径(如 AcsWorkStatus.doorOnlineStatus[0])
        expect_equals: 期望值(等于则 probe_ok=True)
        expect_in: 期望值列表(值在其中则 probe_ok=True)
        na_if_absent: 若该路径字段缺失,返回 probe_na=True(不强制 fail)
        endpoint: 保留字段(用于场景化适配器调用哪个端点取快照,本 Probe 不直接用)
    """

    def __init__(self, field: str, expect_equals: Any = None,
                 expect_in: list[Any] | None = None,
                 na_if_absent: str | None = None,
                 endpoint: str | None = None, **_: Any) -> None:
        self._field = field
        self._expect_equals = expect_equals
        self._expect_in = expect_in
        self._na_if_absent = na_if_absent
        self._endpoint = endpoint

    def check(self, snapshot: Any) -> dict[str, Any]:
        """对 snapshot 执行字段断言,返回 fact 字典。

        除了 ``probe_ok`` 外,还附带 ``probe_value``(实际字段值),
        供观察者(ScenarioLiveReporter)在逐轮输出里展示真实状态值。
        """
        # 先检查 na_if_absent:若该字段缺失,标记 NA 不强制 fail
        if self._na_if_absent:
            if resolve_field(snapshot, self._na_if_absent, _SENTINEL) is _SENTINEL:
                return {"probe_ok": False, "probe_na": True, "probe_value": None}
        # 解析 field:缺失则 probe_ok=False
        value = resolve_field(snapshot, self._field, _SENTINEL)
        if value is _SENTINEL:
            return {"probe_ok": False, "probe_value": None}
        # compare_probe 签名是 (value, probe_obj),probe_obj 需暴露 expect_equals/expect_in
        probe_obj = _ProbeParams(expect_equals=self._expect_equals, expect_in=self._expect_in)
        ok = compare_probe(value, probe_obj)
        return {"probe_ok": bool(ok), "probe_value": value}


__all__ = ["FieldProbe"]
