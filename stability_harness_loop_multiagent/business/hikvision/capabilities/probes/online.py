# business/hikvision/capabilities/probes/online.py
"""OnlineProbe —— 单设备在线状态断言(FieldProbe 的特化)。

预设 field=AcsWorkStatus.doorOnlineStatus[0] + expect_equals=1,
na_if_absent=AcsWorkStatus.doorOnlineStatus(字段缺失视为 NA 不强制 fail)。
"""
from typing import Any

from .field import FieldProbe


class OnlineProbe(FieldProbe):
    """单设备在线状态 Probe。

    Args:
        expect_equals: 期望的 doorOnlineStatus 值(默认 1=online)
        na_if_absent: 存在性字段路径(默认 AcsWorkStatus.doorOnlineStatus)
    """

    def __init__(self, expect_equals: Any = 1,
                 na_if_absent: str | None = "AcsWorkStatus.doorOnlineStatus",
                 **_: Any) -> None:
        super().__init__(
            field="AcsWorkStatus.doorOnlineStatus[0]",
            expect_equals=expect_equals,
            na_if_absent=na_if_absent,
        )


__all__ = ["OnlineProbe"]
