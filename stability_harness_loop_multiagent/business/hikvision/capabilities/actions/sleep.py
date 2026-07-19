"""SleepAction —— 等待指定秒数(组合用例的时序控制)。

用于在 stress 阶段引入显式时序间隔(例如 reboot 后等待设备冷却)。
本 Action 不依赖 ctx,纯本地 sleep,不与设备交互。
"""
import time
from typing import Any

from .base import ActionResult


class SleepAction:
    """sleep Action:阻塞指定秒数后返回 ok=True。

    Args:
        seconds: 等待秒数(默认 1.0)
    """

    def __init__(self, seconds: float = 1.0, **_: Any) -> None:
        self._seconds = seconds

    def execute(self, ctx: Any) -> ActionResult:
        """执行 sleep,返回 ActionResult。"""
        time.sleep(self._seconds)
        return ActionResult(ok=True, data={"slept": self._seconds})


__all__ = ["SleepAction"]
