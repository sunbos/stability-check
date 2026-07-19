# business/hikvision/capabilities/preconditions/serial_mode.py
"""SerialModePrecondition —— 用例开始前设置串口模式。

复用 SwitchSerialAction.execute 执行实际切换,只把 ActionResult.ok 转成 bool。
"""
from typing import Any

from ..actions.switch_serial import SwitchSerialAction


class SerialModePrecondition:
    """串口模式前置设置。

    Args:
        mode: 目标模式(默认 externMode)
        port: 串口号(默认 1)
    """

    def __init__(self, mode: str = "externMode", port: int = 1, **_: Any) -> None:
        self._mode = mode
        self._port = port

    def setup(self, ctx: Any) -> bool:
        """执行串口模式切换,返回是否成功。"""
        action = SwitchSerialAction(port=self._port, mode=self._mode)
        result = action.execute(ctx)
        return result.ok


__all__ = ["SerialModePrecondition"]
