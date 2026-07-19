# business/hikvision/capabilities/actions/switch_serial.py
"""SwitchSerialAction —— 切换串口外设模式(串口外设用例)。

client.get_serial_config 返回扁平 dict(如 {"id":"1","mode":"externMode",...}),
set_serial_config 接受扁平 dict。本 Action 先 GET 取完整配置,改 mode,再 PUT 回写。
"""
import logging
from typing import Any

from .base import ActionResult

logger = logging.getLogger(__name__)


class SwitchSerialAction:
    """切换串口模式 Action。

    Args:
        port: 串口号(默认 1)
        mode: 目标模式(默认 externMode,可选 readerMode/accessControlHost 等)
    """

    def __init__(self, port: int = 1, mode: str = "externMode", **_: Any) -> None:
        self._port = port
        self._mode = mode

    def execute(self, ctx: Any) -> ActionResult:
        """切换串口模式。"""
        try:
            cfg = ctx.client.get_serial_config(port=self._port)
            # get_serial_config 返回扁平 dict(已剥离 SerialPort 外壳)
            cfg["mode"] = self._mode
            ctx.client.set_serial_config(port=self._port, fields=cfg)
            return ActionResult(ok=True, data={"port": self._port, "mode": self._mode})
        except Exception as exc:  # noqa: BLE001
            logger.warning("SwitchSerialAction 失败: %s", exc)
            return ActionResult(ok=False, error=str(exc))


__all__ = ["SwitchSerialAction"]
