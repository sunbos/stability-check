# business/hikvision/capabilities/preconditions/device_online.py
"""DeviceOnlinePrecondition —— 用例开始前验证设备在线(从 worker.py 迁出)。

通过 client.get_time() 是否成功返回判断设备在线。
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


class DeviceOnlinePrecondition:
    """设备在线前置检查。

    setup() 成功 = 设备在线可通信;失败 = 设备不可达,ScenarioWorker 应中止用例。
    """

    def __init__(self, **_: Any) -> None:
        pass

    def setup(self, ctx: Any) -> bool:
        """检查 ctx.client.get_time() 是否成功。"""
        try:
            t = ctx.client.get_time()
            return bool(t and t.get("Time"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("DeviceOnlinePrecondition 失败: %s", exc)
            return False


__all__ = ["DeviceOnlinePrecondition"]
