# business/hikvision/capabilities/actions/upgrade.py
"""UpgradeAction —— 主设备/子设备固件升级(从 worker.py 迁出)。

主设备:POST /ISAPI/System/updateFirmware + wait_online(600s,升级比 reboot 更久)
子设备:POST /ISAPI/System/BulkUpgradeChildDeviceList?format=json
"""
import logging
from typing import Any

from .base import ActionResult

logger = logging.getLogger(__name__)


class UpgradeAction:
    """固件升级 Action。

    Args:
        target: "main" 或 "child"
        firmware_url: 固件下载 URL(http/https)
        child_ids: 子设备 ID 列表(target="child" 时必填)
        wait_online_timeout: 主设备升级后等待上线的超时秒数(默认 600s)
    """

    def __init__(self, target: str = "main",
                 firmware_url: str = "",
                 child_ids: list[str] | None = None,
                 wait_online_timeout: float = 600.0,
                 **_: Any) -> None:
        self._target = target
        self._firmware_url = firmware_url
        self._child_ids = list(child_ids or [])
        self._wait_online_timeout = wait_online_timeout

    def execute(self, ctx: Any) -> ActionResult:
        """执行固件升级。"""
        client = ctx.client
        if self._target == "main":
            try:
                payload = {"deviceDeviceName": "main",
                           "firmwareURL": self._firmware_url}
                client.request_json("POST", "/ISAPI/System/updateFirmware",
                                    body=payload)
                ok = client.wait_online(timeout=self._wait_online_timeout)
                if not ok:
                    return ActionResult(ok=False, error="升级后设备未在超时内重新上线")
                return ActionResult(ok=True, data={"target": "main"})
            except Exception as exc:  # noqa: BLE001
                logger.warning("UpgradeAction 主设备升级失败: %s", exc)
                return ActionResult(ok=False, error=str(exc))
        elif self._target == "child":
            if not self._child_ids:
                return ActionResult(ok=False, error="target=child 但 child_ids 为空")
            try:
                payload = {"childDeviceList": [
                    {"deviceID": cid, "firmwareURL": self._firmware_url}
                    for cid in self._child_ids]}
                client.request_json(
                    "POST",
                    "/ISAPI/System/BulkUpgradeChildDeviceList?format=json",
                    body=payload)
                return ActionResult(
                    ok=True,
                    data={"target": "child", "count": len(self._child_ids)})
            except Exception as exc:  # noqa: BLE001
                logger.warning("UpgradeAction 子设备升级失败: %s", exc)
                return ActionResult(ok=False, error=str(exc))
        return ActionResult(ok=False, error=f"未知 target: {self._target}")


__all__ = ["UpgradeAction"]
