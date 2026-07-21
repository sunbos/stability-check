"""RebootAction —— 主设备/子设备重启(从 worker.py 迁出)。

主设备:client.reboot() + client.wait_online(timeout)
子设备:client.request_json("PUT", "/ISAPI/System/RebootBatchChild?format=json", body)

RebootAction 不依赖 ctx 的其他字段,只读 ctx.client(HikvisionClient 实例)。

返回的 ``ActionResult.data`` 携带 ``timing`` 字段(秒),便于观察者展示耗时细分:
    timing = {"reboot_put": 0.3, "wait_online": 60.5}
``reboot_put`` 是 PUT /ISAPI/System/reboot 调用本身耗时;
``wait_online`` 是 wait_online() 轮询总耗时(含设备掉线 + HTTP 恢复 + 401 重置 auth 重试)。
"""
import logging
import time
from typing import Any

from .base import ActionResult

logger = logging.getLogger(__name__)


class RebootAction:
    """重启设备 Action。

    Args:
        target: "main"(主设备)或 "child"(子设备列表)
        wait_online_timeout: 主设备重启后等待上线的超时秒数(默认 180)
        probe_endpoint: wait_online 的探测端点(默认 /ISAPI/System/deviceInfo)
        child_ids: 子设备 ID 列表(target="child" 时必填)
    """

    def __init__(self, target: str = "main",
                 wait_online_timeout: float = 180.0,
                 probe_endpoint: str = "/ISAPI/System/deviceInfo",
                 child_ids: list[str] | None = None,
                 **_: Any) -> None:
        self._target = target
        self._wait_online_timeout = wait_online_timeout
        self._probe_endpoint = probe_endpoint
        self._child_ids = list(child_ids or [])

    def execute(self, ctx: Any) -> ActionResult:
        """执行重启。

        Args:
            ctx: 上下文对象,必须暴露 .client 属性(HikvisionClient 实例)
        """
        client = ctx.client
        if self._target == "main":
            try:
                t0 = time.monotonic()
                client.reboot()
                reboot_put = time.monotonic() - t0
                t1 = time.monotonic()
                ok = client.wait_online(
                    timeout=self._wait_online_timeout,
                    probe_endpoint=self._probe_endpoint,
                )
                wait_online = time.monotonic() - t1
                if not ok:
                    return ActionResult(ok=False, error="设备未在超时内重新上线",
                                        data={"timing": {"reboot_put": reboot_put,
                                                         "wait_online": wait_online}})
                return ActionResult(ok=True, data={
                    "target": "main",
                    "timing": {"reboot_put": round(reboot_put, 2),
                               "wait_online": round(wait_online, 2)},
                })
            except Exception as exc:  # noqa: BLE001
                logger.warning("RebootAction 主设备重启失败: %s", exc)
                return ActionResult(ok=False, error=str(exc))
        elif self._target == "child":
            if not self._child_ids:
                return ActionResult(ok=False, error="target=child 但 child_ids 为空")
            try:
                payload = {"childDeviceList": [{"deviceID": cid} for cid in self._child_ids]}
                client.request_json(
                    "PUT", "/ISAPI/System/RebootBatchChild?format=json", body=payload)
                return ActionResult(
                    ok=True,
                    data={"target": "child", "count": len(self._child_ids)})
            except Exception as exc:  # noqa: BLE001
                logger.warning("RebootAction 子设备重启失败: %s", exc)
                return ActionResult(ok=False, error=str(exc))
        return ActionResult(ok=False, error=f"未知 target: {self._target}")


__all__ = ["RebootAction"]
