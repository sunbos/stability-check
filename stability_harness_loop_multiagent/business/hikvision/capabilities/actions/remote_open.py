# business/hikvision/capabilities/actions/remote_open.py
"""RemoteOpenAction —— 远程开门(从 worker.py 迁出)。

通过 client.remote_open_door(door) 触发协议开门,后续由 QueryEventsAction
查询事件链、EventChainProbe 断言。
"""
import logging
from typing import Any

from .base import ActionResult

logger = logging.getLogger(__name__)


class RemoteOpenAction:
    """远程开门 Action。

    Args:
        door: 门号(默认 1)
    """

    def __init__(self, door: int = 1, **_: Any) -> None:
        self._door = door

    def execute(self, ctx: Any) -> ActionResult:
        """触发远程开门。

        Args:
            ctx: 上下文对象,必须暴露 .client 属性
        """
        try:
            ctx.client.remote_open_door(door_no=self._door)
            return ActionResult(ok=True, data={"door": self._door})
        except Exception as exc:  # noqa: BLE001
            logger.warning("RemoteOpenAction 失败: %s", exc)
            return ActionResult(ok=False, error=str(exc))


__all__ = ["RemoteOpenAction"]
