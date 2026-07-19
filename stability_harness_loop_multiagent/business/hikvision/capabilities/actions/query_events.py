# business/hikvision/capabilities/actions/query_events.py
"""QueryEventsAction —— 查询 3 事件链(从 worker.py 迁出)。

用 ctx.client.query_event_chain() 查询 remote_open + lock_open + lock_close,
结果存入 ctx.events(覆盖上一轮),供后续 EventChainProbe 断言。

baseline_serials 优先从 ctx.baseline["serialNos"] 读取(由 BaselineRecordPrecondition 产出);
若 ctx 无 baseline 则不过滤。
"""
import logging
from typing import Any

from .base import ActionResult

logger = logging.getLogger(__name__)


class QueryEventsAction:
    """查询事件链 Action。

    Args:
        open_iso: 开门时刻的设备时间(ISO 8601)。若为 None,从 ctx.last_open_iso 读取;
            若两者都无,用 ctx.client.get_time() 现取(此时 backward_buffer 应较大)
        backward_buffer: 向前回溯秒数(默认 30s)
    """

    def __init__(self, open_iso: str | None = None,
                 backward_buffer: float = 30.0, **_: Any) -> None:
        self._open_iso = open_iso
        self._backward_buffer = backward_buffer

    def execute(self, ctx: Any) -> ActionResult:
        """查询 3 事件链,结果存入 ctx.events。"""
        # 解析 open_iso
        open_iso = self._open_iso or getattr(ctx, "last_open_iso", None)
        if not open_iso:
            try:
                open_iso = ctx.client.get_time()["Time"]["localTime"]
            except Exception as exc:  # noqa: BLE001
                logger.warning("QueryEventsAction 取设备时间失败: %s", exc)
                return ActionResult(ok=False, error=str(exc))

        # 读取 baseline_serials(可能不存在)
        baseline_serials = None
        baseline = getattr(ctx, "baseline", None)
        if isinstance(baseline, dict):
            baseline_serials = baseline.get("serialNos")

        try:
            events = ctx.client.query_event_chain(
                open_iso=open_iso,
                backward_buffer=self._backward_buffer,
                baseline_serials=baseline_serials,
            )
            ctx.events = events  # 供 EventChainProbe 读
            return ActionResult(ok=True, data={
                "trigger": len(events["trigger"]),
                "opened": len(events["opened"]),
                "closed": len(events["closed"]),
            })
        except Exception as exc:  # noqa: BLE001
            logger.warning("QueryEventsAction 失败: %s", exc)
            ctx.events = {"trigger": [], "opened": [], "closed": []}
            return ActionResult(ok=False, error=str(exc))


__all__ = ["QueryEventsAction"]
