# business/hikvision/capabilities/preconditions/baseline_record.py
"""BaselineRecordPrecondition —— 记录基线 serialNo + 可选重启时长。

产出的 baseline 写入 ctx.baseline,供后续 QueryEventsAction 引用(serialNos 过滤)
和 RebootAction 引用(reboot_duration 替代 wait_online 默认超时)。

baseline 结构:
    {
        "serialNos": {"trigger": int, "opened": int, "closed": int},
        "reboot_duration": int  # 仅 record_reboot_duration=True 时存在
    }
"""
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class BaselineRecordPrecondition:
    """基线记录 Precondition。

    Args:
        record_reboot_duration: 是否记录重启时长(默认 False,只记 serialNos)
        backward_buffer: 查询事件链时的回溯秒数(默认 300s,覆盖历史事件)
        reboot_wait_timeout: 重启等待超时(默认 180s)
    """

    def __init__(self, record_reboot_duration: bool = False,
                 backward_buffer: float = 300.0,
                 reboot_wait_timeout: float = 180.0,
                 **_: Any) -> None:
        self._record_reboot_duration = record_reboot_duration
        self._backward_buffer = backward_buffer
        self._reboot_wait_timeout = reboot_wait_timeout

    def setup(self, ctx: Any) -> bool:
        """记录基线 serialNo + 可选重启时长,写入 ctx.baseline。"""
        try:
            # 1. 查询当前事件链,取每类 max(serialNo) 作为基线
            client = ctx.client
            open_iso = client.get_time()["Time"]["localTime"]
            events = client.query_event_chain(
                open_iso=open_iso,
                backward_buffer=self._backward_buffer,
                baseline_serials=None,  # 基线查询本身不需要过滤
            )
            serials = {
                "trigger": _max_serial(events.get("trigger", [])),
                "opened": _max_serial(events.get("opened", [])),
                "closed": _max_serial(events.get("closed", [])),
            }
            ctx.baseline = {"serialNos": serials}

            # 2. 可选:记录 reboot 时长
            if self._record_reboot_duration:
                start = time.time()
                client.reboot()
                ok = client.wait_online(timeout=self._reboot_wait_timeout)
                duration = time.time() - start
                ctx.baseline["reboot_duration"] = int(duration) + 10  # 加 10s buffer
                if not ok:
                    logger.warning("BaselineRecordPrecondition 重启后未上线")
                    return False

            logger.info("BaselineRecordPrecondition 成功: %s", ctx.baseline)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("BaselineRecordPrecondition 失败: %s", exc)
            return False


def _max_serial(events: list[dict[str, Any]]) -> int:
    """取事件列表中最大的 serialNo,空列表返回 0。"""
    if not events:
        return 0
    try:
        return max(int(e.get("serialNo", 0)) for e in events)
    except (ValueError, TypeError):
        return 0


__all__ = ["BaselineRecordPrecondition"]
