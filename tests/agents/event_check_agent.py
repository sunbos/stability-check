"""重启事件核对（仅使用标准库 time / json）。

提供 check_reboot_event：基于设备返回的 AcsEvent 事件列表，
确认在重启时间附近是否确实产生了重启事件。
"""

import time


def _epoch_to_str(epoch: float) -> str:
    """由 epoch 秒构造 'YYYY-MM-DDTHH:MM:SS'（不带空格 / 时区）。"""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(epoch))


def _parse_event_time(time_str: str) -> float | None:
    """解析事件时间字符串（如 '2026-06-05T08:22:07+08:00'）为 epoch 秒。

    由于 time.strptime 不支持末尾时区偏移，先去掉末尾的 '+HH:MM' / '-HH:MM'
    再解析。解析失败返回 None（交由调用方跳过该条）。
    """
    if not time_str:
        return None
    s = time_str.strip()
    # 去掉末尾时区偏移（形如 +08:00 或 -05:00）
    if s[-6:-5] in ("+", "-") and s[-3] == ":":
        s = s[:-6]
    try:
        struct = time.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
    return time.mktime(struct)


def check_reboot_event(client, t_reboot: float, t_recover: float, window: float) -> bool:
    """核对重启事件是否真实发生。

    - 用 t_reboot - window / t_recover + window 构造查询窗口时间字符串；
    - 调 client.get_reboot_events(start, end)；
    - 若返回列表非空，且至少一条事件的 time 解析后 epoch >= t_reboot - 1，
      返回 True，否则 False。
    """
    start = _epoch_to_str(t_reboot - window)
    end = _epoch_to_str(t_recover + window)

    events = client.get_reboot_events(start, end)
    if not events:
        return False

    for ev in events:
        time_str = ev.get("time") if isinstance(ev, dict) else None
        epoch = _parse_event_time(time_str) if time_str else None
        if epoch is None:
            continue
        if epoch >= t_reboot - 1:
            return True

    return False
