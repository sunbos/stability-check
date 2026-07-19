"""In-memory fake HikvisionClient for tests.

Simulates the cross-major event chain: remote_open_door() produces
REMOTE_OPEN(3,1024) + LOCK_OPEN(5,21) + LOCK_CLOSE(5,22) events.
Optionally injects a time skew for self-healing tests.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List


class FakeHikvisionClient:
    """In-memory client implementing the same surface as HikvisionClient."""

    def __init__(self, time_skew_seconds: float = 0.0,
                 restart_duration: float = 0.05) -> None:
        self._events: List[Dict[str, Any]] = []
        self._serial = 0
        self._skew = time_skew_seconds
        self._win_start = self._now_iso()
        self._win_end = self._win_start
        self._door_open = False
        self._door_open_at = 0.0
        self._close_delay = 0.05  # 模拟开门后自动落锁的延迟（测试用，极短）
        self._reboot_called = False
        # Simulate device going offline during reboot. After reboot() is
        # called, get_work_status() raises ConnectionError for
        # restart_duration seconds, then succeeds again. This lets the
        # three-phase _wait_online logic (offline -> back -> confirm)
        # work correctly in unit tests.
        self._restart_duration = restart_duration
        self._reboot_at: float = 0.0
        self._door_offline = False  # 模拟门离线（doorOnlineStatus != 1）
        # 串口 1 外设类型(mode) 模拟，用于前置条件测试。
        self._serial_caps: Dict[str, Any] = {
            "mode": ["readerMode", "externMode", "accessControlHost",
                     "accessDetection"],
            "baudRate": ["19200"], "dataBits": ["8"], "stopBits": ["1"],
            "parityType": ["none"], "flowCtrl": ["none"],
        }
        self._serial_cfg: Dict[str, str] = {
            "id": "1", "enabled": "true", "serialPortType": "RS485",
            "serialAddress": "1", "duplexMode": "half",
            "direction": "bdirectional", "baudRate": "19200", "dataBits": "8",
            "parityType": "none", "stopBits": "1", "flowCtrl": "none",
            "deviceName": "serial", "mode": "readerMode",
            "outputDataType": "cardNo",
        }
        self._serial_set_called = False
        self._fail_remote_open = False

    def _now_iso(self) -> str:
        t = datetime.now(timezone(timedelta(hours=8))) + timedelta(seconds=self._skew)
        return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")

    def remote_open_door(self, door_no: int = 1) -> Dict[str, Any]:
        if self._fail_remote_open:
            raise RuntimeError("remoteDoorControlFailedDoorOffline")
        self._serial += 1
        ts = self._now_iso()
        self._events.append({
            "major": 3, "minor": 1024, "time": ts,
            "remoteHostAddr": "192.168.3.20", "doorNo": door_no,
            "serialNo": self._serial,
        })
        self._serial += 1
        self._events.append({
            "major": 5, "minor": 21, "time": ts,
            "doorNo": door_no, "serialNo": self._serial,
        })
        self._serial += 1
        self._events.append({
            "major": 5, "minor": 22, "time": ts,
            "doorNo": door_no, "serialNo": self._serial,
        })
        self._win_start = ts
        self._win_end = self._now_iso()
        # 模拟门锁状态：开门(解锁)后经过 _close_delay 自动落锁(关闭)。
        self._door_open = True
        self._door_open_at = time.monotonic()
        return {"status": "ok"}

    def reboot(self) -> Dict[str, Any]:
        self._reboot_called = True
        self._reboot_at = time.monotonic()
        return {"status": "ok"}

    def get_time(self) -> Dict[str, Any]:
        return {"Time": {"localTime": self._now_iso(), "timeZone": "CST-8:00"}}

    def set_time(self, local_time: str, timezone: str = "CST-8:00") -> Dict[str, Any]:
        # Sync: clear skew
        self._skew = 0.0
        return {"status": "ok"}

    def get_work_status(self) -> Dict[str, Any]:
        # Simulate device offline during reboot window. After reboot(),
        # get_work_status raises ConnectionError for restart_duration
        # seconds, then succeeds. This mirrors real device behavior where
        # HTTP service is unavailable during the 30-60s restart cycle.
        if self._reboot_at > 0:
            elapsed = time.monotonic() - self._reboot_at
            if elapsed < self._restart_duration:
                raise ConnectionError("device rebooting (simulated)")
        # 模拟门锁状态：开门(1)后经过 _close_delay 自动落锁(0)。
        if self._door_open and (time.monotonic() - self._door_open_at) >= self._close_delay:
            self._door_open = False
        door_lock = [1] if self._door_open else [0]
        door_online = [2] if self._door_offline else [1]
        return {"AcsWorkStatus": {"cardReaderOnlineStatus": "true",
                                  "doorLockStatus": door_lock,
                                  "doorOnlineStatus": door_online}}

    def query_events(self, major: int, minor: int,
                     start: str, end: str) -> List[Dict[str, Any]]:
        return [e for e in self._events
                if e["major"] == major and e["minor"] == minor
                and start <= e["time"] <= end]

    # Test helpers
    def suppress_event(self, major: int, minor: int) -> None:
        """Remove events matching (major, minor) to simulate missing events."""
        self._events = [e for e in self._events
                        if not (e["major"] == major and e["minor"] == minor)]

    # ---- 串口外设类型（前置条件测试） ----
    def get_serial_capabilities(self, port: int = 1) -> Dict[str, Any]:
        return dict(self._serial_caps)

    def get_serial_config(self, port: int = 1) -> Dict[str, Any]:
        return dict(self._serial_cfg)

    def set_serial_config(self, port: int,
                          fields: Dict[str, str]) -> Dict[str, Any]:
        """模拟切换外设类型：写入配置并返回自动重启响应（statusCode=7）。

        设置 ``_reboot_at`` 使 get_work_status 在 restart_duration 内抛错，
        从而让 worker 的 3 阶段 _wait_online 探测逻辑（离线→上线→确认）可用。
        """
        self._serial_set_called = True
        self._serial_cfg = dict(fields)
        self._reboot_at = time.monotonic()
        return {"statusCode": 7, "statusString": "Reboot Required",
                "subStatusCode": "autoReboot", "errorMsg": "deviceName"}

    def set_door_offline(self, offline: bool = True) -> None:
        """测试辅助：模拟门离线（doorOnlineStatus != 1）。"""
        self._door_offline = offline
