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
        self._reboot_called = False
        # Simulate device going offline during reboot. After reboot() is
        # called, get_work_status() raises ConnectionError for
        # restart_duration seconds, then succeeds again. This lets the
        # three-phase _wait_online logic (offline -> back -> confirm)
        # work correctly in unit tests.
        self._restart_duration = restart_duration
        self._reboot_at: float = 0.0

    def _now_iso(self) -> str:
        t = datetime.now(timezone(timedelta(hours=8))) + timedelta(seconds=self._skew)
        return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")

    def remote_open_door(self, door_no: int = 1) -> Dict[str, Any]:
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
        return {"AcsWorkStatus": {"cardReaderOnlineStatus": "true"}}

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
