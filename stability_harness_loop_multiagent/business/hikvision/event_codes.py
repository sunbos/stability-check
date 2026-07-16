"""Hikvision ISAPI AcsEvent (major, minor) code constants.

Events come from two sources:
  - External/protocol triggered (remote open, face auth)
  - Device action triggered (lock open/close), split into
    active (follows a prior event) and passive (door auto-closes).
A single door-open cycle spans multiple events across majors.
"""

from typing import Tuple


class HikEventCode:
    """(major, minor) tuples for Hikvision AcsEvent queries."""

    REMOTE_OPEN: Tuple[int, int] = (3, 1024)   # remote open (external/protocol)
    LOCK_OPEN:   Tuple[int, int] = (5, 21)     # lock opened (device action, active)
    LOCK_CLOSE:  Tuple[int, int] = (5, 22)     # lock closed (device action, passive)
    FACE_PASS:   Tuple[int, int] = (5, 75)     # face auth passed (external/protocol)


__all__ = ["HikEventCode"]
