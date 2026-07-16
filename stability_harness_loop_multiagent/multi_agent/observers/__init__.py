"""MAS 观察者角色（上报/通知智能体；仅作观察）。"""

from .base import ObserverAgent
from .scribe import ScribeAgent
from .notifier import NotifierAgent

__all__ = ["ObserverAgent", "ScribeAgent", "NotifierAgent"]
