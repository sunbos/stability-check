"""TargetAdapter — the contract for the object the MAS acts upon.

Generic, scenario-agnostic. A concrete scenario implements this protocol
(e.g. a device, service, or resource adapter) and registers WorkerAgents that
drive it. The loop engine never imports this; workers do.
"""

from dataclasses import dataclass, field
from typing import Any, List, Protocol, runtime_checkable


@dataclass
class Event:
    """A domain event observed from the target."""

    kind: str
    payload: Any = None
    ts: float = field(default=0.0)


@dataclass
class Result:
    """Outcome of an act() operation."""

    ok: bool
    data: Any = None
    error: str = ""


@dataclass
class State:
    """A point-in-time observation of the target."""

    snapshot: Any = None


@runtime_checkable
class TargetAdapter(Protocol):
    def act(self, operation: Any) -> Result:
        """Perform an operation on the target. Returns a Result."""
        ...

    def observe(self) -> State:
        """Observe the target's current state."""
        ...

    def events(self, since: float) -> List[Event]:
        """Return events occurring at or after ``since`` (epoch seconds)."""
        ...


__all__ = ["TargetAdapter", "Event", "Result", "State"]
