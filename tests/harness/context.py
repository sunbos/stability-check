"""Shared context + task board (read-only view + Coordinator writable subclass).

ReadOnlyContext
---------------
Read-only view held by all agents. Contains baseline (injected at startup,
read-only), strategy_text (injected at startup, read-only),
round_history_snapshot (immutable tuple snapshot, replaced by Coordinator after
each round broadcast), aborted (read-only, broadcast by Coordinator via
coord/abort).

CoordinatorContext
------------------
Writable subclass held only by Coordinator. Other agents must not hold this
type. Provides append_round / mark_aborted / publish_state and other write
methods. Each write refreshes round_history_snapshot (immutable tuple).

TaskBoard
---------
Shared task list (whiteboard). Coordinator maintains it; agents read ctx.board
directly or via the bus. Task statuses: 'pending' | 'doing' | 'done' | 'failed'.

Design notes
------------
- ReadOnlyContext fields are read-only externally (baseline/strategy_text are
  immutable after startup; round_history_snapshot is an immutable tuple —
  Coordinator replaces the reference rather than mutating the contents).
- Any agent that wants to write authoritative state must do so via a bus
  message (Coordinator is the sole writer of authoritative round results and
  abort flags).
- TaskBoard remains shared (a Coordinator tool, not an inter-agent channel).

Stdlib only (dataclasses, typing). No third-party deps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Task:
    """A single item in the task board."""

    name: str
    status: str = "pending"  # 'pending' | 'doing' | 'done' | 'failed'
    result: Optional[dict] = None


class ReadOnlyContext:
    """Read-only view held by all agents. Coordinator holds a writable subclass.

    Design principles:
    - baseline is injected once at startup (read-only, never mutated)
    - round_history_snapshot is refreshed by Coordinator after each round
      broadcast (read-only snapshot, immutable tuple)
    - any agent that wants to write authoritative state must do so via a bus
      message
    """

    def __init__(
        self,
        baseline: Optional[dict] = None,
        strategy_text: str = "",
    ) -> None:
        self._baseline: dict = baseline if baseline is not None else {}
        self._strategy_text: str = strategy_text
        self._round_history_snapshot: tuple = ()
        self._aborted: bool = False

    # ------------------------------------------------------------------ #
    # Read-only properties
    # ------------------------------------------------------------------ #
    @property
    def baseline(self) -> dict:
        return self._baseline

    @property
    def strategy_text(self) -> str:
        return self._strategy_text

    @property
    def round_history_snapshot(self) -> tuple:
        return self._round_history_snapshot

    @property
    def aborted(self) -> bool:
        return self._aborted

    # ------------------------------------------------------------------ #
    # Convenience accessors (read-only)
    # ------------------------------------------------------------------ #
    def latest_round(self) -> Optional[dict]:
        """Return the latest round result (read-only). None when no history."""
        return self._round_history_snapshot[-1] if self._round_history_snapshot else None

    def history(self, last_n: int = 0) -> tuple:
        """Return a history snapshot (read-only). last_n=0 means all."""
        if last_n == 0:
            return self._round_history_snapshot
        return self._round_history_snapshot[-last_n:]

    # ------------------------------------------------------------------ #
    # Log (kept mutable for backward compat; log is auxiliary, not authoritative)
    # ------------------------------------------------------------------ #
    def append_log(self, entry: str) -> int:
        """Append a log entry to the internal log list. Return its index.

        Note: log is not authoritative state; kept as a mutable list for
        backward compatibility with existing code.
        """
        if not hasattr(self, "_log"):
            self._log: list = []
        self._log.append(entry)
        return len(self._log) - 1

    @property
    def log(self) -> list:
        """Log list (auxiliary field, not authoritative state)."""
        if not hasattr(self, "_log"):
            self._log = []
        return self._log


class CoordinatorContext(ReadOnlyContext):
    """Writable context held only by Coordinator. Other agents must not hold it.

    Provides methods to append rounds, mark aborted, broadcast state. Each
    write automatically refreshes round_history_snapshot (immutable tuple).
    """

    def __init__(
        self,
        baseline: Optional[dict] = None,
        strategy_text: str = "",
    ) -> None:
        super().__init__(baseline=baseline, strategy_text=strategy_text)
        self._round_history: list = []
        self._consecutive_failures: int = 0
        self._total_failures: int = 0
        self._consecutive_reboots: int = 0
        self.board = TaskBoard()

    # ------------------------------------------------------------------ #
    # Write methods (Coordinator only)
    # ------------------------------------------------------------------ #
    def append_round(self, result: dict) -> None:
        """Coordinator only: append a round result and refresh snapshot + counters."""
        self._round_history.append(result)
        self._round_history_snapshot = tuple(self._round_history)
        self._update_counters(result)

    def mark_aborted(self) -> None:
        """Coordinator only: mark the whole burn-in session as aborted."""
        self._aborted = True

    def mark_reboot(self) -> None:
        """Coordinator only: record a reboot (increment consecutive_reboots)."""
        self._consecutive_reboots += 1

    def reset_consecutive_reboots(self) -> None:
        """Coordinator only: reset consecutive_reboots counter."""
        self._consecutive_reboots = 0

    def set_consecutive_reboots(self, n: int) -> None:
        """Coordinator only: sync consecutive_reboots to an absolute value.

        Used when Coordinator maintains its own internal counter (legacy
        behavior during Phase 1) and needs to push the value into ctx for
        publish_state() broadcasts. Prefer mark_reboot / reset_consecutive_reboots
        for incremental updates in new code.
        """
        self._consecutive_reboots = int(n)

    def set_baseline(self, baseline: dict) -> None:
        """Coordinator only: set baseline (only called during startup)."""
        self._baseline = baseline

    def set_strategy(self, strategy_text: str) -> None:
        """Coordinator only: set strategy text (only called during startup)."""
        self._strategy_text = strategy_text

    def publish_state(self, bus):
        """Broadcast a state snapshot after each round (for agents to refresh views).

        Returns a coroutine (caller must await), because bus.publish is async.
        """
        return bus.publish("context/state", {
            "round_history_snapshot": self._round_history_snapshot,
            "aborted": self._aborted,
            "counters": {
                "consecutive_failures": self._consecutive_failures,
                "total_failures": self._total_failures,
                "consecutive_reboots": self._consecutive_reboots,
            },
        })

    # ------------------------------------------------------------------ #
    # Read-only access (Coordinator-internal counters)
    # ------------------------------------------------------------------ #
    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def total_failures(self) -> int:
        return self._total_failures

    @property
    def consecutive_reboots(self) -> int:
        return self._consecutive_reboots

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _update_counters(self, result: dict) -> None:
        """Update counters based on this round's result."""
        passed = result.get("passed", False)
        if passed:
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1
            self._total_failures += 1


# ── Backward-compat alias: old code referencing RunContext still works ──────
# (Phase 1 transition only; Phase 3 will delete this alias)
RunContext = CoordinatorContext


class TaskBoard:
    """Shared task list (whiteboard) for all agents."""

    def __init__(self) -> None:
        self.tasks: list = []

    def add(self, task: Task) -> None:
        """Add a task. If a same-name task already exists, overwrite it."""
        for i, t in enumerate(self.tasks):
            if t.name == task.name:
                self.tasks[i] = task
                return
        self.tasks.append(task)

    def mark(self, name: str, status: str, result: Optional[dict] = None) -> bool:
        """Mark the task named `name` as `status` (optionally with result).
        Return True if the task was found.
        """
        for t in self.tasks:
            if t.name == name:
                t.status = status
                t.result = result
                return True
        return False

    def get_pending(self, role: Optional[str] = None) -> list:
        """Return pending (status == 'pending') tasks.

        When `role` is given, only return tasks whose name starts with 'role/'.
        """
        out = [t for t in self.tasks if t.status == "pending"]
        if role is not None:
            out = [t for t in out if t.name.startswith(role + "/")]
        return out

    def snapshot(self) -> list:
        """Return a list of dict snapshots for all tasks (for logging/reporting)."""
        return [
            {"name": t.name, "status": t.status, "result": t.result}
            for t in self.tasks
        ]
