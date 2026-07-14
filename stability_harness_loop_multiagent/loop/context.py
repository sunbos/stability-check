"""SharedContext — loop-owned writable state with immutable per-round snapshots.

The loop holds the only writable view (SharedContext). After each round it
refreshes an immutable snapshot exposed to agents as ReadOnlyContext, so agents
observe a frozen history and keep their own private state. No shared mutable
state races.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple


@dataclass(frozen=True)
class RoundRecord:
    """Immutable record of a single loop iteration."""

    round_no: int
    facts: dict
    verdict: str
    risk_score: float
    critical: bool
    timestamp: float = field(default_factory=time.time)


class ReadOnlyContext:
    """Frozen view handed to agents. baseline/strategy/snapshot are immutable."""

    def __init__(
        self,
        baseline: Any,
        strategy_text: str,
        snapshot: Tuple[RoundRecord, ...],
        aborted: bool = False,
        abort_reason: str = "",
    ) -> None:
        self._baseline = baseline
        self._strategy_text = strategy_text
        self._snapshot = snapshot
        self._aborted = aborted
        self._abort_reason = abort_reason

    @property
    def baseline(self) -> Any:
        return self._baseline

    @property
    def strategy_text(self) -> str:
        return self._strategy_text

    @property
    def round_history(self) -> Tuple[RoundRecord, ...]:
        return self._snapshot

    @property
    def round_count(self) -> int:
        return len(self._snapshot)

    @property
    def aborted(self) -> bool:
        return self._aborted

    @property
    def abort_reason(self) -> str:
        return self._abort_reason


class SharedContext:
    """Writable context owned exclusively by the control loop."""

    def __init__(
        self, baseline: Any = None, strategy_text: str = ""
    ) -> None:
        self._baseline = baseline
        self._strategy_text = strategy_text
        self._history: list = []
        self._snapshot: Tuple[RoundRecord, ...] = ()
        self._aborted = False
        self._abort_reason = ""

    # ---- writes (loop only) ------------------------------------------
    def append_round(self, record: RoundRecord) -> None:
        self._history.append(record)
        self._snapshot = tuple(self._history)

    def mark_aborted(self, reason: str = "") -> None:
        self._aborted = True
        self._abort_reason = reason

    def snapshot(self) -> ReadOnlyContext:
        return ReadOnlyContext(
            self._baseline,
            self._strategy_text,
            self._snapshot,
            self._aborted,
            self._abort_reason,
        )

    # ---- reads --------------------------------------------------------
    @property
    def round_count(self) -> int:
        return len(self._history)

    @property
    def aborted(self) -> bool:
        return self._aborted


__all__ = ["SharedContext", "ReadOnlyContext", "RoundRecord"]
