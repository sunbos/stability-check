"""SharedContext —— 由 ControlLoop 独占的可写状态，附带每轮的不可变快照。

循环持有唯一的可写视图（SharedContext）。每轮结束后，它会刷新出一个
不可变的快照，作为 ReadOnlyContext 暴露给智能体，于是智能体观察到的是一份
冻结的历史，并各自保有私有状态。不存在共享可变状态导致的竞态。
"""

import time
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple


@dataclass(frozen=True)
class RoundRecord:
    """单次循环迭代的不可变记录。"""

    round_no: int
    facts: dict
    verdict: str
    risk_score: float
    critical: bool
    timestamp: float = field(default_factory=time.time)


class ReadOnlyContext:
    """交付给智能体的冻结视图。baseline/strategy/snapshot 均不可变。"""

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
    """由 ControlLoop 独占的可写上下文。"""

    def __init__(
        self, baseline: Any = None, strategy_text: str = ""
    ) -> None:
        self._baseline = baseline
        self._strategy_text = strategy_text
        self._history: list = []
        self._snapshot: Tuple[RoundRecord, ...] = ()
        self._aborted = False
        self._abort_reason = ""

    # ---- 写入（仅循环） --------------------------------------------
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

    # ---- 读取 --------------------------------------------------------
    @property
    def round_count(self) -> int:
        return len(self._history)

    @property
    def aborted(self) -> bool:
        return self._aborted


__all__ = ["SharedContext", "ReadOnlyContext", "RoundRecord"]
