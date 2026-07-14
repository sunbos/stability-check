"""Termination — composable StopConditions OR-combined by a TerminationPolicy.

Each condition evaluates a ReadOnlyContext and returns (should_halt, reason).
The policy OR-composes conditions in *precedence* order (list order by default,
or an explicit ``precedence`` list of class names) and returns the first halting
condition's reason.

Included conditions:
  - CountStop            — max rounds reached.
  - DurationStop         — wall-clock budget exhausted.
  - FailThresholdStop    — cumulative AND/OR consecutive failures breached.
  - ExternalAbortStop    — harness/abort on the bus, a settable flag/callable,
                            or ctx.aborted set by the loop (the watchdog's path).
  - ExternalStop         — backward-compatible alias of ExternalAbortStop.
"""

from typing import List, Optional, Tuple

from .context import ReadOnlyContext


class StopCondition:
    """Protocol base. Subclass and implement ``evaluate``."""

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        raise NotImplementedError


class CountStop(StopCondition):
    """Halt after ``max_rounds`` iterations have completed."""

    def __init__(self, max_rounds: int) -> None:
        self.max_rounds = max_rounds

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        if self.max_rounds and ctx.round_count >= self.max_rounds:
            return True, f"reached max_rounds={self.max_rounds}"
        return False, ""


class DurationStop(StopCondition):
    """Halt after ``max_duration`` seconds since start."""

    def __init__(self, max_duration: float, start_ts: float = None) -> None:
        import time

        self.max_duration = max_duration
        self.start_ts = start_ts if start_ts is not None else time.time()

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        import time

        if self.max_duration and (time.time() - self.start_ts) >= self.max_duration:
            return True, f"exceeded max_duration={self.max_duration}s"
        return False, ""


class FailThresholdStop(StopCondition):
    """Halt when cumulative or consecutive failed rounds breach thresholds."""

    def __init__(
        self,
        cumulative: int = 0,
        consecutive: int = 0,
        fail_verdicts: tuple = ("fail", "abort"),
    ) -> None:
        self.cumulative = cumulative
        self.consecutive = consecutive
        self.fail_verdicts = fail_verdicts

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        history = ctx.round_history
        if self.cumulative:
            fails = sum(1 for r in history if r.verdict in self.fail_verdicts)
            if fails >= self.cumulative:
                return True, f"cumulative failures={fails} >= {self.cumulative}"
        if self.consecutive:
            run = 0
            best = 0
            for r in history:
                run = run + 1 if r.verdict in self.fail_verdicts else 0
                best = max(best, run)
            if best >= self.consecutive:
                return True, f"consecutive failures={best} >= {self.consecutive}"
        return False, ""


class ExternalAbortStop(StopCondition):
    """Halt on an external abort signal.

    Three independent triggers, any of which halts:
      1. a ``harness/abort`` (or custom ``topic``) message on the bus, when a
         bus is supplied at construction (this is how the Watchdog aborts);
      2. a settable flag / callable ``flag`` (e.g. a SIGINT handler, CLI);
      3. ``ctx.aborted`` — the loop marks the context aborted on its own halt.

    The condition is self-contained: pass a bus and it subscribes itself. Call
    ``detach()`` (or let the process exit) to stop listening.
    """

    def __init__(
        self,
        bus=None,
        topic: str = "harness/abort",
        flag: Optional[callable] = None,
    ) -> None:
        self._topic = topic
        self._flag = flag
        self._raised = False
        self._bus = bus
        self._unsub = None
        if bus is not None:
            self._unsub = bus.subscribe(topic, self._on_message)

    def _on_message(self, topic: str, message) -> None:
        self._raised = True

    def set(self) -> None:
        """Manually raise the abort (mirrors the old ExternalStop API)."""
        self._raised = True

    def unset(self) -> None:
        self._raised = False

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        if self._raised:
            return True, "external abort signal"
        if self._flag is not None:
            try:
                if self._flag():
                    return True, "external abort flag"
            except Exception:  # noqa: BLE001 - never let a bad flag block halt
                return True, "external abort flag raised exception"
        if getattr(ctx, "aborted", False):
            return True, "context aborted"
        return False, ""

    def detach(self) -> None:
        if self._unsub is not None:
            try:
                self._unsub()
            except Exception:  # noqa: BLE001
                pass
            self._unsub = None


# Backward-compatible alias (the original signature ``ExternalStop(flag=None)``
# still works; ``flag`` is a supported keyword).
ExternalStop = ExternalAbortStop


class TerminationPolicy:
    """OR-composes StopConditions in precedence order.

    ``conditions`` is evaluated in order; the *first* condition that halts wins.
    By default the list order is the precedence. Pass ``precedence`` to reorder
    by class name without rebuilding the list:

        TerminationPolicy(
            [CountStop(10), ExternalAbortStop(bus), DurationStop(3600)],
            precedence=["ExternalAbortStop", "DurationStop", "CountStop"],
        )

    A condition whose class name is absent from ``precedence`` is evaluated last
    (stable relative order preserved). Precedence lets an external abort or a
    duration breach outrank a plain round count, for example.
    """

    def __init__(
        self,
        conditions: List[StopCondition],
        precedence: Optional[List[str]] = None,
    ) -> None:
        self.conditions = list(conditions)
        if precedence:
            order = {name: i for i, name in enumerate(precedence)}
            self.conditions.sort(
                key=lambda c: order.get(type(c).__name__, len(order))
            )
        self.precedence = list(precedence) if precedence else None

    def should_halt(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        for cond in self.conditions:
            stop, reason = cond.evaluate(ctx)
            if stop:
                return True, reason
        return False, ""


__all__ = [
    "StopCondition",
    "TerminationPolicy",
    "CountStop",
    "DurationStop",
    "FailThresholdStop",
    "ExternalAbortStop",
    "ExternalStop",
]
