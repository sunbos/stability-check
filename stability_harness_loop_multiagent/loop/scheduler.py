"""Scheduler — inter-round pacing, exponential backoff, jitter, and retry budget.

Generic, standard-library only (``math`` / ``random`` / ``time``). Provides:

  - ``interval(recover_time)`` — adaptive cadence:
        next = clamp(recover_time * K + base, MIN, MAX)
    A slow target (large recover_time) yields a longer cooldown; a fast target
    stays compact. No manual tuning required.
  - ``backoff_delay(attempt)`` — exponential backoff for *retries* with optional
    jitter and a hard ``backoff_max`` cap (AWS REL05-BP03 style).
  - ``RetryBudget`` — bounds the number of retries and/or total retry wait so a
    flaky target cannot blow up the loop (circuit-breaker-ish blast-radius cap).

The Scheduler owns a ``RetryBudget`` (optional). ``can_retry()`` / ``consume()``
let a caller honour the budget before sleeping on ``backoff_delay``.
"""

import math
import random
import time
from typing import Callable, Optional


def clamp(value: float, lo: float, hi: float) -> float:
    """Clamp ``value`` into [lo, hi]. hi < lo swaps the bounds safely."""
    if hi < lo:
        lo, hi = hi, lo
    return max(lo, min(value, hi))


class RetryBudget:
    """Caps retries by count and/or by aggregate wait time.

    ``max_retries`` — hard cap on how many retries are permitted.
    ``max_total_wait`` — hard cap on cumulative retry sleep seconds.
    ``cooldown`` — minimum spacing enforced between retries (informational).
    """

    def __init__(
        self,
        max_retries: Optional[int] = None,
        max_total_wait: Optional[float] = None,
        cooldown: float = 0.0,
    ) -> None:
        self.max_retries = max_retries
        self.max_total_wait = max_total_wait
        self.cooldown = cooldown
        self._used = 0
        self._waited = 0.0

    def allow(self) -> bool:
        if self.max_retries is not None and self._used >= self.max_retries:
            return False
        if self.max_total_wait is not None and self._waited >= self.max_total_wait:
            return False
        return True

    def consume(self, wait: float) -> None:
        self._used += 1
        self._waited += wait

    def exhausted(self) -> bool:
        return not self.allow()

    @property
    def used(self) -> int:
        return self._used

    @property
    def waited(self) -> float:
        return self._waited

    def reset(self) -> None:
        self._used = 0
        self._waited = 0.0

    def as_dict(self) -> dict:
        return {
            "used": self._used,
            "waited": round(self._waited, 3),
            "max_retries": self.max_retries,
            "max_total_wait": self.max_total_wait,
        }


class Scheduler:
    """Adaptive interval + exponential backoff + jitter + retry budget.

    Adaptive cadence (steady state):
        interval(recover_time) = clamp(recover_time * k + base, min, max)

    Retry backoff (transient failures):
        backoff_delay(attempt) = clamp(base_b * factor**attempt, 0, backoff_max)
                               * jitter_factor
    """

    def __init__(
        self,
        *,
        base: float = 1.0,
        k: float = 1.5,
        min_interval: float = 0.0,
        max_interval: float = float("inf"),
        backoff_base: float = 1.0,
        backoff_factor: float = 2.0,
        backoff_max: float = float("inf"),
        jitter: float = 0.1,
        budget: Optional[RetryBudget] = None,
        seed: Optional[int] = None,
    ) -> None:
        self.base = float(base)
        self.k = float(k)
        self.min_interval = float(min_interval)
        self.max_interval = float(max_interval)
        self.backoff_base = float(backoff_base)
        self.backoff_factor = float(backoff_factor)
        self.backoff_max = float(backoff_max)
        # jitter in [0, 1): 0 == deterministic, 1 == up to +/-100% swing.
        self.jitter = max(0.0, min(1.0, float(jitter)))
        self.budget = budget or RetryBudget()
        self._rng = random.Random(seed)

    # ---- adaptive steady-state interval -------------------------------
    def adaptive(self, recover_time: float = 0.0) -> float:
        """clamp(recover_time * k + base, MIN, MAX)."""
        if recover_time is None:
            recover_time = 0.0
        raw = float(recover_time) * self.k + self.base
        return clamp(raw, self.min_interval, self.max_interval)

    def interval(self, recover_time: Optional[float] = None) -> float:
        """Convenience for the next inter-round sleep.

        ``None`` recovers no measurement -> uses ``base`` (clamped)."""
        return self.adaptive(0.0 if recover_time is None else recover_time)

    # ---- retry backoff -----------------------------------------------
    def backoff_delay(self, attempt: int, *, jitter: Optional[float] = None) -> float:
        """Exponential backoff for retry ``attempt`` (0-based), capped + jittered.

        Returns a strictly positive delay (``>= 0`` floor of ``min_interval``)."""
        attempt = max(0, int(attempt))
        raw = self.backoff_base * (self.backoff_factor ** attempt)
        raw = clamp(raw, 0.0, self.backoff_max)
        j = self.jitter if jitter is None else max(0.0, min(1.0, float(jitter)))
        if j > 0:
            # symmetric multiplicative jitter: raw * (1 - j) .. raw * (1 + j)
            raw = raw * (1.0 - j + 2.0 * j * self._rng.random())
        return max(0.0, raw)

    def can_retry(self) -> bool:
        return self.budget.allow()

    def consume_retry(self, wait: float) -> None:
        self.budget.consume(wait)

    def retry_delay(self, attempt: int, *, auto_consume: bool = False,
                    jitter: Optional[float] = None) -> float:
        """One-shot helper: honour the budget, compute delay, optionally consume.

        Returns 0.0 when the budget is exhausted (caller should give up)."""
        if not self.can_retry():
            return 0.0
        delay = self.backoff_delay(attempt, jitter=jitter)
        if auto_consume:
            self.consume_retry(delay)
        return delay

    def as_dict(self) -> dict:
        return {
            "base": self.base,
            "k": self.k,
            "min": self.min_interval,
            "max": self.max_interval,
            "backoff_base": self.backoff_base,
            "backoff_factor": self.backoff_factor,
            "backoff_max": self.backoff_max,
            "jitter": self.jitter,
            "budget": self.budget.as_dict(),
        }


__all__ = ["Scheduler", "RetryBudget", "clamp"]
