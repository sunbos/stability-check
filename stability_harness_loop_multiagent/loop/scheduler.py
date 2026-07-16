"""Scheduler —— 轮间定速、指数退避、抖动，以及重试预算。

通用，仅使用标准库（``math`` / ``random`` / ``time``）。提供：

  - ``interval(recover_time)`` —— 自适应节奏：
        next = clamp(recover_time * K + base, MIN, MAX)
    较慢的目标（较大的 recover_time）会得到更长的冷却时间；较快的目标
    则保持紧凑。无需手动调参。
  - ``backoff_delay(attempt)`` —— 针对*重试*的指数退避，带可选的抖动和硬性
    ``backoff_max`` 上限（类似 AWS REL05-BP03）。
  - ``RetryBudget`` —— 限制重试次数和/或重试等待的总时长，使一个不稳定的目标
    无法拖垮整个循环（类似熔断器的爆炸半径上限）。

Scheduler 持有一个 ``RetryBudget``（可选）。``can_retry()`` / ``consume()``
让调用方在按 ``backoff_delay`` 睡眠之前先遵守预算。
"""

import math
import random
import time
from typing import Callable, Optional


def clamp(value: float, lo: float, hi: float) -> float:
    """将 ``value`` 限制在 [lo, hi]。当 hi < lo 时安全地交换边界。"""
    if hi < lo:
        lo, hi = hi, lo
    return max(lo, min(value, hi))


class RetryBudget:
    """按次数和/或按累计等待时长限制重试。

    ``max_retries`` —— 允许的重试次数的硬性上限。
    ``max_total_wait`` —— 累计重试睡眠秒数的硬性上限。
    ``cooldown`` —— 重试之间强制的最小间隔（仅供参考）。
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
    """自适应间隔 + 指数退避 + 抖动 + 重试预算。

    自适应节奏（稳态）：
        interval(recover_time) = clamp(recover_time * k + base, min, max)

    重试退避（瞬时失败）：
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
        # 抖动范围在 [0, 1)：0 == 确定性，1 == 上下浮动最多 +/-100%。
        self.jitter = max(0.0, min(1.0, float(jitter)))
        self.budget = budget or RetryBudget()
        self._rng = random.Random(seed)

    # ---- 自适应稳态间隔 ---------------------------------------------
    def adaptive(self, recover_time: float = 0.0) -> float:
        """clamp(recover_time * k + base, MIN, MAX)。"""
        if recover_time is None:
            recover_time = 0.0
        raw = float(recover_time) * self.k + self.base
        return clamp(raw, self.min_interval, self.max_interval)

    def interval(self, recover_time: Optional[float] = None) -> float:
        """用于下一轮轮间睡眠的便捷方法。

        ``None`` 表示没有测量值 -> 使用 ``base``（经 clamp）。"""
        return self.adaptive(0.0 if recover_time is None else recover_time)

    # ---- 重试退避 ---------------------------------------------------
    def backoff_delay(self, attempt: int, *, jitter: Optional[float] = None) -> float:
        """针对第 attempt 次（从 0 开始计）重试的指数退避，带上限与抖动。

        返回一个严格为正（``>= 0`` 下限为 ``min_interval``）的延迟。"""
        attempt = max(0, int(attempt))
        raw = self.backoff_base * (self.backoff_factor ** attempt)
        raw = clamp(raw, 0.0, self.backoff_max)
        j = self.jitter if jitter is None else max(0.0, min(1.0, float(jitter)))
        if j > 0:
            # 对称乘性抖动：raw * (1 - j) .. raw * (1 + j)
            raw = raw * (1.0 - j + 2.0 * j * self._rng.random())
        return max(0.0, raw)

    def can_retry(self) -> bool:
        return self.budget.allow()

    def consume_retry(self, wait: float) -> None:
        self.budget.consume(wait)

    def retry_delay(self, attempt: int, *, auto_consume: bool = False,
                    jitter: Optional[float] = None) -> float:
        """一次性辅助方法：遵守预算、计算延迟，并可选地消费。

        当预算耗尽时返回 0.0（调用方应放弃）。"""
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
