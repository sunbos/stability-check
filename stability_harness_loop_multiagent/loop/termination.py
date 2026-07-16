"""Termination —— 可组合的 StopCondition，由 TerminationPolicy 以 OR 方式组合。

每个条件评估一个 ReadOnlyContext 并返回 (should_halt, reason)。策略按*优先级*
顺序（默认即列表顺序，或显式的 ``precedence`` 类名列表）以 OR 方式组合各条件，
并返回第一个触发停止的条件的原因。

内置条件：
  - CountStop            —— 达到最大轮数。
  - DurationStop         —— 墙钟预算耗尽。
  - FailThresholdStop    —— 累计 与/或 连续 失败次数越过阈值。
  - ExternalAbortStop    —— 总线上的 harness/abort、一个可设置的标志/可调用对象，
                            或由循环设置的 ctx.aborted（看门狗的路径）。
  - ExternalStop         —— ExternalAbortStop 的向后兼容别名。
"""

from typing import List, Optional, Tuple

from .context import ReadOnlyContext


class StopCondition:
    """协议基类。子类化并实现 ``evaluate``。"""

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        raise NotImplementedError


class CountStop(StopCondition):
    """在 ``max_rounds`` 次迭代完成后停止。"""

    def __init__(self, max_rounds: int) -> None:
        self.max_rounds = max_rounds

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        if self.max_rounds and ctx.round_count >= self.max_rounds:
            return True, f"已到达 max_rounds={self.max_rounds}"
        return False, ""


class DurationStop(StopCondition):
    """在自启动起经过 ``max_duration`` 秒后停止。"""

    def __init__(self, max_duration: float, start_ts: float = None) -> None:
        import time

        self.max_duration = max_duration
        self.start_ts = start_ts if start_ts is not None else time.time()

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        import time

        if self.max_duration and (time.time() - self.start_ts) >= self.max_duration:
            return True, f"超出 max_duration={self.max_duration}s"
        return False, ""


class FailThresholdStop(StopCondition):
    """当累计或连续失败轮次越过阈值时停止。"""

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
                return True, f"累计失败={fails} >= {self.cumulative}"
        if self.consecutive:
            run = 0
            best = 0
            for r in history:
                run = run + 1 if r.verdict in self.fail_verdicts else 0
                best = max(best, run)
            if best >= self.consecutive:
                return True, f"连续失败={best} >= {self.consecutive}"
        return False, ""


class ExternalAbortStop(StopCondition):
    """在外部中止信号触发时停止。

    三种相互独立的触发器，任意一个都会停止：
      1. 总线上的 ``harness/abort``（或自定义 ``topic``）消息，当构造时提供了
         bus 时（看门狗正是借此中止）；
      2. 一个可设置的标志 / 可调用对象 ``flag``（例如 SIGINT 处理器、CLI）；
      3. ``ctx.aborted`` —— 循环在自己停止时将上下文标记为已中止。

    该条件是自包含的：传入一个 bus 它就会自行订阅。调用 ``detach()``
    （或让进程退出）即可停止监听。
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
        """手动触发中止（镜像旧的 ExternalStop API）。"""
        self._raised = True

    def unset(self) -> None:
        self._raised = False

    def evaluate(self, ctx: ReadOnlyContext) -> Tuple[bool, str]:
        if self._raised:
            return True, "外部中止信号"
        if self._flag is not None:
            try:
                if self._flag():
                    return True, "外部中止标志"
            except Exception:  # noqa: BLE001 - 绝不让一个坏标志阻断停止
                return True, "外部中止标志抛出异常"
        if getattr(ctx, "aborted", False):
            return True, "上下文已中止"
        return False, ""

    def detach(self) -> None:
        if self._unsub is not None:
            try:
                self._unsub()
            except Exception:  # noqa: BLE001
                pass
            self._unsub = None


# 向后兼容别名（原始签名 ``ExternalStop(flag=None)`` 仍可工作；``flag`` 是受支持的
# 关键字参数）。
ExternalStop = ExternalAbortStop


class TerminationPolicy:
    """按优先级顺序以 OR 方式组合 StopCondition。

    ``conditions`` 按顺序评估；*首个*触发停止的条件胜出。默认情况下列表顺序即
    为优先级。传入 ``precedence`` 可按类名重排优先级而无需重建列表：

        TerminationPolicy(
            [CountStop(10), ExternalAbortStop(bus), DurationStop(3600)],
            precedence=["ExternalAbortStop", "DurationStop", "CountStop"],
        )

    一个类名未出现在 ``precedence`` 中的条件会被排在最后（保持原有的相对顺序）。
    优先级使得例如外部中止或时长越界可以高于普通的轮数统计。
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
