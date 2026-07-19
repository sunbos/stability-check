# business/hikvision/capabilities/probes/event_chain.py
"""EventChainProbe —— 事件链断言(从 worker.py 迁出)。

验证 ctx.events(QueryEventsAction 产出)中是否包含期望的事件。
事件链:remote_open(trigger) → lock_open(opened) → lock_closed(closed)。

lock_closed 是软事实:缺失不强制 fail,但产出 lock_closed_soft=True,
由 Advisor 加风险分(由 ScenarioWorker 组合时配置 Advisor 处理)。
"""
from typing import Any


class EventChainProbe:
    """事件链断言 Probe。

    Args:
        expect_trigger: 是否期望 trigger 事件(默认 True)
        expect_opened: 是否期望 opened 事件(默认 True)
        expect_closed: 是否期望 closed 事件(默认 True,但缺失为软事实)
    """

    def __init__(self, expect_trigger: bool = True,
                 expect_opened: bool = True,
                 expect_closed: bool = True, **_: Any) -> None:
        self._expect_trigger = expect_trigger
        self._expect_opened = expect_opened
        self._expect_closed = expect_closed

    def check(self, snapshot: Any) -> dict[str, bool]:
        """对 snapshot(ctx,含 events 属性)执行事件链断言,返回 fact 字典。

        Args:
            snapshot: 上下文对象,需暴露 .events 属性(dict[str, list])
                      若为 dict 则取 snapshot["events"]
        """
        # 兼容 ctx 对象与 dict
        if isinstance(snapshot, dict):
            events = snapshot.get("events", {})
        else:
            events = getattr(snapshot, "events", {})

        facts: dict[str, bool] = {}
        if self._expect_trigger:
            facts["trigger_ok"] = len(events.get("trigger", [])) > 0
        if self._expect_opened:
            facts["opened_ok"] = len(events.get("opened", [])) > 0
        if self._expect_closed:
            closed_list = events.get("closed", [])
            if closed_list:
                facts["closed_ok"] = True
            else:
                # 软事实:缺失不强制 fail,但标记 soft(Advisor 加风险分)
                facts["closed_ok"] = False
                facts["closed_soft"] = True
        return facts


__all__ = ["EventChainProbe"]
