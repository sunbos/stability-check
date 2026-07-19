"""MAS 交互协议 —— AdvisorContract、ObserverContract，以及加权投票合并的再导出。

Advisor 仅具建议性：它们投票（风险、置信度）并可能提出事件，但绝不裁决通过/失败。
Observer 消费事件并上报；它们绝不裁决。

``combine_votes`` 的规范实现位于 ``core.voting``（跨引擎共享契约内核），loop 引擎
的 ``ControlLoop`` 与 MAS 的 ``AdvisorContract`` 聚合都从 core 导入同一份，
不再各自维护（见 P0 之后的契约收敛）。这里仅做再导出以保持向后兼容。
"""

from typing import Any, Protocol, Tuple, runtime_checkable

from ..core.voting import combine_votes


@runtime_checkable
class AdvisorContract(Protocol):
    def on_round(self, round_info: Any) -> None:
        """当一个循环轮次完成时调用（订阅了 loop/done）。"""
        ...

    def vote(self) -> Tuple[float, float]:
        """返回 (risk_score, confidence)。confidence<=0 表示弃权。"""
        ...

    def raise_incident(self, severity: str, detail: Any) -> None:
        """在总线上提出一个事件（warn/critical）。"""
        ...


@runtime_checkable
class ObserverContract(Protocol):
    def on_event(self, event: Any) -> None:
        """消费一个事件用于上报/通知。绝不裁决。"""
        ...


__all__ = ["AdvisorContract", "ObserverContract", "combine_votes"]
