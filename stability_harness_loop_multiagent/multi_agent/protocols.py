"""MAS 交互协议 —— AdvisorContract、ObserverContract，以及加权投票合并器。

顾问仅具建议性：它们投票（风险、置信度）并可能提出事件，但绝不裁决通过/失败。
观察者消费事件并上报；它们绝不裁决。combine_votes 是规范的加权合并器
（在 ControlLoop 中本地镜像了一份，以保留引擎隔离）。
"""

from typing import Any, List, Protocol, Tuple, runtime_checkable


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


def combine_votes(
    votes: List[Any],
    default_neutral: float = 50.0,
    fast_path_risk: float = 90.0,
) -> float:
    """带置信度加权的投票合并。

    ``votes`` 是一个列表，每个元素为以下之一：
      - (risk, confidence) 元组，或
      - (risk, confidence, weight) 元组，或
      - 带有 risk/confidence/weight 键的字典。
    规则：
      - 快速路径：任意 risk >= fast_path_risk 立即胜出。
      - confidence <= 0 => 弃权（权重视为 0）。
      - 全部弃权 => default_neutral（50）。
    """
    norm: List[Tuple[float, float, float]] = []
    for v in votes:
        if isinstance(v, dict):
            norm.append(
                (float(v.get("risk", 50.0)),
                 float(v.get("confidence", 0.0)),
                 float(v.get("weight", 1.0)))
            )
        else:
            items = list(v)
            risk = float(items[0])
            conf = float(items[1]) if len(items) > 1 else 0.0
            w = float(items[2]) if len(items) > 2 else 1.0
            norm.append((risk, conf, w))

    for risk, conf, _w in norm:  # 快速路径
        if risk >= fast_path_risk:
            return risk

    num = 0.0
    den = 0.0
    for risk, conf, w in norm:
        if conf <= 0:
            continue
        num += risk * w * conf
        den += w * conf
    if den == 0:
        return default_neutral
    return num / den


__all__ = ["AdvisorContract", "ObserverContract", "combine_votes"]
