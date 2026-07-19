"""core.voting —— 规范的加权投票合并器（跨引擎共享的契约内核）。

投票合并是真正的跨引擎契约：loop 引擎（``ControlLoop`` 用它合并 Advisor 的
投票）与 multi_agent 引擎（``AdvisorContract`` 的聚合）需要**完全相同**的
语义。把它放进 core，让两份实现收敛为一份，杜绝漂移（此前
``loop/driver._default_combine`` 与 ``multi_agent/protocols.combine_votes``
各自维护，存在语义不一致的风险）。

引擎隔离：core 是中立内核，只依赖 typing，绝不导入任何引擎（harness / loop /
multi_agent）。
"""

from typing import Any, List, Tuple


def combine_votes(
    votes: List[Any],
    default_neutral: float = 50.0,
    fast_path_risk: float = 90.0,
) -> float:
    """带置信度加权的投票合并。

    ``votes`` 是列表，每个元素为以下之一：
      - ``(risk, confidence)`` 元组，或
      - ``(risk, confidence, weight)`` 元组，或
      - 带有 ``risk`` / ``confidence`` / ``weight`` 键的字典。

    合并规则：
      - 快速路径：任意 ``risk >= fast_path_risk`` 立即胜出（避免被低权重稀释）。
      - ``confidence <= 0`` => 视为弃权（权重视为 0）。
      - 全部弃权 => 返回 ``default_neutral``（默认 50，即中性风险）。
      - 否则返回置信度×权重加权的平均风险。
    """
    norm: List[Tuple[float, float, float]] = []
    for v in votes:
        if isinstance(v, dict):
            norm.append(
                (
                    float(v.get("risk", 50.0)),
                    float(v.get("confidence", 0.0)),
                    float(v.get("weight", 1.0)),
                )
            )
        else:
            items = list(v)
            risk = float(items[0])
            conf = float(items[1]) if len(items) > 1 else 0.0
            w = float(items[2]) if len(items) > 2 else 1.0
            norm.append((risk, conf, w))

    for risk, _conf, _w in norm:  # 快速路径：高风险一票否决
        if risk >= fast_path_risk:
            return risk

    num = 0.0
    den = 0.0
    for risk, conf, w in norm:
        if conf <= 0:
            continue  # 弃权
        num += risk * w * conf
        den += w * conf
    if den == 0:
        return default_neutral
    return num / den


__all__ = ["combine_votes"]
