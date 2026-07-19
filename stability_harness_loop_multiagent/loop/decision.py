"""DecisionAuthority —— 循环唯一的、确定性的裁决权。

事实独裁（安全底线）：如果任何一个事实为 False，裁决即为 ``fail``，且风险分数
永远无法将其升级为 pass。风险只会附加 ``warn``/``recheck`` 注解。一个
关键事件会强制产生 ``recheck``。出错时，默认退回一个保守的 ``warn(60)``，
而非乐观的 pass。

Verdict.decision ∈ {pass, warn, recheck, fail, abort}。

一致性规则（安全底线）：
  - 关键事件            -> recheck   （不可被降级）
  - 投票超时（无投票）  -> 中性风险 50（见 ``NEUTRAL_RISK``）；当每个 Advisor 都
                            弃权/沉默时，Loop 的投票合并器会返回该值，于是矩阵
                            在事实层面将其当作一次干净的通过。
  - 决策错误           -> 保守的 warn(60)（``CONSERVATIVE_RISK``），
                            绝不采用乐观的通过。
"""

from dataclasses import dataclass
from typing import Any, Dict, Mapping

# 当没有 Advisor 及时投票时（投票超时 / 全部弃权）使用的中性风险值。
NEUTRAL_RISK: float = 50.0
# 当决策路径出错时所采用的保守风险值。
CONSERVATIVE_RISK: float = 60.0


@dataclass
class Verdict:
    decision: str
    risk_score: float = 50.0
    critical: bool = False
    reason: str = ""

    @property
    def passed(self) -> bool:
        return self.decision == "pass"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "decision": self.decision,
            "risk_score": self.risk_score,
            "critical": self.critical,
            "reason": self.reason,
        }


class DecisionAuthority:
    """确定性的决策矩阵。

    ``facts`` 将事实名映射到 bool（True == 已满足）。任意一个 False => fail。
    风险区间：<60 为 pass，60-80 为 warn，>80 为 recheck。关键事件强制 recheck。
    ``error=True`` 时返回保守的 warn(60)。
    """

    def decide(
        self,
        facts: Mapping[str, Any],
        risk_score: float = NEUTRAL_RISK,
        critical: bool = False,
        error: bool = False,
    ) -> Verdict:
        if error:
            return Verdict(
                "warn", risk_score=CONSERVATIVE_RISK, critical=critical,
                reason="决策错误 -> 保守 warn(60)",
            )

        # 事实独裁：任何未满足 / 为 falsy 的事实 => fail。
        for name, ok in (facts or {}).items():
            if not ok:
                return Verdict(
                    "fail", risk_score=risk_score, critical=critical,
                    reason=f"事实未满足: {name}",
                )

        if critical:
            return Verdict(
                "recheck", risk_score=risk_score, critical=True,
                reason="关键事件 -> recheck",
            )

        if risk_score > 80:
            return Verdict("recheck", risk_score=risk_score, reason="风险 > 80")
        if risk_score >= 60:
            return Verdict("warn", risk_score=risk_score, reason="风险 60-80")
        return Verdict("pass", risk_score=risk_score, reason="所有事实满足，风险低")


__all__ = ["DecisionAuthority", "Verdict", "NEUTRAL_RISK", "CONSERVATIVE_RISK"]
