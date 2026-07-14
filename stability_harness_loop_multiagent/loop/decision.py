"""DecisionAuthority — the loop's sole, deterministic verdict authority.

Fact dictatorship (safety floor): if ANY fact is False the verdict is ``fail``,
and risk score can NEVER upgrade it to pass. Risk only adds ``warn``/``recheck``
annotations. A critical incident forces ``recheck``. On error, defaults to a
conservative ``warn(60)`` rather than an optimistic pass.

Verdict.decision ∈ {pass, warn, recheck, fail, abort}.

Coherence rules (safety floor):
  - critical incident            -> recheck   (cannot be downgraded)
  - voting timeout (no votes)   -> neutral risk 50 (see ``NEUTRAL_RISK``); the
                                    loop's vote combiner returns this when every
                                    advisor abstains / is silent, so the matrix
                                    treats it as a clean pass on facts.
  - decision error              -> conservative warn(60) (``CONSERVATIVE_RISK``),
                                    never an optimistic pass.
"""

from dataclasses import dataclass
from typing import Any, Dict, Mapping

# Neutral risk when no advisor votes in time (voting timeout / all abstain).
NEUTRAL_RISK: float = 50.0
# Conservative risk assigned when the decision path errors out.
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
    """Deterministic decision matrix.

    ``facts`` maps fact-name -> bool (True == satisfied). Any False => fail.
    Risk ranges: <60 pass, 60-80 warn, >80 recheck. Critical forces recheck.
    ``error=True`` returns a conservative warn(60).
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
                reason="decision error -> conservative warn(60)",
            )

        # Fact dictatorship: any unsatisfied / falsy fact => fail.
        for name, ok in (facts or {}).items():
            if not ok:
                return Verdict(
                    "fail", risk_score=risk_score, critical=critical,
                    reason=f"fact failed: {name}",
                )

        if critical:
            return Verdict(
                "recheck", risk_score=risk_score, critical=True,
                reason="critical incident -> recheck",
            )

        if risk_score > 80:
            return Verdict("recheck", risk_score=risk_score, reason="risk > 80")
        if risk_score >= 60:
            return Verdict("warn", risk_score=risk_score, reason="risk 60-80")
        return Verdict("pass", risk_score=risk_score, reason="all facts ok, low risk")


__all__ = ["DecisionAuthority", "Verdict", "NEUTRAL_RISK", "CONSERVATIVE_RISK"]
