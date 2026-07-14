"""MAS interaction protocols — AdvisorContract, ObserverContract, and the
weighted vote combiner.

Advisors are advisory-only: they vote (risk, confidence) and may raise incidents,
but never decide pass/fail. Observers consume events and report; they never
decide. combine_votes is the canonical weighted combiner (mirrored locally in
ControlLoop to preserve engine isolation).
"""

from typing import Any, List, Protocol, Tuple, runtime_checkable


@runtime_checkable
class AdvisorContract(Protocol):
    def on_round(self, round_info: Any) -> None:
        """Called when a loop round completes (subscribed to loop/done)."""
        ...

    def vote(self) -> Tuple[float, float]:
        """Return (risk_score, confidence). confidence<=0 means abstain."""
        ...

    def raise_incident(self, severity: str, detail: Any) -> None:
        """Raise an incident (warn/critical) onto the bus."""
        ...


@runtime_checkable
class ObserverContract(Protocol):
    def on_event(self, event: Any) -> None:
        """Consume an event for reporting/notification. Never decides."""
        ...


def combine_votes(
    votes: List[Any],
    default_neutral: float = 50.0,
    fast_path_risk: float = 90.0,
) -> float:
    """Weighted, confidence-scaled vote combination.

    ``votes`` is a list of either:
      - (risk, confidence) tuples, or
      - (risk, confidence, weight) tuples, or
      - dicts with keys risk/confidence/weight.
    Rules:
      - fast path: any risk >= fast_path_risk wins immediately.
      - confidence <= 0 => abstain (weight treated as 0).
      - all abstain => default_neutral (50).
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

    for risk, conf, _w in norm:  # fast path
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
