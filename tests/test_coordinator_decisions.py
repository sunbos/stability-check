"""Unit tests for Coordinator decision matrix (Phase 6).

Tests the new vote combination, decision matrix, and incident ack logic
added to Coordinator. These are the core decision functions that implement
the autonomous-MAS policy layer (design §5.2 / §5.3 / §5.4).
"""

from __future__ import annotations

import asyncio
import os
import sys

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_HARNESS_DIR = os.path.join(_THIS_DIR, "harness")
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from bus import EventBus  # noqa: E402
from context import CoordinatorContext  # noqa: E402
from agent import AgentSpec  # noqa: E402
from coordinator import Coordinator  # noqa: E402


def _make_coordinator() -> Coordinator:
    """Build a Coordinator with minimal context (no device needed)."""
    bus = EventBus()
    ctx = CoordinatorContext()
    spec = AgentSpec("coord", "coordinator", "", "", "", "")
    return Coordinator(spec, bus, ctx)


# ------------------------------------------------------------------ #
# _combine_votes — weighted risk score combination (design §5.3)
# ------------------------------------------------------------------ #
def test_combine_votes_weighted():
    """Two voters with confidence → confidence-weighted average."""
    coord = _make_coordinator()
    replies = [
        {
            "voter": "trend_supervisor",
            "risk_score": 30,
            "confidence": 0.8,
            "method": "rule",
        },
        {
            "voter": "risk_analyst",
            "risk_score": 70,
            "confidence": 0.6,
            "method": "llm",
        },
    ]
    result = coord._combine_votes(replies)
    # weighted: (30 * 0.5 * 0.8 + 70 * 0.5 * 0.6) / (0.5*0.8 + 0.5*0.6)
    # = (12 + 21) / (0.4 + 0.3) = 33 / 0.7 ≈ 47
    assert result["risk_score"] == 47
    assert result["method"] == "weighted"
    assert len(result["voters"]) == 2


def test_combine_votes_empty():
    """No replies → default neutral risk (50)."""
    coord = _make_coordinator()
    result = coord._combine_votes([])
    assert result["risk_score"] == 50
    assert result["method"] == "default"


def test_combine_votes_all_abstain():
    """All voters abstain (confidence=0) → neutral risk (50)."""
    coord = _make_coordinator()
    replies = [
        {
            "voter": "trend_supervisor",
            "risk_score": 50,
            "confidence": 0.0,
            "method": "abstain",
        },
        {
            "voter": "risk_analyst",
            "risk_score": 50,
            "confidence": 0.0,
            "method": "abstain",
        },
    ]
    result = coord._combine_votes(replies)
    assert result["risk_score"] == 50
    assert result["method"] == "all_abstain"


def test_combine_votes_single_voter():
    """One voter → its risk score (weighted by its own confidence)."""
    coord = _make_coordinator()
    replies = [
        {
            "voter": "trend_supervisor",
            "risk_score": 40,
            "confidence": 0.9,
            "method": "rule",
        },
    ]
    result = coord._combine_votes(replies)
    assert result["risk_score"] == 40
    assert result["method"] == "weighted"


# ------------------------------------------------------------------ #
# _apply_decision_matrix — fact + risk → decision (design §5.4)
# ------------------------------------------------------------------ #
def test_decision_matrix_fail_overrides_risk():
    """Fact-layer dictatorship: found=False or changed=True → fail."""
    coord = _make_coordinator()
    assert (
        coord._apply_decision_matrix(
            found=False, changed=False, risk_score=10, has_critical=False
        )
        == "fail"
    )
    assert (
        coord._apply_decision_matrix(
            found=True, changed=True, risk_score=10, has_critical=False
        )
        == "fail"
    )


def test_decision_matrix_pass_low_risk():
    """Facts pass + risk < 60 → pass."""
    coord = _make_coordinator()
    assert (
        coord._apply_decision_matrix(
            found=True, changed=False, risk_score=30, has_critical=False
        )
        == "pass"
    )


def test_decision_matrix_warn_medium_risk():
    """Facts pass + risk 60-80 → warn."""
    coord = _make_coordinator()
    assert (
        coord._apply_decision_matrix(
            found=True, changed=False, risk_score=70, has_critical=False
        )
        == "warn"
    )


def test_decision_matrix_recheck_high_risk():
    """Facts pass + risk > 80 → recheck."""
    coord = _make_coordinator()
    assert (
        coord._apply_decision_matrix(
            found=True, changed=False, risk_score=85, has_critical=False
        )
        == "recheck"
    )


def test_decision_matrix_critical_forces_recheck():
    """Critical incident forces recheck regardless of risk."""
    coord = _make_coordinator()
    assert (
        coord._apply_decision_matrix(
            found=True, changed=False, risk_score=30, has_critical=True
        )
        == "recheck"
    )


# ------------------------------------------------------------------ #
# _should_ack_incident — incident → ack decision (design §5.2)
# ------------------------------------------------------------------ #
def test_incident_ack_critical():
    """Critical incident → accepted + recheck."""
    coord = _make_coordinator()
    ack = coord._should_ack_incident({"severity": "critical"}, current_risk=30)
    assert ack["decision"] == "accepted"
    assert ack["action"] == "coord/recheck"


def test_incident_ack_warn_high_risk():
    """Warn incident + risk > 60 → accepted + recheck."""
    coord = _make_coordinator()
    ack = coord._should_ack_incident({"severity": "warn"}, current_risk=70)
    assert ack["decision"] == "accepted"
    assert ack["action"] == "coord/recheck"


def test_incident_ack_warn_low_risk():
    """Warn incident + risk <= 60 → logged (no action)."""
    coord = _make_coordinator()
    ack = coord._should_ack_incident({"severity": "warn"}, current_risk=40)
    assert ack["decision"] == "logged"
    assert ack["action"] == "none"


def test_incident_ack_info():
    """Info incident → logged regardless of risk."""
    coord = _make_coordinator()
    ack = coord._should_ack_incident({"severity": "info"}, current_risk=90)
    assert ack["decision"] == "logged"
    assert ack["action"] == "none"


# ------------------------------------------------------------------ #
# _collect_votes — async vote collection with timeout
# ------------------------------------------------------------------ #
def test_collect_votes_receives_replies():
    """Coordinator collects vote/reply from autonomous agents via bus."""

    async def _test():
        coord = _make_coordinator()
        # Simulate voters that reply on vote/request
        async def _trend_voter(msg):
            await coord.bus.publish("vote/reply", {
                "voter": "trend_supervisor",
                "risk_score": 25,
                "confidence": 0.8,
                "method": "rule",
                "req_id": msg.get("req_id"),
            })

        async def _risk_voter(msg):
            await coord.bus.publish("vote/reply", {
                "voter": "risk_analyst",
                "risk_score": 35,
                "confidence": 0.7,
                "method": "llm",
                "req_id": msg.get("req_id"),
            })

        coord.bus.subscribe("vote/request", _trend_voter)
        coord.bus.subscribe("vote/request", _risk_voter)

        result = await coord._collect_votes(
            round_no=1,
            facts={"found": True, "changed": False, "t_recover": 60.0},
        )
        assert result["method"] == "weighted"
        assert len(result["voters"]) == 2
        assert 20 <= result["risk_score"] <= 40

    asyncio.run(_test())


def test_collect_votes_timeout_no_replies():
    """No voters reply → timeout → default neutral risk."""

    async def _test():
        coord = _make_coordinator()
        # No subscribers to vote/request
        result = await coord._collect_votes(
            round_no=1,
            facts={"found": True, "changed": False, "t_recover": 60.0},
            timeout=0.3,
        )
        assert result["risk_score"] == 50
        assert result["method"] == "default"

    asyncio.run(_test())
