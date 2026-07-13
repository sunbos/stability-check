"""Unit tests for TrendSupervisorAgent (Phase 4).

TrendSupervisor is an autonomous agent that:
- Subscribes to round/done: accumulates private sliding windows
- Detects trend anomalies and raises incidents
- Responds to vote/request with rule-based risk scores
- Does NOT depend on LLM (pure deterministic rules)
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
from context import ReadOnlyContext  # noqa: E402
from agent import AgentSpec  # noqa: E402
from trend_supervisor_agent import TrendSupervisorAgent  # noqa: E402


def _make_agent() -> TrendSupervisorAgent:
    """Build a TrendSupervisorAgent with minimal ReadOnlyContext."""
    bus = EventBus()
    ctx = ReadOnlyContext()
    spec = AgentSpec("trend", "trend", "", "", "", "")
    return TrendSupervisorAgent(spec, bus, ctx)


def _feed_rounds(agent, rounds):
    """Feed a list of round dicts to the agent sequentially."""
    for r in rounds:
        asyncio.run(agent._on_round_done(r))


def test_initial_state():
    """Agent starts with empty windows."""
    agent = _make_agent()
    assert len(agent.recover_time_window) == 0
    assert len(agent.fail_rate_window) == 0
    assert agent.baseline_recover_time is None


def test_accumulates_windows():
    """round/done appends to both sliding windows."""
    agent = _make_agent()
    _feed_rounds(agent, [
        {"round": 1, "passed": True, "recover_time": 60.0},
        {"round": 2, "passed": False, "recover_time": 70.0},
    ])
    assert list(agent.recover_time_window) == [60.0, 70.0]
    assert list(agent.fail_rate_window) == [True, False]


def test_consecutive_3_increments_raises_warn():
    """recover_time consecutive 3 increments → raise warn incident."""
    agent = _make_agent()
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)  # capture
    _feed_rounds(agent, [
        {"round": 1, "passed": True, "recover_time": 60.0},
        {"round": 2, "passed": True, "recover_time": 62.0},
        {"round": 3, "passed": True, "recover_time": 65.0},
    ])
    assert len(incidents) >= 1
    inc = incidents[0]
    assert inc["severity"] == "warn"
    assert inc["raised_by"] == "trend_supervisor"
    assert "递增" in inc["description"]


def test_consecutive_5_increments_raises_critical():
    """recover_time consecutive 5 increments → raise critical incident."""
    agent = _make_agent()
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)
    _feed_rounds(agent, [
        {"round": 1, "passed": True, "recover_time": 60.0},
        {"round": 2, "passed": True, "recover_time": 62.0},
        {"round": 3, "passed": True, "recover_time": 65.0},
        {"round": 4, "passed": True, "recover_time": 70.0},
        {"round": 5, "passed": True, "recover_time": 78.0},
    ])
    criticals = [i for i in incidents if i["severity"] == "critical"]
    assert len(criticals) >= 1


def test_fail_rate_over_30pct_raises_warn():
    """fail_rate > 30% in 10-round window → raise warn."""
    agent = _make_agent()
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)
    # 4 fails out of 10 = 40%
    rounds = []
    for i in range(10):
        rounds.append({"round": i + 1, "passed": (i >= 4), "recover_time": 60.0})
    _feed_rounds(agent, rounds)
    warn_incs = [i for i in incidents if "fail_rate" in i.get("category", "")]
    assert len(warn_incs) >= 1


def test_recover_time_2x_avg_raises_warn():
    """Single recover_time > 2x historical average → raise warn."""
    agent = _make_agent()
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)
    _feed_rounds(agent, [
        {"round": 1, "passed": True, "recover_time": 60.0},
        {"round": 2, "passed": True, "recover_time": 60.0},
        {"round": 3, "passed": True, "recover_time": 60.0},
        {"round": 4, "passed": True, "recover_time": 130.0},  # > 2x avg(60)
    ])
    spike_incs = [i for i in incidents if "spike" in i.get("category", "")]
    assert len(spike_incs) >= 1


def test_vote_reply_warmup_abstain():
    """Vote request with insufficient data → abstain."""
    agent = _make_agent()
    reply = agent.compute_vote({
        "round": 1,
        "facts": {"found": True, "changed": False, "t_recover": 60.0},
        "history_summary": {},
        "question": "rate_risk_0_100",
    })
    assert reply["method"] == "abstain"
    assert reply["confidence"] == 0


def test_vote_reply_rule():
    """Vote request with sufficient data → rule-based risk score."""
    agent = _make_agent()
    _feed_rounds(agent, [
        {"round": 1, "passed": True, "recover_time": 60.0},
        {"round": 2, "passed": True, "recover_time": 61.0},
        {"round": 3, "passed": True, "recover_time": 60.5},
    ])
    reply = agent.compute_vote({
        "round": 4,
        "facts": {"found": True, "changed": False, "t_recover": 60.0},
        "history_summary": {},
        "question": "rate_risk_0_100",
    })
    assert reply["method"] == "rule"
    assert reply["voter"] == "trend_supervisor"
    assert 0 <= reply["risk_score"] <= 100
    assert 0 <= reply["confidence"] <= 1


def test_no_incident_on_stable():
    """Stable recover_time + all pass → no incidents raised."""
    agent = _make_agent()
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)
    _feed_rounds(agent, [
        {"round": 1, "passed": True, "recover_time": 60.0},
        {"round": 2, "passed": True, "recover_time": 60.0},
        {"round": 3, "passed": True, "recover_time": 60.0},
    ])
    assert len(incidents) == 0


def test_window_maxlen_10():
    """Sliding windows cap at 10 entries."""
    agent = _make_agent()
    for i in range(15):
        asyncio.run(agent._on_round_done({
            "round": i + 1, "passed": True, "recover_time": 60.0,
        }))
    assert len(agent.recover_time_window) == 10
    assert len(agent.fail_rate_window) == 10
