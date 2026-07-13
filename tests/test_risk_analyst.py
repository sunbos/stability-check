"""Unit tests for RiskAnalyst capabilities (Phase 5).

Tests the new vote + proactive incident capabilities added to AnalystAgent
(upgraded to RiskAnalyst role). Existing advise/report capabilities are
covered by test_burnin.py.
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
from analyst_agent import AnalystAgent  # noqa: E402


class _FakeLLM:
    """Fake LLM client returning a canned JSON response."""

    def __init__(self, response: str):
        self.model = "fake-model"
        self._response = response

    def chat(self, system_prompt, user_prompt, timeout=25.0):
        return self._response


def _make_agent(llm=None) -> AnalystAgent:
    """Build an AnalystAgent with an optional injected LLM."""
    bus = EventBus()
    ctx = ReadOnlyContext()
    spec = AgentSpec("analyst", "analyst", "", "", "", "")
    agent = AnalystAgent(spec, bus, ctx)
    # Force LLM state (bypass _ensure_llm's get_client() which needs env vars)
    agent._llm = llm
    agent._llm_loaded = True
    return agent


def _vote_request(round_no=1, **facts):
    return {
        "round": round_no,
        "req_id": f"req-{round_no}",
        "facts": {
            "found": True,
            "changed": False,
            "t_recover": 60.0,
            **facts,
        },
        "history_summary": {},
        "question": "rate_risk_0_100",
        "timeout_sec": 5.0,
    }


# ------------------------------------------------------------------ #
# Vote reply tests
# ------------------------------------------------------------------ #
def test_vote_abstain_without_llm():
    """LLM unavailable -> rule-based fallback vote (not abstain).

    Design §7: LLM failure degrades to rule-based voting with moderate
    confidence (0.4), providing a valid vote instead of abstaining.
    """
    agent = _make_agent(llm=None)
    reply = agent.compute_vote(_vote_request())
    assert reply["method"] == "rule"
    assert reply["voter"] == "risk_analyst"
    assert reply["confidence"] == 0.4
    assert 0 <= reply["risk_score"] <= 100


def test_vote_llm_with_valid_json():
    """LLM returns valid JSON -> vote/reply with llm method."""
    fake = _FakeLLM('{"risk_score": 35, "rationale": "稳定", "confidence": 0.7}')
    agent = _make_agent(llm=fake)
    reply = agent.compute_vote(_vote_request())
    assert reply["method"] == "llm"
    assert reply["voter"] == "risk_analyst"
    assert reply["risk_score"] == 35
    assert 0 <= reply["confidence"] <= 1


def test_vote_abstain_on_unparseable_llm():
    """LLM returns garbage -> rule-based fallback (graceful degradation)."""
    fake = _FakeLLM("我不明白")
    agent = _make_agent(llm=fake)
    reply = agent.compute_vote(_vote_request())
    assert reply["method"] == "rule"


# ------------------------------------------------------------------ #
# Proactive incident tests
# ------------------------------------------------------------------ #
def test_proactive_critical_on_3_consecutive_high_risk():
    """risk_score > 80 for 3 consecutive votes -> critical incident."""
    fake = _FakeLLM('{"risk_score": 85, "rationale": "劣化", "confidence": 0.8}')
    agent = _make_agent(llm=fake)
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)

    for i in range(1, 4):
        asyncio.run(agent._on_vote_request(_vote_request(i)))

    criticals = [i for i in incidents if i["severity"] == "critical"]
    assert len(criticals) >= 1
    assert agent._consecutive_high_risk == 3


def test_proactive_warn_on_single_very_high_risk():
    """Single round risk_score >= 90 -> warn incident."""
    fake = _FakeLLM('{"risk_score": 92, "rationale": "严重异常", "confidence": 0.9}')
    agent = _make_agent(llm=fake)
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)

    asyncio.run(agent._on_vote_request(_vote_request(1)))

    warns = [i for i in incidents if i["severity"] == "warn"]
    assert len(warns) >= 1


def test_no_proactive_incident_below_threshold():
    """risk_score <= 80 -> no proactive incident."""
    fake = _FakeLLM('{"risk_score": 50, "rationale": "正常", "confidence": 0.8}')
    agent = _make_agent(llm=fake)
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)

    for i in range(1, 4):
        asyncio.run(agent._on_vote_request(_vote_request(i)))

    assert len(incidents) == 0


def test_consecutive_high_risk_resets_on_drop():
    """risk drops <= 80 -> consecutive counter resets."""
    fake = _FakeLLM('{"risk_score": 85, "rationale": "高", "confidence": 0.8}')
    agent = _make_agent(llm=fake)
    incidents = []
    agent._raise_incident = lambda **kw: incidents.append(kw)

    asyncio.run(agent._on_vote_request(_vote_request(1)))  # 85, consecutive=1
    fake._response = '{"risk_score": 50, "rationale": "恢复", "confidence": 0.7}'
    asyncio.run(agent._on_vote_request(_vote_request(2)))  # 50, consecutive=0
    fake._response = '{"risk_score": 85, "rationale": "又高", "confidence": 0.8}'
    asyncio.run(agent._on_vote_request(_vote_request(3)))  # 85, consecutive=1

    # Only 1 consecutive after reset, no critical
    criticals = [i for i in incidents if i["severity"] == "critical"]
    assert len(criticals) == 0
    assert agent._consecutive_high_risk == 1


# ------------------------------------------------------------------ #
# Private state tests
# ------------------------------------------------------------------ #
def test_recent_rounds_accumulates():
    """round/done appends to recent_rounds private window."""
    agent = _make_agent(llm=None)
    for i in range(3):
        asyncio.run(agent._on_round_done({
            "round": i + 1, "passed": True, "recover_time": 60.0,
        }))
    assert len(agent.recent_rounds) == 3


def test_recent_rounds_maxlen_10():
    """recent_rounds caps at 10 entries."""
    agent = _make_agent(llm=None)
    for i in range(15):
        asyncio.run(agent._on_round_done({
            "round": i + 1, "passed": True, "recover_time": 60.0,
        }))
    assert len(agent.recent_rounds) == 10


def test_last_risk_score_updated_on_vote():
    """Vote reply updates last_risk_score."""
    fake = _FakeLLM('{"risk_score": 42, "rationale": "ok", "confidence": 0.7}')
    agent = _make_agent(llm=fake)
    asyncio.run(agent._on_vote_request(_vote_request(1)))
    assert agent.last_risk_score == 42


# ------------------------------------------------------------------ #
# Backward-compat tests
# ------------------------------------------------------------------ #
def test_advise_still_works():
    """Existing analyst/advise decision logic still works (backward compat)."""
    agent = _make_agent(llm=None)  # rule-based
    decision = agent.decide({"kind": "no_recovery", "consecutive_failures": 0})
    assert decision["continue"] is False
    assert decision["source"] == "rule"
