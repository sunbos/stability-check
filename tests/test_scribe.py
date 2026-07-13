"""Unit tests for ScribeAgent private timeline + summary (Phase 3).

Scribe no longer reads ctx.round_history. It accumulates a private timeline
via round/done subscription and computes summary() from that private state.
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
from scribe_agent import ScribeAgent  # noqa: E402


def _make_scribe() -> ScribeAgent:
    """Build a ScribeAgent with a minimal ReadOnlyContext (no cfg needed)."""
    bus = EventBus()
    ctx = ReadOnlyContext()
    spec = AgentSpec("scribe", "scribe", "", "", "", "")
    return ScribeAgent(spec, bus, ctx)


def test_scribe_initial_state():
    """Scribe starts with empty timeline and narrative."""
    scribe = _make_scribe()
    assert scribe.timeline == []
    assert scribe.narrative == []
    assert scribe._aborted is False


def test_scribe_round_done_accumulates_timeline():
    """_on_round_done appends to private timeline."""
    scribe = _make_scribe()
    asyncio.run(scribe._on_round_done({
        "round": 1, "passed": True, "found": True, "changed": False,
        "recover_time": 60.0,
    }))
    assert len(scribe.timeline) == 1
    assert scribe.timeline[0]["round"] == 1


def test_scribe_summary_from_private_timeline():
    """summary() computes stats from private timeline, not ctx."""
    scribe = _make_scribe()
    asyncio.run(scribe._on_round_done({
        "round": 1, "passed": True, "recover_time": 60.0,
    }))
    asyncio.run(scribe._on_round_done({
        "round": 2, "passed": False, "recover_time": 80.0,
    }))
    s = scribe.summary()
    assert s["total"] == 2
    assert s["passed"] == 1
    assert s["failed"] == 1
    assert s["aborted"] is False
    assert s["avg_recover_time"] == 70.0
    assert s["max_recover_time"] == 80.0


def test_scribe_summary_empty():
    """summary() with no rounds returns zeros."""
    scribe = _make_scribe()
    s = scribe.summary()
    assert s["total"] == 0
    assert s["passed"] == 0
    assert s["failed"] == 0
    assert s["avg_recover_time"] is None
    assert s["max_recover_time"] is None


def test_scribe_abort_sets_private_flag():
    """_on_abort sets private _aborted flag (not reading ctx.aborted)."""
    scribe = _make_scribe()
    asyncio.run(scribe._on_abort({"reason": "threshold exceeded"}))
    assert scribe._aborted is True
    assert scribe._abort_reason == "threshold exceeded"
    s = scribe.summary()
    assert s["aborted"] is True
    assert s["reason"] == "threshold exceeded"


def test_scribe_summary_has_narrative():
    """summary() includes narrative list."""
    scribe = _make_scribe()
    asyncio.run(scribe._on_round_done({
        "round": 1, "passed": True, "recover_time": 60.0,
    }))
    s = scribe.summary()
    assert isinstance(s["narrative"], list)
    assert len(s["narrative"]) >= 1


def test_scribe_does_not_read_ctx_round_history():
    """Scribe must not access ctx.round_history (private state only).

    This is a structural test: verify Scribe has its own timeline attribute
    and summary() does not reference ctx.round_history.
    """
    scribe = _make_scribe()
    assert hasattr(scribe, "timeline")
    # summary() should work even if ctx has no round_history attribute at all
    s = scribe.summary()
    assert "total" in s
