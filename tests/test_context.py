"""Unit tests for ReadOnlyContext + CoordinatorContext (Phase 1 refactor).

Covers the new read-only/writable context split introduced by Phase 1 of the
autonomous multi-agent redesign (see
docs/plans/2026-07-13-autonomous-multiagent-design.md §6).

These tests are written first (TDD). They FAIL until tests/harness/context.py
is rewritten in Task 2 to expose ReadOnlyContext + CoordinatorContext.
"""

from __future__ import annotations

import os
import sys

# Let tests/ top-level modules (agents / harness) be importable by absolute name.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_HARNESS_DIR = os.path.join(_THIS_DIR, "harness")
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from context import ReadOnlyContext, CoordinatorContext, TaskBoard, Task  # noqa: E402


# ── ReadOnlyContext ────────────────────────────────────────────────


def test_readonly_context_initial_state():
    """ReadOnlyContext initializes with empty/None defaults."""
    ctx = ReadOnlyContext()
    assert ctx.baseline is None or ctx.baseline == {}
    assert ctx.strategy_text == ""
    assert ctx.round_history_snapshot == ()
    assert ctx.aborted is False


def test_readonly_context_history_returns_tuple():
    """history() returns a tuple snapshot."""
    ctx = ReadOnlyContext()
    assert ctx.history() == ()
    assert ctx.history(5) == ()


def test_readonly_context_latest_round_empty():
    """latest_round() returns None when no history."""
    ctx = ReadOnlyContext()
    assert ctx.latest_round() is None


def test_readonly_context_baseline_immutable_after_init():
    """baseline is set at init and read-only afterwards (no setter)."""
    ctx = ReadOnlyContext(baseline={"key": "value"})
    assert ctx.baseline == {"key": "value"}
    # ReadOnlyContext should not expose set_baseline()
    assert not hasattr(ctx, "set_baseline")


# ── CoordinatorContext ─────────────────────────────────────────────


def test_coordinator_context_inherits_readonly():
    """CoordinatorContext is a subclass of ReadOnlyContext."""
    assert issubclass(CoordinatorContext, ReadOnlyContext)


def test_coordinator_context_append_round():
    """append_round() adds a round and refreshes the snapshot."""
    ctx = CoordinatorContext()
    round1 = {"round_no": 1, "passed": True}
    ctx.append_round(round1)
    assert ctx.round_history_snapshot == (round1,)
    assert ctx.latest_round() == round1


def test_coordinator_context_append_multiple_rounds():
    """Multiple append_round calls accumulate correctly."""
    ctx = CoordinatorContext()
    r1, r2 = {"round_no": 1}, {"round_no": 2}
    ctx.append_round(r1)
    ctx.append_round(r2)
    assert ctx.history() == (r1, r2)
    assert ctx.history(1) == (r2,)


def test_coordinator_context_snapshot_is_tuple():
    """round_history_snapshot is always a tuple (immutable)."""
    ctx = CoordinatorContext()
    ctx.append_round({"round_no": 1})
    assert isinstance(ctx.round_history_snapshot, tuple)


def test_coordinator_context_mark_aborted():
    """mark_aborted() sets aborted flag."""
    ctx = CoordinatorContext()
    assert ctx.aborted is False
    ctx.mark_aborted()
    assert ctx.aborted is True


def test_coordinator_context_counters():
    """append_round updates consecutive_failures and total_failures counters."""
    ctx = CoordinatorContext()
    ctx.append_round({"round_no": 1, "passed": True})
    assert ctx.consecutive_failures == 0
    assert ctx.total_failures == 0

    ctx.append_round({"round_no": 2, "passed": False})
    assert ctx.consecutive_failures == 1
    assert ctx.total_failures == 1

    ctx.append_round({"round_no": 3, "passed": False})
    assert ctx.consecutive_failures == 2
    assert ctx.total_failures == 2

    ctx.append_round({"round_no": 4, "passed": True})
    assert ctx.consecutive_failures == 0
    assert ctx.total_failures == 2


def test_coordinator_context_publish_state():
    """publish_state() broadcasts state snapshot via bus (mock async bus).

    publish_state returns a coroutine (because bus.publish is async); caller
    must await it. We use asyncio.run to drive the coroutine in test.
    """
    import asyncio

    ctx = CoordinatorContext()
    ctx.append_round({"round_no": 1, "passed": True})

    captured = []

    class MockBus:
        async def publish(self, topic, message):
            captured.append((topic, message))

    asyncio.run(ctx.publish_state(MockBus()))
    assert len(captured) == 1
    topic, msg = captured[0]
    assert topic == "context/state"
    assert "round_history_snapshot" in msg
    assert "aborted" in msg
    assert "counters" in msg


# ── TaskBoard (unchanged, smoke test) ──────────────────────────────


def test_taskboard_still_works():
    """TaskBoard API unchanged after refactor."""
    board = TaskBoard()
    board.add(Task(name="test", status="pending"))
    assert len(board.tasks) == 1
    assert board.mark("test", "done") is True
    assert board.get_pending() == []
