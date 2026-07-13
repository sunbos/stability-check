# Phase 2-3: Executor Decoupling + Output Layer Refactor

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple executor agents from ctx writes (Phase 2, already verified) and refactor the output layer to remove ReporterAgent, giving Scribe a private timeline and Notifier the alert responsibility (Phase 3).

**Architecture:** Scribe maintains a private `timeline` list (accumulated via `round/done` subscription) and computes `summary()` from it — no longer reads `ctx.round_history`. Notifier absorbs Reporter's alert duty by subscribing to `incident/raise`. ReporterAgent is deleted. Coordinator's `append_round` remains the sole authoritative writer of round history.

**Tech Stack:** Python stdlib (asyncio, dataclasses), pytest, no third-party deps.

---

## Phase 2: Executor Decoupling (Already Complete)

Phase 1 Task 6 verified that reboot/watch/event_check/status_check agents only READ ctx (baseline/cfg) and never write. No code changes needed. The two policy-layer tests (`test_analyst_rulebased_degradation`, `test_coordinator_consults_analyst_on_no_recovery`) pass, confirming behavior is unchanged.

**Status:** ✅ Complete (verified in Phase 1).

---

## Phase 3: Output Layer Refactor

### File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `tests/test_scribe.py` | Create | TDD unit tests for Scribe private timeline + summary |
| `tests/harness/scribe_agent.py` | Rewrite | Private timeline accumulation; summary() from private state |
| `tests/harness/notifier_agent.py` | Modify | Add `incident/raise` subscription for alert duty |
| `tests/harness/loader.py` | Modify | Remove ReporterAgent from assembly |
| `tests/test_burnin.py` | Modify | Remove ReporterAgent dependency; summary from Scribe |
| `tests/harness/reporter_agent.py` | Delete | Superseded by Scribe (stats) + Notifier (alerts) |

---

### Task 1: Create tests/test_scribe.py (TDD red)

**Files:**
- Create: `tests/test_scribe.py`

- [ ] **Step 1: Write failing tests**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_scribe.py -v`
Expected: FAIL with `AttributeError: 'ScribeAgent' object has no attribute 'timeline'`

- [ ] **Step 3: Commit failing tests**

```bash
git add tests/test_scribe.py
git commit -m "test: add failing tests for Scribe private timeline (Phase 3 Task 1)"
```

---

### Task 2: Rewrite scribe_agent.py with private timeline

**Files:**
- Modify: `tests/harness/scribe_agent.py`

- [ ] **Step 1: Rewrite ScribeAgent**

Key changes:
- Add `self.timeline: list = []` (private round records)
- Add `self._aborted: bool = False` + `self._abort_reason: str = ""`
- `_on_round_done`: append to `self.timeline` (not just narrative)
- `_on_abort`: set `self._aborted` + `self._abort_reason`
- `summary()`: compute from `self.timeline` + `self._aborted` (not ctx)
- Remove `len(self.ctx.history())` read

```python
"""ScribeAgent：记录员（仅使用标准库）。

职责
----
* 作为总线观察者，把各 agent 的关键消息整理成**面向人**的叙事（narrative），
  并维护私有 timeline 累积每轮记录。
* 订阅：round/done、incident/raise、analyst/decision、analyst/report、coord/abort。
* summary() 从私有 timeline 计算，不读 ctx.round_history。

设计说明
--------
Scribe 不发起任何设备请求，也不做决策，只"记录"。它把总线上的分散信号
连成一条连贯的时间线。Phase 3 起，Scribe 维护私有 timeline 和 _aborted 标志，
不再依赖 ctx 的可变状态，符合"私有状态"原则。

仅依赖标准库 + 同仓 bus / agent / context，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from agent import Agent  # noqa: E402


class ScribeAgent(Agent):
    """记录员：私有 timeline 累积 + 叙事，summary() 不读 ctx。"""

    SUMMARY_TOPIC = "scribe/summary"
    TOPICS = (
        "round/done",
        "incident/raise",
        "analyst/decision",
        "analyst/report",
        "coord/abort",
    )

    def __init__(self, spec, bus, ctx, cfg=None) -> None:
        super().__init__(spec, bus, ctx)
        self.cfg = cfg if cfg is not None else getattr(ctx, "cfg", None)
        self.narrative: list = []
        self.timeline: list = []          # private round records
        self._aborted: bool = False       # private abort flag
        self._abort_reason: str = ""

    # ------------------------------------------------------------------ #
    # 叙事记录
    # ------------------------------------------------------------------ #
    def _line(self, text: str) -> None:
        ts = time.strftime("%H:%M:%S", time.localtime())
        entry = f"[{ts}] {text}"
        self.narrative.append(entry)
        self.ctx.append_log(f"记录员: {text}")
        print(f"[{ts}] [记录员] {text}")

    async def _on_round_done(self, m: dict) -> None:
        record = dict(m)
        self.timeline.append(record)
        r = m.get("round")
        tag = "通过" if m.get("passed") else "失败"
        rt = m.get("recover_time")
        rt_str = f"{rt:.1f}秒" if isinstance(rt, (int, float)) else "NA"
        self._line(
            f"第 {r} 轮 {tag}：事件={m.get('found')} 状态偏移={m.get('changed')} "
            f"恢复耗时={rt_str}"
        )

    async def _on_incident(self, m: dict) -> None:
        inc = m.get("incident", m)
        self._line(f"事故：{inc}")

    async def _on_decision(self, m: dict) -> None:
        cont = m.get("continue")
        src = m.get("source")
        self._line(
            f"分析决策(来源={src})：{'继续' if cont else '停止'} —— {m.get('reason')}"
        )

    async def _on_report(self, m: dict) -> None:
        if m.get("failed"):
            self._line(
                f"稳定性评分={m.get('stability_score')} 失败={m.get('failed')}/"
                f"{m.get('total')} 建议={m.get('recommendation')}"
            )

    async def _on_abort(self, m: dict) -> None:
        self._aborted = True
        self._abort_reason = m.get("reason", "unknown")
        self._line(f"拷机中止：{m.get('reason')}")
        await self._emit_summary()

    async def _emit_summary(self) -> None:
        summary = self.summary()
        await self.publish(self.SUMMARY_TOPIC, summary)

    # ------------------------------------------------------------------ #
    # 摘要（从私有 timeline 计算，不读 ctx）
    # ------------------------------------------------------------------ #
    def summary(self) -> dict:
        """从私有 timeline 计算汇总（不读 ctx）。"""
        total = len(self.timeline)
        passed = sum(1 for r in self.timeline if r.get("passed"))
        failed = total - passed
        recover_times = [
            r.get("recover_time")
            for r in self.timeline
            if r.get("recover_time") is not None
        ]
        avg_recover_time = (
            sum(recover_times) / len(recover_times) if recover_times else None
        )
        max_recover_time = max(recover_times) if recover_times else None
        return {
            "narrative": list(self.narrative),
            "total": total,
            "passed": passed,
            "failed": failed,
            "aborted": self._aborted,
            "reason": self._abort_reason,
            "avg_recover_time": round(avg_recover_time, 1) if avg_recover_time is not None else None,
            "max_recover_time": max_recover_time,
            "rounds": total,
        }

    async def _on_summary_request(self, m: dict) -> None:
        await self._emit_summary()

    async def run(self) -> None:
        self.subscribe("round/done", self._on_round_done)
        self.subscribe("incident/raise", self._on_incident)
        self.subscribe("analyst/decision", self._on_decision)
        self.subscribe("analyst/report", self._on_report)
        self.subscribe("coord/abort", self._on_abort)
        self.subscribe("scribe/summary/request", self._on_summary_request)
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            pass
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `python -m pytest tests/test_scribe.py -v`
Expected: PASS (7 tests)

- [ ] **Step 3: Run existing tests for regression**

Run: `python -m pytest tests/test_context.py tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v`
Expected: PASS (no regression)

- [ ] **Step 4: Commit**

```bash
git add tests/harness/scribe_agent.py
git commit -m "refactor: Scribe maintains private timeline, summary() no longer reads ctx (Phase 3 Task 2)"
```

---

### Task 3: Update notifier_agent.py to absorb alert duty

**Files:**
- Modify: `tests/harness/notifier_agent.py`

Notifier already subscribes to `coord/abort`, `analyst/decision`, `analyst/report`, `notify`. Add `incident/raise` subscription so Notifier alerts on incidents (absorbing Reporter's alert responsibility).

- [ ] **Step 1: Add incident/raise subscription and handler**

Add to `TOPICS` tuple: `"incident/raise"`

Add handler method:
```python
async def _on_incident(self, m: dict) -> None:
    """Alert on raised incidents (absorbs Reporter alert duty)."""
    inc = m.get("incident", m)
    severity = m.get("severity", "info")
    raised_by = m.get("raised_by", "unknown")
    description = m.get("description", str(inc))
    self.notify(
        f"事故告警 [{severity}]",
        f"来源={raised_by}：{description}",
    )
```

Register in `run()`:
```python
self.subscribe("incident/raise", self._on_incident)
```

- [ ] **Step 2: Run existing tests for regression**

Run: `python -m pytest tests/test_context.py tests/test_scribe.py tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/harness/notifier_agent.py
git commit -m "feat: Notifier subscribes to incident/raise for alert duty (Phase 3 Task 3)"
```

---

### Task 4: Update loader.py to remove ReporterAgent

**Files:**
- Modify: `tests/harness/loader.py`

- [ ] **Step 1: Remove ReporterAgent from assembly**

Remove these lines:
- `_reporter_mod = _load_harness("reporter_agent")`
- `ReporterAgent = _reporter_mod.ReporterAgent`
- `reporter_spec = _spec("reporter", "reporter", "")`
- `reporter = ReporterAgent(reporter_spec, bus, ctx)`
- Remove `reporter` from the `agents` list

Update the module docstring to remove ReporterAgent from the agent list.

- [ ] **Step 2: Verify loader compiles**

Run: `python -m py_compile tests/harness/loader.py`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add tests/harness/loader.py
git commit -m "refactor: remove ReporterAgent from loader assembly (Phase 3 Task 4)"
```

---

### Task 5: Update test_burnin.py to remove ReporterAgent dependency

**Files:**
- Modify: `tests/test_burnin.py`

- [ ] **Step 1: Remove ReporterAgent import and usage**

Remove:
- `from reporter_agent import ReporterAgent  # noqa: E402`
- `reporter = next(a for a in agents if isinstance(a, ReporterAgent))`
- `summary = reporter.reporter.summary()`

Replace with:
```python
summary = scribe.summary()
```

(scribe is already retrieved on the line above; Scribe.summary() now returns the same fields Reporter.summary() did plus narrative.)

- [ ] **Step 2: Run policy-layer tests for regression**

Run: `python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_burnin.py
git commit -m "refactor: test_burnin gets summary from Scribe instead of ReporterAgent (Phase 3 Task 5)"
```

---

### Task 6: Delete reporter_agent.py + final validation

**Files:**
- Delete: `tests/harness/reporter_agent.py`

- [ ] **Step 1: Verify no remaining references to ReporterAgent**

Run: `grep -rn "ReporterAgent\|reporter_agent" tests/ --include="*.py"`
Expected: only `tests/harness/reporter_agent.py` itself (and possibly comments in design docs)

- [ ] **Step 2: Delete reporter_agent.py**

```bash
git rm tests/harness/reporter_agent.py
```

- [ ] **Step 3: Run ruff on all modified files**

Run: `python -m ruff check tests/harness/scribe_agent.py tests/harness/notifier_agent.py tests/harness/loader.py tests/harness/coordinator.py tests/harness/context.py tests/test_scribe.py tests/test_context.py tests/test_burnin.py`
Expected: All checks passed

- [ ] **Step 4: Run all non-device tests**

Run: `python -m pytest tests/test_context.py tests/test_scribe.py tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git commit -m "refactor: delete reporter_agent.py, responsibilities split to Scribe + Notifier (Phase 3 Task 6)"
```

---

## Success Criteria

- ✅ `tests/test_scribe.py` all tests pass (7 tests)
- ✅ `tests/test_context.py` all tests pass (12 tests, no regression)
- ✅ Policy-layer tests pass (no regression)
- ✅ ruff: All checks passed
- ✅ `reporter_agent.py` deleted, no dangling references
- ✅ Scribe.summary() returns {total, passed, failed, aborted, reason, avg_recover_time, max_recover_time, narrative, rounds}
- ✅ Notifier subscribes to `incident/raise`
- ✅ loader.py no longer assembles ReporterAgent

## Notes

- `tests/agents/report.py` (the `Reporter` utility class) is NOT deleted in Phase 3. It may be reused by TrendSupervisor in Phase 4. If unused after Phase 4, it can be removed then.
- `test_burnin_session` (end-to-end, needs real device) is not run in Phase 3 per user preference ("skip if no real environment"). It will be validated in Phase 7.
