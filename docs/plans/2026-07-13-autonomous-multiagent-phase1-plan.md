# Phase 1: Infrastructure Refactor — ReadOnlyContext + CoordinatorContext

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `RunContext` into `ReadOnlyContext` + `CoordinatorContext` without changing any runtime behavior; all existing tests must stay green.

**Architecture:** Split the single shared `RunContext` into a read-only view (held by all agents) and a writable subclass (held only by Coordinator). Introduce `round_history_snapshot` as an immutable tuple. Add `publish_state()` to broadcast state snapshots. TaskBoard stays shared.

**Tech Stack:** Python 3.13, stdlib only (dataclasses, asyncio), pytest, no third-party deps.

**Branch:** Create `feat/autonomous-multiagent` before starting Task 1.

**Reference:** `docs/plans/2026-07-13-autonomous-multiagent-design.md` §6 (Shared State Refactor)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `tests/harness/context.py` | Rewrite | `ReadOnlyContext` + `CoordinatorContext` + `TaskBoard` (unchanged) + `Task` (unchanged) |
| `tests/harness/agent.py` | Modify (type hint only) | `Agent.__init__` accepts `ReadOnlyContext` instead of untyped `ctx` |
| `tests/harness/coordinator.py` | Modify | Replace direct `ctx.round_history.append` with `ctx.append_round`; replace `ctx.aborted = True` with `ctx.mark_aborted()`; add `ctx.publish_state(bus)` call after each round |
| `tests/harness/scribe_agent.py` | Modify | Replace `self.ctx.round_history` reads with `self.ctx.history()`; replace `self.ctx.aborted` read with `self.ctx.aborted` (still valid, read-only) |
| `tests/harness/status_check_agent.py` | Modify (verify only) | `self.ctx.baseline` read stays valid (read-only) — verify no writes |
| `tests/test_context.py` | Create | Unit tests for new `ReadOnlyContext` + `CoordinatorContext` API |
| `tests/test_burnin.py` | Modify (if needed) | Update any direct `RunContext()` construction to `CoordinatorContext()` |

---

## Task 1: Create new context unit tests (TDD)

**Files:**
- Create: `tests/test_context.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_context.py
"""Unit tests for ReadOnlyContext + CoordinatorContext (Phase 1 refactor)."""

from __future__ import annotations

import pytest

from harness.context import ReadOnlyContext, CoordinatorContext, TaskBoard


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

    Note: publish_state returns a coroutine (because bus.publish is async);
    caller must await it. We use asyncio.run to drive the coroutine in test.
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
    from harness.context import Task
    board.add(Task(name="test", status="pending"))
    assert len(board.tasks) == 1
    assert board.mark("test", "done") is True
    assert board.get_pending() == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_context.py -v`
Expected: FAIL with `ImportError: cannot import name 'ReadOnlyContext' from 'harness.context'`

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_context.py
git commit -m "test: add failing tests for ReadOnlyContext + CoordinatorContext (Phase 1 Task 1)"
```

---

## Task 2: Rewrite context.py with ReadOnlyContext + CoordinatorContext

**Files:**
- Modify: `tests/harness/context.py` (full rewrite of RunContext section; keep Task + TaskBoard unchanged)

- [ ] **Step 1: Rewrite context.py**

```python
# tests/harness/context.py
"""共享上下文 + 任务清单（只读视图 + Coordinator 可写子类）。

ReadOnlyContext
---------------
所有 agent 持有的只读视图。包含 baseline（启动注入，只读）、
strategy_text（启动注入，只读）、round_history_snapshot（不可变 tuple 快照，
由 Coordinator 每轮广播后替换）、aborted（只读，由 Coordinator 通过
coord/abort 广播）。

CoordinatorContext
------------------
Coordinator 专有的可写上下文。其他 agent 不应持有此类型。提供 append_round /
mark_aborted / publish_state 等写入方法。

TaskBoard
---------
所有 agent 共同的任务清单（白板）。协调者维护它；agent 通过总线或直接读
ctx.board 获取/更新共同清单。任务状态：'pending' | 'doing' | 'done' | 'failed'。

设计说明
--------
- ReadOnlyContext 字段对外只读（baseline/strategy_text 启动后不变；
  round_history_snapshot 是不可变 tuple，Coordinator 替换引用而非修改内容）
- 任何 agent 想写入权威状态必须通过总线消息
- TaskBoard 保留共享（Coordinator 工具，非 agent 间通信）

仅依赖标准库 dataclasses，无第三方依赖。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Task:
    """清单中的一项任务。"""

    name: str
    status: str = "pending"  # 'pending' | 'doing' | 'done' | 'failed'
    result: Optional[dict] = None


class ReadOnlyContext:
    """所有 agent 持有的只读视图。Coordinator 持有可写子类。

    设计原则：
    - baseline 启动时一次性注入（只读，不变）
    - round_history_snapshot 由 Coordinator 每轮广播后更新（只读快照，不可变 tuple）
    - 任何 agent 想写入权威状态必须通过总线消息
    """

    def __init__(
        self,
        baseline: Optional[dict] = None,
        strategy_text: str = "",
    ) -> None:
        self._baseline: dict = baseline if baseline is not None else {}
        self._strategy_text: str = strategy_text
        self._round_history_snapshot: tuple = ()
        self._aborted: bool = False

    # ------------------------------------------------------------------ #
    # 只读属性
    # ------------------------------------------------------------------ #
    @property
    def baseline(self) -> dict:
        return self._baseline

    @property
    def strategy_text(self) -> str:
        return self._strategy_text

    @property
    def round_history_snapshot(self) -> tuple:
        return self._round_history_snapshot

    @property
    def aborted(self) -> bool:
        return self._aborted

    # ------------------------------------------------------------------ #
    # 便捷访问（只读）
    # ------------------------------------------------------------------ #
    def latest_round(self) -> Optional[dict]:
        """获取最近一轮结果（只读）。无历史时返回 None。"""
        return self._round_history_snapshot[-1] if self._round_history_snapshot else None

    def history(self, last_n: int = 0) -> tuple:
        """获取历史快照（只读）。last_n=0 表示全部。"""
        if last_n == 0:
            return self._round_history_snapshot
        return self._round_history_snapshot[-last_n:]

    # ------------------------------------------------------------------ #
    # 日志（保留 append_log 兼容现有代码；log 是辅助字段，非权威状态）
    # ------------------------------------------------------------------ #
    def append_log(self, entry: str) -> int:
        """追加一条日志到内部 log 列表。返回其索引。

        注：log 不属于权威状态，保留为可变列表以兼容现有代码。
        """
        if not hasattr(self, "_log"):
            self._log: list = []
        self._log.append(entry)
        return len(self._log) - 1

    @property
    def log(self) -> list:
        """日志列表（辅助字段，非权威状态）。"""
        if not hasattr(self, "_log"):
            self._log = []
        return self._log


class CoordinatorContext(ReadOnlyContext):
    """Coordinator 专有的可写上下文。其他 agent 不应持有此类型。

    提供追加轮次、标记中止、广播状态等写入方法。每次写入后自动刷新
    round_history_snapshot（不可变 tuple）。
    """

    def __init__(
        self,
        baseline: Optional[dict] = None,
        strategy_text: str = "",
    ) -> None:
        super().__init__(baseline=baseline, strategy_text=strategy_text)
        self._round_history: list = []
        self._consecutive_failures: int = 0
        self._total_failures: int = 0
        self._consecutive_reboots: int = 0
        self.board = TaskBoard()

    # ------------------------------------------------------------------ #
    # 写入方法（仅 Coordinator 调用）
    # ------------------------------------------------------------------ #
    def append_round(self, result: dict) -> None:
        """Coordinator 专用：追加轮次结果并刷新快照 + 计数器。"""
        self._round_history.append(result)
        self._round_history_snapshot = tuple(self._round_history)
        self._update_counters(result)

    def mark_aborted(self) -> None:
        """Coordinator 专用：标记整场拷机已中止。"""
        self._aborted = True

    def mark_reboot(self) -> None:
        """Coordinator 专用：记录一次 reboot（递增 consecutive_reboots）。"""
        self._consecutive_reboots += 1

    def reset_consecutive_reboots(self) -> None:
        """Coordinator 专用：重置 consecutive_reboots 计数器。"""
        self._consecutive_reboots = 0

    def set_baseline(self, baseline: dict) -> None:
        """Coordinator 专用：设置基线（仅在启动阶段调用）。"""
        self._baseline = baseline

    def set_strategy(self, strategy_text: str) -> None:
        """Coordinator 专用：设置策略文本（仅在启动阶段调用）。"""
        self._strategy_text = strategy_text

    def publish_state(self, bus):
        """每轮结束后广播状态快照（供其他 agent 更新本地视图）。

        Returns a coroutine (caller must await), because bus.publish is async.
        """
        return bus.publish("context/state", {
            "round_history_snapshot": self._round_history_snapshot,
            "aborted": self._aborted,
            "counters": {
                "consecutive_failures": self._consecutive_failures,
                "total_failures": self._total_failures,
                "consecutive_reboots": self._consecutive_reboots,
            },
        })

    # ------------------------------------------------------------------ #
    # 只读访问（Coordinator 专有的内部计数器）
    # ------------------------------------------------------------------ #
    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def total_failures(self) -> int:
        return self._total_failures

    @property
    def consecutive_reboots(self) -> int:
        return self._consecutive_reboots

    # ------------------------------------------------------------------ #
    # 内部方法
    # ------------------------------------------------------------------ #
    def _update_counters(self, result: dict) -> None:
        """根据本轮结果更新计数器。"""
        passed = result.get("passed", False)
        if passed:
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1
            self._total_failures += 1


# ── 向后兼容别名：旧代码引用 RunContext 仍可用（仅 Phase 1 过渡期）────
# 注：Phase 3 将删除此别名
RunContext = CoordinatorContext


class TaskBoard:
    """所有 agent 共同的任务清单（白板）。"""

    def __init__(self) -> None:
        self.tasks: list = []

    def add(self, task: Task) -> None:
        """添加一个任务。若同名任务已存在则覆盖。"""
        for i, t in enumerate(self.tasks):
            if t.name == task.name:
                self.tasks[i] = task
                return
        self.tasks.append(task)

    def mark(self, name: str, status: str, result: Optional[dict] = None) -> bool:
        """把名为 name 的任务标记为 status（可附带 result）。返回是否找到该任务。"""
        for t in self.tasks:
            if t.name == name:
                t.status = status
                t.result = result
                return True
        return False

    def get_pending(self, role: Optional[str] = None) -> list:
        """返回待处理（status == 'pending'）的任务列表。

        role 指定时，仅返回名称以 'role/' 开头的任务（便于按角色过滤）。
        """
        out = [t for t in self.tasks if t.status == "pending"]
        if role is not None:
            out = [t for t in out if t.name.startswith(role + "/")]
        return out

    def snapshot(self) -> list:
        """返回全部任务的 dict 快照列表（供日志/上报使用）。"""
        return [
            {"name": t.name, "status": t.status, "result": t.result}
            for t in self.tasks
        ]
```

- [ ] **Step 2: Run the new unit tests to verify they pass**

Run: `python -m pytest tests/test_context.py -v`
Expected: PASS (all 11 tests)

- [ ] **Step 3: Run existing tests to verify no regression**

Run: `python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v`
Expected: PASS (existing tests still green because `RunContext = CoordinatorContext` alias preserves backward compatibility)

- [ ] **Step 4: Commit**

```bash
git add tests/harness/context.py
git commit -m "refactor: split RunContext into ReadOnlyContext + CoordinatorContext (Phase 1 Task 2)

- ReadOnlyContext: read-only view held by all agents (baseline, strategy_text,
  round_history_snapshot as immutable tuple, aborted)
- CoordinatorContext: writable subclass with append_round, mark_aborted,
  publish_state; auto-updates counters and snapshot
- RunContext = CoordinatorContext alias for backward compatibility
- TaskBoard unchanged

All existing tests stay green via alias. New unit tests cover the new API."
```

---

## Task 3: Update agent.py type hint

**Files:**
- Modify: `tests/harness/agent.py:55` (type hint only, no behavior change)

- [ ] **Step 1: Update the type hint in Agent.__init__**

In `tests/harness/agent.py`, find line 55:
```python
    def __init__(self, spec: AgentSpec, bus: EventBus, ctx) -> None:
```

Change to:
```python
    def __init__(self, spec: AgentSpec, bus: EventBus, ctx: ReadOnlyContext) -> None:
```

And add the import at the top (after `from bus import EventBus`, around line 24):
```python
from context import ReadOnlyContext
```

- [ ] **Step 2: Verify syntax**

Run: `python -m py_compile tests/harness/agent.py && echo OK`
Expected: `OK`

- [ ] **Step 3: Run existing tests**

Run: `python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add tests/harness/agent.py
git commit -m "refactor: type-hint Agent.__init__ ctx as ReadOnlyContext (Phase 1 Task 3)"
```

---

## Task 4: Update coordinator.py to use CoordinatorContext API

**Files:**
- Modify: `tests/harness/coordinator.py` (replace direct ctx writes with new API)

- [ ] **Step 1: Locate all direct ctx writes in coordinator.py**

Run: `grep -n "self.ctx\." tests/harness/coordinator.py`

Note every line that writes to ctx (append to round_history, set aborted, etc.). Typical patterns:
- `self.ctx.round_history.append(...)` → `self.ctx.append_round(...)`
- `self.ctx.aborted = True` → `self.ctx.mark_aborted()`
- `self.ctx.consecutive_reboots += 1` → `self.ctx.mark_reboot()`
- `self.ctx.consecutive_reboots = 0` → `self.ctx.reset_consecutive_reboots()`
- `self.ctx.set_baseline(...)` → stays the same (still on CoordinatorContext)
- `self.ctx.set_strategy(...)` → stays the same

- [ ] **Step 2: Apply each replacement**

For each match found in Step 1, apply the replacement:

**Pattern A — round_history.append → append_round:**
```python
# Before:
self.ctx.round_history.append(record)
# After:
self.ctx.append_round(record)
```

**Pattern B — aborted = True → mark_aborted:**
```python
# Before:
self.ctx.aborted = True
# After:
self.ctx.mark_aborted()
```

**Pattern C — consecutive_reboots += 1 → mark_reboot:**
```python
# Before:
self.ctx.consecutive_reboots += 1
# After:
self.ctx.mark_reboot()
```

**Pattern D — consecutive_reboots = 0 → reset_consecutive_reboots:**
```python
# Before:
self.ctx.consecutive_reboots = 0
# After:
self.ctx.reset_consecutive_reboots()
```

**Pattern E — consecutive_failures / total_failures:**
These are now auto-updated by `append_round()`. Remove any manual `self.ctx.consecutive_failures += 1` lines — they are redundant.

- [ ] **Step 3: Add publish_state call after each round**

Find the place where Coordinator broadcasts `round/done` (search for `'round/done'`). Immediately after that `bus.publish('round/done', ...)` call, add:
```python
await self.ctx.publish_state(self.bus)
```

Note: `publish_state` itself calls `bus.publish`, which is sync in the current EventBus (returns a coroutine). If `publish_state` is called from async context, make it work by either:
- Option A: make `publish_state` async and `await` it
- Option B: keep `publish_state` sync and let it return the coroutine (caller awaits)

Choose Option A for clarity. Update `CoordinatorContext.publish_state` to be async:
```python
async def publish_state(self, bus) -> None:
    """每轮结束后广播状态快照（供其他 agent 更新本地视图）。"""
    await bus.publish("context/state", {...})
```

And update the test in `tests/test_context.py`:
```python
def test_coordinator_context_publish_state():
    # ... make MockBus.publish a coroutine or use pytest-asyncio
```

Actually, simpler: keep `publish_state` sync and have it return the coroutine; caller `await`s the call:
```python
# In CoordinatorContext:
def publish_state(self, bus) -> None:
    """每轮结束后广播状态快照。Returns a coroutine (caller must await)."""
    return bus.publish("context/state", {...})

# In coordinator.py:
await self.ctx.publish_state(self.bus)
```

Update the test accordingly:
```python
import asyncio

def test_coordinator_context_publish_state():
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
```

- [ ] **Step 4: Verify syntax**

Run: `python -m py_compile tests/harness/coordinator.py tests/harness/context.py && echo OK`
Expected: `OK`

- [ ] **Step 5: Run existing tests**

Run: `python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery tests/test_context.py -v`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add tests/harness/coordinator.py tests/harness/context.py tests/test_context.py
git commit -m "refactor: Coordinator uses CoordinatorContext API (append_round, mark_aborted, publish_state) (Phase 1 Task 4)

- Replace direct ctx.round_history.append with ctx.append_round
- Replace ctx.aborted = True with ctx.mark_aborted()
- Replace manual consecutive_reboots increments with mark_reboot/reset
- Add ctx.publish_state(bus) call after each round/done broadcast
- publish_state returns coroutine (caller awaits)
- Remove redundant manual counter updates (now auto in append_round)"
```

---

## Task 5: Update scribe_agent.py to use read-only API

**Files:**
- Modify: `tests/harness/scribe_agent.py` (replace `self.ctx.round_history` reads with `self.ctx.history()`)

- [ ] **Step 1: Locate all round_history reads in scribe_agent.py**

Run: `grep -n "round_history" tests/harness/scribe_agent.py`

- [ ] **Step 2: Replace reads with history() call**

For each match:
```python
# Before:
self.ctx.round_history
# After:
self.ctx.history()
```

For slices like `self.ctx.round_history[-5:]`:
```python
# Before:
self.ctx.round_history[-5:]
# After:
self.ctx.history(5)
```

Note: `self.ctx.aborted` reads stay unchanged (still a valid read-only property).

- [ ] **Step 3: Verify syntax**

Run: `python -m py_compile tests/harness/scribe_agent.py && echo OK`
Expected: `OK`

- [ ] **Step 4: Run existing tests**

Run: `python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/harness/scribe_agent.py
git commit -m "refactor: Scribe uses ctx.history() instead of direct round_history reads (Phase 1 Task 5)"
```

---

## Task 6: Verify all executor agents don't write to ctx

**Files:**
- Verify: `tests/harness/reboot_agent.py`, `tests/harness/watch_agent.py`, `tests/harness/event_check_agent.py`, `tests/harness/status_check_agent.py`

- [ ] **Step 1: Grep for any ctx writes in executor agents**

Run for each file:
```
grep -n "self\.ctx\.\(round_history\|aborted\s*=\|consecutive\|total_failures\|set_baseline\|set_strategy\|record_round\)" tests/harness/reboot_agent.py tests/harness/watch_agent.py tests/harness/event_check_agent.py tests/harness/status_check_agent.py
```

Expected: No matches (executor agents should only read `self.ctx.baseline`).

- [ ] **Step 2: If any writes found, remove them**

If a write is found (e.g., `self.ctx.round_history.append(...)`), it's a bug — executor agents should not write authoritative state. Replace with publishing a message on the bus instead, or remove if redundant (Coordinator handles it).

- [ ] **Step 3: Run all existing tests**

Run: `python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery tests/test_context.py -v`
Expected: ALL PASS

- [ ] **Step 4: Commit (only if changes were made)**

```bash
git add tests/harness/
git commit -m "refactor: remove unauthorized ctx writes from executor agents (Phase 1 Task 6)"
```

If no changes were needed, skip this commit.

---

## Task 7: Run full test suite + verify no RunContext direct construction remains

**Files:**
- Verify: all test files and harness files

- [ ] **Step 1: Grep for any remaining direct RunContext construction**

Run: `grep -rn "RunContext(" tests/`
Expected: Only the alias definition in `context.py` and possibly `test_burnin.py` fixture. If `test_burnin.py` constructs `RunContext()`, change it to `CoordinatorContext()`.

- [ ] **Step 2: Update test_burnin.py if needed**

If `test_burnin.py` has `RunContext()`:
```python
# Before:
ctx = RunContext()
# After:
ctx = CoordinatorContext()
```

Add the import:
```python
from harness.context import CoordinatorContext
```

- [ ] **Step 3: Run ruff on all modified files**

Run: `python -m ruff check tests/harness/context.py tests/harness/agent.py tests/harness/coordinator.py tests/harness/scribe_agent.py tests/test_context.py tests/test_burnin.py`
Expected: No new errors (pre-existing F401 unrelated imports are OK)

- [ ] **Step 4: Run the full policy-layer test suite**

Run: `python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery tests/test_context.py -v`
Expected: ALL PASS

- [ ] **Step 5: Final commit**

```bash
git add tests/test_burnin.py
git commit -m "refactor: replace RunContext() with CoordinatorContext() in test fixtures (Phase 1 Task 7)"
```

- [ ] **Step 6: Phase 1 complete — verify clean state**

Run: `git status`
Expected: clean working tree

Run: `git log --oneline -8`
Expected: 6-7 commits for Phase 1, each self-contained.

---

## Phase 1 Success Criteria

- [ ] `tests/test_context.py` all 11+ tests pass
- [ ] `tests/test_burnin.py::test_analyst_rulebased_degradation` passes
- [ ] `tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery` passes
- [ ] `python -m ruff check` reports no NEW errors (pre-existing F401 OK)
- [ ] No direct `ctx.round_history.append` / `ctx.aborted = True` writes outside `CoordinatorContext`
- [ ] `ReadOnlyContext` has no setters (baseline/strategy_text/aborted are read-only)
- [ ] `round_history_snapshot` is always a tuple
- [ ] `CoordinatorContext.publish_state` broadcasts state after each round
- [ ] All commits are on `feat/autonomous-multiagent` branch

---

## Notes for Phase 2

Phase 1 keeps `RunContext = CoordinatorContext` alias for backward compatibility. Phase 2-3 will:
- Remove the alias once all callers use `CoordinatorContext` directly
- Update executor agents to fully decouple from ctx writes
- Delete `ReporterAgent` and update `loader.py`

Do NOT remove the alias in Phase 1 — it's the safety net that keeps existing tests green.
