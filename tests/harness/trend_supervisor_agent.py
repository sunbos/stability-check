"""TrendSupervisorAgent: autonomous trend-monitoring agent (L3, pure rules).

Role
----
* Autonomous layer (L3) agent that proactively detects "boiling frog" anomalies
  that the deterministic Loop Core cannot catch per-round:
  - recover_time consecutive creep (3 rounds → warn, 5 rounds → critical)
  - fail_rate sliding-window > 30% → warn
  - recover_time single-round spike > 2x historical average → warn
* Subscribes to `round/done`: accumulates private sliding windows. Does NOT
  read `ctx.round_history` directly (adheres to the private-state principle
  from the autonomous-MAS refactor).
* Raises incidents on the bus (`incident/raise`) when trend anomalies are
  detected — this is the proactive capability that distinguishes an
  autonomous agent from a reactive one.
* Responds to `vote/request` with a rule-based risk score (`vote/reply`).
* Pure deterministic rules — no LLM dependency. Always available, never
  degrades. This is the design intent: TrendSupervisor is the rule-based
  counterpart to the LLM-based RiskAnalyst, so the autonomous layer always
  has at least one voter even when LLM is unavailable.

Stdlib only (asyncio, collections.deque, uuid, time). No third-party deps.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from collections import deque
from typing import Optional

# Allow direct execution / import of this module (same pattern as peers).
_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from agent import Agent  # noqa: E402


class TrendSupervisorAgent(Agent):
    """Autonomous trend supervisor (L3): pure-rule trend detection + voting.

    Private state (per design §6.3 TrendSupervisorState):
        recover_time_window   — deque[float], maxlen=10
        fail_rate_window      — deque[bool], maxlen=10 (True=pass)
        baseline_recover_time — first observed recover_time (float | None)

    Detectors raise incidents only on *newly crossed* thresholds (dedup), so
    sustained anomalies don't flood the bus on every round.
    """

    ROUND_DONE = "round/done"
    VOTE_REQUEST = "vote/request"
    VOTE_REPLY = "vote/reply"
    INCIDENT = "incident/raise"
    ABORT = "coord/abort"

    # Window / warmup
    WINDOW_SIZE = 10
    WARMUP_ROUNDS = 3  # min rounds before voting (below → abstain)

    # Trend-increment thresholds (consecutive increasing rounds)
    INCR_WARN_STREAK = 3
    INCR_CRITICAL_STREAK = 5

    # Fail-rate threshold
    FAIL_RATE_THRESHOLD = 0.30
    FAIL_RATE_MIN_SAMPLES = 5  # need at least this many samples to judge

    # Spike threshold
    SPIKE_MULTIPLIER = 2.0  # single round > 2x historical avg → warn
    SPIKE_MIN_HISTORY = 3   # need at least this many prior rounds

    def __init__(self, spec, bus, ctx, cfg=None) -> None:
        super().__init__(spec, bus, ctx)
        self.cfg = cfg
        # Private sliding windows (design §6.3)
        self.recover_time_window: deque = deque(maxlen=self.WINDOW_SIZE)
        self.fail_rate_window: deque = deque(maxlen=self.WINDOW_SIZE)
        self.baseline_recover_time: Optional[float] = None
        # Dedup trackers: only raise on newly-crossed thresholds
        self._last_incr_streak: int = 0
        self._last_fail_rate: float = 0.0
        self._last_spike: bool = False
        self._stopped: bool = False

    # ------------------------------------------------------------------ #
    # Round ingestion + trend detection
    # ------------------------------------------------------------------ #
    async def _on_round_done(self, message: dict) -> None:
        """Accumulate round into windows and run all trend detectors."""
        if self._stopped:
            return
        recover_time = message.get("recover_time")
        passed = bool(message.get("passed", False))
        round_no = message.get("round_no") or message.get("round")

        # Record baseline on first valid recover_time
        if self.baseline_recover_time is None and recover_time is not None:
            self.baseline_recover_time = float(recover_time)

        # Append to windows (deque maxlen handles eviction)
        if recover_time is not None:
            self.recover_time_window.append(float(recover_time))
        self.fail_rate_window.append(passed)

        # 可见性：窗口状态打印（让自治层状态可观察）。
        ts = time.strftime("%H:%M:%S", time.localtime())
        rt_str = (
            f"{recover_time:.1f}秒"
            if isinstance(recover_time, (int, float))
            else "NA"
        )
        window_size = len(self.recover_time_window)
        if window_size < self.WARMUP_ROUNDS:
            status = f"预热中({window_size}/{self.WARMUP_ROUNDS})"
        else:
            status = "已就绪"
        window_preview = list(self.recover_time_window)[-3:]
        window_str = ",".join(f"{v:.1f}" for v in window_preview)
        print(
            f"[{ts}] [趋势监督] 第 {round_no} 轮入窗: "
            f"恢复={rt_str} 通过={passed} "
            f"窗口={window_size}/{self.WINDOW_SIZE} [{window_str}] {status}"
        )

        # Run all detectors (each dedups internally)
        await self._detect_trend_increment()
        await self._detect_fail_rate()
        await self._detect_spike()

    async def _detect_trend_increment(self) -> None:
        """Detect consecutive recover_time increments (creep)."""
        window = list(self.recover_time_window)
        if len(window) < 2:
            self._last_incr_streak = 1 if window else 0
            return
        # Count consecutive strictly-increasing rounds from the end
        streak = 1
        for i in range(len(window) - 1, 0, -1):
            if window[i] > window[i - 1]:
                streak += 1
            else:
                break
        # Raise only on newly-crossed threshold (dedup)
        if (
            streak >= self.INCR_CRITICAL_STREAK
            and self._last_incr_streak < self.INCR_CRITICAL_STREAK
        ):
            await self._try_raise(
                severity="critical",
                raised_by="trend_supervisor",
                category="trend_increment_critical",
                description=(
                    f"recover_time 连续 {streak} 轮递增（疑似性能劣化）"
                ),
                evidence={
                    "trend": window[-self.INCR_CRITICAL_STREAK:],
                    "streak": streak,
                },
                suggestion="recheck",
            )
        elif (
            streak >= self.INCR_WARN_STREAK
            and self._last_incr_streak < self.INCR_WARN_STREAK
        ):
            await self._try_raise(
                severity="warn",
                raised_by="trend_supervisor",
                category="trend_increment",
                description=f"recover_time 连续 {streak} 轮递增",
                evidence={
                    "trend": window[-self.INCR_WARN_STREAK:],
                    "streak": streak,
                },
                suggestion="recheck",
            )
        self._last_incr_streak = streak

    async def _detect_fail_rate(self) -> None:
        """Detect fail rate exceeding threshold in sliding window."""
        if len(self.fail_rate_window) < self.FAIL_RATE_MIN_SAMPLES:
            self._last_fail_rate = 0.0
            return
        fails = sum(1 for p in self.fail_rate_window if not p)
        rate = fails / len(self.fail_rate_window)
        # Raise only on upward crossing (dedup)
        if (
            rate > self.FAIL_RATE_THRESHOLD
            and self._last_fail_rate <= self.FAIL_RATE_THRESHOLD
        ):
            await self._try_raise(
                severity="warn",
                raised_by="trend_supervisor",
                category="fail_rate_high",
                description=(
                    f"失败率 {rate * 100:.0f}% 超过阈值 "
                    f"{self.FAIL_RATE_THRESHOLD * 100:.0f}%"
                ),
                evidence={
                    "fails": fails,
                    "total": len(self.fail_rate_window),
                    "rate": round(rate, 3),
                },
                suggestion="recheck",
            )
        self._last_fail_rate = rate

    async def _detect_spike(self) -> None:
        """Detect single recover_time > 2x historical average."""
        window = list(self.recover_time_window)
        if len(window) <= self.SPIKE_MIN_HISTORY:
            self._last_spike = False
            return
        current = window[-1]
        history = window[:-1]
        avg = sum(history) / len(history)
        is_spike = avg > 0 and current > self.SPIKE_MULTIPLIER * avg
        # Raise only on newly-detected spike (dedup)
        if is_spike and not self._last_spike:
            await self._try_raise(
                severity="warn",
                raised_by="trend_supervisor",
                category="recover_time_spike",
                description=(
                    f"recover_time={current:.1f} 超过历史均值 {avg:.1f} 的 "
                    f"{self.SPIKE_MULTIPLIER}x"
                ),
                evidence={
                    "current": current,
                    "avg": round(avg, 2),
                    "history": history,
                },
                suggestion="recheck",
            )
        self._last_spike = is_spike

    # ------------------------------------------------------------------ #
    # Incident raising
    # ------------------------------------------------------------------ #
    async def _raise_incident(self, **kw) -> None:
        """Build a full incident message and publish to incident/raise.

        Real implementation is async. Tests may replace this with a sync
        callable to capture kwargs (see _try_raise for the bridge).
        """
        incident = {
            "incident_id": f"inc-{uuid.uuid4().hex[:8]}",
            "timestamp": time.time(),
            **kw,
        }
        await self.bus.publish(self.INCIDENT, incident)

    async def _try_raise(self, **kw) -> None:
        """Call _raise_incident, awaiting if it's a coroutine.

        This bridge supports test mocking of _raise_incident with a sync
        callable (lambda **kw: ...). When the real async _raise_incident is
        in place, calling it returns a coroutine which we then await.
        """
        # 可见性：事故 raise 打印（让自治层主动行为可观察）。
        ts = time.strftime("%H:%M:%S", time.localtime())
        severity = kw.get("severity", "?")
        category = kw.get("category", "?")
        description = kw.get("description", "")
        print(
            f"[{ts}] [趋势监督] 主动 raise 事故: "
            f"严重={severity} 类别={category} 描述={description}"
        )
        result = self._raise_incident(**kw)
        if asyncio.iscoroutine(result):
            await result

    # ------------------------------------------------------------------ #
    # Vote reply
    # ------------------------------------------------------------------ #
    def compute_vote(self, vote_request: dict) -> dict:
        """Compute a rule-based risk score for a vote request.

        Returns a vote reply dict (without req_id; the bus handler adds it).
        Abstains when insufficient data (warmup phase).
        """
        if len(self.recover_time_window) < self.WARMUP_ROUNDS:
            return {
                "voter": "trend_supervisor",
                "risk_score": 50,
                "rationale": "预热中，数据不足，弃权",
                "confidence": 0.0,
                "method": "abstain",
            }

        risk = 10  # baseline low risk for stable trend
        rationales: list = []

        # Fail-rate contribution
        fails = sum(1 for p in self.fail_rate_window if not p)
        rate = (
            fails / len(self.fail_rate_window) if self.fail_rate_window else 0.0
        )
        if rate > self.FAIL_RATE_THRESHOLD:
            risk += 40
            rationales.append(f"失败率 {rate * 100:.0f}%")

        # Trend-increment contribution
        window = list(self.recover_time_window)
        streak = 1
        for i in range(len(window) - 1, 0, -1):
            if window[i] > window[i - 1]:
                streak += 1
            else:
                break
        if streak >= self.INCR_CRITICAL_STREAK:
            risk += 50
            rationales.append(f"连续 {streak} 轮递增（critical）")
        elif streak >= self.INCR_WARN_STREAK:
            risk += 25
            rationales.append(f"连续 {streak} 轮递增")

        # Spike contribution
        if len(window) > self.SPIKE_MIN_HISTORY:
            current = window[-1]
            history = window[:-1]
            avg = sum(history) / len(history)
            if avg > 0 and current > self.SPIKE_MULTIPLIER * avg:
                risk += 30
                rationales.append(
                    f"恢复耗时突增 {current:.1f}/{avg:.1f}"
                )

        risk = max(0, min(100, risk))
        # Confidence scales with data amount (more data → more confidence)
        confidence = min(0.9, 0.4 + 0.05 * len(self.recover_time_window))

        return {
            "voter": "trend_supervisor",
            "risk_score": risk,
            "rationale": "；".join(rationales) if rationales else "趋势稳定，无异常",
            "confidence": round(confidence, 2),
            "method": "rule",
        }

    async def _on_vote_request(self, message: dict) -> None:
        """Respond to vote/request with a vote/reply (correlated by req_id)."""
        reply = self.compute_vote(message)
        reply["req_id"] = message.get("req_id")
        await self.bus.publish(self.VOTE_REPLY, reply)

    # ------------------------------------------------------------------ #
    # Abort handling
    # ------------------------------------------------------------------ #
    async def _on_abort(self, message: dict) -> None:
        """Stop processing on coord/abort."""
        self._stopped = True

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        """Subscribe to round/done + vote/request + coord/abort, then wait."""
        self.subscribe(self.ROUND_DONE, self._on_round_done)
        self.subscribe(self.VOTE_REQUEST, self._on_vote_request)
        self.subscribe(self.ABORT, self._on_abort)
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    # Standalone demo: feed a few rounds and print detections.
    from bus import EventBus
    from context import ReadOnlyContext
    from agent import AgentSpec

    bus = EventBus()
    ctx = ReadOnlyContext()
    spec = AgentSpec("trend", "trend", "", "", "", "")
    agent = TrendSupervisorAgent(spec, bus, ctx)

    async def _demo():
        async def _on_incident(msg):
            print("[事故]", msg)
        bus.subscribe("incident/raise", _on_incident)

        demo_rounds = [
            {"round": 1, "passed": True, "recover_time": 60.0},
            {"round": 2, "passed": True, "recover_time": 62.0},
            {"round": 3, "passed": True, "recover_time": 65.0},
            {"round": 4, "passed": True, "recover_time": 70.0},
            {"round": 5, "passed": True, "recover_time": 78.0},
        ]
        for r in demo_rounds:
            await agent._on_round_done(r)
            vote = agent.compute_vote({
                "round": r["round"],
                "facts": {"found": True, "changed": False},
                "question": "rate_risk_0_100",
            })
            print(f"[投票] 第 {r['round']} 轮 -> {vote}")

    asyncio.run(_demo())
