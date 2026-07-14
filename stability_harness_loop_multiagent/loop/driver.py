"""ControlLoop — the deterministic loop engine (sense->plan->act->check->decide->halt).

Drives entirely through the bus (no direct engine imports). It:
  - publishes ``loop/tick`` (sense/plan/act trigger for workers),
  - gathers ``target/recovered`` and ``target/checked`` facts (with timeouts),
  - requests votes via ``loop/vote/request`` and gathers ``agent/vote/reply``,
  - applies the DecisionAuthority, appends a RoundRecord to SharedContext,
  - publishes ``loop/done``; on recheck publishes ``loop/recheck`` (max once);
  - acks every incident raised by others, and halts on termination/harness/abort.

Holds the authoritative verdict. Risk combination is a local deterministic
helper (engine-isolation: the canonical ``combine_votes`` lives in multi_agent/protocols
for MAS-side aggregation; this loop keeps its own to avoid importing multi_agent).
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from ..harness.agent import Agent, AgentSpec
from ..harness.bus import EventBus
from .context import RoundRecord, SharedContext
from .decision import (
    CONSERVATIVE_RISK,
    NEUTRAL_RISK,
    DecisionAuthority,
    Verdict,
)
from .scheduler import RetryBudget, Scheduler
from .termination import (
    CountStop,
    DurationStop,
    FailThresholdStop,
    StopCondition,
    TerminationPolicy,
)


class ControlLoop(Agent):
    def __init__(
        self,
        bus: EventBus,
        ctx: SharedContext,
        decision: DecisionAuthority,
        termination: TerminationPolicy,
        *,
        vote_timeout: float = 1.0,
        recover_timeout: float = 30.0,
        check_timeout: float = 30.0,
        recheck_limit: int = 1,
        combine: Callable[[List[Dict[str, Any]]], float] = None,
        scheduler: Optional[Scheduler] = None,
        telemetry=None,
    ) -> None:
        super().__init__(
            bus,
            AgentSpec(
                id="control-loop",
                role="coordinator",
                capabilities={"decision", "orchestration"},
                subscriptions=["loop/recheck", "agent/incident", "harness/abort"],
            ),
        )
        self.ctx = ctx
        self.decision = decision
        self.termination = termination
        self.vote_timeout = vote_timeout
        self.recover_timeout = recover_timeout
        self.check_timeout = check_timeout
        self.recheck_limit = recheck_limit
        self.combine = combine or self._default_combine
        # Pacing/backoff/retry-budget engine. Defaults keep the loop safe: a
        # 1s base interval avoids a busy-spin, and the adaptive formula stretches
        # the cooldown for slow targets automatically.
        self.scheduler = scheduler or Scheduler()
        self.telemetry = telemetry

        self._verdict: Optional[Verdict] = None
        self._has_critical = False
        self._incidents: List[Dict[str, Any]] = []
        self._recheck_pending = 0
        self._round_no = 0
        self._stop = False
        self._log = logging.getLogger("stability_harness_loop_multiagent.loop")

    # ---- authoritative verdict ---------------------------------------
    @property
    def verdict(self) -> Optional[Verdict]:
        return self._verdict

    # ---- main loop ----------------------------------------------------
    async def run(self) -> None:
        while not self._stop:
            halt, reason = self.termination.should_halt(self.ctx.snapshot())
            if halt:
                self._halt(reason)
                break
            recover_time = await self._run_round()
            if self.ctx.aborted:
                break
            # Pace the loop to the next round using the scheduler. A slow target
            # (large recover_time) gets a longer cooldown automatically; a fast
            # one stays compact. This is the loop's only inter-round wait, so a
            # stuck scheduler can't deadlock the loop.
            interval = self.scheduler.interval(recover_time)
            if interval and interval > 0:
                await asyncio.sleep(interval)

    # ---- one iteration ------------------------------------------------
    async def _run_round(self) -> Optional[float]:
        self._round_no += 1
        if self.telemetry:
            self.telemetry.metric("loop.round", self._round_no)

        # Subscribe synchronously BEFORE publishing loop/tick, so the worker's
        # fire-and-forget target/* publications are never missed.
        recover_time: Optional[float] = None
        rec_start = time.time()

        def _on_recover(msg: Any) -> None:
            nonlocal recover_time
            if recover_time is None and isinstance(msg, dict) and msg.get("recovered"):
                recover_time = time.time() - rec_start

        rec_unsub, rec_buf = self._start_collect("target/recovered", on_first=_on_recover)
        chk_unsub, chk_buf = self._start_collect("target/checked")
        self.bus.publish("loop/tick", {"round": self._round_no})
        await asyncio.sleep(max(self.recover_timeout, self.check_timeout))
        rec_unsub()
        chk_unsub()
        recovered_list = rec_buf
        facts_list = chk_buf

        facts = self._merge_facts(facts_list, recovered_list)
        votes, risk, critical, verdict = [], NEUTRAL_RISK, False, None
        try:
            votes = await self._collect_votes()
            risk = self.combine(votes)
            critical = self._has_critical
            verdict = self.decision.decide(facts, risk, critical)
        except Exception:  # noqa: BLE001
            # Conservative fallback: never an optimistic pass. Voting timeout /
            # all-abstain already yields NEUTRAL_RISK=50 via combine; here we
            # guard the whole decide path and fall back to warn(60).
            self._log.exception("decision error -> conservative warn(60)")
            try:
                verdict = self.decision.decide(
                    facts, NEUTRAL_RISK, self._has_critical, error=True
                )
                risk = CONSERVATIVE_RISK
            except Exception:  # noqa: BLE001
                verdict = Verdict(
                    "warn", risk_score=CONSERVATIVE_RISK,
                    critical=self._has_critical,
                    reason="decision error -> conservative warn(60)",
                )
                risk = CONSERVATIVE_RISK
        self._verdict = verdict

        record = RoundRecord(
            round_no=self._round_no,
            facts=facts,
            verdict=verdict.decision,
            risk_score=risk,
            critical=critical,
            timestamp=time.time(),
        )
        self.ctx.append_round(record)

        self.bus.publish(
            "loop/done",
            {
                "round": self._round_no,
                "verdict": verdict.decision,
                "risk": risk,
                "critical": critical,
                "facts": facts,
                "recover_time": recover_time,
            },
        )
        if self.telemetry:
            self.telemetry.metric(
                "loop.verdict",
                1.0,
                decision=verdict.decision,
                risk=risk,
                round=self._round_no,
            )

        # recheck (bounded)
        if verdict.decision == "recheck" and self._recheck_pending < self.recheck_limit:
            self._recheck_pending += 1
            self.bus.publish("loop/recheck", {"round": self._round_no})
        else:
            self._recheck_pending = 0

        # ack incidents raised by others (never our own)
        for inc in self._incidents:
            self.bus.publish(
                "agent/incident/ack",
                {"req_id": inc.get("req_id"), "incident": inc},
            )
        self._incidents.clear()
        self._has_critical = False
        return recover_time

    # ---- incoming events ---------------------------------------------
    async def handle(self, topic: str, message: Any) -> None:
        if topic == "agent/incident":
            sev = (message or {}).get("severity", "warn")
            if sev == "critical":
                self._has_critical = True
            self._incidents.append(message or {})
        elif topic == "harness/abort":
            reason = (message or {}).get("reason", "watchdog abort")
            self._halt(reason)
        elif topic == "loop/recheck":
            pass  # recheck is driven from within _run_round

    # ---- helpers ------------------------------------------------------
    def _halt(self, reason: str) -> None:
        self._stop = True
        self.ctx.mark_aborted(reason)
        self.bus.publish("loop/abort", {"reason": reason})

    def _start_collect(self, reply_topic: str, on_first=None):
        """Synchronously subscribe a collector; returns (unsub, buffer).

        ``on_first`` (optional callable(msg)) is invoked once on the first
        matching message — used to timestamp target recovery without coupling
        the collector buffer to timing logic.
        """
        buf: List[Any] = []
        seen = {"first": False}

        def handler(_t, msg):
            buf.append(msg)
            if on_first is not None and not seen["first"]:
                seen["first"] = True
                on_first(msg)

        unsub = self.bus.subscribe(reply_topic, handler)
        return unsub, buf

    async def _gather(self, reply_topic: str, timeout: float) -> List[Any]:
        unsub, buf = self._start_collect(reply_topic)
        try:
            await asyncio.sleep(timeout)
        finally:
            unsub()
        return buf

    async def _collect_votes(self) -> List[Dict[str, Any]]:
        unsub, replies = self._start_collect("agent/vote/reply")
        self.bus.publish("loop/vote/request", {"round": self._round_no})
        try:
            await asyncio.sleep(self.vote_timeout)
        finally:
            unsub()
        return [m for m in replies if isinstance(m, dict)]

    def _merge_facts(self, facts_list, recovered_list) -> Dict[str, Any]:
        facts: Dict[str, Any] = {}
        for msg in facts_list:
            if isinstance(msg, dict):
                facts.update(msg.get("facts", {}) or {})
        if not facts:
            facts["checks_received"] = False  # no worker checked -> fail
        # recovery: any False (or no reply) => not recovered
        if recovered_list:
            recovered = all(
                (m.get("recovered") if isinstance(m, dict) else bool(m))
                for m in recovered_list
            )
        else:
            recovered = False
        facts["recovered"] = recovered
        return facts

    @staticmethod
    def _default_combine(votes: List[Dict[str, Any]]) -> float:
        # fast-path: any risk >= 90 wins immediately
        for v in votes:
            if v.get("risk", 0) >= 90:
                return float(v["risk"])
        num = 0.0
        den = 0.0
        for v in votes:
            risk = float(v.get("risk", 50.0))
            conf = float(v.get("confidence", 0.0))
            w = float(v.get("weight", 1.0))
            if conf <= 0:  # abstain
                continue
            num += risk * w * conf
            den += w * conf
        if den == 0:
            return 50.0  # neutral default when all abstain
        return num / den


@dataclass
class RunConfig:
    """Declarative control-loop run parameters (generic vocabulary).

    Maps high-level knobs onto a ``TerminationPolicy`` + the ``ControlLoop``
    timeouts. No concrete scenario is baked in — the caller supplies the
    worker / advisor / observer / adapter. ``max_duration=0`` (default)
    disables the duration stop; ``fail_threshold=0`` disables the fail stop;
    both are OR-combined with ``max_rounds`` (always honoured when > 0).
    """

    max_rounds: int = 10
    max_duration: float = 0.0
    fail_threshold: int = 0
    fail_consecutive: int = 0
    vote_timeout: float = 0.5
    recover_timeout: float = 1.0
    check_timeout: float = 1.0
    recheck_limit: int = 1

    def build_termination(self) -> TerminationPolicy:
        conds: List[StopCondition] = []
        if self.max_rounds:
            conds.append(CountStop(self.max_rounds))
        if self.max_duration:
            conds.append(DurationStop(self.max_duration))
        if self.fail_threshold or self.fail_consecutive:
            conds.append(
                FailThresholdStop(
                    cumulative=self.fail_threshold,
                    consecutive=self.fail_consecutive,
                )
            )
        return TerminationPolicy(conds)


__all__ = ["ControlLoop", "RunConfig"]
