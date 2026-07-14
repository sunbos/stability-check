"""Smoke test / self-contained demo for the generic stability_harness_loop_multiagent framework.

This proves the generic template RUNS end-to-end with NO concrete scenario baked
in. It wires the three engines purely through the EventBus:

    harness : EventBus, Telemetry, Watchdog (liveness / deadlock detector)
    loop    : ControlLoop + RunConfig + DecisionAuthority + TerminationPolicy
    multi_agent     : FakeTargetAdapter + WorkerAgent + AdvisorAgent + ObserverAgent

The only "target" is a synthetic in-memory counter (FakeTargetAdapter): the
worker increments it on every act() and reports synthetic state/events. No
real device, service, or domain is involved, so the demo stays generic.

Run directly:  python stability_harness_loop_multiagent/examples/smoke.py
Used by tests: tests/test_stability_harness_loop_multiagent_smoke.py imports run_smoke / the roles here.

Standard library only; asserts via exceptions (raises == failure).
"""

import asyncio
import os
import sys

# Make the stability_harness_loop_multiagent package importable when run as a bare script from the repo
# root (python stability_harness_loop_multiagent/examples/smoke.py).
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from stability_harness_loop_multiagent import (
    AdvisorAgent,
    AgentSpec,
    ControlLoop,
    DecisionAuthority,
    EventBus,
    ObserverAgent,
    RunConfig,
    SharedContext,
    Scheduler,
    Telemetry,
    Watchdog,
    WorkerAgent,
)
from stability_harness_loop_multiagent.harness.telemetry import MemorySink
from stability_harness_loop_multiagent.multi_agent.adapter import Event, Result, State, TargetAdapter


# --------------------------------------------------------------------------
# Fake target — a synthetic, scenario-agnostic "thing" the MAS acts upon.
# --------------------------------------------------------------------------
class FakeTargetAdapter:
    """A counter that increments on every act(); reports synthetic state/events.

    Implements the TargetAdapter *protocol* structurally (no subclass needed):
    act() -> Result, observe() -> State, events(since) -> List[Event].
    """

    def __init__(self, fail: bool = False) -> None:
        self.counter = 0
        self.fail = fail  # when True, observe() reports an unhealthy state

    def act(self, operation) -> Result:
        self.counter += 1
        return Result(ok=True, data={"counter": self.counter, "op": operation})

    def observe(self) -> State:
        return State(snapshot={"up": not self.fail, "counter": self.counter})

    def events(self, since: float):
        evs = [Event(kind="acted", payload={"counter": self.counter}, ts=since)]
        if self.fail:
            evs.append(
                Event(kind="degraded", payload={"reason": "injected-failure"}, ts=since)
            )
        return evs


# --------------------------------------------------------------------------
# MAS roles — generic; no concrete domain knowledge.
# --------------------------------------------------------------------------
class FakeWorker(WorkerAgent):
    """Worker that drives the FakeTargetAdapter.

    Publishes the standard pipeline on every loop/tick:
        target/acted, target/recovered, target/checked, agent/<role>/done
    """

    def check(self, tick: dict) -> dict:
        snap = self.adapter.observe().snapshot
        up = isinstance(snap, dict) and bool(snap.get("up", True))
        return {"acted": True, "state_ok": up}


class FixedAdvisor(AdvisorAgent):
    """Minimal advisor: votes a fixed (risk, confidence) every round."""

    def __init__(self, bus, spec, *, risk: float = 30.0, confidence: float = 0.9,
                 weight: float = 1.0) -> None:
        super().__init__(bus, spec, weight=weight)
        self._risk = float(risk)
        self._confidence = float(confidence)

    def vote(self):
        return (self._risk, self._confidence)


class PrintingObserver(ObserverAgent):
    """Observer that records every event it sees and prints loop/done summaries."""

    def __init__(self, bus, spec) -> None:
        super().__init__(bus, spec)
        self.seen = []

    def on_event(self, topic: str, message) -> None:
        self.seen.append((topic, message))
        if topic == "loop/done":
            v = (message or {}).get("verdict")
            r = (message or {}).get("risk")
            print(f"[observer] round {message.get('round')} verdict={v} risk={r}")


# --------------------------------------------------------------------------
# End-to-end driver. Returns a dict of artifacts for inspection / assertions.
# --------------------------------------------------------------------------
async def run_smoke(
    fail: bool = False,
    max_rounds: int = 5,
    *,
    run_timeout: float = 30.0,
) -> dict:
    bus = EventBus()
    mem = MemorySink()
    tel = Telemetry(bus=bus, sinks=[mem])

    ctx = SharedContext(baseline={"kind": "fake"}, strategy_text="smoke-demo")
    decision = DecisionAuthority()

    # RunConfig -> TerminationPolicy. max_duration=0 disables the duration stop;
    # a huge fail_threshold keeps the loop alive until max_rounds is hit.
    cfg = RunConfig(max_rounds=max_rounds, max_duration=0.0, fail_threshold=10_000)
    term = cfg.build_termination()

    loop = ControlLoop(
        bus,
        ctx,
        decision,
        term,
        # tiny timeouts so the demo finishes in well under a second
        vote_timeout=0.1,
        recover_timeout=0.05,
        check_timeout=0.05,
        recheck_limit=0,
        # no inter-round idle so the whole run is fast & deterministic
        scheduler=Scheduler(base=0.0, min_interval=0.0),
        telemetry=tel,
    )

    adapter = FakeTargetAdapter(fail=fail)
    worker = FakeWorker(
        bus, AgentSpec(id="w1", role="fake", capabilities={"act"}), adapter
    )
    advisor = FixedAdvisor(
        bus, AgentSpec(id="a1", role="risk"), risk=30.0, confidence=0.9, weight=1.0
    )
    observer = PrintingObserver(
        bus,
        AgentSpec(
            id="o1",
            role="scribe",
            subscriptions=["loop/done", "target/#", "agent/#"],
        ),
    )
    # Generous stall budget: the watchdog must NOT abort this healthy run.
    dog = Watchdog(bus, stall_timeout=300.0, check_interval=0.05)

    # Start all agents; the loop is started as an agent and awaited to completion.
    for a in (worker, advisor, observer, dog):
        await a.start()
    await loop.start()

    try:
        # Deterministic termination: the loop halts on CountStop(max_rounds).
        # wait_for doubles as a deadlock detector — a stuck loop times out.
        await asyncio.wait_for(loop._task, run_timeout)
    finally:
        for a in (worker, advisor, observer, dog):
            await a.stop()

    return {
        "ctx": ctx,
        "loop": loop,
        "observer": observer,
        "adapter": adapter,
        "telemetry": tel,
        "config": cfg,
    }


def assert_healthy(result: dict) -> None:
    ctx = result["ctx"]
    loop = result["loop"]
    observer = result["observer"]
    adapter = result["adapter"]

    # 1) loop reaches termination
    assert ctx.round_count >= 1, "loop produced no rounds"
    assert ctx.aborted, "loop did not reach a termination state"
    # exactly max_rounds rounds ran
    assert ctx.round_count == result["config"].max_rounds, (
        f"expected {result['config'].max_rounds} rounds, got {ctx.round_count}"
    )
    # 2) verdicts produced
    assert loop.verdict is not None, "no authoritative verdict was set"
    history = ctx.snapshot().round_history
    assert len(history) == ctx.round_count
    assert all(r.verdict == "pass" for r in history), (
        "healthy run should produce only 'pass' verdicts: "
        f"{[r.verdict for r in history]}"
    )
    # 3) observers received events (loop/done at minimum)
    assert observer.seen, "observer received no events"
    assert any(t == "loop/done" for t, _ in observer.seen)
    # 4) the worker actually acted each round through the fake adapter
    assert adapter.counter == ctx.round_count, (
        f"worker acted {adapter.counter} times but {ctx.round_count} rounds ran"
    )


def assert_failing_fact(result: dict) -> None:
    ctx = result["ctx"]
    loop = result["loop"]
    history = ctx.snapshot().round_history

    # fact dictatorship: an injected failing fact must yield a 'fail' verdict,
    # even though the advisor confidently voted low risk (30).
    assert any(r.verdict == "fail" for r in history), (
        "failing fact should force at least one 'fail' verdict: "
        f"{[r.verdict for r in history]}"
    )
    assert loop.verdict is not None
    # the final verdict reflects the failure
    assert ctx.snapshot().round_history[-1].verdict == "fail"
    # sanity: a fact was actually false
    assert any(
        not ok for r in history for ok in r.facts.values()
    ), "expected at least one falsy fact in the recorded rounds"


async def _main() -> None:
    print("=== stability_harness_loop_multiagent generic smoke (healthy scenario) ===")
    healthy = await run_smoke(fail=False, max_rounds=5)
    assert_healthy(healthy)
    print(
        f"OK healthy: rounds={healthy['ctx'].round_count} "
        f"verdicts={[r.verdict for r in healthy['ctx'].snapshot().round_history]} "
        f"observer_events={len(healthy['observer'].seen)} "
        f"acts={healthy['adapter'].counter}"
    )

    print("\n=== stability_harness_loop_multiagent generic smoke (fact-dictatorship scenario) ===")
    failing = await run_smoke(fail=True, max_rounds=5)
    assert_failing_fact(failing)
    print(
        f"OK failing: rounds={failing['ctx'].round_count} "
        f"verdicts={[r.verdict for r in failing['ctx'].snapshot().round_history]}"
    )

    print("\nALL SMOKE ASSERTIONS PASSED")


if __name__ == "__main__":
    asyncio.run(_main())
