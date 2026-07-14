"""ExampleWorkerAgent — a generic demonstration of the WorkerAgent contract.

Uses a TargetAdapter to *act* and *observe* on a GENERIC target (no concrete
device, no burn-in specifics). It shows how to override ``do_work`` / ``recover``
/ ``check`` and verifies the publish pipeline:

    loop/tick -> act() -> target/acted, target/recovered, target/checked,
                             agent/<role>/done

To use this against a real system, supply a concrete ``TargetAdapter``
implementation (e.g. a device / service / resource adapter) — no other change is
needed. WorkerAgents are execution-only: they never decide pass/fail.
"""

import asyncio
import logging

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from .base import WorkerAgent
from ..adapter import TargetAdapter, State


class ExampleWorkerAgent(WorkerAgent):
    """Generic worker: cycles act -> recover -> check on each ``loop/tick``."""

    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        adapter: TargetAdapter,
        *,
        operation: str = "ping",
        recover_polls: int = 3,
        recover_interval: float = 0.5,
        facts: dict = None,
    ) -> None:
        super().__init__(bus, spec, adapter)
        self.operation = operation
        self.recover_polls = recover_polls
        self.recover_interval = recover_interval
        self._facts = dict(facts or {})
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.worker.{self.role}")

    # ---- act ----------------------------------------------------------
    def do_work(self, tick: dict):
        """Invoke the adapter's ``act()``. Returns whatever the adapter reports."""
        op = tick.get("operation", self.operation)
        result = self.adapter.act(op)
        self._log.info("acted op=%r ok=%s", op, result.ok)
        return result

    # ---- recover (polled readiness) -----------------------------------
    async def recover(self, tick: dict) -> bool:
        """Poll ``adapter.observe()`` until the target reports readiness.

        Generic readiness rule: a target is considered recovered unless it
        explicitly reports ``{"up": False}``. Override for domain-specific
        stabilization (e.g. wait for a known-good state snapshot).
        """
        last = None
        for _ in range(self.recover_polls):
            state = self.adapter.observe()
            snap = state.snapshot if isinstance(state, State) else state
            last = snap
            if isinstance(snap, dict) and snap.get("up") is False:
                await asyncio.sleep(self.recover_interval)
                continue
            return True
        # never observed a clear failure -> treat as recovered
        return True

    # ---- check (fact production) --------------------------------------
    def check(self, tick: dict) -> dict:
        """Return fact checks consumed by the DecisionAuthority.

        Generic example facts: the act produced a result and the observed state
        is healthy. Override to assert domain invariants.
        """
        facts = dict(self._facts)
        try:
            state = self.adapter.observe()
            snap = state.snapshot if isinstance(state, State) else state
        except Exception:  # noqa: BLE001 - observation failure is a failed fact
            snap = None
        facts.setdefault("acted", True)
        if isinstance(snap, dict):
            facts.setdefault("state_ok", bool(snap.get("up", True)))
        return facts


__all__ = ["ExampleWorkerAgent"]


if __name__ == "__main__":  # run with: python -m stability_harness_loop_multiagent.multi_agent.workers.example
    import time

    from ...harness.bus import EventBus
    from ..adapter import Result, State

    class StubAdapter:
        def act(self, operation):
            return Result(ok=True, data={"op": operation})

        def observe(self):
            return State(snapshot={"up": True})

        def events(self, since):
            return []

    async def demo() -> None:
        bus = EventBus()
        collected = []

        def collect(_t, msg):
            collected.append(msg)

        for t in ("target/acted", "target/recovered", "target/checked",
                  "agent/example/done"):
            bus.subscribe(t, collect)

        worker = ExampleWorkerAgent(
            bus, AgentSpec(id="w1", role="example"), StubAdapter()
        )
        await worker.act({"round": 1, "operation": "demo"})
        await asyncio.sleep(0.05)  # let fire-and-forget handlers flush
        print(f"published {len(collected)} messages:")
        for m in collected:
            print("  ", m)
        worker._log.info("demo done at t=%.2f", time.time())

    asyncio.run(demo())
