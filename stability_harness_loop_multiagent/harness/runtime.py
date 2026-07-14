"""Runtime — agent lifecycle registry, supervisor, and harness-level routing.

Owns the agent registry and drives the *lifecycle* of every agent in the system:
``spawn`` / ``pause`` / ``resume`` / ``shutdown`` keyed by ``AgentSpec.id``. A
background supervisor loop watches each agent task and restarts unexpectedly
failed ones (bounded retries). On ``harness/abort`` (emitted by the Watchdog or
Governance) it gracefully shuts everything down.

Message routing is intentionally delegated to the EventBus — the single cross-engine
seam. The runtime never fans messages out by hand; it only starts/stops agents and
the bus delivers topics to whatever ``AgentSpec.subscriptions`` each agent declared.
The provided ``route`` helper is a thin convenience over ``bus.publish_and_wait``.

Engine isolation: imports only from this harness package (bus, agent). It never
imports loop/ or multi_agent/.
"""

import asyncio
import logging
from typing import Callable, Dict, List, Optional

from .agent import Agent, AgentSpec
from .bus import EventBus


class Runtime:
    def __init__(
        self,
        bus: EventBus,
        *,
        telemetry=None,
        max_restarts: int = 3,
        supervisor_interval: float = 5.0,
    ) -> None:
        self.bus = bus
        self.telemetry = telemetry
        self.max_restarts = max_restarts
        self.supervisor_interval = supervisor_interval

        self._agents: Dict[str, Agent] = {}
        self._factory: Dict[str, Callable[[AgentSpec], Agent]] = {}
        self._restart_count: Dict[str, int] = {}
        # ids that are intentionally stopped (pause/shutdown) -> supervisor skips them
        self._intentional_stop: set = set()
        self._running = False
        self._supervisor_task: Optional["asyncio.Task"] = None
        self._abort_unsub: Optional[Callable[[], None]] = None
        self._abort_reason: Optional[str] = None
        self._log = logging.getLogger("stability_harness_loop_multiagent.runtime")

    # ---- registry ---------------------------------------------------
    def register(self, agent: Agent, factory: Optional[Callable[[AgentSpec], Agent]] = None) -> None:
        """Register an already-built agent under its spec.id."""
        self._agents[agent.id] = agent
        if factory is not None:
            self._factory[agent.id] = factory
        self._restart_count.setdefault(agent.id, 0)
        if self.telemetry:
            self.telemetry.metric("runtime.register", 1.0, agent=agent.id, role=agent.role)

    def spawn(self, spec: AgentSpec, factory: Callable[[AgentSpec], Agent]) -> Agent:
        """Build an agent from a spec via ``factory`` and register it."""
        agent = factory(spec)
        self.register(agent, factory)
        return agent

    def get(self, agent_id: str) -> Optional[Agent]:
        return self._agents.get(agent_id)

    @property
    def agents(self) -> Dict[str, Agent]:
        return dict(self._agents)

    def __contains__(self, agent_id: str) -> bool:
        return agent_id in self._agents

    def __len__(self) -> int:
        return len(self._agents)

    # ---- lifecycle --------------------------------------------------
    async def start_all(self) -> None:
        for agent in list(self._agents.values()):
            if agent.id in self._intentional_stop:
                continue
            await agent.start()
        if self.telemetry:
            self.telemetry.metric("runtime.started", float(len(self._agents)))

    async def start(self, agent_id: str) -> None:
        """(Re)start an agent. If it was paused, clears the intentional-stop flag."""
        agent = self._agents.get(agent_id)
        if agent is None:
            raise KeyError(agent_id)
        self._intentional_stop.discard(agent_id)
        await agent.start()

    async def pause(self, agent_id: str) -> None:
        """Stop an agent's task but keep it registered so it can be resumed."""
        agent = self._agents.get(agent_id)
        if agent is None:
            raise KeyError(agent_id)
        self._intentional_stop.add(agent_id)
        await agent.stop()
        if self.telemetry:
            self.telemetry.metric("runtime.pause", 1.0, agent=agent_id)

    async def resume(self, agent_id: str) -> None:
        await self.start(agent_id)

    async def shutdown(self, agent_id: str) -> None:
        agent = self._agents.get(agent_id)
        if agent is None:
            return
        self._intentional_stop.add(agent_id)
        self._factory.pop(agent_id, None)
        await agent.stop()
        self._agents.pop(agent_id, None)
        self._restart_count.pop(agent_id, None)
        if self.telemetry:
            self.telemetry.metric("runtime.shutdown", 1.0, agent=agent_id)

    async def shutdown_all(self) -> None:
        for agent_id in list(self._agents.keys()):
            await self.shutdown(agent_id)
        self._running = False

    # ---- harness-level routing -------------------------------------
    async def route(self, topic: str, message=None) -> None:
        """Fan a message out to all subscribers via the bus (await completion)."""
        await self.bus.publish_and_wait(topic, message)

    # ---- abort + supervisor ----------------------------------------
    def _on_abort(self, topic: str, message) -> None:
        reason = (message or {}).get("reason", "harness abort")
        self._abort_reason = reason
        self._log.warning("runtime received harness/abort: %s", reason)
        # Stop the supervisor loop; run()'s finally block performs the shutdown.
        self._running = False

    async def _supervise(self) -> None:
        while self._running:
            await asyncio.sleep(self.supervisor_interval)
            for agent_id, agent in list(self._agents.items()):
                if agent_id in self._intentional_stop:
                    continue
                task = agent._task
                if task is None or not task.done():
                    continue
                if task.cancelled():
                    # clean cancellation -> treat as intentional stop
                    self._intentional_stop.add(agent_id)
                    continue
                # A supervised, long-running agent finishing (whether by a normal
                # return or an exception swallowed inside Agent._run_loop) has
                # died and should be kept alive -> bounded restart.
                self._restart_count[agent_id] = self._restart_count.get(agent_id, 0) + 1
                n = self._restart_count[agent_id]
                if n > self.max_restarts:
                    self._log.error(
                        "agent %s exceeded max_restarts=%d; leaving dead",
                        agent_id, self.max_restarts,
                    )
                    self._intentional_stop.add(agent_id)
                    continue
                self._log.warning(
                    "supervisor restarting agent %s (attempt %d/%d)",
                    agent_id, n, self.max_restarts,
                )
                try:
                    await agent.stop()  # clear stale subscriptions
                    await agent.start()
                except Exception:  # noqa: BLE001
                    self._log.exception("restart failed for %s", agent_id)
                if self.telemetry:
                    self.telemetry.metric(
                        "runtime.restart", 1.0, agent=agent_id, attempt=n
                    )

    async def run(self) -> None:
        """Start all agents, watch ``harness/abort``, and run the supervisor until abort."""
        self._running = True
        self._abort_unsub = self.bus.subscribe("harness/abort", self._on_abort)
        await self.start_all()
        try:
            self._supervisor_task = asyncio.ensure_future(self._supervise())
            await self._supervisor_task
        finally:
            if self._abort_unsub is not None:
                self._abort_unsub()
                self._abort_unsub = None
            await self.shutdown_all()


__all__ = ["Runtime"]
