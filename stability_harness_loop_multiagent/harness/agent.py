"""Agent base + AgentSpec — engine-neutral registration metadata and lifecycle.

An Agent holds a bus reference and a private state dict. It never reaches into
another agent directly; all interaction goes through the bus. Subscriptions
declared in the spec are wired to ``handle`` on ``start``. Override ``run`` for
proactive behaviour (self-driven loops), or ``handle`` to react to topics.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from .bus import EventBus


@dataclass
class AgentSpec:
    """Engine-neutral metadata describing an agent's role and wiring."""

    id: str
    role: str
    capabilities: Set[str] = field(default_factory=set)
    subscriptions: List[str] = field(default_factory=list)
    lifecycle_hooks: Dict[str, Callable] = field(default_factory=dict)


class Agent:
    def __init__(self, bus: EventBus, spec: AgentSpec) -> None:
        self.bus = bus
        self.spec = spec
        self.state: Dict[str, Any] = {}  # private, never shared
        self._task: Optional["asyncio.Task"] = None
        self._running = False
        self._subscriptions: List[Callable[[], None]] = []
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.agent.{spec.role}")

    # ---- identity -----------------------------------------------------
    @property
    def id(self) -> str:
        return self.spec.id

    @property
    def role(self) -> str:
        return self.spec.role

    # ---- bus helpers --------------------------------------------------
    def subscribe(self, topic: str, handler: Callable) -> Callable[[], None]:
        return self.bus.subscribe(topic, handler)

    def publish(self, topic: str, message: Any = None) -> None:
        self.bus.publish(topic, message)

    async def request(
        self, topic: str, message: Any = None, timeout: float = 1.0
    ) -> Any:
        return await self.bus.request(topic, message, timeout)

    def respond(self, incoming: Any, response: Any) -> None:
        """Reply to a request given the message that carried its req_id."""
        req_id = incoming.get("req_id") if isinstance(incoming, dict) else None
        if req_id:
            self.bus.reply(req_id, response)

    # ---- lifecycle ----------------------------------------------------
    async def start(self) -> None:
        self._running = True
        for hook in self.spec.lifecycle_hooks.get("on_start", []):
            try:
                hook(self)
            except Exception:  # noqa: BLE001
                self._log.exception("on_start hook error")
        for topic in self.spec.subscriptions:
            self._subscriptions.append(
                self.bus.subscribe(topic, self._dispatch)
            )
        self._task = asyncio.ensure_future(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        for unsub in self._subscriptions:
            unsub()
        self._subscriptions.clear()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        for hook in self.spec.lifecycle_hooks.get("on_stop", []):
            try:
                hook(self)
            except Exception:  # noqa: BLE001
                self._log.exception("on_stop hook error")

    # ---- behaviour hooks (override) ----------------------------------
    async def run(self) -> None:
        """Proactive behaviour. Default no-op; reactive agents use handle()."""
        await asyncio.sleep(0)  # pragma: no cover - default inert

    async def handle(self, topic: str, message: Any) -> None:
        """React to a subscribed topic. Override in subclasses."""
        return None

    # ---- internals ----------------------------------------------------
    async def _dispatch(self, topic: str, message: Any) -> None:
        try:
            result = self.handle(topic, message)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001
            self._log.exception("handle error topic=%r", topic)

    async def _run_loop(self) -> None:
        try:
            await self.run()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            self._log.exception("run error")


__all__ = ["Agent", "AgentSpec"]
