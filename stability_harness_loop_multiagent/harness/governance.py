"""Governance — access control, quota, circuit breaker, budget enforcement.

A harness-level guard. It inspects an operation request (a plain dict) and, when a
policy is breached, publishes ``harness/abort`` — the same single seam the Watchdog
uses and the ControlLoop listens to — so no loop/multi_agent imports are required. All
policies are domain-agnostic; a request is described as::

    {"role": str, "capability": str, "operation": str,
     "cost": float, "quota_key": str (optional, defaults to role)}

Components
----------
- ``AccessControl``  allow/deny by role -> capability -> operation whitelist (default deny).
- ``Quota``          per-key rolling counter capped at a ceiling.
- ``Budget``         cumulative spend capped at a ceiling.
- ``CircuitBreaker`` closed/open/half-open; opens after N consecutive failures.
- ``Governance``     bundles the above; ``evaluate`` returns (allowed, reason, breaches);
                     ``enforce`` additionally publishes ``harness/abort`` on breach.
- ``GovernanceAgent`` optional bus-native wrapper that subscribes to
  ``harness/govern/request`` and replies allow/deny via req_id.

Engine isolation: imports only from this harness package (bus, agent).
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from .agent import Agent, AgentSpec
from .bus import EventBus


class CircuitBreaker:
    """Closed / Open / Half-open state machine with cooldown-based recovery."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"

    def __init__(
        self,
        failure_threshold: int = 5,
        cooldown: float = 30.0,
        half_open_probes: int = 1,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self.half_open_probes = half_open_probes
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probes = 0
        self._log = logging.getLogger("stability_harness_loop_multiagent.governance.breaker")

    @property
    def state(self) -> str:
        return self._state

    def allow(self) -> bool:
        """Whether a call is permitted now. May transition OPEN -> HALF_OPEN."""
        if self._state == self.CLOSED:
            return True
        if self._state == self.OPEN:
            if (time.monotonic() - self._opened_at) >= self.cooldown:
                self._state = self.HALF_OPEN
                self._probes = 0
                self._log.info("circuit breaker -> half-open")
                return True
            return False
        # HALF_OPEN
        return self._probes < self.half_open_probes

    def record_success(self) -> None:
        if self._state == self.HALF_OPEN:
            self._probes += 1
            if self._probes >= self.half_open_probes:
                self._state = self.CLOSED
                self._failures = 0
                self._log.info("circuit breaker -> closed")
        elif self._state == self.OPEN:
            self._state = self.CLOSED
            self._failures = 0

    def record_failure(self) -> None:
        if self._state == self.HALF_OPEN:
            self._open()
            return
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._open()

    def reset(self) -> None:
        self._state = self.CLOSED
        self._failures = 0
        self._probes = 0

    def _open(self) -> None:
        self._state = self.OPEN
        self._opened_at = time.monotonic()
        self._failures = 0
        self._log.warning("circuit breaker -> open")


class AccessControl:
    """Default-deny allow/deny by role -> capability -> operation whitelist.

    ``policy`` shape::

        {role: {capability: ["op1", "op2"] | "*"}, ...}

    ``"*"`` matches any role/capability/operation. A capability value of ``"*"``
    allows every operation under that capability.
    """

    def __init__(self, policy: Optional[Dict[str, Dict[str, Any]]] = None, default_deny: bool = True) -> None:
        self._policy = policy or {}
        self.default_deny = default_deny

    def allow(self, role: str, capability: str = "*", operation: str = "*") -> Tuple[bool, str]:
        allowed = self._lookup(role, capability, operation)
        if allowed is not None:
            return (True, "allowed") if allowed else (False, f"deny {role}/{capability}/{operation}")
        if self.default_deny:
            return (False, f"deny {role}/{capability}/{operation} (no rule)")
        return (True, "allowed (default permit)")

    def _lookup(self, role: str, capability: str, operation: str) -> Optional[bool]:
        for r in (role, "*"):
            caps = self._policy.get(r)
            if not caps:
                continue
            for c in (capability, "*"):
                ops = caps.get(c)
                if ops is None:
                    continue
                if ops == "*" or operation == "*":
                    return True
                if operation in ops:
                    return True
                return False  # explicit capability matched but op not listed -> deny
        return None


class Quota:
    """Per-key rolling counter capped at ``limit``. ``window==0`` => cumulative."""

    def __init__(self, limit: int, window: float = 0.0) -> None:
        self.limit = limit
        self.window = window
        self._counts: Dict[str, int] = {}
        self._first: Dict[str, float] = {}

    def consume(self, key: str, amount: int = 1) -> Tuple[bool, str]:
        now = time.monotonic()
        if self.window > 0:
            first = self._first.get(key)
            if first is None or (now - first) >= self.window:
                self._counts[key] = 0
                self._first[key] = now
        used = self._counts.get(key, 0) + amount
        if used > self.limit:
            return (False, f"quota exceeded for {key}: {used}/{self.limit}")
        self._counts[key] = used
        return (True, "ok")

    def reset(self, key: str = None) -> None:
        if key is None:
            self._counts.clear()
            self._first.clear()
        else:
            self._counts.pop(key, None)
            self._first.pop(key, None)


class Budget:
    """Cumulative spend capped at ``limit``."""

    def __init__(self, limit: float) -> None:
        self.limit = limit
        self._spent = 0.0

    def spend(self, amount: float) -> Tuple[bool, str]:
        self._spent += float(amount)
        if self._spent > self.limit:
            return (False, f"budget exceeded: {self._spent}/{self.limit}")
        return (True, "ok")

    def remaining(self) -> float:
        return max(0.0, self.limit - self._spent)

    def reset(self) -> None:
        self._spent = 0.0


class Governance:
    """Bundles access/quota/budget/breaker policies and enforces a request gate."""

    def __init__(
        self,
        bus: Optional[EventBus] = None,
        *,
        access: Optional[AccessControl] = None,
        quotas: Optional[Dict[str, Quota]] = None,
        budgets: Optional[Dict[str, Budget]] = None,
        breakers: Optional[Dict[str, CircuitBreaker]] = None,
        emit_abort: bool = True,
        telemetry=None,
    ) -> None:
        self.bus = bus
        self.access = access
        self.quotas = quotas or {}
        self.budgets = budgets or {}
        self.breakers = breakers or {}
        self.emit_abort = emit_abort
        self.telemetry = telemetry
        self._log = logging.getLogger("stability_harness_loop_multiagent.governance")

    def evaluate(self, req: Dict[str, Any]) -> Tuple[bool, str, List[Tuple[str, str]]]:
        """Return (allowed, reason, breaches). Side-effecting: consumes quota/budget."""
        breaches: List[Tuple[str, str]] = []
        role = req.get("role", "")
        cap = req.get("capability", "*")
        op = req.get("operation", "*")
        cost = float(req.get("cost", 0.0))
        quota_key = req.get("quota_key", role)

        if self.access is not None:
            ok, reason = self.access.allow(role, cap, op)
            if not ok:
                breaches.append(("access", reason))

        for name, q in self.quotas.items():
            ok, reason = q.consume(quota_key)
            if not ok:
                breaches.append((f"quota:{name}", reason))

        for name, b in self.budgets.items():
            ok, reason = b.spend(cost)
            if not ok:
                breaches.append((f"budget:{name}", reason))

        allowed = not breaches
        reason = "; ".join(r for _, r in breaches) if breaches else "ok"
        if self.telemetry:
            self.telemetry.metric(
                "governance.evaluate", 0.0 if allowed else 1.0, allowed=allowed
            )
        if not allowed and self.emit_abort and self.bus is not None:
            self.bus.publish(
                "harness/abort",
                {
                    "reason": f"governance breach: {reason}",
                    "breaches": [n for n, _ in breaches],
                },
            )
        return (allowed, reason, breaches)

    def enforce(self, req: Dict[str, Any]) -> bool:
        """Gate: return True only if the request passes every policy."""
        allowed, _reason, _breaches = self.evaluate(req)
        return allowed

    # ---- circuit breaker helpers (explicit, stateful) --------------
    def breaker_allow(self, name: str) -> bool:
        b = self.breakers.get(name)
        return True if b is None else b.allow()

    def breaker_record(self, name: str, success: bool) -> None:
        b = self.breakers.get(name)
        if b is None:
            return
        if success:
            b.record_success()
        else:
            b.record_failure()


class GovernanceAgent(Agent):
    """Bus-native governance gate. Replies allow/deny to ``harness/govern/request``."""

    def __init__(
        self,
        bus: EventBus,
        governance: Governance,
        *,
        topic: str = "harness/govern/request",
    ) -> None:
        super().__init__(
            bus,
            AgentSpec(
                id="governance",
                role="governance",
                capabilities={"access-control", "quota", "circuit-breaker", "budget"},
                subscriptions=[topic],
            ),
        )
        self.gov = governance
        self.topic = topic

    async def handle(self, topic: str, message) -> None:
        if topic != self.topic:
            return
        req = message if isinstance(message, dict) else {}
        allowed, reason, _breaches = self.gov.evaluate(req)
        self.respond(message, {"allowed": allowed, "reason": reason})


__all__ = [
    "CircuitBreaker",
    "AccessControl",
    "Quota",
    "Budget",
    "Governance",
    "GovernanceAgent",
]
