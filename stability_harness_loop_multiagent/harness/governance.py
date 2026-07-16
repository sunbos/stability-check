"""治理 —— 访问控制、配额、熔断器、预算执行。

一种位于 harness 层的守卫。它检查一个操作请求（一个普通字典），当某项
策略被违反时，发布 ``harness/abort`` —— 这与看门狗使用的、ControlLoop 监听的
是同一个接缝，因此无需 import loop/multi_agent。所有策略都与领域无关；
一个请求的描述形如::

    {"role": str, "capability": str, "operation": str,
     "cost": float, "quota_key": str (可选，默认等于 role)}

组件
----------
- ``AccessControl``  按 role -> capability -> operation 白名单执行允许/拒绝（默认拒绝）。
- ``Quota``          按 key 的滚动计数器，封顶于一个上限。
- ``Budget``         累计开销，封顶于一个上限。
- ``CircuitBreaker`` 关闭/打开/半开；在连续 N 次失败后打开。
- ``Governance``     将上述组件打包；``evaluate`` 返回 (allowed, reason, breaches)；
                     ``enforce`` 额外在违反时发布 ``harness/abort``。
- ``GovernanceAgent`` 可选的、原生挂载于总线的封装，订阅
  ``harness/govern/request`` 并通过 req_id 回复允许/拒绝。

引擎隔离：仅从本 harness 包（bus、agent）导入。
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from .agent import Agent, AgentSpec
from .bus import EventBus


class CircuitBreaker:
    """基于冷却恢复的 关闭 / 打开 / 半开 状态机。"""

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
        """当前是否允许调用。可能触发 OPEN -> HALF_OPEN 的转移。"""
        if self._state == self.CLOSED:
            return True
        if self._state == self.OPEN:
            if (time.monotonic() - self._opened_at) >= self.cooldown:
                self._state = self.HALF_OPEN
                self._probes = 0
                self._log.info("熔断器 -> 半开")
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
                self._log.info("熔断器 -> 关闭")
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
        self._log.warning("熔断器 -> 打开")


class AccessControl:
    """按 role -> capability -> operation 白名单执行的默认拒绝式 允许/拒绝。

    ``policy`` 结构::

        {role: {capability: ["op1", "op2"] | "*"}, ...}

    ``"*"`` 匹配任意的 role/capability/operation。某个 capability 的值为 ``"*"``
    时，允许该 capability 下的所有操作。
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
                return False  # 明确匹配到 capability 但操作未列出 -> 拒绝
        return None


class Quota:
    """按 key 的滚动计数器，封顶于 ``limit``。``window==0`` 表示累计计数。"""

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
    """累计开销，封顶于 ``limit``。"""

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
    """打包 访问/配额/预算/熔断 策略，并对请求网关进行强制执行。"""

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
        """返回 (allowed, reason, breaches)。有副作用：会消费配额/预算。"""
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
        """网关：仅当请求通过所有策略时才返回 True。"""
        allowed, _reason, _breaches = self.evaluate(req)
        return allowed

    # ---- 熔断器辅助方法（显式、有状态） --------------
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
    """原生挂载于总线的治理网关。对 ``harness/govern/request`` 回复允许/拒绝。"""

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
