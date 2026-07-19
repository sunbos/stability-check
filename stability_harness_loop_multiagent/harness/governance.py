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
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from ..core.agent import Agent, AgentSpec
from ..core.bus import EventBus


@dataclass(frozen=True)
class DeniedOp:
    """按操作拒绝的规则。``role`` / ``capability`` 为 ``None`` 或 ``"*"`` 表示匹配任意。

    用于按 ``role`` / ``capability`` / ``op`` 维度声明细粒度拒绝，而非全局拉黑
    某个操作名。例如 ``DeniedOp(op="reboot", role="risk")`` 仅禁止 ``risk``
    角色的 ``reboot``；``DeniedOp(op="reboot", role="*")`` 等价于全局禁止 reboot；
    ``DeniedOp(op="reboot")``（``role/capability`` 为 ``None``）同样等价于全局禁止。

    ``match`` 控制 ``op`` 的匹配方式，默认 ``"exact"``（精确相等），另支持：
      - ``"prefix"``：入参操作名以规则 op 为前缀即命中（如 ``DeniedOp(op="reboot",
        match="prefix")`` 同时拒绝 ``reboot`` / ``reboot_hard`` / ``reboot_soft``）。
      - ``"suffix"``：入参操作名以规则 op 为后缀即命中（如 ``DeniedOp(op="_now",
        match="suffix")`` 拒绝 ``force_reboot_now`` / ``diag_now``）。
      - ``"contains"``：入参操作名包含规则 op 子串即命中（如 ``DeniedOp(op="temp",
        match="contains")`` 拒绝 ``read_temp`` / ``temp_flush``）。
      - ``"regex"``：规则 op 作为正则表达式，对入参操作名做**全匹配**即命中
        （如 ``DeniedOp(op=r"diag_.*", match="regex")`` 拒绝一切以 ``diag_`` 开头的操作）。
    无论哪种模式，``role`` / ``capability`` 维度始终按 ``None``/``"*"`` 通配规则叠加。
    """

    op: str
    role: Optional[str] = None
    capability: Optional[str] = None
    # 操作匹配方式：exact（默认）/ prefix / suffix / contains / regex。
    match: str = "exact"


def _coerce_denied_op(x: Union[str, DeniedOp]) -> DeniedOp:
    """把配置项归一为 ``DeniedOp``：字符串视为 ``DeniedOp(op=x)``，保持向后兼容。"""
    if isinstance(x, DeniedOp):
        return x
    return DeniedOp(op=x)


def _op_matches(pattern: str, op: str, mode: str) -> bool:
    """按 ``mode`` 比较规则 ``pattern`` 与入参操作名 ``op``。

    - ``exact``：精确相等（默认，向后兼容既有行为）。
    - ``prefix``：入参操作名以 pattern 为前缀即命中。
    - ``suffix``：入参操作名以 pattern 为后缀即命中（如 ``DeniedOp(op="_now",
      match="suffix")`` 拒绝 ``force_reboot_now`` / ``diag_now``）。
    - ``contains``：入参操作名包含 pattern 子串即命中（如 ``DeniedOp(op="temp",
      match="contains")`` 拒绝 ``read_temp`` / ``temp_flush``）。
    - ``regex``：pattern 作为正则表达式，对 op 做全匹配（``re.fullmatch``）；
      非法正则按"未命中"处理，绝不抛出。
    """
    if mode == "prefix":
        return op.startswith(pattern)
    if mode == "suffix":
        return op.endswith(pattern)
    if mode == "contains":
        return pattern in op
    if mode == "regex":
        try:
            return re.fullmatch(pattern, op) is not None
        except re.error:
            return False
    return pattern == op


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

    def can_consume(self, key: str, amount: int = 1) -> Tuple[bool, str]:
        """非突变探测：在不扣减的前提下返回该次消耗是否被允许。"""
        now = time.monotonic()
        if self.window > 0:
            first = self._first.get(key)
            if first is None or (now - first) >= self.window:
                used = amount
            else:
                used = self._counts.get(key, 0) + amount
        else:
            used = self._counts.get(key, 0) + amount
        if used > self.limit:
            return (False, f"quota exceeded for {key}: {used}/{self.limit}")
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

    def can_spend(self, amount: float) -> Tuple[bool, str]:
        """非突变探测：在不扣减的前提下返回该次花费是否被允许。"""
        if self._spent + float(amount) > self.limit:
            return (False, f"budget exceeded: {self._spent + float(amount)}/{self.limit}")
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
        denied_operations: Optional[Iterable[Union[str, DeniedOp]]] = None,
        emit_abort: bool = True,
        telemetry=None,
    ) -> None:
        self.bus = bus
        self.access = access
        self.quotas = quotas or {}
        self.budgets = budgets or {}
        self.breakers = breakers or {}
        # 按操作拒绝集合：在轮级闸门允许的前提下，仍可细粒度拒绝特定操作
        # （如允许开门但禁止重启）。支持按 role/capability/op 维度声明
        # （DeniedOp）；字符串配置项自动归一为 DeniedOp(op=...)，向后兼容。
        # 由网关在回复中作为 denied_ops 返回，由 Worker 决定跳过这些操作。
        # 不影响 allowed 判定。
        self.denied_operations: List[DeniedOp] = [
            _coerce_denied_op(x) for x in (denied_operations or [])
        ]
        self.emit_abort = emit_abort
        self.telemetry = telemetry
        self._log = logging.getLogger("stability_harness_loop_multiagent.governance")

    def evaluate(self, req: Dict[str, Any]) -> Tuple[bool, str, List[Tuple[str, str]]]:
        """返回 (allowed, reason, breaches)。

        两阶段（避免"被拒绝也扣额度"的副作用）：
          1. 先算 breaches（不突变）：访问控制在第一步；配额/预算用 ``can_consume`` /
             ``can_spend`` 非突变探测。
          2. 仅当全部通过，才真正提交 ``consume`` / ``spend``；任一失败则整体 denied 且
             不突变任何配额/预算。
        """
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
            ok, reason = q.can_consume(quota_key)
            if not ok:
                breaches.append((f"quota:{name}", reason))

        for name, b in self.budgets.items():
            ok, reason = b.can_spend(cost)
            if not ok:
                breaches.append((f"budget:{name}", reason))

        allowed = not breaches
        reason = "; ".join(r for _, r in breaches) if breaches else "ok"

        # 仅当全部策略通过才提交配额/预算的消耗；被拒绝时不突变任何状态。
        if allowed:
            for q in self.quotas.values():
                q.consume(quota_key)
            for b in self.budgets.values():
                b.spend(cost)

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

    def matches_denied_op(self, op: str, role: str, capability: str) -> bool:
        """判断 ``(op, role, capability)`` 是否被某个 ``DeniedOp`` 规则命中。

        规则匹配：``op`` 按规则声明的 ``match`` 方式比较（``exact`` 精确相等 /
        ``prefix`` 前缀 / ``regex`` 正则全匹配）；若规则指定了 role/capability
        且不为 ``None``/``"*"``，则必须与入参一致（``None`` 与 ``"*"`` 均视为匹配任意）。
        任一规则命中即返回 ``True``。
        """
        for d in self.denied_operations:
            if not _op_matches(d.op, op, d.match):
                continue
            if d.role not in (None, "*") and d.role != role:
                continue
            if d.capability not in (None, "*") and d.capability != capability:
                continue
            return True
        return False

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
        # 在轮级允许的前提下，返回被细粒度拒绝的操作集合（按操作鉴权）。
        # 匹配按 role/capability/op 维度（DeniedOp），而非仅操作名全局拉黑。
        denied_ops = [
            op for op in (req.get("operations") or [])
            if self.gov.matches_denied_op(op, req.get("role", ""), req.get("capability", "*"))
        ]
        reply = {"allowed": allowed, "reason": reason, "denied_ops": denied_ops}
        # 结构化事实上报：让观测面板看到「哪一轮、为何被拒/放行、按操作拒绝哪些」。
        # 这是治理决策的权威事实来源（网关处），调用方 fail-closed 超时路径在
        # worker 侧另发一条 fact，二者不重复。
        if self.gov.telemetry is not None:
            self.gov.telemetry.fact(
                "governance.decision",
                allowed=allowed,
                reason=reason,
                denied_ops=denied_ops,
                role=req.get("role"),
                capability=req.get("capability"),
                operation=req.get("operation"),
                round=req.get("round"),
            )
        self.respond(message, reply)


async def gate_allowed(agent: Agent, req: Dict[str, Any], timeout: float = 1.0,
                        telemetry=None) -> bool:
    """异步治理闸门（fail-closed）。

    向 ``harness/govern/request`` 发起一次 request/reply，返回是否放行。

    - 正常回复：以回复中的 ``allowed`` 为准。
    - 超时或任何异常：一律 fail-closed 返回 ``False``（绝不乐观放行）。

    用法（在异步上下文，如 ``Worker.act()`` 入口）：::

        if not await gate_allowed(self, {"role": self.role,
                                          "capability": "door-test",
                                          "operation": "round"}, timeout=1.0):
            # 跳过本轮破坏性操作，并上报 denied 事实
            return

    总线话题名是契约字符串，调用方无需 import 治理实现。

    ``telemetry``（可选）：传入时，在 fail-closed（超时/错误）路径上额外发一条
    ``governance.decision`` 结构化事实，便于观测「为何被拒」。
    """
    try:
        reply = await agent.request("harness/govern/request", req, timeout=timeout)
    except Exception:  # noqa: BLE001 - 总线超时/错误一律视为拒绝
        if telemetry is not None:
            telemetry.fact(
                "governance.decision", allowed=False, reason="timeout/fail-closed",
                denied_ops=[], role=req.get("role"),
                capability=req.get("capability"),
                operation=req.get("operation"), round=req.get("round"),
            )
        return False
    if not isinstance(reply, dict):
        return False
    return bool(reply.get("allowed", False))


async def governance_decision(agent: Agent, req: Dict[str, Any],
                              timeout: float = 1.0,
                              telemetry=None) -> Optional[Dict[str, Any]]:
    """异步治理闸门（富回复，fail-closed）。

    与 ``gate_allowed`` 类似，但返回完整的回复字典（含 ``allowed`` 与
    ``denied_ops``），便于调用方做按操作鉴权。超时或异常时返回 ``None``
    （调用方应视为整轮拒绝）。

    ``telemetry``（可选）：同 ``gate_allowed``，在 fail-closed 路径上发事实。
    """
    try:
        reply = await agent.request("harness/govern/request", req, timeout=timeout)
    except Exception:  # noqa: BLE001
        if telemetry is not None:
            telemetry.fact(
                "governance.decision", allowed=False, reason="timeout/fail-closed",
                denied_ops=[], role=req.get("role"),
                capability=req.get("capability"),
                operation=req.get("operation"), round=req.get("round"),
            )
        return None
    if not isinstance(reply, dict):
        return None
    return reply


__all__ = [
    "CircuitBreaker",
    "AccessControl",
    "Quota",
    "Budget",
    "DeniedOp",
    "Governance",
    "GovernanceAgent",
    "gate_allowed",
    "governance_decision",
]
