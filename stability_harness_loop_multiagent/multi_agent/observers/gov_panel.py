"""GovernancePanelAgent —— 消费治理结构化事实的观测面板（dashboard）。

订阅 ``harness/fact/governance.decision``，维护它*自己的*治理决策时间线，并
提供 ``panel()`` 聚合视图（放行/拒绝分布、按角色/能力/操作的拒绝计数、超时
fail-closed 计数、轮次覆盖）。它绝不裁决。通过发布 ``governance/panel`` 回应
``governance/panel/request``，便于其他组件/外部消费者拉取当前观测面板。

纯观察：可安全增删，不影响治理或循环行为。与三引擎约束一致——它只经事件总线
订阅 harness 事实主题，不 import harness 实现。
"""

import logging
import time
from collections import Counter

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from .base import ObserverAgent


class GovernancePanelAgent(ObserverAgent):
    # 本 Observer 关心的主题：治理决策事实 + 拉取面板的请求。
    DEFAULT_SUBSCRIPTIONS = (
        "harness/fact/governance.decision",
        "governance/panel/request",
    )

    def __init__(self, bus: EventBus, spec: AgentSpec) -> None:
        for needed in self.DEFAULT_SUBSCRIPTIONS:
            if needed not in spec.subscriptions:
                spec.subscriptions.append(needed)
        super().__init__(bus, spec)
        self._facts: list = []
        self._log = logging.getLogger(
            f"stability_harness_loop_multiagent.multi_agent.observer.{self.role}"
        )

    # ---- 记录 -------------------------------------------------------
    def on_event(self, topic: str, message) -> None:
        msg = message if isinstance(message, dict) else {"payload": message}
        if topic == "harness/fact/governance.decision":
            self._facts.append({"topic": topic, "message": msg, "ts": time.time()})
        elif topic == "governance/panel/request":
            self.publish(
                "governance/panel",
                {
                    "panel": self.panel(),
                    "timeseries": self.timeseries(),
                    "req_id": msg.get("req_id"),
                },
            )

    # ---- 面板聚合 ---------------------------------------------------
    def panel(self) -> dict:
        """对迄今收到的治理决策事实做聚合，返回结构化观测面板。"""
        decisions = [
            e["message"] for e in self._facts
            if e["topic"] == "harness/fact/governance.decision"
        ]
        allowed = [d for d in decisions if d.get("allowed")]
        denied = [d for d in decisions if not d.get("allowed")]
        fail_closed = [
            d for d in denied
            if "fail-closed" in str(d.get("reason", ""))
        ]
        by_role = Counter(
            d.get("role") for d in decisions if d.get("role") is not None
        )
        by_capability = Counter(
            d.get("capability") for d in decisions if d.get("capability") is not None
        )
        denied_ops_by_op = Counter()
        for d in decisions:
            for op in (d.get("denied_ops") or []):
                denied_ops_by_op[op] += 1
        rounds = sorted(
            {d.get("round") for d in decisions if d.get("round") is not None}
        )
        return {
            "total": len(decisions),
            "allowed": len(allowed),
            "denied": len(denied),
            "fail_closed": len(fail_closed),
            "by_role": dict(by_role),
            "by_capability": dict(by_capability),
            "denied_ops_by_op": dict(denied_ops_by_op),
            "rounds_covered": len(rounds),
            "rounds_observed": rounds,
        }

    # ---- 时间序列视图（零依赖，供趋势图 / 报告消费） ----------------
    def timeseries(self) -> dict:
        """返回按时间排序、可画图的时间序列结构。

        结构（全部为等长列表，索引对齐到每条治理决策事实）：
          - ``ts``              : 收齐时刻（time.time() 浮点，秒）。
          - ``rounds``          : 该决策所属轮次（缺省为 ``None``）。
          - ``allowed``         : 是否放行（bool）。
          - ``denied_ops``      : 该决策被拒的操作名列表。
          - ``cum_allowed``     : 截至该刻的累计放行数。
          - ``cum_denied``      : 截至该刻的累计拒绝数（含 fail-closed）。
          - ``cum_fail_closed`` : 截至该刻的累计 fail-closed 数。
          - ``step``            : 决策序号（1-based）。
        纯标准库，便于 plotly/matplotlib 直接喂入，无第三方依赖。
        """
        ts, rounds, allowed, denied_ops = [], [], [], []
        cum_allowed = cum_denied = cum_fail_closed = 0
        cum_allowed_l, cum_denied_l, cum_fc_l, step_l = [], [], [], []
        # 按收到时间排序，保证趋势图单调。
        ordered = sorted(self._facts, key=lambda e: e["ts"])
        for i, e in enumerate(ordered, start=1):
            if e["topic"] != "harness/fact/governance.decision":
                continue
            d = e["message"]
            is_allowed = bool(d.get("allowed"))
            is_fc = "fail-closed" in str(d.get("reason", ""))
            cum_allowed += int(is_allowed)
            cum_denied += int(not is_allowed)
            cum_fail_closed += int(is_fc)
            ts.append(e["ts"])
            rounds.append(d.get("round"))
            allowed.append(is_allowed)
            denied_ops.append(list(d.get("denied_ops") or []))
            cum_allowed_l.append(cum_allowed)
            cum_denied_l.append(cum_denied)
            cum_fc_l.append(cum_fail_closed)
            step_l.append(i)
        return {
            "step": step_l,
            "ts": ts,
            "rounds": rounds,
            "allowed": allowed,
            "denied_ops": denied_ops,
            "cum_allowed": cum_allowed_l,
            "cum_denied": cum_denied_l,
            "cum_fail_closed": cum_fc_l,
        }

    # ---- 文本 dashboard ---------------------------------------------
    def render(self) -> str:
        """返回一份人类可读的治理观测面板文本（dashboard）。"""
        p = self.panel()
        lines = [
            "=" * 52,
            "治理观测面板 (Governance Panel)",
            "=" * 52,
            f"  决策总数 : {p['total']}",
            f"  放行     : {p['allowed']}",
            f"  拒绝     : {p['denied']}  (其中 fail-closed 超时: {p['fail_closed']})",
            f"  覆盖轮次 : {p['rounds_covered']}",
            "-" * 52,
            "  按角色决策数:",
        ]
        if p["by_role"]:
            for role, n in p["by_role"].items():
                lines.append(f"    - {role}: {n}")
        else:
            lines.append("    (无)")
        lines.append("  按能力决策数:")
        if p["by_capability"]:
            for cap, n in p["by_capability"].items():
                lines.append(f"    - {cap}: {n}")
        else:
            lines.append("    (无)")
        lines.append("  按操作拒绝数:")
        if p["denied_ops_by_op"]:
            for op, n in p["denied_ops_by_op"].items():
                lines.append(f"    - {op}: {n}")
        else:
            lines.append("    (无)")
        lines.append("=" * 52)
        return "\n".join(lines)

    @property
    def facts(self) -> list:
        """私有治理事实时间线的只读副本。"""
        return list(self._facts)


__all__ = ["GovernancePanelAgent"]
