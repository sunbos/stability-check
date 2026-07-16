"""RiskAnalyst —— 建议性风险打分器（类自主 L3 角色）。

从 ``loop/done`` 累积一份*私有*风险历史，在 ``loop/vote/request`` 上投出带权重的
``(risk, confidence)`` 票，并运行一个*主动*定时器（每 ``check_interval`` 秒，
默认 45）定期重新扫描其风险历史，并可能提出事件：

    * 连续 3 个高风险轮次（risk > high_risk_threshold） -> critical
    * 单个极端风险轮次（risk >= extreme_risk_threshold）  -> warn

引入主动重扫，是为了即使没有新的轮次及时到来参与投票，也能暴露出关键的
趋势 —— 该智能体按自己的时钟监视。

仅具建议性：它绝不裁决通过/失败 —— 它只通过投票 + 事件来给出建议。
"""

import asyncio
import logging
import time

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from .base import AdvisorAgent


class RiskAnalyst(AdvisorAgent):
    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        *,
        weight: float = 0.5,
        check_interval: float = 45.0,
        high_risk_threshold: float = 80.0,
        extreme_risk_threshold: float = 90.0,
        high_streak: int = 3,
        min_samples: int = 3,
    ) -> None:
        super().__init__(bus, spec, weight=weight)
        self.check_interval = check_interval
        self.high_risk_threshold = high_risk_threshold
        self.extreme_risk_threshold = extreme_risk_threshold
        self.high_streak = high_streak
        self.min_samples = min_samples

        self._risks: list = []            # 私有风险历史
        self._last_round_ts: float = 0.0
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.advisor.{self.role}")

    # ---- 响应式：轮次摄入（loop/done） -----------------------------
    def on_round(self, round_info: dict) -> None:
        r = round_info or {}
        self._risks.append(float(r.get("risk", 50.0)))
        if len(self._risks) > 200:
            self._risks.pop(0)
        self._last_round_ts = time.time()

    # ---- 主动定时器循环 -------------------------------------------
    async def run(self) -> None:
        while self._running:
            await asyncio.sleep(self.check_interval)
            try:
                self._proactive_check()
            except Exception:  # noqa: BLE001 - 隔离：绝不杀死该智能体
                self._log.exception("主动检查出错")

    def _proactive_check(self) -> None:
        if not self._risks:
            return

        # 从末尾开始计数的连续高风险 streak
        streak = 0
        for v in reversed(self._risks):
            if v > self.high_risk_threshold:
                streak += 1
            else:
                break
        if streak >= self.high_streak:
            self.raise_incident(
                "critical", {"kind": "high_risk_streak", "streak": streak}
            )
            return

        # 单个极端风险读数 -> warn
        if self._risks[-1] >= self.extreme_risk_threshold:
            self.raise_incident(
                "warn", {"kind": "extreme_risk", "value": self._risks[-1]}
            )

    # ---- 投票（risk, confidence） ---------------------------------
    def vote(self) -> tuple:
        if not self._risks:
            return (50.0, 0.0)  # 有数据之前先弃权
        # 保守：近期峰值主导投票
        recent = self._risks[-self.min_samples:]
        risk = max(recent)
        conf = min(1.0, len(self._risks) / self.min_samples)
        return (float(risk), float(conf))


__all__ = ["RiskAnalyst"]
