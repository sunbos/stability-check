"""TrendSupervisorAgent —— 建议性趋势监视器（类自主 L3 角色）。

保有从 ``loop/done`` 收集的、*私有*的轮次结果窗口 —— 它绝不直接读取共享的循环
上下文（自包含的自主状态）。每轮它向窗口追加数据；一个*主动*定时器（每
``check_interval`` 秒，默认 30）重新扫描窗口以发现有害趋势，并可能提出事件。
它也会针对 ``loop/vote/request`` 投出带权重的 ``(risk, confidence)`` 票。

仅具建议性：它绝不裁决通过/失败。循环的 DecisionAuthority 是唯一仲裁者。其
风险分数只起*注解*作用（warn/recheck），而一个 ``critical`` 事件会强制循环
重新检查。这呼应了自治原则：各智能体独立监视并告警，它们不统治。
"""

import asyncio
import logging
import time

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from .base import AdvisorAgent


class TrendSupervisorAgent(AdvisorAgent):
    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        *,
        weight: float = 0.5,
        check_interval: float = 30.0,
        warn_streak: int = 3,
        critical_streak: int = 5,
        fail_rate_threshold: float = 0.30,
        min_samples: int = 5,
        spike_factor: float = 2.0,
        stale_timeout: float = 300.0,
    ) -> None:
        super().__init__(bus, spec, weight=weight)
        self.check_interval = check_interval
        self.warn_streak = warn_streak
        self.critical_streak = critical_streak
        self.fail_rate_threshold = fail_rate_threshold
        self.min_samples = min_samples
        self.spike_factor = spike_factor
        self.stale_timeout = stale_timeout

        self._window: list = []            # 私有：轮次字典列表
        self._last_round_ts: float = 0.0
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.advisor.{self.role}")

    # ---- 响应式：轮次摄入（loop/done） -----------------------------
    def on_round(self, round_info: dict) -> None:
        self._window.append(round_info or {})
        if len(self._window) > 200:
            self._window.pop(0)
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
        # 陈旧数据告警：一段时间没有新轮次 -> warn
        if self._last_round_ts and (time.time() - self._last_round_ts) > self.stale_timeout:
            self.raise_incident(
                "warn",
                {
                    "kind": "stale",
                    "seconds_since_round": time.time() - self._last_round_ts,
                },
            )

        inc = self._detect_trend()
        if inc is not None:
            self.raise_incident(inc["severity"], inc["detail"])

    # ---- 趋势检测（私有窗口） -------------------------------------
    def _detect_trend(self):
        if len(self._window) < 2:
            return None
        risks = [float(r.get("risk", 50.0)) for r in self._window]
        verdicts = [r.get("verdict", "") for r in self._window]

        # 从末尾开始计数的、严格递增的风险连续序列
        inc_streak = 0
        for a, b in zip(reversed(risks[1:]), reversed(risks[:-1])):
            if b > a:
                inc_streak += 1
            else:
                break

        if inc_streak >= self.critical_streak:
            return {
                "severity": "critical",
                "detail": {"kind": "increasing_risk", "streak": inc_streak},
            }
        if inc_streak >= self.warn_streak:
            return {
                "severity": "warn",
                "detail": {"kind": "increasing_risk", "streak": inc_streak},
            }

        # 失败率向上穿越阈值
        if len(self._window) >= self.min_samples:
            fails = sum(1 for v in verdicts if v in ("fail", "abort"))
            rate = fails / len(self._window)
            if rate > self.fail_rate_threshold:
                return {"severity": "warn", "detail": {"kind": "fail_rate", "rate": rate}}

        # 风险尖峰 > 近期均值 的 spike_factor 倍
        recent = risks[-self.min_samples:]
        mean = sum(recent) / len(recent)
        if mean > 0 and risks[-1] > self.spike_factor * mean:
            return {
                "severity": "warn",
                "detail": {"kind": "risk_spike", "value": risks[-1], "mean": mean},
            }
        return None

    # ---- 投票（risk, confidence） ---------------------------------
    def vote(self) -> tuple:
        if len(self._window) < 2:
            return (20.0, 0.3)  # 数据稀薄 -> 低风险、低置信度
        risks = [float(r.get("risk", 50.0)) for r in self._window]
        verdicts = [r.get("verdict", "") for r in self._window]

        inc_streak = 0
        for a, b in zip(reversed(risks[1:]), reversed(risks[:-1])):
            if b > a:
                inc_streak += 1
            else:
                break

        risk = 20.0
        if inc_streak >= self.critical_streak:
            risk = 85.0
        elif inc_streak >= self.warn_streak:
            risk = 65.0

        if len(self._window) >= self.min_samples:
            rate = sum(1 for v in verdicts if v in ("fail", "abort")) / len(self._window)
            if rate > self.fail_rate_threshold:
                risk = max(risk, 75.0)

        conf = min(1.0, len(self._window) / self.min_samples)
        return (float(risk), float(conf))


__all__ = ["TrendSupervisorAgent"]
