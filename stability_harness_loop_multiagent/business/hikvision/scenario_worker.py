"""ScenarioWorker —— 数据驱动的稳定性执行 Agent。

每轮流水线（感知->规划->执行->检查->裁决 在 ControlLoop 内）：
  do_work(tick)  -> 执行 stress（reboot/upgrade/issue/none）+ 探测 observe()，
                     缓存快照。
  recover(tick)  -> 在线等待已在 adapter 内完成；此处返回 True（设备可达即恢复）。
  check(tick)    -> 用 probe 字段与期望值断言，产出事实字典交给 DecisionAuthority。

事实独裁（安全底线，见 loop/decision.py）：``probe_ok`` 为 False 即 fail；
``probe_na``（用例不适用）不强制失败。截止时间 / NA-早停 通过发布
``harness/abort`` 安全中止循环（ControlLoop 已订阅该话题）。

本类位于 business（领域）层，仅 import multi_agent（WorkerAgent 基类）与
harness 的语义（总线话题字符串），不破坏三引擎约束。
"""

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, Optional

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from ...multi_agent.adapter import TargetAdapter
from ...multi_agent.workers.base import WorkerAgent
from .scenario_schema import (
    Scenario,
    _SENTINEL,
    compare_probe,
    resolve_field,
)


class ScenarioWorker(WorkerAgent):
    def __init__(self, bus: EventBus, spec: AgentSpec, adapter: TargetAdapter,
                 scenario: Scenario, *, recover_timeout: float = 180.0) -> None:
        super().__init__(bus, spec, adapter)
        self._sc = scenario
        self._recover_timeout = recover_timeout
        self._last_snapshot: Any = None
        self._last_stress_ok: bool = True
        self._na: bool = False
        self._early_stop: bool = False
        self._stop_reason: Optional[str] = None
        # 跨轮统计（观测用）：轮数 / 通过 / 失败 / NA。
        self._chain: Dict[str, int] = {"rounds": 0, "pass": 0,
                                       "fail": 0, "na": 0, "stress_fail": 0}

    # ---- 主流水线（重写以用线程包裹长 IO + 处理早停） ------------------
    async def act(self, tick: dict) -> None:
        round_no = tick.get("round")
        # 早停 1：截止时间（NT，未测试）。在施加压力前判定，避免无谓重启设备。
        if self._past_deadline():
            self._emit_early_stop(tick, "deadline reached (NT)")
            return
        try:
            result = await asyncio.to_thread(self.do_work, tick)
        except Exception as exc:  # noqa: BLE001 - 探测异常按设备不可达处理
            result = None
            self._last_snapshot = {}  # 空快照 -> 字段缺失 -> 断言失败
            self._mark("do_work_error", reason=str(exc))
        self.publish(
            "target/acted",
            {"role": self.role, "round": round_no, "result": result},
        )
        recovered = await self.recover(tick)
        self.publish(
            "target/recovered",
            {"role": self.role, "round": round_no, "recovered": recovered},
        )
        facts = self.check(tick)
        self.publish(
            "target/checked",
            {"role": self.role, "round": round_no, "facts": facts},
        )
        self.publish("agent/" + self.role + "/done", {"round": round_no})
        # 早停 2：本轮 NA 且配置要求 NA 即停（否则 NA 轮仅跳过、继续循环）。
        if self._na and self._sc.loop.stop_on_na:
            self._emit_early_stop(tick, "not applicable (NA)")

    def _emit_early_stop(self, tick: dict, reason: str) -> None:
        self._early_stop = True
        self._stop_reason = reason
        self._mark("early_stop", reason=reason)
        # 必须补齐 target/* 三件套，否则 ControlLoop 会因收不到检查而判 fail。
        self.publish("target/acted",
                     {"role": self.role, "round": tick.get("round"),
                      "result": {"early_stop": True}})
        self.publish("target/recovered",
                     {"role": self.role, "round": tick.get("round"),
                      "recovered": True})
        self.publish("target/checked",
                     {"role": self.role, "round": tick.get("round"),
                      "facts": {"probe_ok": True, "early_stop": True}})
        self.publish("agent/" + self.role + "/done", {"round": tick.get("round")})
        # 经由总线安全中止循环（与看门狗同契约）。
        self.publish("harness/abort", {"reason": reason})

    def _past_deadline(self) -> bool:
        dl = self._sc.loop.deadline
        if not dl:
            return False
        try:
            h, m = (int(x) for x in dl.split(":"))
            return datetime.now().time() >= datetime(2000, 1, 1, h, m).time()
        except Exception:  # noqa: BLE001
            return False

    # ---- 执行 ----------------------------------------------------------
    def do_work(self, tick: dict) -> Any:
        if self._sc.stress.type != "none":
            res = self.adapter.act({"op": "stress"})
            self._last_stress_ok = res.ok
            if not res.ok:
                self._chain["stress_fail"] += 1
                self._mark("stress_failed", error=res.error)
        try:
            snap = self.adapter.observe().snapshot
        except Exception as exc:  # noqa: BLE001
            snap = {}
            self._mark("observe_error", reason=str(exc))
        self._last_snapshot = snap
        self._chain["rounds"] += 1
        return {"stress_ok": self._last_stress_ok, "probe": snap}

    async def recover(self, tick: dict) -> bool:
        # 在线等待已在 adapter 的 stress 阶段完成；此处仅确认本轮有快照。
        return self._last_snapshot is not None

    # ---- 检查 / 断言 ---------------------------------------------------
    def check(self, tick: dict) -> dict:
        snap = self._last_snapshot
        p = self._sc.probe
        self._na = False

        # NA：存在性字段缺失 => 本用例在该设备上不适用，不判失败。
        if p.na_if_absent:
            if resolve_field(snap, p.na_if_absent, default=_SENTINEL) is _SENTINEL:
                self._na = True
                self._chain["na"] += 1
                self._mark("probe_na", field=p.na_if_absent)
                return {"probe_ok": True, "probe_na": True, "probe_value": None}

        value = resolve_field(snap, p.field, default=_SENTINEL)
        ok = compare_probe(value, p) if value is not _SENTINEL else False
        if ok:
            self._chain["pass"] += 1
        else:
            self._chain["fail"] += 1
        self._mark("probe_checked", value=value, ok=ok)
        return {"probe_ok": ok, "probe_value": value}

    # ---- 观测辅助 ------------------------------------------------------
    def get_chain_stats(self) -> Dict[str, int]:
        return dict(self._chain)

    @property
    def stop_reason(self) -> Optional[str]:
        return self._stop_reason

    def _mark(self, stage: str, **extra: Any) -> None:
        """轻量本地标记（不依赖遥测），便于观测时间线。"""
        entry = {"stage": stage, "t": round(time.monotonic(), 3)}
        entry.update(extra)
        if not hasattr(self, "_timeline"):
            self._timeline: list = []
        self._timeline.append(entry)


__all__ = ["ScenarioWorker"]
