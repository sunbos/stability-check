"""ScenarioWorker —— 数据驱动的稳定性执行 Agent（capabilities 组合器）。

每轮流水线（act → do_work → recover → check → publish）保持不变，内部 do_work/check
改为调用 capabilities/ 的原子能力（8 actions + 4 probes + 3 preconditions）。

Capabilities 通过 ctx（SimpleNamespace）共享状态：
- ctx.client: HikvisionClient 实例（从 adapter._client 获取）
- ctx.events: 事件链查询结果（由 QueryEventsAction 写入，EventChainProbe 读取）
- ctx.baseline: 基线 serialNos + 可选 reboot_duration（由 BaselineRecordPrecondition 写入）
- ctx.last_open_iso: 上次 remote_open 的设备时间（由 RemoteOpenAction 写入，QueryEventsAction 读取）

事实独裁（安全底线，见 loop/decision.py）：probe_ok 为 False 即 fail；
probe_na（用例不适用）不强制失败。截止时间 / NA-早停 通过发布 harness/abort
安全中止循环（ControlLoop 已订阅该话题）。

本类位于 business（领域）层，仅 import multi_agent（WorkerAgent 基类）与
harness 的语义（总线话题字符串），不破坏三引擎约束。
"""

import asyncio
import time
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Optional

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from ...multi_agent.adapter import TargetAdapter
from ...multi_agent.workers.base import WorkerAgent
from .capabilities import create_action, create_precondition, create_probe
from .scenario_schema import Scenario


class ScenarioWorker(WorkerAgent):
    """ScenarioWorker（capabilities 组合器版）。

    通过 create_action/create_probe/create_precondition 把 scenario.actions /
    probes / preconditions 转换为原子能力实例，在 do_work/check 中顺序执行。
    """

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
        # 预构建能力实例（从 scenario 的 actions/probes/preconditions 列表工厂化）
        self._preconditions = [create_precondition(p) for p in scenario.preconditions]
        self._actions = [create_action(a) for a in scenario.actions]
        self._probes = [create_probe(p) for p in scenario.probes]
        # 运行时上下文（在 pre_loop_setup 中构造）
        self._ctx: Optional[SimpleNamespace] = None

    # ---- 循环前准备 ----------------------------------------------------
    def pre_loop_setup(self) -> bool:
        """循环开始前执行所有 preconditions，初始化 ctx。

        Returns:
            True 全部 precondition setup 成功；False 任一失败（应中止用例）
        """
        client = self._get_client()
        self._ctx = SimpleNamespace(
            client=client,
            events={"trigger": [], "opened": [], "closed": []},
            baseline={},
            last_open_iso=None,
        )
        for precondition in self._preconditions:
            try:
                ok = precondition.setup(self._ctx)
            except Exception as exc:  # noqa: BLE001
                self._mark("precondition_error", reason=str(exc))
                return False
            if not ok:
                self._mark("precondition_failed")
                return False
        return True

    def _get_client(self) -> Any:
        """从 adapter 获取底层 client（ScenarioISAPIAdapter._client）。

        ScenarioISAPIAdapter 是薄壳，核心逻辑在 HikvisionClient；capabilities 直接
        操作 client 避免多一层转发。FakeScenarioAdapter 没有 _client，返回 None
        （PR4c 删除 FakeScenarioAdapter 后此 fallback 可清理）。
        """
        return getattr(self._adapter, "_client", None)

    # ---- 主流水线（保持原 act 流水线，内部切换到 capabilities） ----------
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

    # ---- 执行（actions 链） ---------------------------------------------
    def do_work(self, tick: dict) -> Any:
        """顺序执行 actions 链，返回执行后的 snapshot。"""
        if self._past_deadline():
            self._emit_early_stop(tick, "deadline reached (NT)")
            return None
        # 若 ctx 未初始化（pre_loop_setup 未调用），自动初始化（兼容旧调用方）
        if self._ctx is None:
            self.pre_loop_setup()
        # 顺序执行 actions
        for action in self._actions:
            try:
                result = action.execute(self._ctx)
            except Exception as exc:  # noqa: BLE001
                self._last_stress_ok = False
                self._chain["stress_fail"] += 1
                self._mark("action_error", reason=str(exc))
                self._last_snapshot = {}
                return {"stress_ok": False, "probe": {}}
            if not result.ok:
                self._last_stress_ok = False
                self._chain["stress_fail"] += 1
                self._mark("action_failed", reason=result.error)
                self._last_snapshot = {}
                return {"stress_ok": False, "probe": {}}
        self._last_stress_ok = True
        # 拉取 snapshot（供 FieldProbe/OnlineProbe/CountProbe 用）
        self._last_snapshot = self._fetch_snapshot()
        self._chain["rounds"] += 1
        return {"stress_ok": True, "probe": self._last_snapshot}

    def _fetch_snapshot(self) -> dict:
        """从设备拉取 snapshot（用 client.get_work_status）。

        若 client 不可用（FakeScenarioAdapter 无 _client）或调用失败，返回空 dict
        （字段缺失 → 断言失败，符合"设备不可达即失败"语义）。
        """
        client = getattr(self._ctx, "client", None) if self._ctx else None
        if client is None:
            return {}
        try:
            return client.get_work_status()
        except Exception:  # noqa: BLE001
            return {}

    async def recover(self, tick: dict) -> bool:
        """在线等待已在 action 内完成（如 RebootAction.wait_online）；此处仅确认有快照。"""
        return self._last_snapshot is not None

    # ---- 检查（probes 链） ------------------------------------------------
    def check(self, tick: dict) -> dict:
        """用 probes 链组合产出事实字典。

        每个 probe.check(snapshot) 返回 dict[str, bool]，合并所有 probe 的结果。
        - 任一 fact key 以 _na 结尾且为 True → 标记本轮 NA
        - 任一 fact key 以 _soft 结尾 → 软事实（不强制 fail，Advisor 加风险分）
        - 其余 fact 任一为 False → probe_ok=False（事实独裁 fail）
        """
        self._na = False
        facts: dict = {}
        soft_facts: dict = {}
        # snapshot 选择：若 ctx 有 events 属性（EventChainProbe 需要），传 ctx；
        # 否则传 _last_snapshot（FieldProbe/OnlineProbe/CountProbe 用）
        snapshot = self._ctx if self._ctx is not None else self._last_snapshot
        for probe in self._probes:
            try:
                fact = probe.check(snapshot)
            except Exception as exc:  # noqa: BLE001
                self._mark("probe_error", reason=str(exc))
                fact = {"probe_ok": False}
            for k, v in fact.items():
                if k.endswith("_na") and v:
                    self._na = True
                elif k.endswith("_soft"):
                    soft_facts[k] = v
                else:
                    facts[k] = v
        # 事实独裁：任一非 soft 非 na 的 fact 为 False → fail
        ok = all(facts.values()) if facts else True
        if self._na:
            self._chain["na"] += 1
            # NA 不强制失败，返回 probe_na=True
            facts = {"probe_ok": True, "probe_na": True}
        elif ok:
            self._chain["pass"] += 1
        else:
            self._chain["fail"] += 1
        # 软事实合并到最终 facts（Advisor 可读 closed_soft 等加风险分）
        facts.update(soft_facts)
        self._mark("probe_checked", ok=ok, na=self._na)
        return facts

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
