"""稳定性检查：状态比对代理（仅使用标准库）。

StatusCheckAgent 订阅 'device/recovered'；收到恢复信号后调用设备
get_work_status()，与 ctx.baseline 按 baseline.fields 逐字段比对
（列表按元素比较），并应用策略附加断言（来自 ctx.strategy_text，由
strategy.Strategy 解析）；最后 publish 'check/status'。

保留原 check_status(client, baseline, extra_asserts) 纯函数逻辑，便于
测试与复用；agent 形态只是对其的封装与编排。
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Optional

from bus import EventBus
from agent import Agent, AgentSpec
from context import RunContext

# 让本模块可直接 import agents/ 下的 device_client 与 strategy（与 agent.py 同手法）
_AGENTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "agents")
if _AGENTS_DIR not in sys.path:
    sys.path.insert(0, _AGENTS_DIR)

from device_client import DeviceClient  # noqa: E402
from strategy import Strategy  # noqa: E402


def _not_equal(expected, actual) -> bool:
    """判断两个值是否不同；若均为列表则按元素逐项比较。"""
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            return True
        return any(_not_equal(e, a) for e, a in zip(expected, actual))
    return expected != actual


def _resolve_baseline(baseline):
    """把 baseline 归一为 (fields, status)。

    兼容 Baseline 实例（含 .fields / .status）与普通 dict（直接作为状态，
    字段取全部键）。返回 (fields: list, status: dict)。
    """
    if baseline is None:
        return [], {}
    if hasattr(baseline, "fields") and hasattr(baseline, "status"):
        return list(baseline.fields), baseline.status if isinstance(
            baseline.status, dict
        ) else {}
    if isinstance(baseline, dict):
        return list(baseline.keys()), baseline
    return [], {}


def check_status(client, baseline, extra_asserts=None) -> tuple[bool, dict]:
    """对比设备当前状态与基线快照（保留的原逻辑）。

    参数:
      client          - DeviceClient 实例（需提供 get_work_status()）。
      baseline        - Baseline 实例（.fields / .status）或 dict（状态快照）。
      extra_asserts   - 可选，list of (field, expected) 附加断言。

    返回:
      (all_ok, diff)，diff 为 {字段: {"expected":..., "actual":...}}。

    任何异常（如网络/解析错误）向上抛出，不在此处吞掉。
    """
    current = client.get_work_status()
    fields, status = _resolve_baseline(baseline)
    diff: dict = {}

    # 1) 基线字段逐项比较（列表按元素逐项比较）
    for field_name in fields:
        expected = status.get(field_name)
        actual = current.get(field_name)
        if _not_equal(expected, actual):
            diff[field_name] = {"expected": expected, "actual": actual}

    # 2) 额外断言比较
    if extra_asserts:
        for field_name, expected in extra_asserts:
            actual = current.get(field_name)
            if _not_equal(expected, actual):
                diff[field_name] = {"expected": expected, "actual": actual}

    return (len(diff) == 0, diff)


class StatusCheckAgent(Agent):
    """状态比对代理：设备恢复后校验工作状态是否回归基线。"""

    ROLE = "status"

    def __init__(self, spec: AgentSpec, bus: EventBus, ctx: RunContext) -> None:
        super().__init__(spec, bus, ctx)
        self.strategy = Strategy(getattr(ctx, "strategy_text", "") or "")

    # ------------------------------------------------------------------ #
    # 业务处理
    # ------------------------------------------------------------------ #
    async def step(self, message: dict) -> dict:
        """处理一条 'device/recovered' 消息，返回 check/status 负载。"""
        round_no = message.get("round_no")
        # 连续重启次数由协调者经消息带来；缺失时置 0
        consecutive_reboots = message.get("consecutive_reboots", 0) or 0

        error: Optional[str] = None
        diff: dict = {}
        changed = False
        try:
            extra_asserts = self.strategy.extra_status_asserts(
                round_no, consecutive_reboots
            )
            _, diff = check_status(self.client, self.ctx.baseline, extra_asserts)
            changed = len(diff) > 0
        except Exception as e:  # noqa: BLE001 - 检查异常记入 error 并发布
            error = str(e)

        return {
            "round_no": round_no,
            "changed": changed,
            "diff": diff,
            "error": error,
        }

    # ------------------------------------------------------------------ #
    # 订阅 'device/recovered'，结果 publish 到 'check/status'
    # ------------------------------------------------------------------ #
    async def _on_recovered(self, message: dict) -> None:
        result = await self.step(message)
        await self.publish("check/status", result or {})

    async def run(self) -> None:
        """状态比对代理主循环：订阅 'device/recovered' 并阻塞等待消息。"""
        self.subscribe("device/recovered", self._on_recovered)
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            # 被协调者取消时干净退出
            pass


def _demo_main() -> None:
    """独立运行：构建最小 spec/ctx，进入主循环等待 'device/recovered'。"""
    host = os.environ.get("DEVICE_HOST", "192.168.3.33")
    user = os.environ.get("DEVICE_USER", "admin")
    password = os.environ.get("DEVICE_PASSWORD", "")

    spec = AgentSpec(
        name="status-check",
        role="status",
        endpoint=f"http://{host}/ISAPI/AccessControl/AcsWorkStatus",
        user=user,
        password=password,
        host=host,
    )
    bus = EventBus()
    ctx = RunContext(baseline={}, strategy_text="")
    agent = StatusCheckAgent(spec, bus, ctx)
    asyncio.run(agent.run())


if __name__ == "__main__":
    _demo_main()
