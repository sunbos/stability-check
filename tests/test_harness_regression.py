"""Harness 引擎回归测试 —— 锁定 MVP 的安全最佳实践。

框架坚持“三引擎互不 import”，harness 提供两项关键安全能力：

  - 存活 / 死锁探测（Watchdog）：循环卡死时向外发布 ``harness/abort``。
  - 生命周期监督 / 重启（Runtime）：智能体意外死亡时自动重启，超过
    ``max_restarts`` 后标记为死亡；收到 ``harness/abort`` 时优雅关停一切。

这两项能力此前只在 hikvision 路径间接存在，且**从未被通用回归覆盖**
（grep 全仓 ``Runtime(`` 仅在 re-export 出现；Watchdog 的 stall→abort
路径无人触发）。本文件用纯标准库的假智能体把它们锁死，避免在 MVP 阶段
留下未验证的隐患。仅使用标准库，无外部依赖。
"""

import asyncio

import pytest

from stability_harness_loop_multiagent.core.agent import Agent, AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.runtime import Runtime
from stability_harness_loop_multiagent.harness.watchdog import Watchdog


class StallLoop(Agent):
    """模拟一个卡死的“循环”：启动后什么都不做，永不发布任何活动主题。"""

    async def run(self) -> None:
        while self._running:
            await asyncio.sleep(0.02)

    async def handle(self, topic, message):
        return None


class DyingAgent(Agent):
    """启动即“死”的假智能体：``run()`` 立即返回，用于检验监督器重启。"""

    def __init__(self, bus, spec, *, starts):
        super().__init__(bus, spec)
        # 可变列表跨重启共享，每次（重新）启动都追加一次
        self._starts = starts

    async def run(self) -> None:
        self._starts.append(1)

    async def handle(self, topic, message):
        return None


@pytest.mark.asyncio
async def test_watchdog_aborts_stalled_loop():
    """死锁安全网：停滞的循环应在 stall_timeout 内被探测并触发 harness/abort。"""
    bus = EventBus()
    aborts = []
    bus.subscribe("harness/abort", lambda t, m: aborts.append(m))

    dog = Watchdog(bus, stall_timeout=0.2, check_interval=0.05)
    await dog.start()
    try:
        stall = StallLoop(bus, AgentSpec(id="loop", role="coordinator"))
        await stall.start()
        try:
            await asyncio.sleep(0.6)  # 远 > stall_timeout，足以触发中止
        finally:
            await stall.stop()
    finally:
        await dog.stop()

    assert aborts, "Watchdog 应当探测到停滞并发布 harness/abort"
    assert any(
        "stall" in (m or {}).get("reason", "") for m in aborts
    ), f"abort 原因应指明 stall：{aborts}"


@pytest.mark.asyncio
async def test_runtime_supervisor_restarts_failed_agent():
    """harness 韧性：意外死亡的智能体应被监督器自动重启，直到超过上限。"""
    bus = EventBus()
    starts = []
    agent = DyingAgent(bus, AgentSpec(id="d1", role="die"), starts=starts)

    rt = Runtime(bus, max_restarts=2, supervisor_interval=0.05)
    rt.register(agent)

    rt_task = asyncio.ensure_future(rt.run())
    try:
        await asyncio.sleep(0.5)  # 足够完成：初始启动 + 2 次重启 + 超限
    finally:
        bus.publish("harness/abort", {"reason": "test-stop"})
        try:
            await asyncio.wait_for(rt_task, timeout=2.0)
        except asyncio.TimeoutError:
            rt_task.cancel()

    # 初始 1 次 + 重启 2 次（max_restarts） = 3 次启动
    assert len(starts) == 3, f"期望 3 次启动，实际 {len(starts)}：{starts}"
    # 超过上限后该智能体应被标记为“主动停止”（死亡），不再被重启
    assert agent.id in rt._intentional_stop


@pytest.mark.asyncio
async def test_runtime_shuts_down_on_abort():
    """优雅关停：收到 harness/abort 后 Runtime.run 退出并关停所有智能体。"""
    bus = EventBus()
    agent = Agent(bus, AgentSpec(id="a1", role="noop"))
    rt = Runtime(bus, supervisor_interval=0.05)
    rt.register(agent)

    rt_task = asyncio.ensure_future(rt.run())
    await asyncio.sleep(0.1)
    assert not rt_task.done(), "Runtime 应在 harness/abort 之前持续运行"

    bus.publish("harness/abort", {"reason": "test"})
    await asyncio.wait_for(rt_task, timeout=2.0)
    assert rt_task.done(), "Runtime 应在 harness/abort 后退出"
    assert agent._task is None, "关停后智能体任务应被取消"


__all__ = [
    "test_watchdog_aborts_stalled_loop",
    "test_runtime_supervisor_restarts_failed_agent",
    "test_runtime_shuts_down_on_abort",
]
