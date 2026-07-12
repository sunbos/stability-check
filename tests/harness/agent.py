"""Agent 基类：可独立运行、可寻址、可单独被协调者驱动。

设计目标
--------
* 每个 agent 由 AgentSpec 描述（名称/角色/ISAPI 端点/凭据/主机）。
* agent 通过持有的 EventBus 通信，不直接互调。
* agent 可单独 `asyncio.run(agent.run())` 启动，也能被协调者统一驱动：
  - 默认 run() 订阅自身输入主题 '<role>/in'，收到消息调用 self.step()，
    把结果 publish 到 '<role>/out'，随后阻塞直到被取消。
  - 协调者也可直接 `await agent.step(message)` 串行驱动，无需事件循环常驻。
* agent 用 DeviceClient(spec.host, spec.user, spec.password) 发起各自请求
  （复用 tests/agents/device_client.py，仅标准库实现）。

仅依赖标准库 asyncio / dataclasses / os / sys，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass

from bus import EventBus

# 让 harness/agent.py 能直接 `from device_client import DeviceClient`：
# device_client 位于本目录的上级 agents/ 下。
_AGENTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "agents")
if _AGENTS_DIR not in sys.path:
    sys.path.insert(0, _AGENTS_DIR)

from device_client import DeviceClient  # noqa: E402


@dataclass
class AgentSpec:
    """一个 agent 的寻址与凭据描述。

    endpoint: 该 agent 调用的 ISAPI 地址，如
              'http://192.168.3.33/ISAPI/System/reboot'。
    host:     主机基址（便于构造 DeviceClient，如 '192.168.3.33'）。
    """

    name: str
    role: str
    endpoint: str
    user: str
    password: str
    host: str


class Agent:
    """Agent 基类。子类通常覆盖 step() 与/或 run()。"""

    def __init__(self, spec: AgentSpec, bus: EventBus, ctx) -> None:
        self.spec = spec
        self.bus = bus
        self.ctx = ctx
        self._device_client: DeviceClient | None = None
        self._stop = None  # asyncio.Event，在 run() 内创建

    # ------------------------------------------------------------------ #
    # 便捷转发
    # ------------------------------------------------------------------ #
    def publish(self, topic: str, message: dict):
        """便捷转发：publish 是协程，调用方需 await。"""
        return self.bus.publish(topic, message)

    def subscribe(self, topic: str, handler) -> None:
        """便捷转发：注册总线订阅。"""
        self.bus.subscribe(topic, handler)

    # ------------------------------------------------------------------ #
    # 设备客户端（懒加载）
    # ------------------------------------------------------------------ #
    @property
    def client(self) -> DeviceClient:
        """懒加载的 DeviceClient，凭据来自 spec。"""
        if self._device_client is None:
            self._device_client = DeviceClient(
                self.spec.host, self.spec.user, self.spec.password
            )
        return self._device_client

    # ------------------------------------------------------------------ #
    # 业务处理：子类覆盖
    # ------------------------------------------------------------------ #
    async def step(self, message: dict) -> dict:
        """处理单条消息，返回结果 dict。默认实现原样返回空 dict。子类应覆盖。"""
        return {}

    # ------------------------------------------------------------------ #
    # 独立主循环
    # ------------------------------------------------------------------ #
    async def _on_input(self, message: dict) -> None:
        """默认输入处理器：调用 step 并把结果发到 '<role>/out'。"""
        result = await self.step(message)
        await self.publish(f"{self.spec.role}/out", result or {})

    async def run(self) -> None:
        """该 agent 的独立主循环。

        默认实现：订阅 '<role>/in'，收到消息调用 self.step 并发布到 '<role>/out'，
        随后阻塞直到被取消。子类可覆盖以提供自定义循环。
        """
        self.subscribe(f"{self.spec.role}/in", self._on_input)
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            # 被协调者取消时干净退出
            pass

    def stop(self) -> None:
        """请求 agent 退出 run() 主循环（若正在运行）。"""
        if self._stop is not None:
            self._stop.set()
