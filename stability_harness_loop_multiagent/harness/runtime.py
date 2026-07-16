"""Runtime —— 智能体生命周期注册表、监督器，以及 harness 层级的路由。

持有智能体注册表，并驱动系统中每个智能体的*生命周期*：
``spawn`` / ``pause`` / ``resume`` / ``shutdown``，以 ``AgentSpec.id`` 为键。
一个后台监督循环监视每个智能体任务，并重启意外失败的智能体（有限重试）。
当收到 ``harness/abort``（由看门狗或治理模块发出）时，它会优雅地关闭一切。

消息路由被有意地委托给 EventBus —— 这个唯一的跨引擎接缝。运行时绝不会
手动将消息扇出；它只负责启动/停止智能体，而总线会把主题投递给每个智能体
在 ``AgentSpec.subscriptions`` 中声明的订阅。所提供的 ``route`` 辅助方法只是
对 ``bus.publish_and_wait`` 的一层薄封装。

引擎隔离：仅从本 harness 包（bus、agent）导入。它从不导入 loop/ 或 multi_agent/。
"""

import asyncio
import logging
from typing import Callable, Dict, List, Optional

from .agent import Agent, AgentSpec
from .bus import EventBus


class Runtime:
    def __init__(
        self,
        bus: EventBus,
        *,
        telemetry=None,
        max_restarts: int = 3,
        supervisor_interval: float = 5.0,
    ) -> None:
        self.bus = bus
        self.telemetry = telemetry
        self.max_restarts = max_restarts
        self.supervisor_interval = supervisor_interval

        self._agents: Dict[str, Agent] = {}
        self._factory: Dict[str, Callable[[AgentSpec], Agent]] = {}
        self._restart_count: Dict[str, int] = {}
        # 被主动停止（pause/shutdown）的 id -> 监督器会跳过它们
        self._intentional_stop: set = set()
        self._running = False
        self._supervisor_task: Optional["asyncio.Task"] = None
        self._abort_unsub: Optional[Callable[[], None]] = None
        self._abort_reason: Optional[str] = None
        self._log = logging.getLogger("stability_harness_loop_multiagent.runtime")

    # ---- 注册表 -----------------------------------------------------
    def register(self, agent: Agent, factory: Optional[Callable[[AgentSpec], Agent]] = None) -> None:
        """将已构建好的智能体注册到其 spec.id 之下。"""
        self._agents[agent.id] = agent
        if factory is not None:
            self._factory[agent.id] = factory
        self._restart_count.setdefault(agent.id, 0)
        if self.telemetry:
            self.telemetry.metric("runtime.register", 1.0, agent=agent.id, role=agent.role)

    def spawn(self, spec: AgentSpec, factory: Callable[[AgentSpec], Agent]) -> Agent:
        """通过 ``factory`` 从 spec 构建一个智能体并注册它。"""
        agent = factory(spec)
        self.register(agent, factory)
        return agent

    def get(self, agent_id: str) -> Optional[Agent]:
        return self._agents.get(agent_id)

    @property
    def agents(self) -> Dict[str, Agent]:
        return dict(self._agents)

    def __contains__(self, agent_id: str) -> bool:
        return agent_id in self._agents

    def __len__(self) -> int:
        return len(self._agents)

    # ---- 生命周期 --------------------------------------------------
    async def start_all(self) -> None:
        for agent in list(self._agents.values()):
            if agent.id in self._intentional_stop:
                continue
            await agent.start()
        if self.telemetry:
            self.telemetry.metric("runtime.started", float(len(self._agents)))

    async def start(self, agent_id: str) -> None:
        """（重新）启动一个智能体。若它曾被暂停，则清除“主动停止”标记。"""
        agent = self._agents.get(agent_id)
        if agent is None:
            raise KeyError(agent_id)
        self._intentional_stop.discard(agent_id)
        await agent.start()

    async def pause(self, agent_id: str) -> None:
        """停止一个智能体的任务，但保留其注册状态以便稍后恢复。"""
        agent = self._agents.get(agent_id)
        if agent is None:
            raise KeyError(agent_id)
        self._intentional_stop.add(agent_id)
        await agent.stop()
        if self.telemetry:
            self.telemetry.metric("runtime.pause", 1.0, agent=agent_id)

    async def resume(self, agent_id: str) -> None:
        await self.start(agent_id)

    async def shutdown(self, agent_id: str) -> None:
        agent = self._agents.get(agent_id)
        if agent is None:
            return
        self._intentional_stop.add(agent_id)
        self._factory.pop(agent_id, None)
        await agent.stop()
        self._agents.pop(agent_id, None)
        self._restart_count.pop(agent_id, None)
        if self.telemetry:
            self.telemetry.metric("runtime.shutdown", 1.0, agent=agent_id)

    async def shutdown_all(self) -> None:
        for agent_id in list(self._agents.keys()):
            await self.shutdown(agent_id)
        self._running = False

    # ---- harness 层级路由 -------------------------------------------
    async def route(self, topic: str, message=None) -> None:
        """通过总线将消息扇出给所有订阅者（等待完成）。"""
        await self.bus.publish_and_wait(topic, message)

    # ---- 中止 + 监督器 ---------------------------------------------
    def _on_abort(self, topic: str, message) -> None:
        reason = (message or {}).get("reason", "harness abort")
        self._abort_reason = reason
        self._log.warning("runtime 收到 harness/abort: %s", reason)
        # 停止监督器循环；run() 的 finally 块负责执行关闭。
        self._running = False

    async def _supervise(self) -> None:
        while self._running:
            await asyncio.sleep(self.supervisor_interval)
            for agent_id, agent in list(self._agents.items()):
                if agent_id in self._intentional_stop:
                    continue
                task = agent._task
                if task is None or not task.done():
                    continue
                if task.cancelled():
                    # 干净的取消 -> 视为主动停止
                    self._intentional_stop.add(agent_id)
                    continue
                # 一个被监督的长期运行智能体结束了（无论是正常返回，还是
                # 异常被 Agent._run_loop 吞掉），都属于“死亡”，应当保持其存活
                # -> 进行有限次重启。
                self._restart_count[agent_id] = self._restart_count.get(agent_id, 0) + 1
                n = self._restart_count[agent_id]
                if n > self.max_restarts:
                    self._log.error(
                        "智能体 %s 超出 max_restarts=%d；保留为死亡状态",
                        agent_id, self.max_restarts,
                    )
                    self._intentional_stop.add(agent_id)
                    continue
                self._log.warning(
                    "监督器正在重启智能体 %s（第 %d/%d 次）",
                    agent_id, n, self.max_restarts,
                )
                try:
                    await agent.stop()  # 清除过期订阅
                    await agent.start()
                except Exception:  # noqa: BLE001
                    self._log.exception("重启 %s 失败", agent_id)
                if self.telemetry:
                    self.telemetry.metric(
                        "runtime.restart", 1.0, agent=agent_id, attempt=n
                    )

    async def run(self) -> None:
        """启动所有智能体，监听 ``harness/abort``，并运行监督器直到中止。"""
        self._running = True
        self._abort_unsub = self.bus.subscribe("harness/abort", self._on_abort)
        await self.start_all()
        try:
            self._supervisor_task = asyncio.ensure_future(self._supervise())
            await self._supervisor_task
        finally:
            if self._abort_unsub is not None:
                self._abort_unsub()
                self._abort_unsub = None
            await self.shutdown_all()


__all__ = ["Runtime"]
