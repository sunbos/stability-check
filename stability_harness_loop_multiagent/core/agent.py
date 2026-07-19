"""智能体基类与 AgentSpec —— 与引擎无关的注册元数据和生命周期。

一个 Agent 持有一个事件总线引用和一个私有状态字典。它绝不会直接访问
另一个 Agent；所有交互都通过事件总线完成。在 spec 中声明的订阅会在
``start`` 时绑定到 ``handle``。覆盖 ``run`` 实现主动行为（自驱循环），
或覆盖 ``handle`` 以响应主题。
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from .bus import EventBus


@dataclass
class AgentSpec:
    """描述一个智能体角色与连接方式的、与引擎无关的元数据。"""

    id: str
    role: str
    capabilities: Set[str] = field(default_factory=set)
    subscriptions: List[str] = field(default_factory=list)
    lifecycle_hooks: Dict[str, Callable] = field(default_factory=dict)


class Agent:
    def __init__(self, bus: EventBus, spec: AgentSpec) -> None:
        self.bus = bus
        self.spec = spec
        self.state: Dict[str, Any] = {}  # 私有状态，绝不共享
        self._task: Optional["asyncio.Task"] = None
        self._running = False
        self._subscriptions: List[Callable[[], None]] = []
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.agent.{spec.role}")

    # ---- 身份标识 -----------------------------------------------------
    @property
    def id(self) -> str:
        return self.spec.id

    @property
    def role(self) -> str:
        return self.spec.role

    @property
    def is_running(self) -> bool:
        """智能体当前是否处于激活运行状态（任务存在且未完成）。"""
        return self._running and self._task is not None and not self._task.done()

    # ---- 总线辅助方法 --------------------------------------------------
    def subscribe(self, topic: str, handler: Callable) -> Callable[[], None]:
        return self.bus.subscribe(topic, handler)

    def publish(self, topic: str, message: Any = None) -> None:
        self.bus.publish(topic, message)

    async def request(
        self, topic: str, message: Any = None, timeout: float = 1.0
    ) -> Any:
        return await self.bus.request(topic, message, timeout)

    def respond(self, incoming: Any, response: Any) -> None:
        """根据携带 req_id 的消息，对某个请求做出回复。"""
        req_id = incoming.get("req_id") if isinstance(incoming, dict) else None
        if req_id:
            self.bus.reply(req_id, response)

    # ---- 生命周期 ----------------------------------------------------
    async def start(self) -> None:
        self._running = True
        for hook in self.spec.lifecycle_hooks.get("on_start", []):
            try:
                hook(self)
            except Exception:  # noqa: BLE001
                self._log.exception("on_start 钩子出错")
        for topic in self.spec.subscriptions:
            self._subscriptions.append(
                self.bus.subscribe(topic, self._dispatch)
            )
        self._task = asyncio.ensure_future(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        for unsub in self._subscriptions:
            unsub()
        self._subscriptions.clear()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        for hook in self.spec.lifecycle_hooks.get("on_stop", []):
            try:
                hook(self)
            except Exception:  # noqa: BLE001
                self._log.exception("on_stop 钩子出错")

    # ---- 行为钩子（可覆盖） ------------------------------------------
    async def run(self) -> None:
        """主动行为。默认空操作；响应式智能体使用 handle()。"""
        await asyncio.sleep(0)  # pragma: no cover - 默认惰性实现

    async def handle(self, topic: str, message: Any) -> None:
        """响应已订阅的主题。在子类中覆盖。"""
        return None

    # ---- 内部实现 ----------------------------------------------------
    async def _dispatch(self, topic: str, message: Any) -> None:
        try:
            result = self.handle(topic, message)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001
            self._log.exception("handle 出错 topic=%r", topic)

    async def _run_loop(self) -> None:
        try:
            await self.run()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            self._log.exception("run 出错")


__all__ = ["Agent", "AgentSpec"]
