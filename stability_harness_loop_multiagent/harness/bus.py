"""EventBus —— 唯一的跨引擎通信接缝。

进程内异步实现。仅使用标准库。
- publish(topic, msg)：发送即忘；处理器通过 create_task 调度。
- publish_and_wait(topic, msg)：等待所有处理器完成（提供同步保证）。
- subscribe(topic, handler)：处理器可为同步或异步。返回一个
  取消订阅的可调用对象。支持尾随 '#' 通配符（例如 "a/#" 匹配 "a"、"a/b"）。
- request(topic, msg, timeout)：发布并等待由 req_id 关联的首次回复。
  响应方调用 bus.reply(req_id, response)。超时则抛出 TimeoutError。
- 处理器的异常会被记录，绝不向上传播给发布者。
"""

import asyncio
import logging
import secrets
import time
from typing import Any, Callable, Dict, List, Optional


def _match(sub_topic: str, pub_topic: str) -> bool:
    """判断某个订阅主题是否匹配已发布的主题。"""
    if sub_topic == pub_topic:
        return True
    if sub_topic.endswith("#"):
        prefix = sub_topic[:-1]  # "a/#" -> "a/"
        if prefix == "":
            return True  # 单独的 "#" 匹配一切
        if pub_topic == prefix.rstrip("/"):
            return True
        return pub_topic.startswith(prefix)
    return False


class EventBus:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._handlers: Dict[str, List[Callable]] = {}
        self._reply_waiters: Dict[str, "asyncio.Future"] = {}
        self._log = logger or logging.getLogger("stability_harness_loop_multiagent.bus")

    # ---- 订阅 ---------------------------------------------------------
    def subscribe(self, topic: str, handler: Callable) -> Callable[[], None]:
        """为某个主题注册处理器。返回一个取消订阅的可调用对象。"""
        self._handlers.setdefault(topic, []).append(handler)

        def unsubscribe() -> None:
            lst = self._handlers.get(topic)
            if lst and handler in lst:
                lst.remove(handler)

        return unsubscribe

    def unsubscribe(self, topic: str, handler: Callable) -> None:
        lst = self._handlers.get(topic)
        if lst and handler in lst:
            lst.remove(handler)

    # ---- 回复关联 -----------------------------------------------------
    def reply(self, req_id: str, message: Any) -> None:
        """解除一个由 req_id 标识的待处理请求 future。"""
        fut = self._reply_waiters.get(req_id)
        if fut is not None and not fut.done():
            fut.set_result(message)

    # ---- 发布 ---------------------------------------------------------
    def publish(self, topic: str, message: Any = None) -> None:
        """向所有匹配的处理器广播（发送即忘）。"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        for handler in self._matching(topic):
            loop.create_task(self._run_handler(handler, topic, message))

    async def publish_and_wait(self, topic: str, message: Any = None) -> None:
        """广播并等待每个处理器完成。"""
        tasks = [
            self._run_handler(handler, topic, message)
            for handler in self._matching(topic)
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def request(
        self, topic: str, message: Any = None, timeout: float = 1.0
    ) -> Any:
        """发布并等待由 req_id 关联的首次回复。"""
        req_id = secrets.token_hex(8)
        if isinstance(message, dict):
            envelope = dict(message)
        else:
            envelope = {"payload": message}
        envelope["req_id"] = req_id

        fut: "asyncio.Future" = asyncio.get_event_loop().create_future()
        self._reply_waiters[req_id] = fut
        self.publish(topic, envelope)
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._reply_waiters.pop(req_id, None)

    # ---- 内部实现 -----------------------------------------------------
    def _matching(self, topic: str) -> List[Callable]:
        out: List[Callable] = []
        for sub_topic, handlers in self._handlers.items():
            if _match(sub_topic, topic):
                out.extend(handlers)
        return out

    async def _run_handler(self, handler: Callable, topic: str, message: Any) -> None:
        try:
            result = handler(topic, message)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 - 隔离：绝不泄漏给发布者
            self._log.exception("bus 处理器出错 on topic=%r", topic)


__all__ = ["EventBus"]
