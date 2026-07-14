"""进程内异步事件总线 EventBus（实时 pub/sub + 请求/响应）。

设计原则
--------
所有 agent 之间**只通过总线通信**：agent 不直接调用彼此的方法，而是
publish/subscribe 主题上的消息。这样未来可把 EventBus 换成网络传输
（例如基于 WebSocket / MQ 的实现）而不需要改动任何 agent 代码——agent
看到的方法签名保持不变。

特性
----
* 异步发布/订阅：publish(topic, message) fire-and-forget 广播给所有匹配
  的 handler，不等待 handler 完成。这是真正的异步消息总线语义。
* 同步发布：publish_and_wait(topic, message) 等待所有 handler 完成（用于
  测试或需要同步保证的场景）。
* 请求/响应：request(topic, message, timeout) 发布消息并等待关联响应；响应在
  topic + '/reply' 上返回，由消息内的 'req_id' 关联。超时抛 TimeoutError。
* 主题层级：支持简单的 '#' 通配符。
    - 订阅 'a/b'     只收主题为 'a/b' 的消息。
    - 订阅 'a/#'     收 'a'、'a/b'、'a/c'、'a/b/c' 等（'#' 必须位于末尾）。
    - 精确主题（无通配符）仅与同名主题匹配。

仅依赖标准库 asyncio / secrets，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import logging
import secrets

logger = logging.getLogger("burnin.bus")


def _gen_req_id() -> str:
    """生成一个随机 req_id，用于在 request/reply 中关联消息。"""
    return secrets.token_hex(8)


class EventBus:
    """进程内异步事件总线：agent 间唯一的通信通道。

    publish() 是 fire-and-forget（真异步），publish_and_wait() 是同步等待。
    这确保 vote/request 等广播不会阻塞发布者等待 LLM 等慢 handler。
    """

    def __init__(self) -> None:
        # sub_topic -> list[handler]，handler 可为普通函数或协程函数
        self._handlers: dict[str, list] = {}
        # 正在执行的 fire-and-forget task 引用（防止 GC 回收）
        self._pending_tasks: set = set()

    # ------------------------------------------------------------------ #
    # 订阅 / 取消订阅
    # ------------------------------------------------------------------ #
    def subscribe(self, topic: str, handler) -> None:
        """注册一个处理器 handler(message: dict)，可为普通函数或 async 函数。"""
        self._handlers.setdefault(topic, [])
        if handler not in self._handlers[topic]:
            self._handlers[topic].append(handler)

    def unsubscribe(self, topic: str, handler) -> None:
        """移除某个主题上的指定处理器（不存在则忽略）。"""
        handlers = self._handlers.get(topic)
        if handlers and handler in handlers:
            handlers.remove(handler)
            if not handlers:
                self._handlers.pop(topic, None)

    # ------------------------------------------------------------------ #
    # 主题匹配
    # ------------------------------------------------------------------ #
    @staticmethod
    def _match(sub_topic: str, pub_topic: str) -> bool:
        """判断 sub_topic 是否应收到 pub_topic 上的消息。"""
        if sub_topic == pub_topic:
            return True
        # 末尾 '#' 通配：'a/#' 匹配 'a' 以及任意以 'a/' 开头的主题
        if sub_topic.endswith("/#"):
            prefix = sub_topic[:-2]  # 去掉 '/#'
            return pub_topic == prefix or pub_topic.startswith(prefix + "/")
        return False

    def _matched_handlers(self, topic: str) -> list:
        """返回所有匹配 topic 的 handler 列表。"""
        matched: list = []
        for sub_topic, handlers in self._handlers.items():
            if self._match(sub_topic, topic):
                matched.extend(handlers)
        return matched

    # ------------------------------------------------------------------ #
    # 发布（fire-and-forget / 同步等待）
    # ------------------------------------------------------------------ #
    async def publish(self, topic: str, message: dict) -> None:
        """Fire-and-forget 广播：不等待 handler 完成，立即返回。

        异步 handler 用 asyncio.create_task 在后台执行；同步 handler 直接调用。
        handler 异常不会传播给发布者，只会被记录。
        这确保 vote/request 等广播不会被慢 handler（如 LLM 调用）阻塞。
        """
        handlers = self._matched_handlers(topic)
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                task = asyncio.create_task(self._safe_invoke(handler, message))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
            else:
                # 同步 handler 直接调用（快，不阻塞事件循环）
                try:
                    handler(message)
                except Exception:  # noqa: BLE001
                    logger.exception("Sync handler error on topic %s", topic)

    async def publish_and_wait(self, topic: str, message: dict) -> None:
        """同步广播：等待所有匹配 handler 完成后返回。

        用于需要同步保证的场景（如测试、conftest baseline 抓取）。
        生产代码应优先使用 publish()（fire-and-forget）。
        """
        handlers = self._matched_handlers(topic)
        for handler in handlers:
            await self._safe_invoke(handler, message)

    @staticmethod
    async def _safe_invoke(handler, message: dict) -> None:
        """安全调用 handler，异常不传播（仅记录日志）。

        fire-and-forget 模式下 handler 异常不应影响发布者或其他 handler。
        """
        try:
            result = handler(message)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001
            logger.exception("Handler error on message")

    # ------------------------------------------------------------------ #
    # 请求 / 响应
    # ------------------------------------------------------------------ #
    async def request(
        self, topic: str, message: dict, timeout: float = 10.0
    ) -> dict:
        """发布消息并等待关联响应。

        响应主题为 topic + '/reply'；用 message 内的 'req_id' 关联。若 message
        未携带 'req_id'，自动生成一个。超时抛 TimeoutError。返回响应消息 dict。

        注意：内部使用 publish（fire-and-forget），确保不被慢 handler 阻塞。
        """
        req_id = message.get("req_id") or _gen_req_id()
        outgoing = dict(message)
        outgoing["req_id"] = req_id

        reply_topic = topic + "/reply"
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()

        async def _reply_handler(msg: dict) -> None:
            if msg.get("req_id") == req_id and not fut.done():
                fut.set_result(msg)

        self.subscribe(reply_topic, _reply_handler)
        try:
            await self.publish(topic, outgoing)
            return await asyncio.wait_for(fut, timeout)
        finally:
            self.unsubscribe(reply_topic, _reply_handler)
