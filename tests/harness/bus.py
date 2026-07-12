"""进程内异步事件总线 EventBus（实时 pub/sub + 请求/响应）。

设计原则
--------
所有 agent 之间**只通过总线通信**：agent 不直接调用彼此的方法，而是
publish/subscribe 主题上的消息。这样未来可把 EventBus 换成网络传输
（例如基于 WebSocket / MQ 的实现）而不需要改动任何 agent 代码——agent
看到的方法签名保持不变。

特性
----
* 实时发布/订阅：publish(topic, message) 把消息广播给订阅该 topic 的全部处理器。
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
import secrets


def _gen_req_id() -> str:
    """生成一个随机 req_id，用于在 request/reply 中关联消息。"""
    return secrets.token_hex(8)


class EventBus:
    """进程内异步事件总线：agent 间唯一的通信通道。"""

    def __init__(self) -> None:
        # sub_topic -> list[handler]，handler 可为普通函数或协程函数
        self._handlers: dict[str, list] = {}
        self._lock = asyncio.Lock()

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

    # ------------------------------------------------------------------ #
    # 发布
    # ------------------------------------------------------------------ #
    async def publish(self, topic: str, message: dict) -> None:
        """把消息广播给所有匹配主题的处理器（顺序调用，各自 await）。"""
        matched: list = []
        for sub_topic, handlers in self._handlers.items():
            if self._match(sub_topic, topic):
                matched.extend(handlers)
        for handler in matched:
            await self._invoke(handler, message)

    @staticmethod
    async def _invoke(handler, message: dict) -> None:
        """调用处理器；若是协程函数则 await 其结果。"""
        result = handler(message)
        if asyncio.iscoroutine(result):
            await result

    # ------------------------------------------------------------------ #
    # 请求 / 响应
    # ------------------------------------------------------------------ #
    async def request(
        self, topic: str, message: dict, timeout: float = 10.0
    ) -> dict:
        """发布消息并等待关联响应。

        响应主题为 topic + '/reply'；用 message 内的 'req_id' 关联。若 message
        未携带 'req_id'，自动生成一个。超时抛 TimeoutError。返回响应消息 dict。
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
