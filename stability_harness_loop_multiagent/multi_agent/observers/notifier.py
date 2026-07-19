"""NotifierAgent —— 带可插拔通知通道的 Observer。

消费通用的通知主题（``agent/incident``、``loop/abort``、``notify``），并将每个
事件分派到一个通道。通道有：

    * ``print``   （默认）—— 将事件漂亮地打印到 stdout。
    * ``webhook`` —— 桩实现。``_send_webhook`` 默认惰性（no-op），除非提供了
      ``webhook_url``，此时它只记录意图。这样框架得以仅依赖标准库，同时保留
      一个清晰的扩展点。

无决策权：它只上报其所观察到的内容。通过覆盖 ``_dispatch_channel`` 可接入
额外通道。
"""

import json
import logging
import time

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from .base import ObserverAgent


class NotifierAgent(ObserverAgent):
    DEFAULT_SUBSCRIPTIONS = ("agent/incident", "loop/abort", "notify")

    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        *,
        channel: str = "print",
        webhook_url: str = None,
    ) -> None:
        for needed in self.DEFAULT_SUBSCRIPTIONS:
            if needed not in spec.subscriptions:
                spec.subscriptions.append(needed)
        super().__init__(bus, spec)
        self.channel = channel
        self.webhook_url = webhook_url
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.observer.{self.role}")

    # ---- 通知 -------------------------------------------------------
    def on_event(self, topic: str, message) -> None:
        msg = message if isinstance(message, dict) else {"payload": message}
        record = {"topic": topic, "message": msg, "ts": time.time()}
        if self.channel == "webhook":
            self._send_webhook(record)
        else:
            self._print(record)

    # ---- 通道 -------------------------------------------------------
    def _print(self, record: dict) -> None:
        try:
            text = json.dumps(record, ensure_ascii=False, default=str)
        except Exception:  # noqa: BLE001
            text = str(record)
        print(f"[notify:{self.role}] {text}")

    def _send_webhook(self, record: dict) -> None:
        # 桩：真实部署会在此接入 webhook_url + 一个 HTTP 传输层。
        # 保持惰性，使框架仅依赖标准库；绝不抛出。
        if self.webhook_url:
            self._log.info("webhook 通知 -> %s（桩）: %r", self.webhook_url, record)
        else:
            self._log.debug("已选择 webhook 通道但未提供 webhook_url；跳过")


__all__ = ["NotifierAgent"]
