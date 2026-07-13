"""NotifierAgent：通知智能体（仅使用标准库）。

职责
----
* 订阅需要“让人知道”的事件：coord/abort（中止）、analyst/decision（分析决策）、
  analyst/report（带失败的分析）、notify（通用通知主题），以及 ReporterAgent 的告警。
* 通过可插拔 channel 发送通知：默认仅打印 + 预留 webhook 钩子（不在 MVP 真实发送）。
  channel 可通过 config.notifier_channel 选择（如 'print' / 'webhook'）。

设计说明
--------
当人不在场时，Notifier 是把“异常 / 停机决策”推送给人的唯一出口。它自身不做任何
设备请求与决策，只转发总线上的关键信号。保持可插拔，未来接企业微信/邮件只需实现
send()。

仅依赖标准库 + 同仓 bus / agent，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys

_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from agent import Agent  # noqa: E402


class NotifierAgent(Agent):
    """通知智能体：把关键信号推送给人（打印 + 预留 webhook）。"""

    TOPICS = (
        "coord/abort",
        "analyst/decision",
        "analyst/report",
        "notify",
    )

    def __init__(self, spec, bus, ctx, cfg=None) -> None:
        super().__init__(spec, bus, ctx)
        self.cfg = cfg if cfg is not None else getattr(ctx, "cfg", None)
        # 通知通道：'print'（默认）或 'webhook'。
        self.channel = self._read_channel()
        self.sent: list = []

    def _read_channel(self) -> str:
        if self.cfg is not None:
            ch = getattr(self.cfg, "notifier_channel", None)
            if ch:
                return str(ch)
        return os.environ.get("BURNIN_NOTIFIER", "print")

    # ------------------------------------------------------------------ #
    # 发送（可插拔）
    # ------------------------------------------------------------------ #
    def notify(self, title: str, body: str) -> None:
        """发送一条通知。print 通道直接打印；webhook 通道预留钩子。"""
        msg = f"[通知:{self.channel}] {title} | {body}"
        print(msg)
        self.sent.append(msg)
        if self.channel == "webhook":
            self._send_webhook(title, body)

    def _send_webhook(self, title: str, body: str) -> None:
        """预留 webhook 发送钩子（MVP 不真实发送）。子类/配置可覆盖。"""
        # 这里不做真实网络请求，避免把通知副作用引入核心路径。
        pass

    # ------------------------------------------------------------------ #
    # 总线处理
    # ------------------------------------------------------------------ #
    async def _on_abort(self, m: dict) -> None:
        self.notify("拷机中止", f"reason={m.get('reason')}")

    async def _on_decision(self, m: dict) -> None:
        cont = m.get("continue")
        self.notify(
            "分析决策",
            f"{'继续' if cont else '停止'}（来源={m.get('source')}）：{m.get('reason')}",
        )

    async def _on_report(self, m: dict) -> None:
        # 仅在有失败时通知，避免刷屏。
        if m.get("failed"):
            self.notify(
                "稳定性告警",
                f"评分={m.get('stability_score')} 失败={m.get('failed')}/"
                f"{m.get('total')} 建议={m.get('recommendation')}",
            )

    async def _on_notify(self, m: dict) -> None:
        self.notify(m.get("title", "通知"), m.get("body", str(m)))

    # ------------------------------------------------------------------ #
    # 主循环
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        self.subscribe("coord/abort", self._on_abort)
        self.subscribe("analyst/decision", self._on_decision)
        self.subscribe("analyst/report", self._on_report)
        self.subscribe("notify", self._on_notify)
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    from bus import EventBus
    from context import RunContext
    from agent import AgentSpec
    from config import load_config_from_env

    cfg = load_config_from_env()
    bus = EventBus()
    ctx = RunContext()
    ctx.cfg = cfg
    spec = AgentSpec("notifier", "notifier", "", cfg.user, cfg.password, cfg.host)
    agent = NotifierAgent(spec, bus, ctx, cfg=cfg)

    async def _demo():
        await bus.publish("coord/abort", {"reason": "power loss"})
        await bus.publish("analyst/decision", {"continue": False, "source": "rule", "reason": "断电"})
        await bus.publish("notify", {"title": "测试", "body": "hello"})

    asyncio.run(_demo())
