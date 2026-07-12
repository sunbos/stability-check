"""RebootAgent：监听协调者下发的重启指令，对设备执行远程重启。

仅依赖标准库，不引入第三方依赖，不修改任何 foundation 文件。
通信严格只走 EventBus：订阅 'coord/reboot' 指令，执行后用
'reboot/done' 回报，绝不直调其它 agent。
"""

from __future__ import annotations

import asyncio
import time

from agent import Agent


class RebootAgent(Agent):
    """按协调者指令对设备执行远程重启的 agent。

    行为
    ----
    * 订阅 'coord/reboot'（协调者下发的重启指令，消息含 round_no）。
    * 收到指令后调用 self.client.reboot()（client 即
      DeviceClient(spec.host, spec.user, spec.password)，由基类懒加载）。
    * 成功：publish 'reboot/done' -> {round_no, t_reboot, ok: True, error: None}
    * 失败：publish 'reboot/done' -> {round_no, t_reboot, ok: False, error: str}
      并重新抛出该异常（绝不吞掉）。
    """

    REBOOT_TOPIC = "coord/reboot"
    DONE_TOPIC = "reboot/done"

    # ------------------------------------------------------------------ #
    # 指令处理
    # ------------------------------------------------------------------ #
    async def _on_reboot(self, message: dict) -> None:
        """处理一条 'coord/reboot' 指令：执行重启并回报。"""
        round_no = message.get("round_no")
        t_reboot = time.time()
        try:
            await asyncio.to_thread(self.client.reboot)
            await self.publish(
                self.DONE_TOPIC,
                {
                    "round_no": round_no,
                    "t_reboot": t_reboot,
                    "ok": True,
                    "error": None,
                },
            )
        except Exception as exc:  # noqa: BLE001
            # 先回报失败，再重新抛出，不让异常被静默吞掉。
            await self.publish(
                self.DONE_TOPIC,
                {
                    "round_no": round_no,
                    "t_reboot": t_reboot,
                    "ok": False,
                    "error": str(exc),
                },
            )
            raise

    # ------------------------------------------------------------------ #
    # 业务 step（供协调者直接 await agent.step(message) 串行驱动）
    # ------------------------------------------------------------------ #
    async def step(self, message: dict) -> dict:
        """执行一次重启，返回结果 dict（同 'reboot/done' 的内容）。"""
        round_no = message.get("round_no")
        t_reboot = time.time()
        try:
            await asyncio.to_thread(self.client.reboot)
            return {
                "round_no": round_no,
                "t_reboot": t_reboot,
                "ok": True,
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            result = {
                "round_no": round_no,
                "t_reboot": t_reboot,
                "ok": False,
                "error": str(exc),
            }
            raise

    # ------------------------------------------------------------------ #
    # 独立主循环
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        """独立主循环：订阅重启指令并持续等待，直到被取消。

        可 `asyncio.run(agent.run())` 单独启动。
        """
        self.subscribe(self.REBOOT_TOPIC, self._on_reboot)
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            # 被协调者取消时干净退出
            pass
