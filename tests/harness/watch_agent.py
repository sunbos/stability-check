"""WatchAgent：监视某次重启后设备的 DOWN->UP 完整恢复周期。

职责（仅通过总线通信，不修改 foundation）
------------------------------------------------
* 订阅 'reboot/done'；当收到 ok=True 的消息后，开始监视对应设备：
  轮询 self.client.get_work_status() 直到先观察到掉线（异常）再观察到
  恢复（成功），即 DOWN->UP 完整周期。
* 恢复判定逻辑严格参考 supervisor.py：
  - POLL = 5s 轮询间隔。
  - 不能只判断“当前在线”——重启命令发出后设备往往仍在线片刻，首次
    轮询会误判已恢复（t_recover≈0）。必须观察到 先掉线 再 上线。
  - 超时取 ctx 中的 recover_timeout（缺失时回退默认 180s）。
* 恢复后 publish 'device/recovered' -> {round_no, t_reboot, t_recover: time.time()}；
  超时未恢复则 publish 'device/recovered' -> {round_no, t_reboot, t_recover: None}
  （协调者据此判定该轮失败）。
* 可单独 asyncio.run(agent.run()) 运行。

仅依赖标准库 asyncio / time，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

# 让本模块能直接 `from agent import Agent` / `from bus import EventBus`：
# watch_agent.py 与 agent.py / bus.py 同处 harness/ 目录。
_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from agent import Agent  # noqa: E402

# 恢复轮询间隔（秒），与 supervisor.POLL_INTERVAL 保持一致。
POLL_INTERVAL = 5.0
# recover_timeout 缺失时的回退默认值（与 config.RunConfig 默认一致）。
DEFAULT_RECOVER_TIMEOUT = 180.0

DONE_TOPIC = "reboot/done"
RECOVERED_TOPIC = "device/recovered"


class WatchAgent(Agent):
    """监视一次重启后设备的 DOWN->UP 完整恢复周期。"""

    def __init__(self, spec, bus, ctx) -> None:
        super().__init__(spec, bus, ctx)
        self._stop = None
        self._tasks: set = set()
        # 订阅 'reboot/done'；ok=True 时启动监视（后台任务，不阻塞总线发布）。
        self.subscribe(DONE_TOPIC, self._on_reboot_done)

    # ------------------------------------------------------------------ #
    # 订阅处理：收到 ok=True 的 'reboot/done' 后启动监视协程
    # ------------------------------------------------------------------ #
    async def _on_reboot_done(self, message: dict) -> None:
        if message.get("ok") is not True:
            return
        round_no = message.get("round_no")
        t_reboot = message.get("t_reboot", time.time())
        task = asyncio.create_task(self._watch_cycle(round_no, t_reboot))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ------------------------------------------------------------------ #
    # 监视协程：轮询直到先 DOWN 再 UP，或超时
    # ------------------------------------------------------------------ #
    async def _watch_cycle(self, round_no, t_reboot: float) -> None:
        recover_timeout = getattr(self.ctx, "recover_timeout", DEFAULT_RECOVER_TIMEOUT)
        poll_deadline = t_reboot + recover_timeout

        saw_down = False
        t_recover = None
        while time.time() < poll_deadline:
            try:
                self.client.get_work_status()
                # 此刻能成功取状态 => 设备在线。
                if saw_down:
                    # 已经历过掉线，这次成功即视为恢复完成。
                    t_recover = time.time()
                    break
                # 仍处于重启前的在线态，继续等待其掉线。
            except Exception:  # noqa: BLE001 - 取状态失败即视为设备已掉线
                saw_down = True
            await asyncio.sleep(POLL_INTERVAL)

        await self.publish(
            RECOVERED_TOPIC,
            {
                "round_no": round_no,
                "t_reboot": t_reboot,
                "t_recover": t_recover,  # 超时未恢复则为 None（协调者判失败）
            },
        )

    # ------------------------------------------------------------------ #
    # 独立主循环：保持事件循环存活以驱动后台监视任务
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            # 被协调者取消时干净退出
            pass
