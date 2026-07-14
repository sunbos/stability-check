"""重启事件核对 Agent（仅使用标准库）。

基于 harness/agent.py 的 Agent 基类，把 tests/agents/event_check_agent.py 中的
重启事件核对逻辑改造为 agent 形态：

  - 订阅 'device/recovered'；收到消息（含 round_no, t_reboot, t_recover）后，
    调用 self.client.get_reboot_events 在窗口 [t_reboot-window, t_recover+window]
    内判定是否存在 major=3/minor=123 重启事件（窗口内存在即 True，±5s 容错，
    时间无法解析亦视为已产生）。
  - publish 'check/event' -> {round_no, found: bool, error: str|None}。
  - 可单独运行（见 __main__）。

窗口 window 取自 RunConfig.event_window：优先从 self.ctx.cfg 取，其次默认 30 秒。
不修改 foundation（bus/agent/context）。
"""

from __future__ import annotations

import os
import sys
import time
import asyncio

# 让本文件既能被 harness 包导入，也能 `python event_check_agent.py` 直接运行：
_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
_AGENTS_DIR = os.path.join(os.path.dirname(_HARNESS_DIR), "agents")
for _p in (_HARNESS_DIR, _AGENTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from agent import Agent  # noqa: E402

# 复用 device_client 的 DeviceClient（已由 agent.py 间接加入 sys.path）。
from device_client import DeviceClient  # noqa: E402


# ----------------------------------------------------------------------------- #
# 时间解析 / 窗口判定（保留自 tests/agents/event_check_agent.py 的逻辑）
# ----------------------------------------------------------------------------- #
def _epoch_to_str(epoch: float) -> str:
    """由 epoch 秒构造 'YYYY-MM-DDTHH:MM:SS'（不带空格 / 时区）。"""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(epoch))


def _parse_event_time(time_str: str) -> float | None:
    """解析事件时间字符串（如 '2026-06-05T08:22:07+08:00'）为 epoch 秒。

    由于 time.strptime 不支持末尾时区偏移，先去掉末尾的 '+HH:MM' / '-HH:MM'
    再解析。解析失败返回 None（交由调用方跳过该条）。
    """
    if not time_str:
        return None
    s = time_str.strip()
    # 去掉末尾时区偏移（形如 +08:00 或 -05:00）
    if len(s) >= 6 and s[-6:-5] in ("+", "-") and s[-3] == ":":
        s = s[:-6]
    try:
        struct = time.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
    return time.mktime(struct)


# ----------------------------------------------------------------------------- #
# Agent
# ----------------------------------------------------------------------------- #
class EventCheckAgent(Agent):
    """核对单轮重启是否真的在设备事件日志中落了重启记录。"""

    ROLE = "event"
    RECOVERED_TOPIC = "device/recovered"
    RESULT_TOPIC = "check/event"
    RECHECK_TOPIC = "coord/recheck"  # Phase 3: subscribe to recheck requests

    # ------------------------------------------------------------------ #
    # 配置取值
    # ------------------------------------------------------------------ #
    def _event_window(self) -> float:
        """窗口取 RunConfig.event_window：优先 self.ctx.cfg，默认 30。"""
        cfg = getattr(self.ctx, "cfg", None)
        if cfg is not None:
            window = getattr(cfg, "event_window", None)
            if window is not None:
                return float(window)
        return 30.0

    # ------------------------------------------------------------------ #
    # 核心核对逻辑
    # ------------------------------------------------------------------ #
    def _judge(self, events, start_epoch: float, end_epoch: float) -> bool:
        """在窗口内判定是否存在 major=3/minor=123 重启事件。

        - 窗口内存在即 True；
        - 事件时间无法解析时也视为“已产生”；
        - 事件若携带 major/minor 字段则一并校验（默认请求已限定 3/123）。
        """
        if not events:
            return False
        for ev in events:
            if not isinstance(ev, dict):
                continue
            major = ev.get("major")
            minor = ev.get("minor")
            if major is not None and minor is not None:
                if (major, minor) != (3, 123):
                    continue
            time_str = ev.get("time")
            epoch = _parse_event_time(time_str) if time_str else None
            if epoch is None:
                # 事件存在但时间无法解析：仍视为已产生重启事件
                return True
            if start_epoch <= epoch <= end_epoch:
                return True
        return False

    async def check(self, message: dict) -> dict:
        """核对一条 'device/recovered' 消息，返回结果 dict。

        返回值形如 {round_no, found: bool, error: str|None}。
        """
        round_no = message.get("round_no")
        t_reboot = message.get("t_reboot")
        t_recover = message.get("t_recover")
        window = self._event_window()

        try:
            if t_reboot is None:
                return {
                    "round_no": round_no,
                    "found": False,
                    "error": "消息中缺少 t_reboot",
                }
            t_reboot = float(t_reboot)
            t_recover = float(t_recover) if t_recover is not None else t_reboot

            start = _epoch_to_str(t_reboot - window)
            end = _epoch_to_str(t_recover + window)
            # 窗口两侧各放宽 5s 容错
            start_epoch = t_reboot - window - 5
            end_epoch = t_recover + window + 5

            events = self.client.get_reboot_events(start, end)
            found = self._judge(events, start_epoch, end_epoch)
            return {"round_no": round_no, "found": bool(found), "error": None}
        except Exception as e:  # 网络/解析等异常：记录错误，不抛出
            return {"round_no": round_no, "found": False, "error": str(e)}

    # ------------------------------------------------------------------ #
    # 消息处理
    # ------------------------------------------------------------------ #
    async def step(self, message: dict) -> dict:
        """供协调者直接 await agent.step(message) 驱动。"""
        return await self.check(message)

    async def _on_recovered(self, message: dict) -> None:
        """'device/recovered' 处理器：核对并发布 'check/event'。"""
        result = await self.check(message)
        await self.publish(self.RESULT_TOPIC, result)

    async def _on_recheck(self, message: dict) -> None:
        """'coord/recheck' 处理器：重新核对并发布 'check/event'（Phase 3）。

        Recheck 消息包含 t_reboot/t_recover，直接复用 check 逻辑。
        """
        result = await self.check(message)
        await self.publish(self.RESULT_TOPIC, result)

    async def run(self) -> None:
        """独立主循环：订阅 'device/recovered' + 'coord/recheck'。"""
        self.subscribe(self.RECOVERED_TOPIC, self._on_recovered)
        self.subscribe(self.RECHECK_TOPIC, self._on_recheck)  # Phase 3
        self._stop = asyncio.Event()
        try:
            await self._stop.wait()
        except asyncio.CancelledError:
            pass


# ----------------------------------------------------------------------------- #
# 单独运行入口
# ----------------------------------------------------------------------------- #
if __name__ == "__main__":
    import json

    from bus import EventBus
    from context import RunContext
    from config import load_config_from_env, RunConfig

    cfg: RunConfig = load_config_from_env()
    spec = Agent.AgentSpec if hasattr(Agent, "AgentSpec") else None  # 兼容性占位
    # 构造 AgentSpec（AgentSpec 定义在 agent 模块中）
    from agent import AgentSpec

    agent_spec = AgentSpec(
        name="event-checker",
        role=EventCheckAgent.ROLE,
        endpoint=f"http://{cfg.host}/ISAPI/AccessControl/AcsEvent",
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
    )
    ctx = RunContext()
    ctx.cfg = cfg

    bus = EventBus()
    agent = EventCheckAgent(agent_spec, bus, ctx)

    async def _demo() -> None:
        # 演示：监听结果并打印
        async def _on_result(msg: dict) -> None:
            print("[事件核对]", json.dumps(msg, ensure_ascii=False))

        bus.subscribe("check/event", _on_result)
        await agent.run()

    print(
        f"事件核对智能体就绪：订阅 {EventCheckAgent.RECOVERED_TOPIC} -> "
        f"发布 {EventCheckAgent.RESULT_TOPIC}（窗口={cfg.event_window}秒）。"
    )
    print("按 Ctrl+C 退出。")
    try:
        asyncio.run(_demo())
    except KeyboardInterrupt:
        print("\n已停止。")
