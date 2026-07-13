"""协调者（Coordinator）：整场拷机的管理者与共享清单维护者。

职责
----
* 持有 RunContext 与 TaskBoard（共享清单）。
* 驱动每一轮：发 'coord/reboot' -> 收 'reboot/done' -> 收 'device/recovered'
  -> 收 'check/event' + 'check/status' -> 评估本轮 passed = found and not changed。
* 维护 round_no / total_failures / consecutive_failures / consecutive_reboots。
* 失败超阈值时 publish 'coord/abort' 并结束。
* 自适应间隔：interval = clamp(recover_time*k + base_interval, interval_min, interval_max)。

仅依赖标准库 asyncio / time / dataclasses / os / sys，不修改任何 foundation。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

# 让本模块能绝对导入 bus / agent / context（同处 harness/）与 agents/ 下的模块。
_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
_AGENTS_DIR = os.path.join(os.path.dirname(_HARNESS_DIR), "agents")
for _p in (_HARNESS_DIR, _AGENTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from agent import Agent  # noqa: E402
from bus import EventBus  # noqa: E402
from context import RunContext, Task  # noqa: E402


class Coordinator(Agent):
    """拷机协调者：驱动整场拷机并维护共享清单。"""

    REBOOT_TOPIC = "coord/reboot"
    REBOOT_DONE_TOPIC = "reboot/done"
    RECOVERED_TOPIC = "device/recovered"
    EVENT_TOPIC = "check/event"
    STATUS_TOPIC = "check/status"
    ABORT_TOPIC = "coord/abort"
    ROUND_DONE_TOPIC = "round/done"
    # 事故 / 分析决策（策略层）：无恢复（疑似断电）时请求 Analyst 是否继续。
    INCIDENT_TOPIC = "incident/raise"
    ADVISE_TOPIC = "analyst/advise"
    ADVISE_TIMEOUT = 35.0  # 须大于 Analyst 的 LLM 调用耗时（~25s），否则会误降级

    def __init__(self, spec, bus: EventBus, ctx: RunContext, cfg=None) -> None:
        super().__init__(spec, bus, ctx)
        self.cfg = cfg if cfg is not None else getattr(ctx, "cfg", None)

        # ---- 计数器 / 轮次状态 ----
        self.round_no = 0
        self.total_failures = 0
        self.consecutive_failures = 0
        self.consecutive_reboots = 0

        self._start_time = 0.0
        self._aborted = False
        self._stop_event = None
        self._round_done_event: asyncio.Event | None = None
        self._round = self._new_round_state()
        # CoordinatorContext.aborted 默认 False，无需在此重复设置。

        # ---- 配置（缺失时回退默认值，保持可独立运行）----
        c = self.cfg
        self.fail_threshold = getattr(c, "fail_threshold", 5) if c else 5
        self.fail_consecutive = getattr(c, "fail_consecutive", 3) if c else 3
        self.recover_timeout = getattr(c, "recover_timeout", 180.0) if c else 180.0
        self.base_interval = getattr(c, "base_interval", 60.0) if c else 60.0
        self.interval_min = getattr(c, "interval_min", 30.0) if c else 30.0
        self.interval_max = getattr(c, "interval_max", 600.0) if c else 600.0
        self.k = getattr(c, "k", 1.5) if c else 1.5
        self.max_rounds = getattr(c, "max_rounds", 0) if c else 0
        self.max_duration = getattr(c, "max_duration", 0.0) if c else 0.0

    # ------------------------------------------------------------------ #
    # 内部状态辅助
    # ------------------------------------------------------------------ #
    @staticmethod
    def _new_round_state() -> dict:
        """本轮待收结果状态（同一时刻只有一轮在飞）。"""
        return {
            "round_no": None,
            "t_reboot": None,
            "t_recover": None,
            "reboot_ok": None,
            "event": None,
            "status": None,
            "got_event": False,
            "got_status": False,
        }

    # ------------------------------------------------------------------ #
    # 订阅处理
    # ------------------------------------------------------------------ #
    async def _on_reboot_done(self, message: dict) -> None:
        round_no = message.get("round_no")
        if round_no != self.round_no:
            return
        self._round["t_reboot"] = message.get("t_reboot")
        self._round["reboot_ok"] = message.get("ok")
        if message.get("ok") is not True:
            # 重启失败：设备不会再 recovered，本轮直接判失败并收尾。
            self._round["t_recover"] = None
            self._complete_failure(round_no, "重启失败", message.get("error"))

    async def _on_recovered(self, message: dict) -> None:
        round_no = message.get("round_no")
        if round_no != self.round_no:
            return
        self._round["t_recover"] = message.get("t_recover")

    async def _on_event(self, message: dict) -> None:
        round_no = message.get("round_no")
        if round_no != self.round_no:
            return
        self._round["event"] = message
        self._round["got_event"] = True
        self._maybe_evaluate()

    async def _on_status(self, message: dict) -> None:
        round_no = message.get("round_no")
        if round_no != self.round_no:
            return
        self._round["status"] = message
        self._round["got_status"] = True
        self._maybe_evaluate()

    async def _on_abort(self, message: dict) -> None:
        """收到 'coord/abort'（可能由自身触发）：标记中止并释放轮次等待。"""
        self._aborted = True
        self.ctx.mark_aborted()
        reason = message.get("reason", "unknown")
        self.ctx.append_log(f"[协调者] 中止: {reason}")
        if self._round_done_event is not None:
            self._round_done_event.set()

    # ------------------------------------------------------------------ #
    # 评估 / 收尾
    # ------------------------------------------------------------------ #
    def _maybe_evaluate(self) -> None:
        if self._round["got_event"] and self._round["got_status"]:
            # 用同步评估（内部会 await publish），由调用方在协程中 await。
            # 这里直接调用协程并交由事件循环驱动：把评估作为后台任务触发，
            # 避免阻塞当前 publish 链（保持与 bus 顺序调用一致）。
            asyncio.create_task(self._evaluate())

    async def _evaluate(self) -> None:
        r = self._round
        round_no = r["round_no"]
        ev = r["event"] or {}
        st = r["status"] or {}

        found = bool(ev.get("found", False))
        changed = bool(st.get("changed", False))
        t_recover = r["t_recover"]
        t_reboot = r["t_reboot"]

        # 事故：重启成功但设备始终未恢复（疑似断电 / 网络中断）。
        # 交由策略层的 Analyst 决定是否继续；无 Analyst（或超时/异常）时
        # 回退到确定性核心，绝不因 LLM 不可用而卡死整场拷机。
        if t_recover is None and r["reboot_ok"] is True:
            await self._handle_no_recovery(round_no, r)
            return

        recover_time = (
            t_recover - t_reboot
            if (t_recover is not None and t_reboot is not None)
            else None
        )
        passed = bool(found and not changed)

        # 干净通过：重启成功 + 已恢复 + 检查无错 + 状态回归基线且事件已落。
        clean = (
            r["reboot_ok"] is True
            and t_recover is not None
            and not ev.get("error")
            and not st.get("error")
            and passed
        )
        failed = not clean

        if failed:
            self.total_failures += 1
            self.consecutive_failures += 1
        else:
            self.consecutive_failures = 0
        # 每一轮都执行了一次重启，计数累加（供策略/日志）。
        self.consecutive_reboots += 1
        self.ctx.set_consecutive_reboots(self.consecutive_reboots)

        record = {
            "round": round_no,
            "round_no": round_no,
            "passed": passed,
            "found": found,
            "changed": changed,
            "t_reboot": t_reboot,
            "t_recover": t_recover,
            "recover_time": recover_time,
            "reboot_ok": r["reboot_ok"],
            "event_error": ev.get("error"),
            "status_error": st.get("error"),
            "diff": st.get("diff"),
            "timestamp": time.time(),
        }

        # 写入共享清单与轮次历史。
        self.ctx.board.mark(f"round/{round_no}", "done", record)
        self.ctx.append_round(record)

        tag = "通过" if passed else "失败"
        ts = time.strftime("%H:%M:%S", time.localtime())
        rt_str = (
            f"{recover_time:.1f}秒"
            if isinstance(recover_time, (int, float))
            else "NA"
        )
        print(
            f"[{ts}] [拷机] 第 {round_no} 轮 {tag} "
            f"事件={found} 状态偏移={changed} "
            f"恢复耗时={rt_str} "
            f"累计失败={self.total_failures} "
            f"连续失败={self.consecutive_failures}"
        )
        self.ctx.append_log(
            f"[拷机] 第 {round_no} 轮 {tag} "
            f"累计失败={self.total_failures}"
        )

        # 回报 ReporterAgent 做汇总。
        await self.publish(self.ROUND_DONE_TOPIC, record)
        # Broadcast state snapshot so autonomous agents can refresh their views.
        await self.ctx.publish_state(self.bus)

        # 检查中止阈值。
        if self.total_failures >= self.fail_threshold:
            await self._abort(
                f"累计失败 {self.total_failures} >= 阈值 {self.fail_threshold}"
            )
            return
        if self.consecutive_failures >= self.fail_consecutive:
            await self._abort(
                f"连续失败 {self.consecutive_failures} "
                f">= 上限 {self.fail_consecutive}"
            )
            return

        # 释放本轮等待（run 主循环据此推进）。
        if self._round_done_event is not None:
            self._round_done_event.set()

    def _complete_failure(self, round_no, reason: str, error=None) -> None:
        """重启失败 / 无 recovered 等无法走正常核对路径时的收尾。

        直接作为失败轮结束，并发布 'round/done' 供 ReporterAgent 记录。
        """
        self.total_failures += 1
        self.consecutive_failures += 1
        self.consecutive_reboots += 1
        self.ctx.set_consecutive_reboots(self.consecutive_reboots)

        record = {
            "round": round_no,
            "round_no": round_no,
            "passed": False,
            "found": False,
            "changed": True,
            "t_reboot": self._round.get("t_reboot"),
            "t_recover": self._round.get("t_recover"),
            "recover_time": None,
            "reboot_ok": self._round.get("reboot_ok"),
            "event_error": None,
            "status_error": error,
            "diff": {},
            "note": reason,
            "timestamp": time.time(),
        }
        self.ctx.board.mark(f"round/{round_no}", "failed", record)
        self.ctx.append_round(record)
        self.ctx.append_log(f"[拷机] 第 {round_no} 轮 失败: {reason}")
        ts = time.strftime("%H:%M:%S", time.localtime())
        print(f"[{ts}] [拷机] 第 {round_no} 轮 失败: {reason}")

        asyncio.create_task(self._publish_and_check_abort(record))

    async def _publish_and_check_abort(self, record: dict) -> None:
        await self.publish(self.ROUND_DONE_TOPIC, record)
        # Broadcast state snapshot so autonomous agents can refresh their views.
        await self.ctx.publish_state(self.bus)
        if self.total_failures >= self.fail_threshold:
            await self._abort(
                f"累计失败 {self.total_failures} >= 阈值 {self.fail_threshold}"
            )
            return
        if self.consecutive_failures >= self.fail_consecutive:
            await self._abort(
                f"连续失败 {self.consecutive_failures} "
                f">= 上限 {self.fail_consecutive}"
            )
            return
        if self._round_done_event is not None:
            self._round_done_event.set()

    async def _abort(self, reason: str) -> None:
        self._aborted = True
        self.ctx.mark_aborted()
        await self.publish(self.ABORT_TOPIC, {"reason": reason})
        # 释放 run() 主循环对 round/done 的等待，使其进入中止分支。
        if self._round_done_event is not None:
            self._round_done_event.set()

    # ------------------------------------------------------------------ #
    # 事故处理：无恢复（疑似断电）→ 请求 Analyst 决策
    # ------------------------------------------------------------------ #
    async def _handle_no_recovery(self, round_no: int, r: dict) -> None:
        """重启成功但设备未恢复：作为事故上报并请 Analyst 决策是否继续。"""
        incident = {
            "kind": "no_recovery",
            "round_no": round_no,
            "consecutive_failures": self.consecutive_failures,
            "total_failures": self.total_failures,
        }
        # 广播事故（供 Scribe / Notifier 记录），同时请求 Analyst 决策。
        await self.publish(self.INCIDENT_TOPIC, {"incident": incident})
        decision = await self._consult_analyst(incident)

        if decision is not None and decision.get("continue") is False:
            # 策略层（人或 LLM 代理）判断应停机：中止整个拷机。
            await self._abort(
                f"分析智能体叫停: {decision.get('reason', '设备未恢复')}"
            )
            return

        # Analyst 建议继续，或 Analyst 不可用（确定性降级）：照常记失败 + 阈值检查。
        self._complete_failure(
            round_no, "设备未恢复（疑似断电）", "未恢复"
        )

    async def _consult_analyst(self, incident: dict) -> dict | None:
        """请求 Analyst 的事故决策，返回 {continue, reason, source} 或 None。

        任何异常（Analyst 未在线 / 超时 / LLM 故障）均返回 None，由确定性核心
        自行决策——保证 LLM 永不阻塞或劫持核心循环。
        """
        try:
            return await self.bus.request(
                self.ADVISE_TOPIC, {"incident": incident}, timeout=self.ADVISE_TIMEOUT
            )
        except Exception:  # noqa: BLE001 - 含 TimeoutError / 无订阅者等情况
            self.ctx.append_log(
                "[协调者] 分析智能体不可用，回退确定性决策"
            )
            return None

    # ------------------------------------------------------------------ #
    # 单轮驱动
    # ------------------------------------------------------------------ #
    async def _start_round(self, round_no: int) -> None:
        self._round = self._new_round_state()
        self._round["round_no"] = round_no
        # 在共享清单上登记本轮任务（pending），完成时用 mark 更新其状态。
        self.ctx.board.add(Task(name=f"round/{round_no}", status="pending"))
        await self.publish(self.REBOOT_TOPIC, {"round_no": round_no})

    def _compute_interval(self) -> float:
        """自适应间隔：recover_time*k + base_interval，clamp 到 [min, max]。"""
        r = self._round
        t_reboot = r.get("t_reboot")
        t_recover = r.get("t_recover")
        if t_reboot is not None and t_recover is not None:
            recover_time = t_recover - t_reboot
        else:
            recover_time = 0.0
        interval = recover_time * self.k + self.base_interval
        interval = max(self.interval_min, min(self.interval_max, interval))
        return interval

    # ------------------------------------------------------------------ #
    # 独立主循环（主驱动）
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        self._start_time = time.time()
        self._round_done_event = asyncio.Event()
        self._aborted = False
        # CoordinatorContext.aborted 由 mark_aborted() 管理，run() 启动时无需重置。

        # 订阅关心的主题。
        self.subscribe(self.REBOOT_DONE_TOPIC, self._on_reboot_done)
        self.subscribe(self.RECOVERED_TOPIC, self._on_recovered)
        self.subscribe(self.EVENT_TOPIC, self._on_event)
        self.subscribe(self.STATUS_TOPIC, self._on_status)
        self.subscribe(self.ABORT_TOPIC, self._on_abort)

        self.ctx.append_log("[协调者] 运行开始")

        while not self._aborted:
            # 终止：达到最大轮次。
            if self.max_rounds > 0 and self.round_no >= self.max_rounds:
                break
            # 终止：超过最大运行时长。
            if self.max_duration > 0 and (time.time() - self._start_time) > self.max_duration:
                self.ctx.append_log("[协调者] 已达最大运行时长")
                break

            # 启动新一轮（保证上一轮 round/done 已收才发新 reboot）。
            self.round_no += 1
            await self._start_round(self.round_no)

            # 等待本轮结果收齐（事件/状态到齐或失败收尾会 set 该事件）。
            await self._round_done_event.wait()
            self._round_done_event.clear()

            if self._aborted:
                break

            # 自适应冷却后再发下一轮。
            interval = self._compute_interval()
            self.ctx.append_log(
                f"[协调者] 下一轮前冷却 {interval:.1f} 秒"
            )
            await asyncio.sleep(interval)

        self.ctx.append_log("[协调者] 运行结束")


if __name__ == "__main__":
    # 简易独立运行入口（需外部已建 bus + ctx + cfg）。
    from bus import EventBus
    from context import RunContext
    from config import load_config_from_env
    from agent import AgentSpec

    cfg = load_config_from_env()
    bus = EventBus()
    ctx = RunContext(strategy_text=cfg.strategy_text)
    ctx.cfg = cfg
    spec = AgentSpec(
        name="coordinator",
        role="coordinator",
        endpoint="",
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
    )
    coord = Coordinator(spec, bus, ctx, cfg=cfg)
    print("协调者就绪；执行 asyncio.run(coordinator.run()) 即可驱动。")
    asyncio.run(coord.run())
