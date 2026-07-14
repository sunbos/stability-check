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
import secrets
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
    INCIDENT_ACK_TOPIC = "incident/ack"
    ADVISE_TOPIC = "analyst/advise"
    ADVISE_TIMEOUT = 35.0  # 须大于 Analyst 的 LLM 调用耗时（~25s），否则会误降级
    # Vote 机制（设计 §5.3）：Coordinator 广播 vote/request，收集 vote/reply。
    VOTE_REQUEST_TOPIC = "vote/request"
    VOTE_REPLY_TOPIC = "vote/reply"
    # Recheck 触发（设计 §5.4）：高风险/critical 时强制重检。
    RECHECK_TOPIC = "coord/recheck"
    # 投票者权重（设计 §5.3）：两个自治层 agent 等权。
    WEIGHTS = {"trend_supervisor": 0.5, "risk_analyst": 0.5}

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

        # ---- 决策矩阵状态（设计 §5.2 / §5.4）----
        # 跟踪未处理的 critical 事故，供决策矩阵强制 recheck。
        self._has_critical_incident: bool = False
        # 最近一次综合风险分（供 _should_ack_incident 使用）。
        self._last_risk_score: int = 50
        # Recheck 状态：决策矩阵返回 recheck 时触发重新检查（最多 1 次）。
        self._recheck_pending: bool = False

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
        # 投票超时（设计 §5.3）：无投票者时快速超时回退默认风险。
        self.vote_timeout = getattr(c, "vote_timeout", 1.0) if c else 1.0

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

        # ---- 策略层：投票 + 决策矩阵（设计 §5.3 / §5.4）----
        # 仅在事实层干净通过时收集投票；事实层失败时跳过投票（节省超时）。
        # 决策矩阵是建议性的：风险分不能把 fail 改成 pass（安全底线），
        # 但可以在 pass 上叠加 warn/recheck 标记供日志和后续 recheck 机制使用。
        decision = "fail" if failed else "pass"
        risk_score = self._last_risk_score
        if not failed:
            try:
                vote_result = await self._collect_votes(
                    round_no=round_no,
                    facts={
                        "found": found,
                        "changed": changed,
                        "t_recover": recover_time,
                    },
                    timeout=self.vote_timeout,
                )
                risk_score = vote_result["risk_score"]
                self._last_risk_score = risk_score
                decision = self._apply_decision_matrix(
                    found=found,
                    changed=changed,
                    risk_score=risk_score,
                    has_critical=self._has_critical_incident,
                )
                # 可见性：决策矩阵推理过程打印。
                ts_dm = time.strftime("%H:%M:%S", time.localtime())
                print(
                    f"[{ts_dm}] [L2·协调者] 第 {round_no} 轮: "
                    f"事实(found={found},changed={changed}) "
                    f"风险={risk_score} "
                    f"critical={self._has_critical_incident} "
                    f"-> 决策={decision} "
                    f"(投票方法={vote_result.get('method')})"
                )
            except Exception:  # noqa: BLE001 - 投票失败不阻塞核心循环
                # Phase 5: 保守降级——投票异常时 warn 而非 pass（安全优先）
                decision = "warn"
                risk_score = 60  # 保守中高风险
                self._last_risk_score = risk_score
                ts_err = time.strftime("%H:%M:%S", time.localtime())
                print(
                    f"[{ts_err}] [L2·协调者] 第 {round_no} 轮: "
                    f"投票异常，保守降级决策=warn 风险={risk_score}"
                )
            finally:
                # 每轮评估后重置 critical 标记（已 consumed）。
                self._has_critical_incident = False
        else:
            # 可见性：事实层失败时也打印决策矩阵推理。
            ts_dm = time.strftime("%H:%M:%S", time.localtime())
            print(
                f"[{ts_dm}] [L2·协调者] 第 {round_no} 轮: "
                f"事实层失败(found={found},changed={changed}) "
                f"-> 决策=fail (跳过投票)"
            )

        if failed:
            self.total_failures += 1
            self.consecutive_failures += 1
        else:
            self.consecutive_failures = 0
        # 每一轮都执行了一次重启，计数累加（供策略/日志）。
        self.consecutive_reboots += 1
        self.ctx.set_consecutive_reboots(self.consecutive_reboots)

        # ---- Recheck 机制（设计 §5.4）：决策矩阵返回 recheck 时触发重新检查 ----
        # 发布 coord/recheck，EventCheck/StatusCheck 重新检查设备。
        # 最多 recheck 1 次（_recheck_pending 标志防止无限循环）。
        if decision == "recheck" and not self._recheck_pending:
            self._recheck_pending = True
            ts_rc = time.strftime("%H:%M:%S", time.localtime())
            print(f"[{ts_rc}] [L2·协调者] 第 {round_no} 轮触发 recheck（风险={risk_score}）")
            await self.publish(self.RECHECK_TOPIC, {
                "round_no": round_no,
                "trigger": "decision_matrix",
                "t_reboot": t_reboot,
                "t_recover": t_recover,
            })
            # Reset check flags to await recheck results
            self._round["got_event"] = False
            self._round["got_status"] = False
            self._round["event"] = None
            self._round["status"] = None
            return  # Don't finalize round; await recheck results

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
            "decision": decision,
            "risk_score": risk_score,
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
        decision_str = f" 决策={decision} 风险={risk_score}" if not failed else ""
        print(
            f"[{ts}] [L2·协调者] 第 {round_no} 轮 {tag} "
            f"事件={found} 状态偏移={changed} "
            f"恢复耗时={rt_str} "
            f"累计失败={self.total_failures} "
            f"连续失败={self.consecutive_failures}"
            f"{decision_str}"
        )
        self.ctx.append_log(
            f"[L2·协调者] 第 {round_no} 轮 {tag} "
            f"累计失败={self.total_failures}"
        )

        # 广播给观察者（Scribe/Notifier）做记录。
        await self.publish(self.ROUND_DONE_TOPIC, record)
        # Broadcast state snapshot so autonomous agents can refresh their views.
        await self.ctx.publish_state(self.bus)

        # Reset recheck flag after round is finalized.
        self._recheck_pending = False

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

        直接作为失败轮结束，并发布 'round/done' 供 Scribe 记录。
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
        self.ctx.append_log(f"[L2·协调者] 第 {round_no} 轮 失败: {reason}")
        ts = time.strftime("%H:%M:%S", time.localtime())
        print(f"[{ts}] [L2·协调者] 第 {round_no} 轮 失败: {reason}")

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
    # 决策矩阵：投票综合 + 事实/风险决策 + 事故 ack（设计 §5.2/§5.3/§5.4）
    # ------------------------------------------------------------------ #
    def _combine_votes(self, replies: list) -> dict:
        """综合投票回复为单一风险评估（设计 §5.3）。

        使用 confidence × voter_weight 加权平均。返回：
        - 无回复 → {"risk_score": 50, "method": "default", "voters": []}
        - 全部弃权（confidence=0）→ {"risk_score": 50, "method": "all_abstain", "voters": [...]}
        - 正常加权 → {"risk_score": <int>, "method": "weighted", "voters": [...]}
        """
        if not replies:
            return {"risk_score": 50, "method": "default", "voters": []}

        total_weight_conf = 0.0
        weighted_sum = 0.0
        voters: list = []
        for r in replies:
            voter = r.get("voter", "unknown")
            risk = r.get("risk_score", 50)
            conf = float(r.get("confidence", 0.0))
            weight = self.WEIGHTS.get(voter, 0.0)
            wc = weight * conf
            total_weight_conf += wc
            weighted_sum += risk * wc
            voters.append(voter)

        if total_weight_conf == 0:
            return {"risk_score": 50, "method": "all_abstain", "voters": voters}

        risk_score = round(weighted_sum / total_weight_conf)
        return {"risk_score": risk_score, "method": "weighted", "voters": voters}

    def _apply_decision_matrix(
        self,
        found: bool,
        changed: bool,
        risk_score: int,
        has_critical: bool,
    ) -> str:
        """应用决策矩阵确定本轮结果（设计 §5.4）。

        事实层独裁（安全底线）：found=False 或 changed=True → "fail"。
        风险分修饰通过决策：<60→pass, 60-80→warn, >80→recheck。
        Critical 事故强制 recheck（无论风险分）。
        安全保证：风险分绝不能把 fail 改成 pass。
        """
        # 事实层独裁（安全底线）
        if not found or changed:
            return "fail"
        # Critical 事故强制 recheck
        if has_critical:
            return "recheck"
        # 风险修饰决策
        if risk_score > 80:
            return "recheck"
        if risk_score >= 60:
            return "warn"
        return "pass"

    def _should_ack_incident(self, incident: dict, current_risk: int) -> dict:
        """决定如何确认事故（设计 §5.2）。

        返回 {"decision": str, "action": str, "reason": str}：
        - critical → accepted + coord/recheck
        - warn + risk>60 → accepted + coord/recheck
        - warn + risk<=60 → logged + none
        - info → logged + none
        """
        severity = (incident.get("severity") or "info").lower()
        if severity == "critical":
            return {
                "decision": "accepted",
                "action": "coord/recheck",
                "reason": "Critical incident requires immediate recheck",
            }
        if severity == "warn" and current_risk > 60:
            return {
                "decision": "accepted",
                "action": "coord/recheck",
                "reason": (
                    f"Warn incident with elevated risk ({current_risk})"
                    " triggers recheck"
                ),
            }
        return {
            "decision": "logged",
            "action": "none",
            "reason": "Incident logged but no action required",
        }

    async def _collect_votes(
        self, round_no: int, facts: dict, timeout: float = 5.0
    ) -> dict:
        """广播 vote/request 并收集 vote/reply（设计 §5.3）。

        按 req_id 过滤回复，超时后回退到默认中性风险（50）。
        无投票者时 method="default"；全部弃权时 method="all_abstain"。
        """
        req_id = secrets.token_hex(8)
        replies: list = []

        async def _vote_handler(msg: dict) -> None:
            if msg.get("req_id") == req_id:
                replies.append(msg)
                # 可见性：每个投票到达时立即打印（让投票过程可观察）。
                ts = time.strftime("%H:%M:%S", time.localtime())
                voter = msg.get("voter", "unknown")
                risk = msg.get("risk_score", "?")
                conf = msg.get("confidence", "?")
                method = msg.get("method", "?")
                rationale = msg.get("rationale", "")
                print(
                    f"[{ts}] [L2·协调者] 收到 {voter} 投票: "
                    f"风险={risk} 置信度={conf} 方法={method} "
                    f"说明={rationale}"
                )

        self.bus.subscribe(self.VOTE_REPLY_TOPIC, _vote_handler)
        # 可见性：vote/request 发出时打印。
        ts_req = time.strftime("%H:%M:%S", time.localtime())
        print(
            f"[{ts_req}] [L2·协调者] 第 {round_no} 轮发起投票请求 "
            f"(超时={timeout:.1f}s 事实={facts})"
        )
        try:
            await self.bus.publish(
                self.VOTE_REQUEST_TOPIC,
                {
                    "round_no": round_no,
                    "facts": facts,
                    "req_id": req_id,
                    "history_summary": {},
                },
            )
            # 收集回复：无回复时等待完整超时；有回复后等待短暂静默期再退出。
            # Phase 4 快速路径：任一投票者 risk>=90 立即返回（不等其他）。
            FAST_PATH_THRESHOLD = 90
            loop = asyncio.get_event_loop()
            deadline = loop.time() + timeout
            last_count = 0
            last_change = loop.time()
            while loop.time() < deadline:
                await asyncio.sleep(0.02)
                now = loop.time()
                if len(replies) > last_count:
                    last_count = len(replies)
                    last_change = now
                    # Fast path: high-risk vote triggers immediate return
                    for r in replies:
                        if r.get("risk_score", 0) >= FAST_PATH_THRESHOLD:
                            ts_fp = time.strftime("%H:%M:%S", time.localtime())
                            print(
                                f"[{ts_fp}] [L2·协调者] 快速路径触发: "
                                f"{r.get('voter')} 风险={r.get('risk_score')} "
                                f">= {FAST_PATH_THRESHOLD}"
                            )
                            break
                    else:
                        continue
                    break  # Fast path exit
                elif replies and (now - last_change) >= 0.05:
                    break
        finally:
            self.bus.unsubscribe(self.VOTE_REPLY_TOPIC, _vote_handler)

        result = self._combine_votes(replies)
        # 可见性：综合结果打印。
        ts_done = time.strftime("%H:%M:%S", time.localtime())
        voters_str = ",".join(result.get("voters", [])) or "(无)"
        print(
            f"[{ts_done}] [L2·协调者] 综合结果: "
            f"风险={result['risk_score']} 方法={result['method']} "
            f"投票者=[{voters_str}]"
        )
        return result

    async def _on_incident(self, message: dict) -> None:
        """处理 incident/raise：按决策矩阵 ack 并跟踪 critical（设计 §5.2）。

        Coordinator 必须确认每个事故（强制回声），但不 ack 自己 raise 的
        （raised_by == "coordinator"）。
        """
        # 兼容两种消息格式：直接 incident dict 或 {"incident": {...}} 包装。
        incident = message if "severity" in message else message.get("incident", message)
        raised_by = incident.get("raised_by", "")

        # 不 ack 自己 raise 的事故（设计 §5.2 强制回声规则）。
        if raised_by == "coordinator":
            return

        severity = (incident.get("severity") or "info").lower()
        # 跟踪 critical 事故，供决策矩阵强制 recheck。
        if severity == "critical":
            self._has_critical_incident = True

        ack = self._should_ack_incident(incident, self._last_risk_score)
        # 可见性：事故 ack 打印（让 incident 闭环可观察）。
        ts = time.strftime("%H:%M:%S", time.localtime())
        category = incident.get("category", "?")
        desc = incident.get("description", "")
        print(
            f"[{ts}] [L2·协调者] 收到 {raised_by} 事故: "
            f"严重={severity} 类别={category} 描述={desc}"
        )
        print(
            f"[{ts}] [L2·协调者] 事故确认: "
            f"决策={ack['decision']} 动作={ack['action']} 原因={ack['reason']}"
        )
        await self.publish(
            self.INCIDENT_ACK_TOPIC,
            {
                "incident_id": incident.get("incident_id"),
                "ack_decision": ack["decision"],
                "ack_action": ack["action"],
                "ack_reason": ack["reason"],
                "ack_by": "coordinator",
            },
        )

        # 若 action 为 recheck，触发重检信号。
        if ack["action"] == self.RECHECK_TOPIC:
            await self.publish(
                self.RECHECK_TOPIC,
                {
                    "round_no": self.round_no,
                    "trigger": "incident",
                    "incident_id": incident.get("incident_id"),
                },
            )

    # ------------------------------------------------------------------ #
    # 单轮驱动
    # ------------------------------------------------------------------ #
    async def _start_round(self, round_no: int) -> None:
        self._round = self._new_round_state()
        self._round["round_no"] = round_no
        # 在共享清单上登记本轮任务（pending），完成时用 mark 更新其状态。
        self.ctx.board.add(Task(name=f"round/{round_no}", status="pending"))
        # 轮次分隔线：每轮开始时打印醒目分隔，便于阅读。
        ts = time.strftime("%H:%M:%S", time.localtime())
        print(f"[{ts}] [L2·协调者] ═════════════ 第 {round_no} 轮开始 ═════════════")
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
        # 订阅事故（设计 §5.2）：接收自治层 agent 的 incident/raise 并 ack。
        self.subscribe(self.INCIDENT_TOPIC, self._on_incident)

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
