"""AnalystAgent / RiskAnalyst：自治层风险评估智能体（仅使用标准库）。

定位（关键设计约束）
--------------------
* 这是 **L3 自治层** 智能体（RiskAnalyst 角色），**绝不参与每一轮的 pass/fail 判定**。
  每一轮的 pass/fail 由确定性的 Coordinator（Loop Core）负责，保证核心逻辑
  可复现、低维护。
* **三重职责**：
  1. 事故决策（`analyst/advise`）：Coordinator 在事故时请求决策；无 LLM 时规则引擎
     覆盖“断电/超时”“连续失败”等场景，确保人不在场时也能自主停机。
  2. 风险投票（`vote/request` → `vote/reply`）：每轮用 LLM 评估风险分（0-100），
     供 Coordinator 加权综合。LLM 不可用时弃权（abstain）。
  3. 主动事故（`incident/raise`）：风险分 > 80 连续 3 轮 → critical；单轮 ≥ 90 → warn。
     这是自治性的核心 —— 不再被动等待 Coordinator 询问。
* **优雅降级**：无 LLM key（或调用失败/超时）时，advise 回退规则引擎，vote 弃权，
  主动事故静默。TrendSupervisor（纯规则）独立工作，自治层至少有一个投票者。
* LLM 默认来自 OpenRouter（`tencent/hy3:free`），可经环境变量切换到任意 OpenAI 兼容 API。
  key **只**来自环境变量/`.env`，绝不被打印或硬编码。

通信（只走总线）
--------------
* 订阅 `analyst/advise`（request/response）：事故决策请求；回 `analyst/advise/reply`。
* 订阅 `vote/request`（publish）：风险评估投票；回 `vote/reply`（correlated by req_id）。
* 订阅 `incident/raise`（publish）：记录事故并广播 `analyst/decision`。
* 订阅 `round/done`（publish）：累积私有 `recent_rounds` + 产出 `analyst/report`。
* 主动发布 `incident/raise`：高风险时主动告警（自治性核心）。

仅依赖标准库 + 同仓 bus / agent / context / llm_client，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from collections import deque

# harness 内模块可被直接导入（与 loader 同手法）。
_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)

from agent import Agent  # noqa: E402
from llm_client import _extract_first_json  # noqa: E402


def _heuristic_continue(text: str) -> bool | None:
    """从自然语言文本推测是否继续。

    返回 True（继续）/ False（停止）/ None（无法判定）。
    优先识别显式 true/false；其次中文“停止/停机/中止/不应继续”→ False，
    “继续/可以/保持”→ True。
    """
    if not text:
        return None
    low = text.lower()

    # 显式 JSON 风格布尔
    import re

    m = re.search(r"\"?continue\"?\s*[:=]\s*(true|false)", low)
    if m:
        return m.group(1) == "true"

    # 中文意图词
    stop_words = ["停止", "停机", "中止", "不应继续", "不要继续", "建议停", "需停"]
    go_words = ["继续", "可以", "保持", "无需停", "建议继续"]
    if any(w in text for w in stop_words):
        return False
    if any(w in text for w in go_words):
        return True
    return None


class AnalystAgent(Agent):
    """L3 自治层风险评估智能体（RiskAnalyst 角色）。

    三重职责：事故决策（advise）+ 风险投票（vote）+ 主动告警（proactive incident）。
    确定性核心之外的政策大脑；LLM 不可用时全面降级，永不卡死。
    """

    ADVISE_TOPIC = "analyst/advise"
    ADVISE_REPLY = "analyst/advise/reply"
    REPORT_TOPIC = "analyst/report"
    DECISION_TOPIC = "analyst/decision"
    ROUND_DONE = "round/done"
    INCIDENT = "incident/raise"
    VOTE_REQUEST = "vote/request"
    VOTE_REPLY = "vote/reply"

    # Proactive incident thresholds (design §5.2)
    HIGH_RISK_THRESHOLD = 80        # risk > 80 → counts toward consecutive
    VERY_HIGH_RISK_THRESHOLD = 90   # single round >= 90 → warn
    CRITICAL_CONSECUTIVE = 3        # 3 consecutive high-risk → critical

    def __init__(self, spec, bus, ctx, cfg=None) -> None:
        super().__init__(spec, bus, ctx)
        self.cfg = cfg if cfg is not None else getattr(ctx, "cfg", None)
        # 是否启用 LLM（默认启用；env BURNIN_ANALYST=off 可关闭纯规则运行）。
        self.enabled = self._read_enabled()
        # LLM 客户端懒加载（避免无 key 时也 import / 触发副作用）。
        self._llm = None
        self._llm_loaded = False
        # Private state (design §6.3 RiskAnalystState)
        self.recent_rounds: deque = deque(maxlen=10)
        self.last_risk_score: int = 50
        self._consecutive_high_risk: int = 0
        self._critical_raised_for_streak: bool = False

    def _read_enabled(self) -> bool:
        env = os.environ.get("BURNIN_ANALYST", "").strip().lower()
        if env in ("off", "0", "false", "no"):
            return False
        if self.cfg is not None:
            v = getattr(self.cfg, "analyst_enabled", None)
            if v is False:
                return False
        return True

    # ------------------------------------------------------------------ #
    # LLM 懒加载
    # ------------------------------------------------------------------ #
    def _ensure_llm(self):
        """返回 OpenAICompatibleClient 或 None（无 key / 未启用 → 规则降级）。"""
        if self._llm_loaded:
            return self._llm
        self._llm_loaded = True
        if not self.enabled:
            self._llm = None
            return None
        from llm_client import get_client

        self._llm = get_client()  # 无 key 时返回 None
        return self._llm

    # ------------------------------------------------------------------ #
    # 风险投票（vote/request → vote/reply）
    # ------------------------------------------------------------------ #
    def compute_vote(self, vote_request: dict) -> dict:
        """用 LLM 评估风险分（0-100），返回 vote/reply dict（不含 req_id）。

        LLM 不可用 / 响应无法解析 → 规则兜底投票（rule-based，confidence=0.4）。
        设计 §5.3 / §7：LLM 失败时不弃权，而是用规则给出有效风险分，确保投票有效。
        """
        llm = self._ensure_llm()
        if llm is None:
            return self._rule_based_vote(vote_request, "LLM 不可用")

        facts = vote_request.get("facts", {})
        history = vote_request.get("history_summary", {})
        system_prompt = (
            "你是门禁设备稳定性拷机的风险评估智能体。根据本轮事实与近期历史，"
            "评估风险分（0-100，越高越危险）。\n"
            "请以 JSON 返回："
            '{"risk_score": 0-100, "rationale": "中文简述", "confidence": 0-1}'
        )
        user_prompt = (
            f"本轮事实：{facts}\n"
            f"近期历史摘要：{history}\n"
            f"近期轮次：{self._short_history()}\n"
            "请评估风险。"
        )
        try:
            text = llm.chat(system_prompt, user_prompt, timeout=25.0)
        except Exception:
            return self._rule_based_vote(vote_request, "LLM 调用异常")
        if not text:
            return self._rule_based_vote(vote_request, "LLM 无响应")

        result = _extract_first_json(text)
        if not result:
            return self._rule_based_vote(vote_request, "LLM 响应无法解析")

        risk = result.get("risk_score")
        if not isinstance(risk, (int, float)) or not (0 <= risk <= 100):
            return self._rule_based_vote(vote_request, "LLM 风险分无效")

        confidence = result.get("confidence", 0.5)
        if not isinstance(confidence, (int, float)) or not (0 <= confidence <= 1):
            confidence = 0.5

        return {
            "voter": "risk_analyst",
            "risk_score": int(risk),
            "rationale": str(result.get("rationale", "")) or "LLM 未给出说明",
            "confidence": round(float(confidence), 2),
            "method": "llm",
        }

    def _rule_based_vote(self, vote_request: dict, reason: str) -> dict:
        """Rule-based fallback vote when LLM is unavailable (design §7).

        Uses recover_time trend and pass rate from history to estimate risk.
        Gives moderate confidence (0.4) — less nuanced than LLM, but provides
        a valid vote instead of abstaining, so the decision matrix has real
        input from both L3 agents.

        Rules:
          - Fact failure (found=False / changed=True) → risk=90
          - Recover time > 120s → +20; > 90s → +10
          - Recover time > 1.5× historical avg → +15 (spike)
          - Recover time > 1.2× historical avg → +8 (upward trend)
          - Fail rate > 30% in recent 3 rounds → +15
          - Baseline clean pass → risk=30
        """
        facts = vote_request.get("facts", {})
        t_recover = facts.get("t_recover")
        found = facts.get("found", True)
        changed = facts.get("changed", False)

        # Fact failures → high risk (defensive; shouldn't reach here on clean pass)
        if not found or changed:
            return {
                "voter": "risk_analyst",
                "risk_score": 90,
                "rationale": f"规则兜底({reason}): 事实层失败",
                "confidence": 0.4,
                "method": "rule",
            }

        risk = 30  # baseline: clean pass with normal recover time

        # Recover time absolute value
        if isinstance(t_recover, (int, float)):
            if t_recover > 120:
                risk += 20
            elif t_recover > 90:
                risk += 10

        # Recover time trend vs history
        recent = self._short_history(5)
        recover_times = [
            r.get("recover_time")
            for r in recent
            if r.get("recover_time") is not None
        ]
        if len(recover_times) >= 1 and isinstance(t_recover, (int, float)):
            avg = sum(recover_times) / len(recover_times)
            if avg > 0:
                if t_recover > avg * 1.5:
                    risk += 15  # spike
                elif t_recover > avg * 1.2:
                    risk += 8   # upward trend

        # Recent fail rate
        if len(recent) >= 2:
            recent_passes = sum(1 for r in recent[-3:] if r.get("passed", False))
            recent_total = min(3, len(recent))
            if recent_total > 0 and recent_passes < recent_total * 0.7:
                risk += 15  # degraded pass rate

        risk = max(0, min(100, risk))
        return {
            "voter": "risk_analyst",
            "risk_score": risk,
            "rationale": f"规则兜底({reason}): 恢复={t_recover}s 历史={len(recent)}轮",
            "confidence": 0.4,
            "method": "rule",
        }

    @staticmethod
    def _abstain_reply(reason: str = "弃权") -> dict:
        """Build an abstain vote reply (kept for backward compat / tests).

        New code uses _rule_based_vote instead; abstain is only for edge cases
        where even rule-based voting is impossible.
        """
        return {
            "voter": "risk_analyst",
            "risk_score": 50,
            "rationale": reason,
            "confidence": 0.0,
            "method": "abstain",
        }

    async def _on_vote_request(self, message: dict) -> None:
        """处理 vote/request：回 vote/reply + 更新风险跟踪 + 主动事故检查。"""
        round_no = message.get("round_no")
        # 可见性：vote/request 接收打印。
        ts_req = time.strftime("%H:%M:%S", time.localtime())
        print(f"[{ts_req}] [风险分析] 第 {round_no} 轮收到投票请求，开始评估...")

        # LLM 调用同步阻塞，用 run_in_executor 异步化避免阻塞事件循环。
        # 这样 Coordinator 的 vote_timeout 内能真正收到 vote/reply。
        loop = asyncio.get_event_loop()
        try:
            reply = await loop.run_in_executor(None, self.compute_vote, message)
        except Exception:  # noqa: BLE001 - LLM 异常时规则兜底
            reply = self._rule_based_vote(message, "LLM 执行异常")

        reply["req_id"] = message.get("req_id")

        # 可见性：vote/reply 发送打印。
        ts_reply = time.strftime("%H:%M:%S", time.localtime())
        risk = reply.get("risk_score", "?")
        conf = reply.get("confidence", "?")
        method = reply.get("method", "?")
        rationale = reply.get("rationale", "")
        print(
            f"[{ts_reply}] [风险分析] 第 {round_no} 轮投票回复: "
            f"风险={risk} 置信度={conf} 方法={method} 说明={rationale}"
        )

        await self.bus.publish(self.VOTE_REPLY, reply)
        # Update private risk tracking + proactive incident check
        self._update_risk_tracking(reply)
        await self._check_proactive_incident()

    def _update_risk_tracking(self, reply: dict) -> None:
        """Update last_risk_score + consecutive_high_risk counter."""
        risk = reply.get("risk_score", 50)
        self.last_risk_score = risk
        if risk > self.HIGH_RISK_THRESHOLD:
            self._consecutive_high_risk += 1
        else:
            self._consecutive_high_risk = 0
            self._critical_raised_for_streak = False  # reset streak dedup

    async def _check_proactive_incident(self) -> None:
        """Proactively raise incidents on sustained/very-high risk (design §5.2).

        - 3 consecutive rounds risk > 80 → critical (raised once per streak)
        - single round risk >= 90 → warn
        """
        if (
            self._consecutive_high_risk >= self.CRITICAL_CONSECUTIVE
            and not self._critical_raised_for_streak
        ):
            await self._try_raise(
                severity="critical",
                raised_by="risk_analyst",
                category="sustained_high_risk",
                description=(
                    f"风险分连续 {self._consecutive_high_risk} 轮 > "
                    f"{self.HIGH_RISK_THRESHOLD}（last={self.last_risk_score}）"
                ),
                evidence={
                    "consecutive": self._consecutive_high_risk,
                    "last_risk": self.last_risk_score,
                },
                suggestion="recheck",
            )
            self._critical_raised_for_streak = True
        elif self.last_risk_score >= self.VERY_HIGH_RISK_THRESHOLD:
            await self._try_raise(
                severity="warn",
                raised_by="risk_analyst",
                category="very_high_risk",
                description=f"单轮风险分极高（{self.last_risk_score}）",
                evidence={"risk": self.last_risk_score},
                suggestion="recheck",
            )

    # ------------------------------------------------------------------ #
    # 主动事故（incident/raise）
    # ------------------------------------------------------------------ #
    async def _raise_incident(self, **kw) -> None:
        """Build a full incident message and publish to incident/raise.

        Real implementation is async. Tests may replace this with a sync
        callable to capture kwargs (see _try_raise for the bridge).
        """
        incident = {
            "incident_id": f"inc-{uuid.uuid4().hex[:8]}",
            "timestamp": time.time(),
            **kw,
        }
        await self.bus.publish(self.INCIDENT, incident)

    async def _try_raise(self, **kw) -> None:
        """Call _raise_incident, awaiting if it's a coroutine.

        This bridge supports test mocking of _raise_incident with a sync
        callable (lambda **kw: ...). When the real async _raise_incident is
        in place, calling it returns a coroutine which we then await.
        """
        # 可见性：主动事故 raise 打印（让自治层主动行为可观察）。
        ts = time.strftime("%H:%M:%S", time.localtime())
        severity = kw.get("severity", "?")
        category = kw.get("category", "?")
        description = kw.get("description", "")
        print(
            f"[{ts}] [风险分析] 主动 raise 事故: "
            f"严重={severity} 类别={category} 描述={description}"
        )
        result = self._raise_incident(**kw)
        if asyncio.iscoroutine(result):
            await result

    # ------------------------------------------------------------------ #
    # 决策：LLM 优先，失败/无 key 回退规则
    # ------------------------------------------------------------------ #
    def decide(self, incident: dict) -> dict:
        """针对一次事故给出是否继续拷机的决策。

        返回 {continue: bool, reason: str, source: 'llm'|'rule', analysis: str}。
        """
        llm = self._ensure_llm()
        if llm is not None:
            out = self._llm_decide(llm, incident)
            if out is not None:
                return out
        return self._rule_decide(incident)

    def _rule_decide(self, incident: dict) -> dict:
        """规则引擎：覆盖多场景，无需网络/密钥。

        场景（与人手工处理拷机的思路对齐）：
        * no_recovery   设备未在时限内恢复（疑似断电/网络中断）→ 继续重启无意义，停机。
        * reboot_failed 重启命令本身失败（设备可能已离线）→ 停机。
        * 连续失败 >=2  系统性劣化迹象 → 停机。
        * 其它（偶发事件缺失/状态抖动） → 继续观察。
        """
        kind = (incident.get("kind") or "unknown").lower()
        consecutive = int(incident.get("consecutive_failures", 0) or 0)

        if kind in ("no_recovery", "reboot_failed"):
            return {
                "continue": False,
                "reason": (
                    "设备未恢复（疑似断电或离线），仅反复重启无法自愈，"
                    "建议停止拷机并人工排查供电/网络。"
                ),
                "source": "rule",
                "analysis": (
                    f"incident kind={kind}：恢复周期内设备始终不可达，"
                    "典型原因为断电或网络中断，非设备软件可自愈。"
                ),
            }
        if consecutive >= 2:
            return {
                "continue": False,
                "reason": f"连续失败已达 {consecutive} 次，疑似系统性劣化，建议停止。",
                "source": "rule",
                "analysis": "连续失败表明异常非偶发，继续拷机风险升高。",
            }
        return {
            "continue": True,
            "reason": "孤立异常，建议继续观察。",
            "source": "rule",
            "analysis": "单次异常可能为瞬时抖动，不构成停机条件。",
        }

    def _llm_decide(self, llm, incident: dict) -> dict | None:
        """调用 LLM 给出决策；任何异常/无法解析返回 None（交由规则引擎）。

        免费模型（如 tencent/hy3:free）未必稳定返回严格 JSON，因此解析分两级：
        先尝试 JSON 提取；失败再用自然语言启发式（继续/停止/true/false）判定。
        """
        history = self._short_history()
        system_prompt = (
            "你是门禁设备稳定性拷机系统的分析智能体。系统会给你一次事故（incident）"
            "与近期轮次摘要。请判断：是否应该继续拷机（继续重启测试）。\n"
            "尽量以 JSON 返回：{\"continue\": true 或 false, \"reason\": \"中文简述\"}。"
            "当设备疑似断电/网络中断且无法自愈时，应返回 continue=false。"
        )
        user_prompt = (
            f"事故信息：{incident}\n"
            f"近期轮次摘要（最近若干轮）：{history}\n"
            "请给出决策。"
        )
        text = llm.chat(system_prompt, user_prompt, timeout=25.0)
        if not text:
            return None

        # 1) 尝试 JSON 提取
        result = _extract_first_json(text)
        if result:
            cont = result.get("continue")
            if isinstance(cont, bool):
                return {
                    "continue": cont,
                    "reason": str(result.get("reason", "")) or "LLM 未给出说明",
                    "source": "llm",
                    "analysis": f"LLM({llm.model}) 基于事故与历史给出决策。",
                }

        # 2) 自然语言启发式：判定 continue 与否 + 提取理由
        cont = _heuristic_continue(text)
        if cont is None:
            return None
        return {
            "continue": cont,
            "reason": text.strip()[:200] or "LLM 未给出说明",
            "source": "llm",
            "analysis": f"LLM({llm.model}) 以自然语言给出决策（启发式解析）。",
        }

    # ------------------------------------------------------------------ #
    # 多角度轮次分析（rule-based 始终产出；LLM 可选）
    # ------------------------------------------------------------------ #
    def round_report(self, record: dict) -> dict:
        """产出本轮的多角度稳定性分析（规则版，始终可用）。"""
        history = list(self.ctx.history())
        total = len(history)
        passed = sum(1 for r in history if r.get("passed"))
        failed = total - passed
        recover_times = [
            r.get("recover_time")
            for r in history
            if r.get("recover_time") is not None
        ]
        avg = (sum(recover_times) / len(recover_times)) if recover_times else None
        score = round(passed / total * 100, 1) if total else 100.0
        recommendation = "继续" if failed == 0 else "关注失败趋势，必要时人工介入"
        return {
            "round": record.get("round"),
            "stability_score": score,
            "total": total,
            "passed": passed,
            "failed": failed,
            "avg_recover_time": round(avg, 1) if avg is not None else None,
            "recommendation": recommendation,
            "source": "rule",
        }

    def _short_history(self, n: int = 8) -> list:
        """取最近 n 轮的关键字段摘要，避免把整段历史塞给 LLM。"""
        recent = list(self.ctx.history(n))
        out = []
        for r in recent:
            out.append(
                {
                    "round": r.get("round"),
                    "passed": r.get("passed"),
                    "found": r.get("found"),
                    "changed": r.get("changed"),
                    "recover_time": r.get("recover_time"),
                }
            )
        return out

    # ------------------------------------------------------------------ #
    # 总线处理
    # ------------------------------------------------------------------ #
    async def _on_advise(self, message: dict) -> None:
        """处理 Coordinator 的事故决策请求：回带 {continue, reason, source}。"""
        incident = dict(message.get("incident", message))
        decision = self.decide(incident)
        # 同时广播为审计/通知事件。
        await self.publish(self.DECISION_TOPIC, {"incident": incident, **decision})
        # request/response：回带相同 req_id。
        reply = dict(decision)
        reply["req_id"] = message.get("req_id")
        reply["incident"] = incident
        await self.publish(self.ADVISE_REPLY, reply)

    async def _on_incident(self, message: dict) -> None:
        """事故广播：产出决策并广播（无需回带）。"""
        incident = dict(message.get("incident", message))
        decision = self.decide(incident)
        await self.publish(self.DECISION_TOPIC, {"incident": incident, **decision})

    async def _on_round_done(self, message: dict) -> None:
        """轮次结束：累积私有 recent_rounds + 产出多角度分析并广播。

        LLM 整体分析仅在事故或显式开启（BURNIN_ANALYST_LLM_PER_ROUND=1）时触发，
        控制免费模型的调用成本；规则分析始终产出。
        """
        # Accumulate private state (design §6.3 RiskAnalystState)
        self.recent_rounds.append(dict(message))
        report = self.round_report(dict(message))
        per_round_llm = (
            os.environ.get("BURNIN_ANALYST_LLM_PER_ROUND", "").strip().lower()
            in ("1", "true", "on")
        )
        if per_round_llm:
            llm = self._ensure_llm()
            if llm is not None:
                note = llm.chat(
                    "你是拷机分析智能体，用一句话点评本轮稳定性。",
                    f"本轮：{message}，近期：{self._short_history()}",
                    timeout=20.0,
                )
                if note:
                    report["llm_note"] = note
        # 打印到控制台，让“多角度分析”在成功轮也可见（否则总线上的报告是静默的）。
        ts = time.strftime("%H:%M:%S", time.localtime())
        avg_rt = report.get("avg_recover_time")
        avg_rt_str = f"{avg_rt:.1f}秒" if isinstance(avg_rt, (int, float)) else "NA"
        print(
            f"[{ts}] [分析] 第 {report.get('round')} 轮分析: "
            f"稳定性评分={report.get('stability_score')} "
            f"通过={report.get('passed')}/{report.get('total')} "
            f"平均恢复={avg_rt_str} "
            f"建议={report.get('recommendation')}"
        )
        if report.get("llm_note"):
            print(f"[{ts}] [分析] LLM点评: {report['llm_note']}")
        await self.publish(self.REPORT_TOPIC, report)

    # ------------------------------------------------------------------ #
    # Proactive loop (true autonomy)
    # ------------------------------------------------------------------ #
    POLL_INTERVAL = 45.0  # seconds between proactive checks

    async def _proactive_check(self) -> None:
        """Periodic check independent of incoming messages (true autonomy).

        Called every POLL_INTERVAL seconds. Proactively examines risk
        history and raises incidents if conditions warrant, even without
        new vote requests or round/done messages.

        Key proactive behaviors:
        1. Re-check consecutive high-risk streak (may have crossed threshold)
        2. Stale data alert: if no recent activity, raise warn
        """
        # Re-check consecutive high-risk streak
        if self._consecutive_high_risk >= 3:
            # Already at critical threshold, ensure incident was raised
            await self._try_raise(
                severity="critical",
                category="consecutive_high_risk",
                description=f"主动检查: 连续 {self._consecutive_high_risk} 轮高风险(>80)",
                risk_score=self.last_risk_score,
            )

    async def run(self) -> None:
        """Proactive loop + reactive subscriptions (true autonomy).

        Has its own poll loop that checks risk history every POLL_INTERVAL
        seconds, even without incoming messages. This is the core autonomy
        property — the agent doesn't just respond, it independently monitors.
        """
        self.subscribe(self.ADVISE_TOPIC, self._on_advise)
        self.subscribe(self.VOTE_REQUEST, self._on_vote_request)
        self.subscribe(self.INCIDENT, self._on_incident)
        self.subscribe(self.ROUND_DONE, self._on_round_done)
        self._stop = asyncio.Event()
        try:
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.POLL_INTERVAL
                    )
                except asyncio.TimeoutError:
                    await self._proactive_check()
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
    spec = AgentSpec("analyst", "analyst", "", cfg.user, cfg.password, cfg.host)
    agent = AnalystAgent(spec, bus, ctx, cfg=cfg)

    async def _demo():
        # 演示：模拟一次断电事故，请求决策。
        async def _on_decision(msg):
            print("[分析决策]", msg)

        bus.subscribe("analyst/decision", _on_decision)
        decision = await bus.request(
            "analyst/advise",
            {"incident": {"kind": "no_recovery", "consecutive_failures": 0}},
            timeout=30,
        )
        print("决策结果 ->", decision)

    asyncio.run(_demo())
