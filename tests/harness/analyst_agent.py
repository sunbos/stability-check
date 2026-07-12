"""AnalystAgent：策略层分析智能体（仅使用标准库）。

定位（关键设计约束）
--------------------
* 这是**策略层 / 政策层**智能体，**绝不参与每一轮的 pass/fail 判定**。
  每一轮的 pass/fail 由确定性的 Coordinator（Loop Core）负责，保证核心逻辑
  可复现、低维护。
* Analyst 只在**事故（incident）**时给出“是否继续拷机”的政策性决策；当人不在场
  时，它能自主处理突发情况（例如重启过程中设备断电 → 不再恢复），决定是否停机。
* **优雅降级**：无 LLM key（或 LLM 调用失败/超时）时，自动回退到规则引擎
  （rule-based）。规则引擎覆盖“断电/超时”“连续失败”等多种场景，因此即便没有
  网络/密钥，整套拷机依旧可靠运行。
* LLM 来自 OpenRouter（默认 `tencent/hy3:free`），key **只**来自环境变量/`.env`，
  绝不被打印或硬编码。

通信（只走总线）
--------------
* 订阅 `analyst/advise`（request/response）：Coordinator 在事故时请求决策；
  Analyst 在 `analyst/advise/reply` 回带相同 req_id 的 {continue, reason, source}。
* 订阅 `incident/raise`（publish）：记录事故并广播 `analyst/decision`（供 Scribe /
  Notifier 审计与通知）。
* 订阅 `round/done`（publish）：每次轮次产出多角度 `analyst/report`（稳定性评分、
  失败趋势、恢复耗时），LLM 仅在事故或显式开启时做整体分析，控制调用成本。

仅依赖标准库 + 同仓 bus / agent / context / llm_client，无第三方依赖。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

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
    """策略层分析智能体：事故决策 + 多角度分析，确定性核心之外的政策大脑。"""

    ADVISE_TOPIC = "analyst/advise"
    ADVISE_REPLY = "analyst/advise/reply"
    REPORT_TOPIC = "analyst/report"
    DECISION_TOPIC = "analyst/decision"
    ROUND_DONE = "round/done"
    INCIDENT = "incident/raise"

    def __init__(self, spec, bus, ctx, cfg=None) -> None:
        super().__init__(spec, bus, ctx)
        self.cfg = cfg if cfg is not None else getattr(ctx, "cfg", None)
        # 是否启用 LLM（默认启用；env BURNIN_ANALYST=off 可关闭纯规则运行）。
        self.enabled = self._read_enabled()
        # LLM 客户端懒加载（避免无 key 时也 import / 触发副作用）。
        self._llm = None
        self._llm_loaded = False

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
        """返回 OpenRouterClient 或 None（无 key / 未启用 → 规则降级）。"""
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
        history = self.ctx.round_history
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
        recent = self.ctx.round_history[-n:]
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
        """轮次结束：产出多角度分析并广播。

        LLM 整体分析仅在事故或显式开启（BURNIN_ANALYST_LLM_PER_ROUND=1）时触发，
        控制免费模型的调用成本；规则分析始终产出。
        """
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
        print(
            f"[analyst] 第 {report.get('round')} 轮分析: "
            f"稳定性评分={report.get('stability_score')} "
            f"通过={report.get('passed')}/{report.get('total')} "
            f"平均恢复={report.get('avg_recover_time')}s "
            f"建议={report.get('recommendation')}"
        )
        if report.get("llm_note"):
            print(f"[analyst] LLM点评: {report['llm_note']}")
        await self.publish(self.REPORT_TOPIC, report)

    async def run(self) -> None:
        """独立主循环：订阅 advise / incident / round/done，直到被取消。"""
        self.subscribe(self.ADVISE_TOPIC, self._on_advise)
        self.subscribe(self.INCIDENT, self._on_incident)
        self.subscribe(self.ROUND_DONE, self._on_round_done)
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
    spec = AgentSpec("analyst", "analyst", "", cfg.user, cfg.password, cfg.host)
    agent = AnalystAgent(spec, bus, ctx, cfg=cfg)

    async def _demo():
        # 演示：模拟一次断电事故，请求决策。
        async def _on_decision(msg):
            print("[analyst/decision]", msg)

        bus.subscribe("analyst/decision", _on_decision)
        decision = await bus.request(
            "analyst/advise",
            {"incident": {"kind": "no_recovery", "consecutive_failures": 0}},
            timeout=30,
        )
        print("advise ->", decision)

    asyncio.run(_demo())
