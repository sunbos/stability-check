"""HikvisionAdvisor：解析 BURNIN_STRATEGY -> 计划，并发布 hikvision/plan。

LLM 解析优先用模块级 ``chat_json`` + ``LLMPlan``（openai structured output）；
也支持以可调用对象形式注入 ``llm_parse``（便于确定性测试）。无 LLM 密钥时
回退到 ``_rule_based_parse`` 规则兜底。Advisor 只投票 / 上报事件；
绝不执行操作，也不决定裁决。

可选校验闸门（opt-in）：当 ``enable_verify=True`` 时，解析出的计划在采纳前
经 ``harness/verify/request`` 做 fail-closed 护栏；被拒/超时则丢弃该计划
（不发布 hikvision/plan），由规则兜底接管。纯总线契约，不 import 校验实现。
"""

import asyncio
import logging
from typing import Callable, Dict, Optional

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from ...multi_agent.advisors.base import AdvisorAgent
from .llm import chat_json, get_client
from .llm_plan import LLMPlan


ADVISOR_SYSTEM_PROMPT = """你是海康门禁稳定性测试的计划解析器。
输入：用户的自然语言测试指令（如"重启3次后检查在线"）。
输出：严格遵守 LLMPlan schema 的 JSON。
- skip_reboot：是否跳过重启（若指令不要求重启则为 true）
- operations：操作列表，可选值 "reboot" / "remote_open" / "query_events" / "noop"
- risk_note：风险备注，无则空字符串
不要输出任何其他内容。"""


class HikvisionAdvisor(AdvisorAgent):
    def __init__(self, bus: EventBus, spec: AgentSpec,
                 instruction: str,
                 llm_parse: Optional[Callable[[str], Dict]] = None,
                 *, weight: float = 1.0,
                 enable_verify: bool = False,
                 verify_timeout: float = 1.0) -> None:
        super().__init__(bus, spec, weight=weight)
        self._instruction = instruction
        self._llm_parse = llm_parse
        self._plan: Dict = {}
        # 校验闸门（opt-in）：默认关闭。开启后在采纳计划前发 harness/verify/request。
        self._enable_verify = enable_verify
        self._verify_timeout = verify_timeout
        self._log = logging.getLogger("stability_harness_loop_multiagent.business.hikvision.advisor")

    async def start(self) -> None:
        await super().start()
        # 启动时解析指令并发布一次计划。LLM 解析是阻塞型 HTTP 调用，用 to_thread
        # 移出事件循环，避免冻结整个 asyncio 调度（含看门狗/总线）。
        self._log.info("解析指令为计划（大模型）：开始")
        self._plan = await self._resolve_plan(self._instruction)
        if self._enable_verify:
            self._log.info("调用大模型护栏校验计划（可能耗时数十秒）：开始")
            allowed, reason = await self._verify_plan(self._plan)
            if not allowed:
                # fail-closed：校验拒绝/超时时丢弃计划，由规则兜底接管。
                # 仅打一行简洁、归属清晰的日志（含原因），不转储整份 plan dict。
                self._log.warning(
                    "大模型校验闸门拒绝计划（reason=%s），回退规则兜底（fail-closed），"
                    "不再发布 hikvision/plan", reason
                )
                self._plan = {}
                return
            self._log.info("大模型校验闸门通过，采纳 LLM 计划并发布 hikvision/plan")
        self.publish("hikvision/plan", self._plan)
        # 总线 publish 是 fire-and-forget（create_task），需让出一次事件循环，
        # 让订阅者 handler 有机会被调度，否则在 to_thread 提前让出导致 run-loop
        # task 提前完成后，stop 的 await 不再让出会使消息滞留。
        await asyncio.sleep(0)

    async def _resolve_plan(self, instruction: str) -> Dict:
        """解析指令为计划 dict。

        优先级：注入的 ``llm_parse``（测试用）> 模块级 ``chat_json`` + ``LLMPlan``
        > ``_rule_based_parse`` 规则兜底（无 LLM 密钥时）。
        """
        if self._llm_parse is not None:
            return await asyncio.to_thread(self._llm_parse, instruction)
        client = get_client()
        if client is not None:
            plan = await asyncio.to_thread(
                chat_json, client, ADVISOR_SYSTEM_PROMPT, instruction, LLMPlan
            )
            if plan:
                return plan
        return self._rule_based_parse(instruction)

    def _rule_based_parse(self, instruction: str) -> Dict:
        """无 LLM 时的规则兜底（关键词匹配）。"""
        plan: Dict = {"skip_reboot": False, "operations": [], "risk_note": ""}
        if "重启" in instruction or "reboot" in instruction.lower():
            plan["operations"].append("reboot")
        if "开门" in instruction or "open" in instruction.lower():
            plan["operations"].append("remote_open")
        if not plan["operations"]:
            plan["operations"] = ["noop"]
        return plan

    async def _verify_plan(self, plan: Dict) -> tuple:
        """向 harness/verify/request 发起校验，返回 (是否放行, 原因)。fail-closed。"""
        req = {"item": plan, "kind": "plan"}
        try:
            reply = await self.request(
                "harness/verify/request", req, timeout=self._verify_timeout
            )
        except Exception:  # noqa: BLE001 - 总线超时/错误一律视为拒绝
            return False, "verify-request-timeout-or-error"
        if not isinstance(reply, dict):
            return False, "no-reply"
        allowed = bool(reply.get("allowed", False))
        reason = str(reply.get("reason", "")) if not allowed else ""
        return allowed, reason

    def on_round(self, round_info: dict) -> None:
        super().on_round(round_info)
        # 在私有窗口中跟踪风险趋势（继承自基类）

    def vote(self) -> tuple:
        # 简单趋势：若近期有任一轮失败，则抬高风险
        window = self._private_window
        if window and any(isinstance(r, (int, float)) and r >= 60 for r in window[-10:]):
            return (75.0, 0.8)
        return (30.0, 0.7)


__all__ = ["HikvisionAdvisor"]
