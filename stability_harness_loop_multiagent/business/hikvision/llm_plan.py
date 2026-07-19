"""LLMPlan —— Advisor 的 LLM 解析计划 schema(pydantic 强类型)。

用于 openai SDK 的 structured output:client.beta.chat.completions.parse(
    response_format=LLMPlan
) 自动校验 LLM 返回,无需手写 JSON 抽取。
"""
from pydantic import BaseModel, Field


class LLMPlan(BaseModel):
    """LLM 解析出的稳定性测试计划。"""
    skip_reboot: bool = Field(
        default=False, description="本轮是否跳过重启(若指令不要求重启则为 true)"
    )
    operations: list[str] = Field(
        default_factory=list,
        description='操作列表,可选值 "reboot" / "remote_open" / "query_events" / "noop"'
    )
    risk_note: str = Field(
        default="", description="风险备注,无则空字符串"
    )
