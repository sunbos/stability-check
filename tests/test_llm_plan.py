"""LLMPlan pydantic schema 单测(纯逻辑)"""
import pytest
from pydantic import ValidationError
from stability_harness_loop_multiagent.business.hikvision.llm_plan import LLMPlan


def test_llm_plan_default_values():
    """LLMPlan:默认值正确"""
    plan = LLMPlan()
    assert plan.skip_reboot is False
    assert plan.operations == []
    assert plan.risk_note == ""


def test_llm_plan_parse_from_dict():
    """LLMPlan:从 dict 构造,字段正确填充"""
    plan = LLMPlan(skip_reboot=True, operations=["reboot", "remote_open"],
                    risk_note="高风险")
    assert plan.skip_reboot is True
    assert plan.operations == ["reboot", "remote_open"]
    assert plan.risk_note == "高风险"


def test_llm_plan_operations_must_be_list():
    """LLMPlan:operations 必须是 list,pydantic 自动校验"""
    with pytest.raises(ValidationError):
        LLMPlan(operations="reboot")  # 字符串不是 list


def test_llm_plan_model_dump_returns_dict():
    """LLMPlan:model_dump() 返回 dict(用于 advisor._plan)"""
    plan = LLMPlan(skip_reboot=False, operations=["noop"])
    d = plan.model_dump()
    assert d == {"skip_reboot": False, "operations": ["noop"], "risk_note": ""}
