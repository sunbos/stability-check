# tests/test_capabilities/test_action_noop.py
"""NoopAction 单测(纯逻辑)。"""
from stability_harness_loop_multiagent.business.hikvision.capabilities.actions.noop import NoopAction


def test_noop_action_returns_ok():
    """NoopAction:返回 ok=True,不执行任何操作"""
    action = NoopAction()
    result = action.execute(ctx=None)
    assert result.ok is True
    assert result.data == {}
