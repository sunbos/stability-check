# tests/test_capabilities/test_action_sleep.py
"""SleepAction 单测(纯逻辑,不真 sleep,只验证时间计算)。"""
import time
from stability_harness_loop_multiagent.business.hikvision.capabilities.actions.sleep import SleepAction


def test_sleep_action_returns_ok():
    """SleepAction:返回 ok=True"""
    action = SleepAction(seconds=0.01)
    result = action.execute(ctx=None)
    assert result.ok is True


def test_sleep_action_actually_sleeps():
    """SleepAction:实际 sleep 指定秒数"""
    action = SleepAction(seconds=0.05)
    start = time.time()
    action.execute(ctx=None)
    elapsed = time.time() - start
    assert elapsed >= 0.04  # 允许微小误差
