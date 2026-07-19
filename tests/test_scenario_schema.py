"""Scenario pydantic schema 单测（纯逻辑）。

TDD 红灯阶段：本测试期望失败，因为 scenario_schema.py 尚未改造为 pydantic。
Task 3.2 将实现新的 pydantic schema（ActionSpec / ProbeSpec / PreconditionSpec /
TargetCfg 等），届时本测试应当全绿。

本测试覆盖：
  - 最小合法 scenario 构造
  - probes 至少一条（空列表校验失败）
  - ActionSpec.type 必须在 Literal 白名单内
  - ProbeSpec.type 支持 field/online/count/event_chain
  - LoopCfg.deadline 必须为 HH:MM 格式
  - LoopCfg.max_rounds 必须 >= 1
  - from_yaml 能加载真实 YAML 文件
  - ${VAR} 环境变量插值
"""

import pytest
from pydantic import ValidationError

from stability_harness_loop_multiagent.business.hikvision.scenario_schema import (
    ActionSpec,
    LoopCfg,
    PreconditionSpec,
    ProbeSpec,
    Scenario,
    TargetCfg,
    from_yaml,
)


def test_scenario_minimal_valid():
    """最小合法 scenario：id + target + 1 probe + loop"""
    s = Scenario(
        id="test_001",
        name="测试",
        target=TargetCfg(host="192.168.3.33"),
        probes=[ProbeSpec(type="field", params={"field": "online", "expect_equals": 1})],
        loop=LoopCfg(max_rounds=1),
    )
    assert s.id == "test_001"
    assert s.target.host == "192.168.3.33"
    assert s.probes[0].type == "field"


def test_scenario_no_probes_fails():
    """至少一个 probe，否则校验失败"""
    with pytest.raises(ValidationError):
        Scenario(
            id="test",
            name="x",
            target=TargetCfg(host="x"),
            probes=[],
            loop=LoopCfg(max_rounds=1),
        )


def test_action_spec_invalid_type():
    """ActionSpec.type 必须在 Literal 白名单内"""
    with pytest.raises(ValidationError):
        ActionSpec(type="invalid_type", params={})


def test_probe_spec_valid_types():
    """ProbeSpec.type 支持 field/online/count/event_chain"""
    for t in ["field", "online", "count", "event_chain"]:
        ProbeSpec(type=t, params={})


def test_loop_deadline_format_validation():
    """LoopCfg.deadline 必须是 HH:MM 格式"""
    LoopCfg(deadline="23:50")  # 合法
    with pytest.raises(ValidationError):
        LoopCfg(deadline="25:99")  # 非法


def test_loop_max_rounds_must_be_positive():
    """LoopCfg.max_rounds 必须 >= 1"""
    LoopCfg(max_rounds=1)  # 合法
    with pytest.raises(ValidationError):
        LoopCfg(max_rounds=0)  # 非法


def test_from_yaml_loads_real_scenario(tmp_path):
    """from_yaml 能加载真实 YAML 文件"""
    yaml_content = """
id: Stability_0001
name: 重启测试
category: 重启稳定性
level: L2
target:
  host: 192.168.3.33
  port: 80
actions:
  - type: reboot
    params: {target: main, wait_online_timeout: 180}
probes:
  - type: field
    params:
      endpoint: /ISAPI/AccessControl/AcsWorkStatus?format=json
      field: AcsWorkStatus.doorOnlineStatus[0]
      expect_equals: 1
loop:
  max_rounds: 3
  interval_seconds: 90
"""
    yaml_file = tmp_path / "test.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")
    s = from_yaml(str(yaml_file))
    assert s.id == "Stability_0001"
    assert s.actions[0].type == "reboot"
    assert s.probes[0].params["expect_equals"] == 1


def test_env_var_interpolation(tmp_path, monkeypatch):
    """${VAR} 环境变量插值"""
    monkeypatch.setenv("TEST_HOST", "10.0.0.1")
    yaml_content = """
id: test
name: x
target:
  host: ${TEST_HOST}
probes:
  - type: field
    params: {field: x, expect_equals: 1}
loop:
  max_rounds: 1
"""
    yaml_file = tmp_path / "env.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")
    s = from_yaml(str(yaml_file))
    assert s.target.host == "10.0.0.1"
