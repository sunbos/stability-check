"""场景化稳定性层的纯逻辑测试（schema 校验 / 字段解析 / 断言比较）。

端到端用例（事实独裁裁决 / NA 不失败 / 截止时间早停 / 重启失败导致离线即失败）
需要真实设备，走真机集成路径，不在此文件覆盖。
"""

import pytest

from stability_harness_loop_multiagent.business.hikvision.scenario_schema import (
    Scenario,
    compare_probe,
    from_dict,
    from_yaml,
    resolve_field,
)


def _base_dict(**overrides):
    d = {
        "id": "T1", "name": "测试用例",
        "target": {"host": "192.168.3.33"},
        "stress": {"type": "none"},
        "probe": {
            "endpoint": "/ISAPI/AccessControl/AcsWorkStatus?format=json",
            "field": "AcsWorkStatus.doorOnlineStatus[0]",
            "expect_equals": 1,
        },
        "loop": {"max_rounds": 3, "interval_seconds": 0, "stop_on_na": False},
    }
    d.update(overrides)
    return d


# ---- schema / 字段解析 / 比较（同步） -----------------------------------
def test_schema_basic_valid():
    sc = from_dict(_base_dict())
    assert isinstance(sc, Scenario)
    assert sc.stress.type == "none"
    assert sc.probe.expect_equals == 1


def test_schema_invalid_stress_type():
    with pytest.raises(ValueError):
        from_dict(_base_dict(stress={"type": "explode"}))


def test_schema_requires_expect():
    with pytest.raises(ValueError):
        from_dict(_base_dict(probe={"endpoint": "/x", "field": "a.b"}))


def test_schema_env_interpolation(tmp_path, monkeypatch):
    monkeypatch.setenv("HIK_PASSWORD", "secret123")
    p = tmp_path / "s.yaml"
    p.write_text(
        "id: E1\nname: e\n"
        "target:\n  host: h\n  password: \"${HIK_PASSWORD}\"\n"
        "stress:\n  type: none\n"
        "probe:\n  endpoint: /x\n  field: a.b\n  expect_equals: 1\n"
        "loop:\n  max_rounds: 1\n",
        encoding="utf-8",
    )
    sc = from_yaml(str(p))
    assert sc.target.password == "secret123"


def test_schema_env_default():
    sc = from_dict(_base_dict(target={"host": "h",
                                      "password": "${MISSING_VAR:-fallback}"}))
    assert sc.target.password == "fallback"


def test_resolve_field_nested_and_list():
    snap = {"AcsWorkStatus": {"doorOnlineStatus": [2, 1]}}
    assert resolve_field(snap, "AcsWorkStatus.doorOnlineStatus[1]") == 1
    assert resolve_field(snap, "AcsWorkStatus.doorOnlineStatus[0]") == 2


def test_resolve_field_absent_returns_sentinel():
    from stability_harness_loop_multiagent.business.hikvision.scenario_schema import (
        _SENTINEL,
    )
    snap = {"AcsWorkStatus": {}}
    assert resolve_field(snap, "AcsWorkStatus.doorOnlineStatus[0]",
                         default=_SENTINEL) is _SENTINEL
    assert resolve_field({}, "missing", default=_SENTINEL) is _SENTINEL


def test_compare_probe_equals_in_coerce():
    class _P:
        expect_equals = 1
        expect_in = None
    assert compare_probe("1", _P()) is True          # 字符串数字
    assert compare_probe(1, _P()) is True
    assert compare_probe(2, _P()) is False

    class _P2:
        expect_equals = None
        expect_in = ["online", "connect"]
    assert compare_probe("connect", _P2()) is True
    assert compare_probe("offline", _P2()) is False
