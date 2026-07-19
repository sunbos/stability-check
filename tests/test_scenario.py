"""场景化稳定性层的测试（纯合成适配器，不连真实设备）。

覆盖：schema 校验 / 字段解析 / 断言比较 / 事实独裁裁决 / NA 不失败 /
截止时间早停 / 重启失败导致离线即失败。全部沿用框架测试不变量
（MemorySink 断言、合成适配器、极短超时）。
"""

import os

import pytest

from stability_harness_loop_multiagent.business.hikvision.scenario_adapter import (
    FakeScenarioAdapter,
)
from stability_harness_loop_multiagent.business.hikvision.scenario_runner import (
    run_scenario,
)
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


# ---- 端到端（合成适配器） -----------------------------------------------
@pytest.mark.asyncio
async def test_scenario_reboot_all_pass():
    sc = from_dict(_base_dict(stress={"type": "reboot", "reboot_after": True}))
    adapter = FakeScenarioAdapter(sc)  # 默认在线快照 -> 全部通过
    res = await run_scenario(sc, adapter=adapter, run_timeout=30)
    s = res["summary"]
    assert s["rounds"] == 3
    assert s["pass"] == 3
    assert s["fail"] == 0
    assert s["na"] == 0
    assert s["stop_reason"] is None   # 正常完成（未因 NA/截止早停）
    assert adapter.stress_calls == 3


@pytest.mark.asyncio
async def test_scenario_probe_mismatch_fails():
    sc = from_dict(_base_dict())
    probe_values = [{"AcsWorkStatus": {"doorOnlineStatus": [2]}}]  # 离线 != 1
    adapter = FakeScenarioAdapter(sc, probe_values=probe_values)
    res = await run_scenario(sc, adapter=adapter, run_timeout=30)
    s = res["summary"]
    assert s["fail"] == 3            # 事实独裁：断言失败 -> fail 裁决
    assert s["pass"] == 0
    assert s["verdicts"].get("fail", 0) == 3


@pytest.mark.asyncio
async def test_scenario_na_not_fail_and_stop_on_na():
    sc = from_dict(_base_dict(
        probe={"endpoint": "/x", "field": "AcsWorkStatus.doorOnlineStatus[0]",
               "expect_equals": 1,
               "na_if_absent": "AcsWorkStatus.doorOnlineStatus"},
        loop={"max_rounds": 100, "interval_seconds": 0, "stop_on_na": True},
    ))
    # 空快照 -> 存在性字段缺失 -> NA
    adapter = FakeScenarioAdapter(sc, probe_values=[{}])
    res = await run_scenario(sc, adapter=adapter, run_timeout=30)
    s = res["summary"]
    assert s["na"] == 1
    assert s["fail"] == 0
    assert s["aborted"] is True
    assert "NA" in (s["abort_reason"] or "")


@pytest.mark.asyncio
async def test_scenario_na_continues_when_stop_on_na_false():
    sc = from_dict(_base_dict(
        stress={"type": "none"},
        probe={"endpoint": "/x", "field": "AcsWorkStatus.doorOnlineStatus[0]",
               "expect_equals": 1,
               "na_if_absent": "AcsWorkStatus.doorOnlineStatus"},
        loop={"max_rounds": 3, "interval_seconds": 0, "stop_on_na": False},
    ))
    adapter = FakeScenarioAdapter(sc, probe_values=[{}, {}, {}])
    res = await run_scenario(sc, adapter=adapter, run_timeout=30)
    s = res["summary"]
    assert s["na"] == 3
    assert s["fail"] == 0
    assert s["stop_reason"] is None   # NA 但未要求早停 -> 正常跑完


@pytest.mark.asyncio
async def test_scenario_deadline_early_stop_nt():
    sc = from_dict(_base_dict(loop={"max_rounds": 100, "interval_seconds": 0,
                                    "deadline": "00:00"}))  # 必已过去
    adapter = FakeScenarioAdapter(sc)
    res = await run_scenario(sc, adapter=adapter, run_timeout=30)
    s = res["summary"]
    assert s["aborted"] is True
    assert "deadline" in (s["abort_reason"] or "")
    # 截止时间早停在施加压力前，不应有任何 stress / observe 调用。
    assert adapter.stress_calls == 0


@pytest.mark.asyncio
async def test_scenario_reboot_failure_offline_fails():
    sc = from_dict(_base_dict(
        stress={"type": "reboot", "reboot_after": True},
        loop={"max_rounds": 1, "interval_seconds": 0, "stop_on_na": False},
    ))
    # 重启失败 + 探测为空（设备离线）-> 字段缺失 -> 断言失败 -> fail
    adapter = FakeScenarioAdapter(sc, probe_values=[{}], fail_stress=True)
    res = await run_scenario(sc, adapter=adapter, run_timeout=30)
    s = res["summary"]
    assert s["stress_fail"] == 1
    assert s["fail"] == 1
