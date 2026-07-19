# tests/test_capabilities/test_probe_field.py
"""FieldProbe 单测(纯逻辑,验证字段断言)。"""
from stability_harness_loop_multiagent.business.hikvision.capabilities.probes.field import FieldProbe


def test_field_probe_equals_pass():
    """FieldProbe:字段值等于期望值 → probe_ok=True"""
    probe = FieldProbe(field="AcsWorkStatus.doorOnlineStatus[0]", expect_equals=1)
    snapshot = {"AcsWorkStatus": {"doorOnlineStatus": [1]}}
    fact = probe.check(snapshot)
    assert fact["probe_ok"] is True


def test_field_probe_equals_fail():
    """FieldProbe:字段值不等于期望值 → probe_ok=False"""
    probe = FieldProbe(field="AcsWorkStatus.doorOnlineStatus[0]", expect_equals=1)
    snapshot = {"AcsWorkStatus": {"doorOnlineStatus": [0]}}
    fact = probe.check(snapshot)
    assert fact["probe_ok"] is False


def test_field_probe_na_if_absent():
    """FieldProbe:na_if_absent 字段缺失 → probe_na=True"""
    probe = FieldProbe(field="online", expect_equals=1, na_if_absent="netWorkStatus")
    snapshot = {"AcsWorkStatus": {}}  # 缺 netWorkStatus
    fact = probe.check(snapshot)
    assert fact.get("probe_na") is True


def test_field_probe_field_missing_no_na():
    """FieldProbe:字段缺失但无 na_if_absent → probe_ok=False"""
    probe = FieldProbe(field="missing.field", expect_equals=1)
    snapshot = {}
    fact = probe.check(snapshot)
    assert fact["probe_ok"] is False
