# tests/test_hikvision_adapter.py
from stability_harness_loop_multiagent.business.hikvision.adapter import HikvisionAdapter
from stability_harness_loop_multiagent.business.hikvision.event_codes import HikEventCode
from stability_harness_loop_multiagent.multi_agent.adapter import Result, State
from tests.fakes.fake_hikvision import FakeHikvisionClient


def test_adapter_act_remote_open_returns_ok_result():
    client = FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    res = adapter.act({"op": "remote_open_door", "door_no": 1})
    assert isinstance(res, Result)
    assert res.ok is True


def test_adapter_observe_returns_state_with_work_status():
    client = FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    st = adapter.observe()
    assert isinstance(st, State)
    assert "AcsWorkStatus" in st.snapshot


def test_adapter_events_returns_list_type():
    # Adapter.events() is a generic surface returning []; Worker uses
    # client.query_events directly for ISO-window cross-major queries.
    client = FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    client.remote_open_door(1)
    evs = adapter.events(0.0)
    assert isinstance(evs, list)


def test_adapter_act_unknown_op_returns_error():
    client = FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    res = adapter.act({"op": "unknown_op"})
    assert res.ok is False
    assert "unknown op" in res.error


def test_adapter_act_reboot_returns_ok():
    client = FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    res = adapter.act({"op": "reboot"})
    assert res.ok is True
