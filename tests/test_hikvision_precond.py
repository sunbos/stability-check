# tests/test_hikvision_precond.py
"""前置条件就绪（串口外设类型切换自愈）+ 事件链跨轮统计 测试。"""
import pytest

from stability_harness_loop_multiagent.business.hikvision.worker import HikvisionWorker
from stability_harness_loop_multiagent.business.hikvision.adapter import HikvisionAdapter
from stability_harness_loop_multiagent.business.hikvision.event_codes import HikEventCode
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.core.agent import AgentSpec
from tests.fakes.fake_hikvision import FakeHikvisionClient


def _make_worker(client=None, required_serial_mode=None, run_reboot=False):
    bus = EventBus()
    spec = AgentSpec(id="w1", role="hik", capabilities={"act"})
    client = client or FakeHikvisionClient()
    adapter = HikvisionAdapter(client)
    worker = HikvisionWorker(
        bus, spec, adapter, client,
        time_skew_threshold=3.0,
        diagnostic=None,
        run_reboot=run_reboot,
        probe_interval=0.01,
        probe_confirm_count=2,
        warmup_time=0.0,
        max_recover_timeout=1.0,
        event_check_delay=0.0,
        required_serial_mode=required_serial_mode,
        serial_port=1,
    )
    return bus, worker, client


def test_fake_client_serial_methods():
    """fake 提供串口能力/配置 GET 与切换 PUT（返回 autoReboot）。"""
    fake = FakeHikvisionClient()
    caps = fake.get_serial_capabilities(1)
    assert "externMode" in caps["mode"]
    cfg = fake.get_serial_config(1)
    assert cfg["mode"] == "readerMode"
    res = fake.set_serial_config(1, dict(cfg, mode="externMode"))
    assert res["subStatusCode"] == "autoReboot"
    assert res["statusCode"] == 7
    assert fake._serial_set_called is True


@pytest.mark.asyncio
async def test_worker_precondition_no_mode_door_online_ok():
    """未配置 required_serial_mode 且门在线 → 前置条件满足。"""
    bus, worker, client = _make_worker()
    info = worker.pre_loop_setup()
    pre = info["precond"]
    assert pre["satisfied"] is True
    assert pre.get("serial_fixed") is False
    assert info["setup_done"] is True


@pytest.mark.asyncio
async def test_worker_precondition_no_mode_door_offline_fail():
    """未配置串口模式且门离线 → 前置条件失败（cause=precond）。"""
    client = FakeHikvisionClient()
    client.set_door_offline(True)
    bus, worker, client = _make_worker(client=client)
    info = worker.pre_loop_setup()
    pre = info["precond"]
    assert pre["satisfied"] is False
    assert pre["cause"] == "precond"
    assert worker.get_chain_stats()["precond_failed"] == 1
    assert info["setup_done"] is True  # 前置条件失败不阻断 setup 完成


@pytest.mark.asyncio
async def test_worker_precondition_fixes_serial_mode():
    """required != 当前 mode → 自动切换 + 等待重启上线 → 前置条件满足。"""
    client = FakeHikvisionClient()  # 当前 mode=readerMode
    bus, worker, client = _make_worker(
        client=client, required_serial_mode="externMode")
    info = worker.pre_loop_setup()
    pre = info["precond"]
    assert pre["satisfied"] is True
    assert pre["serial_fixed"] is True
    assert pre["new_mode"] == "externMode"
    assert worker.get_chain_stats()["precond_fixed"] == 1
    # 设备配置确实被改写
    assert client.get_serial_config(1)["mode"] == "externMode"


@pytest.mark.asyncio
async def test_worker_precondition_mode_already_ok():
    """required == 当前 mode → 不触发 PUT，前置条件直接满足。"""
    client = FakeHikvisionClient()  # 当前 mode=readerMode
    bus, worker, client = _make_worker(
        client=client, required_serial_mode="readerMode")
    info = worker.pre_loop_setup()
    pre = info["precond"]
    assert pre["satisfied"] is True
    assert pre.get("serial_fixed") is False
    assert client._serial_set_called is False


@pytest.mark.asyncio
async def test_worker_chain_stats_accumulates():
    """两轮运行后事件链统计累积正确（每轮 trigger/opened/closed 各 1）。

    先运行 pre_loop_setup 建立基线 serialNo，使每轮查询只统计上一轮之后的
    新事件（避免回看窗口混入上一轮残余事件导致计数虚高）。
    """
    bus, worker, client = _make_worker(run_reboot=False)
    worker.pre_loop_setup()
    for rnd in (1, 2):
        tick = {"round": rnd, "operation": {"op": "remote_open_door"}}
        await worker.act(tick)
    stats = worker.get_chain_stats()
    assert stats["rounds"] == 2
    assert stats["trigger"] == 2
    assert stats["opened"] == 2
    assert stats["closed"] == 2


@pytest.mark.asyncio
async def test_worker_in_loop_offline_marks_cause():
    """循环中门离线（remote_open 被拒）→ 标注为设备问题，不计入前置条件。"""
    client = FakeHikvisionClient()
    client._fail_remote_open = True
    bus, worker, client = _make_worker(client=client, run_reboot=False)
    tick = {"round": 1, "operation": {"op": "remote_open_door"}}
    await worker.act(tick)
    stats = worker.get_chain_stats()
    assert stats["in_loop_offline"] == 1
    # 前置条件未参与（未调用 pre_loop_setup），故 precond 计数保持 0。
    assert stats["precond_failed"] == 0
    facts = worker.check(tick)
    assert facts["door_offline"]["in_loop"] is True
    assert facts["door_offline"]["cause"] == "device_problem"


def test_timeline_uses_device_clock_anchor():
    """阶段时间线应使用设备时间锚（而非本机时间），且按 monotonic 偏移推算。

    本机与设备时钟存在偏差时，时间线全程须落在设备时钟下，便于与设备事件
    日志对齐排查时序。
    """
    import time as _time
    from datetime import datetime, timedelta, timezone

    bus, worker, client = _make_worker()
    # 手动建立设备时间锚（模拟 setup 开头读取到的设备时间，故意与本机偏差 21s）。
    anchor = "2026-01-01T12:00:00+08:00"
    worker._dev_ref_iso = anchor
    worker._dev_ref_mono = _time.monotonic()
    worker._t0 = _time.monotonic()
    worker._timeline = []
    worker._mark("a")
    _time.sleep(1.0)
    worker._mark("b")
    ts_a = datetime.fromisoformat(worker._timeline[0]["ts"])
    ts_b = datetime.fromisoformat(worker._timeline[1]["ts"])
    # b 比 a 晚约 1s，且整体以设备锚为基准（而非本机 now）。
    delta = (ts_b - ts_a).total_seconds()
    assert 0.8 <= delta <= 1.5
    # a 的时间应约等于设备锚（偏移极小）。
    exp_a = datetime.fromisoformat(anchor)
    assert abs((ts_a - exp_a).total_seconds()) < 0.5


def test_timeline_falls_back_to_local_when_no_anchor():
    """未建立设备时间锚时，时间线回退本机时间，不抛错。"""
    from datetime import datetime
    bus, worker, client = _make_worker()
    worker._dev_ref_iso = ""
    worker._timeline = []
    worker._mark("x")
    assert worker._timeline[0]["ts"]  # 非空字符串即可
    # 解析为合法 ISO 时间。
    datetime.fromisoformat(worker._timeline[0]["ts"])
