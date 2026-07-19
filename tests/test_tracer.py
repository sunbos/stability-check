"""EngineBusTracer 的单元测试：三引擎活动追踪器。

验证：话题 -> 引擎归类、不带 round 的事件归属、分栏面板、跨轮聚合、导出。
"""

import asyncio
import contextlib
import io
import json

import pytest

from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.tracer import (
    EngineBusTracer,
    engine_of,
)


@pytest.mark.asyncio
async def test_engine_of_classifies_by_prefix():
    assert engine_of("loop/tick") == "Loop"
    assert engine_of("agent/vote/reply") == "MAS"
    assert engine_of("hikvision/plan") == "MAS"
    assert engine_of("target/recovered") == "MAS"
    assert engine_of("harness/abort") == "Harness"
    assert engine_of("unknown/x") == "Other"


async def _drain() -> None:
    # 让总线异步调度 handler（create_task）有机会执行。
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_collect_classify_and_panel():
    bus = EventBus()
    tr = EngineBusTracer(bus)
    bus.publish("loop/tick", {"round": 1})
    bus.publish("agent/vote/reply",
                {"role": "risk", "risk": 30, "confidence": 0.9, "round": 1})
    bus.publish("target/recovered", {"recovered": True})  # 不带 round
    bus.publish("harness/liveness/heartbeat", {"idle": 0.1, "ts": 1.0})
    bus.publish("loop/done", {"round": 1, "verdict": "pass", "risk": 30,
                              "recover_time": 12.3})
    await _drain()

    counts = tr.per_engine_counts()
    assert counts["Loop"] >= 2
    assert counts["MAS"] >= 2
    assert counts["Harness"] >= 1

    # 不带 round 的 target/recovered 应归属 current_round=1（由 loop/tick 推进）。
    topics = {e.topic for e in tr.events_for_round(1)}
    assert "target/recovered" in topics

    panel = tr.panel_for_round(1)
    assert "[Loop]" in panel and "[MAS]" in panel and "[Harness]" in panel
    assert "裁决 pass" in panel
    tr.detach()


@pytest.mark.asyncio
async def test_report_aggregation_across_rounds():
    bus = EventBus()
    tr = EngineBusTracer(bus)
    for r in (1, 2, 3):
        bus.publish("loop/tick", {"round": r})
        bus.publish("agent/vote/reply",
                    {"role": "risk", "risk": 30, "confidence": 0.9, "round": r})
        bus.publish("loop/done", {"round": r, "verdict": "pass", "risk": 30})
        bus.publish("harness/liveness/heartbeat", {"idle": 0.0, "ts": 1.0})
    await _drain()

    rep = tr.report()
    assert rep["total_events"] >= 12
    votes = rep["votes"]
    assert len(votes) == 1 and votes[0]["role"] == "risk"
    assert votes[0]["rounds"] == 3
    assert abs(votes[0]["avg_risk"] - 30.0) < 1e-6
    assert abs(votes[0]["avg_conf"] - 0.9) < 1e-6
    assert rep["watchdog"]["heartbeats"] == 3
    assert rep["watchdog"]["aborts"] == 0
    # 未挂载治理网关时应为 None。
    assert rep["governance"] is None
    tr.detach()


@pytest.mark.asyncio
async def test_export_json_and_html(tmp_path):
    bus = EventBus()
    tr = EngineBusTracer(bus)
    bus.publish("loop/tick", {"round": 1})
    bus.publish("loop/done", {"round": 1, "verdict": "pass", "risk": 30})
    bus.publish("harness/liveness/heartbeat", {"idle": 0.0, "ts": 1.0})
    await _drain()

    jp = tmp_path / "rep.json"
    tr.export_json(str(jp))
    assert jp.exists()
    data = json.loads(jp.read_text(encoding="utf-8"))
    assert data["total_events"] >= 3
    assert "per_engine_counts" in data

    hp = tmp_path / "rep.html"
    tr.export_html(str(hp))
    html = hp.read_text(encoding="utf-8")
    assert "三引擎架构观测报告" in html
    assert "[Loop]" in html
    tr.detach()


@pytest.mark.asyncio
async def test_setup_panel_groups_round_zero_and_is_idempotent(tmp_path):
    bus = EventBus()
    tr = EngineBusTracer(bus)
    # 启动/校验阶段（首轮 loop/tick 之前）的网关活动，不带 round 字段，
    # 应归属 current_round=0，而非进入任意 Round N 面板。
    bus.publish("harness/verify/request", {"op": "plan_check"})
    bus.publish("harness/verify/response", {"allowed": False})
    bus.publish("hikvision/plan", {"text": "reboot then probe"})
    await _drain()

    # panel_for_round(0) 渲染为「启动/校验」独立面板，且强制展示三引擎栏：
    # 即便某引擎在启动阶段无活动，也要显式可见（避免遗漏架构活动）。
    panel = tr.panel_for_round(0)
    assert "启动/校验" in panel
    assert "[Loop]" in panel and "（无活动）" in panel  # 启动阶段 Loop 未发事件
    assert "[MAS]" in panel and "Advisor 发布计划" in panel
    assert "[Harness]" in panel and "校验响应 allowed=False" in panel

    # 幂等：首次打印有内容，二次因 _setup_printed 标志不再输出。
    buf1, buf2 = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(buf1):
        tr.print_setup_panel()
    with contextlib.redirect_stdout(buf2):
        tr.print_setup_panel()
    assert buf1.getvalue().strip() != ""
    assert buf2.getvalue().strip() == ""

    # HTML 导出应包含启动/校验面板（此前 round=0 被 `if e.round` 当 falsy 排除）。
    hp = tmp_path / "rep_setup.html"
    tr.export_html(str(hp))
    html = hp.read_text(encoding="utf-8")
    assert "启动/校验" in html
    tr.detach()
