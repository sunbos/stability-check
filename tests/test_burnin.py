"""稳定性拷机会话测试（总线驱动的多智能体团队）。

整轮拷机作为单个会话运行（不使用 parametrize）。仅使用标准库 asyncio / os / sys。

由 harness/loader.build_system 装配 EventBus / RunContext / 全部 Agent（含
Coordinator / Analyst / Scribe / Notifier），用 conftest 的 baseline 填充
RunContext.baseline，启动所有 agent 的 run() 协程，以 Coordinator.run 为主驱动；
结束后打印 Reporter 汇总、Scribe 叙事与 Analyst 报告，并断言未被失败阈值中止。

另含两条不依赖真实设备的策略层测试：
  - test_analyst_rulebased_degradation：无 LLM key 时规则引擎对“断电”事故给出停机决策。
  - test_coordinator_consults_analyst_on_no_recovery：模拟设备未恢复，验证 Coordinator
    请求 analyst/advise 并尊重“停止”决策而中止（含确定性降级：无 Analyst 时照常记失败）。
"""

import os
import sys
import asyncio

import pytest

# 让 tests 顶层模块（agents / harness）可被绝对导入。
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_AGENTS_DIR = os.path.join(_THIS_DIR, "agents")
_HARNESS_DIR = os.path.join(_THIS_DIR, "harness")
for _p in (_AGENTS_DIR, _HARNESS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from loader import build_system  # noqa: E402
from coordinator import Coordinator  # noqa: E402
from reporter_agent import ReporterAgent  # noqa: E402
from analyst_agent import AnalystAgent  # noqa: E402
from scribe_agent import ScribeAgent  # noqa: E402
from notifier_agent import NotifierAgent  # noqa: E402


def test_burnin_session(run_config, baseline):
    """执行一轮完整拷机会话并断言未被失败阈值中止。"""
    bus, ctx, agents = build_system(run_config)

    # 用 conftest 抓到的基线填充共享上下文（Baseline 实例，含 fields / status）。
    ctx.set_baseline(baseline)
    ctx.baseline_fields = getattr(baseline, "fields", None)

    coordinator = next(a for a in agents if isinstance(a, Coordinator))
    reporter = next(a for a in agents if isinstance(a, ReporterAgent))
    scribe = next(a for a in agents if isinstance(a, ScribeAgent))

    async def _drive() -> None:
        # 其余 agent 先启动并注册订阅；稍作让出确保订阅到位。
        others = [a for a in agents if a is not coordinator]
        tasks = [asyncio.create_task(a.run()) for a in others]
        await asyncio.sleep(0.05)  # 让各 agent 完成订阅注册

        # Coordinator 为主驱动。
        coord_task = asyncio.create_task(coordinator.run())
        await coord_task

        # 协调者结束后取消其余 agent 并清理。
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(_drive())

    summary = reporter.reporter.summary()
    print("Burn-in summary:", summary)
    print("Scribe narrative:", scribe.summary().get("narrative"))

    assert not ctx.aborted, (
        f"Burn-in aborted by failure threshold: {summary}"
    )


# --------------------------------------------------------------------------- #
# 策略层（Analyst / Coordinator 事故路径）测试：不依赖真实设备
# --------------------------------------------------------------------------- #
def test_analyst_rulebased_degradation():
    """无 LLM key 时，规则引擎对“断电/未恢复”事故给出 continue=False 停机决策。

    验证 graceful degradation：即便没有 OpenRouter key，Analyst 也能可靠决策，
    整套拷机不会因缺 LLM 而失能。
    """
    from bus import EventBus
    from context import RunContext
    from agent import AgentSpec

    # 确保无 key：临时清掉环境变量（仅本测试作用域内）。
    saved = os.environ.pop("OPENROUTER_API_KEY", None)
    try:
        bus = EventBus()
        ctx = RunContext()
        spec = AgentSpec("analyst", "analyst", "", "admin", "x", "192.168.3.33")
        agent = AnalystAgent(spec, bus, ctx)

        decision = agent.decide({"kind": "no_recovery", "consecutive_failures": 0})
        print("rule-based decision:", decision)
        assert decision["continue"] is False
        assert decision["source"] == "rule"
    finally:
        if saved is not None:
            os.environ["OPENROUTER_API_KEY"] = saved


def test_coordinator_consults_analyst_on_no_recovery():
    """模拟设备未恢复（疑似断电）：Coordinator 请求 analyst/advise，尊重“停止”决策而中止。

    用一个 fake Analyst 在总线上回应 analyst/advise（continue=False），
    验证 Coordinator 走 _handle_no_recovery 并中止；同时验证“无 Analyst”时
    确定性降级（照常记失败，不卡死）。
    """
    from bus import EventBus
    from context import RunContext
    from agent import AgentSpec
    from config import RunConfig

    # ---- 场景 A：Analyst 在线，决策“停止” ----
    cfg = RunConfig(
        host="192.168.3.33", user="admin", password="x",
        max_rounds=0, recover_timeout=5, fail_threshold=5, fail_consecutive=3,
    )
    bus = EventBus()
    ctx = RunContext()
    ctx.cfg = cfg

    coord_spec = AgentSpec("coordinator", "coordinator", "", "admin", "x", "192.168.3.33")
    coordinator = Coordinator(coord_spec, bus, ctx, cfg=cfg)

    async def _fake_analyst(msg: dict) -> None:
        reply = {
            "continue": False,
            "reason": "rule-based power loss",
            "source": "rule",
            "req_id": msg.get("req_id"),
            "incident": msg.get("incident"),
        }
        await bus.publish("analyst/advise/reply", reply)

    bus.subscribe("analyst/advise", _fake_analyst)

    async def _scenario() -> None:
        coordinator._start_time = 0.0
        coordinator._round_done_event = asyncio.Event()
        coordinator._aborted = False
        coordinator.ctx.aborted = False
        coordinator.round_no = 1  # 模拟 run() 主循环已推进到本轮
        coordinator._round["round_no"] = 1
        coordinator.subscribe("reboot/done", coordinator._on_reboot_done)
        coordinator.subscribe("device/recovered", coordinator._on_recovered)
        coordinator.subscribe("check/event", coordinator._on_event)
        coordinator.subscribe("check/status", coordinator._on_status)
        coordinator.subscribe("coord/abort", coordinator._on_abort)

        # 模拟一轮：重启成功 → 未恢复（断电）→ 事件/状态核对到齐。
        await bus.publish("reboot/done", {"round_no": 1, "t_reboot": 100.0, "ok": True})
        await bus.publish("device/recovered", {"round_no": 1, "t_reboot": 100.0, "t_recover": None})
        await bus.publish("check/event", {"round_no": 1, "found": False, "error": None})
        await bus.publish("check/status", {"round_no": 1, "changed": False, "diff": {}, "error": None})

        # 等待本轮收尾（_evaluate 走 _handle_no_recovery → analyst 停止 → abort）。
        await asyncio.wait_for(coordinator._round_done_event.wait(), timeout=10)
        return coordinator.ctx.aborted

    aborted = asyncio.run(_scenario())
    assert aborted is True, "Coordinator should abort when Analyst decides stop on power loss"

    # ---- 场景 B：无 Analyst 在线 → 确定性降级，照常记失败且不卡死 ----
    bus2 = EventBus()
    ctx2 = RunContext()
    ctx2.cfg = cfg
    coordinator2 = Coordinator(coord_spec, bus2, ctx2, cfg=cfg)

    async def _scenario_no_analyst() -> dict:
        coordinator2._start_time = 0.0
        coordinator2._round_done_event = asyncio.Event()
        coordinator2._aborted = False
        coordinator2.ctx.aborted = False
        coordinator2.round_no = 1  # 模拟 run() 主循环已推进到本轮
        coordinator2._round["round_no"] = 1
        coordinator2.subscribe("reboot/done", coordinator2._on_reboot_done)
        coordinator2.subscribe("device/recovered", coordinator2._on_recovered)
        coordinator2.subscribe("check/event", coordinator2._on_event)
        coordinator2.subscribe("check/status", coordinator2._on_status)
        coordinator2.subscribe("coord/abort", coordinator2._on_abort)

        await bus2.publish("reboot/done", {"round_no": 1, "t_reboot": 100.0, "ok": True})
        await bus2.publish("device/recovered", {"round_no": 1, "t_reboot": 100.0, "t_recover": None})
        await bus2.publish("check/event", {"round_no": 1, "found": False, "error": None})
        await bus2.publish("check/status", {"round_no": 1, "changed": False, "diff": {}, "error": None})

        # 无 Analyst 时 bus.request 超时（ADVISE_TIMEOUT=35s 太长，这里直接断言不卡死：
        # 通过缩短超时验证会回退到确定性失败记账。）
        coordinator2.ADVISE_TIMEOUT = 0.5
        await asyncio.wait_for(coordinator2._round_done_event.wait(), timeout=10)
        return {
            "aborted": coordinator2.ctx.aborted,
            "total_failures": coordinator2.total_failures,
        }

    result = asyncio.run(_scenario_no_analyst())
    # 无 Analyst：不应被 Analyst 叫停，但应确定性记一次失败（设备确实没恢复）。
    assert result["aborted"] is False
    assert result["total_failures"] == 1
