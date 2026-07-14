"""系统装配层（loader）：依据 RunConfig 构造 EventBus / RunContext / 全部 Agent。

仅依赖标准库 os / sys / asyncio / importlib，不修改任何 foundation 文件。

build_system(cfg) -> (EventBus, RunContext, list[Agent])
  - EventBus：所有 agent 唯一的通信通道。
  - RunContext：共享上下文（strategy_text 已填；baseline 尽力抓一次，可被调用方覆盖）
  - list[Agent]：RebootAgent / WatchAgent / EventCheckAgent / StatusCheckAgent /
    Coordinator / AnalystAgent / ScribeAgent / NotifierAgent / TrendSupervisorAgent
    （顺序即订阅/启动顺序）。
  - 打印各 agent 的 endpoint，体现“可寻址”。

注意：harness/ 与 agents/ 下存在同名 agent 模块，且部分 harness 模块会把
agents/ 推到 sys.path 前部造成遮蔽。这里用 importlib 按显式文件路径加载 harness
版本，确保用到的是 harness/agent.py 派生出的 Agent 实现。
"""

from __future__ import annotations

import importlib.util
import os
import sys
import time

# 让 loader 既能解析 harness 内的模块（bus/agent/context），也能解析 agents/ 下的
# device_client / strategy / config / report。harness 在前，agents 在后（不抢前）。
_HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
_AGENTS_DIR = os.path.join(os.path.dirname(_HARNESS_DIR), "agents")
if _HARNESS_DIR not in sys.path:
    sys.path.insert(0, _HARNESS_DIR)
if _AGENTS_DIR not in sys.path:
    sys.path.append(_AGENTS_DIR)


def _load_harness(name: str):
    """按显式文件路径加载 harness 下的模块，避免被 agents/ 同名模块遮蔽。"""
    path = os.path.join(_HARNESS_DIR, name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# foundation（仅 harness 内存在，且不会与 agents/ 冲突）
from bus import EventBus  # noqa: E402
from context import RunContext  # noqa: E402
from agent import AgentSpec  # noqa: E402

# agents/ 下的纯工具（无同名冲突）
from device_client import DeviceClient  # noqa: E402

# harness agent 模块：按文件路径加载，确保用 harness 版本。
_reboot_mod = _load_harness("reboot_agent")
_watch_mod = _load_harness("watch_agent")
_event_mod = _load_harness("event_check_agent")
_status_mod = _load_harness("status_check_agent")
_coordinator_mod = _load_harness("coordinator")
_analyst_mod = _load_harness("analyst_agent")
_scribe_mod = _load_harness("scribe_agent")
_notifier_mod = _load_harness("notifier_agent")
_trend_mod = _load_harness("trend_supervisor_agent")

RebootAgent = _reboot_mod.RebootAgent
WatchAgent = _watch_mod.WatchAgent
EventCheckAgent = _event_mod.EventCheckAgent
StatusCheckAgent = _status_mod.StatusCheckAgent
Coordinator = _coordinator_mod.Coordinator
AnalystAgent = _analyst_mod.AnalystAgent
ScribeAgent = _scribe_mod.ScribeAgent
NotifierAgent = _notifier_mod.NotifierAgent
TrendSupervisorAgent = _trend_mod.TrendSupervisorAgent


def _isapi(host: str, path: str) -> str:
    """拼出完整 ISAPI 端点：http://{host}{path}。"""
    host = host.rstrip("/")
    if not host.startswith("http://") and not host.startswith("https://"):
        host = "http://" + host
    return host + path


def build_system(cfg) -> tuple:
    """根据 RunConfig 装配整套拷机系统。

    返回 (bus, ctx, agents)：
      - bus: 事件总线
      - ctx: 共享上下文（strategy_text 已填；baseline 尽力抓一次，可被调用方覆盖）
      - agents: 各 agent 实例列表（含 Coordinator，作为主驱动）
    """
    bus = EventBus()
    ctx = RunContext(strategy_text=getattr(cfg, "strategy_text", "") or "")
    # 部分 agent 读取 ctx.cfg（如 EventCheckAgent 取 event_window）。
    ctx.cfg = cfg

    # baseline：尽力用 DeviceClient 抓一次，失败则留空（调用方 / conftest 可覆盖）。
    try:
        client = DeviceClient(cfg.host, cfg.user, cfg.password)
        snapshot = client.get_work_status()
        ctx.set_baseline(snapshot)
    except Exception as exc:  # noqa: BLE001 - 装配阶段不阻塞，仅留日志
        ctx.set_baseline({})
        ctx.append_log(f"系统装配：基线抓取失败: {exc}")

    host = cfg.host

    def _spec(name: str, role: str, endpoint: str) -> AgentSpec:
        return AgentSpec(
            name=name,
            role=role,
            endpoint=endpoint,
            user=cfg.user,
            password=cfg.password,
            host=cfg.host,
        )

    reboot_spec = _spec(
        "reboot", "reboot", _isapi(host, "/ISAPI/System/reboot")
    )
    watch_spec = _spec(
        "watch", "watch", _isapi(host, "/ISAPI/AccessControl/AcsWorkStatus")
    )
    event_spec = _spec(
        "event", "event", _isapi(host, "/ISAPI/AccessControl/AcsEvent")
    )
    status_spec = _spec(
        "status", "status", _isapi(host, "/ISAPI/AccessControl/AcsWorkStatus")
    )
    coord_spec = _spec("coordinator", "coordinator", "")
    analyst_spec = _spec("analyst", "analyst", "")
    scribe_spec = _spec("scribe", "scribe", "")
    notifier_spec = _spec("notifier", "notifier", "")
    trend_spec = _spec("trend_supervisor", "trend_supervisor", "")

    reboot = RebootAgent(reboot_spec, bus, ctx)
    watch = WatchAgent(watch_spec, bus, ctx)
    event = EventCheckAgent(event_spec, bus, ctx)
    status = StatusCheckAgent(status_spec, bus, ctx)
    coordinator = Coordinator(coord_spec, bus, ctx, cfg=cfg)
    analyst = AnalystAgent(analyst_spec, bus, ctx, cfg=cfg)
    scribe = ScribeAgent(scribe_spec, bus, ctx, cfg=cfg)
    notifier = NotifierAgent(notifier_spec, bus, ctx, cfg=cfg)
    trend = TrendSupervisorAgent(trend_spec, bus, ctx, cfg=cfg)

    agents = [
        reboot, watch, event, status,
        coordinator, analyst, scribe, notifier, trend,
    ]

    # 体现“可寻址”：打印各 agent 的 endpoint。
    ts = time.strftime("%H:%M:%S", time.localtime())
    print(f"[{ts}] [系统] 已装配的智能体及其端点:")
    for a in agents:
        print(f"  - {a.spec.role:12s} -> {a.spec.endpoint or '(无端点)'}")

    return bus, ctx, agents


if __name__ == "__main__":
    from config import load_config_from_env

    cfg = load_config_from_env()
    bus, ctx, agents = build_system(cfg)
    print(
        f"上下文: 策略={ctx.strategy_text!r} "
        f"基线键={list(ctx.baseline.keys())}"
    )
