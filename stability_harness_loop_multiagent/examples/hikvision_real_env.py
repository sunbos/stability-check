"""海康威视门禁稳定性 真实环境冒烟测试。

完整跑通以下组件：
  - 真实海康设备（通过 urllib 走 HTTP ISAPI + Digest 鉴权）
  - 真实大模型（经 OpenRouter，模型 tencent/hy3:free），自动探测：
    依次从 LLM_API_KEY / OPENROUTER_API_KEY / .env 读取；
    若未配置密钥，则回退到确定性的规则兜底逻辑。

设备默认值取自 configs/door_restart_stability.yaml（主干测试场景：
192.168.3.33/admin/121212..）。可通过环境变量覆盖（两套命名并存，优先级见下）：
    STABILITY_DEVICE_IP / STABILITY_DEVICE_USERNAME / STABILITY_DEVICE_PASSWORD
    STABILITY_STRATEGY
    BURNIN_HOST / BURNIN_USER / BURNIN_PASSWORD / BURNIN_STRATEGY
前端只需维护一套「3 输入框 + 策略」即可驱动完整稳定性回归（与 generic_harness
共用同一套 os.environ 变量约定）。

用法：
    python -m stability_harness_loop_multiagent.examples.hikvision_real_env
    python -m stability_harness_loop_multiagent.examples.hikvision_real_env --rounds 1
    python -m stability_harness_loop_multiagent.examples.hikvision_real_env --no-llm

每轮进度实时打印，带有清晰的分割线（裁决 + 事实 + 风险），
方便观察循环实时运行情况。
"""

import argparse
import asyncio
from datetime import datetime
import json
import logging
import os
import sys

# 以裸脚本方式运行时，确保包可被正常导入。
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from stability_harness_loop_multiagent.business.hikvision.adapter import HikvisionAdapter
from stability_harness_loop_multiagent.business.hikvision.advisor import HikvisionAdvisor
from stability_harness_loop_multiagent.business.hikvision.client import HikvisionClient
from stability_harness_loop_multiagent.business.hikvision.diagnostic import (
    DiagnosticKernel, HEAL_RETRIGGER, HEAL_TIME_SYNC,
)
from stability_harness_loop_multiagent.business.hikvision.llm import (
    chat_json, get_client, get_model_name,
)
from stability_harness_loop_multiagent.business.hikvision.runner import (
    _default_llm_decide, _default_parse, _make_llm_decide, _patch_worker_plan_handler,
)
from stability_harness_loop_multiagent.business.hikvision.worker import HikvisionWorker
from stability_harness_loop_multiagent.core.agent import AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.telemetry import MemorySink, Telemetry
from stability_harness_loop_multiagent.harness.verify import VerificationAgent, Verifier
from stability_harness_loop_multiagent.harness.watchdog import Watchdog
from stability_harness_loop_multiagent.harness.governance import Governance, GovernanceAgent
from stability_harness_loop_multiagent.loop.context import SharedContext
from stability_harness_loop_multiagent.loop.decision import DecisionAuthority
from stability_harness_loop_multiagent.loop.driver import ControlLoop, RunConfig
from stability_harness_loop_multiagent.loop.scheduler import Scheduler
from stability_harness_loop_multiagent.multi_agent.observers.scribe import ScribeAgent
from stability_harness_loop_multiagent.multi_agent.advisors.risk_analyst import RiskAnalyst
from stability_harness_loop_multiagent.multi_agent.advisors.trend_supervisor import TrendSupervisorAgent
from stability_harness_loop_multiagent.multi_agent.observers.notifier import NotifierAgent
from stability_harness_loop_multiagent.multi_agent.observers.gov_panel import GovernancePanelAgent
from stability_harness_loop_multiagent.harness.tracer import EngineBusTracer

# 终端报告渲染（rich 优先，标准库回退）统一由 examples/_report 提供，所有示例共用。
from stability_harness_loop_multiagent.examples._report import (  # noqa: E402
    Table, _RICH, _CONSOLE, _rich_escape,
    _VERDICT_CN, _disp_len, _pad, _VCOLOR,
    _print_header, _kv, _flush_kv,
)


# ---- 中文显示映射 -------------------------------------------------
_FACT_CN = {
    "remote_open_triggered": "远程开门已触发",
    "lock_opened": "门锁已打开",
    "lock_closed": "门锁已关闭",
    "recovered": "已恢复在线",
    "door_offline": "循环内离线",
    "checks_received": "已收到检查",
    "self_healed": "已自愈",
}
_STAGE_CN = {
    "setup_start": "准备开始",
    "setup_baseline_start": "记录基线开始",
    "setup_baseline_done": "记录基线完成",
    "setup_reboot_start": "基准重启开始",
    "setup_reboot_done": "基准重启完成",
    "setup_probe_start": "基准探测开始",
    "setup_probe_done": "基准探测完成",
    "setup_warmup_start": "基准预热开始",
    "setup_warmup_done": "基准预热完成",
    "setup_done": "准备完成",
    "act_start": "执行开始",
    "act_done": "执行完成",
    "door_open_seen": "检测到开门",
    "door_closed_seen": "检测到关门",
    "door_poll_start": "门状态轮询开始",
    "door_poll_done": "门状态轮询完成",
    "query_events_start": "查询事件开始",
    "query_window": "查询窗口",
    "query_events_done": "查询事件完成",
    "reboot_start": "重启开始",
    "reboot_done": "重启完成",
    "reboot_wait_start": "重启等待开始",
    "reboot_wait_done": "重启等待完成",
    "probe_start": "探测开始",
    "probe_back_online": "探测到重新在线",
    "probe_offline_seen": "探测到离线",
    "probe_done": "探测完成",
    "verify_online_start": "验证在线开始",
    "verify_online_done": "验证在线完成",
    "recover_start": "恢复开始",
    "heal_diagnose_start": "自愈诊断开始",
    "heal_diagnose_done": "自愈诊断完成",
    "heal_time_sync_done": "时间同步完成",
    "heal_time_sync_failed": "时间同步失败",
    "recover_done": "恢复完成",
    "check_done": "检查完成",
    "precond_start": "前置条件开始",
    "precond_door_ok": "前置条件 门在线",
    "precond_door_offline": "前置条件 门离线",
    "precond_capabilities_failed": "前置条件 读取能力失败",
    "precond_mode_unsupported": "前置条件 串口模式不支持",
    "precond_config_failed": "前置条件 读取配置失败",
    "precond_serial_ok": "前置条件 串口模式已就绪",
    "precond_serial_mismatch": "前置条件 串口模式不匹配",
    "precond_serial_put_failed": "前置条件 串口切换失败",
    "precond_serial_reboot_required": "前置条件 串口切换触发重启",
    "precond_serial_reboot_wait": "前置条件 等待重启上线",
    "precond_serial_reboot_failed": "前置条件 等待重启失败",
    "precond_serial_fixed": "前置条件 串口模式已修正",
    "precond_serial_mismatch_after": "前置条件 串口模式修正后仍不匹配",
    "precond_serial_confirm_failed": "前置条件 确认串口模式失败",
    "precond_done": "前置条件完成",
    "act_door_offline": "循环中门离线(设备问题)",
}
_EXTRA_CN = {
    "ok": "成功", "online": "在线", "duration": "耗时", "error": "错误",
    "raw": "原始计数", "filtered": "过滤后计数", "delay": "延迟",
    "wait": "等待", "mode": "模式", "lookback": "回看窗口",
    "start": "起点", "end": "终点", "found": "命中", "count": "次数",
    "skew": "时间偏差", "decision": "决策", "healed": "已自愈",
    "recovered": "已恢复", "reason": "原因", "interval": "间隔",
    "confirm": "确认次数", "source": "来源",
    # 时间线 / 事实中出现的英文键 -> 中文（保持全中文报告风格统一）
    "op": "操作", "saw_open": "见开门", "saw_close": "见关门",
    "open": "开门时间", "open_duration": "开门保持",
    "backward_buffer": "回看余量", "backward": "回看",
    "remote_open_triggered": "开门已触发", "lock_opened": "门锁已开",
    "lock_closed_found": "门锁已关", "in_loop": "循环内",
}
_STATEKEY_CN = {
    "plan": "计划", "skip_reboot": "跳过重启",
    "event_check_delay_adjust": "事件查询延迟调整",
    "trigger_interval_adjust": "触发间隔调整",
    "diagnose_whitelist": "诊断白名单",
    "op": "操作", "act_ok": "执行成功", "act_error": "执行错误",
    "reboot_ok": "重启成功", "online": "在线", "error": "错误",
    "timeline": "时间线",
}
_OP_CN = {"remote_open_door": "远程开门"}
# 计划（Advisor 发布 / 规则兜底）内部字段 -> 中文，用于 Worker 状态树状展示。
_PLAN_KEY_CN = {
    "test_type": "测试类型",
    "device_category": "设备类别",
    "brand": "品牌",
    "test_scenario": "测试场景",
    "test_objective": "测试目标",
    "test_description": "测试描述",
    "preconditions": "前置条件",
    "test_steps": "测试步骤",
    "expected_result": "预期结果",
    "metrics": "度量指标",
}
# 串口外设模式枚举（设备返回的原始英文值 -> 中文，保持报告术语统一）
_SERIAL_MODE_CN = {
    "readerMode": "读卡器模式",
    "accessControlHost": "门禁主机",
    "accessDetection": "门禁侦测",
}


def _cn_dict(d):
    """递归翻译字典的键（用于 Worker 状态 / 末轮阶段展示）。"""
    if not isinstance(d, dict):
        return d
    out = {}
    for k, v in d.items():
        label = _STATEKEY_CN.get(k, k)
        if k == "op" and isinstance(v, str):
            v = _OP_CN.get(v, v)
        out[label] = _cn_dict(v) if isinstance(v, dict) else v
    return out


def _format_state_tree(state) -> str:
    """把 worker.state / 末轮阶段等嵌套字典渲染成中文键值树（多行）。

    顶层键经 _STATEKEY_CN 翻译；计划类 dict 的内部英文键再经 _PLAN_KEY_CN
    翻译；列表（前置条件/测试步骤/度量）逐项成行，长文本交终端自然折行。
    配合 _kv 的多行续行对齐，终端阅读性最佳。
    """
    if not isinstance(state, dict) or not state:
        return "（空）"

    def _walk(v, indent: int) -> list:
        pad = "  " * indent
        lines = []
        if isinstance(v, dict):
            for k, val in v.items():
                label = _PLAN_KEY_CN.get(k, _STATEKEY_CN.get(k, k))
                if isinstance(val, dict):
                    lines.append(f"{pad}{label}:")
                    lines.extend(_walk(val, indent + 1))
                elif isinstance(val, list):
                    if not val:
                        lines.append(f"{pad}{label}: （无）")
                    elif all(not isinstance(x, (dict, list)) for x in val):
                        lines.append(f"{pad}{label}:")
                        for item in val:
                            lines.append(f"{pad}  - {item}")
                    else:
                        lines.append(f"{pad}{label}: {val}")
                else:
                    lines.append(f"{pad}{label}: {val}")
        else:
            lines.append(f"{pad}{v}")
        return lines

    out = []
    for top_k, top_v in state.items():
        label = _STATEKEY_CN.get(top_k, top_k)
        if isinstance(top_v, dict):
            out.append(f"{label}:")
            out.extend(_walk(top_v, 1))
        elif isinstance(top_v, list):
            out.append(f"{label}:")
            for item in top_v:
                out.append(f"  - {item}")
        else:
            out.append(f"{label}: {top_v}")
    return "\n".join(out)


def _build_llm_verifier(use_llm: bool):
    """构造大模型计划校验器（opt-in 校验网关的 ``Verifier``）。

    仅当 ``use_llm`` 且真正配置了密钥时，挂一个 LLM 输入护栏：把压测计划交给
    模型判断「是否安全可执行」。无 LLM / 解析失败时放行（回退规则逻辑），
    保证闭环稳健、绝不因校验器异常而误伤正常计划。
    """
    if not use_llm:
        return None
    client = get_client()
    if client is None:
        return None
    verifier = Verifier(fail_closed=True)
    _vlog = logging.getLogger(
        "stability_harness_loop_multiagent.examples.hikvision_real_env.verify"
    )
    # 关键：这是「受控压测」计划，重启/开门等操作是测试本身的目的，并非对生产系统的
    # 破坏性变更。默认放行，仅当计划确实危险（如 skip_reboot=true 破坏测试前提）时才拒。
    system_prompt = (
        "你是一名海康门禁稳定性压测的安全校验器。输入是一个受控的压测计划 JSON，"
        "字段含：skip_reboot(是否跳过重启)、event_check_delay_adjust(事件查询"
        "延迟调整)、trigger_interval_adjust(触发间隔调整)、diagnose_whitelist"
        "(自愈白名单)。这是一个受控的稳定性压测计划：重启/开门等操作是测试本身的"
        "目的，并非对生产系统的破坏性变更。请仅在该计划明确危险时拒绝，例如 "
        "skip_reboot=true（跳过重启会破坏测试前提）或包含不可逆/破坏性操作。"
        "默认应返回 {\"allowed\": true}；仅当确实不安全时才返回 "
        "{\"allowed\": false, \"reason\": \"...\"}。只返回 JSON。"
    )

    def _llm_plan_guard(item):
        # 模块级 chat_json 内部已捕获异常并返回 None；此处不再 try/except。
        # 超时（20s）视作调用失败，按放行处理，不阻断压测。
        res = chat_json(
            client, system_prompt, json.dumps(item, ensure_ascii=False), timeout=20.0
        )
        _vlog.debug("LLM 计划校验原始返回: %s", res)
        if not isinstance(res, dict):
            return (True, "llm-no-json-allow")
        # chat_json(response_model=None) 能解析 JSON 时直接返回 dict,
        # 否则返回 {"text": <原文>}。先看 res 自身是否含 allowed/allow 字段。
        if "allowed" in res or "allow" in res:
            if res.get("allowed") is False or res.get("allow") is False:
                return (False, str(res.get("reason", "llm-rejected")))
            return (True, "")
        # 回退:尝试再解析 {"text": ...} 中的 JSON(双保险)。
        try:
            parsed = json.loads(res.get("text", ""))
        except (json.JSONDecodeError, TypeError):
            return (True, "llm-no-json-allow")
        if not isinstance(parsed, dict):
            return (True, "llm-no-json-allow")
        if parsed.get("allowed") is False or parsed.get("allow") is False:
            return (False, str(parsed.get("reason", "llm-rejected")))
        return (True, "")
    verifier.add_input_guardrail("llm_plan_safety", _llm_plan_guard)
    return verifier


def _b(v) -> str:
    """布尔值转中文「是 / 否」，其余原样。"""
    if isinstance(v, bool):
        return "是" if v else "否"
    return str(v)


def _fmt_fact_value(v) -> str:
    """把事实值格式化为单行可读字符串（布尔 -> 是/否，字典去 <> 括号）。"""
    if isinstance(v, dict):
        meta = "  ".join(f"{_EXTRA_CN.get(k, k)}={_b(vv)}"
                         for k, vv in v.items())
        return meta
    return _b(v)


def _fmt_extra_val(k, v) -> str:
    """时间线附加字段的单值格式化：op 翻译为操作名，布尔 -> 是/否，字典拍平为 k=v 空格分隔。"""
    if k == "op" and isinstance(v, str):
        return _OP_CN.get(v, v)
    if isinstance(v, dict):
        return " ".join(f"{kk}={_b(vv)}" for kk, vv in v.items())
    return _b(v)


def _fmt_work_status(status: dict) -> str:
    """把庞大的 AcsWorkStatus 压成一行可读状态（ONLINE/OFFLINE + 关键字段）。"""
    ws = status.get("AcsWorkStatus", status) if isinstance(status, dict) else {}
    if not isinstance(ws, dict):
        return str(status)

    def _first(x):
        return x[0] if isinstance(x, list) and x else x

    online = _first(ws.get("doorOnlineStatus"))
    lock = _first(ws.get("doorLockStatus"))
    dstat = _first(ws.get("doorStatus"))
    state = "在线" if online == 1 else "离线"
    return f"{state} · 锁状态={lock} · 门状态={dstat}"


def _parse_ts(ts) -> "datetime | None":
    """解析阶段时间线里的 ISO 时间戳（如 2026-07-18T23:32:48+08:00）。"""
    if not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _print_round(round_no: int, total_rounds: int, verdict: str, risk: float,
                 facts: dict, timeline: list = None, *, quiet: bool = False) -> None:
    """打印单轮结果：先结论横幅，再事实清单，最后阶段时间线表。

    装了 rich 时用表格/分隔线渲染（自动 CJK 对齐）；未装时回退标准库宽度感知对齐。
    """
    vmark = {"pass": "PASS", "fail": "FAIL", "warn": "WARN",
             "recheck": "RECHECK", "abort": "ABORT"}.get(verdict, verdict.upper())
    color = _VCOLOR.get(verdict, "white")
    # 从时间线抽取关键 SLI：重启恢复耗时。
    recovery = None
    for e in (timeline or []):
        if e.get("stage") == "reboot_wait_start":
            try:
                recovery = float(e.get("wait"))
            except (TypeError, ValueError):
                recovery = None
            break

    # ---- rich 渲染路径 ----
    if _RICH:
        head = f"ROUND {round_no}/{total_rounds}"
        parts = [head, f"[[bold {color}]{vmark}[/]]", f"risk={risk:.1f}"]
        if recovery is not None:
            parts.append(f"recovery={recovery:.1f}s")
        _CONSOLE.rule("  ".join(parts))
        if quiet:
            return
        if facts:
            ft = Table(show_header=False, show_edge=False, padding=(0, 2),
                       title="事实", title_justify="left")
            ft.add_column("fact", style="bold")
            ft.add_column("value")
            for k, v in facts.items():
                ft.add_row(_rich_escape(_FACT_CN.get(k, k)),
                           _rich_escape(_fmt_fact_value(v)))
            _CONSOLE.print(ft)
        if timeline:
            # 与其他报告表格一致：默认 box 的 `│` 列分隔线 + 表头下划线，无外边框。
            tt = Table(show_header=True, show_edge=False, padding=(0, 2),
                       title="阶段时间线", title_justify="left")
            tt.add_column("阶段", header_style="bold")
            tt.add_column("累计(s)", justify="right", header_style="bold")
            tt.add_column("间隔(s)", justify="right", header_style="bold")
            tt.add_column("详情", header_style="bold")
            prev_t = 0.0
            for entry in timeline:
                stage = _STAGE_CN.get(entry.get("stage", "?"),
                                      entry.get("stage", "?"))
                t = float(entry.get("t", 0.0))
                dt = t - prev_t
                prev_t = t
                # 单行展示阶段及关键附加信息（省略 stage/ts/t 自身键）。
                extras = {k: v for k, v in entry.items()
                          if k not in ("stage", "ts", "t")}
                extra_str = " ".join(
                    f"{_EXTRA_CN.get(k, k)}={_fmt_extra_val(k, v)}"
                    for k, v in extras.items()
                ) if extras else ""
                tt.add_row(_rich_escape(stage), f"{t:.2f}", f"{dt:+.2f}",
                           _rich_escape(extra_str))
            _CONSOLE.print(tt)
        return

    # ---- 标准库回退路径（宽度感知对齐）----
    print("\n" + "─" * 72)
    head = f"ROUND {round_no}/{total_rounds}"
    line = f"  {head:<12} {vmark:<8} risk={risk:.1f}"
    if recovery is not None:
        line += f"   recovery={recovery:.1f}s"
    print(line)
    print("─" * 72)
    if quiet:
        return
    # 事实清单（关键指标）
    if facts:
        print("  事实:")
        for k, v in facts.items():
            print(f"    - {_FACT_CN.get(k, k)}: {_fmt_fact_value(v)}")
    # 阶段时间线（对齐表：阶段 / 累计秒 / 间隔秒 / 关键详情）
    if timeline:
        print("  阶段时间线:")
        print(f"    {_pad('阶段', 22)}{'累计(s)':>10}{'间隔(s)':>10}   详情")
        print(f"    {'─'*22}{'─'*10}{'─'*10}   {'─'*40}")
        prev_t = 0.0
        for entry in timeline:
            stage = _STAGE_CN.get(entry.get("stage", "?"),
                                  entry.get("stage", "?"))
            t = float(entry.get("t", 0.0))
            dt = t - prev_t
            prev_t = t
            extras = {k: v for k, v in entry.items()
                      if k not in ("stage", "ts", "t")}
            extra_str = " ".join(
                f"{_EXTRA_CN.get(k, k)}={_fmt_extra_val(k, v)}"
                for k, v in extras.items()
            ) if extras else ""
            print(f"    {_pad(stage, 22)}{t:>10.2f}{dt:>+10.2f}   {extra_str}")


async def _run_with_progress(
    client: HikvisionClient,
    max_rounds: int,
    run_timeout: float,
    instruction: str,
    use_llm: bool,
    *,
    run_reboot: bool = True,
    probe_interval: float = 5.0,
    probe_confirm_count: int = 2,
    warmup_time: float = 60.0,
    max_recover_timeout: float = 180.0,
    event_check_delay: float = 3.0,
    open_duration: float | None = None,
    device_writes: dict | None = None,
    required_serial_mode: str | None = None,
    serial_port: int = 1,
    verifier: "Verifier | None" = None,
    all_agents: bool = False,
    quiet: bool = False,
) -> dict:
    """自定义运行器，每轮进度实时打印。

    与 run_hikvision_stability 类似，但在 loop.start() 之前就向
    loop/done 订阅打印器，从而让用户实时看到每一轮的结果。重启 / 探测 / 预热
    等配置（spec §4.1、§4.2、§6）转发给 HikvisionWorker。同时会在 Loop 启动前
    运行 ``worker.pre_loop_setup()``，以便基准重启耗时只测量一次并在每轮复用。
    """
    # use_llm=True: llm_parse 留 None，advisor 内部自取 LLM 客户端 + LLMPlan 解析
    #   （无密钥则回退规则兜底）；diagnostic 用 _make_llm_decide() 探测 LLM。
    # use_llm=False (--no-llm): 强制传 _default_parse，绕过 LLM 调用保证确定性。
    if use_llm:
        llm_parse = None
        llm_decide = _make_llm_decide() or _default_llm_decide
    else:
        llm_parse = _default_parse
        llm_decide = _default_llm_decide

    bus = EventBus()
    mem = MemorySink()
    tel = Telemetry(bus=bus, sinks=[mem])
    # MVP 全量接入（opt-in，--all-agents）：挂载治理网关 + 多 Advisor + 通知 +
    # 治理面板。默认（生产最佳实践）不挂，保持最小集。Governance() 默认放行态
    # （无 access/配额/预算/熔断规则），不会误拒操作导致循环跑不通；governance
    # 接总线后，治理决策事实发到 harness/fact/governance.decision 供面板/追踪消费。
    gov = Governance() if all_agents else None
    if gov is not None:
        gov.telemetry = tel
    # 三引擎活动追踪器：订阅总线全部话题，按 Loop/MAS/Harness 归类，
    # 供每轮分栏面板与 SUMMARY 架构观测段使用（业界标准可观测层）。
    tracer = EngineBusTracer(bus)
    ctx = SharedContext(baseline={"kind": "hikvision"}, strategy_text=instruction)
    decision = DecisionAuthority()
    # ControlLoop 会等待 max(recover_timeout, check_timeout) 才能收到
    # target/checked 事件（见 driver.py 的轮次逻辑）。当 run_reboot=True 时，
    # 重启 + 探测 + 预热可能耗时 max_recover_timeout + warmup_time，需额外
    # 预留探测 + 开门 + 查事件的缓冲。当 run_reboot=False 时，do_work 仅做一次
    # 远程开门 + 等待门关闭 + 事件查询；查询延迟须 >= openDuration+余量+1，
    # 因此超时按生效的开启保持时间计算。
    if run_reboot:
        round_act_timeout = max_recover_timeout + warmup_time + 30.0
    else:
        effective_open = open_duration if open_duration else 2.0
        # 轮询等待门关闭的最坏情况为 openDuration*3+5，超时须覆盖该上限 + 查询开销。
        round_act_timeout = max(event_check_delay, effective_open * 3.0 + 5.0) + 3.0
    cfg = RunConfig(max_rounds=max_rounds, max_duration=0.0, fail_threshold=10_000,
                    vote_timeout=0.1, recover_timeout=round_act_timeout,
                    check_timeout=round_act_timeout, recheck_limit=0)
    term = cfg.build_termination()
    loop = ControlLoop(
        bus, ctx, decision, term,
        vote_timeout=cfg.vote_timeout,
        recover_timeout=cfg.recover_timeout,
        check_timeout=cfg.check_timeout,
        recheck_limit=cfg.recheck_limit,
        # k=0.0 关闭自适应冷却：每轮的 round_act_timeout 已经等待了
        # 完整的「重启+探测+预热+开门」周期。若不关闭，恢复时间（约 180s）
        # 乘以 k（1.5）= 每轮额外睡眠 270s，5 轮重启测试将耗时 40 分钟以上。
        scheduler=Scheduler(base=0.0, k=0.0, min_interval=0.0),
        telemetry=tel,
    )

    # 实时进度打印器：在 loop.start() 之前订阅 loop/done。
    # `worker` 在下方定义；闭包按引用捕获它，因此在回调触发（loop.start()
    # 之后）时它已经绑定。
    def _on_loop_done(_topic, msg):
        if not isinstance(msg, dict):
            return
        # 抓取 worker 的每轮时间线快照（可能被并发追加，但 list() 给出的
        # 浅拷贝已足够用于打印）。
        tl = list(getattr(worker, "_timeline", []))
        _print_round(
            msg.get("round", 0),
            cfg.max_rounds,
            msg.get("verdict", "?"),
            float(msg.get("risk", 0.0)),
            msg.get("facts", {}) or {},
            timeline=tl,
            quiet=quiet,
        )
        # 首轮前打印启动/校验阶段面板（round=0 的网关活动，如 LLM 校验、
        # Advisor 计划发布、治理决策），让三引擎在运行早期的活动也显式可见。
        if msg.get("round", 0) == 1:
            tracer.print_setup_panel()
        # 每轮三引擎活动分栏面板，让 Harness/Loop/MAS 的协作显式可见。
        tracer.print_panel(msg.get("round", 0))
    bus.subscribe("loop/done", _on_loop_done)

    adapter = HikvisionAdapter(client)
    diagnostic = DiagnosticKernel(
        llm_decide=llm_decide,
        whitelist=[HEAL_TIME_SYNC, HEAL_RETRIGGER],
    )
    worker = HikvisionWorker(
        bus,
        AgentSpec(id="w1", role="hik",
                  subscriptions=["hikvision/plan"]),
        adapter, client, time_skew_threshold=3.0,
        diagnostic=diagnostic,
        run_reboot=run_reboot,
        probe_interval=probe_interval,
        probe_confirm_count=probe_confirm_count,
        warmup_time=warmup_time,
        max_recover_timeout=max_recover_timeout,
        event_check_delay=event_check_delay,
        # worker 自行 GET 设备 openDuration（来源标 device）；此处 open_duration
        # 仅用于上方超时估算。显式 --set 的 PUT 由 device_writes 处理。
        open_duration=None,
        device_writes=device_writes,
        required_serial_mode=required_serial_mode,
        serial_port=serial_port,
        governance=gov,
        enable_governance=(gov is not None),
        governance_timeout=1.0,
    )
    _patch_worker_plan_handler(worker)

    advisor = HikvisionAdvisor(
        bus, AgentSpec(id="a1", role="risk"),
        instruction=instruction,
        llm_parse=llm_parse,
        # 大模型校验网关：解析出的计划先经 harness/verify/request 由 LLM 护栏裁决；
        # 被拒/超时则丢弃计划、由规则兜底接管（fail-closed）。
        enable_verify=(verifier is not None),
        verify_timeout=30.0,
    )
    scribe = ScribeAgent(
        bus, AgentSpec(id="o1", role="scribe",
                       subscriptions=["loop/done", "agent/incident", "target/#"]),
    )
    # 看门狗的 stall_timeout 必须超过 max_recover_timeout + warmup_time。
    stall_timeout = max(300.0, max_recover_timeout + warmup_time + 60.0)
    dog = Watchdog(bus, stall_timeout=stall_timeout, check_interval=0.05)

    # opt-in 大模型校验网关：仅当 verifier 提供时挂载 VerificationAgent，
    # 与 advisor.enable_verify 配对（advisor 在采纳计划前发 harness/verify/request）。
    extra_agents = []
    if verifier is not None:
        verify_agent = VerificationAgent(bus, verifier)
        extra_agents.append(verify_agent)
    # MVP 全量接入（--all-agents）：挂载额外建议型 Advisor（独立风险视角/趋势监督）、
    # 治理网关 + 治理观测面板、通知 Observer。默认不挂，保持最小集。
    gov_panel = None
    if all_agents:
        extra_agents.append(RiskAnalyst(
            bus, AgentSpec(id="a2", role="risk-analyst"), weight=0.5))
        extra_agents.append(TrendSupervisorAgent(
            bus, AgentSpec(id="a3", role="trend"), weight=0.5))
        extra_agents.append(GovernanceAgent(bus, gov))
        gov_panel = GovernancePanelAgent(
            bus, AgentSpec(id="o2", role="gov-panel",
                           subscriptions=["harness/fact/governance.decision",
                                          "governance/panel/request"]))
        extra_agents.append(gov_panel)
        extra_agents.append(NotifierAgent(
            bus, AgentSpec(id="o3", role="notifier")))

    # 启动顺序很关键：校验网关 verify_agent 必须先于 advisor 订阅
    # harness/verify/request，否则 advisor.start() 发起的校验请求无人应答而超时→兜底。

    # 让 advisor / 校验网关的进度日志可见（干净格式输出到 stdout），避免长时间的
    # LLM 解析/校验/基准重启"静默等待"被误判为卡死。
    for _nm in (
        "stability_harness_loop_multiagent.business.hikvision.advisor",
        "stability_harness_loop_multiagent.agent.verify",  # VerificationAgent 的实际 logger
    ):
        _l = logging.getLogger(_nm)
        if not _l.handlers:
            _h = logging.StreamHandler(sys.stdout)
            _h.setFormatter(logging.Formatter("%(message)s"))
            _l.addHandler(_h)
            _l.propagate = False
        _l.setLevel(logging.INFO)

    if not quiet:
        print("▶ 正在启动 Agent：大模型解析指令 + 计划校验"
              "（可能耗时数十秒，请稍候）...", flush=True)
    for a in (worker, scribe, dog, *extra_agents, advisor):
        await a.start()
    # Loop 前准备：记录基线 + 基准重启 + 测量耗时。
    # 在 worker.start() 之后（以便其能发布事件）但在 loop.start() 之前执行，
    # 这样测量到的 baseline_reboot_duration 才能供 do_work() 使用。
    # 放在线程中运行，因为 pre_loop_setup 在基准重启探测时可能阻塞 60-180s；
    # 这绝不能阻塞事件循环。打印测量到的耗时，让用户看到 Loop 前的基线。
    if not quiet:
        print("\n--- Loop 前准备（记录基线 + 基准重启，约 60s，请稍候）---", flush=True)
    setup_info = await asyncio.to_thread(worker.pre_loop_setup)
    if not quiet:
        print("--- Loop 前准备（记录基线 + 基准重启） ---")
        duration = setup_info.get("baseline_reboot_duration", 0.0)
        print(f"  基准重启耗时: {duration:.2f}s")
        print(f"  基线序列号: {setup_info.get('baseline', {}).get('serialNos', {})}")
        print(f"  门锁开启保持(openDuration)={setup_info.get('open_duration')}s"
              f"（{setup_info.get('open_duration_source')}）")
        precond = setup_info.get("precond", {})
        if precond:
            print(f"  前置条件就绪: {'是' if precond.get('satisfied') else '否'}"
                  f"（串口修正={precond.get('serial_fixed')}）")
            if precond.get("current_mode") is not None:
                print(f"    当前串口模式={precond.get('current_mode')} "
                      f"期望={required_serial_mode}")
            if precond.get("cause"):
                print(f"    前置条件未就绪成因={precond.get('cause')}")
        print(f"  准备完成: {setup_info.get('setup_done')}")
        plan_state = "已采纳 LLM 计划" if getattr(advisor, "_plan", None) else "校验拒绝 → 规则兜底"
        print(f"  大模型计划: {plan_state}")
    await loop.start()
    try:
        await asyncio.wait_for(loop._task, run_timeout)
    finally:
        for a in (worker, advisor, scribe, dog, *extra_agents):
            await a.stop()
    result = {"ctx": ctx, "loop": loop, "worker": worker,
              "advisor": advisor, "telemetry": tel, "config": cfg,
              "tracer": tracer}
    if gov_panel is not None:
        result["gov_panel"] = gov_panel
    return result


async def _main() -> int:
    parser = argparse.ArgumentParser(description="海康威视 真实环境冒烟测试")
    parser.add_argument("--rounds", type=int, default=3,
                        help="最大轮数（默认 3）")
    parser.add_argument("--timeout", type=float, default=1200.0,
                        help="运行超时时间，单位秒（默认 1200，适配重启压测）")
    parser.add_argument("--no-llm", action="store_true",
                        help="强制使用规则兜底逻辑（跳过大模型调用）")
    parser.add_argument("--no-verify", action="store_true",
                        help="禁用大模型计划校验网关（默认启用，需 LLM 密钥）")
    parser.add_argument("--all-agents", action="store_true",
                        help="MVP 全量接入：挂载治理网关+多Advisor+通知+治理面板（默认仅最小集）")
    parser.add_argument("--host", default=None,
                        help="覆盖设备 IP（优先 STABILITY_DEVICE_IP，其次 BURNIN_HOST 环境变量）")
    parser.add_argument("--user", default=None,
                        help="覆盖设备用户名（优先 STABILITY_DEVICE_USERNAME，其次 BURNIN_USER）")
    parser.add_argument("--password", default=None,
                        help="覆盖设备密码（优先 STABILITY_DEVICE_PASSWORD，其次 BURNIN_PASSWORD）")
    # 重启 + 探测 + 预热配置（spec §4.1、§4.2、§6 worker.*）
    parser.add_argument("--no-reboot", action="store_true",
                        help="跳过重启阶段（每轮仅执行远程开门）")
    parser.add_argument("--quiet", action="store_true",
                        help="静默模式：仅打印每轮结论横幅与末尾 SUMMARY，省略 RUN CONFIG / 阶段时间线等细节")
    parser.add_argument("--warmup", type=float, default=60.0,
                        help="设备上线后的预热秒数（spec §6，默认 60）")
    parser.add_argument("--probe-interval", type=float, default=5.0,
                        help="探测轮询间隔秒数（spec §4.1，默认 5）")
    parser.add_argument("--probe-confirm", type=int, default=2,
                        help="确认在线的连续成功次数（spec §4.1，默认 2）")
    parser.add_argument("--max-recover", type=float, default=180.0,
                        help="重启后等待设备上线的最大秒数（默认 180）")
    parser.add_argument("--event-delay", type=float, default=3.0,
                        help="远程开门后查询事件日志前的等待秒数"
                             "（spec §6 worker.event_check_delay，默认 3）")
    parser.add_argument("--set", action="append", default=None, metavar="KEY=VALUE",
                        help="显式将某项设备参数写入设备（PUT），可重复。默认不传："
                             "只 GET，不修改设备配置。仅列出在白名单内的键才会被写入，"
                             "例如 --set openDuration=8。新增可写能力无需新增指令，"
                             "只需在 worker._DEVICE_WRITE_WHITELIST 登记。")
    parser.add_argument("--serial-mode", default=None,
                        help="前置条件期望的串口 1 外设类型(mode)，如 externMode/"
                             "readerMode/accessControlHost/accessDetection。若当前模式"
                             "不符，自动 PUT 切换并等待设备自动重启(autoReboot)上线；"
                             "不传则只在门离线时判定前置条件失败（不自动切换）。")
    parser.add_argument("--trace-html", default=None, metavar="PATH",
                        help="导出三引擎架构观测报告为 HTML（零依赖）到指定路径")
    parser.add_argument("--trace-json", default=None, metavar="PATH",
                        help="导出三引擎架构观测报告为 JSON 到指定路径")
    args = parser.parse_args()

    # 解析 --set KEY=VALUE（可重复）为显式写入意图字典。
    # 白名单在 worker._DEVICE_WRITE_WHITELIST 校验；未列出的键被忽略。
    device_writes: dict = {}
    if args.set:
        for item in args.set:
            if "=" not in item:
                print(f"  警告：忽略无效 --set 参数（缺少 '='）：{item!r}")
                continue
            k, v = item.split("=", 1)
            device_writes[k.strip()] = v.strip()

    # 设备配置优先级：命令行参数 -> 环境变量(STABILITY_DEVICE_* 优先于 BURNIN_*) -> 主干默认值。
    # 两套命名并存，使前端只需维护一套「3 输入框 + 策略」即可驱动完整稳定性回归。
    host = (args.host or os.environ.get("STABILITY_DEVICE_IP")
            or os.environ.get("BURNIN_HOST") or "192.168.3.33")
    user = (args.user or os.environ.get("STABILITY_DEVICE_USERNAME")
            or os.environ.get("BURNIN_USER") or "admin")
    pwd = (args.password or os.environ.get("STABILITY_DEVICE_PASSWORD")
           or os.environ.get("BURNIN_PASSWORD") or "121212..")
    instruction = (os.environ.get("STABILITY_STRATEGY")
                   or os.environ.get("BURNIN_STRATEGY") or "")
    run_reboot = not args.no_reboot
    # 大模型校验网关：仅当未显式禁用且 LLM 启用时构建（内部无密钥会自动回退 None）。
    verifier = None if args.no_verify else _build_llm_verifier(use_llm=not args.no_llm)

    # 先连接设备：获取工作状态 / 设备时间 / 门锁开启保持时间(openDuration)。
    # 边界原则：默认只 GET（读取当前值用于本地查询时序），不主动修改设备；
    # 仅当用户显式传入 --open-duration 时才 PUT 写入设备。
    client = HikvisionClient(host=host, port=80, username=user, password=pwd,
                             http_timeout=15.0)
    try:
        status = client.get_work_status()
        t = client.get_time().get("Time", {})
        # 读取门锁开启保持时间（只读，不写入设备）
        try:
            door_param = client.get_door_param(1)
            dev_open = float(door_param.get("openDuration") or 2.0)
            open_src = "从设备读取"
        except Exception as exc:  # noqa: BLE001
            dev_open = 2.0
            open_src = f"读取失败，回退默认({exc})"
    except Exception as exc:  # noqa: BLE001
        print(f"  连接出错: {exc}")
        print("  请检查设备可达性 / 凭据。已中止。")
        return 2

    if not args.quiet:
        _print_header("海康威视 真实环境稳定性测试 · RUN CONFIG")
        _kv("设备", f"{host}:80 (user={user})")
        _kv("门禁状态", _fmt_work_status(status))
        _kv("设备时间", f"{t.get('localTime')}")
        # 显示串口 1 当前外设类型(mode)，便于判断前置条件（门离线可能因 mode 不对）。
        try:
            serial_cfg = client.get_serial_config(1)
            mode = serial_cfg.get("mode")
            mode_cn = _SERIAL_MODE_CN.get(mode, mode)
            _kv("串口1 模式", f"{mode_cn} (deviceName={serial_cfg.get('deviceName')})")
        except Exception as exc:  # noqa: BLE001
            _kv("串口1 模式", f"读取失败（不影响运行）: {exc}")
        _kv("轮数", args.rounds)
        _kv("运行超时", f"{args.timeout}s")
        _kv("LLM", "禁用 (--no-llm)" if args.no_llm else "自动探测 (LLM_API_KEY/.env)")
        _kv("策略", instruction if instruction else "(空)")
        _kv("每轮流程", "启用：重启 → 探测 → 预热 → 开门" if run_reboot else "禁用 (--no-reboot)：仅开门")
        if args.serial_mode:
            _kv("前置串口模式", f"期望={args.serial_mode}（不符将自动切换并等待重启上线）")
        else:
            _kv("前置串口模式", "未指定（门离线将判前置条件失败，不自动切换）")
        if run_reboot:
            _kv("探测/预热/恢复", f"interval={args.probe_interval}s confirm={args.probe_confirm} "
                                  f"warmup={args.warmup}s maxRecover={args.max_recover}s")
        _kv("事件查询延迟", f"{args.event_delay}s")
        if "openDuration" in device_writes:
            _kv("门锁开启保持(openDuration)", f"将写入设备: {device_writes['openDuration']}s（会修改设备配置）")
        else:
            _kv("门锁开启保持(openDuration)", f"{dev_open}s（{open_src}，只读，用于查询时序）")

        # LLM 探测预览（不会真正调用 API）
        if not args.no_llm:
            llm = get_client()
            if llm is None:
                _kv("LLM 状态", "未配置密钥 → 规则兜底（.env 设 LLM_API_KEY）")
            else:
                _kv("LLM 状态", f"就绪（模型={get_model_name()}）")
        _kv("大模型校验", "启用（计划经 LLM 护栏裁决）" if verifier is not None
            else "禁用（--no-verify 或未配置 LLM）")
        _kv("MVP 全量接入", "是（治理+多Advisor+通知+面板）" if args.all_agents
            else "否（仅最小集：Worker+Advisor+Observer+看门狗）")
        _flush_kv()

    if not args.quiet:
        _print_header("运行稳定性循环（每轮进度如下）")
    try:
        result = await _run_with_progress(
            client=client,
            max_rounds=args.rounds,
            run_timeout=args.timeout,
            instruction=instruction,
            use_llm=not args.no_llm,
            run_reboot=run_reboot,
            probe_interval=args.probe_interval,
            probe_confirm_count=args.probe_confirm,
            warmup_time=args.warmup,
            max_recover_timeout=args.max_recover,
            event_check_delay=args.event_delay,
            required_serial_mode=args.serial_mode,
            serial_port=1,
            open_duration=(float(device_writes["openDuration"])
                           if "openDuration" in device_writes else dev_open),
            device_writes=device_writes,
            verifier=verifier,
            all_agents=args.all_agents,
            quiet=args.quiet,
        )
    except asyncio.TimeoutError:
        print(f"\n错误：运行在 {args.timeout}s 后超时")
        return 3
    except Exception as exc:  # noqa: BLE001
        print(f"\n运行出错：{exc}")
        return 4

    # 最终汇总
    _print_header("SUMMARY · 测试汇总")
    ctx = result["ctx"]
    history = ctx.snapshot().round_history
    # 区分「正常结束（达到轮数/时长上限）」与「真正中止（harness 触发）」。
    # ControlLoop 对所有停止都标 aborted=True，但到达上限属预期结束，不应
    # 误报为「中止」。原因以『已到达』开头即视为正常结束。
    abort_reason = ctx.snapshot().abort_reason
    normal_end = bool(abort_reason) and abort_reason.startswith("已到达")
    decisions = {}
    for r in history:
        key = _VERDICT_CN.get(r.verdict, r.verdict)
        decisions[key] = decisions.get(key, 0) + 1
    passed = sum(1 for r in history if r.verdict == "pass")
    total = max(1, len(history))
    # 从全量时间线抽取 SLI：总时长（用 ISO 时间戳，跨轮真实墙钟）、平均恢复耗时。
    # 注意：时间线的「累计(s)」字段每轮从 0 重新计数，不能直接 max-min 当总时长；
    # 且 _timeline 每轮重置，故总时长必须用跨轮累积的 _full_timeline 统计。
    full_tl = list(getattr(result["worker"], "_full_timeline", []) or
                   getattr(result["worker"], "_timeline", []))
    tss = [_parse_ts(e.get("ts")) for e in full_tl]
    tss = [x for x in tss if x is not None]
    total_time = (max(tss) - min(tss)).total_seconds() if tss else 0.0
    recoveries = []
    # 真实的「重启恢复耗时」= 重启触发(reboot_start) → 重新在线(reboot_wait_done)
    # 的墙钟差，而非 reboot_wait_done.actual（那是「轮起点 → 恢复」累计，含重启前
    # ~9s 的开门阶段，会虚高到 ~76s）。逐条把 reboot_wait_done 与最近的
    # reboot_start.t 配对相减；找不到配对时回退到 actual（累计口径）。
    last_reboot_start_t = None
    for e in full_tl:
        stage = e.get("stage")
        if stage == "reboot_start":
            last_reboot_start_t = e.get("t")
        elif stage == "reboot_wait_done" and "actual" in e:
            try:
                val = float(e["actual"])
                if last_reboot_start_t is not None:
                    recoveries.append(val - float(last_reboot_start_t))
                else:
                    recoveries.append(val)
            except (TypeError, ValueError):
                pass
    avg_recovery = (sum(recoveries) / len(recoveries)) if recoveries else None
    stats = result["worker"].get_chain_stats()

    _kv("总轮数", ctx.round_count)
    _kv("结束方式", "正常结束（达上限）" if normal_end else ("中止" if ctx.aborted else "未结束"))
    _kv("结束原因", repr(abort_reason))
    _kv("裁决分布", decisions)
    _kv("最终裁决", _VERDICT_CN.get(result['loop'].verdict.decision,
                                    result['loop'].verdict.decision))
    _kv("通过率", f"{passed}/{len(history)} ({100.0 * passed / total:.0f}%)")
    _kv("平均重启恢复耗时", f"{avg_recovery:.2f}s" if avg_recovery is not None else "-")
    _kv("总测试时长", f"{total_time:.1f}s")
    _kv("事件链", f"触发={stats.get('trigger')} 开门={stats.get('opened')} "
                  f"关门={stats.get('closed')}")
    _kv("循环内离线", stats.get('in_loop_offline'))
    _kv("前置修正/失败", f"{stats.get('precond_fixed')} / {stats.get('precond_failed')}")
    _kv("Worker 状态", _format_state_tree(result['worker'].state))
    # 末轮关键阶段（完整时间线已在每轮实时打印，此处仅留关键字段）。
    last_stages = dict(result['worker']._last_work_stages)
    last_stages.pop("timeline", None)
    _kv("末轮阶段", _format_state_tree(last_stages))
    # 架构观测：三引擎活动（业界标准可观测层）。展示各引擎事件分布、
    # 投票汇总、看门狗心跳、治理决策，让 Harness/Loop/MAS 协作显式可见。
    _tracer = result.get("tracer")
    if _tracer is not None:
        _tr = _tracer.report()
        _pc = _tr["per_engine_counts"]
        _kv("引擎事件数", f"Loop={_pc.get('Loop', 0)}  "
                          f"MAS={_pc.get('MAS', 0)}  Harness={_pc.get('Harness', 0)}")
        _vsum = _tr["votes"]
        if _vsum:
            _vtxt = "; ".join(
                f"{v['role']} 风险≈{v['avg_risk']:.1f} 置信≈{v['avg_conf']:.2f} "
                f"({v['rounds']}轮)" for v in _vsum
            )
            _kv("投票汇总(MAS→Loop)", _vtxt)
        _wd = _tr["watchdog"]
        _kv("看门狗(Harness)", f"心跳 {_wd['heartbeats']} 次, 中止 {_wd['aborts']} 次, "
                               f"最近 idle={_wd['last_idle_s']}s")
        _gov = _tr["governance"]
        _kv("治理决策(Harness)",
             f"决策 {_gov['decisions']} (放行 {_gov['allowed']}/拒绝 {_gov['denied']})"
             if _gov else "未挂载")
        _gp = result.get("gov_panel")
        if _gp is not None:
            print(_gp.render())
        _m = _tr["metrics"]
        if _m:
            _kv("遥测指标(Harness)", ", ".join(f"{k}×{v}" for k, v in _m.items()))
    _flush_kv()

    # 可选：导出三引擎架构观测报告（业界标准可追溯性）。
    _tracer = result.get("tracer")
    if _tracer is not None:
        if args.trace_html:
            _tracer.export_html(args.trace_html)
            print(f"  架构观测报告(HTML) 已导出: {args.trace_html}")
        if args.trace_json:
            _tracer.export_json(args.trace_json)
            print(f"  架构观测报告(JSON) 已导出: {args.trace_json}")

    # 退出码：全部通过为 0，存在失败为 1
    return 0 if all(r.verdict == "pass" for r in history) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
