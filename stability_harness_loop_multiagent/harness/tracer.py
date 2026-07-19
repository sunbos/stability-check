"""EngineBusTracer —— 三引擎活动追踪器（Harness 可观测层）。

订阅事件总线全部话题（"#" 通配），将每条事件归一化为带「引擎归属」的
结构化记录，从而让 Harness / Loop / Multi-Agent 三引擎在运行与报告中
显式可见。这是业界标准可观测性的核心：分层、带标签、可聚合、可追溯。

归属规则（按话题前缀）：
  - loop/        -> Loop      （确定性控制循环 / 裁决 / 中止 / 调度）
  - agent/       -> MAS       （建议型 / 观察型智能体）
  - hikvision/   -> MAS       （业务计划）
  - target/      -> MAS       （目标适配 / Worker 事实回传）
  - harness/     -> Harness   （治理 / 校验 / 遥测 / 看门狗）

仅依赖 core.bus，不 import loop/ 或 multi_agent/，符合三引擎隔离约束。
"""

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# 话题前缀 -> 引擎归属（顺序敏感：先匹配者优先）。
_ENGINE_PREFIXES = (
    ("loop/", "Loop"),
    ("agent/", "MAS"),
    ("hikvision/", "MAS"),
    ("target/", "MAS"),
    ("harness/", "Harness"),
)


def engine_of(topic: str) -> str:
    """根据话题前缀判定事件所属引擎。"""
    for prefix, engine in _ENGINE_PREFIXES:
        if topic.startswith(prefix):
            return engine
    return "Other"


def _brief_facts(facts: Dict[str, Any]) -> str:
    """把 facts 字典渲染成简短列表，软事实带关键字段以免误导。

    硬事实（bool）仅列键名；软事实（dict，如 door_offline / lock_closed）
    每轮都会写入，仅列键名会让读者误以为「门离线了 / 门关了」，故把关键
    字段（door_offline 的 in_loop、lock_closed 的 found）带到名字后。
    """
    parts: List[str] = []
    for k, v in facts.items():
        if isinstance(v, dict):
            if k == "door_offline":
                parts.append(f"{k}(in_loop={v.get('in_loop')})")
            elif k == "lock_closed":
                parts.append(f"{k}(found={v.get('found')})")
            elif "in_loop" in v:
                parts.append(f"{k}(in_loop={v.get('in_loop')})")
            else:
                parts.append(k)
        else:
            parts.append(k)
    return "[" + ", ".join(parts) + "]"


@dataclass
class EngineEvent:
    """一条被追踪的总线事件（已带引擎归属与摘要）。"""

    ts: float
    engine: str
    topic: str
    round: Any
    summary: str
    payload: Dict[str, Any]


class EngineBusTracer:
    """三引擎活动追踪器。

    订阅总线全部话题（"#"），对每条事件做归一化与引擎归类。提供：
      - 实时分栏面板（panel_for_round / print_panel）
      - 跨轮聚合报告（report：引擎计数 / 投票 / 看门狗 / 治理 / 指标）
      - 机器可读导出（export_json / export_html，零依赖）
    """

    def __init__(self, bus=None, maxlen: int = 5000) -> None:
        self._events: List[EngineEvent] = []
        self._maxlen = maxlen
        # 当前轮次：loop/tick 与 loop/done 携带 round，用于把不带 round 的
        # 事件（如 target/*、agent/hik/done）归属到正确的轮次。
        self._current_round: Any = 0
        # 启动/校验面板（round=0）是否已打印，保证幂等只打印一次。
        self._setup_printed: bool = False
        # 与 maxlen 截断无关的权威计数：心跳/事件数/中止数不依赖被裁剪的
        # ``_events`` 缓冲，否则长运行（心跳高频）下 SUMMARY 的计数会与实时
        # 面板对不上（如 6072 实时 vs 4954 终结），甚至逻辑矛盾（心跳数 >
        # 总 harness 事件数）。
        self._engine_counts: Dict[str, int] = {
            "Harness": 0, "Loop": 0, "MAS": 0, "Other": 0,
        }
        self._hb_total: int = 0
        self._abort_total: int = 0
        self._unsub = None
        if bus is not None:
            self.attach(bus)

    # ---- 生命周期 ----------------------------------------------------
    def attach(self, bus) -> "EngineBusTracer":
        """订阅总线全部话题。可重复 attach（以最后一次为准）。"""
        if self._unsub:
            self._unsub()
        self._unsub = bus.subscribe("#", self._on_event)
        return self

    def detach(self) -> None:
        if self._unsub:
            self._unsub()
            self._unsub = None

    # ---- 事件收集 ----------------------------------------------------
    def _on_event(self, topic: str, message: Any) -> None:
        msg = message if isinstance(message, dict) else {}
        # 推进当前轮次（loop/tick 先于本轮所有事件发布）。
        if topic in ("loop/tick", "loop/done"):
            r = msg.get("round") or msg.get("round_no")
            if r:
                self._current_round = r
        round_no = msg.get("round") or msg.get("round_no") or self._current_round
        engine = engine_of(topic)
        summary = self._summarize(topic, msg)
        # 增量计数：独立于可能被裁剪的 _events 缓冲，保证心跳/事件/中止计数
        # 不受 maxlen 影响（见 __init__ 注释）。
        self._engine_counts[engine] = self._engine_counts.get(engine, 0) + 1
        if topic == "harness/liveness/heartbeat":
            self._hb_total += 1
        if topic == "harness/abort":
            self._abort_total += 1
        self._events.append(
            EngineEvent(time.time(), engine, topic, round_no, summary, msg)
        )
        if self._maxlen and len(self._events) > self._maxlen:
            self._events.pop(0)

    @staticmethod
    def _brief(msg: dict) -> str:
        op = msg.get("op") or (msg.get("operation") or {}).get("op")
        if op:
            return f"op={op}"
        if "ok" in msg:
            return f"ok={msg.get('ok')}"
        return "acted"

    def _summarize(self, topic: str, msg: dict) -> str:
        """把一条总线事件转成人类可读摘要。"""
        if topic == "loop/tick":
            return f"触发 tick (round={msg.get('round')})"
        if topic == "loop/vote/request":
            return "请求 Advisor 投票"
        if topic == "agent/vote/reply":
            return (f"投票 risk={msg.get('risk')} conf={msg.get('confidence')} "
                    f"(role={msg.get('role')})")
        if topic == "loop/done":
            # risk/recover 经确定位小数格式化，避免浮点尾巴
            # （30.000000000000004）与 ROUND 头的 risk=30.0 看似不一致，
            # 削弱事实性可信度。
            risk = msg.get("risk")
            recover = msg.get("recover_time")
            risk_s = f"{float(risk):.1f}" if risk is not None else str(risk)
            recover_s = (f"{float(recover):.2f}"
                         if recover is not None else str(recover))
            return f"裁决 {msg.get('verdict')} risk={risk_s} recover={recover_s}"
        if topic == "agent/hik/done":
            return f"Worker 完成 (round={msg.get('round')})"
        if topic == "hikvision/plan":
            return "Advisor 发布计划 hikvision/plan"
        if topic == "target/acted":
            return f"目标动作 {self._brief(msg)}"
        if topic == "target/recovered":
            return f"恢复 recovered={msg.get('recovered')}"
        if topic == "target/checked":
            facts = (msg.get("facts") or {})
            # 软事实（如 door_offline/lock_closed）以字典存储，每轮都写入，
            # 仅列键名会误导读者以为「门离线了 / 门关了」。把关键字段带到名字
            # 后（door_offline(in_loop=False)），让事实真实状态显式可见。
            return f"检查完成 facts={_brief_facts(facts)}"
        if topic == "harness/verify/request":
            return "校验请求 (LLM 护栏)"
        if topic == "harness/verify/response":
            return f"校验响应 allowed={msg.get('allowed')}"
        if topic.startswith("harness/metric/"):
            name = topic.split("/", 2)[-1]
            return f"指标 {name}={msg.get('value')}"
        if topic.startswith("harness/fact/"):
            name = topic.split("/", 2)[-1]
            return f"事实 {name}"
        if topic == "harness/liveness/heartbeat":
            idle = msg.get("idle")
            return (f"看门狗心跳 idle={idle:.1f}s"
                    if isinstance(idle, (int, float)) else "看门狗心跳")
        if topic == "harness/abort":
            return f"看门狗中止 {msg.get('reason')}"
        if topic == "loop/recheck":
            return "recheck 发布"
        if topic == "agent/incident/ack":
            return "事件 ack"
        if topic == "agent/incident":
            return f"事件 sev={msg.get('severity')}"
        return topic

    # ---- 查询 --------------------------------------------------------
    def events_for_round(self, r: Any) -> List[EngineEvent]:
        return [e for e in self._events if e.round == r]

    # ---- 实时分栏面板 ------------------------------------------------
    def panel_for_round(self, r: Any, *, skip_trace: bool = True) -> str:
        """生成三引擎分栏面板（[Loop]/[MAS]/[Harness]）。

        r == 0（或 "0"）表示启动/校验阶段——发生在首轮 ``loop/tick`` 之前的
        网关活动（如 LLM 校验请求/响应、Advisor 发布计划、治理决策），此前
        不归任何 Round 面板、也不进 HTML 导出；此处单独特立呈现。
        """
        is_setup = (r == 0 or r == "0")
        evs = sorted(self.events_for_round(r), key=lambda e: e.ts)
        if skip_trace:
            evs = [e for e in evs if e.topic != "harness/trace"]
        if not evs:
            return ""
        by_engine: Dict[str, List[EngineEvent]] = {}
        for e in evs:
            by_engine.setdefault(e.engine, []).append(e)
        title = ("  ── 引擎活动 (启动/校验) ──" if is_setup
                 else f"  ── 引擎活动 (Round {r}) ──")
        lines = [title]
        for engine in ("Loop", "MAS", "Harness"):
            ev_list = by_engine.get(engine)
            if not ev_list:
                # 启动/校验阶段强制展示三引擎栏（含空栏），确保 Harness/
                # Loop/MAS 架构在运行早期显式可见、不遗漏；空栏标注无活动。
                if is_setup:
                    lines.append(f"    [{engine}]")
                    lines.append("        • （无活动）")
                continue
            lines.append(f"    [{engine}]")
            # 看门狗心跳频率极高（约 0.1s 一次），逐条打印会淹没面板；
            # 折叠为一行（业界标准：聚合而非逐条），其余事件仍逐条可见。
            if engine == "Harness":
                hb = [e for e in ev_list
                      if e.topic == "harness/liveness/heartbeat"]
                others = [e for e in ev_list
                          if e.topic != "harness/liveness/heartbeat"]
                if hb:
                    idles = [e.payload.get("idle") for e in hb
                             if isinstance(e.payload.get("idle"), (int, float))]
                    lo = min(idles) if idles else None
                    hi = max(idles) if idles else None
                    span = (f" (idle {lo:.1f}s→{hi:.1f}s)"
                            if lo is not None else "")
                    lines.append(f"        • 看门狗心跳 ×{len(hb)}{span}")
                for e in others:
                    lines.append(f"        • {e.summary}")
            else:
                for e in ev_list:
                    lines.append(f"        • {e.summary}")
        return "\n".join(lines)

    def print_panel(self, r: Any) -> None:
        text = self.panel_for_round(r)
        if text:
            print(text, flush=True)

    def print_setup_panel(self) -> None:
        """打印启动/校验阶段的独立面板（round=0 的网关活动）。

        幂等：仅在首次有内容时打印一次（同一次运行内重复调用安全）。
        """
        if self._setup_printed:
            return
        text = self.panel_for_round(0)
        if text:
            self._setup_printed = True
            print(text, flush=True)

    # ---- 跨轮聚合报告 ------------------------------------------------
    def per_engine_counts(self) -> Dict[str, int]:
        return dict(self._engine_counts)

    def votes_summary(self) -> List[Dict[str, Any]]:
        """聚合各 Advisor 的投票（MAS -> Loop 的风险/置信）。"""
        by_role: Dict[str, List[dict]] = {}
        for e in self._events:
            if e.topic == "agent/vote/reply":
                role = e.payload.get("role", "?")
                by_role.setdefault(role, []).append(e.payload)
        out = []
        for role, votes in by_role.items():
            risks = [float(v.get("risk", 0) or 0) for v in votes]
            confs = [float(v.get("confidence", 0) or 0) for v in votes]
            out.append({
                "role": role,
                "rounds": len(votes),
                "avg_risk": sum(risks) / len(risks) if risks else 0.0,
                "avg_conf": sum(confs) / len(confs) if confs else 0.0,
            })
        return out

    def watchdog_summary(self) -> Dict[str, Any]:
        beats = [e for e in self._events if e.topic == "harness/liveness/heartbeat"]
        last_idle = beats[-1].payload.get("idle") if beats else None
        return {
            "heartbeats": self._hb_total,
            "aborts": self._abort_total,
            "last_idle_s": (round(last_idle, 1)
                            if isinstance(last_idle, (int, float)) else None),
        }

    def governance_summary(self) -> Optional[Dict[str, Any]]:
        facts = [e for e in self._events
                 if e.topic == "harness/fact/governance.decision"]
        if not facts:
            return None
        allowed = sum(1 for e in facts if e.payload.get("allowed"))
        return {"decisions": len(facts), "allowed": allowed,
                "denied": len(facts) - allowed}

    def metrics_summary(self) -> Dict[str, int]:
        c: Dict[str, int] = {}
        for e in self._events:
            if e.topic.startswith("harness/metric/"):
                name = e.topic.split("/", 2)[-1]
                c[name] = c.get(name, 0) + 1
        return c

    def rounds_summary(self) -> List[Dict[str, Any]]:
        out = []
        for e in self._events:
            if e.topic == "loop/done":
                out.append({
                    "round": e.round,
                    "verdict": e.payload.get("verdict"),
                    "risk": e.payload.get("risk"),
                    "recover_time": e.payload.get("recover_time"),
                })
        return out

    def report(self) -> Dict[str, Any]:
        """跨轮聚合报告（机器可读字典）。"""
        return {
            "total_events": len(self._events),
            "per_engine_counts": self.per_engine_counts(),
            "rounds": self.rounds_summary(),
            "votes": self.votes_summary(),
            "watchdog": self.watchdog_summary(),
            "governance": self.governance_summary(),
            "metrics": self.metrics_summary(),
        }

    # ---- 导出 --------------------------------------------------------
    def export_json(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.report(), f, ensure_ascii=False, indent=2, default=str)

    def export_html(self, path: str) -> None:
        """导出零依赖 HTML 报告（含引擎分布 / 投票 / 看门狗 / 治理 / 每轮面板）。"""
        rep = self.report()
        rows = "".join(
            f"<tr><td>{engine}</td><td>{cnt}</td></tr>"
            for engine, cnt in rep["per_engine_counts"].items()
        )
        engine_tbl = "<table border='1'>" + rows + "</table>"
        vrows = "".join(
            f"<tr><td>{v['role']}</td><td>{v['rounds']}</td>"
            f"<td>{v['avg_risk']:.1f}</td><td>{v['avg_conf']:.2f}</td></tr>"
            for v in rep["votes"]
        ) or "<tr><td colspan='4'>无投票</td></tr>"
        vote_tbl = ("<table border='1'><tr><th>Advisor</th><th>轮数</th>"
                    "<th>平均风险</th><th>平均置信</th></tr>" + vrows + "</table>")
        wd = rep["watchdog"]
        gov = rep["governance"]
        gov_txt = (f"决策 {gov['decisions']} "
                   f"(放行 {gov['allowed']}/拒绝 {gov['denied']})") if gov else "未挂载"
        panels = []
        # 启动/校验阶段（round=0）单独特立面板：因 round=0 为 falsy，
        # 不进入下方轮次循环，需在此显式加入。
        setup_panel = self.panel_for_round(0)
        if setup_panel:
            sp = (setup_panel.replace("&", "&amp;").replace("<", "&lt;")
                  .replace("\n", "<br>").replace(" ", "&nbsp;"))
            panels.append("<h4>启动/校验</h4><pre>" + sp + "</pre>")
        for r in sorted({e.round for e in self._events
                         if e.round not in (0, "0") and e.round}):
            p = (self.panel_for_round(r)
                 .replace("&", "&amp;").replace("<", "&lt;")
                 .replace("\n", "<br>").replace(" ", "&nbsp;"))
            panels.append(f"<h4>Round {r}</h4><pre>{p}</pre>")
        html = (
            "<!doctype html><html lang='zh'><head><meta charset='utf-8'>"
            "<title>三引擎架构观测报告</title></head><body>"
            "<h1>三引擎架构观测报告</h1>"
            f"<p>总事件数: {rep['total_events']}</p>"
            "<h2>引擎事件分布</h2>" + engine_tbl +
            "<h2>投票汇总 (MAS → Loop)</h2>" + vote_tbl +
            "<h2>看门狗 (Harness)</h2>"
            f"<p>心跳 {wd['heartbeats']} 次, 中止 {wd['aborts']} 次, "
            f"最近 idle={wd['last_idle_s']}s</p>"
            "<h2>治理决策 (Harness)</h2><p>" + gov_txt + "</p>"
            "<h2>每轮引擎活动</h2>" + "".join(panels) +
            "</body></html>"
        )
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)


__all__ = ["EngineBusTracer", "EngineEvent", "engine_of"]
