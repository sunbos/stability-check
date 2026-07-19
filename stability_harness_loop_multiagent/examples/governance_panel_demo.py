"""治理观测面板 —— 真实拉取演示（request / reply）+ 趋势报告。

自包含、与领域无关、不依赖海康或任何真实设备。它演示：

1. 通过 ``GovernanceAgent`` 网关处理若干轮 ``harness/govern/request``，
   网关在决策点发出结构化事实 ``governance.decision``（kind=fact）。
2. ``GovernancePanelAgent``（Observer）订阅该事实并聚合成面板。
3. 一个外部消费者通过发布 ``governance/panel/request`` 经总线**拉取**当前
   面板（面板回复 ``governance/panel``，内含聚合 ``panel`` 与时间序列 ``timeseries``），
   拿到后调用 ``render()`` 打印 dashboard，并用 ``timeseries`` 生成趋势报告。

趋势报告：优先用 ``plotly``（若已安装）输出交互式 HTML；缺失时回退为
零依赖的 CSV（同样完全来自总线拉取，不经任何对象引用）。

同时演示 ``DeniedOp`` 的 ``match`` 维度：``exact`` / ``prefix`` / ``suffix`` /
``contains`` / ``regex`` / 通配 ``"*"``。

直接运行：  python stability_harness_loop_multiagent/examples/governance_panel_demo.py
"""

import asyncio
import os
import sys

# 当作为裸脚本从仓库根目录运行时让 stability_harness_loop_multiagent 可导入。
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from stability_harness_loop_multiagent.core.agent import AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.harness.governance import (
    AccessControl,
    DeniedOp,
    Governance,
    GovernanceAgent,
)
from stability_harness_loop_multiagent.harness.telemetry import MemorySink, Telemetry
from stability_harness_loop_multiagent.multi_agent.observers.gov_panel import (
    GovernancePanelAgent,
)


class PanelPuller:
    """经总线拉取治理面板的演示消费者。

    订阅 ``governance/panel``；发布 ``governance/panel/request`` 后，面板会把当前
    聚合面板作为 ``governance/panel`` 回发，本对象收集最近一份。
    """

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.panels: list = []

    def attach(self) -> None:
        self.bus.subscribe("governance/panel", self._on_panel)

    def _on_panel(self, _topic: str, message) -> None:
        self.panels.append(message)

    async def pull(self, req_id: str = "demo-1") -> None:
        # 发送即忘的请求；面板会回发 governance/panel（异步派发）。
        await self.bus.publish_and_wait(
            "governance/panel/request", {"req_id": req_id}
        )
        # 让面板回发的 governance/panel 也派发到本订阅者。
        await asyncio.sleep(0.02)


def build_trend_report(ts: dict, prefix: str = "governance_trend") -> str:
    """由总线拉取的 ``timeseries`` 生成趋势报告。

    优先用 ``plotly`` 输出交互式 HTML（含累计放行/拒绝/超时趋势 + 每轮被拒操作数柱状图）；
    若 ``plotly`` 不可用，则回退为零依赖的 CSV。二者均写入系统临时目录，返回最终产物路径。
    """
    import csv
    import tempfile
    from datetime import datetime

    out_dir = tempfile.gettempdir()
    step = ts.get("step", [])
    denied_per_step = [len(x) for x in ts.get("denied_ops", [])]

    # 零依赖 CSV（始终产出，作为稳定可读产物）。
    csv_path = os.path.join(out_dir, f"{prefix}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["step", "ts", "round", "allowed", "denied_op_count",
                    "cum_allowed", "cum_denied", "cum_fail_closed"])
        for i in range(len(step)):
            w.writerow([
                step[i],
                f"{ts['ts'][i]:.3f}",
                ts["rounds"][i],
                ts["allowed"][i],
                denied_per_step[i],
                ts["cum_allowed"][i],
                ts["cum_denied"][i],
                ts["cum_fail_closed"][i],
            ])
    print(f"[趋势报告] CSV（零依赖）: {csv_path}")

    # 优先 plotly 交互式 HTML。
    try:
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go
    except Exception:  # noqa: BLE001 - plotly 缺失时回退 CSV
        print("[趋势报告] 未安装 plotly，已回退 CSV。")
        return csv_path

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        subplot_titles=("累计 放行 / 拒绝 / fail-closed 趋势",
                        "每轮被拒操作数"),
        vertical_spacing=0.12,
    )
    fig.add_trace(
        go.Scatter(x=step, y=ts["cum_allowed"], name="累计放行",
                   mode="lines+markers", line=dict(color="#2ca02c")),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=step, y=ts["cum_denied"], name="累计拒绝(含fail-closed)",
                   mode="lines+markers", line=dict(color="#d62728")),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=step, y=ts["cum_fail_closed"], name="累计 fail-closed",
                   mode="lines+markers", line=dict(color="#ff7f0e", dash="dot")),
        row=1, col=1,
    )
    fig.add_trace(
        go.Bar(x=step, y=denied_per_step, name="每轮被拒操作数",
               marker_color="#9467bd"),
        row=2, col=1,
    )
    fig.update_xaxes(title_text="决策序号 (step)", row=2, col=1)
    fig.update_yaxes(title_text="累计数", row=1, col=1)
    fig.update_yaxes(title_text="被拒操作数", row=2, col=1)
    fig.update_layout(
        title=f"治理观测趋势报告 · 生成于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        height=720, template="plotly_white", legend=dict(orientation="h"),
    )
    html_path = os.path.join(out_dir, f"{prefix}.html")
    fig.write_html(html_path)
    print(f"[趋势报告] HTML（plotly 交互式）: {html_path}")
    return html_path


async def _main() -> None:
    bus = EventBus()
    # Telemetry 连着同一总线 → 治理决策事实真正发到 harness/fact/governance.decision。
    tel = Telemetry(bus=bus, sinks=[MemorySink()])

    gov = Governance(
        access=AccessControl(policy={"hik": {"door-test": "*"},
                                     "risk": {"door-test": "*"}}),
        # 多维拒绝规则，覆盖五种匹配方式 + 通配角色：
        denied_operations=[
            DeniedOp(op="reboot", role="hik"),                 # exact + 角色维度
            DeniedOp(op="diag_", match="prefix"),              # 前缀：拒绝 diag_* 系列
            DeniedOp(op="_now", match="suffix"),               # 后缀：拒绝 *_now 系列
            DeniedOp(op="temp", match="contains"),             # 子串：拒绝含 temp 的操作
            DeniedOp(op=r"force_.*_now", match="regex"),       # 正则全匹配
            DeniedOp(op="snapshot", role="*"),                 # 通配角色
        ],
        telemetry=tel,
    )
    gate = GovernanceAgent(bus, gov)
    panel = GovernancePanelAgent(
        bus, AgentSpec(id="o2", role="gov-panel",
                       subscriptions=["harness/fact/governance.decision",
                                      "governance/panel/request"]),
    )
    puller = PanelPuller(bus)
    await gate.start()
    await panel.start()
    puller.attach()

    # 模拟 6 轮治理决策（每轮带不同操作集，触发不同拒绝规则）。
    rounds = [
        {"role": "hik", "capability": "door-test", "operation": "round",
         "operations": ["reboot", "remote_open_door", "diag_health"], "round": 1},
        {"role": "risk", "capability": "door-test", "operation": "round",
         "operations": ["snapshot", "force_reboot_now", "remote_open_door"], "round": 2},
        {"role": "hik", "capability": "door-test", "operation": "round",
         "operations": ["diag_net", "reboot", "flush_temp"], "round": 3},
        {"role": "risk", "capability": "door-test", "operation": "round",
         "operations": ["diag_cpu", "read_temp_now", "remote_open_door"], "round": 4},
        {"role": "hik", "capability": "door-test", "operation": "round",
         "operations": ["snapshot", "reboot"], "round": 5},
        {"role": "risk", "capability": "door-test", "operation": "round",
         "operations": ["diag_health", "force_push_now"], "round": 6},
    ]
    for r in rounds:
        await bus.publish_and_wait("harness/govern/request", r)
        await asyncio.sleep(0.02)  # 让事实派发到面板

    # 真实拉取面板：经总线 request/reply，而非直接读对象引用。
    await puller.pull(req_id="demo-1")

    assert puller.panels, "未收到治理面板回复（governance/panel）"
    pulled = puller.panels[-1]["panel"]
    req_id_back = puller.panels[-1].get("req_id")
    assert req_id_back == "demo-1", "面板回复应回带 req_id"

    print(panel.render())
    print(f"\n[拉取] req_id={req_id_back} 经总线收到面板聚合：{pulled}")

    # 正确性断言（演示面板确实聚合了多维拒绝规则）。
    assert pulled["total"] == 6, pulled
    assert pulled["allowed"] == 6, "6 轮都通过轮级访问闸门，应全部放行"
    assert pulled["denied_ops_by_op"].get("reboot") == 3, pulled
    assert pulled["denied_ops_by_op"].get("diag_health") == 2, pulled
    assert pulled["denied_ops_by_op"].get("snapshot") == 2, pulled
    assert pulled["denied_ops_by_op"].get("force_reboot_now") == 1, pulled
    assert pulled["denied_ops_by_op"].get("diag_net") == 1, pulled
    assert pulled["denied_ops_by_op"].get("flush_temp") == 1, pulled
    assert pulled["denied_ops_by_op"].get("diag_cpu") == 1, pulled
    assert pulled["denied_ops_by_op"].get("read_temp_now") == 1, pulled
    assert pulled["denied_ops_by_op"].get("force_push_now") == 1, pulled
    assert pulled["rounds_observed"] == [1, 2, 3, 4, 5, 6], pulled
    assert pulled["by_role"] == {"hik": 3, "risk": 3}, pulled

    # 趋势报告：完全来自总线拉取的 timeseries，不经任何对象引用。
    ts = puller.panels[-1].get("timeseries")
    assert ts is not None and len(ts["step"]) == 6, "面板回复应携带时间序列"
    report_path = build_trend_report(ts, prefix="governance_trend_demo")
    print(f"\n[趋势报告] 已生成：{report_path}")

    print("\nALL GOVERNANCE PANEL DEMO ASSERTIONS PASSED")


if __name__ == "__main__":
    asyncio.run(_main())
