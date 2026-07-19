"""examples 共享终端报告渲染（rich 优先，标准库回退）。

目标：让所有示例脚本（hikvision_real_env / generic_harness / ...）的终端输出
风格全局统一——同一套对齐、表格、CJK 宽度感知逻辑只维护一份。

- rich 为「示例可选依赖」（pyproject: examples = ["rich>=13"]），装了即用其
  表格/边框渲染（自动 CJK 宽度感知、列对齐）；未装则回退标准库宽度感知对齐。
- 框架核心包（stability_harness_loop_multiagent/）保持零第三方依赖，本模块
  只被 examples/ 下的脚本 import。
"""

import unicodedata

try:
    from rich.console import Console
    from rich.markup import escape as _rich_escape
    from rich.table import Table
    from rich import box

    _CONSOLE = Console()
    _RICH = True
except Exception:  # noqa: BLE001 - 未安装 rich 时优雅回退
    _CONSOLE = None
    _RICH = False
    _rich_escape = lambda s: s
    Table = None
    box = None


# 裁决结论中文映射（通用，所有示例共用）。
_VERDICT_CN = {
    "pass": "通过", "warn": "警告", "recheck": "复检",
    "fail": "失败", "abort": "中止",
}

# 裁决配色（rich 标记），提升横幅可读性：通过=绿 失败/中止=红 警告/复检=黄。
_VCOLOR = {
    "pass": "green", "fail": "red", "warn": "yellow",
    "recheck": "yellow", "abort": "red",
}


def _disp_len(s: str) -> int:
    """字符串的终端显示宽度：CJK 全角字符计 2，其余计 1（标准库即可实现）。"""
    return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1
               for c in str(s))


def _pad(s: str, width: int, align: str = "<") -> str:
    """按显示宽度对齐填充（解决中文全角导致的列错位）。"""
    s = str(s)
    gap = max(width - _disp_len(s), 0)
    return s + " " * gap if align == "<" else " " * gap + s


def _print_header(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"=== {title}")
    print("=" * 72)


# _kv 先缓冲，待本区块所有 _kv 调用完再统一渲染（rich 表格 / 标准库对齐二选一）。
_KV_ROWS: list = []


def _kv(label: str, value: str) -> None:
    """收集一行「键 / 值」，延迟到 _flush_kv() 统一渲染。"""
    _KV_ROWS.append((str(label), str(value)))


def _flush_kv() -> None:
    """渲染并清空已缓冲的 _kv 行。

    - 装了 rich：用无边框双列表格，自动 CJK 宽度感知与列对齐；
    - 未装：回退到标准库 _pad 宽度感知对齐。
    值经 rich.markup.escape 转义，避免内容里的方括号被当成样式标记。
    """
    global _KV_ROWS
    rows = _KV_ROWS
    _KV_ROWS = []
    if not rows:
        return
    if _RICH:
        table = Table(show_header=False, show_edge=False, padding=(0, 2))
        table.add_column("key", style="bold cyan", no_wrap=True)
        table.add_column("value")
        for k, v in rows:
            table.add_row(_rich_escape(k), _rich_escape(v))
        _CONSOLE.print(table)
    else:
        w = max(_disp_len(k) for k, _ in rows)
        for k, v in rows:
            # 支持多行值（如 Worker 状态树）：首行与 key 对齐，续行缩进到
            # value 列，保证树状内容在终端也清晰对齐。
            vlines = str(v).split("\n")
            print(f"  {_pad(k, w)}  {vlines[0]}")
            for extra in vlines[1:]:
                print(f"  {' ' * w}  {extra}")


def _print_round(round_no, total_rounds, verdict, risk, facts,
                 *, quiet: bool = False) -> None:
    """通用单轮打印：结论横幅 + 事实清单（rich 表格 / 标准库回退）。

    不带阶段时间线（时间线为 hikvision 领域专属，由 hikvision_real_env 自行
    在其 _print_round 中用本模块的原语扩展）。事实键名按原样展示。
    """
    vmark = {"pass": "PASS", "fail": "FAIL", "warn": "WARN",
             "recheck": "RECHECK", "abort": "ABORT"}.get(verdict, str(verdict).upper())
    color = _VCOLOR.get(verdict, "white")
    if _RICH:
        _CONSOLE.rule("  ".join([
            f"ROUND {round_no}/{total_rounds}",
            f"[[bold {color}]{vmark}[/]]",
            f"risk={float(risk):.1f}",
        ]))
        if quiet:
            return
        if facts:
            ft = Table(show_header=False, show_edge=False, padding=(0, 2),
                       title="事实", title_justify="left")
            ft.add_column("fact", style="bold")
            ft.add_column("value")
            for k, v in facts.items():
                ft.add_row(_rich_escape(str(k)), _rich_escape(str(v)))
            _CONSOLE.print(ft)
        return
    # 标准库回退路径（宽度感知对齐）
    print("\n" + "─" * 72)
    head = f"ROUND {round_no}/{total_rounds}"
    line = f"  {head:<14} {vmark:<8} risk={float(risk):.1f}"
    print(line)
    print("─" * 72)
    if quiet:
        return
    if facts:
        print("  事实:")
        for k, v in facts.items():
            print(f"    - {k}: {v}")


# ---------------------------------------------------------------------------
# 公共 API（hikvision 稳定性测试场景用）：单轮报告 / 最终汇总 / 分节标题。
# 与上方带下划线前缀的内部辅助（_kv / _flush_kv / _print_round）并存：
# 内部辅助面向 examples/generic_harness.py 的细粒度组合原语，公共 API
# 面向 hikvision 稳定性测试场景的「一行一个表格」接口（plan §PR5 Task 5.1）。
# ---------------------------------------------------------------------------


def print_round_report(round_no: int, verdict: str, facts: dict,
                       remote_open: str = "N/A", lock_open: str = "N/A",
                       lock_closed: str = "N/A", recovered: str = "N/A") -> None:
    """打印单轮稳定性测试报告（rich 表格 / 标准库回退）。

    与 plan §PR5 Task 5.1 一致：固定 6 行指标（Verdict / probe_ok /
    remote_open / lock_open / lock_closed / recovered），调用方零排版负担。
    Verdict 行按通过/失败配色高亮，便于在长日志中一眼定位异常轮次。
    """
    vmark = {"pass": "PASS", "fail": "FAIL", "warn": "WARN",
             "recheck": "RECHECK", "abort": "ABORT"}.get(verdict, str(verdict).upper())
    color = _VCOLOR.get(verdict, "white")
    probe_ok = str(facts.get("probe_ok", "N/A")) if isinstance(facts, dict) else "N/A"
    rows = [
        ("Verdict", vmark),
        ("probe_ok", probe_ok),
        ("remote_open", str(remote_open)),
        ("lock_open", str(lock_open)),
        ("lock_closed", str(lock_closed)),
        ("recovered", str(recovered)),
    ]
    if _RICH:
        table = Table(title=f"Round {round_no}", show_lines=True, box=box.SQUARE)
        table.add_column("指标", style="cyan", no_wrap=True)
        table.add_column("值", style="magenta")
        for k, v in rows:
            # Verdict 行带颜色高亮（直观区分通过/失败/中止）
            if k == "Verdict":
                table.add_row(
                    _rich_escape(k),
                    f"[bold {color}]{_rich_escape(v)}[/bold {color}]",
                )
            else:
                table.add_row(_rich_escape(k), _rich_escape(v))
        _CONSOLE.print(table)
        return
    # 标准库回退（与上方 _print_round 风格一致）
    _print_header(f"ROUND {round_no}")
    w = max(_disp_len(k) for k, _ in rows)
    for k, v in rows:
        print(f"  {_pad(k, w)}  {v}")


def print_final_summary(rounds: list[dict]) -> None:
    """打印最终汇总表（rich 表格 / 标准库回退）。

    每行一轮，列：轮次 / Verdict / remote_open / lock_open / lock_closed /
    recovered。Verdict 单元格按通过/失败配色高亮，便于在多轮结果中一眼
    发现异常轮次。
    """
    headers = ["轮次", "Verdict", "remote_open", "lock_open",
               "lock_closed", "recovered"]
    if _RICH:
        table = Table(title="稳定性测试结果汇总", show_lines=True, box=box.SQUARE)
        table.add_column(headers[0], justify="right", style="cyan", no_wrap=True)
        table.add_column(headers[1], style="magenta")
        for h in headers[2:]:
            table.add_column(h)
        for r in rounds:
            v = r.get("verdict", "?")
            color = _VCOLOR.get(v, "white")
            vmark = {"pass": "PASS", "fail": "FAIL", "warn": "WARN",
                     "recheck": "RECHECK", "abort": "ABORT"}.get(
                v, str(v).upper())
            table.add_row(
                str(r.get("round", "?")),
                f"[bold {color}]{vmark}[/bold {color}]",
                str(r.get("remote_open", "N/A")),
                str(r.get("lock_open", "N/A")),
                str(r.get("lock_closed", "N/A")),
                str(r.get("recovered", "N/A")),
            )
        _CONSOLE.print(table)
        return
    # 标准库回退
    _print_header("SUMMARY · 稳定性测试结果汇总")
    widths = [_disp_len(h) for h in headers]
    print("  " + "  ".join(_pad(h, w) for h, w in zip(headers, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for r in rounds:
        cells = [
            str(r.get("round", "?")),
            str(r.get("verdict", "?")),
            str(r.get("remote_open", "N/A")),
            str(r.get("lock_open", "N/A")),
            str(r.get("lock_closed", "N/A")),
            str(r.get("recovered", "N/A")),
        ]
        print("  " + "  ".join(_pad(c, w) for c, w in zip(cells, widths)))


def print_section(title: str) -> None:
    """打印分节标题（rich rule / 标准库回退）。

    用于在长输出中划分阶段（如「Pre-loop Setup」「Round 1」「Summary」），
    与 _print_header 区别在于：print_section 是公共 API（无下划线前缀），
    由 hikvision 稳定性测试场景调用；_print_header 是内部辅助。
    """
    if _RICH:
        _CONSOLE.rule(f"[bold cyan]{_rich_escape(title)}[/bold cyan]")
        return
    _print_header(title)
