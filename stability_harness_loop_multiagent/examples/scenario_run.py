"""场景化稳定性运行入口（CLI）。

把《门禁对讲通用稳定性用例集》里的任意一条用例写成 YAML 后，用本脚本直接运行，
无需改任何代码：

  # 真实运行（会按 YAML 连接设备；敏感信息走环境变量）
  export HIK_PASSWORD=xxx
  python -m stability_harness_loop_multiagent.examples.scenario_run \\
      --scenario configs/stability_0001_reboot.yaml --rounds 3

  # 长巡类用例（网络/硬件状态）：带截止时间，越过即停止（NT）
  python -m stability_harness_loop_multiagent.examples.scenario_run \\
      --scenario configs/stability_0009_wired_network.yaml

新增用例 = 复制 configs/scenario_template.yaml 改字段即可，本脚本与框架代码零改动。

注：YAML 中的 ``${HIK_PASSWORD}`` / ``${HIK_HOST}`` 等占位符从环境变量插值。
本入口启动时会自动加载项目根的 ``.env``（不覆盖已设的环境变量），因此把
``HIK_PASSWORD=xxx`` 写进 ``.env`` 即可，无需在 shell 里手动 export。
"""

import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

# 强制 stdout 行缓冲：即便被管道/启动器包裹（非 TTY 块缓冲）也能实时流出，
# 真实设备长回归时不会"跑完才打印"。
try:
    sys.stdout.reconfigure(line_buffering=True)
except (AttributeError, ValueError):
    pass

# 启动时加载 .env（不覆盖已设的环境变量）。YAML 中的 ${HIK_PASSWORD} / ${HIK_HOST}
# 等占位符依赖此处的环境变量；不加载会导致密码为空、DigestAuth 协商失败、设备 401。
# 复用 llm._load_dotenv 避免重复实现。
from stability_harness_loop_multiagent.business.hikvision.llm import _load_dotenv
_load_dotenv()

from stability_harness_loop_multiagent.business.hikvision.scenario_schema import (
    from_yaml,
)
from stability_harness_loop_multiagent.business.hikvision.scenario_runner import (
    render_summary,
    run_scenario,
)

# rich 可选依赖（[examples] extras）：缺失时回退标准库 print
try:
    from rich.console import Console
    _HAS_RICH = True
except ImportError:  # pragma: no cover
    _HAS_RICH = False


def _print_header(scenario, verbose: bool) -> None:
    """打印场景头部信息（rich 可用时用 cyan 标题色，否则 print）。"""
    if _HAS_RICH:
        console = Console()
        console.print(f"[bold cyan][场景][/bold cyan] {scenario.id} - {scenario.name}")
        console.print(f"[cyan][类别][/cyan] {scenario.category or '-'}  "
                      f"压力={scenario.stress.type}  "
                      f"探测={scenario.probe.endpoint} 字段={scenario.probe.field}")
        console.print(f"[cyan][停止][/cyan] max_rounds={scenario.loop.max_rounds}  "
                      f"interval={scenario.loop.interval_seconds}s  "
                      f"deadline={scenario.loop.deadline}  "
                      f"stop_on_na={scenario.loop.stop_on_na}")
        console.print("[cyan][模式][/cyan] 真实设备（按 YAML target 连接）")
        mode = "三引擎 trace（verbose）" if verbose else "逐轮汇总"
        console.print(f"[bold]=== 逐轮输出（{mode}）===[/bold]")
        if verbose:
            # 引擎/角色完整名称图例（不简写）
            console.print("  [dim]引擎:[/dim] "
                          "[cyan]Loop[/cyan] 确定性循环  "
                          "[magenta]MAS[/magenta] 多智能体  "
                          "[green]Harness[/green] 运行时/治理")
            console.print("  [dim]角色:[/dim] "
                          "Loop / Worker / Scribe / Advisor / Verifier / Watchdog")
    else:
        print(f"[场景] {scenario.id} - {scenario.name}", flush=True)
        print(f"[类别] {scenario.category or '-'}  压力={scenario.stress.type}  "
              f"探测={scenario.probe.endpoint} 字段={scenario.probe.field}", flush=True)
        print(f"[停止] max_rounds={scenario.loop.max_rounds}  "
              f"interval={scenario.loop.interval_seconds}s  "
              f"deadline={scenario.loop.deadline}  "
              f"stop_on_na={scenario.loop.stop_on_na}", flush=True)
        print(f"[模式] 真实设备（按 YAML target 连接）", flush=True)
        mode = "三引擎 trace（verbose）" if verbose else "逐轮汇总"
        print(f"=== 逐轮输出（{mode}）===", flush=True)
        if verbose:
            print("  引擎: Loop 确定性循环  MAS 多智能体  Harness 运行时/治理",
                  flush=True)
            print("  角色: Loop / Worker / Scribe / Advisor / Verifier / Watchdog",
                  flush=True)


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="运行一份场景化稳定性用例（YAML 驱动，零代码扩展）",
    )
    p.add_argument("--scenario", required=True, help="场景 YAML 路径")
    p.add_argument("--rounds", type=int, default=None,
                   help="覆盖 loop.max_rounds")
    p.add_argument("--interval", type=float, default=None,
                   help="覆盖 loop.interval_seconds")
    p.add_argument("--timeout", type=float, default=None,
                   help="整体运行超时（秒）")
    p.add_argument("--json", action="store_true",
                   help="以 JSON 形式打印汇总后退出")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="打印三引擎（Loop/MAS/Harness）每一步动作的 trace，"
                        "让框架协作过程显式可见（如 loop/tick → target/acted → "
                        "target/checked → loop/done）")
    return p


def main() -> int:
    args = _build_arg_parser().parse_args()
    scenario = from_yaml(args.scenario)
    if args.rounds is not None:
        scenario.loop.max_rounds = max(1, args.rounds)
    if args.interval is not None:
        scenario.loop.interval_seconds = args.interval

    _print_header(scenario, args.verbose)

    result = asyncio.run(
        run_scenario(scenario, run_timeout=args.timeout, live=True,
                     verbose=args.verbose)
    )
    summary = result["summary"]
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        # spec §6.6：用 rich.table 渲染汇总；无 rich 时回退 print
        render_summary(summary)
    return 0 if (summary['fail'] == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
