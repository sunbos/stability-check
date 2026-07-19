"""场景化稳定性运行入口（CLI）。

把《门禁对讲通用稳定性用例集》里的任意一条用例写成 YAML 后，用本脚本直接运行，
无需改任何代码：

  # dry-run（无真实设备，演示「场景 -> 事实 -> 裁决」链路，全部默认通过）
  python -m stability_harness_loop_multiagent.examples.scenario_run \\
      --scenario configs/stability_0001_reboot.yaml --dry-run

  # 真实运行（会按 YAML 连接设备；敏感信息走环境变量）
  export HIK_PASSWORD=xxx
  python -m stability_harness_loop_multiagent.examples.scenario_run \\
      --scenario configs/stability_0001_reboot.yaml --rounds 3

  # 长巡类用例（网络/硬件状态）：带截止时间，越过即停止（NT）
  python -m stability_harness_loop_multiagent.examples.scenario_run \\
      --scenario configs/stability_0009_wired_network.yaml --dry-run

新增用例 = 复制 configs/scenario_template.yaml 改字段即可，本脚本与框架代码零改动。
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

from stability_harness_loop_multiagent.business.hikvision.scenario_schema import (
    from_yaml,
)
from stability_harness_loop_multiagent.business.hikvision.scenario_runner import (
    run_scenario,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="运行一份场景化稳定性用例（YAML 驱动，零代码扩展）",
    )
    p.add_argument("--scenario", required=True, help="场景 YAML 路径")
    p.add_argument("--dry-run", action="store_true",
                   help="使用内存脚本化适配器，不连接真实设备")
    p.add_argument("--rounds", type=int, default=None,
                   help="覆盖 loop.max_rounds")
    p.add_argument("--interval", type=float, default=None,
                   help="覆盖 loop.interval_seconds")
    p.add_argument("--timeout", type=float, default=None,
                   help="整体运行超时（秒）")
    p.add_argument("--json", action="store_true",
                   help="以 JSON 形式打印汇总后退出")
    return p


def main() -> int:
    args = _build_arg_parser().parse_args()
    scenario = from_yaml(args.scenario)
    if args.rounds is not None:
        scenario.loop.max_rounds = max(1, args.rounds)
    if args.interval is not None:
        scenario.loop.interval_seconds = args.interval

    print(f"[场景] {scenario.id} - {scenario.name}", flush=True)
    print(f"[类别] {scenario.category or '-'}  压力={scenario.stress.type}  "
          f"探测={scenario.probe.endpoint} 字段={scenario.probe.field}", flush=True)
    print(f"[停止] max_rounds={scenario.loop.max_rounds}  "
          f"interval={scenario.loop.interval_seconds}s  "
          f"deadline={scenario.loop.deadline}  stop_on_na={scenario.loop.stop_on_na}", flush=True)
    print(f"[模式] {'dry-run（无设备）' if args.dry_run else '真实设备'}", flush=True)
    print("  运行中（实时逐轮输出）：", flush=True)

    result = asyncio.run(
        run_scenario(scenario, dry_run=args.dry_run, run_timeout=args.timeout,
                     live=True)
    )
    summary = result["summary"]
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print("\n===== 运行汇总 =====")
        print(f"  轮数        : {summary['rounds']}")
        print(f"  裁决分布    : {summary['verdicts']}")
        print(f"  通过/失败/NA: {summary['pass']} / {summary['fail']} / {summary['na']}")
        print(f"  压力失败    : {summary['stress_fail']}")
        print(f"  早停        : {summary['stop_reason'] or '无（正常完成）'}")
        # 结论：仅「断言失败」算未通过；因截止时间(NT)/NA 的有意早停不算失败。
        ok = (summary['fail'] == 0)
        print(f"  结论        : {'通过' if ok else '未通过'}"
              f"{'（含早停: ' + summary['stop_reason'] + '）' if summary['stop_reason'] else ''}")
    return 0 if (summary['fail'] == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
