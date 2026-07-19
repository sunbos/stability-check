"""稳定性用例统一入口：1 marker = 1 用例，parametrize 遍历所有用例。

所有用例必须真实设备执行（HIK_HOST），无 HIK_HOST 时整个测试函数 skip
（spec §7 测试策略：不 fake 不 mock，无环境 skip 不强行造数据）。

用法：
    pytest -m Stability_0001                  # 跑单条用例（真机）
    pytest -m "重启稳定性"                     # 跑类别下所有用例（真机）
    pytest -m "L2"                            # 跑等级 L2 所有用例（真机）
    pytest -m "重启稳定性 and L2"             # 组合筛选（真机）
    pytest --collect-only test_stability_scenario.py  # 只看不跑（无 HIK_HOST 也可）

断言逻辑（与 spec §4 事实独裁一致）：
- 正常终止：abort_reason 以 "已到达 max_rounds" 开头（ControlLoop 在 max_rounds
  到达时调用 _halt 设置 aborted=True，属正常完成）；其他 abort_reason 才算异常中止
- 无 fail 轮（fail=0）：事实独裁下 fail=0 即所有轮 pass/warn/na
- 至少跑 1 轮（rounds>0）：防止空跑误判通过
"""
import os

import pytest

# conftest.py 在 pytest 收集前已加载，且 tests/ 在 sys.path（rootdir 模式），
# 可直接 `from conftest import`。SCENARIO_IDS / SCENARIO_MAP 在 conftest 模块级
# 填充（_scan_scenarios()），import 时即可用。
from conftest import SCENARIO_IDS, SCENARIO_MAP  # noqa: E402

from stability_harness_loop_multiagent.business.hikvision.scenario_runner import (
    run_scenario,
)
from stability_harness_loop_multiagent.business.hikvision.scenario_schema import (
    from_yaml,
)


@pytest.mark.parametrize("scenario_id", SCENARIO_IDS)
async def test_stability_scenario(scenario_id: str):
    """单条稳定性用例：加载 YAML -> run_scenario -> 断言无 fail 且正常终止。"""
    if not os.environ.get("HIK_HOST"):
        pytest.skip(
            f"无 HIK_HOST，用例 {scenario_id} 需真实设备（spec §7：不 fake 不 mock）"
        )

    yaml_path = SCENARIO_MAP[scenario_id]
    scenario = from_yaml(str(yaml_path))
    result = await run_scenario(scenario)
    summary = result["summary"]

    abort_reason = summary.get("abort_reason", "") or ""
    normal_finish = abort_reason.startswith("已到达 max_rounds")
    assert normal_finish, (
        f"用例 {scenario_id} 异常中止："
        f"abort_reason={abort_reason or 'unknown'} "
        f"stop_reason={summary.get('stop_reason', 'unknown')}"
    )
    assert summary["fail"] == 0, (
        f"用例 {scenario_id} 有 {summary['fail']} 轮 fail "
        f"(verdicts={summary['verdicts']})"
    )
    assert summary["rounds"] > 0, (
        f"用例 {scenario_id} 未执行任何轮次（可能 pre_loop_setup 异常）"
    )

