"""稳定性检查：状态比对代理（仅使用标准库）。

提供 check_status(client, baseline, extra_asserts=None) 用于把设备当前
工作状态与基线快照逐字段比较，并支持额外断言。
"""

from typing import Optional


def check_status(client, baseline, extra_asserts=None) -> tuple[bool, dict]:
    """对比设备当前状态与基线状态。

    参数:
      client          - DeviceClient 实例（需提供 get_work_status()）。
      baseline        - Baseline 实例，含 .fields（比对字段列表）与 .status（基线状态 dict）。
      extra_asserts   - 可选，list of (field, expected)，对每个 (field, expected)
                        比较当前状态中该字段是否等于 expected。

    返回:
      (all_ok, diff)
      all_ok 为 True 表示无差异；diff 为 {字段名: {"expected":..., "actual":...}}。

    任何异常（如网络/解析错误）向上抛出，不在此处吞掉。
    """
    current = client.get_work_status()
    diff: dict = {}

    # 1) 基线字段逐项比较（字段值为列表，按元素逐项比较）
    for field_name in baseline.fields:
        expected = baseline.status.get(field_name)
        actual = current.get(field_name)
        if expected != actual:
            diff[field_name] = {"expected": expected, "actual": actual}

    # 2) 额外断言比较
    if extra_asserts:
        for field_name, expected in extra_asserts:
            actual = current.get(field_name)
            if actual != expected:
                diff[field_name] = {"expected": expected, "actual": actual}

    return (len(diff) == 0, diff)
