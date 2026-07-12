"""稳定性拷机会话测试。

整轮拷机作为单个会话运行（不使用 parametrize）。仅使用标准库 asyncio / os / sys。
"""

import os
import sys
import asyncio

import pytest

# 将 agents 目录加入 sys.path，使其内部模块可被以绝对导入方式引用
_AGENTS_DIR = os.path.join(os.path.dirname(__file__), "agents")
if _AGENTS_DIR not in sys.path:
    sys.path.insert(0, _AGENTS_DIR)

from supervisor import Supervisor  # noqa: E402
from config import load_config_from_env  # noqa: E402
from device_client import DeviceClient  # noqa: E402
from strategy import Strategy  # noqa: E402
from report import Reporter  # noqa: E402


def test_burnin_session(run_config, baseline):
    """执行一轮完整拷机会话并断言未被失败阈值中止。"""
    client = DeviceClient(run_config.host, run_config.user, run_config.password)
    strategy = Strategy(run_config.strategy_text)
    reporter = Reporter()

    asyncio.run(
        Supervisor(run_config, client, baseline, strategy, reporter).run()
    )

    print("Burn-in summary:", reporter.summary())

    assert not reporter.aborted, (
        f"Burn-in aborted by failure threshold: {reporter.summary()}"
    )
