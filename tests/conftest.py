"""pytest 配置与共享 fixture。

仅使用标准库 os / sys。将 tests/agents 加入 sys.path，使得其中的同级模块
（config / device_client / ...）可被以绝对导入方式引用。
"""

import os
import sys

import pytest

# 将 agents 目录加入 sys.path，使其内部 `from device_client import ...` 可解析
_AGENTS_DIR = os.path.join(os.path.dirname(__file__), "agents")
if _AGENTS_DIR not in sys.path:
    sys.path.insert(0, _AGENTS_DIR)

from config import load_config_from_env, Baseline  # noqa: E402
from device_client import DeviceClient, DEFAULT_FIELDS  # noqa: E402


@pytest.fixture
def run_config():
    """从环境变量构建运行配置。"""
    return load_config_from_env()


@pytest.fixture
def baseline(run_config):
    """抓取设备当前工作状态（AcsWorkStatus 快照）作为基线，填入 RunContext.baseline。

    返回的 Baseline(status=snapshot, fields=DEFAULT_FIELDS) 可直接赋给
    RunContext.baseline（StatusCheckAgent 兼容 Baseline 实例 / dict 两种形态）。

    抓取失败（如设备不可达 / 认证失败）直接在 fixture 内抛错，
    使对应测试被标记为 error 而非 fail。
    """
    client = DeviceClient(run_config.host, run_config.user, run_config.password)
    snapshot = client.get_work_status()  # 失败则向上抛出 -> pytest error
    return Baseline(status=snapshot, fields=list(DEFAULT_FIELDS))
