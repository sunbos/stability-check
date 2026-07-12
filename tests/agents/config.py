"""运行配置与共享数据类型（仅使用标准库 dataclasses / os）。

提供：
  - RunConfig            燃烧测试的运行配置
  - load_config_from_env 从环境变量构建 RunConfig
  - RoundResult          单轮测试结果
  - Baseline             基线状态快照
"""

from dataclasses import dataclass, field
import os

from device_client import DEFAULT_FIELDS


@dataclass
class RunConfig:
    host: str
    user: str
    password: str
    max_rounds: int = 0            # 0 = 无限轮次
    max_duration: float = 0.0      # 0 = 无限时长（秒）
    base_interval: float = 60.0
    interval_min: float = 30.0
    interval_max: float = 600.0
    recover_timeout: float = 180.0
    fail_threshold: int = 5
    fail_consecutive: int = 3
    strategy_text: str = ""
    k: float = 1.5
    event_window: float = 30.0


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name, "")
    val = (val or "").strip()
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name, "")
    val = (val or "").strip()
    if not val:
        return default
    try:
        return float(val)
    except ValueError:
        return default


def load_config_from_env() -> RunConfig:
    """从环境变量读取配置，缺失/空取默认值。

    HOST / USER / PASSWORD 可覆盖基础连接信息。
    """
    host = os.environ.get("BURNIN_HOST", os.environ.get("HOST", "192.168.3.33"))
    user = os.environ.get("BURNIN_USER", os.environ.get("USER", "admin"))
    password = os.environ.get("BURNIN_PASSWORD", os.environ.get("PASSWORD", "121212.."))

    return RunConfig(
        host=host,
        user=user,
        password=password,
        max_rounds=_env_int("BURNIN_MAX_ROUNDS", 0),
        max_duration=_env_float("BURNIN_MAX_DURATION", 0.0),
        base_interval=_env_float("BURNIN_BASE_INTERVAL", 60.0),
        interval_min=_env_float("BURNIN_INTERVAL_MIN", 30.0),
        interval_max=_env_float("BURNIN_INTERVAL_MAX", 600.0),
        recover_timeout=_env_float("BURNIN_RECOVER_TIMEOUT", 180.0),
        fail_threshold=_env_int("BURNIN_FAIL_THRESHOLD", 5),
        fail_consecutive=_env_int("BURNIN_FAIL_CONSECUTIVE", 3),
        strategy_text=os.environ.get("BURNIN_STRATEGY", "") or "",
        k=_env_float("BURNIN_K", 1.5),
        event_window=_env_float("BURNIN_EVENT_WINDOW", 30.0),
    )


@dataclass
class RoundResult:
    round_no: int
    t_reboot: float
    t_recover: float
    recover_time: float
    reboot_event_found: bool
    status_changed: bool
    status_diff: dict
    passed: bool
    error: str | None = None


@dataclass
class Baseline:
    status: dict
    fields: list = field(default_factory=lambda: list(DEFAULT_FIELDS))
