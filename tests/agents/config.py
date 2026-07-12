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
    # ---- 设备连接信息（对应环境变量 BURNIN_HOST / BURNIN_USER / BURNIN_PASSWORD，
    #      也兼容 HOST / USER / PASSWORD 覆盖）----
    host: str                      # 设备地址，如 192.168.3.33
    user: str                      # Digest 认证用户名，如 admin
    password: str                  # Digest 认证密码

    # ---- 拷机规模（对应 BURNIN_MAX_ROUNDS / BURNIN_MAX_DURATION）----
    max_rounds: int = 0            # 最大轮次；0 = 不限制轮次（配合 max_duration 或手动停止）
    max_duration: float = 0.0      # 最大运行时长（秒）；0 = 不限制（注意：0 表示无限，不是“0 秒就停”）

    # ---- 自适应重启间隔（对应 BURNIN_BASE_INTERVAL / BURNIN_INTERVAL_MIN / BURNIN_INTERVAL_MAX）----
    # 下一轮冷却 = clamp(本轮恢复耗时 * k + base_interval, interval_min, interval_max)
    base_interval: float = 60.0    # 冷却基础值（秒）：恢复耗时之外的固定等待
    interval_min: float = 30.0     # 自适应间隔下限（秒）：防止对恢复慢的设备压得太紧
    interval_max: float = 600.0    # 自适应间隔上限（秒）：防止间隔无限拉长

    # ---- 失败与中止（对应 BURNIN_RECOVER_TIMEOUT / BURNIN_FAIL_THRESHOLD / BURNIN_FAIL_CONSECUTIVE）----
    recover_timeout: float = 180.0 # 单轮重启后等待设备恢复的最长时限（秒）；超时记该轮失败
    fail_threshold: int = 5        # 累计失败达到此值 → 中止拷机
    fail_consecutive: int = 3      # 连续失败达到此值 → 中止拷机（比累计更敏感，抓突发恶化）

    # ---- 策略与判定（对应 BURNIN_STRATEGY / BURNIN_K / BURNIN_EVENT_WINDOW）----
    strategy_text: str = ""        # 策略提示词：如“连续重启2次后断言 wifi=connect”；空=走默认流程
    k: float = 1.5                 # 自适应间隔系数：恢复耗时乘以 k 作为冷却的一部分
    event_window: float = 30.0     # 重启事件查询窗口（秒）：围绕 [t_reboot-window, t_recover+window] 查询


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
