"""稳定性场景的数据驱动 schema（领域无关，可复用于门禁对讲 108 条用例）。

设计目标：把《门禁对讲通用稳定性用例集》里每条用例的结构抽象成一份 YAML，
新增加例 = 新增一份 YAML，**无需改任何代码**。这与框架「三引擎互不 import、
扩展只改一个引擎」的约束一致：本文件属于 business（领域）层，只描述数据，
由 scenario_adapter / scenario_worker / scenario_runner 消费。

一条用例 = 一组可声明的事实：
  - target ：被测设备的连接信息（host/port/user/password）。
  - stress ：每轮施加的「压力操作」—— reboot / upgrade / issue / none。
             reboot/upgrade 会让设备重启，故默认 reboot_after=True（等待上线）。
  - probe  ：每轮「探测」——GET 某 ISAPI 端点，解析字段，与期望值比较；
             若 na_if_absent 指定的「存在性字段」缺失，则本用例在该设备上
             Not Applicable（NA，不判失败）。
  - loop   ：停止条件 —— max_rounds / interval_seconds / deadline(HH:MM) /
             max_duration / stop_on_na / fail_threshold。

与 AGENTS.md 扩展方式对应：
  - 新增目标类型  -> 实现 TargetAdapter（本层提供 ScenarioISAPIAdapter）。
  - 新增领域操作  -> 在 stress.type 注册一种类型（当前 reboot/upgrade/issue/none）。
  - 新中止条件    -> loop 已支持 CountStop/DurationStop/ExternalAbortStop。
"""

import os
import re
from dataclasses import dataclass, field
from typing import Any, List, Optional

try:
    import yaml
except ImportError:  # noqa: BLE001 - 理论上 PyYAML 已随环境安装
    yaml = None  # type: ignore


# 字段解析时用于表达「路径上不存在该键/索引」的哨兵值。
_SENTINEL = object()

# stress.type 的白名单。新增操作类型时在此登记 + 在 adapter 中实现对应行为。
STRESS_TYPES = ("reboot", "upgrade", "issue", "none")


@dataclass
class Target:
    """被测设备连接信息。"""

    host: str
    port: int = 80
    username: str = "admin"
    password: str = ""
    http_timeout: float = 5.0


@dataclass
class Stress:
    """每轮施加的压力操作。"""

    type: str = "none"  # reboot | upgrade | issue | none
    endpoint: Optional[str] = None  # upgrade/issue 必填：ISAPI 路径
    method: str = "PUT"  # upgrade/issue 的请求方法
    body: Any = None  # upgrade/issue 的请求体（dict / str）
    body_file: Optional[str] = None  # 可选：从文件读取 body（覆盖 body）
    reboot_after: bool = True  # reboot/upgrade 会让设备重启，等待上线
    wait_online_timeout: float = 180.0  # 重启后等待上线的最长时间


@dataclass
class Probe:
    """每轮探测与断言。"""

    endpoint: str  # ISAPI 路径（建议带 ?format=json）
    method: str = "GET"
    field: str = ""  # 期望值所在的字段路径，如 AcsWorkStatus.doorOnlineStatus[0]
    expect_equals: Any = None  # 期望值（与字段值相等即通过）
    expect_in: Optional[List[Any]] = None  # 或：字段值落在该集合内即通过
    na_if_absent: Optional[str] = None  # 存在性字段；若缺失 => NA（不判失败）


@dataclass
class LoopCfg:
    """循环停止条件。"""

    max_rounds: int = 1
    interval_seconds: float = 90.0  # 轮间间隔（映射到 Scheduler.base，k=0 固定）
    deadline: Optional[str] = None  # "HH:MM" 墙钟；超过则停止（NT，未测试）
    max_duration: float = 0.0  # 墙钟预算（秒）；0 表示不限制
    stop_on_na: bool = False  # NA 时是否立即停止
    fail_threshold: int = 0  # 累计失败达到即停止（0 表示不限制）


@dataclass
class Scenario:
    """一条完整的稳定性用例（可序列化为 YAML）。"""

    id: str
    name: str
    target: Target
    stress: Stress
    probe: Probe
    loop: LoopCfg
    category: str = ""
    level: str = ""
    verify_enabled: bool = False

    def validate(self) -> None:
        """就地校验场景语义；不合法则抛 ``ValueError``。"""
        if not self.id:
            raise ValueError("scenario.id 不能为空")
        if self.stress.type not in STRESS_TYPES:
            raise ValueError(
                f"stress.type 必须是 {STRESS_TYPES} 之一，收到 {self.stress.type!r}"
            )
        if self.stress.type in ("upgrade", "issue"):
            if not self.stress.endpoint:
                raise ValueError(
                    f"stress.type={self.stress.type} 需要 stress.endpoint"
                )
        if not self.probe.endpoint:
            raise ValueError("probe.endpoint 不能为空")
        if not self.probe.field:
            raise ValueError("probe.field 不能为空（用于断言的字段路径）")
        if self.probe.expect_equals is None and not self.probe.expect_in:
            raise ValueError(
                "probe 至少需要 expect_equals 或 expect_in 之一用于断言"
            )
        if self.loop.max_rounds < 1:
            raise ValueError("loop.max_rounds 必须 >= 1")
        if self.loop.deadline:
            if not re.fullmatch(r"\d{1,2}:\d{2}", self.loop.deadline):
                raise ValueError("loop.deadline 必须为 HH:MM 格式")


# ---- 环境变量插值 -------------------------------------------------------
# 支持 ${VAR} 与 ${VAR:-default} 两种写法，便于把密码等敏感信息从 YAML 外置。
_ENV_RE = re.compile(r"\$\{([^}:]+)(?::-([^}]*))?\}")


def _interp(value: Any) -> Any:
    """递归地对字符串值做 ${VAR} / ${VAR:-default} 插值。"""
    if isinstance(value, str):
        def _sub(m: "re.Match") -> str:
            name = m.group(1)
            default = m.group(2) if m.group(2) is not None else ""
            return os.environ.get(name, default)
        return _ENV_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _interp(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interp(v) for v in value]
    return value


# ---- 字段路径解析 -------------------------------------------------------
# 支持点号分段与列表下标：AcsWorkStatus.doorOnlineStatus[0]、
# netReaderOnlineStatus[2].status。
_TOKEN_RE = re.compile(r"([^.\[\]]+)(?:\[(\d+)\])?")


def resolve_field(snapshot: Any, path: str, default: Any = _SENTINEL) -> Any:
    """按路径从快照中抽取字段值；路径任一节缺失/越界则返回 ``default``。

    ``default`` 默认是哨兵 ``_SENTINEL``，调用方可用 ``is`` 判断「是否存在」。
    """
    cur = snapshot
    for m in _TOKEN_RE.finditer(path):
        key = m.group(1)
        idx = m.group(2)
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
        if idx is not None:
            if not isinstance(cur, (list, tuple)) or int(idx) >= len(cur):
                return default
            cur = cur[int(idx)]
    return cur


def _coerce_eq(a: Any, b: Any) -> bool:
    """宽松相等：允许字符串数字与数值比较（海康常把状态写成字符串）。"""
    if a == b:
        return True
    # 数值 <-> 数字字符串
    try:
        if float(a) == float(b):  # type: ignore[arg-type]
            return True
    except (TypeError, ValueError):
        pass
    return False


def compare_probe(value: Any, probe: Probe) -> bool:
    """把探测值与期望值比较，返回是否通过（True=pass）。"""
    if probe.expect_equals is not None:
        return _coerce_eq(value, probe.expect_equals)
    if probe.expect_in:
        return any(_coerce_eq(value, x) for x in probe.expect_in)
    return False


# ---- YAML 加载 ----------------------------------------------------------
def _as_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _as_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def from_dict(d: dict) -> Scenario:
    """从（已插值）字典构造 Scenario，并校验。"""
    d = _interp(d)
    t = d.get("target", {}) or {}
    target = Target(
        host=str(t.get("host", "")),
        port=_as_int(t.get("port", 80), 80),
        username=str(t.get("username", "admin")),
        password=str(t.get("password", "")),
        http_timeout=_as_float(t.get("http_timeout", 5.0), 5.0),
    )
    s = d.get("stress", {}) or {}
    stress = Stress(
        type=str(s.get("type", "none")),
        endpoint=s.get("endpoint"),
        method=str(s.get("method", "PUT")),
        body=s.get("body"),
        body_file=s.get("body_file"),
        reboot_after=bool(s.get("reboot_after", True)),
        wait_online_timeout=_as_float(s.get("wait_online_timeout", 180.0), 180.0),
    )
    p = d.get("probe", {}) or {}
    probe = Probe(
        endpoint=str(p.get("endpoint", "")),
        method=str(p.get("method", "GET")),
        field=str(p.get("field", "")),
        expect_equals=p.get("expect_equals"),
        expect_in=list(p.get("expect_in")) if p.get("expect_in") else None,
        na_if_absent=p.get("na_if_absent"),
    )
    l = d.get("loop", {}) or {}
    loop = LoopCfg(
        max_rounds=_as_int(l.get("max_rounds", 1), 1),
        interval_seconds=_as_float(l.get("interval_seconds", 90.0), 90.0),
        deadline=l.get("deadline"),
        max_duration=_as_float(l.get("max_duration", 0.0), 0.0),
        stop_on_na=bool(l.get("stop_on_na", False)),
        fail_threshold=_as_int(l.get("fail_threshold", 0), 0),
    )
    sc = Scenario(
        id=str(d.get("id", "")),
        name=str(d.get("name", "")),
        target=target,
        stress=stress,
        probe=probe,
        loop=loop,
        category=str(d.get("category", "")),
        level=str(d.get("level", "")),
        verify_enabled=bool(d.get("verify_enabled", False)),
    )
    sc.validate()
    return sc


def from_yaml(path: str) -> Scenario:
    """从 YAML 文件加载并校验一份场景。"""
    if yaml is None:
        raise RuntimeError("需要 PyYAML 才能加载场景 YAML（pip install pyyaml）")
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"场景 YAML 根必须是一个映射：{path}")
    return from_dict(raw)


__all__ = [
    "Scenario", "Target", "Stress", "Probe", "LoopCfg",
    "STRESS_TYPES", "resolve_field", "compare_probe",
    "from_dict", "from_yaml",
]
