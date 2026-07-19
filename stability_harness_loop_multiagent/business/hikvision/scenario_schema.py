"""稳定性场景的数据驱动 schema（pydantic v2）。

把《门禁对讲通用稳定性用例集》里每条用例的结构抽象成一份 YAML，新增加例 = 新增
一份 YAML，**无需改任何代码**。这与框架「三引擎互不 import、扩展只改一个引擎」
的约束一致：本文件属于 business（领域）层，只描述数据，由 scenario_adapter /
scenario_worker / scenario_runner 消费。

一条用例 = 一组可声明的能力组合：
  - target          ：被测设备的连接信息（host/port/user/password）。
  - preconditions[] ：前置条件（device_online / serial_mode / baseline_record）。
  - actions[]       ：每轮施加的动作（reboot / upgrade / remote_open / dispatch /
                      switch_serial / sleep / query_events / noop / none / issue）。
  - probes[]        ：每轮探测与断言（field / online / count / event_chain）。
                      field 类型需在 params 中给出 expect_equals 或 expect_in。
  - loop            ：停止条件 —— max_rounds / interval_seconds / deadline(HH:MM) /
                      max_duration / stop_on_na / fail_threshold。

向后兼容：旧 YAML（``stress`` + ``probe`` 单数）通过 :func:`_migrate_legacy` 自动
转换为新格式（``actions[]`` + ``probes[]``），现有 configs/*.yaml 无需改动。
"""

import os
import re
from dataclasses import dataclass
from typing import Any, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

# ---- 哨兵与正则 ---------------------------------------------------------
# 字段解析时用于表达「路径上不存在该键/索引」的哨兵值。
_SENTINEL = object()

# ${VAR} 与 ${VAR:-default} 两种写法，便于把密码等敏感信息从 YAML 外置。
_ENV_RE = re.compile(r"\$\{([^}:]+)(?::-([^}]*))?\}")

# 字段路径分段：支持点号分段与列表下标，如 AcsWorkStatus.doorOnlineStatus[0]、
# netReaderOnlineStatus[2].status。
_TOKEN_RE = re.compile(r"([^.\[\]]+)(?:\[(\d+)\])?")

# HH:MM 墙钟校验：小时 0-23，分钟 00-59。
_DEADLINE_RE = re.compile(r"([01]?\d|2[0-3]):[0-5]\d")

# 旧版 stress.type 白名单（向后兼容保留，供旧代码引用）。
STRESS_TYPES = ("reboot", "upgrade", "issue", "none")


# ---- 能力描述（组合的基本单元，pydantic v2）-----------------------------
class ActionSpec(BaseModel):
    """YAML 中的单个 action 描述。

    ``type`` 为动作类型白名单；``params`` 为该动作的参数字典（自由结构，由
    adapter / worker 解释）。新增动作类型时在此 Literal 登记。
    """

    # 8 个新类型 + 2 个旧类型（none/issue）保留向后兼容。
    type: Literal[
        "reboot", "upgrade", "issue", "none",
        "remote_open", "dispatch", "switch_serial",
        "sleep", "query_events", "noop",
    ]
    params: dict[str, Any] = Field(default_factory=dict)


class ProbeSpec(BaseModel):
    """YAML 中的单个 probe 描述。

    ``type`` 为探测类型白名单；``params`` 自由结构。``field`` 类型需在 params 中
    给出 ``expect_equals`` 或 ``expect_in``（由 Scenario 层校验）。
    """

    type: Literal["field", "online", "count", "event_chain"]
    params: dict[str, Any] = Field(default_factory=dict)


class PreconditionSpec(BaseModel):
    """YAML 中的单个 precondition 描述。"""

    type: Literal["device_online", "serial_mode", "baseline_record"]
    params: dict[str, Any] = Field(default_factory=dict)


# ---- 配置块 -------------------------------------------------------------
class TargetCfg(BaseModel):
    """被测设备连接信息。"""

    host: str
    port: int = 80
    username: str = "admin"
    password: str = ""
    http_timeout: float = 5.0


class LoopCfg(BaseModel):
    """循环停止条件。"""

    max_rounds: int = Field(default=1, ge=1)
    interval_seconds: float = 90.0  # 轮间间隔（映射到 Scheduler.base，k=0 固定）
    deadline: Optional[str] = None  # "HH:MM" 墙钟；超过则停止（NT，未测试）
    max_duration: float = 0.0  # 墙钟预算（秒）；0 表示不限制
    stop_on_na: bool = False  # NA 时是否立即停止
    fail_threshold: int = 0  # 累计失败达到即停止（0 表示不限制）

    @field_validator("deadline")
    @classmethod
    def _validate_deadline(cls, v: Optional[str]) -> Optional[str]:
        """deadline 必须为 HH:MM 格式（小时 0-23，分钟 00-59）。"""
        if v is not None and not _DEADLINE_RE.fullmatch(v):
            raise ValueError("loop.deadline 必须为 HH:MM 格式（小时 0-23，分钟 00-59）")
        return v


# ---- 向后兼容的旧视图（dataclass）---------------------------------------
# 旧代码（scenario_worker / scenario_adapter / scenario_runner）通过
# ``scenario.stress`` / ``scenario.probe`` 单数属性访问。新 schema 用 actions[] /
# probes[] 复数存储，这两个 dataclass 作为只读视图从首条 action/probe 派生。
@dataclass
class Stress:
    """每轮施加的压力操作（旧视图，从 actions[0] 派生）。"""

    type: str = "none"  # reboot | upgrade | issue | none | ...
    endpoint: Optional[str] = None  # upgrade/issue 必填：ISAPI 路径
    method: str = "PUT"  # upgrade/issue 的请求方法
    body: Any = None  # upgrade/issue 的请求体（dict / str）
    body_file: Optional[str] = None  # 可选：从文件读取 body（覆盖 body）
    reboot_after: bool = True  # reboot/upgrade 会让设备重启，等待上线
    wait_online_timeout: float = 180.0  # 重启后等待上线的最长时间


@dataclass
class Probe:
    """每轮探测与断言（旧视图，从 probes[0] 派生）。"""

    endpoint: str = ""  # ISAPI 路径（建议带 ?format=json）
    method: str = "GET"
    field: str = ""  # 期望值所在的字段路径，如 AcsWorkStatus.doorOnlineStatus[0]
    expect_equals: Any = None  # 期望值（与字段值相等即通过）
    expect_in: Optional[List[Any]] = None  # 或：字段值落在该集合内即通过
    na_if_absent: Optional[str] = None  # 存在性字段；若缺失 => NA（不判失败）


# 向后兼容别名：旧代码可能 import Target，实际等同 TargetCfg。
Target = TargetCfg


# ---- 完整 Scenario ------------------------------------------------------
class Scenario(BaseModel):
    """一条完整的稳定性用例（可序列化为 YAML）。

    新 schema 用 ``actions[]`` + ``probes[]`` 复数描述能力组合；旧 YAML 的
    ``stress`` + ``probe`` 单数字段由 :func:`_migrate_legacy` 自动迁移。
    """

    id: str
    name: str
    category: str = ""
    level: str = ""
    target: TargetCfg
    preconditions: List[PreconditionSpec] = Field(default_factory=list)
    actions: List[ActionSpec] = Field(default_factory=list)
    probes: List[ProbeSpec] = Field(default_factory=list)
    loop: LoopCfg
    verify_enabled: bool = False

    @model_validator(mode="after")
    def _validate_combination(self) -> "Scenario":
        """组合语义校验：id 非空 + 至少一个 probe + field 类型 probe 需有断言。"""
        if not self.id:
            raise ValueError("scenario.id 不能为空")
        if not self.probes:
            raise ValueError("scenario 至少需要一个 probe")
        for p in self.probes:
            # field 类型 probe 必须给出 expect_equals 或 expect_in 用于断言
            # （其他类型如 online/count/event_chain 由 adapter 自行解释）。
            if p.type == "field":
                if "expect_equals" not in p.params and "expect_in" not in p.params:
                    raise ValueError(
                        "field 类型 probe 至少需要 expect_equals 或 expect_in 之一用于断言"
                    )
        return self

    # ---- 向后兼容属性（从首条 action/probe 派生只读视图）----
    @property
    def stress(self) -> Stress:
        """旧代码访问 ``scenario.stress``：从 ``actions[0]`` 派生 Stress 视图。

        无 action 时返回默认 ``Stress(type="none")``（不施加压力）。
        """
        if not self.actions:
            return Stress(type="none")
        a = self.actions[0]
        params = dict(a.params or {})
        endpoint = params.pop("endpoint", None)
        method = params.pop("method", "PUT")
        body = params.pop("body", None)
        body_file = params.pop("body_file", None)
        reboot_after = params.pop("reboot_after", True)
        wait_online_timeout = params.pop("wait_online_timeout", 180.0)
        return Stress(
            type=a.type,
            endpoint=endpoint,
            method=str(method),
            body=body,
            body_file=body_file,
            reboot_after=bool(reboot_after),
            wait_online_timeout=float(wait_online_timeout),
        )

    @property
    def probe(self) -> Probe:
        """旧代码访问 ``scenario.probe``：从 ``probes[0]`` 派生 Probe 视图。"""
        if not self.probes:
            return Probe()
        p = self.probes[0]
        params = dict(p.params or {})
        expect_in = params.get("expect_in")
        if expect_in is not None and not isinstance(expect_in, list):
            expect_in = list(expect_in)
        return Probe(
            endpoint=str(params.get("endpoint", "")),
            method=str(params.get("method", "GET")),
            field=str(params.get("field", "")),
            expect_equals=params.get("expect_equals"),
            expect_in=expect_in,
            na_if_absent=params.get("na_if_absent"),
        )


# ---- 环境变量插值 -------------------------------------------------------
def _interp_env(value: Any) -> Any:
    """递归地对字符串值做 ``${VAR}`` / ``${VAR:-default}`` 插值。

    支持 dict / list / str 递归处理；其他类型原样返回。
    """
    if isinstance(value, str):
        def _sub(m: "re.Match") -> str:
            name = m.group(1)
            # m.group(2) 为 None（无 :-）或字符串（含空串 :-）；统一转成默认值。
            default = m.group(2) if m.group(2) is not None else ""
            return os.environ.get(name, default)
        return _ENV_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _interp_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interp_env(v) for v in value]
    return value


# ---- 旧 YAML 迁移（stress + probe → actions + probes）------------------
def _migrate_legacy(raw: dict) -> dict:
    """旧 YAML（``stress`` + ``probe`` 单数）迁移到新格式（``actions[]`` + ``probes[]``）。

    旧格式::

        stress: {type: reboot, reboot_after: true, wait_online_timeout: 180}
        probe:  {endpoint: ..., field: ..., expect_equals: 1, ...}

    新格式::

        actions: [{type: reboot, params: {reboot_after: true, ...}}]
        probes:  [{type: field,  params: {endpoint: ..., ...}}]

    旧 ``stress.type`` 中的 ``none`` / ``issue`` 等值在新 Literal 中仍被支持
    （ActionSpec.type Literal 已包含旧值），无需映射。
    """
    if "stress" in raw or "probe" in raw:
        stress = raw.pop("stress", None) or {}
        probe = raw.pop("probe", None) or {}
        if stress:
            raw.setdefault("actions", []).append({
                "type": stress.get("type", "noop"),
                "params": {k: v for k, v in stress.items() if k != "type"},
            })
        if probe:
            raw.setdefault("probes", []).append({
                "type": "field",
                "params": dict(probe),
            })
    return raw


# ---- 字段路径解析 -------------------------------------------------------
def resolve_field(snapshot: Any, path: str, default: Any = _SENTINEL) -> Any:
    """按路径从快照中抽取字段值；路径任一节缺失/越界则返回 ``default``。

    ``default`` 默认是哨兵 ``_SENTINEL``，调用方可用 ``is`` 判断「是否存在」。
    支持 ``.`` 分段与 ``[N]`` 列表下标，如 ``AcsWorkStatus.doorOnlineStatus[0]``。
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


# ---- 探测值比较 ---------------------------------------------------------
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


def compare_probe(value: Any, probe: Any) -> bool:
    """把探测值与期望值比较，返回是否通过（True=pass）。

    ``probe`` 需暴露 ``expect_equals`` 与 ``expect_in`` 属性（兼容旧 :class:`Probe`
    dataclass 与测试中的鸭子类型对象）。签名保持与 ``scenario_worker.py`` 调用
    约定一致：``compare_probe(value, p)``。
    """
    expect_equals = getattr(probe, "expect_equals", None)
    expect_in = getattr(probe, "expect_in", None)
    if expect_equals is not None:
        return _coerce_eq(value, expect_equals)
    if expect_in:
        return any(_coerce_eq(value, x) for x in expect_in)
    return False


# ---- YAML 加载 ----------------------------------------------------------
def from_dict(data: dict) -> Scenario:
    """从（可能含旧字段 / 环境变量占位符的）字典构造 Scenario，并校验。

    流程：深拷贝 → 环境变量插值 → 旧字段迁移 → pydantic 校验。
    """
    raw = _interp_env(dict(data))
    raw = _migrate_legacy(raw)
    return Scenario.model_validate(raw)


def from_yaml(path: str) -> Scenario:
    """从 YAML 文件加载并校验一份场景（自动插值 + 迁移旧格式）。"""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"场景 YAML 根必须是一个映射：{path}")
    raw = _interp_env(raw)
    raw = _migrate_legacy(raw)
    return Scenario.model_validate(raw)


__all__ = [
    # 新 pydantic 模型
    "ActionSpec", "ProbeSpec", "PreconditionSpec",
    "TargetCfg", "LoopCfg", "Scenario",
    # 向后兼容旧视图
    "Target", "Stress", "Probe", "STRESS_TYPES",
    # 工具函数
    "resolve_field", "compare_probe",
    # 加载入口
    "from_dict", "from_yaml",
]
