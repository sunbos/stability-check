# 框架长期演进设计 spec

> 日期:2026-07-20
> 主题:`stability_harness_loop_multiagent` 框架长期演进——基座稳定 + 能力原子化 + 换轮子 + pytest marker 硬规则
> 状态:待用户复审

---

## 1. 背景与目标

### 1.1 当前痛点(用户陈述)

1. **基座稳定性**:框架后续只用于海康门禁稳定性测试,会持续增加稳定性场景(108 条用例),基座必须稳定
2. **worker.py 1020 行不可维护**:单文件混了 5 大类用例逻辑,后续加用例会彻底无法维护
3. **重复造轮子**:`client.py` 手写 HTTP+Digest Auth、`scenario_schema.py` 手写 dataclass+校验、`llm.py` 手写 urllib 调 OpenAI——业界有成熟方案,绝不造轮子
4. **过度抽象风险**:若存在过度抽象,及时调整

### 1.2 框架定位(用户确认)

贴合 AI 时代 **Harness Engineering / Loop Engineering / Multi-Agent Engineering** 概念的稳定性检测基座。不是通用 Agent 框架,是领域特化的稳定性测试框架。

### 1.3 演进目标

- 保留完整架构(core + 三引擎 + business),只换轮子
- 能力原子化 + YAML 组合,新增用例零代码改动
- pytest marker 硬规则:1 marker = 1 用例
- 测试零 fake 零 mock:所有事件链真实设备触发

---

## 2. 架构改造范围

### 2.1 保留不动(核心架构基座)

- `core/`(EventBus / Agent / combine_votes)——契约内核,零内部依赖
- `harness/`(Runtime / Watchdog / Telemetry / Tracer / Governance / Verifier)——治理引擎
- `loop/`(ControlLoop / DecisionAuthority / SharedContext / TerminationPolicy / Scheduler)——控制引擎
- `multi_agent/`(Worker / Advisor / Observer 基类 + TargetAdapter 协议)——多智能体引擎
- **5 大不变量全保留**:引擎隔离、防死锁、事实独裁、fail-closed 不 halt、拒绝不扣配额

### 2.2 改造(换轮子,不动架构)

| 文件 | 现状 | 改造 |
|------|------|------|
| `business/hikvision/client.py`(419 行) | 手写 HTTP + Digest Auth + 线程锁 | 换 `httpx.Client` + `httpx-auth.DigestAuth`,删除手写 Digest |
| `business/hikvision/llm.py`(157 行) | 手写 urllib 调 OpenAI + 手写 JSON 抽取 | 换 `openai` 官方 SDK + pydantic structured output |
| `business/hikvision/advisor.py` | LLM 返回 dict + 字段存在性判断 + 手写 JSON 抽取 | 用 pydantic `LLMPlan` 强类型 + openai structured output |
| `business/hikvision/scenario_schema.py`(283 行) | 手写 dataclass + 手写校验循环 | 换 `pydantic.BaseModel` + `field_validator` + `model_validator` |
| `examples/_report.py` | 标准库 print + 字符串拼接 | 换 `rich.console` + `rich.table` |

### 2.3 新增

| 新增 | 职责 |
|------|------|
| `tests/conftest.py` | `pytest_configure` 扫描 `configs/*.yaml`,按 `id` 动态注册 marker |
| `tests/test_stability_scenario.py` | 单一测试函数,通过 marker + parametrize 跑全部用例 |
| `business/hikvision/capabilities/` 子包 | 能力原子(actions + probes + preconditions) |

### 2.4 删除

- `business/hikvision/worker.py`(1020 行)——逻辑原子化迁到 `capabilities/`
- `tests/fakes/` 整个目录——自证陷阱
- `business/hikvision/scenario_adapter.py` 中的 `FakeScenarioAdapter` 类——自证陷阱
- `multi_agent/` 中的 `FakeTargetAdapter`——自证陷阱
- `examples/smoke.py`(依赖 FakeTargetAdapter)——自证陷阱
- `scenario_run.py --dry-run` 选项(依赖 fake)

### 2.5 不引入

- `langchain` / `langgraph` / `langsmith`(场景错位 + 依赖爆炸)
- 任何 mock 框架(`respx` / `pytest-mock` / `unittest.mock`)
- 任何 fake 类(FakeHikvisionClient / FakeScenarioAdapter / FakeLLMClient / FakeTargetAdapter)

### 2.6 架构不变量自检

`core/` 三引擎零改动 → 引擎隔离、事实独裁、防死锁、fail-closed 全保留。`business/` 仍只被引擎装配,不反向依赖。

---

## 3. 用例组织方式

### 3.1 目录结构

```
configs/
├── scenario_template.yaml          # 模板(保留)
├── stability_0001_reboot.yaml      # 已有
├── stability_0009_wired_network.yaml
├── stability_0002_*.yaml           # 新增(从模板复制改字段)
├── ...
└── stability_0078_*.yaml           # 共约 78 份(只覆盖自动用例)

tests/
├── conftest.py                     # 动态注册 marker
├── test_stability_scenario.py      # 单一测试函数
└── (其他纯逻辑单测 + 真实环境集成测试)
```

### 3.2 conftest.py 工作机制

```python
def pytest_configure(config):
    """扫描 configs/*.yaml,按 id 字段动态注册 marker"""
    for yaml_path in Path("configs").glob("stability_*.yaml"):
        data = yaml.safe_load(yaml_path.read_text())
        scenario_id = data["id"]            # 如 "Stability_0001"
        category = data["category"]         # 如 "重启稳定性"
        level = data["level"]               # 如 "L2"
        # 注册 3 个维度的 marker
        config.addinivalue_line("markers", f"{scenario_id}: 用例 {scenario_id}")
        config.addinivalue_line("markers", f"{category}: 类别 {category}")
        config.addinivalue_line("markers", f"{level}: 等级 {level}")
        SCENARIO_MAP[scenario_id] = yaml_path

def pytest_collection_modifyitems(config, items):
    """给 test_stability_scenario 自动打上对应 marker"""
    for item in items:
        if item.name == "test_stability_scenario":
            scenario_id = item.callspec.params["scenario_id"]
            yaml_path = SCENARIO_MAP[scenario_id]
            data = yaml.safe_load(yaml_path.read_text())
            item.add_marker(getattr(pytest.mark, scenario_id))
            item.add_marker(getattr(pytest.mark, data["category"]))
            item.add_marker(getattr(pytest.mark, data["level"]))
```

### 3.3 test_stability_scenario.py

```python
@pytest.mark.parametrize("scenario_id", SCENARIO_IDS)
def test_stability_scenario(scenario_id, request):
    """所有用例必须真实设备执行,无 HIK_HOST 时整个测试函数 skip。"""
    if not os.environ.get("HIK_HOST"):
        pytest.skip("无 HIK_HOST,需真实设备")
    yaml_path = SCENARIO_MAP[scenario_id]
    result = run_scenario(yaml_path)  # 真实设备执行,无 dry-run
    assert result.verdict in ("pass", "warn", "na"), \
        f"用例 {scenario_id} 失败: {result}"
```

**注**:不用 `@pytest.mark.real_device` marker,直接在函数内 `pytest.skip` 更清晰(无环境时整个 parametrize 不执行)。

### 3.4 使用方式(满足"1 marker = 1 用例"硬规则)

```bash
# 跑单条用例
pytest -m Stability_0001
pytest -m Stability_0009

# 跑一个类别
pytest -m "重启稳定性"
pytest -m "网络连接稳定性"

# 跑一个等级
pytest -m L2

# 组合筛选
pytest -m "重启稳定性 and L2"
pytest -m "not Stability_0001"

# 列出所有 marker(验证硬规则)
pytest --markers | grep Stability_
```

### 3.5 关键设计点

1. **marker 即用例 id**:`Stability_0001` 严格对应 `stability_0001_*.yaml`,1:1 映射
2. **三层 marker 维度**:用例 id + 类别 + 等级,任意维度可筛
3. **无设备 skip**:无 `HIK_HOST` 自动 skip,不强测
4. **YAML 即唯一真相**:用例改动只改 YAML,不碰 .py
5. **新增用例流程**:复制 `scenario_template.yaml` → 改字段 → `pytest -m <新id>` 立即可用

### 3.6 用例范围

只处理自动用例(约 78 条),手动用例(约 30 条)暂不纳入设计范围。

---

## 4. 能力原子化 + YAML 组合(替代 worker.py)

### 4.1 设计动机

用户诉求:`reboot.py`、`network_status.py` 这种功能可能在多个用例中组合复用,避免重复编写。

经代码核查发现:108 用例根本不走 `worker.py`(1020 行硬编码),走的是 `scenario_worker.py`(142 行组合器)。但当前 `scenario_schema.py` 只能描述"1 个 stress + 1 个 probe",无法表达"操作链 + 事件链 + 状态组合检查"——这是 `worker.py` 必须 1020 行的根因。

### 4.2 核心思路

把"操作/探测/前置"都做成**原子能力**,YAML 描述任意组合。新增用例 = 组合已有能力,零代码改动。

### 4.3 目录结构

```
business/hikvision/
├── capabilities/                    # 能力原子(可组合,单一职责)
│   ├── __init__.py                  # create_action/create_probe/create_precondition 工厂
│   ├── actions/
│   │   ├── base.py                  # ActionBase(协议:execute(ctx) -> ActionResult)
│   │   ├── reboot.py                # RebootAction(主设备 + 子设备批量)
│   │   ├── upgrade.py               # UpgradeAction(主设备 + 子设备)
│   │   ├── remote_open.py           # RemoteOpenAction(开门)
│   │   ├── dispatch.py              # DispatchAction(下发配置)
│   │   ├── switch_serial.py         # SwitchSerialAction(切串口模式)
│   │   ├── sleep.py                 # SleepAction(等待)
│   │   ├── query_events.py          # QueryEventsAction(查事件)
│   │   └── noop.py                  # NoopAction(长巡无压力)
│   ├── probes/
│   │   ├── base.py                  # ProbeBase(协议:check(snapshot) -> FactResult)
│   │   ├── field.py                 # FieldProbe(字段值断言,含 na_if_absent)
│   │   ├── online.py                # OnlineProbe(单设备在线)
│   │   ├── count.py                 # CountProbe(子设备在线数量比对)
│   │   └── event_chain.py           # EventChainProbe(事件链 30s 窗口)
│   └── preconditions/
│       ├── base.py                  # PreconditionBase(协议:setup(ctx) -> bool)
│       ├── device_online.py         # DeviceOnlinePrecondition
│       ├── serial_mode.py           # SerialModePrecondition
│       └── baseline_record.py       # BaselineRecordPrecondition(记录基线 serialNo + 重启时长)
├── scenario_worker.py               # 组合器:preconditions + actions + probes(重构)
├── scenario_adapter.py              # 薄壳(被 actions 调用,保留 ScenarioISAPIAdapter,删 Fake)
├── scenario_schema.py               # pydantic schema(扩展:支持 actions/probes 列表)
└── worker.py                        # 删除(全部逻辑迁到 capabilities/)
```

### 4.4 YAML schema 扩展

**现状**(只能 1+1):

```yaml
stress: { type: reboot }
probe:  { endpoint: ..., field: ..., expect_equals: 1 }
```

**扩展后**(可组合):

```yaml
# 用例 0001: 重启稳定性(简单用例)
preconditions:
  - type: device_online
actions:
  - type: reboot
    params: { target: main, wait_online_timeout: 180 }
probes:
  - type: field
    params:
      endpoint: /ISAPI/AccessControl/AcsWorkStatus?format=json
      field: AcsWorkStatus.doorOnlineStatus[0]
      expect_equals: 1
loop:
  max_rounds: 3
  interval_seconds: 90
```

```yaml
# 开门测试(原 worker.py 1020 行逻辑,现在一份 YAML)
preconditions:
  - type: device_online
  - type: serial_mode
    params: { mode: externMode }
  - type: baseline_record
    params: { record_reboot_duration: true }
actions:
  - type: remote_open
    params: { door: 1 }
  - type: sleep
    params: { seconds: 3 }
  - type: query_events
    params: { window: 30, baseline_serial: "${baseline.serial}" }
  - type: reboot
    params: { target: main, wait_online_timeout: "${baseline.reboot_duration}" }
probes:
  - type: event_chain
    params: { expect_sequence: [remote_open, lock_open, lock_closed], window: 30 }
  - type: online
    params: { field: AcsWorkStatus.doorOnlineStatus[0], expect_equals: 1 }
loop:
  max_rounds: 5
  interval_seconds: 90
```

**baseline 插值机制**:precondition `baseline_record` 在 `setup(ctx)` 时把 `serial` 和 `reboot_duration` 写入 `ctx.baseline` 字典。后续 actions/probes 的 `params` 中 `${baseline.xxx}` 在执行前从 `ctx.baseline` 插值(复用现有 `_interp_env` 逻辑,扩展为支持 `ctx` 引用)。这样 preconditions 产出的基线数据可以无损传递给 actions,无需硬编码。

```yaml
# 用例 0009: 长巡网络状态(无操作,仅探测)
preconditions: []
actions: []
probes:
  - type: field
    params:
      endpoint: /ISAPI/AccessControl/AcsWorkStatus?format=json
      field: AcsWorkStatus.doorOnlineStatus[0]
      expect_equals: 1
      na_if_absent: AcsWorkStatus.doorOnlineStatus
loop:
  max_rounds: 1000
  interval_seconds: 10
  deadline: "23:50"
  stop_on_na: true
```

### 4.5 ScenarioWorker 重构(组合器)

```python
class ScenarioWorker(WorkerAgent):
    def pre_loop_setup(self):
        for precondition in self._preconditions:
            precondition.setup(self._ctx)
    
    async def act(self, tick):
        for action in self._actions:       # 顺序执行操作链
            await asyncio.to_thread(action.execute, self._ctx)
        
        facts = {}
        for probe in self._probes:         # 组合探测,产出事实
            fact = probe.check(self._ctx.snapshot)
            facts.update(fact)             # 事实独裁:任一 False → fail
        self.publish("target/checked", {"facts": facts})
```

### 4.6 能力清单(初版)

| 类别 | 能力 | 来源 | 对应用例 |
|------|------|------|---------|
| action | reboot | scenario_adapter.py 迁移 | 0001-0004 |
| action | upgrade | scenario_adapter.py 迁移 | 0005-0008 |
| action | remote_open | worker.py 迁出 | 开门测试 |
| action | dispatch | 新增 | 0061-0078 |
| action | switch_serial | worker.py 迁出 | 串口外设用例 |
| action | sleep | 新增(简单) | 组合用例 |
| action | query_events | worker.py 迁出 | 事件链用例 |
| action | noop | 新增(对应原 stress.type=none 的空操作路径) | 0009-0024 长巡 |
| probe | field | scenario_schema.py 迁移 | 大部分用例 |
| probe | online | worker.py 迁出 | 重启类用例 |
| probe | count | 新增 | 子设备数量比对 |
| probe | event_chain | worker.py 迁出 | 开门测试 |
| precondition | device_online | worker.py 迁出 | 所有用例 |
| precondition | serial_mode | worker.py 迁出 | 串口外设用例 |
| precondition | baseline_record | worker.py 迁出 | 重启类用例 |

**noop 说明**:`NoopAction` 是显式的"无操作"占位,对应原 `scenario_adapter.py` 中 `stress.type == "none"` 的 `return Result(ok=True)` 路径。长巡类用例(0009-0024)在 `actions: []` 时隐式 noop,或显式 `- type: noop` 表达。

### 4.7 关键收益

1. **零重复**:reboot 能力写一次,所有用例复用
2. **可组合**:用例 = preconditions + actions + probes 任意组合
3. **可扩展**:新增能力 = 加一个文件 + 注册工厂
4. **可测试**:每个能力独立单元测试(纯逻辑部分)
5. **单文件 ≤ 200 行**:每个能力职责单一
6. **108 用例可维护**:大部分用例靠组合现有能力,无需写代码

---

## 5. Worker 智能边界

### 5.1 边界决策(用户确认)

**Worker 保持确定性(Strategy Pattern,YAML 驱动),不引入 Function Tool / MCP**。

### 5.2 决策理由

稳定性测试 = 重复执行确定操作 + 断言确定预期,与"开放式 LLM 任务"相反:

| 维度 | 稳定性测试 | 开放式任务(Function Tool 适合) |
|------|----------|---------------------------|
| 操作来源 | YAML 写死 | LLM 运行时决定 |
| 预期结果 | 用例集写死 | LLM 判断 |
| 可重复性 | **必须 100% 可重复** | 允许每次不同 |
| 失败定位 | 必须确定 | LLM 解释 |

给 Worker 加 Function Tool 会破坏可重复性和确定性,无法做"1000 次重启稳定性"测试。

### 5.3 AI 时代概念对应

| AI 时代概念 | 框架对应位置 |
|------------|-------------|
| Harness Engineering | `harness/` 整个引擎 |
| Loop Engineering | `loop/` 整个引擎 |
| Multi-Agent Engineering | `multi_agent/`(Worker 确定性 + Advisor LLM + Observer) |
| Function Tool | 不需要(Worker 确定性) |
| MCP | 不需要(进程内 EventBus 已解耦) |

### 5.4 智能分布

- **Worker**:0% LLM,纯确定性,Strategy Pattern + YAML 驱动
- **Advisor**:LLM 解析指令→计划 + 趋势投票 + fail-closed 护栏校验
- **Diagnostic**:确定性诊断内核

Worker 的"智能"不是 LLM 智能,而是**领域智能**(知道重启后要三阶段探测、知道事件链 30s 窗口、知道用设备时间不用主机时间)。

---

## 6. 造轮子替换实施细节

### 6.1 依赖清单(pyproject.toml)

```toml
[project]
name = "stability-harness-loop-multiagent"
requires-python = ">=3.10"
dependencies = []  # 核心框架仍零运行时依赖

[project.optional-dependencies]
business = [
    "httpx>=0.27",           # HTTP 客户端
    "httpx-auth>=0.22",      # Digest Auth
    "openai>=1.40",          # LLM 客户端
    "pydantic>=2.7",         # YAML schema 校验
    "PyYAML>=6.0",           # YAML 解析
]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.23",
]
docs = ["mkdocs-material", "mkdocstrings[python]"]
examples = ["rich>=13"]
```

### 6.2 client.py 改造(httpx + httpx-auth)

**现状**:419 行手写 Digest Auth + urllib HTTP + 线程锁
**改造后**(目标 ≤ 200 行,保留全部业务方法签名):

```python
import httpx
from httpx_auth import DigestAuth
from threading import Lock

class HikvisionClient:
    """海康 ISAPI 客户端(httpx + Digest Auth)。
    
    业务方法签名保持不变,仅替换 HTTP/Digest 实现:
    - reboot() / get_time() / get_work_status() / get_event_serial()
    - query_events(window, baseline_serial) / remote_open_door(door)
    - get_serial_config(port) / set_serial_config(port, cfg)
    - wait_online(timeout)  # 三阶段探测
    所有方法经 request_json 统一出口,线程锁保护 Digest Auth state。
    """
    
    def __init__(self, host, port, username, password, timeout=5.0):
        base_url = f"http://{host}:{port}"
        self._client = httpx.Client(base_url=base_url, timeout=timeout)
        self._auth = DigestAuth(username, password)
        self._lock = Lock()  # 保护 Digest Auth state(并发 query_events 用)
    
    def request_json(self, method, endpoint, body=None):
        """统一出口:所有业务方法经此调用 httpx。"""
        with self._lock:
            resp = self._client.request(method, endpoint, json=body, auth=self._auth)
            resp.raise_for_status()
            return resp.json()
    
    def reboot(self):
        self.request_json("PUT", "/ISAPI/System/reboot")
    
    def get_time(self) -> str:
        return self.request_json("GET", "/ISAPI/System/time")["time"]
    
    # query_events / remote_open_door / get_serial_config / wait_online 等
    # 方法签名保持不变,内部全部改为调 request_json(详见实现)
```

**删除约 220 行**:手写 Digest 算法(120 行)、手写 urllib 重试(50 行)、手写 XML 解析(50 行)。

### 6.3 llm.py 改造(openai SDK)

**现状**:157 行手写 urllib + 手写 JSON 抽取
**改造后**(目标 ≤ 60 行):

```python
import os
from openai import OpenAI
from pydantic import BaseModel

DEFAULT_MODEL = "tencent/hy3:free"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"

def get_client() -> OpenAI | None:
    key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not key:
        _load_dotenv()
        key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not key:
        return None
    return OpenAI(
        api_key=key,
        base_url=os.environ.get("LLM_BASE_URL", DEFAULT_BASE_URL),
        default_headers={"HTTP-Referer": "stability-harness", "X-Title": "hikvision-advisor"},
    )

def chat_json(client, system_prompt, user_prompt, response_model=None):
    model = os.environ.get("LLM_MODEL", DEFAULT_MODEL)
    try:
        if response_model:
            completion = client.beta.chat.completions.parse(
                model=model,
                messages=[{"role": "system", "content": system_prompt},
                          {"role": "user", "content": user_prompt}],
                response_format=response_model,
            )
            parsed = completion.choices[0].message.parsed
            return parsed.model_dump() if parsed else None
        else:
            completion = client.chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": system_prompt},
                          {"role": "user", "content": user_prompt}],
                temperature=0.0,
                max_tokens=512,
            )
            return {"text": completion.choices[0].message.content}
    except Exception:  # noqa: BLE001 - 失败一律回退规则
        return None
```

**删除约 100 行**:`OpenAICompatibleClient` 类 + `_extract_first_json` + `chat()` / `chat_json()` 手写方法。

### 6.4 advisor.py 改造(pydantic structured output)

```python
from pydantic import BaseModel, Field

class LLMPlan(BaseModel):
    """LLM 解析出的计划 schema(pydantic 强类型)"""
    skip_reboot: bool = Field(default=False, description="本轮是否跳过重启")
    operations: list[str] = Field(default_factory=list, description="操作列表")
    risk_note: str = Field(default="", description="风险备注")

# system prompt 完整内容(替换原 advisor.py 中的硬编码字符串)
ADVISOR_SYSTEM_PROMPT = """你是海康门禁稳定性测试的计划解析器。
输入:用户的自然语言测试指令(如"重启3次后检查在线")。
输出:严格遵守 LLMPlan schema 的 JSON。
- skip_reboot: 是否跳过重启(若指令不要求重启则为 true)
- operations: 操作列表,可选值 "reboot" / "remote_open" / "query_events" / "noop"
- risk_note: 风险备注,无则空字符串
不要输出任何其他内容。"""

class HikvisionAdvisor(AdvisorAgent):
    async def start(self):
        await super().start()
        client = get_client()
        if client:
            plan = await asyncio.to_thread(
                chat_json, client,
                ADVISOR_SYSTEM_PROMPT, self._instruction,
                LLMPlan
            )
            self._plan = plan or {}
        else:
            self._plan = self._rule_based_parse(self._instruction)  # 规则兜底(保留原逻辑)
        
        if self._enable_verify and self._plan:
            allowed, reason = await self._verify_plan(self._plan)
            if not allowed:
                self._plan = {}
        if self._plan:
            self.publish("hikvision/plan", self._plan)
        await asyncio.sleep(0)
    
    def _rule_based_parse(self, instruction: str) -> dict:
        """无 LLM 时的规则兜底(保留原 advisor.py 的规则解析逻辑)。"""
        # 关键词匹配:包含"重启" → operations=["reboot"]
        # 包含"开门" → operations=["remote_open"]
        # 默认 → operations=["noop"]
        # 详见实现(保留原逻辑,约 20 行)
```

**注**:`_rule_based_parse` 是 LLM 失败/无 key 时的兜底,保留原 advisor.py 的关键词匹配逻辑,确保无 LLM 时框架仍可运行(已有不变量)。

### 6.5 scenario_schema.py 改造(pydantic + 能力组合)

```python
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Any, Literal

class ActionSpec(BaseModel):
    type: Literal["reboot", "upgrade", "remote_open", "dispatch",
                   "switch_serial", "sleep", "query_events", "noop"]
    params: dict[str, Any] = Field(default_factory=dict)

class ProbeSpec(BaseModel):
    type: Literal["field", "online", "count", "event_chain"]
    params: dict[str, Any] = Field(default_factory=dict)

class PreconditionSpec(BaseModel):
    type: Literal["device_online", "serial_mode", "baseline_record"]
    params: dict[str, Any] = Field(default_factory=dict)

class TargetCfg(BaseModel):
    host: str
    port: int = 80
    username: str = "admin"
    password: str = ""
    http_timeout: float = 5.0

class LoopCfg(BaseModel):
    max_rounds: int = Field(default=1, ge=1)
    interval_seconds: float = 90.0
    deadline: str | None = None
    max_duration: float = 0.0
    stop_on_na: bool = False
    fail_threshold: int = 0
    
    @field_validator("deadline")
    @classmethod
    def _validate_deadline(cls, v):
        if v and not re.fullmatch(r"\d{1,2}:\d{2}", v):
            raise ValueError("deadline 必须为 HH:MM 格式")
        return v

class Scenario(BaseModel):
    id: str
    name: str
    category: str = ""
    level: str = ""
    target: TargetCfg
    preconditions: list[PreconditionSpec] = Field(default_factory=list)
    actions: list[ActionSpec] = Field(default_factory=list)
    probes: list[ProbeSpec] = Field(default_factory=list)
    loop: LoopCfg
    verify_enabled: bool = False
    
    @model_validator(mode="after")
    def _validate_combination(self):
        if not self.id:
            raise ValueError("scenario.id 不能为空")
        if not self.probes:
            raise ValueError("至少需要一个 probe")
        return self

def from_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return Scenario.model_validate(_interp_env(raw))
```

**收益**:手写校验全删,pydantic 自动类型转换 + 校验;`Literal` 强类型白名单,新增 action type 编译期检查;能力组合通过三个列表表达。

### 6.6 examples/_report.py 改造(rich)

```python
from rich.console import Console
from rich.table import Table

def print_round_report(round_no, verdict, facts, ...):
    console = Console()
    table = Table(title=f"Round {round_no}", show_lines=True)
    table.add_column("指标", style="cyan")
    table.add_column("值", style="magenta")
    table.add_row("Verdict", verdict)
    table.add_row("probe_ok", str(facts.get("probe_ok")))
    console.print(table)
```

---

## 7. 测试与验证策略

### 7.1 测试原则(零 fake + 零 mock)

**绝对禁止**:
- ❌ `FakeHikvisionClient` / `FakeScenarioAdapter` / `FakeLLMClient` / `FakeTargetAdapter` 等任何 fake
- ❌ `respx` / `pytest-mock` / `unittest.mock` 等任何 mock 框架
- ❌ `--dry-run` 模式(用 fake 自证)

**唯一允许**:
- ✅ 真实环境集成测试:真机(`HIK_HOST`)+ 真实 LLM(`LLM_API_KEY`),无环境 skip
- ✅ 纯逻辑单测:不涉及外部 IO 的纯函数,用真实数据结构

### 7.2 测试金字塔

```
                ┌─────────────────────┐
                │  L3 真机验证(手动)   │  ← 5 轮真机 + 真实 LLM,无环境 skip
                └─────────────────────┘
              ┌───────────────────────────┐
              │  L2 真实环境集成(pytest)  │  ← 真机单轮 + 真实 LLM,无环境 skip
              └───────────────────────────┘
            ┌───────────────────────────────┐
            │  L1 纯逻辑单测(pytest)        │  ← 不涉及外部 IO 的纯函数
            └───────────────────────────────┘
```

### 7.3 删除清单

| 文件/类 | 处理 |
|---------|------|
| `tests/fakes/` 整个目录 | 删除 |
| `scenario_adapter.py` 中的 `FakeScenarioAdapter` | 删除类 |
| `multi_agent/` 中的 `FakeTargetAdapter` | 删除 |
| `examples/smoke.py` | 删除(依赖 FakeTargetAdapter) |
| `scenario_run.py --dry-run` 选项 | 删除 |
| 所有引用 fake 的测试 | 改为真实环境 skip 或删除 |

### 7.4 L1 纯逻辑单测(保留 + 新增)

**保留**(已有,纯逻辑):

| 测试文件 | 测试内容 |
|---------|---------|
| `test_stability_harness_loop_multiagent_smoke.py` | 三引擎接线 |
| `test_decision_authority_*.py` | 事实独裁 |
| `test_governance.py` | 治理规则 |
| `test_verify.py` | 校验规则 |
| `test_harness_regression.py` | 引擎隔离守护 |

**新增**(纯逻辑):

| 新测试文件 | 测试内容 |
|-----------|---------|
| `test_scenario_schema.py` | pydantic 校验 + 环境变量插值 |
| `test_capabilities/test_probe_field.py` | FieldProbe 字段断言 |
| `test_capabilities/test_action_sleep.py` | SleepAction 时间计算 |
| `test_voting_combine.py` | combine_votes 合并 |

### 7.5 L2 真实环境集成(新增,无环境 skip)

```python
# tests/test_client_real_device.py
@pytest.mark.skipif(not os.environ.get("HIK_HOST"), reason="无 HIK_HOST")
class TestHikvisionClientReal:
    def test_get_time_returns_valid_iso(self):
        client = HikvisionClient(host=os.environ["HIK_HOST"], ...)
        t = client.get_time()
        assert "T" in t
    
    def test_reboot_and_wait_online(self):
        client = HikvisionClient(...)
        client.reboot()
        assert client.wait_online(timeout=180) is True
    
    def test_remote_open_event_chain_real(self):
        """真实设备:remote_open_door 后 30s 窗口能查到事件链"""
        client = HikvisionClient(...)
        baseline = client.get_event_serial()
        client.remote_open_door(door=1)
        time.sleep(3)
        events = client.query_events(window=30, baseline_serial=baseline)
        event_types = [e["eventType"] for e in events]
        assert "remote_open" in event_types
        assert "lock_open" in event_types

# tests/test_advisor_real_llm.py
@pytest.mark.skipif(not os.environ.get("LLM_API_KEY"), reason="无 LLM_API_KEY")
class TestHikvisionAdvisorRealLLM:
    def test_advisor_parses_instruction_to_plan(self):
        """真实 LLM:验证 openai structured output 真实可用"""
        from stability_harness_loop_multiagent.core.bus import EventBus
        from stability_harness_loop_multiagent.core.agent import AgentSpec
        from stability_harness_loop_multiagent.business.hikvision.advisor import HikvisionAdvisor
        from stability_harness_loop_multiagent.business.hikvision.llm import get_client
        
        bus = EventBus()
        spec = AgentSpec(role="advisor_under_test", category="advisor")
        client = get_client()
        advisor = HikvisionAdvisor(
            bus=bus, spec=spec,
            instruction="重启3次后检查在线",
            llm_parse=lambda instr: client.chat_json(ADVISOR_SYSTEM_PROMPT, instr, LLMPlan)
        )
        advisor.start()
        # 真实 LLM 返回,验证 structured output 字段齐全
        assert "operations" in advisor._plan
        assert isinstance(advisor._plan["operations"], list)
```

### 7.6 L3 真机验证(手动,5 轮)

```bash
pytest -m Stability_0001 --real-device --rounds 5
pytest -m "重启稳定性" --real-device --rounds 5

# 真机验证清单
- [ ] 重启 → wait_online → remote_open → 查事件链 → assert
- [ ] 5 轮 verdict 分布(pass/warn/fail)
- [ ] remote_open/lock_open/lock_closed/recovered 状态完整
- [ ] LLM 解析计划正确(真实 OpenRouter)
- [ ] 治理 fail-closed 闸门正常
```

### 7.7 核心不变量守护

**已有 5 大不变量**(全部保留,零改动):

| 不变量 | 守护测试 |
|--------|---------|
| 循环终止 | `test_stability_harness_loop_multiagent_smoke.py` |
| 裁决产生 | 同上 |
| 事件扇出 | 同上 |
| 事实独裁 | `test_decision_authority_fact_dictatorship_unit` |
| 引擎隔离 | `test_harness_regression.py` |

**新增 4 大不变量**:

| 不变量 | 守护测试 | 验证方式 |
|--------|---------|---------|
| 能力组合等价 | `test_worker_migration_equivalence.py` | 同一用例新旧路径产出相同 facts |
| schema 向后兼容 | `test_scenario_schema_compat.py` | 旧 YAML(stress+probe)能被新 schema 加载 |
| LLM 回退安全 | `test_llm_fallback.py` | 无 LLM_API_KEY 时回退规则,verdict 正常 |
| httpx Digest 正确 | `test_client_real_device.py`(slow) | 真机验证 Digest Auth 头 |

### 7.8 测试覆盖目标

| 模块 | 目标覆盖率 | 验证方式 |
|------|-----------|---------|
| `core/` | ≥ 95% | 纯逻辑单测 |
| `harness/` | ≥ 90% | 纯逻辑单测 |
| `loop/` | ≥ 90% | 纯逻辑单测 |
| `multi_agent/` | ≥ 70% | 纯逻辑 + 真实环境 |
| `business/hikvision/capabilities/` | ≥ 60% | 纯逻辑 + 真实环境 |
| `business/hikvision/scenario_schema.py` | ≥ 90% | 纯逻辑单测 |
| `business/hikvision/client.py` | ≥ 50% | 真实环境(无设备时低覆盖,可接受) |
| `business/hikvision/llm.py` | ≥ 50% | 真实 LLM(无 key 时低覆盖,可接受) |

### 7.9 CI 验证流程

```yaml
jobs:
  test:
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - run: pip install -e ".[business,dev]"
      # L1 纯逻辑单测(始终运行)
      - run: pytest tests/ -v -m "not slow and not real_device and not real_llm"
      # 引擎隔离守护
      - run: pytest tests/test_harness_regression.py -v
      # L2 真实环境集成(仅在有 secrets 时运行)
      - run: pytest -m "real_device" -v
        if: ${{ secrets.HIK_HOST != '' }}
        env: { HIK_HOST: ..., HIK_PASSWORD: ... }
      - run: pytest -m "real_llm" -v
        if: ${{ secrets.LLM_API_KEY != '' }}
        env: { LLM_API_KEY: ... }
```

---

## 8. 分支策略与实施顺序

### 8.1 分支策略(用户确认)

所有改造分支合并到当前工作分支,**不合并到 main**:

```
当前工作分支(非 main)
  ↑
  ├── merge PR1(client.py → httpx)
  ├── merge PR2(llm.py + advisor.py → openai SDK)
  ├── merge PR3(scenario_schema.py → pydantic)
  ├── merge PR4(capabilities/ + 删 worker.py + 删所有 fake)
  └── merge PR5(rich 终端美化)

main 分支:保持稳定,用户手动确认才合并
```

### 8.2 实施顺序(7 个 PR,每个可独立验证)

PR4 拆分为 3 个子 PR,降低单次改造风险:

| 阶段 | 改动 | 验证 | 风险 |
|------|------|------|------|
| **PR1** client.py → httpx | 仅 `client.py` + 相关测试 | 纯逻辑测试全绿 + 真机冒烟 | 低 |
| **PR2** llm.py + advisor.py → openai SDK | `llm.py` + `advisor.py` + `LLMPlan` | 纯逻辑测试全绿 + 真实 LLM 冒烟 | 中 |
| **PR3** scenario_schema.py → pydantic | `scenario_schema.py` 改 pydantic + 旧字段迁移层 | 纯逻辑测试全绿 + 所有 YAML 加载 | 中 |
| **PR4a** capabilities/ 能力原子化(新建,并存) | 新建 `capabilities/` 子包 + 实现 15 个能力,`worker.py` 保留不动 | 新增能力单测全绿,旧路径零影响 | 中 |
| **PR4b** scenario_worker.py 切换到 capabilities | 重构 `scenario_worker.py` 调用 capabilities,`worker.py` 仍保留 | 纯逻辑测试全绿 + 真机 5 轮验证新旧路径 facts 等价 | 高 |
| **PR4c** 删除 worker.py + 删除所有 fake + 删除 --dry-run | 删除 `worker.py` / `tests/fakes/` / `FakeScenarioAdapter` / `FakeTargetAdapter` / `examples/smoke.py` / `--dry-run` | 纯逻辑测试全绿 + 真机 5 轮 | 中 |
| **PR5** rich 终端美化 | `examples/_report.py` + `examples/*.py` | 人工验证 + 冒烟 | 低 |

### 8.3 回滚策略

| PR | 回滚方式 |
|----|---------|
| PR1 | `git revert` |
| PR2 | `git revert` |
| PR3 | `git revert` + 保留旧 dataclass |
| PR4a | `git revert`(新增文件,不影响旧路径) |
| PR4b | `git revert`(scenario_worker.py 回退到旧实现) |
| PR4c | 不能简单 revert(已删除文件),需从备份分支恢复 |
| PR5 | `git revert` |

**PR4b 风险最高**(新旧路径切换),实施时:
- 在独立分支 `feat/capabilities-switch` 完成
- 先用新旧路径并存模式跑真机 5 轮,验证 facts 完全等价
- 等价性确认后才删除旧路径(PR4c)
- 用户手动验证 5 轮真机 + 真实 LLM 后才合并

**PR4c 删除阶段**:
- 在独立分支 `feat/cleanup-fakes` 完成
- 删除前先标记所有 fake 引用点(grep `FakeHikvisionClient` / `FakeScenarioAdapter` 等)
- 一次性删除所有 fake + worker.py + --dry-run + smoke.py
- 删除后纯逻辑测试必须全绿

---

## 9. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 删除 fake 后 L1 单测覆盖降低 | 保留所有纯逻辑单测,只删 fake 相关 |
| 无设备时 client.py 覆盖率低 | 接受(用户原则:无环境 skip) |
| 真机测试耗时长(每轮 90s × 5 轮) | L2 单轮验证 + L3 手动 5 轮,CI 不跑 5 轮 |
| PR4b 新旧路径切换破坏现有测试 | 新旧路径并存跑 5 轮真机验证 facts 等价后才切换 |
| PR4c 删除 worker.py 后无法回退 | 保留 `feat/capabilities-refactor` 备份分支,不删除 |
| pydantic schema 向后兼容 | PR3 加迁移层,旧 YAML(stress+probe)自动转新格式 |
| openai SDK structured output 不稳定 | 保留规则兜底(LLM 失败回退) |
| baseline 插值机制扩展 _interp_env 可能引入 bug | 单测覆盖 `${baseline.xxx}` 插值场景 |

---

## 10. 验收标准

### 10.1 功能验收

- [ ] `pytest -m Stability_0001` 能跑通(真机)
- [ ] `pytest -m "重启稳定性"` 能筛选多用例
- [ ] `pytest --markers | grep Stability_` 列出所有用例 marker
- [ ] 78 条自动用例 YAML 全部能加载(schema 校验通过)
- [ ] 真机 5 轮验证 verdict 分布正常
- [ ] 真实 LLM 解析计划正常(structured output)
- [ ] 治理 fail-closed 闸门正常

### 10.2 架构验收

- [ ] `core/` / `harness/` / `loop/` / `multi_agent/` 零改动(只换轮子)
- [ ] `tests/test_harness_regression.py` 引擎隔离守护通过
- [ ] 5 大不变量测试全绿(循环终止/裁决产生/事件扇出/事实独裁/引擎隔离)
- [ ] `business/hikvision/worker.py` 已删除
- [ ] `tests/fakes/` 已删除
- [ ] `examples/smoke.py` 已删除
- [ ] `--dry-run` 选项已删除

### 10.3 代码质量验收

- [ ] 单文件 ≤ 300 行(`_base.py` 可放宽到 350)
- [ ] 零手写 Digest Auth(httpx-auth 替代)
- [ ] 零手写 LLM JSON 抽取(openai structured output 替代)
- [ ] 零手写 dataclass 校验(pydantic 替代)
- [ ] 零 mock 框架依赖
- [ ] 零 fake 类

### 10.4 测试验收

- [ ] L1 纯逻辑单测全绿(无环境依赖)
- [ ] L2 真实环境集成测试:有环境时全绿,无环境时 skip
- [ ] L3 真机 5 轮验证通过(用户手动)
- [ ] 130 tests 中纯逻辑部分全绿(fake 相关测试已删除)

---

## 11. 不在本次范围

- 手动用例(约 30 条)的自动化:暂不纳入设计范围
- LangChain / LangGraph 集成:场景错位,不引入
- Function Tool / MCP:Worker 保持确定性,不引入
- 跨进程 Agent 协作:当前单进程,EventBus 已足够
- LLM 动态选 Worker:破坏确定性,不引入

---

## 12. 关键决策记录

| # | 决策 | 理由 |
|---|------|------|
| 1 | 保留完整架构,只换轮子 | 架构基座稳定,核心不变量全保留 |
| 2 | YAML + conftest.py 动态 marker | 满足"1 marker = 1 用例"硬规则,零代码新增用例 |
| 3 | 能力原子化 + YAML 组合 | 替代 worker.py 1020 行硬编码,可组合可复用 |
| 4 | Worker 保持确定性,不引入 Function Tool | 稳定性测试需要可重复性,LLM 不参与执行 |
| 5 | 用 openai SDK 不用 langchain | 场景错位 + 依赖爆炸,只复用 LLM 调用这一小点 |
| 6 | 零 fake 零 mock 测试 | 避免自证陷阱,所有事件链真实设备触发 |
| 7 | worker.py 废弃删除 | 为长期考虑,不保留旧路径 |
| 8 | 只处理自动用例(78 条) | 手动用例(30 条)暂不纳入 |
| 9 | 合并到当前工作分支,不合并 main | main 保持稳定,用户手动确认 |
| 10 | 7 个 PR 分阶段实施(PR4 拆 3 子 PR) | 每个 PR 独立可回滚,PR4b 风险最高需真机验证新旧路径等价 |

---

## 附录 A:与业界对比

### A.1 vs LangChain 生态

| 我们的部分 | LangChain 对应 | 是否造轮子 |
|-----------|---------------|-----------|
| `llm.py` 手写 urllib | `ChatOpenAI` | ✅ 真造轮子(改用 openai SDK) |
| `advisor.py` JSON 抽取 | `with_structured_output` | ✅ 真造轮子(改用 pydantic) |
| `capabilities/` YAML 组合 | LCEL 管道 | ❌ 不是造轮子(LCEL 为 LLM 推理,我们确定性操作链) |
| `ControlLoop` | LangGraph | ⚠️ 部分重叠(我们更领域特化) |
| `DecisionAuthority` 事实独裁 | 无对应 | ❌ 不是造轮子(稳定性测试特有) |
| `harness/` 治理+看门狗 | LangGraph interrupt | ❌ 不是造轮子(进程级治理) |

**结论**:只在 LLM 调用这一小点复用 openai SDK,其他部分是领域特化,不是造轮子。

### A.2 vs Robot Framework

| 维度 | Robot Framework | 我们的能力组合 |
|------|----------------|--------------|
| 关键字驱动 | ✅ 核心特性 | ✅ capabilities 即关键字 |
| YAML 数据驱动 | ❌ 用 .robot 文件 | ✅ YAML |
| pytest 集成 | ✅ robot.run | ✅ 原生 pytest marker |
| 稳定性测试特化 | ❌ 通用 | ✅ 事实独裁/fail-closed/看门狗 |

**结论**:能力原子化的思路与 Robot Framework 关键字驱动一致,但我们是稳定性测试领域特化,且原生 pytest 基座。

---

*spec 终版,待用户复审*
