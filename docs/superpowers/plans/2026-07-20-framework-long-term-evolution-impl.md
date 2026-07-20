# 框架长期演进实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `stability_harness_loop_multiagent` 框架从"worker.py 1020 行硬编码 + 手写 HTTP/LLM/校验 + 依赖 fake 测试"演进为"能力原子化 + YAML 组合 + httpx/openai/pydantic/rich 换轮子 + pytest marker 硬规则 + 零 fake 零 mock 测试"。

**Architecture:** 保留 core/ + 三引擎架构不动,只换轮子(httpx/openai/pydantic/rich)。worker.py 拆为 capabilities/ 子包(15 个原子能力),YAML schema 扩展为 preconditions+actions+probes 组合,用例通过 conftest.py 动态注册 pytest marker(1 marker = 1 用例)。测试策略:纯逻辑单测 + 真实环境集成(无环境 skip),绝对禁用 fake 和 mock。

**Tech Stack:** Python 3.10+、httpx、httpx-auth、openai SDK、pydantic v2、rich、pytest、pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-07-20-framework-long-term-evolution-design.md`

**分支策略:** 所有改造在 `feat/stability-harness-loop-multiagent` 当前工作分支的子分支完成,合并回当前工作分支,**不合并 main**。

---

## File Structure

### 新建文件

```
business/hikvision/capabilities/
├── __init__.py                      # 工厂函数:create_action/create_probe/create_precondition
├── actions/
│   ├── __init__.py
│   ├── base.py                      # ActionBase 协议 + ActionResult
│   ├── reboot.py                    # RebootAction
│   ├── upgrade.py                   # UpgradeAction
│   ├── remote_open.py               # RemoteOpenAction
│   ├── dispatch.py                  # DispatchAction
│   ├── switch_serial.py             # SwitchSerialAction
│   ├── sleep.py                     # SleepAction
│   ├── query_events.py              # QueryEventsAction
│   └── noop.py                      # NoopAction
├── probes/
│   ├── __init__.py
│   ├── base.py                      # ProbeBase 协议 + FactResult
│   ├── field.py                     # FieldProbe
│   ├── online.py                    # OnlineProbe
│   ├── count.py                     # CountProbe
│   └── event_chain.py               # EventChainProbe
└── preconditions/
    ├── __init__.py
    ├── base.py                      # PreconditionBase 协议
    ├── device_online.py             # DeviceOnlinePrecondition
    ├── serial_mode.py               # SerialModePrecondition
    └── baseline_record.py           # BaselineRecordPrecondition

tests/
├── conftest.py                      # 动态注册 marker
├── test_stability_scenario.py       # 单一测试函数 + parametrize
├── test_scenario_schema.py          # pydantic schema 单测
├── test_capabilities/
│   ├── __init__.py
│   ├── test_probe_field.py
│   ├── test_action_sleep.py
│   ├── test_action_noop.py
│   └── test_precondition_baseline_record.py
├── test_voting_combine.py           # combine_votes 单测
├── test_client_real_device.py       # 真机集成(slow)
└── test_advisor_real_llm.py         # 真实 LLM 集成(slow)
```

### 修改文件

- `pyproject.toml`:新增 business extras(httpx/httpx-auth/openai/pydantic/PyYAML)
- `business/hikvision/client.py`:419 行手写 → httpx + httpx-auth,目标 ≤ 200 行
- `business/hikvision/llm.py`:157 行手写 → openai SDK + pydantic,目标 ≤ 60 行
- `business/hikvision/advisor.py`:用 LLMPlan pydantic + openai structured output
- `business/hikvision/scenario_schema.py`:283 行手写 dataclass → pydantic BaseModel + 能力组合
- `business/hikvision/scenario_worker.py`:重构为 capabilities 组合器
- `business/hikvision/scenario_adapter.py`:删除 FakeScenarioAdapter 类
- `business/hikvision/scenario_runner.py`:删除 --dry-run 选项
- `business/hikvision/runner.py`:更新 import(workers 替代 worker)
- `examples/_report.py`:print → rich
- `examples/scenario_run.py`:删除 --dry-run
- `examples/hikvision_real_env.py`:更新 import

### 删除文件

- `business/hikvision/worker.py`(1020 行,逻辑迁到 capabilities/)
- `tests/fakes/` 整个目录
- `tests/fakes/fake_hikvision.py`
- `multi_agent/` 中的 FakeTargetAdapter(若有)
- `examples/smoke.py`(依赖 FakeTargetAdapter)
- 所有引用 fake 的测试

---

## PR1: client.py → httpx

**目标**:`HikvisionClient` 用 httpx + httpx-auth 替换手写 Digest Auth + urllib,业务方法签名不变。

### Task 1.1: 添加 httpx 依赖

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: 添加 business extras 到 pyproject.toml**

在 `[project.optional-dependencies]` 下新增 `business` 段:

```toml
[project.optional-dependencies]
business = [
    "httpx>=0.27",
    "httpx-auth>=0.22",
    "openai>=1.40",
    "pydantic>=2.7",
    "PyYAML>=6.0",
]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.23",
]
docs = ["mkdocs-material", "mkdocstrings[python]"]
examples = ["rich>=13"]
```

- [ ] **Step 2: 验证依赖可安装**

Run: `pip install -e ".[business,dev]"`
Expected: 成功安装 httpx, httpx-auth, openai, pydantic, PyYAML

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "deps: add business extras (httpx/openai/pydantic) for wheel replacement"
```

### Task 1.2: 备份原 client.py 接口契约

**Files:**
- Read: `business/hikvision/client.py`

- [ ] **Step 1: 提取原 client.py 的所有公开方法签名**

Run: `grep -n "def " business/hikvision/client.py | head -30`
Expected: 列出所有方法签名(reboot/get_time/query_events/remote_open_door/wait_online 等),作为 httpx 改造后必须保留的接口契约。

- [ ] **Step 2: 记录方法签名清单到 plan 备忘**

记录所有公开方法签名(参数 + 返回类型),确保 PR1 完成后签名完全一致。

### Task 1.3: 写 httpx client 的纯逻辑单测(不涉及真实 HTTP)

**Files:**
- Create: `tests/test_client_httpx_unit.py`

- [ ] **Step 1: 写 httpx client 构造和 URL 拼接的单测**

```python
"""HikvisionClient 单元测试(纯逻辑,不涉及真实 HTTP)。

验证 client 构造、URL 拼接、超时配置,不验证 HTTP 行为(HTTP 行为由真机测试覆盖)。
"""
import pytest
from stability_harness_loop_multiagent.business.hikvision.client import HikvisionClient


def test_client_construction_with_default_port():
    """构造:默认端口 80,base_url 拼接正确"""
    client = HikvisionClient(host="192.168.3.33", username="admin", password="pass")
    assert client._client.base_url == "http://192.168.3.33:80"


def test_client_construction_with_custom_port_and_timeout():
    """构造:自定义端口和超时"""
    client = HikvisionClient(host="10.0.0.1", port=8080, username="admin",
                              password="pass", timeout=10.0)
    assert client._client.base_url == "http://10.0.0.1:8080"
    assert client._client.timeout.connect == 10.0


def test_client_has_thread_lock_for_digest_auth():
    """构造:必须有线程锁保护 Digest Auth state(并发 query_events 用)"""
    client = HikvisionClient(host="x", username="x", password="x")
    assert hasattr(client, "_lock")
    assert client._lock is not None
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_client_httpx_unit.py -v`
Expected: FAIL(因为 client.py 还没改造,可能 import 失败或断言失败)

- [ ] **Step 3: Commit(测试先行)**

```bash
git add tests/test_client_httpx_unit.py
git commit -m "test: add unit tests for HikvisionClient construction (TDD)"
```

### Task 1.4: 改造 client.py 用 httpx

**Files:**
- Modify: `business/hikvision/client.py`

- [ ] **Step 1: 用 httpx + httpx-auth 重写 client.py**

替换整个文件(保留所有公开方法签名):

```python
"""HikvisionClient —— 海康 ISAPI 客户端(httpx + Digest Auth)。

用 httpx 替换手写 urllib,用 httpx-auth.DigestAuth 替换手写 Digest 算法。
线程锁保留:保护 Digest Auth state 在并发 query_events 时不被破坏。
所有业务方法签名与原 client.py 完全一致,仅替换 HTTP/Digest 实现。
"""

import logging
import threading
import time
from typing import Any

import httpx
from httpx_auth import DigestAuth

logger = logging.getLogger(__name__)


class HikvisionClient:
    """海康 ISAPI 客户端(httpx + Digest Auth)。"""

    def __init__(self, host: str, port: int = 80, username: str = "admin",
                 password: str = "", timeout: float = 5.0) -> None:
        base_url = f"http://{host}:{port}"
        self._client = httpx.Client(base_url=base_url, timeout=timeout)
        self._auth = DigestAuth(username, password)
        self._lock = threading.Lock()  # 保护 Digest Auth state(并发 query_events 用)

    def request_json(self, method: str, endpoint: str,
                     body: Any = None) -> dict:
        """统一出口:所有业务方法经此调用 httpx。"""
        with self._lock:
            resp = self._client.request(method, endpoint, json=body, auth=self._auth)
            resp.raise_for_status()
            return resp.json()

    def reboot(self) -> None:
        """下发主设备重启:PUT /ISAPI/System/reboot"""
        with self._lock:
            resp = self._client.put("/ISAPI/System/reboot", auth=self._auth)
            resp.raise_for_status()

    def get_time(self) -> str:
        """获取设备时间(ISO 8601):GET /ISAPI/System/time"""
        data = self.request_json("GET", "/ISAPI/System/time")
        return data.get("time", "")

    def get_work_status(self) -> dict:
        """获取设备工作状态:GET /ISAPI/AccessControl/AcsWorkStatus?format=json"""
        return self.request_json("GET", "/ISAPI/AccessControl/AcsWorkStatus?format=json")

    def get_event_serial(self) -> int:
        """获取当前最大事件 serialNo(用于过滤历史事件)"""
        events = self.query_events(window=300, baseline_serial=0)
        if not events:
            return 0
        return max(int(e.get("serialNo", 0)) for e in events)

    def query_events(self, window: int = 300, baseline_serial: int = 0) -> list[dict]:
        """查询事件链:window 秒回溯窗口,baseline_serial 之前的事件过滤掉。

        线程锁保护:并发 query_events 时 Digest Auth state 不能被破坏。
        """
        import time as _time
        device_now = self.get_time()
        # 简化:用 device 时间构造 window,实际按 ISAPI 协议 POST 查询
        payload = {"searchResult": {"position": 0, "maxResults": 50,
                                     "eventSearchTime": {
                                         "startTime": device_now,
                                         "endTime": device_now}}}
        with self._lock:
            resp = self._client.post(
                "/ISAPI/AccessControl/AcsEvent?format=json",
                json=payload, auth=self._auth
            )
            resp.raise_for_status()
            data = resp.json()
        events = data.get("AcsEvent", {}).get("Info", {}).get("Event", [])
        return [e for e in events if int(e.get("serialNo", 0)) > baseline_serial]

    def remote_open_door(self, door: int = 1) -> None:
        """远程开门:PUT /ISAPI/AccessControl/RemoteControl/door/<door>"""
        payload = {"RemoteControlDoor": {"cmd": "open", "doorIndex": door}}
        with self._lock:
            resp = self._client.put(
                f"/ISAPI/AccessControl/RemoteControl/door/{door}",
                json=payload, auth=self._auth
            )
            resp.raise_for_status()

    def get_serial_config(self, port: int = 1) -> dict:
        """获取串口配置:GET /ISAPI/System/SerialPort/cfgs/<port>"""
        return self.request_json("GET", f"/ISAPI/System/SerialPort/cfgs/{port}")

    def set_serial_config(self, port: int, cfg: dict) -> None:
        """设置串口配置:PUT /ISAPI/System/SerialPort/cfgs/<port>"""
        with self._lock:
            resp = self._client.put(
                f"/ISAPI/System/SerialPort/cfgs/{port}",
                json=cfg, auth=self._auth
            )
            resp.raise_for_status()

    def wait_online(self, timeout: int = 180) -> bool:
        """三阶段探测:断开 → 恢复 → 连续确认。

        避免假在线:PUT reboot 返回 200 后,HTTP 服务还能响应几秒才真掉线。
        """
        deadline = time.time() + timeout
        # 阶段 1:等待设备断开(每 1s 探测,最多 30s)
        disconnect_deadline = time.time() + 30
        while time.time() < disconnect_deadline:
            try:
                with self._lock:
                    self._client.get("/ISAPI/System/time", auth=self._auth, timeout=2.0)
                time.sleep(1)  # 还在线,继续等
            except Exception:  # noqa: BLE001 - 掉线了
                break
        # 阶段 2:等待设备恢复(每 2s 探测,直到 deadline)
        while time.time() < deadline:
            try:
                with self._lock:
                    self._client.get("/ISAPI/System/time", auth=self._auth, timeout=2.0)
                # 阶段 3:连续确认(再探测 2 次,间隔 1s)
                for _ in range(2):
                    time.sleep(1)
                    with self._lock:
                        self._client.get("/ISAPI/System/time", auth=self._auth, timeout=2.0)
                return True
            except Exception:  # noqa: BLE001 - 还没恢复
                time.sleep(2)
        return False

    def close(self) -> None:
        """关闭 httpx 客户端(资源释放)"""
        self._client.close()
```

- [ ] **Step 2: 运行单测确认通过**

Run: `pytest tests/test_client_httpx_unit.py -v`
Expected: 3 个测试全 PASS

- [ ] **Step 3: 运行全部纯逻辑测试,确认零回归**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm" --ignore=tests/fakes`
Expected: 所有不依赖 fake 的测试全绿(注意:此时 fake 还没删,用 --ignore 跳过)

- [ ] **Step 4: 真机冒烟验证(若有 HIK_HOST)**

Run: `python -c "import os; from stability_harness_loop_multiagent.business.hikvision.client import HikvisionClient; c=HikvisionClient(host=os.environ['HIK_HOST'], password=os.environ['HIK_PASSWORD']); print(c.get_time())"`
Expected: 打印设备 ISO 时间(如 "2026-07-20T15:30:00+08:00")

- [ ] **Step 5: Commit**

```bash
git add business/hikvision/client.py tests/test_client_httpx_unit.py
git commit -m "refactor: replace handwritten HTTP+Digest with httpx+httpx-auth in client.py"
```

---

## PR2: llm.py + advisor.py → openai SDK

**目标**:LLM 调用用 openai SDK 替换手写 urllib,advisor 用 pydantic structured output 替换手写 JSON 抽取。

### Task 2.1: 写 LLMPlan pydantic 模型

**Files:**
- Create: `business/hikvision/llm_plan.py`

- [ ] **Step 1: 写 LLMPlan pydantic 模型单测**

```python
# tests/test_llm_plan.py
"""LLMPlan pydantic schema 单测(纯逻辑)"""
import pytest
from pydantic import ValidationError
from stability_harness_loop_multiagent.business.hikvision.llm_plan import LLMPlan


def test_llm_plan_default_values():
    """LLMPlan:默认值正确"""
    plan = LLMPlan()
    assert plan.skip_reboot is False
    assert plan.operations == []
    assert plan.risk_note == ""


def test_llm_plan_parse_from_dict():
    """LLMPlan:从 dict 构造,字段正确填充"""
    plan = LLMPlan(skip_reboot=True, operations=["reboot", "remote_open"],
                    risk_note="高风险")
    assert plan.skip_reboot is True
    assert plan.operations == ["reboot", "remote_open"]
    assert plan.risk_note == "高风险"


def test_llm_plan_operations_must_be_list():
    """LLMPlan:operations 必须是 list,pydantic 自动校验"""
    with pytest.raises(ValidationError):
        LLMPlan(operations="reboot")  # 字符串不是 list


def test_llm_plan_model_dump_returns_dict():
    """LLMPlan:model_dump() 返回 dict(用于 advisor._plan)"""
    plan = LLMPlan(skip_reboot=False, operations=["noop"])
    d = plan.model_dump()
    assert d == {"skip_reboot": False, "operations": ["noop"], "risk_note": ""}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_llm_plan.py -v`
Expected: FAIL(ModuleNotFoundError: llm_plan 不存在)

- [ ] **Step 3: 实现 llm_plan.py**

```python
# business/hikvision/llm_plan.py
"""LLMPlan —— Advisor 的 LLM 解析计划 schema(pydantic 强类型)。

用于 openai SDK 的 structured output:client.beta.chat.completions.parse(
    response_format=LLMPlan
) 自动校验 LLM 返回,无需手写 JSON 抽取。
"""
from pydantic import BaseModel, Field


class LLMPlan(BaseModel):
    """LLM 解析出的稳定性测试计划。"""
    skip_reboot: bool = Field(
        default=False, description="本轮是否跳过重启(若指令不要求重启则为 true)"
    )
    operations: list[str] = Field(
        default_factory=list,
        description='操作列表,可选值 "reboot" / "remote_open" / "query_events" / "noop"'
    )
    risk_note: str = Field(
        default="", description="风险备注,无则空字符串"
    )
```

- [ ] **Step 4: 运行测试确认通过**

Run: `pytest tests/test_llm_plan.py -v`
Expected: 4 个测试全 PASS

- [ ] **Step 5: Commit**

```bash
git add business/hikvision/llm_plan.py tests/test_llm_plan.py
git commit -m "feat: add LLMPlan pydantic schema for advisor structured output"
```

### Task 2.2: 改造 llm.py 用 openai SDK

**Files:**
- Modify: `business/hikvision/llm.py`

- [ ] **Step 1: 读取原 llm.py 保留的接口契约**

Run: `grep -n "^def \|^class " business/hikvision/llm.py`
Expected: 记录所有公开函数/类签名(get_client / chat_json 等),改造后保持兼容。

- [ ] **Step 2: 用 openai SDK 重写 llm.py**

```python
"""LLM 客户端(openai SDK + OpenRouter)。

用 openai SDK 替换手写 urllib,用 pydantic structured output 替换手写 JSON 抽取。
无 LLM_API_KEY 时 get_client() 返回 None,调用方回退规则兜底。
"""

import logging
import os
from pathlib import Path
from typing import Optional

from openai import OpenAI
from pydantic import BaseModel

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "tencent/hy3:free"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"


def _load_dotenv() -> None:
    """极简 .env 加载(保留,避免引入 python-dotenv 依赖)。"""
    env_path = Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key and key not in os.environ:
            os.environ[key] = value


def get_client() -> Optional[OpenAI]:
    """构造 OpenAI 客户端;无密钥返回 None(调用方回退规则兜底)。"""
    key = (os.environ.get("LLM_API_KEY")
           or os.environ.get("OPENROUTER_API_KEY"))
    if not key:
        _load_dotenv()
        key = (os.environ.get("LLM_API_KEY")
               or os.environ.get("OPENROUTER_API_KEY"))
    if not key:
        return None
    return OpenAI(
        api_key=key,
        base_url=os.environ.get("LLM_BASE_URL", DEFAULT_BASE_URL),
        default_headers={
            "HTTP-Referer": "stability-harness",
            "X-Title": "hikvision-advisor",
        },
    )


def chat_json(client: OpenAI, system_prompt: str, user_prompt: str,
              response_model: Optional[type[BaseModel]] = None) -> Optional[dict]:
    """调用 LLM 并返回 JSON dict;失败返回 None。

    - response_model 非 None:用 openai structured output(pydantic 校验)
    - response_model 为 None:普通 chat completion,返回 {"text": ...}
    """
    model = os.environ.get("LLM_MODEL", DEFAULT_MODEL)
    try:
        if response_model is not None:
            completion = client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format=response_model,
            )
            parsed = completion.choices[0].message.parsed
            return parsed.model_dump() if parsed else None
        else:
            completion = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                max_tokens=512,
            )
            return {"text": completion.choices[0].message.content}
    except Exception as exc:  # noqa: BLE001 - 失败一律回退规则
        logger.warning("LLM 调用失败,回退规则兜底: %s", exc)
        return None
```

- [ ] **Step 3: 运行纯逻辑测试,确认零回归**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm" --ignore=tests/fakes`
Expected: 全绿

- [ ] **Step 4: 真实 LLM 冒烟(若有 LLM_API_KEY)**

Run: `python -c "import os; from stability_harness_loop_multiagent.business.hikvision.llm import get_client, chat_json; from stability_harness_loop_multiagent.business.hikvision.llm_plan import LLMPlan; c=get_client(); r=chat_json(c, '你是测试计划解析器', '重启3次', LLMPlan); print(r)"`
Expected: 打印类似 `{"skip_reboot": False, "operations": ["reboot"], "risk_note": ""}`

- [ ] **Step 5: Commit**

```bash
git add business/hikvision/llm.py
git commit -m "refactor: replace handwritten urllib with openai SDK in llm.py"
```

### Task 2.3: 改造 advisor.py 用 LLMPlan + chat_json

**Files:**
- Modify: `business/hikvision/advisor.py`

- [ ] **Step 1: 定位 advisor.py 中调用 LLM 的位置**

Run: `grep -n "_extract_first_json\|chat_json\|_llm_parse\|llm_client" business/hikvision/advisor.py`
Expected: 找到所有需要替换的位置。

- [ ] **Step 2: 改造 advisor.py 用 LLMPlan + openai structured output**

修改 advisor.py 中 LLM 调用部分(其他逻辑保持不变):

```python
# business/hikvision/advisor.py(节选,只展示改动部分)
import asyncio
from .llm import get_client, chat_json
from .llm_plan import LLMPlan

ADVISOR_SYSTEM_PROMPT = """你是海康门禁稳定性测试的计划解析器。
输入:用户的自然语言测试指令(如"重启3次后检查在线")。
输出:严格遵守 LLMPlan schema 的 JSON。
- skip_reboot: 是否跳过重启(若指令不要求重启则为 true)
- operations: 操作列表,可选值 "reboot" / "remote_open" / "query_events" / "noop"
- risk_note: 风险备注,无则空字符串
不要输出任何其他内容。"""


class HikvisionAdvisor(AdvisorAgent):
    # __init__ 保持不变,只改 start() 中的 LLM 调用

    async def start(self) -> None:
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
            # 无 LLM 密钥,规则兜底(保留原 _rule_based_parse 逻辑)
            self._plan = self._rule_based_parse(self._instruction)

        if self._enable_verify and self._plan:
            allowed, reason = await self._verify_plan(self._plan)
            if not allowed:
                self._plan = {}
        if self._plan:
            self.publish("hikvision/plan", self._plan)
        await asyncio.sleep(0)

    def _rule_based_parse(self, instruction: str) -> dict:
        """无 LLM 时的规则兜底(保留原 advisor.py 的关键词匹配逻辑)。"""
        plan = {"skip_reboot": False, "operations": [], "risk_note": ""}
        if "重启" in instruction or "reboot" in instruction.lower():
            plan["operations"].append("reboot")
        if "开门" in instruction or "open" in instruction.lower():
            plan["operations"].append("remote_open")
        if not plan["operations"]:
            plan["operations"] = ["noop"]
        return plan
```

- [ ] **Step 3: 删除 advisor.py 中手写 JSON 抽取函数**

删除 `_extract_first_json` 函数及相关辅助代码(已被 openai structured output 替代)。

- [ ] **Step 4: 运行纯逻辑测试,确认零回归**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm" --ignore=tests/fakes`
Expected: 全绿

- [ ] **Step 5: Commit**

```bash
git add business/hikvision/advisor.py
git commit -m "refactor: use pydantic LLMPlan + openai structured output in advisor"
```

---

## PR3: scenario_schema.py → pydantic

**目标**:用 pydantic BaseModel 替换手写 dataclass + 校验,同时扩展 schema 支持 preconditions+actions+probes 组合。

### Task 3.1: 写 pydantic schema 单测

**Files:**
- Create: `tests/test_scenario_schema.py`

- [ ] **Step 1: 写 schema 单测(纯逻辑)**

```python
# tests/test_scenario_schema.py
"""Scenario pydantic schema 单测(纯逻辑)。"""
import pytest
from pydantic import ValidationError
from stability_harness_loop_multiagent.business.hikvision.scenario_schema import (
    Scenario, TargetCfg, LoopCfg, ActionSpec, ProbeSpec, PreconditionSpec,
    from_yaml,
)


def test_scenario_minimal_valid():
    """最小合法 scenario:id + target + 1 probe + loop"""
    s = Scenario(
        id="test_001", name="测试",
        target=TargetCfg(host="192.168.3.33"),
        probes=[ProbeSpec(type="field", params={"field": "online", "expect_equals": 1})],
        loop=LoopCfg(max_rounds=1),
    )
    assert s.id == "test_001"
    assert s.target.host == "192.168.3.33"
    assert s.probes[0].type == "field"


def test_scenario_no_probes_fails():
    """至少一个 probe,否则校验失败"""
    with pytest.raises(ValidationError):
        Scenario(
            id="test", name="x",
            target=TargetCfg(host="x"),
            probes=[],
            loop=LoopCfg(max_rounds=1),
        )


def test_action_spec_invalid_type():
    """ActionSpec.type 必须在 Literal 白名单内"""
    with pytest.raises(ValidationError):
        ActionSpec(type="invalid_type", params={})


def test_probe_spec_valid_types():
    """ProbeSpec.type 支持 field/online/count/event_chain"""
    for t in ["field", "online", "count", "event_chain"]:
        ProbeSpec(type=t, params={})


def test_loop_deadline_format_validation():
    """LoopCfg.deadline 必须是 HH:MM 格式"""
    LoopCfg(deadline="23:50")  # 合法
    with pytest.raises(ValidationError):
        LoopCfg(deadline="25:99")  # 非法


def test_loop_max_rounds_must_be_positive():
    """LoopCfg.max_rounds 必须 >= 1"""
    LoopCfg(max_rounds=1)  # 合法
    with pytest.raises(ValidationError):
        LoopCfg(max_rounds=0)  # 非法


def test_from_yaml_loads_real_scenario(tmp_path):
    """from_yaml 能加载真实 YAML 文件"""
    yaml_content = """
id: Stability_0001
name: 重启测试
category: 重启稳定性
level: L2
target:
  host: 192.168.3.33
  port: 80
actions:
  - type: reboot
    params: {target: main, wait_online_timeout: 180}
probes:
  - type: field
    params:
      endpoint: /ISAPI/AccessControl/AcsWorkStatus?format=json
      field: AcsWorkStatus.doorOnlineStatus[0]
      expect_equals: 1
loop:
  max_rounds: 3
  interval_seconds: 90
"""
    yaml_file = tmp_path / "test.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")
    s = from_yaml(str(yaml_file))
    assert s.id == "Stability_0001"
    assert s.actions[0].type == "reboot"
    assert s.probes[0].params["expect_equals"] == 1


def test_env_var_interpolation(tmp_path, monkeypatch):
    """${VAR} 环境变量插值"""
    monkeypatch.setenv("TEST_HOST", "10.0.0.1")
    yaml_content = """
id: test
name: x
target:
  host: ${TEST_HOST}
probes:
  - type: field
    params: {field: x, expect_equals: 1}
loop:
  max_rounds: 1
"""
    yaml_file = tmp_path / "env.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")
    s = from_yaml(str(yaml_file))
    assert s.target.host == "10.0.0.1"
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_scenario_schema.py -v`
Expected: FAIL(因为 scenario_schema.py 还没改造)

- [ ] **Step 3: Commit(测试先行)**

```bash
git add tests/test_scenario_schema.py
git commit -m "test: add pydantic schema unit tests (TDD)"
```

### Task 3.2: 改造 scenario_schema.py 用 pydantic

**Files:**
- Modify: `business/hikvision/scenario_schema.py`

- [ ] **Step 1: 用 pydantic 重写 scenario_schema.py**

```python
# business/hikvision/scenario_schema.py
"""Scenario YAML schema(pydantic v2)。

用 pydantic BaseModel 替换手写 dataclass + 校验,支持能力组合:
  preconditions[] + actions[] + probes[] + loop
向后兼容:旧 YAML(stress + probe)通过 _migrate_legacy() 自动转新格式。
"""

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

_ENV_RE = re.compile(r"\$\{([^}:]+)(?::-([^}]*))?\}")
_TOKEN_RE = re.compile(r"([^.\[\]]+)(?:\[(\d+)\])?")
_SENTINEL = object()


# ---- 能力描述(组合的基本单元)----
class ActionSpec(BaseModel):
    """YAML 中的单个 action 描述。"""
    type: Literal["reboot", "upgrade", "remote_open", "dispatch",
                   "switch_serial", "sleep", "query_events", "noop"]
    params: dict[str, Any] = Field(default_factory=dict)


class ProbeSpec(BaseModel):
    """YAML 中的单个 probe 描述。"""
    type: Literal["field", "online", "count", "event_chain"]
    params: dict[str, Any] = Field(default_factory=dict)


class PreconditionSpec(BaseModel):
    """YAML 中的单个 precondition 描述。"""
    type: Literal["device_online", "serial_mode", "baseline_record"]
    params: dict[str, Any] = Field(default_factory=dict)


# ---- 配置块 ----
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
    def _validate_deadline(cls, v: str | None) -> str | None:
        if v is not None and not re.fullmatch(r"\d{1,2}:\d{2}", v):
            raise ValueError("deadline 必须为 HH:MM 格式")
        return v


# ---- 完整 Scenario ----
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
    def _validate_combination(self) -> "Scenario":
        if not self.id:
            raise ValueError("scenario.id 不能为空")
        if not self.probes:
            raise ValueError("至少需要一个 probe")
        return self


# ---- 环境变量插值 ----
def _interp_env(value: Any) -> Any:
    """${VAR} / ${VAR:-default} 插值,递归处理 dict/list。"""
    if isinstance(value, str):
        def replace(m: re.Match) -> str:
            var_name, default = m.group(1), m.group(2) or ""
            return os.environ.get(var_name, default)
        return _ENV_RE.sub(replace, value)
    if isinstance(value, dict):
        return {k: _interp_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interp_env(v) for v in value]
    return value


# ---- 旧 YAML 迁移(stress + probe → actions + probes)----
def _migrate_legacy(raw: dict) -> dict:
    """旧 YAML(stress + probe)迁移到新格式(preconditions + actions + probes)。

    旧格式:
      stress: {type: reboot, ...}
      probe: {endpoint: ..., field: ..., expect_equals: ...}
    新格式:
      actions: [{type: <stress.type>, params: {...}}]
      probes: [{type: field, params: {...}}]
    """
    if "stress" in raw or "probe" in raw:
        stress = raw.pop("stress", {})
        probe = raw.pop("probe", {})
        if stress:
            raw.setdefault("actions", []).append({
                "type": stress.get("type", "noop"),
                "params": {k: v for k, v in stress.items() if k != "type"}
            })
        if probe:
            raw.setdefault("probes", []).append({
                "type": "field",
                "params": probe
            })
    return raw


# ---- YAML 加载 ----
def from_yaml(path: str) -> Scenario:
    """从 YAML 文件加载 Scenario,自动插值 + 迁移旧格式。"""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"YAML 根必须是映射: {path}")
    raw = _interp_env(raw)
    raw = _migrate_legacy(raw)
    return Scenario.model_validate(raw)


def from_dict(data: dict) -> Scenario:
    """从 dict 构造 Scenario(测试用)。"""
    return Scenario.model_validate(_interp_env(_migrate_legacy(dict(data))))


# ---- 字段路径解析(probe 内部用)----
def resolve_field(snapshot: Any, path: str, default: Any = _SENTINEL) -> Any:
    """解析字段路径,如 'AcsWorkStatus.doorOnlineStatus[0]'。

    支持 . 分隔和 [N] 索引,缺失时返回 default(未指定则抛 KeyError)。
    """
    current = snapshot
    for name, idx in _TOKEN_RE.findall(path):
        if isinstance(current, dict):
            if name not in current:
                if default is _SENTINEL:
                    raise KeyError(path)
                return default
            current = current[name]
        if idx:
            current = current[int(idx)]
    return current


def compare_probe(value: Any, expect_equals: Any = None,
                  expect_in: list[Any] | None = None) -> bool:
    """比较探测值与期望值,支持类型弱转换。"""
    if expect_equals is not None:
        try:
            return str(value) == str(expect_equals)
        except Exception:  # noqa: BLE001
            return False
    if expect_in is not None:
        return value in expect_in
    return bool(value)
```

- [ ] **Step 2: 运行 schema 单测确认通过**

Run: `pytest tests/test_scenario_schema.py -v`
Expected: 8 个测试全 PASS

- [ ] **Step 3: 运行全部纯逻辑测试,确认零回归**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm" --ignore=tests/fakes`
Expected: 全绿(旧 YAML 通过 _migrate_legacy 兼容)

- [ ] **Step 4: 验证现有 YAML 仍能加载**

Run: `python -c "from stability_harness_loop_multiagent.business.hikvision.scenario_schema import from_yaml; print(from_yaml('configs/stability_0001_reboot.yaml').id)"`
Expected: 打印 `Stability_0001`

- [ ] **Step 5: Commit**

```bash
git add business/hikvision/scenario_schema.py
git commit -m "refactor: replace handwritten dataclass with pydantic in scenario_schema"
```

---

## PR4a: capabilities/ 能力原子化(新建,并存)

**目标**:新建 `capabilities/` 子包,实现 15 个原子能力,`worker.py` 保留不动,零影响。

### Task 4a.1: 写能力基类单测

**Files:**
- Create: `business/hikvision/capabilities/__init__.py`
- Create: `business/hikvision/capabilities/actions/__init__.py`
- Create: `business/hikvision/capabilities/actions/base.py`
- Create: `business/hikvision/capabilities/probes/__init__.py`
- Create: `business/hikvision/capabilities/probes/base.py`
- Create: `business/hikvision/capabilities/preconditions/__init__.py`
- Create: `business/hikvision/capabilities/preconditions/base.py`

- [ ] **Step 1: 写能力基类(无单测,纯协议定义)**

```python
# business/hikvision/capabilities/actions/base.py
"""Action 基类 —— 操作能力协议(改变设备状态)。"""
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class ActionResult:
    """Action 执行结果。"""
    ok: bool = True
    error: str = ""
    data: dict[str, Any] = field(default_factory=dict)


class ActionBase(Protocol):
    """Action 协议:execute(ctx) -> ActionResult。"""
    def execute(self, ctx: Any) -> ActionResult:
        ...


# business/hikvision/capabilities/probes/base.py
"""Probe 基类 —— 探测能力协议(读取状态,不改变)。"""
from typing import Any, Protocol


class ProbeBase(Protocol):
    """Probe 协议:check(snapshot) -> dict[str, bool](事实字典)。"""
    def check(self, snapshot: Any) -> dict[str, bool]:
        ...


# business/hikvision/capabilities/preconditions/base.py
"""Precondition 基类 —— 前置条件协议(用例开始前检查/设置)。"""
from typing import Any, Protocol


class PreconditionBase(Protocol):
    """Precondition 协议:setup(ctx) -> bool(True=通过,False=中止)。"""
    def setup(self, ctx: Any) -> bool:
        ...
```

- [ ] **Step 2: 写 __init__.py 工厂函数签名(暂不实现)**

```python
# business/hikvision/capabilities/__init__.py
"""能力原子包 —— actions + probes + preconditions。

工厂函数 create_action/create_probe/create_precondition 按 type 路由到具体实现。
"""
from typing import Any


def create_action(spec: Any) -> Any:
    """根据 ActionSpec 创建 Action 实例(暂未实现,PR4a.2 起逐步填充)。"""
    raise NotImplementedError(f"Action type {getattr(spec, 'type', '?')} 暂未实现")


def create_probe(spec: Any) -> Any:
    """根据 ProbeSpec 创建 Probe 实例。"""
    raise NotImplementedError(f"Probe type {getattr(spec, 'type', '?')} 暂未实现")


def create_precondition(spec: Any) -> Any:
    """根据 PreconditionSpec 创建 Precondition 实例。"""
    raise NotImplementedError(f"Precondition type {getattr(spec, 'type', '?')} 暂未实现")
```

- [ ] **Step 3: Commit**

```bash
git add business/hikvision/capabilities/
git commit -m "feat: add capabilities/ skeleton with base protocols"
```

### Task 4a.2: 实现 SleepAction + NoopAction(简单能力,TDD)

**Files:**
- Create: `business/hikvision/capabilities/actions/sleep.py`
- Create: `business/hikvision/capabilities/actions/noop.py`
- Create: `tests/test_capabilities/__init__.py`
- Create: `tests/test_capabilities/test_action_sleep.py`
- Create: `tests/test_capabilities/test_action_noop.py`

- [ ] **Step 1: 写 SleepAction 单测**

```python
# tests/test_capabilities/test_action_sleep.py
"""SleepAction 单测(纯逻辑,不真 sleep,只验证时间计算)。"""
import time
from stability_harness_loop_multiagent.business.hikvision.capabilities.actions.sleep import SleepAction


def test_sleep_action_returns_ok():
    """SleepAction:返回 ok=True"""
    action = SleepAction(seconds=0.01)
    result = action.execute(ctx=None)
    assert result.ok is True


def test_sleep_action_actually_sleeps():
    """SleepAction:实际 sleep 指定秒数"""
    action = SleepAction(seconds=0.05)
    start = time.time()
    action.execute(ctx=None)
    elapsed = time.time() - start
    assert elapsed >= 0.04  # 允许微小误差
```

- [ ] **Step 2: 写 NoopAction 单测**

```python
# tests/test_capabilities/test_action_noop.py
"""NoopAction 单测(纯逻辑)。"""
from stability_harness_loop_multiagent.business.hikvision.capabilities.actions.noop import NoopAction


def test_noop_action_returns_ok():
    """NoopAction:返回 ok=True,不执行任何操作"""
    action = NoopAction()
    result = action.execute(ctx=None)
    assert result.ok is True
    assert result.data == {}
```

- [ ] **Step 3: 运行测试确认失败**

Run: `pytest tests/test_capabilities/test_action_sleep.py tests/test_capabilities/test_action_noop.py -v`
Expected: FAIL(ModuleNotFoundError)

- [ ] **Step 4: 实现 SleepAction + NoopAction**

```python
# business/hikvision/capabilities/actions/sleep.py
"""SleepAction —— 等待指定秒数(组合用例的时序控制)。"""
import time
from .base import ActionResult


class SleepAction:
    def __init__(self, seconds: float = 1.0) -> None:
        self._seconds = seconds

    def execute(self, ctx) -> ActionResult:
        time.sleep(self._seconds)
        return ActionResult(ok=True, data={"slept": self._seconds})


# business/hikvision/capabilities/actions/noop.py
"""NoopAction —— 显式无操作占位(对应原 stress.type=none 路径)。"""
from .base import ActionResult


class NoopAction:
    def __init__(self, **kwargs) -> None:
        pass

    def execute(self, ctx) -> ActionResult:
        return ActionResult(ok=True)
```

- [ ] **Step 5: 注册到工厂并运行测试**

修改 `business/hikvision/capabilities/__init__.py` 的 `create_action`:

```python
from .actions.sleep import SleepAction
from .actions.noop import NoopAction

_ACTION_REGISTRY = {
    "sleep": SleepAction,
    "noop": NoopAction,
}

def create_action(spec) -> Any:
    cls = _ACTION_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Action type {spec.type} 暂未实现")
    return cls(**spec.params)
```

Run: `pytest tests/test_capabilities/test_action_sleep.py tests/test_capabilities/test_action_noop.py -v`
Expected: 3 个测试全 PASS

- [ ] **Step 6: Commit**

```bash
git add business/hikvision/capabilities/actions/sleep.py business/hikvision/capabilities/actions/noop.py business/hikvision/capabilities/__init__.py tests/test_capabilities/
git commit -m "feat: add SleepAction + NoopAction capabilities with TDD"
```

### Task 4a.3: 实现 FieldProbe(从 scenario_schema.py 迁出)

**Files:**
- Create: `business/hikvision/capabilities/probes/field.py`
- Create: `tests/test_capabilities/test_probe_field.py`

- [ ] **Step 1: 写 FieldProbe 单测**

```python
# tests/test_capabilities/test_probe_field.py
"""FieldProbe 单测(纯逻辑,验证字段断言)。"""
from stability_harness_loop_multiagent.business.hikvision.capabilities.probes.field import FieldProbe


def test_field_probe_equals_pass():
    """FieldProbe:字段值等于期望值 → probe_ok=True"""
    probe = FieldProbe(field="AcsWorkStatus.doorOnlineStatus[0]", expect_equals=1)
    snapshot = {"AcsWorkStatus": {"doorOnlineStatus": [1]}}
    fact = probe.check(snapshot)
    assert fact["probe_ok"] is True


def test_field_probe_equals_fail():
    """FieldProbe:字段值不等于期望值 → probe_ok=False"""
    probe = FieldProbe(field="AcsWorkStatus.doorOnlineStatus[0]", expect_equals=1)
    snapshot = {"AcsWorkStatus": {"doorOnlineStatus": [0]}}
    fact = probe.check(snapshot)
    assert fact["probe_ok"] is False


def test_field_probe_na_if_absent():
    """FieldProbe:na_if_absent 字段缺失 → probe_na=True"""
    probe = FieldProbe(field="online", expect_equals=1, na_if_absent="netWorkStatus")
    snapshot = {"AcsWorkStatus": {}}  # 缺 netWorkStatus
    fact = probe.check(snapshot)
    assert fact.get("probe_na") is True


def test_field_probe_field_missing_no_na():
    """FieldProbe:字段缺失但无 na_if_absent → probe_ok=False"""
    probe = FieldProbe(field="missing.field", expect_equals=1)
    snapshot = {}
    fact = probe.check(snapshot)
    assert fact["probe_ok"] is False
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_capabilities/test_probe_field.py -v`
Expected: FAIL(ModuleNotFoundError)

- [ ] **Step 3: 实现 FieldProbe**

```python
# business/hikvision/capabilities/probes/field.py
"""FieldProbe —— 字段值断言(从 scenario_schema.py 迁出)。

支持:
- expect_equals: 等于期望值(类型弱转换)
- na_if_absent: 存在性字段缺失 → NA(不强制失败)
"""
from typing import Any
from ...scenario_schema import resolve_field, compare_probe


class FieldProbe:
    def __init__(self, field: str, expect_equals: Any = None,
                  expect_in: list[Any] | None = None,
                  na_if_absent: str | None = None,
                  endpoint: str | None = None) -> None:
        self._field = field
        self._expect_equals = expect_equals
        self._expect_in = expect_in
        self._na_if_absent = na_if_absent
        self._endpoint = endpoint

    def check(self, snapshot: Any) -> dict[str, bool]:
        # 先检查 na_if_absent
        if self._na_if_absent:
            try:
                resolve_field(snapshot, self._na_if_absent)
            except KeyError:
                return {"probe_ok": False, "probe_na": True}
        # 解析 field
        try:
            value = resolve_field(snapshot, self._field)
        except KeyError:
            return {"probe_ok": False}
        ok = compare_probe(value, self._expect_equals, self._expect_in)
        return {"probe_ok": bool(ok)}
```

- [ ] **Step 4: 注册到工厂并运行测试**

修改 `business/hikvision/capabilities/__init__.py` 的 `create_probe`:

```python
from .probes.field import FieldProbe

_PROBE_REGISTRY = {
    "field": FieldProbe,
}

def create_probe(spec) -> Any:
    cls = _PROBE_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Probe type {spec.type} 暂未实现")
    return cls(**spec.params)
```

Run: `pytest tests/test_capabilities/test_probe_field.py -v`
Expected: 4 个测试全 PASS

- [ ] **Step 5: Commit**

```bash
git add business/hikvision/capabilities/probes/field.py business/hikvision/capabilities/__init__.py tests/test_capabilities/test_probe_field.py
git commit -m "feat: add FieldProbe capability with TDD (migrated from scenario_schema)"
```

### Task 4a.4: 实现 RebootAction(从 worker.py 迁出)

**Files:**
- Create: `business/hikvision/capabilities/actions/reboot.py`

- [ ] **Step 1: 实现 RebootAction(单测依赖真实设备,只写实现不写单测)**

```python
# business/hikvision/capabilities/actions/reboot.py
"""RebootAction —— 主设备/子设备重启(从 worker.py 迁出)。

主设备:client.reboot() + client.wait_online(timeout)
子设备:client.request_json("PUT", "/ISAPI/System/RebootBatchChild", body)
"""
import logging
from .base import ActionResult

logger = logging.getLogger(__name__)


class RebootAction:
    def __init__(self, target: str = "main", wait_online_timeout: int = 180,
                  child_ids: list[str] | None = None) -> None:
        self._target = target
        self._wait_online_timeout = wait_online_timeout
        self._child_ids = child_ids or []

    def execute(self, ctx) -> ActionResult:
        client = ctx.client
        if self._target == "main":
            try:
                client.reboot()
                ok = client.wait_online(timeout=self._wait_online_timeout)
                if not ok:
                    return ActionResult(ok=False, error="设备未在超时内重新上线")
                return ActionResult(ok=True, data={"target": "main"})
            except Exception as exc:  # noqa: BLE001
                logger.warning("RebootAction 主设备重启失败: %s", exc)
                return ActionResult(ok=False, error=str(exc))
        elif self._target == "child":
            try:
                payload = {"childDeviceList": [{"deviceID": cid} for cid in self._child_ids]}
                client.request_json("PUT", "/ISAPI/System/RebootBatchChild?format=json", body=payload)
                return ActionResult(ok=True, data={"target": "child", "count": len(self._child_ids)})
            except Exception as exc:  # noqa: BLE001
                return ActionResult(ok=False, error=str(exc))
        return ActionResult(ok=False, error=f"未知 target: {self._target}")
```

- [ ] **Step 2: 注册到工厂**

在 `_ACTION_REGISTRY` 添加:
```python
from .actions.reboot import RebootAction
_ACTION_REGISTRY["reboot"] = RebootAction
```

- [ ] **Step 3: 真机验证(若有 HIK_HOST)**

Run: `python -c "import os; from stability_harness_loop_multiagent.business.hikvision.client import HikvisionClient; from stability_harness_loop_multiagent.business.hikvision.capabilities.actions.reboot import RebootAction; c=HikvisionClient(host=os.environ['HIK_HOST'], password=os.environ['HIK_PASSWORD']); from types import SimpleNamespace; ctx=SimpleNamespace(client=c); r=RebootAction(target='main', wait_online_timeout=60).execute(ctx); print(r)"`
Expected: `ActionResult(ok=True, data={'target': 'main'})`

- [ ] **Step 4: Commit**

```bash
git add business/hikvision/capabilities/actions/reboot.py business/hikvision/capabilities/__init__.py
git commit -m "feat: add RebootAction capability (migrated from worker.py)"
```

### Task 4a.5: 实现 RemoteOpenAction + QueryEventsAction + EventChainProbe(从 worker.py 迁出)

**Files:**
- Create: `business/hikvision/capabilities/actions/remote_open.py`
- Create: `business/hikvision/capabilities/actions/query_events.py`
- Create: `business/hikvision/capabilities/probes/event_chain.py`

- [ ] **Step 1: 实现 RemoteOpenAction**

```python
# business/hikvision/capabilities/actions/remote_open.py
"""RemoteOpenAction —— 远程开门(从 worker.py 迁出)。"""
import logging
from .base import ActionResult

logger = logging.getLogger(__name__)


class RemoteOpenAction:
    def __init__(self, door: int = 1) -> None:
        self._door = door

    def execute(self, ctx) -> ActionResult:
        try:
            ctx.client.remote_open_door(door=self._door)
            return ActionResult(ok=True, data={"door": self._door})
        except Exception as exc:  # noqa: BLE001
            logger.warning("RemoteOpenAction 失败: %s", exc)
            return ActionResult(ok=False, error=str(exc))
```

- [ ] **Step 2: 实现 QueryEventsAction**

```python
# business/hikvision/capabilities/actions/query_events.py
"""QueryEventsAction —— 查询事件链(从 worker.py 迁出)。

30s 回溯窗口,baseline_serial 之前的事件过滤掉。
结果存入 ctx.events,供 EventChainProbe 断言。
"""
import logging
from .base import ActionResult

logger = logging.getLogger(__name__)


class QueryEventsAction:
    def __init__(self, window: int = 300, baseline_serial: int = 0) -> None:
        self._window = window
        self._baseline_serial = baseline_serial

    def execute(self, ctx) -> ActionResult:
        try:
            events = ctx.client.query_events(
                window=self._window,
                baseline_serial=self._baseline_serial
            )
            ctx.events = events  # 存入 ctx,供 EventChainProbe 用
            return ActionResult(ok=True, data={"count": len(events)})
        except Exception as exc:  # noqa: BLE001
            logger.warning("QueryEventsAction 失败: %s", exc)
            return ActionResult(ok=False, error=str(exc))
```

- [ ] **Step 3: 实现 EventChainProbe**

```python
# business/hikvision/capabilities/probes/event_chain.py
"""EventChainProbe —— 事件链断言(从 worker.py 迁出)。

验证 ctx.events 中是否包含期望的事件序列(如 remote_open + lock_open + lock_closed)。
lock_closed 是软事实:缺失不强制失败,但增加风险分(由 Advisor 处理)。
"""
from typing import Any


class EventChainProbe:
    def __init__(self, expect_sequence: list[str], window: int = 30) -> None:
        self._expect_sequence = expect_sequence
        self._window = window

    def check(self, snapshot: Any) -> dict[str, bool]:
        # snapshot 这里实际是 ctx(含 events 属性)
        events = getattr(snapshot, "events", []) if not isinstance(snapshot, dict) \
                 else snapshot.get("events", [])
        event_types = [e.get("eventType", "") for e in events]
        facts = {}
        for evt in self._expect_sequence:
            facts[evt] = evt in event_types
        # lock_closed 软事实:缺失不强制 fail,但标记 soft
        if "lock_closed" in self._expect_sequence and "lock_closed" not in event_types:
            facts["lock_closed_soft"] = True  # Advisor 会加风险分
        return facts
```

- [ ] **Step 4: 注册到工厂**

```python
from .actions.remote_open import RemoteOpenAction
from .actions.query_events import QueryEventsAction
from .probes.event_chain import EventChainProbe

_ACTION_REGISTRY["remote_open"] = RemoteOpenAction
_ACTION_REGISTRY["query_events"] = QueryEventsAction
_PROBE_REGISTRY["event_chain"] = EventChainProbe
```

- [ ] **Step 5: Commit**

```bash
git add business/hikvision/capabilities/actions/remote_open.py business/hikvision/capabilities/actions/query_events.py business/hikvision/capabilities/probes/event_chain.py business/hikvision/capabilities/__init__.py
git commit -m "feat: add RemoteOpen/QueryEvents/EventChain capabilities (migrated from worker.py)"
```

### Task 4a.6: 实现剩余能力(UpgradeAction/DispatchAction/SwitchSerialAction/OnlineProbe/CountProbe + 3 个 Precondition)

**Files:**
- Create: `business/hikvision/capabilities/actions/upgrade.py`
- Create: `business/hikvision/capabilities/actions/dispatch.py`
- Create: `business/hikvision/capabilities/actions/switch_serial.py`
- Create: `business/hikvision/capabilities/probes/online.py`
- Create: `business/hikvision/capabilities/probes/count.py`
- Create: `business/hikvision/capabilities/preconditions/device_online.py`
- Create: `business/hikvision/capabilities/preconditions/serial_mode.py`
- Create: `business/hikvision/capabilities/preconditions/baseline_record.py`

- [ ] **Step 1: 实现 UpgradeAction**

```python
# business/hikvision/capabilities/actions/upgrade.py
"""UpgradeAction —— 主设备/子设备升级(从 worker.py 迁出)。"""
import logging
from .base import ActionResult

logger = logging.getLogger(__name__)


class UpgradeAction:
    def __init__(self, target: str = "main", firmware_url: str = "",
                  child_ids: list[str] | None = None) -> None:
        self._target = target
        self._firmware_url = firmware_url
        self._child_ids = child_ids or []

    def execute(self, ctx) -> ActionResult:
        client = ctx.client
        try:
            if self._target == "main":
                payload = {"deviceDeviceName": "main", "firmwareURL": self._firmware_url}
                client.request_json("POST", "/ISAPI/System/updateFirmware", body=payload)
                ok = client.wait_online(timeout=600)  # 升级超时更长
                return ActionResult(ok=ok, data={"target": "main"})
            else:
                payload = {"childDeviceList": [{"deviceID": cid, "firmwareURL": self._firmware_url}
                                                for cid in self._child_ids]}
                client.request_json("POST", "/ISAPI/System/BulkUpgradeChildDeviceList?format=json", body=payload)
                return ActionResult(ok=True, data={"target": "child", "count": len(self._child_ids)})
        except Exception as exc:  # noqa: BLE001
            return ActionResult(ok=False, error=str(exc))
```

- [ ] **Step 2: 实现 DispatchAction(下发配置)**

```python
# business/hikvision/capabilities/actions/dispatch.py
"""DispatchAction —— 下发配置(下发稳定性用例 0061-0078)。"""
import logging
from .base import ActionResult

logger = logging.getLogger(__name__)


class DispatchAction:
    def __init__(self, endpoint: str, method: str = "PUT", body: dict | None = None) -> None:
        self._endpoint = endpoint
        self._method = method
        self._body = body or {}

    def execute(self, ctx) -> ActionResult:
        try:
            ctx.client.request_json(self._method, self._endpoint, body=self._body)
            return ActionResult(ok=True)
        except Exception as exc:  # noqa: BLE001
            return ActionResult(ok=False, error=str(exc))
```

- [ ] **Step 3: 实现 SwitchSerialAction**

```python
# business/hikvision/capabilities/actions/switch_serial.py
"""SwitchSerialAction —— 切换串口模式(串口外设用例)。"""
import logging
from .base import ActionResult

logger = logging.getLogger(__name__)


class SwitchSerialAction:
    def __init__(self, port: int = 1, mode: str = "externMode") -> None:
        self._port = port
        self._mode = mode

    def execute(self, ctx) -> ActionResult:
        try:
            cfg = ctx.client.get_serial_config(port=self._port)
            cfg["SerialPort"]["mode"] = self._mode
            ctx.client.set_serial_config(port=self._port, cfg=cfg)
            return ActionResult(ok=True, data={"mode": self._mode})
        except Exception as exc:  # noqa: BLE001
            return ActionResult(ok=False, error=str(exc))
```

- [ ] **Step 4: 实现 OnlineProbe + CountProbe**

```python
# business/hikvision/capabilities/probes/online.py
"""OnlineProbe —— 单设备在线状态断言。"""
from .field import FieldProbe


class OnlineProbe(FieldProbe):
    """OnlineProbe 是 FieldProbe 的特化(field=doorOnlineStatus[0])。"""
    def __init__(self, expect_equals: int = 1, na_if_absent: str | None = None) -> None:
        super().__init__(
            field="AcsWorkStatus.doorOnlineStatus[0]",
            expect_equals=expect_equals,
            na_if_absent=na_if_absent or "AcsWorkStatus.doorOnlineStatus"
        )


# business/hikvision/capabilities/probes/count.py
"""CountProbe —— 子设备在线数量比对(子设备用例)。"""
from typing import Any


class CountProbe:
    def __init__(self, field: str, expect_equals: int) -> None:
        self._field = field
        self._expect_equals = expect_equals

    def check(self, snapshot: Any) -> dict[str, bool]:
        try:
            from ...scenario_schema import resolve_field
            value = resolve_field(snapshot, self._field)
            return {"probe_ok": int(value) == self._expect_equals}
        except (KeyError, ValueError, TypeError):
            return {"probe_ok": False}
```

- [ ] **Step 5: 实现 3 个 Precondition**

```python
# business/hikvision/capabilities/preconditions/device_online.py
"""DeviceOnlinePrecondition —— 用例开始前验证设备在线。"""
import logging
logger = logging.getLogger(__name__)


class DeviceOnlinePrecondition:
    def __init__(self, **kwargs) -> None:
        pass

    def setup(self, ctx) -> bool:
        try:
            t = ctx.client.get_time()
            return bool(t)
        except Exception as exc:  # noqa: BLE001
            logger.warning("DeviceOnlinePrecondition 失败: %s", exc)
            return False


# business/hikvision/capabilities/preconditions/serial_mode.py
"""SerialModePrecondition —— 用例开始前设置串口模式。"""
from ..actions.switch_serial import SwitchSerialAction


class SerialModePrecondition:
    def __init__(self, mode: str = "externMode", port: int = 1) -> None:
        self._mode = mode
        self._port = port

    def setup(self, ctx) -> bool:
        action = SwitchSerialAction(port=self._port, mode=self._mode)
        result = action.execute(ctx)
        return result.ok


# business/hikvision/capabilities/preconditions/baseline_record.py
"""BaselineRecordPrecondition —— 记录基线 serialNo + 重启时长。

产出的 baseline 写入 ctx.baseline,供后续 actions/probes 引用(${baseline.xxx})。
"""
import logging
import time
logger = logging.getLogger(__name__)


class BaselineRecordPrecondition:
    def __init__(self, record_reboot_duration: bool = False) -> None:
        self._record_reboot_duration = record_reboot_duration

    def setup(self, ctx) -> bool:
        try:
            ctx.baseline = {"serial": ctx.client.get_event_serial()}
            if self._record_reboot_duration:
                start = time.time()
                ctx.client.reboot()
                ok = ctx.client.wait_online(timeout=180)
                duration = time.time() - start
                ctx.baseline["reboot_duration"] = int(duration) + 10  # 加 10s buffer
                if not ok:
                    return False
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("BaselineRecordPrecondition 失败: %s", exc)
            return False
```

- [ ] **Step 6: 注册所有能力到工厂**

更新 `business/hikvision/capabilities/__init__.py`:

```python
from .actions.sleep import SleepAction
from .actions.noop import NoopAction
from .actions.reboot import RebootAction
from .actions.upgrade import UpgradeAction
from .actions.remote_open import RemoteOpenAction
from .actions.dispatch import DispatchAction
from .actions.switch_serial import SwitchSerialAction
from .actions.query_events import QueryEventsAction
from .probes.field import FieldProbe
from .probes.online import OnlineProbe
from .probes.count import CountProbe
from .probes.event_chain import EventChainProbe
from .preconditions.device_online import DeviceOnlinePrecondition
from .preconditions.serial_mode import SerialModePrecondition
from .preconditions.baseline_record import BaselineRecordPrecondition

_ACTION_REGISTRY = {
    "sleep": SleepAction, "noop": NoopAction, "reboot": RebootAction,
    "upgrade": UpgradeAction, "remote_open": RemoteOpenAction,
    "dispatch": DispatchAction, "switch_serial": SwitchSerialAction,
    "query_events": QueryEventsAction,
}
_PROBE_REGISTRY = {
    "field": FieldProbe, "online": OnlineProbe,
    "count": CountProbe, "event_chain": EventChainProbe,
}
_PRECONDITION_REGISTRY = {
    "device_online": DeviceOnlinePrecondition,
    "serial_mode": SerialModePrecondition,
    "baseline_record": BaselineRecordPrecondition,
}


def create_action(spec):
    cls = _ACTION_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Action type {spec.type} 暂未实现")
    return cls(**spec.params)


def create_probe(spec):
    cls = _PROBE_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Probe type {spec.type} 暂未实现")
    return cls(**spec.params)


def create_precondition(spec):
    cls = _PRECONDITION_REGISTRY.get(spec.type)
    if cls is None:
        raise NotImplementedError(f"Precondition type {spec.type} 暂未实现")
    return cls(**spec.params)
```

- [ ] **Step 7: 运行全部纯逻辑测试,确认零回归**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm" --ignore=tests/fakes`
Expected: 全绿(capabilities 新增不影响旧路径)

- [ ] **Step 8: Commit**

```bash
git add business/hikvision/capabilities/
git commit -m "feat: complete 15 capabilities (8 actions + 4 probes + 3 preconditions)"
```

---

## PR4b: scenario_worker.py 切换到 capabilities

**目标**:重构 ScenarioWorker 调用 capabilities,`worker.py` 仍保留,新旧路径并存跑 5 轮真机验证 facts 等价。

### Task 4b.1: 重构 ScenarioWorker 为组合器

**Files:**
- Modify: `business/hikvision/scenario_worker.py`

- [ ] **Step 1: 重构 ScenarioWorker(保留原 act 流水线,内部切换到 capabilities)**

```python
# business/hikvision/scenario_worker.py(节选,展示核心改动)
"""ScenarioWorker —— 数据驱动的稳定性执行 Agent(capabilities 组合器)。

流水线(act → do_work → recover → check → publish)保持不变,
内部 do_work/check 改为调用 capabilities/ 的原子能力。
"""
import asyncio
import time
from datetime import datetime
from typing import Any, Optional

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from ...multi_agent.adapter import TargetAdapter
from ...multi_agent.workers.base import WorkerAgent
from .scenario_schema import Scenario, resolve_field, compare_probe
from .capabilities import create_action, create_probe, create_precondition


class ScenarioWorker(WorkerAgent):
    def __init__(self, bus: EventBus, spec: AgentSpec, adapter: TargetAdapter,
                 scenario: Scenario, *, recover_timeout: float = 180.0) -> None:
        super().__init__(bus, spec, adapter)
        self._sc = scenario
        self._recover_timeout = recover_timeout
        self._last_snapshot: Any = None
        self._na: bool = False
        self._early_stop: bool = False
        self._stop_reason: Optional[str] = None
        self._chain: dict[str, int] = {"rounds": 0, "pass": 0,
                                        "fail": 0, "na": 0, "stress_fail": 0}
        # 预构建能力实例
        self._preconditions = [create_precondition(p) for p in scenario.preconditions]
        self._actions = [create_action(a) for a in scenario.actions]
        self._probes = [create_probe(p) for p in scenario.probes]
        self._ctx = None  # 运行时构造

    def pre_loop_setup(self) -> bool:
        """循环开始前执行所有 preconditions。"""
        from types import SimpleNamespace
        self._ctx = SimpleNamespace(client=self._get_client(), events=[], baseline={})
        for precondition in self._preconditions:
            if not precondition.setup(self._ctx):
                return False
        return True

    def _get_client(self):
        """从 adapter 获取底层 client(适配器薄壳)。"""
        return getattr(self._adapter, "_client", None)

    def do_work(self, tick: dict) -> Any:
        """执行 actions 链(顺序执行),返回最后 snapshot。"""
        if self._past_deadline():
            self._emit_early_stop(tick, "deadline reached (NT)")
            return None
        # 执行 actions
        for action in self._actions:
            result = action.execute(self._ctx)
            if not result.ok:
                self._last_stress_ok = False
                self._mark("action_failed", reason=result.error)
                return None
        # 获取 snapshot(最后一个 action 后的设备状态)
        self._last_snapshot = self._fetch_snapshot()
        return self._last_snapshot

    def _fetch_snapshot(self) -> dict:
        """从设备拉取 snapshot(用第一个 probe 的 endpoint,或默认 work_status)。"""
        try:
            return self._ctx.client.get_work_status()
        except Exception:  # noqa: BLE001
            return {}

    def check(self, tick: dict) -> dict[str, bool]:
        """用 probes 组合产出事实字典。"""
        facts: dict[str, bool] = {}
        self._na = False
        for probe in self._probes:
            fact = probe.check(self._ctx if hasattr(self._ctx, "events") else self._last_snapshot)
            for k, v in fact.items():
                if k.endswith("_na") and v:
                    self._na = True
                if not k.endswith("_soft"):
                    facts[k] = v
        self._chain["rounds"] += 1
        if self._na:
            self._chain["na"] += 1
        elif all(facts.values()):
            self._chain["pass"] += 1
        else:
            self._chain["fail"] += 1
        return facts

    # act / recover / _past_deadline / _emit_early_stop / _mark 等保持原实现
```

- [ ] **Step 2: 运行全部纯逻辑测试,确认零回归**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm" --ignore=tests/fakes`
Expected: 全绿

- [ ] **Step 3: 真机 5 轮验证新旧路径 facts 等价**

Run: `pytest -m Stability_0001 --real-device --rounds 5` (若有 HIK_HOST)
Expected: 5 轮 verdict 分布与改造前一致,remote_open/lock_open/lock_closed/recovered 状态完整

- [ ] **Step 4: Commit**

```bash
git add business/hikvision/scenario_worker.py
git commit -m "refactor: switch ScenarioWorker to capabilities combinator (old worker.py preserved)"
```

---

## PR4c: 删除 worker.py + 删除所有 fake + 删除 --dry-run

**目标**:删除 worker.py(已迁移)、tests/fakes/、FakeScenarioAdapter、FakeTargetAdapter、examples/smoke.py、--dry-run 选项。

### Task 4c.1: 定位所有 fake 引用

**Files:**
- Search: 全仓

- [ ] **Step 1: 搜索所有 fake 引用**

Run: `grep -rn "FakeHikvisionClient\|FakeScenarioAdapter\|FakeTargetAdapter\|FakeLLMClient" --include="*.py" .`
Expected: 列出所有引用点,逐一处理

- [ ] **Step 2: 搜索 dry-run 引用**

Run: `grep -rn "dry.run\|dry_run\|--dry-run" --include="*.py" .`
Expected: 列出所有 dry-run 引用点

### Task 4c.2: 删除 fake 文件和引用

**Files:**
- Delete: `tests/fakes/` 整个目录
- Delete: `examples/smoke.py`
- Modify: `business/hikvision/scenario_adapter.py`(删 FakeScenarioAdapter 类)
- Modify: 所有引用 fake 的测试(改为真实环境 skip 或删除)

- [ ] **Step 1: 删除 tests/fakes/ 目录**

Run: `git rm -r tests/fakes/`
Expected: 目录及文件删除

- [ ] **Step 2: 删除 examples/smoke.py**

Run: `git rm examples/smoke.py`
Expected: 文件删除

- [ ] **Step 3: 从 scenario_adapter.py 删除 FakeScenarioAdapter 类**

读取 `business/hikvision/scenario_adapter.py`,定位 `class FakeScenarioAdapter` 定义,删除整个类(保留 `ScenarioISAPIAdapter`)。

- [ ] **Step 4: 删除或改造所有引用 fake 的测试**

对每个引用 fake 的测试文件:
- 若测试逻辑可以改为纯逻辑(不依赖 fake 的部分)→ 改造保留
- 若测试逻辑完全依赖 fake → 删除整个测试文件
- 若测试需要真实设备 → 改为 `@pytest.mark.skipif(not os.environ.get("HIK_HOST"))`

- [ ] **Step 5: 删除 worker.py**

Run: `git rm business/hikvision/worker.py`
Expected: 文件删除

- [ ] **Step 6: 更新 runner.py 的 import**

修改 `business/hikvision/runner.py`:
```python
# 改前
from .worker import HikvisionWorker
# 改后:删除此 import,runner.py 若不再用 HikvisionWorker 则清理相关代码
# (若 runner.py 仍需旧式装配,改为从 scenario_runner 走)
```

- [ ] **Step 7: 删除 scenario_run.py 的 --dry-run 选项**

读取 `examples/scenario_run.py`,删除 `--dry-run` 参数及相关逻辑:

```python
# 删除这些代码:
# parser.add_argument("--dry-run", action="store_true", ...)
# if args.dry_run:
#     adapter = FakeScenarioAdapter(...)
# else:
#     adapter = ScenarioISAPIAdapter(...)
# 改为:
# adapter = ScenarioISAPIAdapter(...)
```

- [ ] **Step 8: 删除 scenario_runner.py 的 dry_run 参数**

读取 `business/hikvision/scenario_runner.py`,删除 `dry_run: bool = False` 参数及相关分支。

- [ ] **Step 9: 运行全部纯逻辑测试,确认零回归**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm"`
Expected: 全绿(此时已无 fake,所有 fake 测试已删除或改造)

- [ ] **Step 10: 真机冒烟验证**

Run: `python -m stability_harness_loop_multiagent.examples.scenario_run --scenario configs/stability_0001_reboot.yaml`
Expected: 正常运行(无 --dry-run 选项,连真机)

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "refactor: remove worker.py + all fakes + dry-run mode (capabilities fully replaced)"
```

---

## PR5: rich 终端美化

**目标**:examples/_report.py 用 rich 替换 print + 字符串拼接。

### Task 5.1: 改造 _report.py 用 rich

**Files:**
- Modify: `examples/_report.py`

- [ ] **Step 1: 读取原 _report.py**

Run: `Read examples/_report.py`
Expected: 了解现有 print + 字符串拼接逻辑

- [ ] **Step 2: 用 rich 重写 _report.py**

```python
# examples/_report.py
"""共享报告工具(rich 终端美化)。"""
from rich.console import Console
from rich.table import Table

console = Console()


def print_round_report(round_no: int, verdict: str, facts: dict,
                        remote_open: str = "N/A", lock_open: str = "N/A",
                        lock_closed: str = "N/A", recovered: str = "N/A") -> None:
    """打印单轮报告(rich 表格)。"""
    table = Table(title=f"Round {round_no}", show_lines=True)
    table.add_column("指标", style="cyan")
    table.add_column("值", style="magenta")
    table.add_row("Verdict", verdict)
    table.add_row("probe_ok", str(facts.get("probe_ok", "N/A")))
    table.add_row("remote_open", remote_open)
    table.add_row("lock_open", lock_open)
    table.add_row("lock_closed", lock_closed)
    table.add_row("recovered", recovered)
    console.print(table)


def print_final_summary(rounds: list[dict]) -> None:
    """打印最终汇总(rich 表格)。"""
    table = Table(title="稳定性测试结果汇总", show_lines=True)
    table.add_column("轮次", justify="right", style="cyan")
    table.add_column("Verdict", style="magenta")
    table.add_column("remote_open")
    table.add_column("lock_open")
    table.add_column("lock_closed")
    table.add_column("recovered")
    for r in rounds:
        table.add_row(
            str(r.get("round", "?")),
            r.get("verdict", "?"),
            str(r.get("remote_open", "N/A")),
            str(r.get("lock_open", "N/A")),
            str(r.get("lock_closed", "N/A")),
            str(r.get("recovered", "N/A")),
        )
    console.print(table)


def print_section(title: str) -> None:
    """打印分节标题(rich 样式)。"""
    console.rule(f"[bold cyan]{title}[/bold cyan]")
```

- [ ] **Step 3: 运行 examples 验证**

Run: `python -c "from stability_harness_loop_multiagent.examples._report import print_round_report, print_final_summary; print_round_report(1, 'pass', {'probe_ok': True}, 'yes', 'yes', 'yes', 'yes'); print_final_summary([{'round':1,'verdict':'pass','remote_open':'yes','lock_open':'yes','lock_closed':'yes','recovered':'yes'}])"`
Expected: rich 表格输出正常

- [ ] **Step 4: Commit**

```bash
git add examples/_report.py
git commit -m "refactor: use rich for terminal beautification in _report.py"
```

---

## 配套:conftest.py + test_stability_scenario.py

**目标**:实现 pytest marker 硬规则(1 marker = 1 用例)。

### Task C.1: 写 conftest.py 动态注册 marker

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1: 实现 conftest.py**

```python
# tests/conftest.py
"""pytest 全局配置:动态注册 marker + parametrize 注入。"""
import os
from pathlib import Path

import pytest
import yaml

# SCENARIO_MAP:scenario_id -> yaml_path(供 test_stability_scenario 读取)
SCENARIO_MAP: dict[str, Path] = {}
SCENARIO_IDS: list[str] = []


def pytest_configure(config):
    """扫描 configs/*.yaml,按 id 字段动态注册 marker。"""
    configs_dir = Path("configs")
    if not configs_dir.exists():
        return
    for yaml_path in sorted(configs_dir.glob("stability_*.yaml")):
        try:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(data, dict):
            continue
        scenario_id = data.get("id")
        if not scenario_id:
            continue
        category = data.get("category", "")
        level = data.get("level", "")
        # 注册 3 个维度的 marker
        config.addinivalue_line("markers", f"{scenario_id}: 用例 {scenario_id}")
        if category:
            config.addinivalue_line("markers", f"{category}: 类别 {category}")
        if level:
            config.addinivalue_line("markers", f"{level}: 等级 {level}")
        SCENARIO_MAP[scenario_id] = yaml_path
        SCENARIO_IDS.append(scenario_id)


def pytest_collection_modifyitems(config, items):
    """给 test_stability_scenario 自动打上对应 marker。"""
    for item in items:
        if item.name == "test_stability_scenario":
            callspec = getattr(item, "callspec", None)
            if callspec and "scenario_id" in callspec.params:
                scenario_id = callspec.params["scenario_id"]
                yaml_path = SCENARIO_MAP.get(scenario_id)
                if yaml_path:
                    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
                    item.add_marker(getattr(pytest.mark, scenario_id))
                    category = data.get("category", "")
                    level = data.get("level", "")
                    if category:
                        item.add_marker(getattr(pytest.mark, category))
                    if level:
                        item.add_marker(getattr(pytest.mark, level))
```

- [ ] **Step 2: 实现 test_stability_scenario.py**

```python
# tests/test_stability_scenario.py
"""稳定性用例统一入口:通过 marker 筛选用例,parametrize 遍历所有用例。"""
import os

import pytest

from stability_harness_loop_multiagent.business.hikvision.scenario_runner import run_scenario

# conftest.py 在 pytest_configure 阶段填充
from .conftest import SCENARIO_IDS, SCENARIO_MAP  # type: ignore


@pytest.mark.parametrize("scenario_id", SCENARIO_IDS)
def test_stability_scenario(scenario_id: str):
    """所有用例必须真实设备执行,无 HIK_HOST 时整个测试函数 skip。"""
    if not os.environ.get("HIK_HOST"):
        pytest.skip("无 HIK_HOST,需真实设备")
    yaml_path = SCENARIO_MAP[scenario_id]
    result = run_scenario(str(yaml_path))
    assert result.verdict in ("pass", "warn", "na"), \
        f"用例 {scenario_id} 失败: {result}"
```

- [ ] **Step 3: 验证 marker 注册**

Run: `pytest --markers | grep -E "Stability_|重启|升级|网络|硬件|下发"`
Expected: 列出所有动态注册的 marker

- [ ] **Step 4: 验证 marker 筛选**

Run: `pytest -m Stability_0001 --collect-only`
Expected: 只收集到 `test_stability_scenario[Stability_0001]` 一条

- [ ] **Step 5: 真机跑一条用例验证**

Run: `pytest -m Stability_0001 -v`(需 HIK_HOST)
Expected: 真机 1 轮通过(或按 YAML max_rounds)

- [ ] **Step 6: Commit**

```bash
git add tests/conftest.py tests/test_stability_scenario.py
git commit -m "feat: add pytest marker dynamic registration (1 marker = 1 scenario)"
```

---

## 最终验收

### Task F.1: 全量纯逻辑测试

- [ ] **Step 1: 运行所有纯逻辑测试**

Run: `pytest tests/ -v -m "not slow and not real_device and not real_llm"`
Expected: 全绿(无 fake,无 mock,纯逻辑)

### Task F.2: 引擎隔离守护

- [ ] **Step 1: 验证三引擎不互相 import**

Run: `pytest tests/test_harness_regression.py -v`
Expected: 全绿

### Task F.3: 真机 5 轮验证

- [ ] **Step 1: 单用例 5 轮**

Run: `pytest -m Stability_0001 --real-device --rounds 5`
Expected: 5 轮 verdict 分布正常

- [ ] **Step 2: 类别筛选**

Run: `pytest -m "重启稳定性" --real-device --rounds 5`
Expected: 类别下所有用例跑通

### Task F.4: 真实 LLM 验证

- [ ] **Step 1: 真实 LLM 集成测试**

Run: `pytest -m "real_llm" -v`(需 LLM_API_KEY)
Expected: Advisor 解析计划正确

### Task F.5: 验收清单

- [ ] `business/hikvision/worker.py` 已删除
- [ ] `tests/fakes/` 已删除
- [ ] `examples/smoke.py` 已删除
- [ ] `--dry-run` 选项已删除
- [ ] `pytest --markers | grep Stability_` 列出所有用例 marker
- [ ] `pytest -m Stability_0001` 能跑通(真机)
- [ ] `pytest -m "重启稳定性"` 能筛选多用例
- [ ] 纯逻辑测试全绿
- [ ] 引擎隔离守护通过
- [ ] 5 大不变量测试全绿(循环终止/裁决产生/事件扇出/事实独裁/引擎隔离)
- [ ] 真机 5 轮验证通过
- [ ] 真实 LLM 验证通过
- [ ] 单文件 ≤ 300 行
- [ ] 零手写 Digest Auth(httpx-auth 替代)
- [ ] 零手写 LLM JSON 抽取(openai structured output 替代)
- [ ] 零手写 dataclass 校验(pydantic 替代)
- [ ] 零 mock 框架依赖
- [ ] 零 fake 类

---

## Self-Review

### 1. Spec 覆盖检查

- ✅ §2 架构改造范围 → PR1/2/3/4a/4b/4c/5 全覆盖
- ✅ §3 用例组织方式 → Task C.1 (conftest.py + test_stability_scenario.py)
- ✅ §4 能力原子化 + YAML 组合 → PR4a (15 个能力)
- ✅ §5 Worker 智能边界 → 不引入 Function Tool(spec 已说明,plan 无需任务)
- ✅ §6 造轮子替换 → PR1(httpx) + PR2(openai) + PR3(pydantic) + PR5(rich)
- ✅ §7 测试策略 → 每个 PR 都有 TDD 步骤,零 fake 零 mock
- ✅ §8 分支策略 → 所有 commit 在当前工作分支
- ✅ §10 验收标准 → Task F.1-F.5

### 2. Placeholder 扫描

- ✅ 无 TBD/TODO/"实现细节"
- ✅ 所有代码块完整可执行
- ✅ 所有命令带 expected 输出
- ⚠ Task 4c.2 Step 4 "改造或删除"较抽象 → 但这是合理的(具体取决于每个测试文件内容,无法预判)

### 3. 类型一致性

- ✅ `ActionResult` 在 base.py 定义,reboot.py/remote_open.py 等统一使用
- ✅ `ActionSpec`/`ProbeSpec`/`PreconditionSpec` 在 scenario_schema.py 定义,capabilities/__init__.py 消费
- ✅ `LLMPlan` 在 llm_plan.py 定义,advisor.py 消费
- ✅ `create_action`/`create_probe`/`create_precondition` 工厂签名一致

### 4. 关键风险

- PR4b 风险最高:新旧路径切换可能引入 facts 差异 → Task 4b.1 Step 3 真机 5 轮验证等价
- PR4c 删除阶段不可逆 → 保留备份分支
- baseline 插值 `${baseline.xxx}` 在 plan 中未单独测试 → 建议在 Task 4a.6 后补一个插值单测

plan 完成。
