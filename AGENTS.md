# AGENTS.md — AI Agent 协作指南

> 本文件面向所有参与本仓库开发的 AI 编码 Agent（Copilot / Cursor / Antigravity / Devin 等），
> 提供项目上下文、架构约束和编码规范，以便 Agent 能快速、正确地理解并修改代码。

---

## 项目概述

**stability_harness_loop_multiagent** 是一个**与领域无关**的可复用 Python 框架，
用「三引擎 + 契约内核」架构构建**自治循环系统**：

| 层 | 职责 | 包路径 |
|------|------|--------|
| **Core（契约内核）** | 跨引擎共享的最小契约：EventBus / Agent / combine_votes | `stability_harness_loop_multiagent/core/` |
| **Harness** | 运行时 / 治理 / 可观测 / 校验 / 看门狗 / 总线追踪 | `stability_harness_loop_multiagent/harness/` |
| **Loop** | 确定性控制循环 / 裁决 / 中止 / 调度 | `stability_harness_loop_multiagent/loop/` |
| **Multi-Agent (MAS)** | 领域执行 / 建议 / 观察（Worker / Advisor / Observer） | `stability_harness_loop_multiagent/multi_agent/` |
| **Business** | 领域实现（当前为海康门禁稳定性测试） | `stability_harness_loop_multiagent/business/hikvision/` |

设计文档见 `docs/plans/`（系统架构、演进路线、用例集、设计方案），文档站见
`mkdocs.yml`（Material 主题，部署至 GitHub Pages）。

---

## 核心架构约束（必读）

### 1. 三引擎互不 import（边界由 `core/` 强制）

```
core（契约内核：EventBus / Agent / combine_votes）
   ↑           ↑           ↑
harness  ←(EventBus)→  loop  ←(EventBus)→  multi_agent
```

- `core/` 是**零内部依赖**的契约包，三个引擎都可以 import 它。
- **禁止** `harness/` 的模块 import `loop/` 或 `multi_agent/`。
- **禁止** `loop/` 的模块 import `multi_agent/`（反之亦然）。
- 所有跨引擎通信**只走 EventBus**（pub/sub + request/reply + `#` 通配）。
- `business/` 是领域装配层，可以 import 任意引擎 + core，但不被任何引擎反向依赖。
- 在做任何修改前请验证此约束（可参考 `tests/test_harness_regression.py`）。

### 2. 事实独裁（安全底线）

`DecisionAuthority`（`loop/decision.py`）拥有唯一裁决权：

- 任何一个事实为 `False` → 本轮裁决为 `fail`，**不可**被风险分或投票翻转为 `pass`。
- 风险分数只能附加 `warn` / `recheck` 注解。
- `critical` 事件强制 `recheck`。
- 决策路径出错时，回退为保守的 `warn(60)`（`CONSERVATIVE_RISK`），**绝不**乐观 `pass`。

### 3. 不可变共享上下文

- `SharedContext`（`loop/context.py`）由 ControlLoop **独占写入**。
- Agent 通过 `ReadOnlyContext`（冻结快照）观察历史。
- Agent 各自维护私有 `state` 字典，**禁止**直接读写另一个 Agent 的状态。

### 4. 防死锁

- 每个跨引擎 `await` 都有超时 + 确定性兜底。
- 投票超时 → 中性风险 `NEUTRAL_RISK = 50`。
- 恢复超时 → 记为失败。
- `max_rounds` / `max_duration` 硬上限保证循环一定终止。
- `recheck` 次数上限为 1。
- ControlLoop `_run_round` / `_collect_votes` 均为**事件驱动**（收齐目标事件即返回 + 超时兜底），
  不再空等满 `vote_timeout` / `recover_timeout`。

---

## 目录结构

```
stability-check/
├── stability_harness_loop_multiagent/        # 主包
│   ├── __init__.py                           # 顶层 re-export 全部公共 API
│   ├── core/                                 # 契约内核（零内部依赖）
│   │   ├── bus.py                            # EventBus（唯一接缝）
│   │   ├── agent.py                          # Agent 基类 + AgentSpec
│   │   └── voting.py                         # combine_votes（投票合并规范实现）
│   ├── harness/                              # Harness 引擎
│   │   ├── runtime.py                        # 生命周期 / 监督器
│   │   ├── watchdog.py                       # 存活 / 死锁检测
│   │   ├── telemetry.py                      # 可观测（trace / metric / log / Sink）
│   │   ├── tracer.py                         # EngineBusTracer：三引擎活动追踪器
│   │   ├── governance.py                     # 访问控制 / 配额 / 熔断 / 预算 / 治理 Agent
│   │   └── verify.py                         # 护栏 / 输入输出校验 / 评估钩子
│   ├── loop/                                 # Loop 引擎
│   │   ├── driver.py                         # ControlLoop + RunConfig
│   │   ├── context.py                        # SharedContext / ReadOnlyContext / RoundRecord
│   │   ├── decision.py                       # DecisionAuthority + Verdict
│   │   ├── termination.py                    # 可组合 StopCondition + TerminationPolicy
│   │   └── scheduler.py                      # 自适应间隔 / 退避 / 抖动 / RetryBudget
│   ├── multi_agent/                          # Multi-Agent 引擎
│   │   ├── adapter.py                        # TargetAdapter 协议
│   │   ├── protocols.py                      # AdvisorContract / ObserverContract
│   │   ├── workers/                          # 执行型 Agent（base + example）
│   │   ├── advisors/                         # 建议型 Agent（base / trend_supervisor / risk_analyst）
│   │   └── observers/                        # 观察型 Agent（base / scribe / notifier / gov_panel）
│   ├── business/hikvision/                   # 海康门禁稳定性测试领域层
│   │   ├── adapter.py                        # HikvisionAdapter（同步阻塞 ISAPI）
│   │   ├── client.py                         # HTTP 客户端（Digest Auth + 线程锁，401 重置 auth）
│   │   ├── advisor.py / diagnostic.py / llm.py / llm_plan.py
│   │   ├── event_codes.py                    # 事件码字典
│   │   ├── scenario_schema.py                # Scenario YAML schema + 校验
│   │   ├── scenario_adapter.py               # ScenarioISAPIAdapter（真机；FakeScenarioAdapter 已删除）
│   │   ├── scenario_worker.py                # ScenarioWorker：探测→事实
│   │   ├── scenario_runner.py                # run_scenario() 装配入口
│   │   └── capabilities/                     # 能力原子化（actions/preconditions/probes）
│   │       ├── actions/                      # reboot / upgrade / remote_open / query_events / sleep / noop / switch_serial
│   │       ├── preconditions/                # baseline_record / device_online / serial_mode
│   │       └── probes/                       # field / count / online / event_chain
│   └── examples/                             # 可运行演示 / CLI
│       ├── generic_harness.py                # 通用装配模板（合成 / 真实设备双模式）
│       ├── scenario_run.py                   # YAML 驱动场景运行 CLI（主路径）
│       ├── governance_panel_demo.py          # 治理观测面板 + 趋势报告演示
│       └── _report.py                        # 共享报告工具
├── configs/                                  # 场景 YAML（数据驱动用例）
│   ├── scenario_template.yaml                # 模板：复制改字段即新增用例
│   ├── door_restart_stability.yaml
│   ├── stability_0001_reboot.yaml
│   └── stability_0009_wired_network.yaml
├── tests/                                    # pytest 测试套件（含 test_capabilities/；无 fakes/）
├── docs/                                     # MkDocs 文档站
│   ├── index.md
│   ├── plans/                                # 设计 / 演进 / 用例集
│   ├── superpowers/                          # spec / impl 记录
│   └── js/                                   # vendored mermaid.min.js + init
├── mkdocs.yml                                # 文档站配置（GitHub Pages 部署）
├── pyproject.toml                            # 零运行时依赖；dev/docs/examples 可选
├── .env.example                              # LLM_API_KEY / Hikvision 设备配置模板
├── AGENTS.md  CLAUDE.md  .gitignore
```

---

## 技术栈与依赖

- **Python 3.10+**（使用 `dataclass`、`Protocol`、`runtime_checkable`、`asyncio`）
- **零第三方运行时依赖**：框架核心仅使用标准库（`asyncio`、`logging`、`time`、`math`、`random`、`secrets`、`urllib`、`hashlib`、`json` 等）
- 可选依赖（仅开发 / 文档 / 示例美化）：
  - `dev`：`pytest>=8` + `pytest-asyncio>=0.23`
  - `docs`：`mkdocs-material` + `mkdocstrings[python]`
  - `examples`：`rich>=13`（终端美化，不装则回退标准库对齐）
- 测试框架：`pytest` + `pytest-asyncio`（`asyncio_mode = "auto"`）

---

## 事件总线话题约定

所有跨引擎通信使用以下话题前缀（命名风格必须保持一致）：

| 域 | 话题 |
|----|------|
| loop | `loop/tick`  `loop/done`  `loop/abort`  `loop/recheck`  `loop/vote/request` |
| agent | `agent/<role>/done`  `agent/vote/reply`  `agent/incident`  `agent/incident/ack` |
| harness | `harness/abort`  `harness/liveness/*`  `harness/metric/*`  `harness/govern/request`  `harness/verify/request`  `harness/fact/*` |
| governance | `governance/decision`  `governance/panel/request`  `governance/panel` |
| target | `target/acted`  `target/recovered`  `target/checked` |
| business | `hikvision/plan` 等（领域自定义） |

`EngineBusTracer`（`harness/tracer.py`）按话题前缀把每条事件归一化为带「引擎归属」
的结构化记录（Loop / MAS / Harness / Other），是分层可观测的入口。

---

## 编码规范

### 语言与风格

- 代码内注释与 docstring 使用**中文**（与现有代码风格一致）。
- 类型注解使用 `typing` 模块（兼容 Python 3.10）；允许 `X | None` 等新语法。
- 异常隔离：Agent `handle` / `run` 中的异常由基类捕获并记日志，**绝不**向上传播给发布者或总线。
- 使用 `# noqa: BLE001` 标记有意的宽泛异常捕获。
- 文档站 AGENTS.md 内容保持**中文**（与 docs/ 一致）。

### 模块间约束

- 新增模块时，确认其仅 import **同引擎**内的模块 + `core/`。
- 跨引擎功能通过 EventBus 话题实现，不要添加直接 import。
- 在顶层 `__init__.py` 中 re-export 所有公共 API 并更新 `__all__`。
- `business/` 是装配层，可以 import 任意引擎；引擎层**不可**反向 import `business/`。

### 扩展方式

| 扩展 | 仅改动 | 做法 |
|------|--------|------|
| 新目标类型 | MAS / business | 实现 `TargetAdapter` + 注册 Worker |
| 新领域操作 | MAS / business | 新增 Worker，订阅 `loop/tick` / `target/*` |
| 新建议型 Agent | MAS | 实现 `AdvisorContract`，走 vote/incident 话题 |
| 新观察型 Agent | MAS | 实现 `ObserverContract`，订阅事件 |
| 新中止条件 | Loop | 实现 `StopCondition`，组合进 `TerminationPolicy` |
| 新调度策略 | Loop | 修改 `scheduler.py` |
| 新遥测 Sink | Harness | 修改 `telemetry.py` |
| 新治理维度 | Harness | 修改 `governance.py`（已有 `DeniedOp` 多维匹配） |
| **新稳定性用例（门禁对讲 108 条等）** | **business** | **写一份 `Scenario` YAML（`configs/*.yaml`），零代码改动** |

> **数据驱动场景层**（见 `business/hikvision/scenario_*`）：把「重启/升级/下发/长巡」类用例抽象成
> `target` + `stress` + `probe` + `loop` 四段 YAML。`scenario_schema.py` 负责 schema/校验/字段解析，
> `scenario_adapter.py` 提供 `ScenarioISAPIAdapter`（真实设备；`FakeScenarioAdapter` 与 `--dry-run` 已删除），
> `scenario_worker.py` 的 `ScenarioWorker` 把探测结果产出为事实交给 `DecisionAuthority`（事实独裁：
> `probe_ok=False`→fail；`na_if_absent` 缺失→NA 不失败），`scenario_runner.py` 的 `run_scenario` 组装
> `ControlLoop`。新增用例 = 复制 `configs/scenario_template.yaml` 改字段；用
> `python -m stability_harness_loop_multiagent.examples.scenario_run --scenario <yaml> [--rounds N]` 直接运行。

> **每次扩展只改一个引擎，另两个引擎零改动。** `business/` 改动不应触发引擎层修改。

---

## 运行入口

```bash
# 通用装配模板（合成 / 真实设备双模式，env 驱动）
python stability_harness_loop_multiagent/examples/generic_harness.py

# YAML 驱动场景（数据驱动用例的主路径，需 .env 配 HIK_HOST）
python -m stability_harness_loop_multiagent.examples.scenario_run \
    --scenario configs/stability_0001_reboot.yaml [--rounds N]

# 治理观测面板 + 趋势报告（plotly 可选，回退 CSV）
python stability_harness_loop_multiagent/examples/governance_panel_demo.py
```

环境变量（见 `.env.example`）：`LLM_API_KEY`（OpenRouter，缺失则回退确定性兜底）、
`HIK_HOST` / `HIK_PASSWORD` 等设备配置、`STABILITY_*` 透传给 `generic_harness.py`。

---

## 测试

### 运行测试

```bash
# 完整测试套件（无 HIK_HOST 自动 skip 真机用例）
pytest tests/ -v

# 纯逻辑单元测试（不需真机，秒级完成）
pytest tests/ -v --ignore=tests/test_stability_scenario.py

# 事实独裁单元测试
pytest tests/test_generic_harness.py::test_generic_harness_failing_fact_tyranny -v

# 仅 hikvision 领域
pytest tests/test_hikvision_*.py -v

# 仅治理 / 校验
pytest tests/test_governance.py tests/test_verify.py tests/test_hikvision_governance*.py -v

# pytest marker 筛选（3 维：scenario_id / category / level）
pytest -m Stability_0001 -v                          # 单条用例（真机）
pytest -m "重启稳定性" -v                              # 类别筛选
pytest -m "L2" -v                                    # 等级筛选
pytest -m "重启稳定性 and L2" -v                      # 组合筛选
```

### 测试不变量

所有测试必须验证以下核心不变量：

1. **循环终止**：ControlLoop 在 `max_rounds` / `max_duration` 内必须终止，不可死锁。
2. **裁决产生**：每轮必须由 `DecisionAuthority` 产生 `Verdict`。
3. **事件扇出**：Observer 必须收到 `loop/done` 等事件（验证总线端到端可用）。
4. **事实独裁**：注入的失败事实必须强制 `fail` 裁决，即使 Advisor 投出低风险。
5. **引擎隔离**：`tests/test_harness_regression.py` 守护引擎层不互相 import。

### 新增测试规范

- 使用 `@pytest.mark.asyncio` 装饰异步测试（`asyncio_mode = "auto"` 下也可省略）。
- 使用极短超时（`vote_timeout=0.1`、`recover_timeout=0.05`）使测试快速确定。
- 使用 `MemorySink` 做遥测断言，不要使用 `PrintSink`。
- **禁止 fake / mock**：架构测试不用 `tests/fakes/`（已删除）、不引入 `FakeHikvisionClient` / `FakeScenarioAdapter` / `FakeTargetAdapter` / `unittest.mock` / `respx` / `pytest-mock`；只用纯逻辑单元测试（无外部 IO 的函数）。
- 真实设备 / 真实 LLM 验证走 `examples/scenario_run.py`（YAML 场景）或 `examples/generic_harness.py`，不进 pytest 默认套件；无 `HIK_HOST` 时 skip 不强行造数据。

---

## 关键类速查

| 类 | 模块 | 作用 |
|----|------|------|
| `EventBus` | `core/bus.py` | 唯一的跨引擎通信接缝 |
| `Agent` / `AgentSpec` | `core/agent.py` | Agent 基类与注册元数据 |
| `combine_votes` | `core/voting.py` | 规范投票合并实现（Loop 内有本地镜像） |
| `Runtime` | `harness/runtime.py` | Agent 注册表 / 生命周期 / 监督器 |
| `Watchdog` | `harness/watchdog.py` | 引擎外的存活 / 死锁检测 |
| `Governance` / `GovernanceAgent` | `harness/governance.py` | 访问控制 / 配额 / 熔断 / 预算 / 总线网关 |
| `Verifier` / `VerificationAgent` | `harness/verify.py` | 护栏 / 校验 / 评估钩子 |
| `Telemetry` / `Sink` | `harness/telemetry.py` | 可观测：trace / metric / log / sink（Print / Memory / Null） |
| `EngineBusTracer` | `harness/tracer.py` | 全总线事件追踪 + 引擎归属归一化 |
| `ControlLoop` / `RunConfig` | `loop/driver.py` | 确定性控制循环（sense→plan→act→check→decide→halt） |
| `DecisionAuthority` / `Verdict` | `loop/decision.py` | 事实独裁决策矩阵 |
| `SharedContext` / `ReadOnlyContext` | `loop/context.py` | 循环独占的可写上下文 / Agent 只读快照 |
| `TerminationPolicy` / `StopCondition` | `loop/termination.py` | 可组合 OR 中止条件 |
| `Scheduler` / `RetryBudget` | `loop/scheduler.py` | 自适应间隔 / 退避 / 抖动 / 重试预算 |
| `TargetAdapter` | `multi_agent/adapter.py` | 被操作目标的协议契约 |
| `WorkerAgent` | `multi_agent/workers/base.py` | 执行型 Agent 基类 |
| `AdvisorAgent` | `multi_agent/advisors/base.py` | 建议型 Agent 基类 |
| `ObserverAgent` | `multi_agent/observers/base.py` | 观察型 Agent 基类 |
| `GovernancePanelAgent` | `multi_agent/observers/gov_panel.py` | 治理决策事实观测面板（dashboard） |
| `HikvisionAdapter` | `business/hikvision/adapter.py` | 海康 ISAPI 同步适配器 |
| `Scenario` / `from_yaml` | `business/hikvision/scenario_schema.py` | YAML 场景 schema |
| `ScenarioISAPIAdapter` | `business/hikvision/scenario_adapter.py` | 场景适配器（真机；`FakeScenarioAdapter` 已删除） |
| `ScenarioWorker` | `business/hikvision/scenario_worker.py` | 探测→事实的 Worker |
| `run_scenario` | `business/hikvision/scenario_runner.py` | 场景化组装 ControlLoop |
| `capabilities/actions/*` | `business/hikvision/capabilities/actions/` | 原子化执行能力（reboot/upgrade/remote_open/query_events/sleep/noop/switch_serial） |
| `capabilities/probes/*` | `business/hikvision/capabilities/probes/` | 原子化探测能力（field/count/online/event_chain） |
| `capabilities/preconditions/*` | `business/hikvision/capabilities/preconditions/` | 原子化前置条件（baseline_record/device_online/serial_mode） |

---

## 常见陷阱

1. **不要在 loop/ 中 import multi_agent/**：ControlLoop 内的投票合并是本地镜像实现，不引用 `core.voting.combine_votes`。
2. **不要让 Advisor 裁决 pass/fail**：Advisor 只投票（risk, confidence），不直接影响 verdict。
3. **不要省略超时**：每个 `asyncio.sleep` / `wait_for` 都有明确的超时值。
4. **不要在 Agent 之间直接通信**：一切走 EventBus，包括同引擎内。
5. **处理 `error=True` 的路径**：`DecisionAuthority.decide(error=True)` 必须返回保守 warn，不能 pass。
6. **不要把 bus.py / agent.py 写回 harness/**：它们已迁入 `core/`，顶层 `__init__` re-export 保持对外 API 不变。
7. **真实设备 IO 必须包 `asyncio.to_thread`**：`HikvisionAdapter` 等同步阻塞 HTTP 不包裹会卡死事件循环，令看门狗/超时安全网失效。
8. **治理拒绝语义是 fail-closed**：网关超时 / 拒绝只拦操作、不 halt 循环（`emit_abort` 默认 `False`），worker 跳过被拒操作仍执行其余操作。

---

## 架构演进路线（持续改进方向）

> 当前三引擎职责到位、互不 import 的底线成立。已知的结构性改进方向已系统记录在
> **`docs/plans/架构演进路线.md`**，任何 Agent 在动手重构前应先阅读该文件，避免重复分析或偏离路线。

核心结论速记：

- **P0 — 契约内核 `core/` 拆分（已完成 · 2026-07-18）**：`loop` 与 `multi_agent` 现只依赖
  `core.agent` + `core.bus`（原 `harness.agent` / `harness.bus` 已迁入 `core/`，两者自包含、零内部依赖）。
  三引擎现真正对等、边界在模块层强制；顶层 `__init__` 仍 re-export 全部公共 API，对外零影响。
  验证：`pytest` 全绿 + `examples/smoke.py` 通过 + 全仓 `harness.agent|harness.bus` 引用为 0。
- **P1 — Governance / Verify 经总线落地（已完成 · 2026-07-18）**：治理能力已真正可用——
  `Governance.evaluate` 改为两阶段（拒绝不突变配额/预算）、新增 `gate_allowed` / `governance_decision`
  异步 fail-closed 闸门、`test_governance.py` / `test_verify.py` 锁定行为；hikvision runner 以 **opt-in**
  方式挂载 `GovernanceAgent` / `VerificationAgent`，worker `act()` 入口做每轮粗粒度闸门。拒绝语义为
  fail-closed 只拦操作、不 halt 循环（`emit_abort` 默认 `False`）。
  - **P1-b 校验真触发**：`HikvisionAdvisor` 解析计划后发 `harness/verify/request`（`enable_verify`，
    fail-closed 丢弃），让挂载但未触发的 `VerificationAgent` 真正生效。
  - **P1-c 熔断器真包裹**：worker 破坏性外部操作经 `_guarded_adapter_act` 受
    `governance.breakers["hikvision-api"]` 保护，连续失败达阈值后跳过调用、避免打爆设备。
  - **P1-d 按操作鉴权（维度化）**：治理新增 `DeniedOp(role?, capability?, op, match?)` 维度规则
    （字符串配置自动归一，向后兼容）；`role` / `capability` 支持 `None` 与 `"*"` 通配；`op` 支持
    `exact`（默认）/ `prefix` / `suffix` / `contains` / `regex` 五种匹配方式，非法正则按未命中处理。
    治理决策点经 `Telemetry.fact("governance.decision", ...)` 发结构化事实（话题 `harness/fact/governance.decision`），
    fail-closed 超时路径由 worker 经 `governance.telemetry` 补发。新增 `GovernancePanelAgent`（Observer，opt-in）
    订阅该事实并聚合成可读「治理观测面板」（放行/拒绝分布、按角色/能力/操作的拒绝计数、超时 fail-closed 计数、轮次覆盖）；
    面板响应 `governance/panel/request` 回发 `governance/panel`（含聚合 `panel` 与时间序列 `timeseries`），
    演示见 `examples/governance_panel_demo.py`（用拉取的 `timeseries` 生成 plotly 交互式趋势报告，未装 plotly 时回退零依赖 CSV）。
- **P2 — ControlLoop 事件驱动化（已完成 · 2026-07-18）**：`_run_round` 改为"收齐 `target/recovered` +
  `target/checked` 即返回 + 超时兜底"；**P2-b** `_collect_votes` 同样事件驱动（静默期 `vote_settle` 提前返回、
  无投票者仍于 `vote_timeout` 上限内终止）。实测全量测试从 ~195s 降至 ~32s（约 160s+ 无效等待消除）。
  防死锁不变量不变（超时仍为硬上限）。
- **P3（可选）— 依赖抽象化（暂缓 · YAGNI）**：用 `Protocol` 定义 `BusProtocol` 进阶收益；当前无替换总线需求，
  按 YAGNI 暂缓，需求出现再推进。

执行顺序建议：先 P0 打地基 → 再 P1 落地治理能力并补单测 → 后 P2 消除时序缺陷 → P3 按需。
详见 `docs/plans/架构演进路线.md`。

---

## 文档站

- 配置：`mkdocs.yml`（Material 主题，中文 `language: zh`）。
- 部署：`.github/workflows/mkdocs-deploy.yml` 推送到 GitHub Pages
  （https://sunbos.github.io/stability-check/）。
- Mermaid 图：`docs/js/mermaid.min.js`（vendored）+ `docs/js/mermaid-init.js`
  （用 `mermaid.run({ nodes })` 渲染 `<div class="mermaid">`，superfences 用 `fence_div_format`）。
- 修改 `docs/` 下 markdown 后，部署会自动触发；浏览器需硬刷新（Ctrl+F5）清缓存。
