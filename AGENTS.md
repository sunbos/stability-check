# AGENTS.md — AI Agent 协作指南

> 本文件面向所有参与本仓库开发的 AI 编码 Agent（Copilot / Cursor / Antigravity / Devin 等），
> 提供项目上下文、架构约束和编码规范，以便 Agent 能快速、正确地理解并修改代码。

---

## 项目概述

**stability_harness_loop_multiagent** 是一个**与领域无关**的可复用 Python 框架，
用「三引擎」架构构建**自治循环系统**：

| 引擎 | 职责 | 包路径 |
|------|------|--------|
| **Harness** | 运行时 / 治理 / 可观测 / 校验 / 看门狗 | `stability_harness_loop_multiagent/harness/` |
| **Loop** | 确定性控制循环 / 裁决 / 中止 / 调度 | `stability_harness_loop_multiagent/loop/` |
| **Multi-Agent (MAS)** | 领域执行 / 建议 / 观察（Worker / Advisor / Observer） | `stability_harness_loop_multiagent/multi_agent/` |

详细设计文档见 `docs/plans/设计文档.md`。

---

## 核心架构约束（必读）

### 1. 三引擎互不 import

```
harness ←(EventBus)→ loop ←(EventBus)→ multi_agent
```

- **禁止** `harness/` 的模块 import `loop/` 或 `multi_agent/`。
- **禁止** `loop/` 的模块 import `multi_agent/`（反之亦然）。
- 所有跨引擎通信**只走 EventBus**（pub/sub + request/reply + `#` 通配）。
- 在做任何修改前请验证此约束。

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

---

## 目录结构

```
stability-check/
├── stability_harness_loop_multiagent/   # 主包
│   ├── __init__.py                      # 公共 API re-export
│   ├── harness/                         # Harness 引擎
│   │   ├── bus.py                       # EventBus（唯一接缝）
│   │   ├── agent.py                     # Agent 基类 + AgentSpec
│   │   ├── runtime.py                   # 生命周期 / 监督器
│   │   ├── watchdog.py                  # 存活 / 死锁检测
│   │   ├── telemetry.py                 # 可观测 (trace / metric / log)
│   │   ├── governance.py                # 访问控制 / 配额 / 熔断 / 预算
│   │   └── verify.py                    # 护栏 / 输入输出校验 / 评估钩子
│   ├── loop/                            # Loop 引擎
│   │   ├── driver.py                    # ControlLoop + RunConfig
│   │   ├── context.py                   # SharedContext / ReadOnlyContext / RoundRecord
│   │   ├── decision.py                  # DecisionAuthority + Verdict
│   │   ├── termination.py               # 可组合 StopCondition + TerminationPolicy
│   │   └── scheduler.py                 # 自适应间隔 / 退避 / 抖动 / RetryBudget
│   ├── multi_agent/                     # Multi-Agent 引擎
│   │   ├── adapter.py                   # TargetAdapter 协议
│   │   ├── protocols.py                 # AdvisorContract / ObserverContract / combine_votes
│   │   ├── workers/                     # 执行型 Agent
│   │   ├── advisors/                    # 建议型 Agent（投票 + 事件）
│   │   └── observers/                   # 观察型 Agent（记录 / 通知 / 治理面板）
│   └── examples/
│       └── smoke.py                     # 端到端冒烟演示
├── tests/
│   └── test_stability_harness_loop_multiagent_smoke.py
├── docs/plans/
│   └── 设计文档.md                       # 完整设计文档
└── .gitignore
```

---

## 技术栈与依赖

- **Python 3.10+**（使用 `dataclass`、`Protocol`、`runtime_checkable`、`asyncio`）
- **零第三方依赖**：仅使用标准库（`asyncio`、`logging`、`time`、`math`、`random`、`secrets`）
- 测试框架：`pytest` + `pytest-asyncio`

---

## 事件总线话题约定

所有跨引擎通信使用以下话题前缀：

| 域 | 话题 |
|----|------|
| loop | `loop/tick`  `loop/done`  `loop/abort`  `loop/recheck`  `loop/vote/request` |
| agent | `agent/<role>/done`  `agent/vote/reply`  `agent/incident`  `agent/incident/ack` |
| harness | `harness/abort`  `harness/liveness/*`  `harness/metric/*`  `harness/govern/request`  `harness/verify/request` |
| target | `target/acted`  `target/recovered`  `target/checked` |

新增话题必须遵循此命名风格。

---

## 编码规范

### 语言与风格

- 代码内注释与 docstring 使用**中文**。
- 类型注解使用 `typing` 模块（兼容 Python 3.10）。
- 异常隔离：Agent `handle` / `run` 中的异常由基类捕获并记日志，**绝不**向上传播给发布者或总线。
- 使用 `# noqa: BLE001` 标记有意的宽泛异常捕获。

### 模块间约束

- 新增模块时，确认其仅 import **同引擎**内的模块。
- 跨引擎功能通过 EventBus 话题实现，不要添加直接 import。
- 在 `__init__.py` 中 re-export 所有公共 API 并更新 `__all__`。

### 扩展方式

| 扩展 | 仅改动 | 做法 |
|------|--------|------|
| 新目标类型 | MAS | 实现 `TargetAdapter` + 注册 Worker |
| 新领域操作 | MAS | 新增 Worker，订阅 `loop/tick` / `target/*` |
| 新建议型 Agent | MAS | 实现 `AdvisorContract`，走 vote/incident 话题 |
| 新观察型 Agent | MAS | 实现 `ObserverContract`，订阅事件 |
| 新中止条件 | Loop | 实现 `StopCondition`，组合进 `TerminationPolicy` |
| 新调度策略 | Loop | 修改 `scheduler.py` |
| 新遥测 Sink | Harness | 修改 `telemetry.py` |
| **新稳定性用例（门禁对讲 108 条等）** | **business** | **写一份 `Scenario` YAML（`configs/*.yaml`），零代码改动**；复杂新操作类型才改 `scenario_adapter.py` |

> **数据驱动场景层**（见 `business/hikvision/scenario_*`）：把「重启/升级/下发/长巡」类用例抽象成
> `target` + `stress` + `probe` + `loop` 四段 YAML。`scenario_schema.py` 负责 schema/校验/字段解析，
> `scenario_adapter.py` 提供 `ScenarioISAPIAdapter`（真实设备）与 `FakeScenarioAdapter`（dry-run/测试），
> `scenario_worker.py` 的 `ScenarioWorker` 把探测结果产出为事实交给 `DecisionAuthority`（事实独裁：
> `probe_ok=False`→fail；`na_if_absent` 缺失→NA 不失败），`scenario_runner.py` 的 `run_scenario` 组装
> `ControlLoop`。新增用例 = 复制 `configs/scenario_template.yaml` 改字段；用
> `python -m stability_harness_loop_multiagent.examples.scenario_run --scenario <yaml> [--dry-run]` 直接运行。

> **每次扩展只改一个引擎，另两个引擎零改动。**

---

## 测试

### 运行测试

```bash
# 完整测试套件
pytest tests/ -v

# 独立冒烟演示（不依赖 pytest）
python stability_harness_loop_multiagent/examples/smoke.py
```

### 测试不变量

所有测试必须验证以下核心不变量：

1. **循环终止**：ControlLoop 在 `max_rounds` / `max_duration` 内必须终止，不可死锁。
2. **裁决产生**：每轮必须由 `DecisionAuthority` 产生 `Verdict`。
3. **事件扇出**：Observer 必须收到 `loop/done` 等事件（验证总线端到端可用）。
4. **事实独裁**：注入的失败事实必须强制 `fail` 裁决，即使 Advisor 投出低风险。

### 新增测试规范

- 使用 `@pytest.mark.asyncio` 装饰异步测试。
- 使用极短超时（`vote_timeout=0.1`、`recover_timeout=0.05`）使测试快速确定。
- 使用 `MemorySink` 做遥测断言，不要使用 `PrintSink`。
- 使用 `FakeTargetAdapter` 或类似的合成适配器，不要引入真实外部依赖。

---

## 关键类速查

| 类 | 模块 | 作用 |
|----|------|------|
| `EventBus` | `harness/bus.py` | 唯一的跨引擎通信接缝 |
| `Agent` / `AgentSpec` | `harness/agent.py` | Agent 基类与注册元数据 |
| `Runtime` | `harness/runtime.py` | Agent 注册表 / 生命周期 / 监督器 |
| `Watchdog` | `harness/watchdog.py` | 引擎外的存活 / 死锁检测 |
| `Governance` | `harness/governance.py` | 访问控制 / 配额 / 熔断 / 预算 |
| `Verifier` | `harness/verify.py` | 护栏 / 校验 / 评估钩子 |
| `Telemetry` | `harness/telemetry.py` | 可观测：trace / metric / log / sink |
| `ControlLoop` | `loop/driver.py` | 确定性控制循环（sense→plan→act→check→decide→halt） |
| `RunConfig` | `loop/driver.py` | 声明式运行参数 → TerminationPolicy |
| `DecisionAuthority` | `loop/decision.py` | 事实独裁决策矩阵 |
| `SharedContext` | `loop/context.py` | 循环独占的可写上下文 |
| `TerminationPolicy` | `loop/termination.py` | 可组合 OR 中止条件 |
| `Scheduler` | `loop/scheduler.py` | 自适应间隔 / 退避 / 抖动 / 重试预算 |
| `TargetAdapter` | `multi_agent/adapter.py` | 被操作目标的协议契约 |
| `WorkerAgent` | `multi_agent/workers/` | 执行型 Agent 基类 |
| `AdvisorAgent` | `multi_agent/advisors/` | 建议型 Agent 基类 |
| `ObserverAgent` | `multi_agent/observers/` | 观察型 Agent 基类 |
| `GovernancePanelAgent` | `multi_agent/observers/gov_panel.py` | 治理决策事实观测面板（dashboard） |

---

## 常见陷阱

1. **不要在 loop/ 中 import multi_agent/**：ControlLoop 内的投票合并是本地镜像实现，不引用 `protocols.combine_votes`。
2. **不要让 Advisor 裁决 pass/fail**：Advisor 只投票（risk, confidence），不直接影响 verdict。
3. **不要省略超时**：每个 `asyncio.sleep` / `wait_for` 都有明确的超时值。
4. **不要在 Agent 之间直接通信**：一切走 EventBus，包括同引擎内。
5. **处理 `error=True` 的路径**：`DecisionAuthority.decide(error=True)` 必须返回保守 warn，不能 pass。

---

## 架构演进路线（持续改进方向）

> 当前三引擎职责到位、互不 import 的底线成立。已知的结构性改进方向已系统记录在
> **`docs/plans/架构演进路线.md`**，任何 Agent 在动手重构前应先阅读该文件，避免重复分析或偏离路线。

核心结论速记：

- **P0 — 契约内核 `core/` 拆分（已完成 · 2026-07-18）**：`loop` 与 `multi_agent` 现只依赖
  `core.agent`+`core.bus`（原 `harness.agent`/`harness.bus` 已迁入 `core/`，两者自包含、零内部依赖）。
  三引擎现真正对等、边界在模块层强制；顶层 `__init__` 仍 re-export 全部公共 API，对外零影响。
  验证：`pytest` 全绿 + `examples/smoke.py` 通过 + 全仓 `harness.agent|harness.bus` 引用为 0。
- **P1 — Governance / Verify 经总线落地（已完成 · 2026-07-18）**：治理能力已真正可用——
  `Governance.evaluate` 改为两阶段（拒绝不突变配额/预算）、新增 `gate_allowed`/`governance_decision`
  异步 fail-closed 闸门、`test_governance.py`/`test_verify.py` 锁定行为；hikvision runner 以 **opt-in**
  方式挂载 `GovernanceAgent`/`VerificationAgent`，worker `act()` 入口做每轮粗粒度闸门。拒绝语义为
  fail-closed 只拦操作、不 halt 循环（`emit_abort` 默认 `False`）。
  - **P1-b 校验真触发**：`HikvisionAdvisor` 解析计划后发 `harness/verify/request`（`enable_verify`，
    fail-closed 丢弃），让挂载但未触发的 `VerificationAgent` 真正生效。
  - **P1-c 熔断器真包裹**：worker 破坏性外部操作经 `_guarded_adapter_act` 受
    `governance.breakers["hikvision-api"]` 保护，连续失败达阈值后跳过调用、避免打爆设备。
  - **P1-d 按操作鉴权（维度化）**：治理新增 `DeniedOp(role?, capability?, op, match?)` 维度规则（字符串配置自动归一，向后兼容）；
    `role`/`capability` 支持 `None` 与 `"*"` 通配（匹配任意）；`op` 支持 `match="exact"`（默认，精确相等）/
    `"prefix"`（前缀）/ `"suffix"`（后缀）/ `"contains"`（子串）/ `"regex"`（正则全匹配）五种匹配方式，非法正则按未命中处理。
    网关按 role/capability/op 维度匹配后回复 `denied_ops`；worker 跳过被拒操作（如 `reboot`）仍执行其余操作（如 `remote_open_door`）。
    治理决策点经 `Telemetry.fact("governance.decision", ...)` 发结构化事实（kind=`fact`，总线话题 `harness/fact/governance.decision`），
    fail-closed 超时路径由 worker 经 `governance.telemetry` 补发。新增 `GovernancePanelAgent`（Observer，opt-in）订阅该事实
    并聚合成可读「治理观测面板」（放行/拒绝分布、按角色/能力/操作的拒绝计数、超时 fail-closed 计数、轮次覆盖）；
    面板响应 `governance/panel/request` 回发 `governance/panel`（内含聚合 `panel` 与时间序列 `timeseries`，实现"经总线真实拉取"），
    演示见 `examples/governance_panel_demo.py`（其用拉取的 `timeseries` 生成 plotly 交互式趋势报告，未装 plotly 时回退零依赖 CSV）。
    runner 在挂载治理网关时一并 `governance.telemetry=tel` 并把面板加入返回字典 `gov_panel`，观测面板可见「哪一轮、为何被拒、拒绝哪些」。
- **P2 — ControlLoop 事件驱动化（已完成 · 2026-07-18）**：`_run_round` 改为"收齐 `target/recovered` +
  `target/checked` 即返回 + 超时兜底"；**P2-b** `_collect_votes` 同样事件驱动（静默期 `vote_settle` 提前返回、
  无投票者仍于 `vote_timeout` 上限内终止）。实测全量测试从 ~195s 降至 ~32s（约 160s+ 无效等待消除）。
  防死锁不变量不变（超时仍为硬上限）。
- **P3（可选）— 依赖抽象化（暂缓 · YAGNI）**：用 `Protocol` 定义 `BusProtocol` 进阶收益；当前无替换总线需求，
  按 YAGNI 暂缓，需求出现再推进。

执行顺序建议：先 P0 打地基 → 再 P1 落地治理能力并补单测 → 后 P2 消除时序缺陷 → P3 按需。
详见演进路线文档。
