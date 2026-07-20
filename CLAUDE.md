# CLAUDE.md — Claude 项目指南

> 本文件为 Claude（Anthropic）提供本项目的上下文，使其能快速理解代码库并正确地
> 进行修改、调试和扩展。完整的协作规范见 `AGENTS.md`，本文件只保留 Claude 高频
> 需要的精简信息。

---

## 项目简介

`stability_harness_loop_multiagent` 是一个**纯 Python、零第三方运行时依赖**的通用框架，
用于构建基于「三引擎 + 契约内核」架构的自治循环系统。框架与领域无关——具体场景
（拷机、巡检、压测、运维编排等）通过实现 `TargetAdapter` 和注册角色化 Agent 来剪裁；
当前内置领域实现是海康门禁稳定性测试（`business/hikvision/`）。

---

## 架构一句话

```
core（契约内核：EventBus / Agent / combine_votes）
   ↑           ↑           ↑
Harness（让系统活着且受约束）
    ↕ EventBus（唯一接缝）
Loop（驱动确定性迭代、独裁裁决）
    ↕ EventBus
Multi-Agent（执行领域操作 + 投票建议 + 观察上报）
    ↕
business/（领域装配层，可 import 任意引擎）
```

**三引擎互不 import，所有跨引擎通信走 EventBus。** `core/` 是三个引擎共同依赖的
零内部依赖契约包，使三引擎在模块层真正对等。

---

## 最重要的规则

### 1. 引擎隔离

- `harness/` 绝不 import `loop/` 或 `multi_agent/`。
- `loop/` 绝不 import `multi_agent/`（反之亦然）。
- 三个引擎都只 import `core/`（不互相 import）。
- `business/` 是装配层，可 import 任意引擎；引擎层不可反向 import `business/`。
- 违反此规则会破坏整个架构的可替换性（由 `tests/test_harness_regression.py` 守护）。

### 2. 事实独裁（绝对安全底线）

在 `loop/decision.py` 中的 `DecisionAuthority`：

```python
# 任何一个事实为 False → 裁决为 fail，风险分无法翻转
for name, ok in facts.items():
    if not ok:
        return Verdict("fail", ...)
```

- 风险分仅加 `warn`（60-80）或 `recheck`（>80）注解。
- `critical` 事件强制 `recheck`。
- 决策出错 → 保守 `warn(60)`（`CONSERVATIVE_RISK`），**绝不** `pass`。

### 3. 不可变快照

- `SharedContext` 由 `ControlLoop` 独占写入。
- Agent 只能通过 `ReadOnlyContext` 冻结快照观察。
- Agent 私有状态在 `self.state` 字典中，绝不共享。

### 4. 防死锁（事件驱动 + 超时兜底）

- 每个跨引擎 `await` 都有超时 + 确定性兜底。
- 投票超时 → `NEUTRAL_RISK = 50`；恢复超时 → 记为失败。
- `max_rounds` / `max_duration` 硬上限保证循环一定终止。
- ControlLoop `_run_round` / `_collect_votes` 已**事件驱动**：收齐目标事件即返回，
  不再空等满超时（实测测试耗时从 ~195s 降至 ~32s）。

---

## 关键文件

| 文件 | 作用 |
|------|------|
| `core/bus.py` | EventBus：publish / subscribe / request-reply / `#` 通配 |
| `core/agent.py` | Agent 基类 + AgentSpec 注册元数据 |
| `core/voting.py` | `combine_votes`（规范实现，Loop 内有本地镜像） |
| `harness/runtime.py` | Agent 生命周期管理、监督器重启 |
| `harness/watchdog.py` | 引擎外的存活/死锁检测器 |
| `harness/governance.py` | AccessControl / Quota / Budget / CircuitBreaker / GovernanceAgent |
| `harness/verify.py` | Verifier 护栏、评估钩子、VerificationAgent |
| `harness/telemetry.py` | 可观测：Sink（Print / Memory / Null） + Telemetry |
| `harness/tracer.py` | EngineBusTracer：全总线事件追踪 + 引擎归属归一化 |
| `loop/driver.py` | ControlLoop（sense→plan→act→check→decide→halt）+ RunConfig |
| `loop/decision.py` | DecisionAuthority + Verdict + NEUTRAL_RISK / CONSERVATIVE_RISK |
| `loop/context.py` | SharedContext / ReadOnlyContext / RoundRecord |
| `loop/termination.py` | StopCondition + TerminationPolicy（OR 组合） |
| `loop/scheduler.py` | Scheduler + RetryBudget + clamp |
| `multi_agent/adapter.py` | TargetAdapter 协议（act / observe / events） |
| `multi_agent/protocols.py` | AdvisorContract / ObserverContract |
| `multi_agent/workers/` | WorkerAgent 基类 + ExampleWorkerAgent |
| `multi_agent/advisors/` | AdvisorAgent + TrendSupervisorAgent + RiskAnalyst |
| `multi_agent/observers/` | ObserverAgent + ScribeAgent + NotifierAgent + GovernancePanelAgent |
| `business/hikvision/adapter.py` | HikvisionAdapter（同步阻塞 ISAPI，需 `asyncio.to_thread` 包裹） |
| `business/hikvision/client.py` | HTTP 客户端（Digest Auth + 线程锁，401 重置 auth 实例） |
| `business/hikvision/scenario_schema.py` | Scenario YAML schema + 校验 |
| `business/hikvision/scenario_adapter.py` | ScenarioISAPIAdapter（真机；`FakeScenarioAdapter` 已删除） |
| `business/hikvision/scenario_worker.py` | ScenarioWorker：探测→事实 |
| `business/hikvision/scenario_runner.py` | `run_scenario()`：YAML 场景装配 ControlLoop |
| `business/hikvision/capabilities/` | 能力原子化（actions / preconditions / probes 三类） |
| `examples/generic_harness.py` | 通用装配模板（合成 / 真实设备双模式） |
| `examples/scenario_run.py` | YAML 驱动场景运行 CLI（主路径） |
| `docs/plans/架构演进路线.md` | P0-P3 演进路线（动手重构前必读） |
| `docs/plans/系统架构.md` | 完整系统架构 + Mermaid 图 |

---

## 常用命令

```bash
# 运行所有测试（无 HIK_HOST 自动 skip 真机用例）
pytest tests/ -v

# 纯逻辑单元测试（不需真机，秒级完成）
pytest tests/ -v --ignore=tests/test_stability_scenario.py

# 事实独裁单元测试
pytest tests/test_generic_harness.py::test_generic_harness_failing_fact_tyranny -v

# 仅治理 / 校验
pytest tests/test_governance.py tests/test_verify.py tests/test_hikvision_governance*.py -v

# pytest marker 筛选（3 维：scenario_id / category / level）
pytest -m Stability_0001 -v                  # 单条用例（真机）
pytest -m "重启稳定性" -v                      # 类别筛选

# YAML 驱动场景（数据驱动用例的主路径，需 .env 配 HIK_HOST）
python -m stability_harness_loop_multiagent.examples.scenario_run \
    --scenario configs/stability_0001_reboot.yaml --rounds 3

# 通用装配模板（合成 / 真实设备双模式，env 驱动）
python stability_harness_loop_multiagent/examples/generic_harness.py

# 本地预览文档站
mkdocs serve  # 需要 [docs] 可选依赖
```

环境变量（见 `.env.example`）：`LLM_API_KEY`（OpenRouter，缺失则回退确定性兜底）、
`HIK_HOST` / `HIK_PASSWORD` 等设备配置。

---

## 代码风格

- **语言**：代码注释、docstring、提交信息使用**中文**（与现有代码一致）。
- **类型注解**：使用 `typing` 模块（兼容 Python 3.10+）；允许 `X | None` 等新语法。
- **异常策略**：Agent 内部的异常被基类捕获并记日志，绝不泄漏给 EventBus 发布者。宽泛异常捕获标记 `# noqa: BLE001`。
- **零第三方运行时依赖**：框架核心仅使用标准库。`dev` / `docs` / `examples` 是可选 extras。
- **新增模块**：只 import 同引擎 + `core/`；跨引擎走 EventBus；公共 API 在顶层 `__init__.py` re-export 并更新 `__all__`。

---

## 事件总线话题速查

```
loop/tick               → 触发 Worker 执行
loop/done               → 一轮结束，Advisor 订阅此话题
loop/abort              → 循环中止
loop/recheck            → 触发 recheck（最多 1 次）
loop/vote/request       → 请求 Advisor 投票
agent/<role>/done       → Worker 完成操作
agent/vote/reply        → Advisor 回复投票
agent/incident          → Advisor 报告事件（warn/critical）
agent/incident/ack      → 循环对事件的 ACK
harness/abort           → 看门狗/治理中止信号
harness/govern/request  → 治理请求
harness/verify/request  → 校验请求
harness/fact/*          → 结构化事实（如 governance.decision）
governance/panel        → 治理观测面板（含 panel + timeseries）
target/acted            → Worker 已执行操作
target/recovered        → 目标已恢复
target/checked          → 事实检查结果
hikvision/plan          → 业务计划（领域自定义）
```

---

## 如何扩展

### 添加新场景（最常见 · 数据驱动）

1. 复制 `configs/scenario_template.yaml`，改 `id` / `name` / `target` / `stress` / `probe` / `loop` 字段。
2. 用 `python -m stability_harness_loop_multiagent.examples.scenario_run --scenario <yaml> --rounds N` 验证（需 `.env` 配 `HIK_HOST`；无真机时跑 `pytest tests/test_scenario.py -v` 验证 schema/装配）。
3. **三引擎代码零改动。** 仅当引入全新操作类型才需改 `scenario_adapter.py`。

### 添加新场景（指令式 · 自定义 Worker）

1. 实现 `TargetAdapter`（`multi_agent/adapter.py` 中的协议）。
2. 创建 `WorkerAgent` 子类，在 `check()` 中编写领域检查逻辑。
3. 可选：添加 `AdvisorAgent`（投票）和 `ObserverAgent`（记录/通知）。
4. 用 `RunConfig` 配置中止条件，创建 `ControlLoop` 并启动。
5. **Harness 和 Loop 引擎代码零改动。**

### 添加新中止条件

1. 继承 `StopCondition`，实现 `evaluate(ctx) -> (bool, str)`。
2. 加入 `TerminationPolicy` 的 `conditions` 列表。
3. 可选用 `precedence` 参数调整优先级。

### 添加新治理策略

1. 使用 `AccessControl`（白名单）、`Quota`（限流）、`Budget`（预算）、`CircuitBreaker`（熔断）。
2. 通过 `Governance.evaluate()` 或 `GovernanceAgent`（总线挂载）启用。
3. 按操作鉴权用 `DeniedOp(role?, capability?, op, match?)`，支持 `exact` / `prefix` / `suffix` / `contains` / `regex` 五种匹配。

---

## 关键设计决策

### 为什么 ControlLoop 中有 combine_votes 的本地镜像？

`core/voting.py` 中有规范的 `combine_votes`，但 `ControlLoop`（`loop/driver.py`）
有自己的 `_default_combine`。这是**故意的**——为了保持引擎隔离，Loop 绝不 import
Multi-Agent 或 core.voting。两份实现逻辑一致：

- 快速路径：任意 risk ≥ 90 立即胜出。
- 弃权（confidence ≤ 0）权重为 0。
- 全部弃权 → 中性默认 50（`NEUTRAL_RISK`）。

### 为什么 Advisor 不能裁决？

Advisor 只投 `(risk, confidence)` 票，由 `DecisionAuthority` 做最终裁决。
这保证了安全底线的确定性——事实独裁不会被任何 Agent 绕过。

### 为什么用 EventBus 而不是直接方法调用？

今天进程内（`asyncio`），明天可换 A2A / MCP 网络传输而**不改 Agent 代码**。
这是架构可替换性的核心保障。

### 为什么 ControlLoop 是事件驱动的？

P2 演进把 `_run_round` / `_collect_votes` 从"等满超时"改为"收齐目标事件即返回 + 超时兜底"，
消除了约 160s+ 的无效等待。防死锁不变量不变（超时仍为硬上限）。

### 为什么需要 `core/` 子包？

P0 演进把 `EventBus` / `Agent` / `combine_votes` 从 `harness/` 迁入 `core/`，让三引擎
真正对等、边界在模块层强制。顶层 `__init__.py` re-export 保持对外 API 完全不变。

### 为什么真实设备 IO 必须包 `asyncio.to_thread`？

`HikvisionAdapter` 是同步阻塞 HTTP（单次操作可能阻塞 30-180s），不包裹会卡死事件循环，
令看门狗/超时安全网失效（见 spec §3.1.3 长 IO 规则）。

### 为什么治理拒绝是 fail-closed 不 halt？

治理网关超时 / 拒绝只拦操作、不 halt 循环（`emit_abort` 默认 `False`）。worker 跳过被拒
操作（如 `reboot`）仍执行其余操作（如 `remote_open_door`），保证测试连续性。

---

## 不要做的事

| ❌ 禁止 | ✅ 应该 |
|---------|---------|
| 在 `loop/` 中 import `multi_agent/` | 通过 EventBus 话题通信 |
| 把 `bus.py` / `agent.py` 写回 `harness/` | 它们已迁入 `core/`，对外 API 由顶层 `__init__` 保持不变 |
| 让 Advisor 直接修改 verdict | Advisor 只投票，Loop 裁决 |
| 省略 `await` 的超时 | 每个跨引擎 await 都有超时 |
| 在 Agent 之间直接引用 | 通过 `self.bus.publish()` 通信 |
| 引入第三方 pip 包（运行时） | 仅使用标准库；dev/docs/examples 走可选 extras |
| 将失败事实翻转为 pass | 事实独裁：False → fail，不可翻转 |
| 共享可变状态 | 使用 ReadOnlyContext 快照 + Agent 私有 state |
| 让 `error=True` 路径返回 pass | 保守 warn(60)，绝不乐观 pass |
| 真实设备同步 IO 不包 `to_thread` | 用 `asyncio.to_thread` 包裹阻塞调用 |
| 引擎层 import `business/` | `business/` 只能被引擎装配，不能反向依赖 |

---

## 测试哲学

- 测试验证**架构不变量**，而非实现细节。
- **禁止 fake / mock**：不使用 `tests/fakes/`（已删除）、不引入 `FakeHikvisionClient` / `FakeScenarioAdapter` / `FakeTargetAdapter` / `unittest.mock` / `respx` / `pytest-mock`；只用纯逻辑单元测试（无外部 IO 的函数）。
- 使用极短超时使测试快速确定性完成。
- 核心不变量：循环终止、裁决产生、事件扇出、事实独裁、引擎隔离。
- 真实设备 / 真实 LLM 验证走 `examples/scenario_run.py`（YAML 场景）或 `examples/generic_harness.py`，不进 pytest 默认套件；无 `HIK_HOST` 时 skip 不强行造数据。
- 遥测断言用 `MemorySink`，不用 `PrintSink`。

---

## 进一步阅读

- `AGENTS.md` —— 完整协作规范（目录结构、关键类速查、扩展方式矩阵、演进路线详记）。
- `docs/plans/架构演进路线.md` —— P0-P3 演进路线，动手重构前必读。
- `docs/plans/系统架构.md` —— 完整系统架构 + 4 张 Mermaid 图。
- `docs/superpowers/specs/2026-07-17-hikvision-door-stability-design.md` —— 门禁稳定性 spec。
- `.env.example` —— 环境变量模板。
