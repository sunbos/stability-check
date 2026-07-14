# StabilityHarnessLoopMultiAgent — 通用三范式模板 v1（全功能）

> 一个**与领域无关**的可复用模板，用三个范式构建「自治循环系统」：
> **Harness Engineering**（运行时/治理）、**Loop Engineering**（控制循环/决策）、
> **Multi-Agent System**（领域执行/建议/观察）。
> 本模板刻意做到**全功能覆盖**；具体场景（如拷机、巡检、压测、运维编排）在其上做**剪裁**即可。
>
> 设计原则、命名、功能清单均对齐业界最佳实践文献（见文末参考）。

## 1. 设计原则（锁定）

1. **三引擎职责单一且互不调用**：HARNESS（让系统活着且受约束）、
   LOOP（驱动确定性迭代并独裁裁决）、MAS（执行领域操作 + 自治监控）。
2. **EventBus 是唯一接缝**：跨引擎只能走总线（pub/sub + req/resp + `#` 通配）。
   今天进程内，明天可换 A2A / MCP 网络传输而**不改 agent 代码**。
3. **看门狗在执行引擎之外**：HARNESS 的存活/死锁检测器独立于 LOOP/MAS，
   即使循环卡死也能注入中止。
4. **事实独裁安全底线**：LOOP 持有唯一裁决权且确定性；MAS 风险分只能**标注**
   （warn/recheck），**永不**把事实 `fail` 改为 `pass`；critical 事故强制 recheck。
5. **可组合中止 + 永不死锁**：每个跨引擎 await 都有超时与确定性兜底；
   中止条件可任意 OR 组合（次数/时长/失败阈值/外部信号）。
6. **优雅降级**：LLM / 目标不可用 → 规则引擎 / 记失败，循环不死等。
7. **不可变共享上下文 + 私有 agent 状态**：LOOP 独占可写，杜绝共享可变状态竞态。

## 2. 三引擎包结构（全功能）

```
stability_harness_loop_multiagent/
├── harness/                 # HARNESS ENGINEERING  (对齐 ETCLOVG: E/T/C/L/O/V/G)
│   ├── bus.py               # EventBus: publish / subscribe / request-reply / '#' 通配
│   ├── agent.py             # Agent 基类 + AgentSpec(id, role, caps, subscriptions, lifecycle)
│   ├── runtime.py           # 生命周期: registry / spawn / pause / resume / shutdown
│   ├── watchdog.py          # 存活 / 陈旧 / 死锁检测（引擎外，发 harness/abort）
│   ├── telemetry.py         # 可观测: trace / metric / structured-log / 可插拔 sink
│   ├── governance.py        # 治理: 访问控制 / 配额 / 断路器 / 预算
│   └── verify.py            # 校验: guardrails / 输入验证 / 评估钩子
├── loop/                    # LOOP ENGINEERING  (对齐 MAPE-K / OODA / 控制论)
│   ├── driver.py            # 控制环: sense → plan → act → check → decide → halt
│   ├── termination.py       # 可组合中止: count / duration / fail-threshold / external / signal
│   ├── scheduler.py         # 间隔 / 退避 / 抖动 / 自适应节奏
│   ├── decision.py          # 决策权: 事实独裁 + 风险标注 + critical → recheck
│   └── context.py           # 共享上下文: LOOP 独占可写 + 不可变快照 + agent 私有状态
└── multi_agent/                     # MULTI-AGENT SYSTEM  (角色: worker / advisor / observer)
    ├── adapter.py           # TargetAdapter 契约（被作用对象；现在定义，实现延迟）
    ├── workers/             # 执行型智能体: 经 Adapter 执行领域操作
    ├── advisors/            # 监控 / 分析型: 投票 + 主动 raise（仅建议，自有主动循环）
    ├── observers/           # 报告 / 通知型: 消费事件，不参与决策
    └── protocols.py         # 智能体交互契约（vote / incident / round 话题）
```

依赖方向：`harness →(bus)← loop →(bus)← multi_agent`，**三引擎互不 import**。
EventBus 是唯一接缝（无需独立 `contracts/` 包）。

## 3. 通用事件总线话题（与领域无关）

| 域 | 话题 |
|----|------|
| loop | `loop/tick` `loop/done` `loop/abort` `loop/recheck` `loop/vote/request` |
| agent | `agent/<role>/done` `agent/vote/reply` `agent/incident` `agent/incident/ack` |
| harness | `harness/abort` `harness/liveness/*` `harness/metric/*` |
| target | `target/acted` `target/recovered` `target/checked` |

所有跨引擎通信只经上述话题（或新增同风格话题），禁止直接方法调用。

## 4. 核心契约（接口骨架，泛型、无实现）

```python
# harness/agent.py
class AgentSpec:            # 注册元数据，引擎中立
    id: str; role: str; capabilities: set[str]; subscriptions: list[str]; lifecycle_hooks: ...

# multi_agent/adapter.py  — 被作用对象（场景实例化时实现）
class TargetAdapter(Protocol):
    def act(self, operation) -> Result          # 对目标执行一个操作
    def observe(self) -> State                  # 观测目标当前状态
    def events(self, since: float) -> list[Event]  # 取回某时刻以来的事件

# loop/termination.py
class StopCondition(Protocol):
    def evaluate(self, ctx) -> tuple[bool, str]  # (是否中止, 原因)
class TerminationPolicy:                         # OR 组合，优先级可配
    conditions: list[StopCondition]
    def should_halt(self, ctx) -> tuple[bool, str]: ...

# loop/decision.py
class DecisionAuthority(Protocol):
    def decide(self, facts, risk_score, critical: bool) -> Verdict
    # Verdict ∈ {pass, warn, recheck, fail, abort}; 事实独裁，risk 仅标注

# multi_agent/protocols.py
class AdvisorContract(Protocol):                 # 仅建议
    def on_round(self, round) -> None           # 订阅 loop/done
    def vote(self) -> tuple[float, float]       # (risk_score, confidence)
    def raise_incident(self, severity, detail) -> None
class ObserverContract(Protocol):                # 仅观察
    def on_event(self, event) -> None           # 订阅事件，不参与决策
```

## 5. 各引擎全功能清单（v1 该有的都要）

### 5.1 HARNESS ENGINEERING — 对齐 ETCLOVG 七层
- **E Execution environment**：进程/容器/沙箱隔离；agent 步骤的运行环境。
- **T Tool interface**：标准化、沙箱化的工具/函数调用契约（MCP 就绪）。
- **C Context management**：prompt 组装、窗口化、短期/长期记忆、压缩。
- **L Lifecycle / Orchestration**：agent 注册表、spawn/stop、supervisor 循环、消息路由、终止。
- **O Observability**：tracing、metrics、structured logging、health 信号（含 liveness/heartbeat）。
- **V Verification**：guardrails、输入/输出校验、评估钩子、自校验。
- **G Governance**：访问控制、策略、成本/配额、审计、断路器、预算。
- 外加：**Watchdog**（存活/陈旧/死锁检测，引擎外）、**Telemetry**（可插拔 sink）。

### 5.2 LOOP ENGINEERING — 对齐 MAPE-K / OODA / 控制论
- **驱动**：`sense → plan → act → check → decide → halt` 确定性循环核心。
- **中止**：可组合 `StopCondition`（count / duration / cumulative & consecutive fail-threshold / external signal），OR 组合、优先级可配。
- **调度**：下一间隔 = `clamp(recover_time × K + base, MIN, MAX)`；抖动重试用指数退避 + 抖动 + 预算上限。
- **决策**：事实独裁矩阵；风险分仅加 `warn`/`recheck`；critical 强制 `recheck`；投票/LLM 出错默认保守 `warn(60)` 而非乐观 `pass`。
- **恢复窗口**：act 与下轮裁决间插入恢复观测窗（轮询 + 超时，记 `t_recover`）。
- **共享上下文**：LOOP 独占可写，每次刷新不可变快照；agent 维护私有状态。

### 5.3 MULTI-AGENT SYSTEM — 角色化
- **TargetAdapter 契约**：目标对象的执行/观测/事件接口（场景实现）。
- **Workers（执行型）**：经 `TargetAdapter` 执行领域操作，发布 `target/*` 与 `agent/<role>/done`。
- **Advisors（建议型）**：订阅 `loop/done`，置信度加权投票（`risk, confidence`）；可主动 raise 事故（自有 30s/45s 主动循环）；**永不裁决**。
- **Observers（观察型）**：消费事件做记录/通知（scribe / notifier），不参与决策。
- **交互协议**：vote/incident/round 话题契约；加权投票（弃票权重=0，全弃票中性默认 50，风险≥90 快路径）。
- **强制事故回声确认**：LOOP ack 他人事故、永不 ack 自身，无告警静默丢失。

## 6. 三者结合最佳实践规则

- **仅总线通信**：单传输边界，未来可换网络传输而不改 agent 代码。
- **防死锁**：每跨引擎 await 有超时 + 确定性兜底（投票超时→中性 50；恢复超时→记失败）；
  外部死锁检测器；`max_rounds`/`max_duration` 硬上限；recheck 上限 1；风险≥90 快路径。
- **安全底线（事实独裁）**：任一事实检查失败 → 本轮 `fail`；风险分只能加 `warn`/`recheck`，
  **永不** `fail→pass`；critical 事故强制 recheck。
- **强制事故回声确认**：协调者 ack 他人事故、永不 ack 自己。
- **优雅降级**：LLM/目标不可用 → 规则引擎/记失败，循环不死等。
- **不可变共享上下文 + 私有 agent 状态**：杜绝共享可变状态竞态。
- **职责分明 > 功能多**：健康(harness) / 迭代+裁决(loop) / 执行+建议(multi_agent) 各一责；
  趋势检测与投票放进 MAS 建议者，LOOP 保持薄确定性协调器。

## 7. 扩展点（每扩展只改一个引擎，另两个零改动）

| 扩展 | 仅改 | 做法 |
|------|------|------|
| 新目标/资源类型 | MAS | 实现 `TargetAdapter`（如 `DeviceAdapter`），注册 worker |
| 新领域操作 | MAS | 新增 worker，订阅 `loop/tick`/`target/*` |
| 新断言/校验 | MAS | worker 内实现检查逻辑，发布 `target/checked` 事实 |
| 新建议型智能体 | MAS | 实现 `AdvisorContract`，走固定 vote/incident 话题（加权汇总支持 N 个） |
| 新报告/通知通道 | MAS | 实现 `ObserverContract`，订阅事件 |
| 新中止条件 | LOOP | 加 `StopCondition` 组合进 `TerminationPolicy` |
| 新调度策略 | LOOP | 改 `scheduler.py` |
| 新遥测 sink / 探针 | HARNESS | 改 `telemetry.py` / `watchdog.py` |

## 8. 实例化（场景在其上剪裁，不碰其他引擎）

具体场景 = **薄一层定制**：
- 实现 `TargetAdapter` 的具体子类（如某设备/服务/资源的适配器）。
- 在 `multi_agent/workers`、`multi_agent/advisors`、`multi_agent/observers` 加对应角色。
- 在 `loop/config` 配中止条件与间隔；在 `loop/decision` 配事实检查项。
- HARNESS / LOOP 引擎代码**零改动**。

> 本模板不绑定任何具体场景；换场景只是换 `multi_agent/` 内的适配器与角色实现。

## 9. 命名对照表（范式术语 → 模块）

| 范式术语 | 模块 |
|----------|------|
| 执行环境 / 工具接口 / 上下文 | `harness/agent.py` `harness/runtime.py` |
| 生命周期 / 编排 | `harness/runtime.py` |
| 可观测 | `harness/telemetry.py` |
| 校验 / 治理 | `harness/verify.py` `harness/governance.py` |
| 看门狗 | `harness/watchdog.py` |
| 控制环 / 调度 / 中止 / 决策 | `loop/driver.py` `loop/scheduler.py` `loop/termination.py` `loop/decision.py` |
| 共享上下文 | `loop/context.py` |
| 目标适配器 | `multi_agent/adapter.py` |
| 执行 / 建议 / 观察 角色 | `multi_agent/workers/` `multi_agent/advisors/` `multi_agent/observers/` |
| 交互协议 | `multi_agent/protocols.py` |

## 10. 参考（业界框架 / 模式）

- **Agent Harness Engineering: A Survey**（2026, CMU/Yale/JHU/Virginia Tech/Amazon）—
  ETCLOVG 七层运行时分类法。
- **MAPE-K**（IBM 自治计算）— Monitor/Analyze/Plan/Execute + Knowledge 控制环。
- **OODA Loop**（Boyd）— 快速迭代决策；**PDCA**（Shewhart/Deming）— 稳定环境验证环。
- **Sense-Plan-Act / 闭环负反馈** — 控制论控制环骨架。
- **Exponential backoff + jitter + retry budget**（AWS REL05-BP03）— 弹性重试。
- **Circuit breaker + retry budget** — 失败爆炸半径约束。
- **OpenClaw Gateway+Pi** — 引擎外死锁检测器（Pi）模式。
- **Voting-or-Consensus 研究（Zhou et al. 2024）** — 置信度加权投票、弃票=0、中性默认 50、快路径 ≥90。
- 框架参照：Microsoft Agent Framework、AutoGen、LangGraph、CrewAI、OpenAI Agents SDK、
  Google ADK、AgentScope、MASFT（多智能体框架分类）。
- **Separation of Concerns** — 驱动与领域解耦。

> 本模板为 v1 全功能基线；后续按具体场景剪裁，并可持续吸纳新框架实践。
