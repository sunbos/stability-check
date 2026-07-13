# 自治型多智能体架构改造设计文档

> Date: 2026-07-13
> Status: Draft (待用户 review)
> Branch: `feat/autonomous-multiagent` (待创建)
> Supersedes (partially): `2026-07-13-burnin-multiagent-design.md` (现有编排型架构)

## 1. Overview

将当前的"编排型多智能体（orchestrator-pattern MAS）"改造为"自治型多智能体
（autonomous MAS）+ 分层投票"架构。保留确定性 Loop Core 保证可复现性，新增自治
层赋予 agent 主动发现、主动提案、风险评估投票能力。

### 改造动机

当前架构虽满足 MAS 形式定义，但存在 5 处"伪多智能体"嫌疑：
1. 共享可变状态过重（RunContext 单一对象，所有 agent 直接读写）
2. Coordinator 主驱动过强（其他 agent 生命周期依附）
3. 决策权高度集中（无协商/投票）
4. Agent 并发性有限（同刻通常仅 1-2 个 agent 在工作）
5. LLM 是 advisor 而非 peer（无主动权）

自治型架构在"主动发现"和"异常处理"上有本质优势：agent 可主动 raise incident、
主动评估风险，不再被动等待 Coordinator 询问。

## 2. Goals / Non-Goals

### Goals
- 引入自治层（TrendSupervisor + RiskAnalyst），赋予主动 raise incident 权
- 引入风险评估投票机制（限定领域：风险分 0-100）
- 拆分 RunContext → ReadOnlyContext + CoordinatorContext + Agent 私有状态
- 保留确定性 Core（事实层 pass/fail 不被投票推翻）
- 删除 ReporterAgent（职责拆分到 TrendSupervisor + Notifier）
- 满足行业对"真多智能体"的定义（自治性、对等性、消息通信）

### Non-Goals
- 不引入 agent 否决权（自治层不能推翻事实层判定）
- 不引入全员投票（避免噪音污染）
- 不做分布式部署（仍 in-process，但为未来留口子）
- 不引入 LLM 主动决策（LLM 仅评估风险，不直接决定 continue/abort）
- 不重构 EventBus（已支持所需通信模式）

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4 — Output（Reactive，无投票权）                          │
│   Scribe          时间线 + 叙事记录（纯记录，不再有评估权）      │
│   Notifier        通知通道（print + webhook）                    │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3 — Autonomous（Proactive，有提案权 + 风险评分权）         │
│   TrendSupervisor 确定性趋势监督（recover_time/fail_rate 趋势） │
│                   无 LLM，纯规则，主动发现"温水煮青蛙"型异常     │
│   RiskAnalyst     LLM 风险分析师（每轮主动评估风险分 + 主动 raise）│
├─────────────────────────────────────────────────────────────────┤
│ Layer 2 — Arbiter（独裁决策，综合规则 + 风险分）                 │
│   Coordinator     主驱动 + 最终决策（continue/abort/recheck）    │
│                   综合规则阈值 + 自治层风险分                    │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1 — Executor（Reactive，事实判定，无投票权）               │
│   RebootAgent     执行 reboot                                    │
│   WatchAgent      监视恢复                                       │
│   EventCheckAgent 事件核对（事实：found=True/False）             │
│   StatusCheckAgent 状态核对（事实：changed=True/False）          │
└─────────────────────────────────────────────────────────────────┘
```

### 关键变化

| 变化 | 原设计 | 新设计 | 理由 |
|---|---|---|---|
| 删除 | ReporterAgent | 职责拆分到 TrendSupervisor（统计）+ Notifier（报警） | 职责重叠 |
| 新增 | — | TrendSupervisor | 专门的趋势监督，纯规则无 LLM |
| 升级 | AnalystAgent（事故时被咨询） | RiskAnalyst（每轮主动评估 + 主动 raise） | 自治化 |
| 降级 | ScribeAgent（有评估权） | Scribe（纯记录） | 职责单一化 |
| 拆分 | RunContext 单一共享 | ReadOnlyContext + CoordinatorContext + 私有状态 | 真正的 MAS |

## 4. Agent Inventory

| Agent | Layer | Role | Subscribes | Publishes | Device calls | Vote权 |
|-------|-------|------|------------|-----------|--------------|--------|
| RebootAgent | L1 | executor | `coord/reboot`, `coord/recheck` (skip) | `reboot/done` | `reboot()` | no |
| WatchAgent | L1 | watcher | `reboot/done` | `device/recovered` | `get_work_status()` | no |
| EventCheckAgent | L1 | checker | `device/recovered`, `coord/recheck` | `check/event` | `get_reboot_events()` | no |
| StatusCheckAgent | L1 | checker | `device/recovered`, `coord/recheck` | `check/status` | `get_work_status()` | no |
| Coordinator | L2 | arbiter | `reboot/done`, `device/recovered`, `check/event`, `check/status`, `incident/raise`, `vote/reply`, `coord/abort` | `coord/reboot`, `coord/recheck`, `round/done`, `incident/ack`, `vote/request`, `incident/raise`, `coord/abort`, `scribe/summary/request`, `context/state` | none | no (arbiter) |
| TrendSupervisor | L3 | autonomous | `round/done`, `coord/abort` | `incident/raise`, `vote/reply` | none | **yes** |
| RiskAnalyst | L3 | autonomous | `round/done`, `vote/request`, `incident/raise`, `coord/abort` | `incident/raise`, `vote/reply`, `analyst/report` | none | **yes** |
| Scribe | L4 | output | `round/done`, `incident/raise`, `incident/ack`, `analyst/report`, `coord/abort`, `scribe/summary/request` | `scribe/summary` | none | no |
| Notifier | L4 | output | `coord/abort`, `incident/raise`, `incident/ack`, `analyst/report`, `notify` | none | none | no |

## 5. Communication Protocol

### 5.1 完整 Topic 流图

```
─── 主循环（每轮）───────────────────────────────────────────────────
Coordinator ──coord/reboot──> RebootAgent
RebootAgent  ──reboot/done──> WatchAgent, Coordinator
WatchAgent   ──device/recovered──> EventCheckAgent, StatusCheckAgent, Coordinator
EventCheck   ──check/event──>  Coordinator            (事实: found=True/False)
StatusCheck  ──check/status──> Coordinator            (事实: changed=True/False)

─── 风险评估投票（每轮，Coordinator 收齐事实后）────────────────────
Coordinator ──vote/request──> (自治层广播)
               TrendSupervisor ──vote/reply──> Coordinator  (规则风险分)
               RiskAnalyst     ──vote/reply──> Coordinator  (LLM 风险分)

─── 决策与广播 ────────────────────────────────────────────────────
Coordinator 内部: 综合事实 + 加权风险分 → decision(pass/fail/warn/recheck)
Coordinator ──round/done──> Scribe, Notifier, TrendSupervisor, RiskAnalyst
Coordinator ──context/state──> (广播状态快照)

─── 自治层主动发起（任何时候）──────────────────────────────────────
TrendSupervisor ──incident/raise──> Coordinator, Scribe, Notifier
RiskAnalyst     ──incident/raise──> Coordinator, Scribe, Notifier
Coordinator     ──incident/ack───> (广播 ack: accepted/rejected/logged)

─── 复检（recheck 决策时）──────────────────────────────────────────
Coordinator ──coord/recheck──> EventCheckAgent, StatusCheckAgent
               (复检只重跑 check，不重 reboot)
EventCheck   ──check/event──>  Coordinator
StatusCheck  ──check/status──> Coordinator

─── 中止 / 结束 ───────────────────────────────────────────────────
Coordinator ──coord/abort──> Scribe, Notifier, TrendSupervisor, RiskAnalyst
Coordinator ──scribe/summary/request──> Scribe
Scribe       ──scribe/summary──> Coordinator
```

### 5.2 Incident 机制

**触发条件**（任何 agent 满足任一即可主动 raise）：

| Agent | 触发条件 | 默认 severity |
|---|---|---|
| TrendSupervisor | recover_time 连续 3 轮递增 | warn |
| TrendSupervisor | fail_rate 滑动窗口（10 轮）> 30% | warn |
| TrendSupervisor | recover_time 单轮 > 2× 历史均值 | warn |
| TrendSupervisor | recover_time 连续 5 轮递增 | critical |
| RiskAnalyst | LLM 判断有异常模式 | warn-critical（LLM 自定） |
| RiskAnalyst | 风险分 > 80 且连续 3 轮 | critical |
| EventCheck / StatusCheck | 事实异常（可选，默认不 raise） | info |

**Incident 消息格式**：
```python
{
    'incident_id': 'inc-001',             # 唯一 ID（uuid4 前 8 位）
    'raised_by': 'trend_supervisor',      # 发起者
    'severity': 'warn',                   # info / warn / critical
    'category': 'trend_degradation',      # 分类
    'description': 'recover_time 连续 3 轮递增',
    'evidence': {'trend': [60, 62, 65, 70], 'window': 4},
    'suggestion': 'recheck',              # continue / recheck / abort
    'timestamp': 1719500000.0
}
```

**Coordinator ack 规则**（强制回声）：
```python
{
    'incident_id': 'inc-001',
    'decision': 'accepted',               # accepted / rejected / logged
    'reason': '采纳，触发复检',
    'action_taken': 'coord/recheck',      # 实际采取的动作：none / coord/recheck / coord/abort
    'timestamp': 1719500001.0
}
```

**ack 决策规则**（确定性，不调用 LLM）：
- severity=critical → 强制 recheck（accepted）
- severity=warn + 当前风险分 > 60 → recheck（accepted）
- severity=warn + 当前风险分 ≤ 60 → logged（仅记录）
- severity=info → logged（仅记录）
- 同类 incident 连续 raise 3 次未被采纳 → 升级 severity 至 critical

**关键约束**：
- Coordinator **必须** ack，不允许静默（自治性的核心保证）
- Coordinator **不 ack 自己 raise 的 incident**（仅 ack 其他 agent raise 的）
- Coordinator 自己 raise incident 的场景：事故发生时（如连续失败、设备未恢复），广播给 Scribe/Notifier 记录
- ack 决策由确定性规则 + 当前风险分综合决定，不调用 LLM（避免循环）
- rejected 时 reason 必填（供 Scribe 记录，便于事后复盘）
- 同一 incident_id 不重复处理（去重）

### 5.3 Vote 机制

**投票时机**：Coordinator 收齐 `check/event` + `check/status` 后，发起 `vote/request`

**Vote request 格式**：
```python
{
    'round': N,
    'facts': {
        'found': True,
        'changed': False,
        't_recover': 61.1,
        'consecutive_failures': 0
    },
    'history_summary': {                  # 只读快照
        'last_5_recover_times': [60.1, 60.5, 61.0, 60.8, 61.1],
        'last_5_results': ['pass']*5,
        'total_passes': 4,
        'total_fails': 0
    },
    'question': 'rate_risk_0_100',        # 固定问题类型
    'timeout_sec': 5.0
}
```

**Vote reply 格式**：
```python
{
    'voter': 'trend_supervisor',
    'risk_score': 35,                     # 0-100 整数
    'rationale': 'recover_time 稳定，无趋势异常',
    'confidence': 0.8,                    # 0-1 置信度
    'method': 'rule'                      # rule / llm / abstain
}
```

**权重综合**（Coordinator 内部）：
```python
WEIGHTS = {
    'trend_supervisor': 0.5,              # 确定性规则
    'risk_analyst': 0.5,                  # LLM
}

# 按 confidence 调整权重
total_weight = sum(WEIGHTS[a] * reply.confidence for a in replies)
combined_risk = (
    sum(replies[a].risk_score * WEIGHTS[a] * replies[a].confidence
        for a in replies) / total_weight
    if total_weight > 0
    else 50                               # 默认中性
)
```

**关键约束**：
- 投票超时（5s）默认该 agent 弃权（risk_score=50, confidence=0, method='abstain'）
- LLM 不可用时 RiskAnalyst 弃权，TrendSupervisor 独立工作
- 投票结果**不直接决策**，只作为 Coordinator 决策的输入信号之一
- 风险分阈值可配置：`BURNIN_RISK_RECHECK_THRESHOLD=80`、`BURNIN_RISK_WARN_THRESHOLD=60`

### 5.4 决策矩阵

| 事实层 | 风险分 | Incident | 决策 | 备注 |
|---|---|---|---|---|
| found=False 或 changed=True | 任意 | 任意 | **fail** | 事实层独裁，不被投票推翻 |
| found=True 且 changed=False | < 60 | 无 / info / warn | **pass** | 低风险直接通过 |
| found=True 且 changed=False | 60-80 | 无 / info / warn | **warn** | 记录警告，仍算 pass |
| found=True 且 changed=False | > 80 | 任意 | **recheck** | 触发复检（不重 reboot） |
| found=True 且 changed=False | 任意 | critical | **recheck** | critical incident 强制复检 |

**安全底线**：
- 事实层 100% 确定性（pass/fail 由 EventCheck/StatusCheck 的事实判定）
- 风险分只影响"pass 之后是否复检/warn"，**不能把 fail 改成 pass**
- critical incident 可强制复检（自治层对 Core 的唯一"硬影响"）

## 6. Shared State Refactor

### 6.1 当前问题

`RunContext` 是单一共享对象，所有 agent 直接读写同一引用：
- `StatusCheckAgent` 直接读 `self.ctx.baseline`
- `ScribeAgent` 直接读 `self.ctx.round_history`、`self.ctx.aborted`
- `Coordinator` 直接写 `self.ctx.round_history`、`self.ctx.aborted`

这是"共享白板"模式，不是真正的 MAS 通信。

### 6.2 新分层结构

```python
# tests/harness/context.py（重构后）

class ReadOnlyContext:
    """所有 agent 持有的只读视图。Coordinator 持有可写子类。

    设计原则：
    - baseline 启动时一次性注入（只读，不变）
    - round_history_snapshot 由 Coordinator 每轮广播后更新（只读快照）
    - 任何 agent 想写入权威状态必须通过总线消息
    """
    baseline: Baseline | None              # 启动时注入，只读
    strategy_text: str                     # 启动时注入，只读
    round_history_snapshot: tuple          # 不可变快照，Coordinator 每轮广播后替换
    aborted: bool                          # 只读，由 Coordinator 通过 coord/abort 广播

    def latest_round(self) -> RoundResult | None:
        """获取最近一轮结果（只读）。"""
        return self.round_history_snapshot[-1] if self.round_history_snapshot else None

    def history(self, last_n: int = 0) -> tuple:
        """获取历史快照（只读）。last_n=0 表示全部。"""
        if last_n == 0:
            return self.round_history_snapshot
        return self.round_history_snapshot[-last_n:]


class CoordinatorContext(ReadOnlyContext):
    """Coordinator 专有的可写上下文。其他 agent 不应持有此类型。"""
    _round_history: list                   # 内部可写列表
    _consecutive_failures: int
    _total_failures: int
    _consecutive_reboots: int

    def append_round(self, result: RoundResult) -> None:
        """Coordinator 专用：追加轮次结果并刷新快照。"""
        self._round_history.append(result)
        self.round_history_snapshot = tuple(self._round_history)
        self._update_counters(result)

    def publish_state(self, bus: EventBus) -> None:
        """每轮结束后广播状态快照（供其他 agent 更新本地视图）。"""
        bus.publish('context/state', {
            'round_history_snapshot': self.round_history_snapshot,
            'aborted': self.aborted,
            'counters': {
                'consecutive_failures': self._consecutive_failures,
                'total_failures': self._total_failures,
            }
        })
```

### 6.3 Agent 私有状态

```python
# TrendSupervisor 私有状态
class TrendSupervisorState:
    recover_time_window: deque[float]       # 滑动窗口（最近 N 轮 recover_time）
    fail_rate_window: deque[bool]           # 滑动窗口（最近 N 轮 pass/fail）
    baseline_recover_time: float | None     # 启动时基线

# RiskAnalyst 私有状态
class RiskAnalystState:
    recent_rounds: deque[RoundResult]       # 最近 N 轮（用于 LLM 上下文）
    last_risk_score: int                    # 上一轮风险分

# Scribe 私有状态
class ScribeState:
    timeline: list[TimelineEntry]           # 时间线
    incidents: list[IncidentRecord]         # 事故记录
    acks: list[AckRecord]                   # Coordinator ack 记录
```

### 6.4 跨 Agent 数据获取协议

| 数据 | 获取方式 | 备注 |
|---|---|---|
| baseline | 启动注入 ReadOnlyContext | 只读不可变 |
| round_history | 订阅 `round/done` 累积 / 读 `ctx.history()` | Coordinator 唯一写入者 |
| 当前轮事实 | 订阅 `check/event` / `check/status` | 由 EventCheck/StatusCheck 发布 |
| 风险分 | 订阅 `vote/reply`（Coordinator）/ `vote/request`（自治层） | 投票机制 |
| incident | 订阅 `incident/raise` / `incident/ack` | 任何 agent 可 raise |
| 统计计数器 | 订阅 `context/state` | 不直接读 ctx 内部字段 |
| TaskBoard | **保留共享**（Coordinator 主导） | 简化设计 |

### 6.5 关键约束

1. `ReadOnlyContext` 字段对外只读（用 `@property` 或冻结 dataclass）
2. `round_history_snapshot` 是 tuple（不可变快照，Coordinator 替换引用）
3. 自治 agent 不直接读 `ctx.round_history`，通过订阅 `round/done` 累积私有窗口
4. TaskBoard 保留共享（它是 Coordinator 工具，非 agent 间通信）
5. baseline 保留共享只读（启动后不变，无并发问题）

## 7. Degradation

### 7.1 LLM 不可用降级链

```
RiskAnalyst 启动时检查 LLM key
├── 有 key → 每轮参与投票（LLM 风险分）
└── 无 key → 弃权（vote/reply: risk=50, confidence=0, method='abstain')
            + 仍然订阅 round/done（保持观察者身份）
            + 仍然可以 raise incident（基于规则兜底）

Coordinator 收到 vote/reply 时：
├── method='llm' → 按 WEIGHTS['risk_analyst'] 加权
├── method='rule' → 按 WEIGHTS['risk_analyst'] 加权
└── method='abstain' → confidence=0 → 不计入加权（自动降权）
```

**关键保证**：
- LLM 不可用不阻塞主循环（vote 超时 5s 弃权）
- TrendSupervisor 纯规则，始终工作（不依赖 LLM）
- 即使所有自治 agent 都弃权，Coordinator 仍按事实层 + 规则阈值决策

### 7.2 Incident 风暴防护

```python
class IncidentRateLimiter:
    """每个 agent 单位时间内 raise incident 的限制"""
    max_per_minute: int = 5
    max_consecutive_same_category: int = 3  # 同类 incident 连续 3 次后升级 severity

    def should_throttle(self, agent_id: str, category: str) -> bool:
        """是否应该限流"""
```

### 7.3 Vote 异常处理

| 异常 | 处理 |
|---|---|
| 投票超时（5s） | 该 agent 弃权（risk=50, confidence=0） |
| RiskAnalyst 无 key | 弃权（method='abstain'） |
| TrendSupervisor 窗口不足 | 弃权（method='abstain', reason='warmup'） |
| LLM 返回格式错误 | RiskAnalyst 弃权 + 记录警告 |
| 所有 agent 弃权 | combined_risk=50（中性），Coordinator 仅按事实层决策 |

## 8. Testing Strategy

### 8.1 可复现性分层

| 层 | 可复现性 | 测试方式 |
|---|---|---|
| 事实层（EventCheck/StatusCheck） | 100% 确定性 | 真实设备 + 固定输入 |
| Coordinator 决策（pass/fail/warn） | 100% 确定性 | 单元测试 + 真实设备 |
| Coordinator 决策（recheck 触发） | 依赖风险分 | 阈值边界测试 |
| TrendSupervisor 风险分 | 100% 确定性 | 单元测试 |
| RiskAnalyst 风险分 | 非确定性（LLM） | 真实 LLM + 区间断言 |
| Incident raise | 确定性触发条件 | 单元测试 |
| Incident ack | 100% 确定性 | 单元测试 |

**关键原则**（遵循 Loop Engineering）：
- 事实层 + 规则层 100% 可复现 → 可单元测试
- LLM 层非确定性 → 用"区间断言"（如 `assert risk_score in [0, 100]`）+ 真实 LLM 调用
- **不 mock LLM**，无 key 时 `pytest.skip`

### 8.2 测试用例清单

```python
# ── 策略层测试（无需设备，无 LLM）──────────────────────────────
def test_trend_supervisor_detects_recover_time_increasing()
def test_trend_supervisor_detects_fail_rate_spike()
def test_trend_supervisor_stable_no_incident()
def test_coordinator_ack_incident_accepted()
def test_coordinator_ack_incident_rejected()
def test_coordinator_ack_mandatory()
def test_vote_timeout_defaults_to_neutral()
def test_risk_analyst_unavailable_degrades()
def test_decision_matrix_fail_not_overridden_by_vote()
def test_decision_matrix_recheck_on_high_risk()

# ── 集成测试（真实设备 + 真实 LLM）────────────────────────────
def test_burnin_session()
def test_burnin_session_with_simulated_trend_degradation()

# ── LLM 集成测试（无设备，需 LLM key）─────────────────────────
def test_risk_analyst_real_llm_call()
def test_vote_request_real_llm()
```

### 8.3 测试基础设施

```python
# tests/harness/testing.py（新增）
class HarnessFixture:
    """轻量测试夹具：装配最小 agent 子集，无需真实设备"""
    def build_minimal_system(self, agents: list[str]) -> tuple[EventBus, ReadOnlyContext, list[Agent]]
    def inject_round_history(self, ctx: CoordinatorContext, rounds: list[dict]) -> None
    def capture_incidents(self, bus: EventBus) -> list[dict]
    def simulate_vote_reply(self, bus: EventBus, voter: str, score: int, confidence: float = 1.0)
```

## 9. Implementation Phases

按 Loop Engineering 方法论，分 7 个阶段，每阶段独立可验证。在新分支
`feat/autonomous-multiagent` 上做。

### Phase 1：基础设施重构（无功能变化）
- 重写 `context.py`：`RunContext` → `ReadOnlyContext` + `CoordinatorContext`
- 更新 `agent.py`：`__init__` 接受 `ReadOnlyContext`
- 更新 `coordinator.py`：持有 `CoordinatorContext`，新增 `publish_state`
- **验证**：现有测试全部通过（行为不变）

### Phase 2：执行层解耦
- 更新 reboot/watch/event_check/status_check agent：移除任何写 ctx
- **验证**：`test_burnin_session` 仍通过（行为不变）

### Phase 3：输出层 + 删除 Reporter
- 更新 `scribe_agent.py`：改为私有 timeline 累积
- 删除 `reporter_agent.py`
- 更新 `notifier_agent.py`：吸收 Reporter 的报警职责
- 更新 `loader.py`：新装配清单
- **验证**：`test_burnin_session` 通过（行为基本不变）

### Phase 4：新增 TrendSupervisor
- 新建 `trend_supervisor_agent.py`
- 维护私有 `trend_window`
- 订阅 `round/done`，实现趋势检测规则
- 主动 `publish('incident/raise', ...)`
- **验证**：新增单元测试（`test_trend_supervisor_*`）

### Phase 5：升级 RiskAnalyst
- 重构 `analyst_agent.py` → `risk_analyst_agent.py`
- 维护私有 `recent_rounds`
- 订阅 `round/done`，每轮主动评估
- 实现 `vote/reply` 响应
- 主动 `publish('incident/raise', ...)`
- **验证**：新增单元测试 + 真实 LLM 集成测试

### Phase 6：Coordinator 决策矩阵升级
- 在 `coordinator.py` 中实现：
  - `vote/request` 发起
  - 加权风险分综合
  - 决策矩阵（pass/fail/warn/recheck）
  - `incident/raise` 处理 + 强制 `incident/ack`
  - `coord/recheck` 复检流程
- **验证**：新增决策矩阵单元测试

### Phase 7：端到端集成
- 更新 `test_burnin.py`：完整 `test_burnin_session`（含投票 + incident 验证）
- 更新 `AGENTS.md`：同步新架构
- 更新 `docs/plans/`：新增设计文档
- **验证**：真实设备 + 真实 LLM 端到端测试通过

## 10. Risks & Mitigations

| 风险 | 应对 |
|---|---|
| Phase 1 重构破坏现有行为 | 现有测试是安全网，必须全绿才进入 Phase 2 |
| LLM 投票引入不稳定 | 5s 超时弃权 + 区间断言 |
| Incident 风暴 | RateLimiter + 同类升级机制 |
| TrendSupervisor 误报 | 趋势检测规则保守（连续 3 轮而非 2 轮） |
| 端到端测试耗时（~5min/轮） | Phase 1-6 用单元测试，Phase 7 才跑端到端 |
| 决策矩阵边界模糊 | 用单元测试覆盖所有事实×风险×incident 组合 |

## 11. Open Questions

无未决问题。所有关键决策已在前述章节明确。

## 12. Success Criteria

- 所有 7 个 Phase 的验证全部通过
- 现有 `test_burnin_session` 在新架构下仍能跑通真实设备
- 新增的自治层测试全部通过（含真实 LLM 调用）
- AGENTS.md 同步更新
- 设计文档与实现一致
- 满足行业对"真多智能体"的定义：自治性、对等性、消息通信、角色分工

## 13. References

- 现有架构设计：`docs/plans/2026-07-13-burnin-multiagent-design.md`
- AGENTS.md：`AGENTS.md`
- Loop Engineering 方法论：用户 memory（2026-07-06 Phase 7 固化）
- 行业参考：LlamaIndex Supervisor-Worker、AutoGen GroupChat、CrewAI Manager-Crew
