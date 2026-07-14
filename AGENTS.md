# AGENTS.md — 燃烧稳定性测试框架

> 基于 pytest 的、事件总线驱动的多智能体框架，用于海康威视门禁设备
> （ISAPI + Digest 认证）的燃烧稳定性测试。

## 1. 项目概述

本项目反复重启一台海康威视门禁设备（`192.168.3.33`，ISAPI + Digest
`admin / 121212..`），并在每次重启后验证两个不变量：

1. 远程重启事件（`AcsEvent`，`major=3, minor=123`）已被记录。
2. 设备工作状态（`AcsWorkStatus`）回到基线快照。

一次运行是一个 pytest 会话。一组智能体通过进程内异步事件总线协作。
确定性循环核心（`Coordinator`）驱动主循环；自治策略层
（`AnalystAgent` / `RiskAnalyst` + `TrendSupervisorAgent`）主动监控趋势、
对风险投票、并在异常时 raise 事故——当 LLM 不可用时优雅降级到规则引擎。
Coordinator 应用决策矩阵，结合事实层独裁与风险分修正，确保拷机永不死锁、
安全底线不被突破。

## 2. 架构（自治 4 层 + 总线驱动）

### 2.1 分层架构图

```
┌──────────────────────────────────────────────────────────────────┐
│                        pytest 测试框架                            │
│              (conftest + 薄测试入口；仅负责驱动)                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              EventBus 事件总线（唯一通信通道）                │ │
│  │          pub/sub 发布订阅 + req/resp 请求响应                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                              ▲ ▼                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  L1 执行层（确定性；复用 DeviceClient）                       │ │
│  │                                                              │ │
│  │  RebootAgent ──→ WatchAgent ──→ EventCheckAgent             │ │
│  │                                   StatusCheckAgent          │ │
│  │  职责：执行重启 / 监控恢复 / 检查事件 / 检查状态              │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                              ▲ ▼                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  L2 仲裁层（确定性循环核心）                                  │ │
│  │                                                              │ │
│  │  Coordinator（协调者）                                        │ │
│  │  职责：重启→恢复→检查→投票→决策→中止                         │ │
│  │  权限：唯一决策者（pass/fail/recheck）                        │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                              ▲ ▼                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  L3 自治层（主动监控；LLM 优先 + 规则兜底）                   │ │
│  │                                                              │ │
│  │  TrendSupervisorAgent（趋势监督）                             │ │
│  │    - 规则趋势检测（递增/失败率/尖峰）                          │ │
│  │    - 主动 raise 事故 + 投票                                   │ │
│  │                                                              │ │
│  │  AnalystAgent / RiskAnalyst（风险分析）                       │ │
│  │    - LLM 风险投票 + 主动告警                                  │ │
│  │    - 事故 advise（断电/未恢复时）                              │ │
│  │  权限：仅建议，不决定 pass/fail                               │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                              ▲ ▼                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  L4 输出层（可观测性）                                        │ │
│  │                                                              │ │
│  │  ScribeAgent（记录员）                                        │ │
│  │    - 私有时间线 + 汇总（含决策分布/风险分）                    │ │
│  │                                                              │ │
│  │  NotifierAgent（通知员）                                      │ │
│  │    - 可插拔通道：print + webhook 钩子                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 每轮主题流图

```
Coordinator ──coord/reboot──→ RebootAgent
RebootAgent ──reboot/done───→ WatchAgent, Coordinator
WatchAgent  ──device/recovered──→ EventCheckAgent, StatusCheckAgent, Coordinator
EventCheckAgent ──check/event──→ Coordinator
StatusCheckAgent ──check/status──→ Coordinator

  [仅 clean pass 时]
  Coordinator ──vote/request──→ TrendSupervisorAgent, AnalystAgent
  TrendSupervisorAgent ──vote/reply──→ Coordinator
  AnalystAgent ──vote/reply──→ Coordinator
  Coordinator ──[决策矩阵]──→ 决策=pass/warn/recheck/fail

  Coordinator ──round/done──→ ScribeAgent, TrendSupervisorAgent, AnalystAgent

  [L3 主动 raise]
  TrendSupervisorAgent ──incident/raise──→ Coordinator, ScribeAgent, NotifierAgent
  AnalystAgent ──incident/raise──→ Coordinator, ScribeAgent, NotifierAgent
  Coordinator ──incident/ack──→ (确认非自身事故)

  [断电/未恢复时]
  Coordinator ──incident/raise──→ ScribeAgent, NotifierAgent
  Coordinator ──analyst/advise──→ AnalystAgent ──analyst/advise/reply──→ Coordinator

  [中止时]
  Coordinator ──coord/abort──→ 所有监听者（Scribe / Notifier / Trend / Analyst）
```

### 2.3 核心原则

确定性循环核心（L2，可复现、低维护）与自治策略层（L3）严格分离。L3 智能体
主动监控趋势、对风险投票、并 raise 事故——但决策矩阵确保 LLM/风险分**永远不能**
将 fail 改成 pass（安全底线）。当 LLM 不可用（无 key / 限流 / 超时）时，规则引擎
接管，拷机永不死锁。

## 3. 目录结构

```
.
├── AGENTS.md                       # 本文件
├── docs/plans/
│   ├── 2026-07-13-burnin-multiagent-design.md        # 原始设计文档
│   └── 2026-07-13-autonomous-multiagent-design.md    # 自治多智能体重设计
└── tests/
    ├── conftest.py                 # 读取 os.environ → RunConfig + Baseline fixture
    ├── test_burnin.py              # 主会话 + 策略层降级测试
    ├── test_context.py             # ReadOnlyContext + CoordinatorContext 单元测试
    ├── test_scribe.py              # ScribeAgent 私有时间线 + 汇总测试
    ├── test_trend_supervisor.py    # TrendSupervisorAgent 趋势检测 + 投票测试
    ├── test_risk_analyst.py        # RiskAnalyst 投票 + 主动事故测试
    ├── test_coordinator_decisions.py # Coordinator 决策矩阵单元测试
    ├── test_integration.py         # 端到端集成测试（4 个场景）
    ├── agents/                     # 早期实现（大部分被 harness/ 取代）
    │   ├── config.py               # RunConfig / RoundResult / Baseline
    │   ├── device_client.py        # Digest + 3 个 ISAPI 调用（重启/AcsEvent/AcsWorkStatus）
    │   ├── strategy.py             # 解析 BURNIN_STRATEGY
    │   ├── report.py               # Reporter（汇总统计+告警）— 仍被 harness 复用
    │   ├── supervisor.py           # 早期单进程循环（已被 harness/coordinator 取代）
    │   ├── reboot_agent.py         # 早期（已取代）
    │   ├── event_check_agent.py    # 早期（已取代）
    │   └── status_check_agent.py   # 早期（已取代）
    └── harness/                    # 当前自治多智能体框架
        ├── bus.py                  # 异步 EventBus（pub/sub + req/resp + '#' 通配符）
        ├── agent.py                # Agent 基类 + AgentSpec
        ├── context.py              # ReadOnlyContext + CoordinatorContext + TaskBoard
        ├── llm_client.py           # OpenAI 兼容聊天客户端（标准库；默认 OpenRouter）
        ├── loader.py               # build_system(cfg) → (bus, ctx, agents)
        ├── coordinator.py          # L2 仲裁者：循环核心 + 决策矩阵 + 事故确认
        ├── reboot_agent.py         # L1 执行者：执行重启
        ├── watch_agent.py          # L1 执行者：监控 DOWN→UP 恢复周期
        ├── event_check_agent.py    # L1 执行者：检查重启事件是否记录
        ├── status_check_agent.py   # L1 执行者：对比工作状态与基线
        ├── analyst_agent.py        # L3 自治：RiskAnalyst（投票+建议+主动事故）
        ├── trend_supervisor_agent.py # L3 自治：规则趋势检测 + 投票
        ├── scribe_agent.py         # L4 输出：记录员（私有时间线 + 汇总）
        └── notifier_agent.py       # L4 输出：可插拔通知通道
```

> 注：`tests/agents/` 包含早期单进程实现。当前框架位于 `tests/harness/`。
> `device_client.py`、`config.py`、`strategy.py`、`report.py` 仍被 `harness/` 复用。

## 4. 智能体 — 角色与主题契约

| 智能体 | 层 | 角色 | 订阅 | 发布 | 设备调用 | 参与判定? |
|--------|---|------|------|------|---------|----------|
| `RebootAgent` | L1 | 执行者 | `coord/reboot` | `reboot/done` | `reboot()` | 否 |
| `WatchAgent` | L1 | 监控者 | `reboot/done` | `device/recovered` | `get_work_status()` 轮询 | 否 |
| `EventCheckAgent` | L1 | 检查者 | `device/recovered` | `check/event` | `get_reboot_events()` | **是** |
| `StatusCheckAgent` | L1 | 检查者 | `device/recovered` | `check/status` | `get_work_status()` | **是** |
| `Coordinator` | L2 | 仲裁者 | `reboot/done`, `device/recovered`, `check/event`, `check/status`, `coord/abort`, `incident/raise`, `vote/reply` | `coord/reboot`, `round/done`, `incident/raise`, `coord/abort`, `analyst/advise`, `vote/request`, `incident/ack`, `coord/recheck` | 无 | **是（决策者）** |
| `TrendSupervisorAgent` | L3 | 自治 | `round/done`, `vote/request`, `coord/abort` | `incident/raise`, `vote/reply` | 无 | 否（仅建议） |
| `AnalystAgent` | L3 | 自治 | `analyst/advise`, `incident/raise`, `round/done`, `vote/request`, `coord/abort` | `analyst/advise/reply`, `analyst/decision`, `analyst/report`, `incident/raise`, `vote/reply` | 无 | 否（仅建议） |
| `ScribeAgent` | L4 | 记录员 | `round/done`, `incident/raise`, `analyst/decision`, `analyst/report`, `coord/abort`, `scribe/summary/request` | `scribe/summary` | 无 | 否 |
| `NotifierAgent` | L4 | 通知员 | `coord/abort`, `analyst/decision`, `analyst/report`, `incident/raise`, `notify` | 无 | 无 | 否 |

### 4.1 决策矩阵（设计 §5.4）

Coordinator 在 clean pass 时收集投票后应用决策矩阵：

| 事实层 | 风险分 | Critical 事故 | 决策 |
|--------|-------|--------------|------|
| `found=False` 或 `changed=True` | 任意 | 任意 | **fail**（独裁） |
| `found=True` 且 `changed=False` | < 60 | 否 | **pass** |
| `found=True` 且 `changed=False` | 60–80 | 否 | **warn** |
| `found=True` 且 `changed=False` | > 80 | 否 | **recheck** |
| `found=True` 且 `changed=False` | 任意 | 是 | **recheck** |

**安全底线**：风险分**永远不能**将 fail 改成 pass。决策矩阵是建议性的——
`passed` 标志仍纯基于事实；`decision` 和 `risk_score` 字段添加到轮次记录中供观察。

### 4.2 投票综合公式

```
综合风险 = Σ(各投票者 risk_score × 权重 × 置信度) / Σ(权重 × 置信度)

权重：TrendSupervisor = 0.5, RiskAnalyst = 0.5
置信度 = 0.0 → 弃权（不参与加权）
全部弃权 → risk = 50（中性默认）
```

## 5. 通信 — EventBus

文件：[tests/harness/bus.py](tests/harness/bus.py)

- 进程内异步总线；仅标准库（`asyncio`、`secrets`）。
- `publish(topic, message)` — 广播到所有匹配的 handler。
- `subscribe(topic, handler)` — handler 可同步或异步。
- `request(topic, message, timeout)` — 发布 + 等待 `topic/reply` 上第一个回复
  （按 `req_id` 关联）。超时抛 `TimeoutError`。
- 主题匹配：精确匹配或尾部 `#` 通配符（`a/#` 匹配 `a`、`a/b`、...）。
- **智能体之间永远不直接调用** — 只通过总线。这为将来替换进程内总线为
  网络传输而不修改 agent 代码留了门。

## 6. 共享状态 — ReadOnlyContext + CoordinatorContext + TaskBoard

文件：[tests/harness/context.py](tests/harness/context.py)

- `ReadOnlyContext`：所有 agent 持有的只读视图。包含基线（启动时注入，
  不可变）、strategy_text（不可变）、round_history_snapshot（不可变元组，
  由 Coordinator 在每轮广播后刷新）、aborted（只读）。
- `CoordinatorContext`：仅由 Coordinator 持有的可写子类。提供 `append_round` /
  `mark_aborted` / `publish_state` 等写方法。每次写操作刷新
  `round_history_snapshot`（不可变元组）。
- `TaskBoard`：共享任务列表（状态：`pending` / `doing` / `done` / `failed`）。
  由 Coordinator 维护；agent 可直接读取。
- **私有状态原则**：L3 自治 agent 维护私有状态（窗口、计数器），**不**直接读
  `ctx.round_history`。它们订阅 `round/done` 并累积自己的私有窗口。这确保了
  自治多智能体的自包含原则。
- `RunContext` 保留为 `CoordinatorContext` 的向后兼容别名。

## 7. 配置（环境变量）

由 `tests/agents/config.py` 的 `load_config_from_env()` 读取；通过
`tests/conftest.py` 注入。

### 运行参数
| 变量 | 默认值 | 含义 |
|------|--------|------|
| `BURNIN_STRATEGY` | `""` | 自然语言策略提示（由 `strategy.py` 解析） |
| `BURNIN_MAX_ROUNDS` | `0` (∞) | 最大轮次 |
| `BURNIN_MAX_DURATION` | `0` (∞) | 最大总时长（秒） |
| `BURNIN_BASE_INTERVAL` | `60` | 轮间基础冷却（秒） |
| `BURNIN_INTERVAL_MIN` / `BURNIN_INTERVAL_MAX` | `30` / `600` | 自适应间隔上下限（秒） |
| `BURNIN_RECOVER_TIMEOUT` | `180` | 单轮恢复超时（秒） |
| `BURNIN_FAIL_THRESHOLD` | `5` | 累计失败 → 中止 |
| `BURNIN_FAIL_CONSECUTIVE` | `3` | 连续失败 → 中止 |
| `BURNIN_K` | `1.5` | 自适应间隔系数 |
| `BURNIN_EVENT_WINDOW` | `30` | 事件检查窗口（秒） |
| `BURNIN_PER_ROUND_LLM` | `0` | 设为 `1/true/on` 时 Analyst 每轮 LLM 点评 |
| `BURNIN_NOTIFIER` | `print` | 通知通道：`print` 或 `webhook` |
| `BURNIN_VOTE_TIMEOUT` | `1.0` | 每轮投票收集超时（秒） |

### 设备凭据
| 变量 | 默认值 | 含义 |
|------|--------|------|
| `BURNIN_HOST` | `192.168.3.33` | 设备地址 |
| `BURNIN_USER` | `admin` | Digest 用户名 |
| `BURNIN_PASSWORD` | （必填） | Digest 密码 |

### LLM（OpenAI 兼容；默认 OpenRouter）
| 变量 | 回退 | 含义 |
|------|------|------|
| `LLM_API_KEY` | `OPENROUTER_API_KEY` | API key（首选名称） |
| `LLM_BASE_URL` | `OPENROUTER_BASE_URL` → `https://openrouter.ai/api/v1` | 基址 URL |
| `LLM_MODEL` | `OPENROUTER_MODEL` → `tencent/hy3:free` | 模型名 |

通过设置 `LLM_BASE_URL` 切换平台（如 DeepSeek `https://api.deepseek.com/v1`、
Moonshot `https://api.moonshot.cn/v1`、本地 Ollama `http://localhost:11434/v1`）。
Key 从环境变量或仓库根目录 `.env` 读取（`.env` 已 gitignore）；永不打印。

## 8. 运行

### 完整拷机会话（需要真实设备）
```powershell
$env:BURNIN_PASSWORD = "121212.."
python -m pytest tests/test_burnin.py::test_burnin_session -v -s
```

### 策略层测试（无需设备）
```powershell
python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation `
                  tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v -s
```

### 自治层单元测试（无需设备）
```powershell
python -m pytest tests/test_context.py tests/test_scribe.py `
                  tests/test_trend_supervisor.py tests/test_risk_analyst.py `
                  tests/test_coordinator_decisions.py tests/test_integration.py -v
```

### 单独运行某个 agent
每个 agent 模块都有 `__main__` 块，例如：
```powershell
python tests/harness/scribe_agent.py
```

## 9. 核心设计原则

1. **严格分层**：确定性循环核心（L2）永不依赖 LLM；LLM 永不决定单轮 pass/fail。
   L3 自治 agent 仅提供建议。
2. **总线唯一通信**：agent 之间无直接方法调用；这使未来分布式部署成为可能。
3. **优雅降级**：LLM 不可用 → 规则引擎；设备不可达 → 记为失败；永不死锁。
4. **自适应间隔**：`next = clamp(recover_time × k + base, MIN, MAX)` —
   设备慢 → 冷却更长；设备快 → 紧凑循环。无需手动调参。
5. **仅标准库**：无第三方依赖（使用 `urllib`、`asyncio`、`secrets`、
   `dataclasses`）。LLM 客户端也用 `urllib`。
6. **安全**：API key 仅来自环境变量 / `.env`；永不硬编码，永不打印。
7. **可复现**：单轮 pass/fail 完全确定性；LLM 仅建议，可覆盖。
8. **自治主动监控**：L3 agent（TrendSupervisor + RiskAnalyst）主动检测趋势
   并 raise 事故，无需被询问。这是核心自治属性——agent 不只是响应查询，
   它们独立监控并告警。
9. **决策矩阵安全**：事实层独裁（found/changed → fail）；风险分只能添加
   warn/recheck 标记，永不能覆盖 fail。Critical 事故无视风险分强制 recheck。
10. **强制事故确认**：Coordinator 必须 ack 其他 agent raise 的每个事故
    （强制回声），但永不 ack 自己 raise 的。这确保没有事故被忽视。
11. **私有状态隔离**：L3 agent 维护私有窗口/计数器，不直接读共享上下文的
    round_history。它们订阅 `round/done` 并累积自己的状态。

## 10. 故障模式与降级

| 故障 | 检测 | 响应 |
|------|------|------|
| 设备未恢复 | `WatchAgent` 轮询超时 | `device/recovered` t_recover=None → Coordinator 记失败 |
| 重启事件缺失 | `EventCheckAgent` 窗口内无 `3/123` | `check/event` found=False → 轮次失败 |
| 状态漂移 | `StatusCheckAgent` 对比基线 | `check/status` changed=True → 轮次失败 |
| 累计失败 ≥ 阈值 | Coordinator 计数器 | `coord/abort` → 优雅关闭 |
| 连续失败 ≥ 阈值 | Coordinator 计数器 | `coord/abort` → 优雅关闭 |
| LLM 不可用 / 超时 | `AnalystAgent._ensure_llm` 返回 None | 规则兜底投票；Coordinator 的 `_consult_analyst` 返回 None → 确定性降级 |
| Analyst 建议停止 | `analyst/advise/reply` continue=False | Coordinator 中止 |
| Analyst 建议继续 | `analyst/advise/reply` continue=True | Coordinator 记失败，阈值仍适用 |
| 无投票者回复 | `_collect_votes` 超时 | 默认中性风险（50）→ 决策矩阵视为 pass |
| 全部弃权 | `_combine_votes` 返回 `all_abstain` | 风险分 = 50 → 决策矩阵视为 pass |
| TrendSupervisor 检测递增连续 | 3 连续 → warn；5 → critical | `incident/raise` → Coordinator acks；critical 强制 recheck |
| TrendSupervisor 检测失败率 > 30% | ≥5 样本，向上穿越 | `incident/raise`（warn）→ Coordinator 记录 |
| TrendSupervisor 检测恢复时间尖峰 | > 2× 历史均值 | `incident/raise`（warn）→ Coordinator 记录 |
| RiskAnalyst：3 连续高风险 | risk > 80 持续 3 轮 | `incident/raise`（critical）→ Coordinator 强制 recheck |
| RiskAnalyst：单轮极高风险 | risk ≥ 90 | `incident/raise`（warn）→ Coordinator 记录 |
| Critical 事故 raise | L3 agent 发布 `incident/raise` severity=critical | Coordinator acks + 设置 `_has_critical_incident` → 决策矩阵强制 recheck |

## 11. 已知限制

- `tests/agents/` 保留了早期单进程实现（`supervisor.py`、`reboot_agent.py`
  等），已被 `harness/` **取代**。仅 `device_client.py`、`config.py`、
  `strategy.py`、`report.py` 仍被复用。
- `ReporterAgent` 已从当前框架中移除（Phase 3）；其功能由 `ScribeAgent` +
  `NotifierAgent` 吸收。
- `CoordinatorContext` 是单内存对象；跨进程不安全（分布式总线需重构）。
- `NotifierAgent` 的 webhook 通道是桩（`_send_webhook` 是空操作）。
- LLM 在事故（advise）和每轮投票（vote）时被咨询；每轮 LLM 点评通过
  `BURNIN_PER_ROUND_LLM=1` 按需开启。
- `test_burnin_session` 需要真实设备，不在 CI 中运行；仅策略层和自治层
  单元测试是无设备的。

## 12. 真正自治多智能体特性（feat/true-autonomous-mas 分支）

以下特性在 `feat/true-autonomous-mas` 分支中实现，达到业界自治多智能体标准：

### 12.1 EventBus 真异步化
- `publish()` 改为 fire-and-forget（`asyncio.create_task` 执行 handler）
- `publish_and_wait()` 保留给需要同步保证的场景（测试/baseline）
- handler 异常不传播，仅记录日志
- `vote_timeout` 现在真正生效（不再被 LLM 调用阻塞）

### 12.2 L3 Agent 主动循环
- TrendSupervisorAgent: 每 30s 主动检查趋势（`_proactive_check`）
  - 陈旧数据告警：>5 分钟无新轮次 → raise warn
- AnalystAgent: 每 45s 主动检查风险历史
  - 连续高风险重新评估：确保 critical 事故被 raise
- 这是核心自治属性——agent 不只响应消息，还独立监控

### 12.3 Recheck 完整实现
- 决策矩阵返回 `recheck` 时，Coordinator 发布 `coord/recheck`
- EventCheckAgent + StatusCheckAgent 订阅 `coord/recheck`，重新检查设备
- Recheck 结果通过 `check/event` + `check/status` 回到 Coordinator
- 最多 recheck 1 次（`_recheck_pending` 防止无限循环）

### 12.4 投票快速路径
- `_collect_votes` 中，任一投票者 `risk >= 90` 立即返回
- 不等待其他投票者，高风险立即触发 recheck

### 12.5 保守错误处理
- 投票异常时 `decision = warn`（不是 pass）
- 风险分设为 60（保守中高风险）
- 安全优先：乐观 pass 被替换为保守 warn
