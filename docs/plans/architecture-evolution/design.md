# 架构演进设计文档（Spec）—— 三引擎契约内核与治理能力落地

> 关联：本文件是 `docs/plans/架构演进路线.md` 的**可执行规格（spec）**，
> 由 Superpowers 流程的 Brainstorming/Design 阶段产出。
> 分析依据：`docs/plans/architecture-evolution/analysis-report.md`（code-explorer 子代理全包扫描）。
> 计划：见同目录 `plan.md`。

---

## 0. 目标与非目标

### 目标
让框架"从架构上就是对的"，后续无需大改：

1. **P0 — 契约内核 `core/` 拆分**：把跨引擎共享的 `EventBus` 与 `Agent`/`AgentSpec`
   抽成独立 `core/` 包，使 harness / loop / multi_agent 三引擎真正对等、边界可在模块层强制。
2. **P1 — 治理能力经总线落地**：把已实现的 `Governance`/`Verifier`（含 `GovernanceAgent`/
   `VerificationAgent` 总线网关）接到业务链路，并补单测锁定行为。
3. **P2 — ControlLoop 事件驱动化**：每轮由"收齐预期回复即返回 + 超时仅兜底"驱动，
   消除 `driver.py:128` 的硬等窗口。

### 非目标（YAGNI）
- 不引入配置中心 / 插件热加载等未被需求驱动的机制。
- 不破坏"三引擎只走 EventBus 通信"的既有约束（任何新增依赖都走总线话题）。
- 不做超出 `core/` 的额外抽象层（P3 `BusProtocol` 标记为可选，本期不做）。

---

## 1. 现状事实（已核实）

- `harness/bus.py` 仅依赖标准库；`harness/agent.py:14` 仅 `from .bus import EventBus`。
  两者**自包含、零内部 harness 依赖**，是天然的契约内核。
- 三引擎对 harness 的真实依赖（去噪后）：
  - `loop/driver.py:22-23` → `harness.agent`、`harness.bus`
  - `multi_agent/{{workers,advisors,observers}}/*/base.py` 及各具体 Agent → `harness.agent`、`harness.bus`
  - 其余 harness 内部模块引用 agent/bus 均为 `from .agent` / `from .bus`（相对 import）。
- `harness/governance.py`、`harness/verify.py` 已实现且 `GovernanceAgent`/`VerificationAgent`
  为总线网关（request/reply），但 `runner.py`/`worker.py`/示例均未实例化或调用它们，且无单测。
- `loop/driver.py:128` 每轮固定 `await asyncio.sleep(max(recover_timeout, check_timeout))`，
  回复收齐后仍硬等满超时（P2 根因）。

---

## 2. 设计原则

1. **最小可用契约**：`core/` 只放真正跨引擎共享的 2 个契约（bus + agent）。
2. **对外 API 零变化**：顶层 `__init__.py` 仍 re-export 全部公共符号；迁移者改 import 目标，不改名字。
3. **总线是唯一接缝**：P1 的治理接入一律走 `harness/govern/request`、`harness/verify/request`，
   loop/mas 不 import 治理实现。
4. **fail-closed 与防死锁不变量不变**：P2 不得以"事件驱动"为名移除超时兜底；
   任何"提前返回"都必须有超时作为硬上限。
5. **TDD**：每个行为变更先写失败测试（Red），再实现（Green），最后重构。

---

## 3. P0 设计：契约内核 `core/` 拆分

### 目标结构
```
stability_harness_loop_multiagent/
├── core/                      # 新增：契约内核
│   ├── __init__.py            # re-export EventBus, Agent, AgentSpec
│   ├── bus.py                 # 原 harness/bus.py
│   └── agent.py               # 原 harness/agent.py
├── harness/                   # 治理引擎（消费 core 契约）
├── loop/                      # Loop 引擎（消费 core 契约）
└── multi_agent/               # MAS 引擎（消费 core 契约）
```

### 依赖关系（三引擎对等）
```
        core/  (bus + agent)
        ↑    ↑     ↑
   harness   loop   multi_agent
```

### 迁移规则（机械）
- `core/bus.py`、`core/agent.py`：内容与现 `harness/bus.py`、`harness/agent.py` 完全一致，
  `agent.py` 内 `from .bus import EventBus` 改为 `from .bus import EventBus`（同包内相对，不变）。
- `harness/agent.py`、`harness/bus.py`：删除。
- harness 内部消费者：`from .agent` / `from .bus` → `from ..core.agent` / `from ..core.bus`
  （涉及 runtime.py、watchdog.py、governance.py、verify.py、telemetry.py）。
- `loop/driver.py`：`from ..harness.agent` / `from ..harness.bus` → `from ..core.agent` / `from ..core.bus`。
- `multi_agent/**`：`from ...harness.agent` / `from ...harness.bus` → `from ...core.agent` / `from ...core.bus`。
- 顶层 `__init__.py`：`from .harness.bus` / `from .harness.agent` → `from .core.bus` / `from .core.agent`。
- 若 `harness/__init__.py` 或 `business/**` 直接引用 `harness.agent`/`harness.bus`，同步改写（全仓 grep 确认）。

### 验收
- `pytest tests/ -q` 全绿；`python stability_harness_loop_multiagent/examples/smoke.py` 正常。
- 静态校验：`grep -rn "harness.agent\|harness.bus" loop/ multi_agent/` 应为 0。
- 三引擎 import 中不再出现对其他引擎治理实现的引用。

---

## 4. P1 设计：治理能力经总线落地（最佳实践）

### 决策对齐（2026-07-18）
1. 范围=清理+最小接业务；2. 拒绝=fail-closed 只拦操作不 halt（`emit_abort` 网关默认 `False`）；
3. 粒度=每轮粗粒度（`act()` 入口一次）；4. 顺序=P1 轻→P2 优先→业务收尾。

### 接入拓扑（全部走总线，网关纯回复模式）
```
HikvisionWorker.act() ──harness/govern/request──▶ GovernanceAgent ──▶ Gov.evaluate
                                                         │ 回复 allowed/denied（fail-closed）
                                                         └─（emit_abort=True 时）publish harness/abort
HikvisionWorker.act() ──harness/verify/request──▶ VerificationAgent ──▶ Verifier
```
> 网关默认 `emit_abort=False`：违例只回复 `allowed=False`，由请求方（Worker）决定跳过该操作，
> **不** halt 整个循环。硬违例 halt 作为可选开关（`emit_abort=True`）保留，与 Watchdog 共用接缝。

### 接线点
- **每轮粗粒度护栏**：`HikvisionWorker.act()` 在 `to_thread(do_work)` 之前，发一次
  `harness/govern/request`（`{role, capability:"door-test", operation:"round"}`），
  仅当回复 `allowed=True` 才执行本轮；外部 API 调用统一受 `CircuitBreaker` 保护（Governance 内部持有）。
- **异步闸门 API**：`harness/governance.gate_allowed(agent, req, timeout)` 封装
  `Agent.request` + fail-closed（超时/异常 → `False`），集中语义。
- **opt-in、非破坏性**：runner 以 `governance=None`/`verifier=None` 为默认；为 `None` 时 worker
  闸门自动放行，既有 47 项测试不受影响。

### 关键修正：`Governance.evaluate` 副作用 Bug
- 现状（governance.py:232-240）：`Quota.consume`/`Budget.spend` 在判定前无条件扣减，
  被访问控制在第一步拒绝的请求仍消耗配额/预算。
- 修正：给 `Quota` 加 `can_consume`、给 `Budget` 加 `can_spend`（非突变探测）；
  `evaluate` 先探测全部策略，全部通过才提交 `consume`/`spend`，任一失败则不突变。

### 组装（runner.py，opt-in）
- 当 `governance` 非 `None`：构造 `GovernanceAgent(bus, governance)` 并随 Watchdog 一同 start。
- 当 `verifier` 非 `None`：构造 `VerificationAgent(bus, verifier)` 并 start。
- Worker/LLM 通过 `gate_allowed` / `Agent.request` 发起，不 import 治理实现。

### 验收
- 新增 `tests/test_governance.py`、`tests/test_verify.py`：固化 8 项探针行为
  （AccessControl / Quota / Budget / CircuitBreaker / evaluate 拒绝不突变 / Verifier fail-closed /
  run_eval / 总线网关）为稳定单测。
- 端到端：注入超配额/非法操作 → 操作被拒绝、配额未误扣；畸形 payload → 建议被丢弃。

---

## 5. P2 设计：ControlLoop 事件驱动化

### 现状
`loop/driver.py:128` 每轮 `await asyncio.sleep(max(recover_timeout, check_timeout))` 硬等；
`_collect_votes` 同理按固定 `vote_timeout` 等待。回复早到也要等满超时，造成无效延迟与测试超时。

### 方案
- `_run_round`：用 `asyncio.wait` + `FIRST_COMPLETED` 等待"所有预期回复到达"或"超时兜底"二者先到。
  预期回复由 `_start_collect` 的 `on_first`/`on_all` 信号驱动；超时兜底保留（防死锁不变量不变）。
- `_collect_votes`：同理，收齐所有 Advisor 的 `agent/vote/reply` 即返回，超时仅兜底。
- 真机环境仍以超时作为存活兜底，循环一定终止（不变量不变）。

### 验收
- `test_runner_completes_with_fake_client` 可将 `run_timeout` 调回合理值（如 20s）仍稳定通过。
- 注入"投票永不回复" → 循环在超时上限内终止（防死锁不变量）。
- 注入"投票极快回复" → round 提前结束，不空等整段超时。

---

## 6. 风险与缓解

| 风险 | 缓解 |
|------|------|
| P0 移动文件误伤未提交改动 | 移动前全仓 grep 确认仅 import 改写；执行后跑全量 pytest + smoke 验证 |
| P1 治理接入改变既有执行时序 | 走总线 request/reply，超时兜底；先单测锁定治理行为再端到端 |
| P2 事件驱动引入竞态 | 保留超时兜底；用 `FIRST_COMPLETED` 不移除等待；新增"早到/晚到/永不"三态测试 |
| 分支已有大量未提交修改 | 每阶段独立、可验证、可回退；完成一阶段即跑测试确认无回归 |

---

## 7. 成功标准（Definition of Done）

- [x] P0：`core/` 拆分完成，全量测试 + smoke 通过，三引擎 import 仅指向 `core`。
- [x] P1：`Governance`/`Verifier` 经总线接入 runner（opt-in），端到端可触发拒绝/护栏；单测覆盖。
- [x] P1-b：`HikvisionAdvisor` 解析计划后发 `harness/verify/request`（fail-closed 丢弃），校验真触发。
- [x] P1-c：worker 破坏性外部操作经 `_guarded_adapter_act` 受 `CircuitBreaker` 保护。
- [x] P1-d：治理 `DeniedOp(role?, capability?, op, match?)` 维度规则（支持 `None`/`"*"` 通配；`op` 支持 `exact`/`prefix`/`suffix`/`contains`/`regex` 匹配）+ 网关 `denied_ops`，按操作鉴权（跳过 reboot 仍执行开门）。
- [x] 治理结构化事实上报：`Telemetry.fact("governance.decision", ...)`（kind="fact"）；网关决策点 + worker fail-closed 超时补发。
- [x] 治理观测面板：`GovernancePanelAgent`（Observer，订阅 `harness/fact/governance.decision`）聚合成 dashboard；新增 `timeseries()` 零依赖时间序列视图；响应 `governance/panel/request` 回发 `governance/panel`（含 `panel`+`timeseries`）真实拉取；runner 接线 `governance.telemetry=tel` 并 opt-in 挂载。演示见 `examples/governance_panel_demo.py`（拉取 timeseries 生成 plotly 趋势报告，未装 plotly 回退 CSV；覆盖五种匹配）。
- [x] P2：`_run_round` 事件驱动（收齐即返回 + 超时兜底）。
- [x] P2-b：`_collect_votes` 事件驱动（静默期提前返回 + 超时兜底）；无投票者仍于上限内终止。
- [ ] P3：`BusProtocol` 抽象——**暂缓（YAGNI）**，仅当替换总线需求出现再推进。
- [x] 文档：`AGENTS.md` 与 `架构演进路线.md` 与实现一致。
