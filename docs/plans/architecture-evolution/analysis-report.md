# 全包架构分析事实记录（Analysis Report）

> 由 code-explorer 子代理广度扫描 + 关键文件精读得出，供 `design.md` / `plan.md` 作为事实锚点。
> 分析日期：2026-07-18。范围：`stability_harness_loop_multiagent/` 全部模块。

---

## A. 模块职责与公共 API（摘要）

| 引擎 | 模块 | 职责 | 公共 API |
|------|------|------|----------|
| harness | bus.py | 进程内异步事件总线（唯一接缝） | `EventBus` |
| harness | agent.py | 智能体基类 + 注册元数据 | `Agent`, `AgentSpec` |
| harness | governance.py | 访问/配额/预算/熔断 + 总线网关 | `Governance`, `GovernanceAgent`, `AccessControl`, `Quota`, `Budget`, `CircuitBreaker` |
| harness | verify.py | 输入/输出护栏 + 评估 + 总线网关 | `Verifier`, `VerificationAgent`, `VerifyError`, `VerifyResult`, `EvalResult`, `EvalReport` |
| harness | runtime.py | 生命周期注册表/监督器/路由 | `Runtime` |
| harness | watchdog.py | 存活/停滞/死锁探测 | `Watchdog` |
| harness | telemetry.py | 结构化日志 + sink | `Telemetry`, `Sink`, `PrintSink`, `MemorySink`, `NullSink` |
| loop | driver.py | ControlLoop + RunConfig | `ControlLoop`, `RunConfig` |
| loop | context.py | 轮次上下文/快照 | `SharedContext`, `ReadOnlyContext`, `RoundRecord` |
| loop | decision.py | 事实独裁决策矩阵 | `DecisionAuthority`, `Verdict`, `NEUTRAL_RISK`, `CONSERVATIVE_RISK` |
| loop | termination.py | 可组合中止条件 | `StopCondition`, `TerminationPolicy`, `CountStop`, `DurationStop`, `FailThresholdStop`, `ExternalAbortStop`, `ExternalStop` |
| loop | scheduler.py | 定速/退避/抖动/重试预算 | `Scheduler`, `RetryBudget`, `clamp` |
| multi_agent | adapter.py | TargetAdapter 协议 | `TargetAdapter`, `Event`, `Result`, `State` |
| multi_agent | protocols.py | Advisor/Observer 契约 + 投票合并 | `AdvisorContract`, `ObserverContract`, `combine_votes` |
| multi_agent | workers/base.py | 执行角色基类 | `WorkerAgent` |
| multi_agent | advisors/base.py | 建议角色基类 | `AdvisorAgent` |
| multi_agent | observers/base.py | 观察角色基类 | `ObserverAgent` |
| business/hikvision | runner.py | 海康端到端组装 | `run_hikvision_stability` |

---

## B. 跨引擎 import 约束核查（关键）

- **harness/ 不 import loop/ 或 multi_agent/**：全仓零命中。harness 内部仅相互 import（`.agent`/`.bus`）。
- **loop/ 对 harness 的 import**（逐文件）：
  - `loop/driver.py:22` `from ..harness.agent import Agent, AgentSpec`
  - `loop/driver.py:23` `from ..harness.bus import EventBus`
  - 其余 loop 模块不 import harness（仅引用 `harness/abort` 话题字符串）。
- **multi_agent/ 对 harness 的 import**（逐文件）：
  - `multi_agent/workers/base.py:11-12` → `harness.agent`, `harness.bus`
  - `multi_agent/workers/example.py:18-19` → `harness.agent`, `harness.bus`
  - `multi_agent/advisors/base.py:10-11` → `harness.agent`, `harness.bus`
  - `multi_agent/advisors/risk_analyst.py:20-21` → `harness.agent`, `harness.bus`
  - `multi_agent/advisors/trend_supervisor.py:17-18` → `harness.agent`, `harness.bus`
  - `multi_agent/observers/base.py:6-7` → `harness.agent`, `harness.bus`
  - `multi_agent/observers/scribe.py:14-15` → `harness.agent`, `harness.bus`
  - `multi_agent/observers/notifier.py:19-20` → `harness.agent`, `harness.bus`
- **结论**：三引擎真实共享的契约内核仅 `harness.agent` + `harness.bus` 两个文件。

---

## C. 契约内核事实锚点（P0 依据）

- `harness/bus.py:13-17` 仅 import 标准库（asyncio, logging, secrets, time, typing）。
- `harness/agent.py:14` 仅 `from .bus import EventBus`；`agent.py` 自身仅标准库 + bus。
- 两者**自包含、零内部 harness 依赖**，是天然可抽离的契约内核。
- harness 内部其他模块对 agent/bus 的引用方式（均为相对 import，改写时统一升级为 `..core`）：
  - `runtime.py:20-21` `from .agent import ...` / `from .bus import ...`
  - `watchdog.py:13-14` 同上
  - `governance.py:29-30` 同上
  - `verify.py:24-25` 同上
  - `telemetry.py:13` `from .bus import EventBus`
- 顶层 `__init__.py:11-12` `from .harness.bus import ...` / `from .harness.agent import ...`。

---

## D. 治理能力接线核查（P1 依据）

- `Governance` / `Verifier` 已实现；`GovernanceAgent` / `VerificationAgent` 已实现为总线网关
  （request/reply），存在于 `harness/governance.py` / `harness/verify.py`。
- 全局搜索 `Governance(` / `Verifier(` / `GovernanceAgent` / `VerificationAgent` 的**调用点**：
  仅在各自模块内部及顶层 `__init__.py` 的 re-export 出现；`runner.py` / `worker.py` / 示例**未实例化或调用**。
- `harness/govern/request`、`harness/verify/request` 等治理总线话题**尚未被订阅**。
- 结论：`governance.py` / `verify.py` 属"能力就绪但未接线"，且与看门狗共用 `harness/abort` 接缝，
  接入成本主要是组装（runner 注册网关）+ Worker/LLM 发 request。

---

## E. ControlLoop 每轮硬等（P2 依据）

- `loop/driver.py:128` `_run_round` 内：`await asyncio.sleep(max(self.recover_timeout, self.check_timeout))`
  在发布 `loop/tick` 后无条件硬等整段超时，即使 `target/recovered` / `target/checked` 早已收齐。
- `_collect_votes`（driver.py 内）按固定 `vote_timeout` 等待，同理不随回复早到而提前结束。
- 结论：这是"每轮固定延迟 + 测试超时回归"的根因；改为"收齐即返回 + 超时兜底"即可消除。

---

## F. 测试覆盖

- `tests/` 覆盖：三引擎冒烟（`test_stability_harness_loop_multiagent_smoke.py`）、hikvision runner/worker、
  决策/终止/调度等。
- **缺口**：`governance.py`、`verify.py`、契约内核（bus/agent）**无专门单测** → P1 补单测锁定行为。
