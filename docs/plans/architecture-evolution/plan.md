# 架构演进实施计划（Plan）

> 由 Superpowers 流程的 Implementation Planning 阶段产出。
> 对应 spec：同目录 `design.md`。
> 执行顺序：P0（地基）→ P1（治理能力）→ P2（事件驱动）。每阶段 TDD（先红后绿）。

---

## 阶段 P0：契约内核 `core/` 拆分（机械、低风险）

### P0-1 创建 `core/` 包
- 新建 `stability_harness_loop_multiagent/core/__init__.py`，re-export `EventBus, Agent, AgentSpec`，
  并更新 `__all__`。
- 新建 `core/bus.py`、`core/agent.py`：内容与现 `harness/bus.py`、`harness/agent.py` 完全一致
  （`agent.py` 内 `from .bus import EventBus` 保持不变，因同包相对）。

### P0-2 删除 harness 内旧文件
- 删除 `harness/bus.py`、`harness/agent.py`。

### P0-3 改写 harness 内部 import
- 文件：`runtime.py`、`watchdog.py`、`governance.py`、`verify.py`、`telemetry.py`
- `from .agent import ...` → `from ..core.agent import ...`
- `from .bus import ...` → `from ..core.bus import ...`

### P0-4 改写 loop / multi_agent import
- `loop/driver.py`：`from ..harness.agent` / `from ..harness.bus` → `from ..core.agent` / `from ..core.bus`
- `multi_agent/workers/base.py`、`workers/example.py`、`advisors/base.py`、`advisors/risk_analyst.py`、
  `advisors/trend_supervisor.py`、`observers/base.py`、`observers/scribe.py`、`observers/notifier.py`：
  `from ...harness.agent` / `from ...harness.bus` → `from ...core.agent` / `from ...core.bus`

### P0-5 改写顶层与业务层 import
- 顶层 `__init__.py`：`from .harness.bus` / `from .harness.agent` → `from .core.bus` / `from .core.agent`
- 全仓 grep `harness.agent\|harness.bus`，若 `harness/__init__.py`、`business/**` 有引用一并改写。

### P0-6 验证（Green）
- `pytest tests/ -q` 全绿。
- `python stability_harness_loop_multiagent/examples/smoke.py` 正常。
- 静态校验：`grep -rn "harness.agent\|harness.bus" loop/ multi_agent/` 应为 0。

---

## 阶段 P1：治理能力经总线落地（最佳实践 · 先红后绿）

### P1-0 决策对齐（2026-07-18，用户授权"做最佳实践"）
1. **范围**：清理 + 最小接业务。治理/校验模块已隔离良好，业务接线非架构必需；
   先让能力可测、可用，再以 opt-in 方式最小接入 hikvision，避免过度耦合。
2. **拒绝语义**：fail-closed，只拦操作、不 halt 循环。`Governance.evaluate` 的
   `emit_abort` 在网关模式下默认 `False`（纯回复）；硬违例 halt 作为可选开关保留。
3. **闸门粒度**：每轮粗粒度——在 `Worker.act()` 异步入口做一次 `harness/govern/request`
   （`operation="round"`），覆盖本轮全部操作；与同步 `do_work` 兼容。
4. **执行顺序**：P1 轻量（修 Bug + 单测 + 闸门 API + opt-in 接线）→ P2 事件驱动化
   （真实收益最高）→ 业务接线收尾。

### P1-1 修 `Governance.evaluate` 副作用 Bug（必须先于接入）
- governance.py:232-240，`Quota.consume`/`Budget.spend` 当前在判定前**无条件**扣减，
  导致"被访问控制在第一步拒绝"的请求仍消耗配额/预算（拒绝也扣额度，语义错误）。
- 改为两阶段：先算 breaches（不突变），全部通过才提交 `consume`/`spend`；任一失败则不突变。
  实现：给 `Quota` 加 `can_consume(key, amount)`、`Budget` 加 `can_spend(amount)`
  非突变探测方法；`evaluate` 先探测、通过后再提交。

### P1-2 固化治理/校验单测（Red→Green）
- 新增 `tests/test_governance.py`：AccessControl 拒绝/放行/默认拒绝/通配；
  Quota 滚动上限 + **拒绝不突变**（P1-1 后）；Budget 累计上限 + **拒绝不突变**；
  CircuitBreaker 状态机；Governance.evaluate 允许时提交、拒绝时不提交；
  `GovernanceAgent` 经总线回复 allowed/denied，且默认 `emit_abort=False` 下**不**发 `harness/abort`。
- 新增 `tests/test_verify.py`：Verifier 输入护栏 fail-closed 抛 `VerifyError`；
  run_eval 聚合评分；`VerificationAgent` 经总线回复 allowed/denied。
- 针对既有实现，应直接 Green（锁定"能力可用"）。

### P1-3 提供干净的异步闸门 API（最佳实践）
- `harness/governance.py` 新增模块函数 `async def gate_allowed(agent, req, timeout=1.0)
  -> bool`：发 `harness/govern/request` 并 `await` 回复；超时/异常一律 fail-closed 返回 `False`。
  集中 fail-closed 语义，便于复用。
- `business/hikvision/worker.py` 的 `act()` 在 `to_thread(do_work)` 之前调用
  `await gate_allowed(self, {"role": self.role, "capability": "door-test",
  "operation": "round"}, timeout=...)`；拒绝则跳过 do_work 并发布 denied 事实。
- **opt-in、非破坏性**：runner 新增可选 `governance=None` / `verifier=None` 参数；
  为 `None` 时 worker 闸门自动放行（行为不变）。既有测试不受影响。

### P1-4 接线 Verifier（最小、opt-in）
- `runner.py`：当 `verifier` 非 `None` 时构造 `VerificationAgent(bus, verifier)` 并随 Watchdog 一同 start。
- 校验点（LLM 护栏）在后续按需接入；本期只保证网关可被挂载、可被总线触发。

### P1-5 验证
- 既有 `tests/` 全绿；新增 P1-2 单测全绿。
- 端到端（或新增集成测试）：注入超配额/非法操作 → 操作被拒绝、配额未误扣；
  LLM 畸形 payload → 建议被丢弃。

---

## 阶段 P2：ControlLoop 事件驱动化（TDD 优先）

### P2-1 红：新增"早到/永不"测试
- 在 `tests/test_stability_harness_loop_multiagent_smoke.py` 或新建 `tests/test_control_loop.py`：
  - 用例 A（早到）：Advisor/Worker 极快回复 → round 在超时前结束（断言耗时 < `max(recover,check)`）。
  - 用例 B（永不）：投票永不回复 → 循环在 `vote_timeout` 上限内终止（防死锁不变量）。
- 执行：应先 Red（当前实现硬等，A 会超时或耗时等于整段超时）。

### P2-2 绿：改写 `_run_round` / `_collect_votes`
- `loop/driver.py:_run_round`：用 `asyncio.wait([reply_ready, timeout_task], FIRST_COMPLETED)`，
  由 `_start_collect` 的完成信号触发 `reply_ready`；超时任务保留为兜底。
- `_collect_votes`：同理，收齐 `agent/vote/reply` 即返回，超时仅兜底。
- 保留所有既有超时值与保守回退（`decide(error=True)→warn(60)`）。

### P2-3 绿：回落 run_timeout
- `tests/test_hikvision_runner.py::test_runner_completes_with_fake_client`：将 `run_timeout` 从 60 调回 20，
  验证事件驱动后不再超时。

### P2-4 验证
- P2-1 用例转 Green；全量 `pytest` 绿；`smoke.py` 正常。

---

## 阶段 P3（可选，本期不做）
- `BusProtocol` 抽象（进阶），按需推进；不在本期范围。

---

## 执行检查点
- P0 完成即跑全量测试 + smoke，确认零回归后再进 P1。
- P1 接线后先单测后集成，确认治理行为正确再进 P2。
- P2 每改一处即跑相关测试，保留超时兜底不变量。
- 全部完成后更新 `AGENTS.md` 与 `架构演进路线.md` 使其与实现一致。
