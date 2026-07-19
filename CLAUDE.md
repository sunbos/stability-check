# CLAUDE.md — Claude 项目指南

> 本文件为 Claude（Anthropic）提供本项目的上下文，使其能快速理解代码库并正确地
> 进行修改、调试和扩展。

---

## 项目简介

`stability_harness_loop_multiagent` 是一个**纯 Python、零第三方依赖**的通用框架，
用于构建基于「三引擎」架构的自治循环系统。框架与领域无关——具体场景（拷机、巡检、
压测、运维编排等）通过实现 `TargetAdapter` 和注册角色化 Agent 来剪裁。

---

## 架构一句话

```
Harness（让系统活着且受约束）
    ↕ EventBus（唯一接缝）
Loop（驱动确定性迭代、独裁裁决）
    ↕ EventBus
Multi-Agent（执行领域操作 + 投票建议 + 观察上报）
```

**三引擎互不 import，所有跨引擎通信走 EventBus。**

---

## 最重要的规则

### 1. 引擎隔离

- `harness/` 绝不 import `loop/` 或 `multi_agent/`。
- `loop/` 绝不 import `multi_agent/`（反之亦然）。
- 违反此规则会破坏整个架构的可替换性。

### 2. 事实独裁（绝对安全底线）

在 `loop/decision.py` 中的 `DecisionAuthority`：

```python
# 任何一个事实为 False → 裁决为 fail，风险分无法翻转
for name, ok in facts.items():
    if not ok:
        return Verdict("fail", ...)
```

- 风险分仅加 `warn`（60-80）或 `recheck`（>80）注解。
- critical 事件强制 `recheck`。
- 决策出错 → 保守 `warn(60)`（`CONSERVATIVE_RISK`），**绝不** `pass`。

### 3. 不可变快照

- `SharedContext` 由 `ControlLoop` 独占写入。
- Agent 只能通过 `ReadOnlyContext` 冻结快照观察。
- Agent 私有状态在 `self.state` 字典中，绝不共享。

---

## 关键文件

| 文件 | 作用 |
|------|------|
| `harness/bus.py` | EventBus：publish / subscribe / request-reply / `#` 通配 |
| `harness/agent.py` | Agent 基类 + AgentSpec 注册元数据 |
| `harness/runtime.py` | Agent 生命周期管理、监督器重启 |
| `harness/watchdog.py` | 引擎外的存活/死锁检测器 |
| `harness/governance.py` | AccessControl / Quota / Budget / CircuitBreaker |
| `harness/verify.py` | Verifier 护栏、评估钩子 |
| `harness/telemetry.py` | 可观测：Sink（Print / Memory / Null） |
| `loop/driver.py` | ControlLoop（sense→plan→act→check→decide→halt）+ RunConfig |
| `loop/decision.py` | DecisionAuthority + Verdict + NEUTRAL_RISK / CONSERVATIVE_RISK |
| `loop/context.py` | SharedContext / ReadOnlyContext / RoundRecord |
| `loop/termination.py` | StopCondition + TerminationPolicy（OR 组合） |
| `loop/scheduler.py` | Scheduler + RetryBudget + clamp |
| `multi_agent/adapter.py` | TargetAdapter 协议（act / observe / events） |
| `multi_agent/protocols.py` | AdvisorContract / ObserverContract / combine_votes |
| `multi_agent/workers/` | WorkerAgent 基类 + ExampleWorkerAgent |
| `multi_agent/advisors/` | AdvisorAgent + TrendSupervisorAgent + RiskAnalyst |
| `multi_agent/observers/` | ObserverAgent + ScribeAgent + NotifierAgent |
| `examples/smoke.py` | 端到端冒烟演示（FakeTargetAdapter + 合成角色） |
| `docs/plans/设计文档.md` | 完整的三引擎设计文档 |

---

## 常用命令

```bash
# 运行所有测试
pytest tests/ -v

# 独立冒烟演示
python stability_harness_loop_multiagent/examples/smoke.py

# 仅运行单元测试（事实独裁）
pytest tests/test_stability_harness_loop_multiagent_smoke.py::test_decision_authority_fact_dictatorship_unit -v
```

---

## 代码风格

- **语言**：代码注释、docstring、提交信息使用**中文**。
- **类型注解**：使用 `typing` 模块（兼容 Python 3.10+）。
- **异常策略**：Agent 内部的异常被基类捕获并记日志，绝不泄漏给 EventBus 发布者。宽泛异常捕获标记 `# noqa: BLE001`。
- **零第三方依赖**：仅使用标准库。请勿引入 `pip install` 的包。

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
target/acted            → Worker 已执行操作
target/recovered        → 目标已恢复
target/checked          → 事实检查结果
```

---

## 如何扩展

### 添加新场景（最常见）

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

---

## 关键设计决策

### 为什么 ControlLoop 中有 combine_votes 的本地镜像？

`multi_agent/protocols.py` 中有规范的 `combine_votes`，但 `ControlLoop`（`loop/driver.py`）
有自己的 `_default_combine`。这是**故意的**——为了保持引擎隔离，Loop 绝不 import
Multi-Agent。两份实现逻辑一致：

- 快速路径：任意 risk ≥ 90 立即胜出。
- 弃权（confidence ≤ 0）权重为 0。
- 全部弃权 → 中性默认 50（`NEUTRAL_RISK`）。

### 为什么 Advisor 不能裁决？

Advisor 只投 `(risk, confidence)` 票，由 `DecisionAuthority` 做最终裁决。
这保证了安全底线的确定性——事实独裁不会被任何 Agent 绕过。

### 为什么用 EventBus 而不是直接方法调用？

今天进程内（`asyncio`），明天可换 A2A / MCP 网络传输而**不改 Agent 代码**。
这是架构可替换性的核心保障。

---

## 不要做的事

| ❌ 禁止 | ✅ 应该 |
|---------|---------|
| 在 `loop/` 中 import `multi_agent/` | 通过 EventBus 话题通信 |
| 让 Advisor 直接修改 verdict | Advisor 只投票，Loop 裁决 |
| 省略 `await` 的超时 | 每个跨引擎 await 都有超时 |
| 在 Agent 之间直接引用 | 通过 `self.bus.publish()` 通信 |
| 引入第三方 pip 包 | 仅使用标准库 |
| 将失败事实翻转为 pass | 事实独裁：False → fail，不可翻转 |
| 共享可变状态 | 使用 ReadOnlyContext 快照 + Agent 私有 state |
| 让 `error=True` 路径返回 pass | 保守 warn(60)，绝不乐观 pass |

---

## 测试哲学

- 测试验证**架构不变量**，而非实现细节。
- 使用合成适配器（`FakeTargetAdapter`），不引入外部依赖。
- 使用极短超时使测试快速确定性完成。
- 核心不变量：循环终止、裁决产生、事件扇出、事实独裁。
