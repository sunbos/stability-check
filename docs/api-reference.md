# API 参考

> 本页由 [mkdocstrings](https://mkdocstrings.github.io/) 从源码 docstring 自动生成。
> 顶层 `stability_harness_loop_multiagent/__init__.py` re-export 全部公共 API,
> 这里按 5 层架构分组列出,与 [代码物理地图](plans/代码物理地图.md) 一一对应。

## 顶层包

::: stability_harness_loop_multiagent

## L1 · core/ 契约内核

零内部依赖的契约包,三引擎共同依赖。`harness/` / `loop/` / `multi_agent/` 都只 import `core.*`。

::: stability_harness_loop_multiagent.core.bus

::: stability_harness_loop_multiagent.core.agent

::: stability_harness_loop_multiagent.core.voting

## L2 · harness/ 治理引擎

让系统"活着且受约束":运行时 / 监督器 / 看门狗 / 遥测 / 治理 / 校验 / 总线追踪。

::: stability_harness_loop_multiagent.harness.runtime

::: stability_harness_loop_multiagent.harness.watchdog

::: stability_harness_loop_multiagent.harness.telemetry

::: stability_harness_loop_multiagent.harness.governance

::: stability_harness_loop_multiagent.harness.verify

::: stability_harness_loop_multiagent.harness.tracer

## L3 · loop/ 控制引擎

确定性迭代 + 事实独裁:控制循环 / 决策权 / 共享上下文 / 中止条件 / 调度。

::: stability_harness_loop_multiagent.loop.driver

::: stability_harness_loop_multiagent.loop.decision

::: stability_harness_loop_multiagent.loop.context

::: stability_harness_loop_multiagent.loop.termination

::: stability_harness_loop_multiagent.loop.scheduler

## L4 · multi_agent/ 多智能体引擎

执行(Worker) + 建议(Advisor) + 观察(Observer) + 目标适配协议。

### 协议契约

::: stability_harness_loop_multiagent.multi_agent.adapter

::: stability_harness_loop_multiagent.multi_agent.protocols

### Worker(执行型)

::: stability_harness_loop_multiagent.multi_agent.workers.base

::: stability_harness_loop_multiagent.multi_agent.workers.example

### Advisor(建议型)

::: stability_harness_loop_multiagent.multi_agent.advisors.base

::: stability_harness_loop_multiagent.multi_agent.advisors.trend_supervisor

::: stability_harness_loop_multiagent.multi_agent.advisors.risk_analyst

### Observer(观察型)

::: stability_harness_loop_multiagent.multi_agent.observers.base

::: stability_harness_loop_multiagent.multi_agent.observers.scribe

::: stability_harness_loop_multiagent.multi_agent.observers.notifier

::: stability_harness_loop_multiagent.multi_agent.observers.gov_panel

## L5 · business/hikvision/ 领域装配层

海康门禁稳定性测试领域层,可 import 任意引擎,不被反向依赖。

### 公共 API

::: stability_harness_loop_multiagent.business.hikvision

### Scenario YAML 场景(schema / adapter / worker / runner)

::: stability_harness_loop_multiagent.business.hikvision.scenario_schema

::: stability_harness_loop_multiagent.business.hikvision.scenario_adapter

::: stability_harness_loop_multiagent.business.hikvision.scenario_worker

::: stability_harness_loop_multiagent.business.hikvision.scenario_runner

### Hikvision 适配器与客户端

::: stability_harness_loop_multiagent.business.hikvision.adapter

::: stability_harness_loop_multiagent.business.hikvision.client

::: stability_harness_loop_multiagent.business.hikvision.advisor

### LLM 与诊断辅助

::: stability_harness_loop_multiagent.business.hikvision.llm

::: stability_harness_loop_multiagent.business.hikvision.llm_plan

::: stability_harness_loop_multiagent.business.hikvision.diagnostic

::: stability_harness_loop_multiagent.business.hikvision.event_codes

### 原子化能力(capabilities)

::: stability_harness_loop_multiagent.business.hikvision.capabilities

#### actions(原子化执行能力)

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.base

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.reboot

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.upgrade

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.remote_open

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.query_events

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.dispatch

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.sleep

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.noop

::: stability_harness_loop_multiagent.business.hikvision.capabilities.actions.switch_serial

#### preconditions(原子化前置条件)

::: stability_harness_loop_multiagent.business.hikvision.capabilities.preconditions.base

::: stability_harness_loop_multiagent.business.hikvision.capabilities.preconditions.baseline_record

::: stability_harness_loop_multiagent.business.hikvision.capabilities.preconditions.device_online

::: stability_harness_loop_multiagent.business.hikvision.capabilities.preconditions.serial_mode

#### probes(原子化探测能力)

::: stability_harness_loop_multiagent.business.hikvision.capabilities.probes.base

::: stability_harness_loop_multiagent.business.hikvision.capabilities.probes.field

::: stability_harness_loop_multiagent.business.hikvision.capabilities.probes.online

::: stability_harness_loop_multiagent.business.hikvision.capabilities.probes.count

::: stability_harness_loop_multiagent.business.hikvision.capabilities.probes.event_chain
