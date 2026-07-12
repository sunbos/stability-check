# 门禁设备稳定性拷机 — 多智能体 + Loop Engineering 设计方案

- 日期：2026-07-13
- 状态：设计已确认（brainstorming 产出）
- 目标设备：`192.168.3.33`（海康门禁/访问控制，ISAPI + Digest 认证 `admin / 121212..`）
- 阶段：MVP（仅 pytest + `os.environ` 注入，无独立前端/看板）

## 1. 背景与目标

通过**长时间、频繁重启**对门禁设备做稳定性拷机（burn-in），验证设备在反复上下电后仍能：
1. 正常重启并产生对应的**远程重启事件**（`AcsEvent` 中 `major=3, minor=123`）；
2. 重启后**门禁工作状态不发生改变**（与基准快照一致）。

采用 **真·多智能体编排**：由一个 Supervisor（监督者）驱动主循环，每轮由 RebootAgent / EventCheckAgent / StatusCheckAgent 三个角色智能体协同，并结合一份**策略提示词**做 loop-engineering 式的动态判断。

## 2. 总体架构与多智能体角色

MVP 形态 = 一个 pytest 会话。启动时从 `os.environ` 读取**策略提示词**与运行参数。一个 **Supervisor** 持有主循环，按轮驱动；每轮由三个**角色智能体**协同：

- **RebootAgent**：执行 `PUT /ISAPI/System/reboot` 发起重启。
- **EventCheckAgent**（重启后）：查询 `AcsEvent(major=3,minor=123)`，判定本次重启是否生成了远程重启事件。
- **StatusCheckAgent**（重启后）：查询 `AcsWorkStatus`，与**基准快照**比对，判定门禁状态是否未改变（doorLockStatus / doorStatus / wifiStatus 等）。

每轮流程：RebootAgent 重启 → Supervisor 轮询设备恢复（**自适应**：记录本轮恢复耗时，动态推算下一轮冷却间隔，设备慢则拉长）→ 恢复后 EventCheck + StatusCheck **并行**校验 → Supervisor 结合**策略提示词**做 loop-engineering 判断（默认：两 agent 均通过即本轮通过；策略可追加条件，如"连续重启 2 次后额外断言 wifi=connect"）→ 记录本轮结论 → 紧凑进入下一轮。

策略提示词以 `os.environ["BURNIN_STRATEGY"]` 注入，Supervisor 在进程开始时读取一次，作为整轮 run 的决策上下文；各 agent 每轮都拿到该上下文做判断。

## 3. 组件与配置契约（os.environ 注入）

pytest 启动时读取以下环境变量，测试人员（不碰代码）只改这些即可：

| 变量 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `BURNIN_STRATEGY` | str | 空 | 策略提示词（自然语言 + 可选结构化指令），Supervisor 全程用作决策上下文 |
| `BURNIN_MAX_ROUNDS` | int | ∞ | 总轮次上限 |
| `BURNIN_MAX_DURATION` | 秒 | ∞ | 总时长上限，到时优雅停止 |
| `BURNIN_BASE_INTERVAL` | 秒 | 60 | 下一轮冷却基础值 |
| `BURNIN_INTERVAL_MIN` | 秒 | 30 | 自适应间隔下界 |
| `BURNIN_INTERVAL_MAX` | 秒 | 600 | 自适应间隔上界 |
| `BURNIN_RECOVER_TIMEOUT` | 秒 | 180 | 设备未恢复即判失败 |
| `BURNIN_FAIL_THRESHOLD` | int | 5 | 累计失败达阈值→中止 |
| `BURNIN_FAIL_CONSECUTIVE` | int | 3 | 连续失败达阈值→中止 |

内部组件（pytest 内角色 agent，非独立进程）：
- `Supervisor`：主循环 + 自适应间隔推算 + 策略判断。
- `DeviceClient`：封装 digest 认证与三个 ISAPI 调用（reboot / AcsEvent / AcsWorkStatus），供 agents 复用。
- `Baseline`：首轮前抓取一次 `AcsWorkStatus` 作为基准快照。

## 4. 数据流与自适应间隔

单轮数据流：
1. 首轮前 `StatusCheckAgent` 抓基准 → 存 `Baseline`。
2. `RebootAgent` 调 reboot，记 `t_reboot`。
3. `Supervisor` 每 N 秒轮询一次可达性，直到恢复或超时 → 记 `t_recover`，得 `recover_time = t_recover - t_reboot`。
4. 恢复后 `EventCheckAgent` 与 `StatusCheckAgent` **并发**执行：
   - EventCheck：查 `AcsEvent` 最近一条 `major=3,minor=123` 时间是否在 `[t_reboot, t_recover+窗口]` 内 → 生成事件则通过。
   - StatusCheck：当前 `AcsWorkStatus` 与 `Baseline` 逐字段比对（默认白名单字段），无差异则通过；策略可追加字段/条件。
5. 两 agent 结果 + `BURNIN_STRATEGY` 交给 `Supervisor` 做最终判定。

**自适应间隔**：`next_interval = clamp(recover_time × k + base, MIN, MAX)`（k 默认 1.5，给恢复留余量）。设备恢复慢→间隔自动拉长，避免压垮；恢复快→紧凑连跑。每轮更新，无需人工调参。

> 实测基线：该设备重启约 30s 不可达、约 70s 恢复在线，可作为 `BASE_INTERVAL` / `k` 的初值参考。

## 5. 失败判定与中止策略

一轮"失败"的三种情况（任一即该轮失败）：
1. **未恢复**：超过 `BURNIN_RECOVER_TIMEOUT` 设备仍不可达 → 记失败，本轮不校验。
2. **无重启事件**：恢复后 `AcsEvent` 查不到本轮 `[t_reboot, t_recover+窗口]` 内的 `major=3,minor=123` → 重启未落日志，记失败。
3. **状态偏离**：`AcsWorkStatus` 与 `Baseline` 比对出现差异（如 wifi 从 connect 变 disconnect、doorLockStatus 改变）→ 记失败，并附差异字段。

**策略提示词的叠加**：`BURNIN_STRATEGY` 可追加条件（例如"连续重启 2 次后额外断言 wifi=connect"），Supervisor 解析后动态增删 StatusCheck 的断言项——这是 loop-engineering 的落点：策略随轮次演化判断逻辑，而非写死。

**中止**：累计失败数 ≥ `BURNIN_FAIL_THRESHOLD` **或** 连续失败 ≥ `BURNIN_FAIL_CONSECUTIVE` → Supervisor 优雅收尾（不再 reboot，等当前轮完成）→ 输出中止报告 + 告警（MVP 先打日志，预留 webhook 钩子）。否则跑到 `MAX_ROUNDS` 或 `MAX_DURATION` 自然结束。

## 6. 测试与落地（pytest 结构）

```
tests/
  conftest.py          # 读 os.environ → 构造 RunConfig + Baseline fixture
  test_burnin.py       # 主会话：Supervisor 主循环，每轮 yield 一个 pytest 用例结论
  agents/
    supervisor.py      # 主循环 + 自适应间隔 + 策略判定
    device_client.py   # digest + 三个 ISAPI 封装（复用已验证命令）
    reboot_agent.py
    event_check_agent.py
    status_check_agent.py
  strategy.py          # 解析 BURNIN_STRATEGY（NL + 结构化指令）
  report.py            # 累计统计 + 中止告警（日志/预留 webhook）
```

- 每轮映射为一个 pytest 检查点（通过/失败），整体天然产出 pytest 报告。
- MVP 不做前端/看板，仅 `os.environ` 注入 + 日志/pytest 报告。
- 三个 ISAPI 调用直接复用已验证的命令与"时间格式坑"经验（写入 `device_client.py`）。

## 7. 已验证的设备事实（来自前期实测）

- 摘要认证：`admin / 121212..`（`--digest`）。
- 重启端点 `PUT /ISAPI/System/reboot` 实测生效，返回 `statusCode 1 / OK`；约 30s 不可达、约 70s 恢复在线；重启会被记录为 `major=3, minor=123` 事件。
- `AcsEvent` 查询：时间字段**不可**用 `2025-07-13T00:00:00 08:00`（空格+时区），须用 `2025-07-13T00:00:00` 或 `+08:00` 格式，否则 `400 badJsonContent`。
- `AcsWorkStatus` 字段含义（单门设备实测）：`doorLockStatus:[0]`=已上锁、`doorStatus:[4]`=正常、`wifiStatus:"connect"`、`dualFrequencyModuleStatus:"offline"`、`InterfaceStatusList` 中 id1 断开 / id2 连通、`cardNum:0`。

## 8. 整合实现：分层架构与“以人为本”的多智能体团队（已落地）

经多轮讨论，最终落地的是**分层 + 总线驱动**的多智能体框架，而非把 LLM 直接塞进
每一轮的判定里。核心分层原则：

> **确定性的 Loop Core（可复现、低维护）必须与 LLM 分析智能体（策略层 / 政策层）
> 严格分离。** LLM 只在不重启每轮 pass/fail 的前提下，对“事故/突发情况”做政策级
> 决策；一旦 LLM 不可用（无 key / 网络/限流），整套拷机由规则引擎降级接管，绝不
> 因 LLM 失效而卡死或失能。

### 8.1 分层结构

```
┌──────────────────────────────────────────────────────────────┐
│ pytest harness（配置 + fixture + 薄测试入口，仅驱动，不含逻辑） │
├──────────────────────────────────────────────────────────────┤
│ 事件总线 EventBus（唯一通信通道：pub/sub + request/response）   │  ← 小组“作战白板”
├──────────────────────────────────────────────────────────────┤
│ 确定性的 Loop Core                                            │
│   Coordinator（重启→恢复监视→核对→自适应间隔→失败阈值中止）     │  ← 永不变的逻辑
├──────────────────────────────────────────────────────────────┤
│ 角色智能体（确定性，复用 DeviceClient）                        │
│   RebootAgent / WatchAgent / EventCheckAgent / StatusCheckAgent│
├──────────────────────────────────────────────────────────────┤
│ 策略层智能体（可插拔，LLM 优先 + 规则降级）                     │
│   AnalystAgent（事故决策 + 多角度分析）                        │
│   ScribeAgent（书记员：把总线信号整理成连贯叙事）              │
│   NotifierAgent（通知：中止/决策/告警，可插拔通道）            │
└──────────────────────────────────────────────────────────────┘
```

### 8.2 以人为本的团队角色

模拟“人不在场时，一小组如何协作处理拷机”的分工：

| 角色 | 职责 | 是否参与每轮 pass/fail |
|------|------|------------------------|
| RebootAgent | 执行远程重启 | 否（执行） |
| WatchAgent | 监视 DOWN→UP 完整恢复周期 | 否（监视） |
| EventCheckAgent | 核对重启事件是否落日志（major=3/minor=123） | 是（核对） |
| StatusCheckAgent | 比对门禁状态是否回归基线 | 是（核对） |
| Coordinator | 主驱动：调度+自适应间隔+失败阈值中止 | 是（确定性判定） |
| AnalystAgent | 事故决策（断电/超时等）+ 多角度稳定性分析 | **否（仅政策层）** |
| ScribeAgent | 实时记录“作战白板”叙事，供复盘 | 否 |
| NotifierAgent | 把中止/决策/告警推送给人 | 否 |
| ReporterAgent | 汇总各轮结果与告警 | 否 |

### 8.3 事故自治（用户离开后的灵活性）

当发生突发情况（如重启过程中设备断电、网络中断导致不再恢复），流程为：

1. WatchAgent 报告 `device/recovered` 且 `t_recover=None`（疑似断电）。
2. Coordinator 判定为**事故**，广播 `incident/raise` 并 `request('analyst/advise')`。
3. AnalystAgent 给出 `{continue, reason, source}`：
   - 有 LLM key → 调用 OpenRouter（`tencent/hy3:free`）做决策；
   - 无 key / 限流 / 超时 → **规则引擎**降级（断电类事故→停机，孤立异常→继续观察）。
4. Analyst 说“停止”→ Coordinator 中止整场拷机；说“继续”→ 确定性核心仍按失败阈值
   记账，绝不因 LLM 而绕过安全阈值。

> 关键：LLM 只拥有“停机”的政策权，没有“绕过安全阈值继续”的越权。即使 LLM 完全
> 不可用，Coordinator 的 `_consult_analyst` 超时后回退确定性逻辑，整场拷机照常运行。

### 8.4 安全约束

- OpenRouter key **只**从环境变量或仓库根 `.env` 读取（`.env` 已加入 `.gitignore`），
  绝不硬编码、绝不打印、绝不写入日志。
- `llm_client.py` 用标准库 `urllib` 实现，无第三方依赖；所有调用 30s 超时，失败静默
  降级。
- 规则引擎覆盖多场景（断电/网络中断、连续失败、偶发抖动），保证零 LLM 依赖也能可靠
  处理事故。

## 9. 落地文件清单

```
tests/
  conftest.py              # 读 os.environ → RunConfig + Baseline fixture
  test_burnin.py           # 主会话（总线驱动全团队）+ 策略层降级测试（不依赖设备）
  agents/
    config.py              # RunConfig / RoundResult / Baseline（含中文注释）
    device_client.py       # Digest + 三个 ISAPI 封装（searchID 随机）
    strategy.py            # 解析 BURNIN_STRATEGY
    report.py              # 累计统计 + 告警
    supervisor.py          # （早期实现，已演进为 bus 版本）
  harness/
    bus.py                 # 进程内异步事件总线（pub/sub + request/response）
    agent.py               # Agent 基类（可独立运行 / 可寻址 / 可单独被驱动）
    context.py             # RunContext + TaskBoard（共享上下文 + 共同清单）
    llm_client.py          # OpenRouter 客户端（仅 stdlib，key 只来自 env/.env）
    reboot_agent.py        # 执行重启
    watch_agent.py         # 监视 DOWN→UP 恢复周期
    event_check_agent.py   # 核对重启事件
    status_check_agent.py  # 比对状态基线
    reporter_agent.py      # 汇总 + 告警
    coordinator.py         # 确定性的 Loop Core（主驱动 + 事故咨询 Analyst）
    analyst_agent.py       # 策略层：LLM 决策 + 规则降级 + 多角度分析
    scribe_agent.py        # 书记员叙事
    notifier_agent.py      # 通知（可插拔通道）
    loader.py              # 依据 RunConfig 装配全部 Agent
```

