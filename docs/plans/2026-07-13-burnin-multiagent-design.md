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
