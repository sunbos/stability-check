# 海康门禁设备稳定性测试框架设计方案
## 基于三引擎自治循环框架的适配与演进

本方案基于通用自治循环框架 `stability-harness-loop-multiagent` 进行业务层适配，用于实现海康门禁设备的无人值守稳定性测试。

---

## 1. 架构映射与设计原则

整体遵循**三层解耦架构**：通用框架引擎零改动（除修复底层时序 Bug 外），业务适配完全收敛在 `adapters/` 中，用例层位于 `tests/`。

```
┌──────────────────────────────────────────────────┐
│  pytest 用例层 (tests/test_door_restart.py)        │
│  - 初始化与组装 (RunConfig / Context / Loop)     │
│  - 用例状态断言与报告整合                        │
└──────────────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────┐
│  业务适配层 (adapters/)                           │
│  - HikvisionAdapter (TargetAdapter 接口封装)      │
│  - DoorRestartWorker (WorkerAgent 核心业务流)     │
│  - 动态配置中心 (ConfigLoader 加载 YAML+环境变量)  │
└──────────────────────────────────────────────────┘
                      │ 
                      ▼ (EventBus 通信)
┌──────────────────────────────────────────────────┐
│  通用框架引擎 (stability_harness_loop_multiagent) │
│  - harness/ (事件总线、Watchdog 死锁检测、遥测)    │
│  - loop/ (ControlLoop 驱动、事实独裁裁决、中止策略)│
└──────────────────────────────────────────────────┘
```

### 核心不变量与设计约束：
1. **事实独裁**：`DecisionAuthority` 具备唯一裁决权。Worker 校验收集的二值事实（`facts`）中只要有任意一个 `False`，本轮判定必为 `fail`。
2. **防死锁/优雅降级**：所有接口调用必须包含超时时间。探活等待引入 `recover_timeout`（硬上限 180s），超时则认定设备未恢复。
3. **引擎隔离**：`adapters` 代码中绝不直接 import `harness` 或 `loop` 内部的具体类，所有跨引擎边界协作仅通过事件总线进行。

---

## 2. 通用框架核心时序修复

### 2.1 缺陷分析
当前框架的 `ControlLoop._run_round` 采用了**固定等待机制**：
```python
self.bus.publish("loop/tick", {"round": self._round_no})
await asyncio.sleep(max(self.recover_timeout, self.check_timeout)) # 固定等待
```
这在设备重启测试中是不合理的：
- 重启耗时通常在 30s ~ 180s 之间波动，若以 180s 作为最大容忍时间（`recover_timeout`），则每轮无论快慢都必须硬等满 180s，1000 轮测试将产生数十个小时 of 无效等待。
- 若设得太小，由于设备未完全启动就去查询，会导致事实校验因通信失败而产生误报（`fail`）。

### 2.2 解决方案（方案 A）
将 `ControlLoop._run_round` 改造为**事件驱动等待 + 最大容忍时间机制**：
1. 在发布 `loop/tick` 之前，订阅 Worker 的完成话题 `agent/#`。
2. 使用 `asyncio.wait_for` 等待该事件发生，并将 `timeout` 设为 `recover_timeout`（默认为 180s）。
3. 一旦 Worker 完成（发送 `agent/<role>/done`），`ControlLoop` 立即进入裁决并继续下一轮；若超时未返回，则抛出异常并降级进入事实合并（缺失的 `checked` 事实会导致本轮失败）。

---

## 3. 分阶段演进路径

为了降低首期交付风险，项目将分三个阶段进行演进：

### Phase 1：MVP 核心跑通（当前重点）
* **目标**：以最简路径跑通「开门+重启」长稳测试。
* **架构**：不注册任何 `Advisor` 智能体。仅通过 `DoorRestartWorker` 串行执行 `do_work` (开门+重启) -> `recover` (探活) -> `check` (事件与状态校验)。
* **裁决方式**：`DecisionAuthority` 仅根据 Worker 返回的二值事实做出 `pass` 或 `fail` 裁决。
* **循环前准备**：
  1. 调用 ISAPI 校时接口对设备进行时钟同步。
  2. 记录设备的基础工作状态（如通道状态、读卡器在线数）存入 `baseline`。
  3. 执行一次探测性重启，实测设备首次重启耗时，作为 `baseline_recover_time` 存入上下文。

### Phase 2：劣化趋势分析（内置规则）
* **目标**：在测试执行中实时发现累积性劣化（如重启响应变慢）。
* **架构**：注册内置的规则顾问（如 `TrendSupervisorAgent`）。
* **业务逻辑**：订阅每轮 `loop/done` 事件，监控 `recover_time` 趋势。如果发现连续 $N$ 轮实际重启时长超过 `baseline_recover_time` 的 $1.5$ 倍，则发出 `agent/vote/reply` 投出高风险值（如 85分）。
* **裁决方式**：虽然设备重启上线且事件齐全（事实为 True），但由于高风险分，`DecisionAuthority` 将本轮标记为 `warn` 或触发 `recheck`，防止测试在设备发生"变慢半宕机"状态下依然乐观 pass。

### Phase 3：LLM 驱动的智能 Advisor（自然语言指令支持）
* **目标**：支持前端通过环境变量 `STABILITY_INSTRUCTION` 传入自然语言测试要求。
* **架构**：注册 `LLMAdvisorAgent`。
* **业务逻辑**：
  1. 环境变量传入自然语言要求（例如："*注意检查事件是否延迟产生，如果重启耗时比基准高一倍或者开门后5秒都没有事件，请标记风险并输出告警*"）。
  2. LLM 顾问启动时用 LLM 解析指令并提取判定规则。
  3. 周期性（如每 10 轮）提取历史 Round 记录，调用 LLM 评估是否符合用户约束，并投出风险评分和原因。
* **优雅降级**：若 LLM 接口调用超时或不可用，`LLMAdvisorAgent` 会选择弃权（`confidence=0`），系统降级为纯事实验证，绝不会卡死主循环。

---

## 4. MVP 场景核心流程设计

以「开门 + 重启」稳定性场景为例，单轮循环内的逻辑映射如下：

```
                    【ControlLoop 发送 loop/tick】
                                 │
                                 ▼
         ┌──────────────────────────────────────────────┐
         │ 1. do_work() 阶段                             │
         │ - 调用 /ISAPI/AccessControl/RemoteOpenDoor    │
         │   下发开门指令                               │
         │ - 等待 3s (事件落库延迟)                      │
         │ - 校验是否有对应的开门事件产生并记录结果       │
         │ - 调用 /ISAPI/System/reboot 接口触发重启     │
         │ - 记录本次动作结果 (ok=True/False)            │
         └──────────────────────┬───────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │ 2. recover() 阶段                            │
         │ - 等待静默期（例如 30s，避开最初离线阶段）    │
         │ - 循环探活：轮询 /ISAPI/AccessControl/...    │
         │   查询设备在线状态（最大容忍时间 180s）      │
         │ - 连续 2 次查询成功，视作恢复上线并记录时长   │
         │ - 预热等待 60s，等待设备内部服务初始化完毕    │
         └──────────────────────┬───────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │ 3. check() 阶段                              │
         │ - 复核状态：获取当前读卡器、模块在线数量，与  │
         │   baseline 比较，必须完全一致                 │
         │ - 查询重启事件：在重启开始至上线后的时间窗口内│
         │   查询 ISAPI 事件日志，必须存在重启事件       │
         │ - 组装二值事实字典返回                       │
         └──────────────────────┬───────────────────────┘
                                │
                                ▼
         【Worker 发送 agent/<role>/done → 触发 Loop 裁决】
```

### Worker 返回事实字典定义：
```python
{
    "door_command_accepted": True, # 开门协议下发成功
    "door_event_recorded": True,   # 开门事件成功落库
    "device_reboot_triggered": True,# 重启协议下发成功
    "device_recovered": True,      # 设备能在 180s 内恢复上线
    "reboot_event_recorded": True, # 重启事件成功落库
    "hardware_state_consistent": True, # 状态参数与循环前 baseline 一致
}
```

---

## 5. 配置中心设计 (YAML & Env)

`configs/door_restart_stability.yaml` 基准配置文件结构：

```yaml
device:
  host: "192.168.1.100"
  port: 80
  username: "admin"
  password: "password123"
  http_timeout: 5

loop:
  mode: "count"
  total_rounds: 1000
  round_interval: 2
  consecutive_failure_threshold: 10
  # 重启探测与容忍参数
  max_recover_timeout: 180       # 最大容忍时间 (秒)
  probe_interval: 5              # 探活时间步长
  warmup_time: 60                # 上线预热时长

event:
  door_open_delay: 3             # 开门后到查询事件的延迟 (秒)
  query_retry: 3                 # 事件查询重试次数
  query_retry_interval: 5        # 重试间隔
```

### 环境变量优先级规则：
优先级依次为：**环境变量 (`os.env`) > YAML 配置文件 > 代码内置默认值**。
环境变量匹配规则：`STABILITY_{YAML节点大写}`。例如：
- `STABILITY_DEVICE_HOST` 覆写 `device.host`
- `STABILITY_LOOP_TOTAL_ROUNDS` 覆写 `loop.total_rounds`
- `STABILITY_INSTRUCTION` 用作 Phase 3 的自然语言规则传入。
