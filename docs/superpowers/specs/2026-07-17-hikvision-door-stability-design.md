# 海康门禁设备稳定性测试框架设计方案
## 基于三引擎自治循环框架的智能自愈与多智能体演进

本方案基于通用自治循环框架 `stability-harness-loop-multiagent` 进行业务层适配，用于实现海康门禁设备具备“自主诊断与自愈能力”的无人值守长稳测试。

---

## 1. 架构映射与“真·多智能体”自治自愈设计

为了充分发挥多智能体的自治性，我们拒绝单一的“脚本化执行”，而是将**测试执行、异常诊断、策略决策、自愈恢复**拆解为多个协同工作的智能体。

```
                    ┌────────────────────────┐
                    │      ControlLoop       │ ◄──────┐
                    │ (事实独裁 + 决策判断)  │        │
                    └───────────┬────────────┘        │
                                │ loop/tick           │
                                ▼                     │
                    ┌────────────────────────┐        │
                    │   LLM Worker Agent     │        │
                    │ (动作执行 + 事实收集)  │        │
                    └─────┬────────────▲─────┘        │
             target/      │            │              │
             checked      │            │ 诊断/恢复     │ agent/
             (Facts)      │ 异常触发   │ 指令         │ vote/reply
                          ▼            │              │ (加权风险分)
                    ┌──────────────────┴─────┐        │
                    │  LLM Diagnostic Agent  │        │
                    │   (异常分析与自愈者)   │        │
                    └───────────┬────────────┘        │
                                │                     │
                                ▼                     │
                    ┌────────────────────────┐        │
                    │   LLM Advisor Agent    │ ───────┘
                    │ (自然语言指令评估)     │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │   Stability Scribe     │
                    │ (仅观察/ScribeObserver) │
                    └────────────────────────┘
```

### 智能体角色定义与自治职责：

| 智能体名称 | 核心职责 | 自治性体现 (LLM / 规则驱动) |
|---|---|---|
| **LLM Worker Agent** | 执行测试指令（如开门、重启），收集基础事实（设备状态、事件是否存在）。 | **动作自治**：解析 `STABILITY_INSTRUCTION`，根据当前设备状态动态决定下一步操作（而非硬编码顺序）。 |
| **LLM Diagnostic Agent** | **自愈核心**：当 Worker 报告异常事实（如“事件未找到”）时介入，分析原因并执行自愈操作。 | **自愈自治**：当发生异常时，自动分析事实（例如比对设备时间与系统时间，检测网络延迟），生成诊断方案（如“执行校时后重新触发/重新查询”），执行自愈并反馈结果。 |
| **LLM Advisor Agent** | 评估长周期运行是否符合用户自然语言定义的期望。 | **规则自治**：评估趋势（如重启时间逐渐变慢）和隐性要求，对整体风险进行打分。 |
| **ControlLoop** | 协调生命周期，判定最终 Verdict，执行连续失败熔断。 | **控制自治**：提供事实独裁安全网，确保即使 LLM 尝试自愈失败，也能安全终止测试。 |
| **Stability Scribe** | 订阅所有 `loop/done` 与 `agent/incident` 事件并落库归档。 | 纯观察型，无决策行为。 |

---

## 2. 核心自愈流程设计 (异常自治处理)

### 2.1 常见异常的自治自愈细节设计

#### 场景 A：查询不到事件 (以开门事件为例)
当 Worker 执行 `RemoteOpenDoor` 后查询不到事件时，触发以下自愈流：
1. **时间偏差诊断**：
   - 触发 `GET /ISAPI/System/time` 获取设备当前时间 `device_time`。
   - 获取测试机当前时间 `host_time`。
   - 计算绝对时间差：$\Delta t = |device\_time - host\_time|$。
2. **时钟对齐自愈**：
   - 如果 $\Delta t > 3s$（`time_skew_threshold`），判定原因为时钟不一致导致查询的时间窗口漂移。
   - 调用 `PUT /ISAPI/System/time` 将设备时间同步为 `host_time`。
3. **重新验证**：
   - 通知 Worker 再次执行 `RemoteOpenDoor`，并使用修正后的时间窗口重新查询事件。
   - 若重新查询成功，将该事实标记为 `True`（表示通过），但在 `RoundRecord` 中备注 `self_healed: time_sync` 并记录调整幅度。

#### 场景 B：设备突然掉线 (无响应)
当 API 调用返回 ConnectionError 或 Timeout 时：
1. **连通性诊断**：
   - 检查本地测试机网卡状态（能否 ping 通网关）。
   - 向设备发送 ICMP Ping：
     - 若 ping 通但 HTTP 80/HTTPS 443 连不上，说明网络可达，但设备 ISAPI 服务卡死或设备在进行热重启。
     - 若 ping 不通，判定为物理掉线。
2. **掉线自愈**：
   - 如果是测试机本身断网，静默挂起并以 10s 步长轮询网卡状态，直至网络恢复。
   - 如果是设备网络异常且该用例包含重启步骤，则等待 `max_recover_timeout`；如果用例不包含重启，立即触发重连重试。

---

## 3. 门禁设备 HTTP ISAPI 核心通信细节

### 3.1 认证机制 (重要元信息)
海康门禁设备所有的 HTTP 接口均要求 **HTTP Digest Authentication（摘要认证）**。
- 业务适配层在初始化 `HikvisionClient` 时，必须使用支持 Digest Auth 的客户端（如 Python `requests.auth.HTTPDigestAuth`，或异步库 `httpx` 的 DigestAuth 插件）。

### 3.2 核心 ISAPI 协议接口定义

#### 1. 远程开门接口
* **协议**：`PUT /ISAPI/AccessControl/RemoteOpenDoor/1?format=json`
* **说明**：数字 `1` 代表通道号（门号）。

#### 2. 重启接口
* **协议**：`PUT /ISAPI/System/reboot?format=json`

#### 3. 校时接口
* **查询协议**：`GET /ISAPI/System/time?format=json`
  - 返回报文示例：
    ```json
    {
      "Time": {
        "localTime": "2026-07-17T02:18:00+08:00",
        "timeZone": "CST-8:00"
      }
    }
    ```
* **设置协议**：`PUT /ISAPI/System/time?format=json`
  - 载荷示例：与查询返回结构一致。

#### 4. 工作状态接口
* **协议**：`GET /ISAPI/AccessControl/AcsWorkStatus?format=json`
  - 用于探活和子模块在线状态复核（如读卡器数量 `cardReaderOnlineStatus`、输入输出模块状态 `IOModuleOnlineStatus`）。

#### 5. 事件查询接口
* **协议**：`POST /ISAPI/AccessControl/AcsEvent?format=json`
* **载荷结构**：
  ```json
  {
    "AcsEventCond": {
      "searchID": "unique-uuid-string",
      "searchResultPosition": 0,
      "maxResults": 30,
      "startTime": "2026-07-17T02:10:00+08:00",
      "endTime": "2026-07-17T02:15:00+08:00",
      "major": 5,
      "minor": 75
    }
  }
  ```
  - 注：`major` 和 `minor` 代表主次事件类型（如：major=5 代表门禁事件，minor=75 代表合法卡开门）。

---

## 4. 关键设计细节与规避

### 4.1 探活“连续 2 次成功”保障机制
为了防止设备重启时由于网络服务初始化过程中的 TCP 端口短暂 Bind 成功导致误报在线，探活必须满足：
1. 检测到状态接口 `/ISAPI/AccessControl/AcsWorkStatus` 返回 HTTP 200。
2. 间隔 `probe_interval`（如 5 秒）后，第二次调用依然返回 HTTP 200。
3. 框架支持 `probe_confirm_count` 配置项，默认值为 `2`，允许设为 `1`。

### 4.2 探测性重启触发约束
- 只有在用例配置文件中 `loop.run_reboot` 设为 `True`，或者测试用例标记为 `reboot_stability` 时，才会在 `pytest` 的 session 准备阶段执行首次重启探测。
- 探测耗时会以 `baseline_recover_time` 变量注入 `SharedContext`。

---

## 5. 自然语言指令驱动与执行机制

前端传入 `STABILITY_INSTRUCTION` 时，大模型在各环节的指导行为如下：

### 5.1 解析期：
在用例启动时，`LLMAdvisorAgent` 调用大模型（如 Gemini 3.5 Flash）解析用户的自然语言。
* **Prompt 模板**：
  ```
  你是一个海康门禁设备稳定性测试规划器。请解析用户的指令：
  "{{ STABILITY_INSTRUCTION }}"
  
  输出为 JSON 格式：
  {
    "skip_reboot": bool,          # 是否跳过重启步骤
    "event_check_delay_adjust": int, # 事件落库等待调整 (秒)
    "trigger_interval_adjust": int,  # 动作触发间隔调整 (秒)
    "diagnose_whitelist": [str]   # 允许的自愈类型，如 ["time_sync", "retry"]
  }
  ```
* 提取后的参数直接覆盖 `test_config` 对应字段，动态修改 Worker 执行流程。

### 5.2 诊断期 (LLM 异常诊断)：
当 `LLM Diagnostic Agent` 被触发时，如果自愈开关 `enable_self_healing` 为 `True`，它将：
1. 收集设备当前信息（如时间差、网络丢包率、错误 HTTP 状态码）。
2. 将环境数据连同指令发给 LLM。
3. LLM 决定采用哪种自愈子流程（TimeSync / WaitNetwork / ReTrigger / Abort）。
4. 即使大模型生成了错误的诊断方案，`ControlLoop` 的事实独裁机制会在下一次 tick 检验中拦截，确保系统不会脱缰。

---

## 6. 配置中心规范 (configs/door_restart_stability.yaml)

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
  run_reboot: true               # 是否执行重启操作
  max_recover_timeout: 180       # 最大容忍时间 (秒)
  probe_interval: 5              # 探活时间步长
  probe_confirm_count: 2         # 探活确认次数
  warmup_time: 60                # 上线预热时间

event:
  door_open_delay: 3             # 开门后到查询事件的延迟 (秒)
  query_retry: 3                 # 事件查询重试次数
  query_retry_interval: 5        # 重试间隔

autonomy:
  enable_self_healing: true      # 开启自治自愈
  diagnostic_llm_model: "gemini-3.5-flash"
  instruction: ""                # 对应 STABILITY_INSTRUCTION 环境变量
```
