# 海康门禁设备稳定性测试框架设计方案
## 基于三引擎自治循环框架的智能自愈与多智能体演进

本方案基于通用自治循环框架 `stability-harness-loop-multiagent` 进行业务层适配，用于实现海康门禁设备具备“自主诊断与自愈能力”的无人值守长稳测试。

---

## 1. 架构映射与“真·多智能体”自治自愈设计

为了充分发挥多智能体的自治性，我们拒绝单一的“脚本化执行”，而是将**测试执行、异常诊断、策略决策、自愈恢复**拆解为多个协同工作的智能体。

> **架构对齐说明（重要）**：框架只有三种 Agent 角色——`WorkerAgent`（执行 act/recover/check）、`AdvisorAgent`（投票 + 提事件，**绝不执行操作**）、`ObserverAgent`（记录/通知）。所谓“LLM Diagnostic Agent”**不是独立 Agent**，而是 `HikvisionWorker` 内部由 LLM 驱动的**诊断内核**：自愈执行落在 Worker 的 `recover()` 内（框架字面“恢复”语义），LLM 仅在内核内被调用以选择自愈子流程。这样既保留“LLM 诊断”的自治性，又不破坏三角色边界与事实独裁安全网。

```
                    ┌────────────────────────┐
                    │      ControlLoop       │ ◄──────┐
                    │ (事实独裁 + 决策判断)  │        │
                    └───────────┬────────────┘        │
                                │ loop/tick           │
                                ▼                     │
                    ┌────────────────────────┐        │
                    │   HikvisionWorker      │        │
                    │  (Agent role=worker)   │        │
                    │  ┌──────────────────┐  │        │
                    │  │ do_work  (执行)  │  │        │
                    │  │ recover  (自愈) ◄┼──┼── LLM 诊断内核（选择子流程）│
                    │  │ check    (断言)  │  │        │
                    │  └──────────────────┘  │        │
                    └─────┬──────────────────┘        │
             target/checked, target/recovered (Facts) │
                          │                           │
                          ▼                           │
                    ┌────────────────────────┐        │
                    │   LLM Advisor Agent    │ ───────┘
                    │ (投票 + incident)      │
                    │  agent/vote/reply      │
                    │  agent/incident        │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │   Stability Scribe     │
                    │ (ObserverAgent/Scribe) │
                    └────────────────────────┘
```

### 智能体角色定义与自治职责：

| 智能体名称 | 框架角色 | 核心职责 | 自治性体现 (LLM / 规则驱动) |
|---|---|---|---|
| **HikvisionWorker** | `WorkerAgent` | 执行测试指令（开门、重启）、事件查询断言、**自愈执行**（时钟对齐/重连重试在 `recover()` 内完成）。 | **动作自治**：根据当前设备状态与事件缺失组合动态决定下一步；`recover()` 内调用 LLM 诊断内核选择自愈子流程。 |
| └ LLM 诊断内核 | Worker 内部子模块（非独立 Agent） | 当 `check()` 报告异常事实时，由 `recover()` 调用，分析原因并建议自愈子流程。 | **自愈自治**：LLM 基于时间差/网络丢包/HTTP 错误码生成方案（TimeSync/WaitNetwork/ReTrigger/Abort），**由 Worker 执行**，绝不绕过 Worker 直接操作设备。 |
| **LLM Advisor Agent** | `AdvisorAgent` | 评估长周期运行是否符合用户自然语言期望；投 `(risk, confidence)` 票；提 `agent/incident` 事件。 | **规则自治**：评估趋势（如重启时间变慢）与隐性要求；**只投票/提事件，绝不执行操作、绝不裁决**。 |
| **ControlLoop** | （Loop 引擎） | 协调生命周期，判定最终 Verdict，执行连续失败熔断。 | **控制自治**：事实独裁安全网，即使 Worker 自愈失败也能安全终止。 |
| **Stability Scribe** | `ObserverAgent`（映射框架 `ScribeAgent`） | 订阅 `loop/done` 与 `agent/incident` 事件并落库归档。 | 纯观察型，无决策、无执行。 |

---

## 2. 核心自愈流程设计 (异常自治处理)

### 2.1 常见异常的自治自愈细节设计

#### 场景 A：查询不到事件 (以开门事件为例)
当 Worker 执行 `RemoteOpenDoor` 后查询不到事件时，**先按事件结构区分缺失类型，再走不同自愈分支**：

##### A.0 缺失类型判定（前置）
依据 3.2 节事件结构本质，远程开门的完整事件链为 3 个事件：远程开门 `(major=3, minor=1024)` → 门锁打开 `(5,21)` → 门锁关闭 `(5,22)`。Worker 在 `check()` 中分别断言，自愈流按缺失组合分流（门锁关闭作为**软事实**，默认不阻塞通过）：

| 远程开门 (3,1024) | 门锁打开 (5,21) | 门锁关闭 (5,22) | 判定与自愈分支 |
|:---:|:---:|:---:|---|
| ✓ | ✓ | ✓ | 正常通过 |
| ✓ | ✓ | ✗ | 门开了未关 → 软事实 `warn`（门可能被保持开启），不自愈 |
| ✓ | ✗ | ✗ | 指令已接收但门锁未动作（机械/电气问题）→ 走 **A.4** 重发开门指令一次；仍失败报 `critical` |
| ✗ | ✓ | ✓ | 异常：门开了但远程指令事件未落库 → 报 `critical`，不自愈（落库机制异常） |
| ✗ | ✗ | * | 设备未响应指令，或时钟漂移导致查询窗口错位 → 走 **A.1** 时钟偏差诊断 → **A.2** 对齐 → **A.3** 重发并重查 |

> **门锁关闭 (5,22) 为软事实**：长稳测试中门可能被人为保持开启，默认 `lock_closed` 不纳入事实独裁，仅由 Advisor 投风险分；如需强制验证可在 `event.expected_events` 保留 `lock_close` 并设 `enforce_lock_close: true`。

##### A.1 时间偏差诊断（主+设备均缺失时触发）
1. 触发 `GET /ISAPI/System/time` 获取设备当前时间 `device_time`。
2. 获取测试机当前时间 `host_time`。
3. 计算绝对时间差：$\Delta t = |device\_time - host\_time|$。

##### A.2 时钟对齐自愈
- 如果 $\Delta t > 3s$（`time_skew_threshold`），判定原因为时钟不一致导致查询的时间窗口漂移。
- 调用 `PUT /ISAPI/System/time` 将设备时间同步为 `host_time`。
- **副作用约束**：时钟同步会让设备事件时间戳跳变，可能干扰后续事件查询窗口计算。故限制：每轮最多同步 1 次，全 run 最多 `max_time_sync_per_run`（默认 3）次；同步后以 `device_time` 重新校准查询窗口起点，而非沿用 `host_time`。

##### A.3 重新验证
- 通知 Worker 再次执行 `RemoteOpenDoor`，并使用修正后的时间窗口重新查询事件。
- 若重新查询成功，将相关事实标记为 `True`（表示通过）。

##### A.4 设备动作缺失自愈（仅门锁打开事件缺失时触发）
- 重发一次 `RemoteOpenDoor` 指令，等待 `door_open_delay` 后重查“门锁打开”事件。
- 仍缺失则通过 `agent/incident`（severity=`critical`）上报，由 `ControlLoop` 强制 `recheck` 或终止。

##### A.5 自愈结果的事实/记录承载（重要）
框架的 `RoundRecord` 是 frozen dataclass，**无 `self_healed` 字段**，不可直接扩展（属 Loop 引擎改动，需登记为框架扩展点）。自愈元信息按以下方式承载：

- **事实层**：自愈成功后，Worker 必须同时重发 `target/checked`（更新事件事实为 `True`）**和** `target/recovered`（`recovered=True`）。后者尤为关键——`ControlLoop` 会将 `recovered=False` 作为事实交给 `DecisionAuthority`，若不重发，自愈“白做”，事实独裁仍判 `fail`。
- **备注层**：将自愈类型与调整幅度塞进 `facts` 字典的非布尔槽位（非 bool truthy 值不触发事实独裁 fail）：
  ```python
  facts["event_<minor>_found"] = True          # bool 事实，参与独裁
  facts["self_healed"] = {                      # dict 元信息，truthy 但非 bool，不触发 fail
      "type": "time_sync",
      "delta": 4.2,
      "round": tick["round"],
  }
  ```
- Observer（`ScribeAgent`）订阅 `loop/done` 时读取 `facts["self_healed"]` 落库归档。

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

### 3.1 认证机制与依赖分层 (重要元信息)

#### 3.1.1 认证机制
海康门禁设备所有的 HTTP 接口均要求 **HTTP Digest Authentication（摘要认证）**。

#### 3.1.2 依赖分层原则
框架本体 `stability_harness_loop_multiagent/` 严格保持**零第三方依赖**（仅使用标准库 `asyncio` / `urllib` / `json` 等），这一约束已在 `AGENTS.md` / `CLAUDE.md` 中固化，**业务适配层同样不引入第三方包**——海康 ISAPI 的 HTTP Digest 认证使用标准库 `urllib.request.HTTPDigestAuthHandler` 实现（与 `master` 分支 `tests/agents/device_client.py` 一致），LLM 调用使用标准库 `urllib.request` 实现 OpenAI 兼容 chat-completions 协议（与 `master` 分支 `tests/harness/llm_client.py` 一致）。这样整个项目**零第三方运行时依赖**，可审计性最强：

| 层 | 依赖策略 | 说明 |
|---|---|---|
| **框架本体** | `dependencies = []`（零依赖） | 可移植性 / 可审计性卖点，不动 |
| **业务适配层**（海康） | 标准库 `urllib.request` + `HTTPDigestAuthHandler` | 与 `master` 一致，零依赖 |
| **LLM 适配层** | 标准库 `urllib.request` + OpenAI 兼容协议 | 与 `master` 一致，零依赖 |
| **开发/测试** | `[dev]` extra 提供 `pytest` / `pytest-asyncio` | 不进运行时 |

#### 3.1.3 async 策略（关键）
框架核心（`EventBus` / `ControlLoop` / `Agent.handle`）已深度绑定 `asyncio`（标准库，非第三方），**Agent 层面 async 是强制的**。业务层 IO 必须遵循以下档位，**严禁在 `act` / `handle` 中直接同步阻塞**，否则会卡死 event loop 并使 `ControlLoop` 的事实收集超时安全网失效：

| IO 类型 | 推荐写法 | 示例 |
|---|---|---|
| 短 IO（<100ms） | 同步放 `do_work` / `check`（这两个是同步方法） | `subprocess.run(["ping", ...])` |
| 长 IO（HTTP / LLM，秒级） | 同步实现 + `await asyncio.to_thread(fn, *args)` 包装 | `await asyncio.to_thread(client.query_events, ...)` |
| 遗留同步 SDK | `await asyncio.to_thread(fn, *args)` 包装 | `await asyncio.to_thread(sync_sdk.call, ...)` |

业务适配层 `HikvisionClient` 使用标准库 `urllib.request` + `HTTPDigestAuthHandler` 实现同步 Digest 认证（`master` 分支 `tests/agents/device_client.py` 已验证）；跨 major 事件查询的并行性由 `HikvisionWorker.recover()`（async）用 `asyncio.gather` + `asyncio.to_thread` 包装同步 `client.query_events` 实现。LLM 调用同理：`master` 分支 `tests/harness/llm_client.py` 已用纯标准库 `urllib.request` 实现 OpenAI 兼容 chat-completions，业务层在 async 入口用 `asyncio.to_thread` 包装即可。

#### 3.1.4 `pyproject.toml` 草案
```toml
[project]
name = "stability-harness-loop-multiagent"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = []  # 整个项目零第三方运行时依赖（框架 + 业务层均只用标准库）

[project.optional-dependencies]
dev = ["pytest>=8", "pytest-asyncio>=0.23"]  # 仅开发/测试

[tool.pytest.ini_options]
asyncio_mode = "auto"
```
安装语义：`pip install .`（运行时零依赖） / `pip install .[dev]`（开发）。无 `[hikvision]` / `[llm]` extra——业务层与 LLM 层均用标准库实现。

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
* **关键约束**：一次查询只能指定**一个** `(major, minor)` 组合点（`major` 必填、`minor` 可省略表示该 major 下全部子事件），**不支持跨 major 一次拉全量**。远程开门事件链跨 major（见下表），故需多次查询。
* **载荷结构**（以远程开门主事件为例，`major=3, minor=1024`）：
  ```json
  {
    "AcsEventCond": {
      "searchID": "09fddf6c2c8f400d829db038ea8e1726",
      "searchResultPosition": 0,
      "maxResults": 24,
      "major": 3,
      "minor": 1024,
      "startTime": "2026-07-17T03:20:00+08:00",
      "endTime": "2026-07-17T03:25:00+08:00",
      "timeReverseOrder": true
    }
  }
  ```
* **返回结构**（`AcsEvent.InfoList[]`，每条含 `major/minor/time/doorNo/serialNo`）：
  ```json
  {
    "AcsEvent": {
      "searchID": "09fddf6c2c8f400d829db038ea8e1726",
      "totalMatches": 3,
      "responseStatusStrg": "OK",
      "numOfMatches": 3,
      "InfoList": [
        {
          "major": 3, "minor": 1024,
          "time": "2026-07-17T03:22:56+08:00",
          "remoteHostAddr": "192.168.3.20",
          "doorNo": 1, "serialNo": 6585
        }
      ]
    }
  }
  ```
  - `serialNo` 为流水号，可用于关联同一开门周期内的多个事件。
  - `timeReverseOrder: true` 按时间倒序返回，便于取最新事件。

##### 事件结构本质（关键认知）
一次业务动作产生的事件由 **「外界/协议触发事件」+「可选的设备动作触发事件」** 两类来源组成。设备动作又分**主动**（由前序事件导致）与**被动**（设备自身状态变化）。各事件是独立的 `(major, minor)` 组合，**可能跨 major**：

| 业务动作 | 事件链 | major | minor | 触发类型 |
|---|---|---|---|---|
| 远程开门 | 远程开门 | 3 | 1024 | 外界/协议触发（`remoteHostAddr` 记录发起端） |
|  | └ 门锁打开 | 5 | 21 | 设备动作·主动（由开门指令导致） |
|  | └ 门锁关闭 | 5 | 22 | 设备动作·被动（门自动关闭时产生，非指令触发） |
| 人脸认证开门 | 人脸认证通过 | 5 | 75 | 外界/协议触发 |
|  | └ 门锁打开 | 5 | 21 | 设备动作·主动 |
|  | └ 门锁关闭 | 5 | 22 | 设备动作·被动 |
| 远程登录 | 远程登录 | — | — | 仅外界/协议触发，无设备动作事件 |

> **要点**：
> ① 事件分两类来源——**外界/协议触发**（远程开门指令、人脸认证）与**设备动作触发**（门锁打开/关闭）；
> ② 设备动作再分**主动**（紧跟前序事件，如门锁打开紧跟开门/认证通过，时间强相关，可作强事实）与**被动**（门机械/定时自动关闭时产生，时间不确定，宜作软事实）；
> ③ 远程开门的外部触发事件 **major=3**，而门锁动作事件 **major=5**，**跨 major**，无法用一次查询拉全；
> ④ 设备动作事件是**可选**的，并非所有业务动作都触发（如远程登录只有外界/协议触发事件）。

##### 查询策略（方案 D：多 major 并行查询 + 集合断言）
方案 C（省略 `minor` 单次拉全）因跨 major 不可行。采用：
1. Worker 对事件链中**每个** `(major, minor)` 组合发起一次查询；
2. 用 `asyncio.gather` **并行**发起，摊平延迟（3 次查询 ≈ 1 次 RTT）；
3. 按"每个期望事件在各自信道内存在"断言，可用 `serialNo` / 时间窗关联同一开门周期。

> **落点说明**：`WorkerAgent.check()` 默认是同步方法（见 [workers/base.py](file:///c:\Users\14435\Desktop\stability-check\stability_harness_loop_multiagent\multi_agent\workers\base.py)），而事件查询是 async HTTP。故将 async 查询放在 `recover()`（async）内完成并缓存结果，`check()` 同步读取缓存断言——既遵守框架的同步 `check` 约定，又能用 async 查询。

```python
class HikEventCode:
    REMOTE_OPEN = (3, 1024)   # 远程开门（外部触发）
    LOCK_OPEN   = (5, 21)     # 门锁打开（设备动作）
    LOCK_CLOSE  = (5, 22)     # 门锁关闭（设备动作）
    FACE_PASS   = (5, 75)     # 人脸认证通过（外界/协议触发）

# HikvisionWorker 内部
async def recover(self, tick: dict) -> bool:
    # 并行查询事件链上的每个 (major, minor) 组合
    w = (tick["window_start"], tick["window_end"])
    trigger, opened, closed = await asyncio.gather(
        self._query_events(*w, *HikEventCode.REMOTE_OPEN),
        self._query_events(*w, *HikEventCode.LOCK_OPEN),
        self._query_events(*w, *HikEventCode.LOCK_CLOSE),
    )
    self._last_events = {"trigger": trigger, "opened": opened, "closed": closed}
    return True  # 恢复标记由自愈逻辑决定，见 2.1

def check(self, tick: dict) -> dict:
    ev = self._last_events
    return {
        "remote_open_triggered": len(ev["trigger"]) > 0,
        "lock_opened": len(ev["opened"]) > 0,
        "lock_closed": len(ev["closed"]) > 0,
    }
```

所有期望事件事实均为 `True` 才算通过——由 `DecisionAuthority` 的事实独裁自动 enforce。门锁关闭事件是否纳入断言可配置（长稳测试中门可能被人为保持开启，`lock_closed` 默认设为软事实或可关闭）。

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

前端传入 `BURNIN_STRATEGY` 时（沿用 `master` 分支 `tests/agents/config.py` 的环境变量约定），大模型在各环节的指导行为如下：

### 5.1 指令投递机制（架构补齐）
框架中 `SharedContext.strategy_text` **不会自动下发给 Agent**（`ReadOnlyContext` 无人主动 push，Advisor 只订阅 `loop/done` / `loop/vote/request`）。故 `BURNIN_STRATEGY` 需显式投递，采用双路径：

| 路径 | 机制 | 用途 |
|---|---|---|
| **主路径·构造注入** | 业务层创建 `HikvisionAdvisor` 时经构造参数 `instruction=...` 注入（runner 从 `os.environ.get("BURNIN_STRATEGY", "")` 读取） | 启动时已知的静态指令，最简单直接 |
| **辅路径·总线广播** | `ControlLoop` 启动时 `publish("loop/strategy", {"text": strategy_text})` 一次性广播；需要的 Agent 在 `AgentSpec.subscriptions` 加 `loop/strategy` | 多 Agent 共享同一指令、或指令需动态刷新 |

> Advisor 解析出的 plan 参数（`skip_reboot` 等）需回传影响 Worker 执行流程，但 Agent 之间禁止直接通信，必须走 EventBus：Advisor 解析完后 `publish("hikvision/plan", plan_dict)`，Worker 在 `subscriptions` 加 `hikvision/plan` 订阅并缓存到私有 `state`，`do_work` / `recover` 读取缓存。

### 5.2 解析期：
在用例启动时，`HikvisionAdvisor`（经 5.1 主路径拿到 `instruction`）调用大模型解析用户的自然语言。**LLM 客户端复用 `master` 分支 `tests/harness/llm_client.py`**——纯标准库 `urllib.request` 实现，默认指向 OpenRouter 免费模型 `tencent/hy3:free`，API key 从环境变量 `LLM_API_KEY` / `OPENROUTER_API_KEY` 或仓库根 `.env` 读取（无 key 时返回 `None`，调用方走规则降级）。
* **Prompt 模板**：
  ```
  你是一个海康门禁设备稳定性测试规划器。请解析用户的指令：
  "{{ BURNIN_STRATEGY }}"

  输出为 JSON 格式：
  {
    "skip_reboot": bool,          # 是否跳过重启步骤
    "event_check_delay_adjust": int, # 事件落库等待调整 (秒)
    "trigger_interval_adjust": int,  # 动作触发间隔调整 (秒)
    "diagnose_whitelist": [str]   # 允许的自愈类型，如 ["time_sync", "retry"]
  }
  ```
* Advisor 解析后 `publish("hikvision/plan", plan_dict)`；Worker 订阅该话题并覆盖私有 `test_config` 字段，动态修改执行流程。
* LLM 调用是同步阻塞的（`urllib.request`），在 async 入口用 `await asyncio.to_thread(advisor._llm_parse, instruction)` 包装。

### 5.3 诊断期（LLM 异常诊断，落在 Worker.recover() 内）：
依据第 1 节架构对齐，**不存在独立的 “LLM Diagnostic Agent”**。当 `HikvisionWorker.check()` 报告异常事实（事件缺失组合见 2.1 A.0）时，由 `recover()`（async）调用 Worker 内部的 **LLM 诊断内核**：

1. `recover()` 收集设备当前信息（时间差、网络丢包率、错误 HTTP 状态码、事件缺失组合）。
2. 将环境数据连同 `hikvision/plan` 缓存的指令发给 LLM 诊断内核。
3. LLM 在 `diagnose_whitelist` 约束下选择自愈子流程（TimeSync / WaitNetwork / ReTrigger / Abort）；白名单外的方案一律 Abort。
4. **Worker 执行**自愈操作（如 `PUT /ISAPI/System/time`），绝不绕过 Worker 直接操作设备。
5. 自愈后按 2.1 A.5 重发 `target/checked` + `target/recovered`。
6. 即使 LLM 生成错误方案，`ControlLoop` 的事实独裁会在下一轮 tick 拦截，确保系统不脱缰；LLM 突变操作必须幂等，单轮自愈次数受 `RetryBudget` 约束。

---

## 6. 配置中心规范 (configs/door_restart_stability.yaml)

> **分区原则**：`loop:` 段只放框架 `RunConfig` 的通用字段（领域无关）；领域参数（探活/重启/事件）归入 `worker:` / `event:` 段，由业务适配层消费，框架 Loop 引擎零引用。
>
> **环境变量约定**（沿用 `master` 分支 `tests/agents/config.py`）：所有运行时参数支持 `BURNIN_*` 前缀环境变量动态覆写，YAML 提供测试期默认值。设备连接信息默认值 `192.168.3.33` / `admin` / `121212..` 来自 `master` 分支（测试场景），**线上场景必须通过 `BURNIN_HOST` / `BURNIN_USER` / `BURNIN_PASSWORD` 环境变量强制覆盖**。LLM 配置通过 `LLM_API_KEY` / `LLM_MODEL` / `LLM_BASE_URL`（兼容 `OPENROUTER_*`）注入，或从仓库根 `.env` 自动加载。

```yaml
device:
  host: "192.168.3.33"          # 测试默认值；线上必须 BURNIN_HOST 覆盖
  port: 80
  username: "admin"
  password: "121212.."          # 测试默认值；线上必须 BURNIN_PASSWORD 覆盖
  http_timeout: 5

# —— 框架 RunConfig 通用字段（映射 loop/driver.RunConfig）——
loop:
  total_rounds: 1000                      # → RunConfig.max_rounds → CountStop
  round_interval: 2                       # → Scheduler(base=...)
  consecutive_failure_threshold: 10       # → RunConfig.fail_consecutive → FailThresholdStop
  vote_timeout: 1.0                       # → RunConfig.vote_timeout
  recover_timeout: 30.0                   # → RunConfig.recover_timeout
  check_timeout: 5.0                      # → RunConfig.check_timeout
  recheck_limit: 1                        # → RunConfig.recheck_limit

# —— 海康 Worker / adapter 私有（框架不消费）——
worker:
  run_reboot: true                        # 是否执行重启操作（领域 flag）
  max_recover_timeout: 180                # 设备重启最大容忍时间 (秒)
  probe_interval: 5                       # 探活时间步长
  probe_confirm_count: 2                  # 探活确认次数（见 4.1）
  warmup_time: 60                         # 上线预热时间

# —— 事件查询与自愈约束 ——
event:
  door_open_delay: 3                      # 开门后到查询事件的延迟 (秒)
  query_retry: 3                          # 事件查询重试次数
  query_retry_interval: 5                 # 重试间隔
  time_skew_threshold: 3                  # 时钟偏差阈值 (秒，见 A.2)
  max_time_sync_per_run: 3                # 全 run 时钟同步次数上限 (见 A.2)
  expected_events:                       # 期望事件链 (见 3.2 方案 D)，[major, minor] 组合
    remote_open: [3, 1024]               # 远程开门（外部触发）
    lock_open:    [5, 21]                # 门锁打开（设备动作）
    lock_close:   [5, 22]                # 门锁关闭（设备动作）；不验证时删除此项
    # face_pass:  [5, 75]                # 人脸认证通过（外界/协议触发）

# —— 自治自愈开关 ——
autonomy:
  enable_self_healing: true               # 开启自治自愈
  diagnostic_llm_model: "tencent/hy3:free"  # OpenRouter 免费模型（master 分支 llm_client.py 默认）
  instruction: ""                         # 对应 BURNIN_STRATEGY 环境变量
```

### 6.1 字段映射速查

| YAML 路径 | 框架落点 | 引擎 |
|---|---|---|
| `loop.total_rounds` | `RunConfig.max_rounds` → `CountStop` | Loop |
| `loop.consecutive_failure_threshold` | `RunConfig.fail_consecutive` → `FailThresholdStop` | Loop |
| `loop.round_interval` | `Scheduler(base=...)` | Loop |
| `loop.vote/recover/check_timeout` | `ControlLoop.__init__` 超时参数 | Loop |
| `loop.recheck_limit` | `ControlLoop.recheck_limit` | Loop |
| `worker.*` | `HikvisionWorker` / `HikvisionAdapter` 私有 | MAS |
| `event.*` | `HikvisionWorker.check()` / 自愈逻辑 | MAS |
| `autonomy.*` | 业务适配层（Diagnostic/Advisor Agent） | MAS |
