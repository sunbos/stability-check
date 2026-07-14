# 真正自治多智能体重构设计

> 将"中心化编排 + 顾问投票"重构为"真正自治多智能体"。
> 日期：2026-07-14

## 1. 动机

当前架构（feat/autonomous-multiagent）实质是"中心化编排 + 投票装饰"，与业界
定义的自治多智能体（autonomous multi-agent system）有 6 个关键差距：

1. EventBus publish 会 await 所有 handler（同步 RPC，非异步消息）
2. L3 agent 被动响应消息，无主动循环
3. coord/recheck 主题已发布但无 agent 订阅（空壳）
4. 投票无快速路径（高风险也要等所有投票者）
5. 投票异常时 decision=pass（过于乐观）
6. 日志是 print 非结构化

## 2. 重构目标

达到业界自治多智能体的 5 个特征：
- agent 有自己的目标（监控趋势/评估风险/记录历史）
- agent 有主动循环（定时器驱动，非被动等消息）
- agent 能主动发起行动（raise incident / 建议 / 告警）
- agent 之间能直接协商（不全部经过 Coordinator）
- 无中心节点也能部分运行（Coordinator 挂了，L3 仍能告警）

## 3. 分阶段设计

### Phase 1: EventBus 真异步化

**问题**：`publish()` 当前 `await` 所有 handler，导致 vote_timeout 失效。

**方案**：
```python
async def publish(self, topic, message):
    """Fire-and-forget: 不等待 handler 完成，立即返回。"""
    handlers = self._match(topic)
    for h in handlers:
        # create_task 异步执行，不 await
        if asyncio.iscoroutinefunction(h):
            asyncio.create_task(self._safe_call(h, message))
        else:
            self._safe_call_sync(h, message)

async def publish_and_wait(self, topic, message):
    """等待所有 handler 完成（用于需要同步的场景，如 test）。"""
    # 原 publish 行为

async def _safe_call(self, handler, message):
    """安全调用 async handler，异常不传播。"""
    try:
        await handler(message)
    except Exception as e:
        log(f"Handler error: {e}")
```

**影响**：
- vote/request publish 立即返回
- Coordinator 的 _collect_votes 真正在 vote_timeout 内收集 vote/reply
- LLM 调用不再阻塞每轮

**风险**：部分代码依赖 publish 的同步语义（如 conftest baseline 抓取）。
**缓解**：保留 `publish_and_wait` 给需要的场景；测试用 `publish_and_wait`。

### Phase 2: L3 Agent 主动循环

**问题**：TrendSupervisor/RiskAnalyst 只在收到消息时判断，不是真正自治。

**方案**：
```python
class TrendSupervisorAgent(Agent):
    POLL_INTERVAL = 30.0  # 每 30 秒主动检查一次

    async def run(self):
        """主动循环 + 消息响应。"""
        self.subscribe("round/done", self._on_round_done)
        self.subscribe("vote/request", self._on_vote_request)
        self._stop = asyncio.Event()
        # 主动循环
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.POLL_INTERVAL
                )
            except asyncio.TimeoutError:
                await self._proactive_check()  # 主动检查
```

**新增主动检查**：
- TrendSupervisor：每 30s 检查恢复时间趋势、失败率
- RiskAnalyst：每 30s 检查风险分历史，连续高风险时 raise

### Phase 3: Recheck 完整实现

**问题**：coord/recheck 无 agent 订阅，安全承诺未兑现。

**方案**：
- Coordinator 发布 coord/recheck 时，记录待 recheck 的轮次
- EventCheckAgent + StatusCheckAgent 订阅 coord/recheck，立即重新检查
- recheck 结果通过 check/event + check/status 回到 Coordinator
- Coordinator 更新该轮的 decision（recheck → pass/fail）

### Phase 4: 投票快速路径

**问题**：即使第一个投票者 risk=95，仍等所有投票者。

**方案**：
```python
async def _collect_votes(self, ...):
    FAST_PATH_THRESHOLD = 90
    while loop.time() < deadline:
        await asyncio.sleep(0.02)
        for reply in replies:
            if reply["risk_score"] >= FAST_PATH_THRESHOLD:
                return self._combine_votes(replies)  # 立即返回
```

### Phase 5: 错误处理保守化 + 结构化日志

**投票异常**：decision=warn（不是 pass）
**结构化日志**：
```python
import logging
logger = logging.getLogger("burnin")
logger.info("vote_collected", extra={
    "round": 1, "voter": "trend_supervisor", "risk": 10, ...
})
```

## 4. 向后兼容

- 保留所有主题名不变
- 保留 RunConfig / AgentSpec / DeviceClient 不变
- 保留 test_burnin.py 的 test_burnin_session 入口不变
- 保留所有现有测试（可能需要适配 publish_and_wait）

## 5. 验收标准

1. vote_timeout=1.0s 真正生效（不再等 15 秒）
2. TrendSupervisor 主动 raise incident（不只在收到消息时）
3. coord/recheck 触发实际重新检查
4. 任一投票者 risk≥90 立即触发 recheck
5. 投票异常时 decision=warn
6. 所有现有测试通过 + 新增测试覆盖新行为
