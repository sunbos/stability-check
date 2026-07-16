"""针对通用 stability_harness_loop_multiagent 框架的冒烟测试 / 自包含演示。

这证明了通用模板能够端到端运行，而无需内置任何具体场景。它纯粹通过
EventBus 将三个引擎连接起来：

    harness : EventBus, Telemetry, Watchdog（存活 / 死锁探测器）
    loop    : ControlLoop + RunConfig + DecisionAuthority + TerminationPolicy
    multi_agent     : FakeTargetAdapter + WorkerAgent + AdvisorAgent + ObserverAgent

唯一的“目标”是一个合成的内存计数器（FakeTargetAdapter）：工作者在每次
act() 时使其自增，并报告合成的状态/事件。没有涉及真实的设备、服务或领域，
因此该演示保持通用。

直接运行：  python stability_harness_loop_multiagent/examples/smoke.py
被测试使用：tests/test_stability_harness_loop_multiagent_smoke.py 导入 run_smoke / 其中的角色。

仅使用标准库；通过异常进行断言（raise == 失败）。
"""

import asyncio
import os
import sys

# 当作为裸脚本从仓库根目录运行时（python stability_harness_loop_multiagent/examples/smoke.py），
# 让 stability_harness_loop_multiagent 包可被导入。
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from stability_harness_loop_multiagent import (
    AdvisorAgent,
    AgentSpec,
    ControlLoop,
    DecisionAuthority,
    EventBus,
    ObserverAgent,
    RunConfig,
    SharedContext,
    Scheduler,
    Telemetry,
    Watchdog,
    WorkerAgent,
)
from stability_harness_loop_multiagent.harness.telemetry import MemorySink
from stability_harness_loop_multiagent.multi_agent.adapter import Event, Result, State, TargetAdapter


# --------------------------------------------------------------------------
# 假目标 —— 一个合成的、与场景无关的“事物”，由 MAS 对其执行操作。
# --------------------------------------------------------------------------
class FakeTargetAdapter:
    """一个在每次 act() 时自增的计数器；报告合成的状态/事件。

    以结构化方式实现 TargetAdapter *协议*（无需子类化）：
    act() -> Result, observe() -> State, events(since) -> List[Event]。
    """

    def __init__(self, fail: bool = False) -> None:
        self.counter = 0
        self.fail = fail  # 为 True 时，observe() 报告不健康状态

    def act(self, operation) -> Result:
        self.counter += 1
        return Result(ok=True, data={"counter": self.counter, "op": operation})

    def observe(self) -> State:
        return State(snapshot={"up": not self.fail, "counter": self.counter})

    def events(self, since: float):
        evs = [Event(kind="acted", payload={"counter": self.counter}, ts=since)]
        if self.fail:
            evs.append(
                Event(kind="degraded", payload={"reason": "injected-failure"}, ts=since)
            )
        return evs


# --------------------------------------------------------------------------
# MAS 角色 —— 通用的；不包含任何具体的领域知识。
# --------------------------------------------------------------------------
class FakeWorker(WorkerAgent):
    """驱动 FakeTargetAdapter 的工作者。

    在每个 loop/tick 上发布标准流水线：
        target/acted, target/recovered, target/checked, agent/<role>/done
    """

    def check(self, tick: dict) -> dict:
        snap = self.adapter.observe().snapshot
        up = isinstance(snap, dict) and bool(snap.get("up", True))
        return {"acted": True, "state_ok": up}


class FixedAdvisor(AdvisorAgent):
    """最小化的顾问：每轮投出一个固定的（风险、置信度）。"""

    def __init__(self, bus, spec, *, risk: float = 30.0, confidence: float = 0.9,
                 weight: float = 1.0) -> None:
        super().__init__(bus, spec, weight=weight)
        self._risk = float(risk)
        self._confidence = float(confidence)

    def vote(self):
        return (self._risk, self._confidence)


class PrintingObserver(ObserverAgent):
    """记录所见到的每个事件，并打印 loop/done 摘要的观察者。"""

    def __init__(self, bus, spec) -> None:
        super().__init__(bus, spec)
        self.seen = []

    def on_event(self, topic: str, message) -> None:
        self.seen.append((topic, message))
        if topic == "loop/done":
            v = (message or {}).get("verdict")
            r = (message or {}).get("risk")
            print(f"[observer] round {message.get('round')} verdict={v} risk={r}")


# --------------------------------------------------------------------------
# 端到端驱动器。返回一个供检查 / 断言使用的产物字典。
# --------------------------------------------------------------------------
async def run_smoke(
    fail: bool = False,
    max_rounds: int = 5,
    *,
    run_timeout: float = 30.0,
) -> dict:
    bus = EventBus()
    mem = MemorySink()
    tel = Telemetry(bus=bus, sinks=[mem])

    ctx = SharedContext(baseline={"kind": "fake"}, strategy_text="smoke-demo")
    decision = DecisionAuthority()

    # RunConfig -> TerminationPolicy。max_duration=0 会禁用“时长停止”；
    # 一个极大的 fail_threshold 会让循环一直存活，直到命中 max_rounds。
    cfg = RunConfig(max_rounds=max_rounds, max_duration=0.0, fail_threshold=10_000)
    term = cfg.build_termination()

    loop = ControlLoop(
        bus,
        ctx,
        decision,
        term,
        # 极短的超时，使演示在远小于一秒内完成
        vote_timeout=0.1,
        recover_timeout=0.05,
        check_timeout=0.05,
        recheck_limit=0,
        # 轮间无空闲，使整次运行快速且确定性
        scheduler=Scheduler(base=0.0, min_interval=0.0),
        telemetry=tel,
    )

    adapter = FakeTargetAdapter(fail=fail)
    worker = FakeWorker(
        bus, AgentSpec(id="w1", role="fake", capabilities={"act"}), adapter
    )
    advisor = FixedAdvisor(
        bus, AgentSpec(id="a1", role="risk"), risk=30.0, confidence=0.9, weight=1.0
    )
    observer = PrintingObserver(
        bus,
        AgentSpec(
            id="o1",
            role="scribe",
            subscriptions=["loop/done", "target/#", "agent/#"],
        ),
    )
    # 充裕的停滞预算：看门狗绝不应中止这次健康运行。
    dog = Watchdog(bus, stall_timeout=300.0, check_interval=0.05)

    # 启动所有智能体；循环作为一个智能体启动，并 await 其完成。
    for a in (worker, advisor, observer, dog):
        await a.start()
    await loop.start()

    try:
        # 确定性终止：循环在 CountStop(max_rounds) 上停止。
        # wait_for 同时充当死锁探测器 —— 一个卡住的循环会超时。
        await asyncio.wait_for(loop._task, run_timeout)
    finally:
        for a in (worker, advisor, observer, dog):
            await a.stop()

    return {
        "ctx": ctx,
        "loop": loop,
        "observer": observer,
        "adapter": adapter,
        "telemetry": tel,
        "config": cfg,
    }


def assert_healthy(result: dict) -> None:
    ctx = result["ctx"]
    loop = result["loop"]
    observer = result["observer"]
    adapter = result["adapter"]

    # 1) 循环到达终止状态
    assert ctx.round_count >= 1, "循环没有产生任何轮次"
    assert ctx.aborted, "循环没有到达一个终止状态"
    # 恰好运行了 max_rounds 轮
    assert ctx.round_count == result["config"].max_rounds, (
        f"期望 {result['config'].max_rounds} 轮，实际 {ctx.round_count}"
    )
    # 2) 产生了裁决
    assert loop.verdict is not None, "没有设置权威裁决"
    history = ctx.snapshot().round_history
    assert len(history) == ctx.round_count
    assert all(r.verdict == "pass" for r in history), (
        "健康运行应当只产生 'pass' 裁决："
        f"{[r.verdict for r in history]}"
    )
    # 3) 观察者收到了事件（至少收到了 loop/done）
    assert observer.seen, "观察者没有收到任何事件"
    assert any(t == "loop/done" for t, _ in observer.seen)
    # 4) 工作者确实通过假适配器在每轮都执行了操作
    assert adapter.counter == ctx.round_count, (
        f"工作者执行了 {adapter.counter} 次，但运行了 {ctx.round_count} 轮"
    )


def assert_failing_fact(result: dict) -> None:
    ctx = result["ctx"]
    loop = result["loop"]
    history = ctx.snapshot().round_history

    # 事实独裁：一个被注入的失败事实必须产生一个 'fail' 裁决，
    # 即使顾问以高置信度投出了低风险（30）。
    assert any(r.verdict == "fail" for r in history), (
        "失败事实应当至少强制出一个 'fail' 裁决："
        f"{[r.verdict for r in history]}"
    )
    assert loop.verdict is not None
    # 最终裁决反映了这次失败
    assert ctx.snapshot().round_history[-1].verdict == "fail"
    # 合理性检查：确实有一个事实为 False
    assert any(
        not ok for r in history for ok in r.facts.values()
    ), "期望在已记录的轮次中至少有一个 falsy 事实"


async def _main() -> None:
    print("=== stability_harness_loop_multiagent 通用冒烟（健康场景） ===")
    healthy = await run_smoke(fail=False, max_rounds=5)
    assert_healthy(healthy)
    print(
        f"OK 健康: rounds={healthy['ctx'].round_count} "
        f"verdicts={[r.verdict for r in healthy['ctx'].snapshot().round_history]} "
        f"observer_events={len(healthy['observer'].seen)} "
        f"acts={healthy['adapter'].counter}"
    )

    print("\n=== stability_harness_loop_multiagent 通用冒烟（事实独裁场景） ===")
    failing = await run_smoke(fail=True, max_rounds=5)
    assert_failing_fact(failing)
    print(
        f"OK 失败: rounds={failing['ctx'].round_count} "
        f"verdicts={[r.verdict for r in failing['ctx'].snapshot().round_history]}"
    )

    print("\nALL SMOKE ASSERTIONS PASSED")


if __name__ == "__main__":
    asyncio.run(_main())
