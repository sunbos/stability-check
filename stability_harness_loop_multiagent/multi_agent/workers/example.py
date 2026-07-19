"""ExampleWorkerAgent —— WorkerAgent 契约的一个通用演示。

使用一个 TargetAdapter，在*通用*目标上执行 *act* 和 *observe*（没有具体的
设备，也没有烧入（burn-in）相关的细节）。它展示了如何覆盖 ``do_work`` /
``recover`` / ``check``，并验证了发布流水线：

    loop/tick -> act() -> target/acted, target/recovered, target/checked,
                             agent/<role>/done

要将其用于真实系统，只需提供一个具体的 ``TargetAdapter`` 实现（例如某个
设备 / 服务 / 资源适配器）—— 无需其他改动。WorkerAgent 仅作执行：它们绝不
裁决通过/失败。
"""

import asyncio
import logging

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from .base import WorkerAgent
from ..adapter import TargetAdapter, State


class ExampleWorkerAgent(WorkerAgent):
    """通用 Worker：在每个 ``loop/tick`` 上循环执行 act -> recover -> check。"""

    def __init__(
        self,
        bus: EventBus,
        spec: AgentSpec,
        adapter: TargetAdapter,
        *,
        operation: str = "ping",
        recover_polls: int = 3,
        recover_interval: float = 0.5,
        facts: dict = None,
    ) -> None:
        super().__init__(bus, spec, adapter)
        self.operation = operation
        self.recover_polls = recover_polls
        self.recover_interval = recover_interval
        self._facts = dict(facts or {})
        self._log = logging.getLogger(f"stability_harness_loop_multiagent.multi_agent.worker.{self.role}")

    # ---- act ----------------------------------------------------------
    def do_work(self, tick: dict):
        """调用适配器的 ``act()``。返回适配器上报的任何内容。"""
        op = tick.get("operation", self.operation)
        result = self.adapter.act(op)
        self._log.info("已执行 op=%r ok=%s", op, result.ok)
        return result

    # ---- recover（轮询就绪） ---------------------------------------
    async def recover(self, tick: dict) -> bool:
        """轮询 ``adapter.observe()``，直到目标报告就绪。

        通用就绪规则：除非目标明确报告 ``{"up": False}``，否则视为已恢复。
        针对领域特定的稳定化逻辑（例如等待某个已知良好状态的快照）进行覆盖。
        """
        last = None
        for _ in range(self.recover_polls):
            state = self.adapter.observe()
            snap = state.snapshot if isinstance(state, State) else state
            last = snap
            if isinstance(snap, dict) and snap.get("up") is False:
                await asyncio.sleep(self.recover_interval)
                continue
            return True
        # 从未观察到明确的失败 -> 视为已恢复
        return True

    # ---- check（事实生成） ----------------------------------------
    def check(self, tick: dict) -> dict:
        """返回由 DecisionAuthority 消费的事实检查。

        通用示例事实：act 产生了结果且观测到的状态是健康的。覆盖它以断言
        领域特定的不变量。
        """
        facts = dict(self._facts)
        try:
            state = self.adapter.observe()
            snap = state.snapshot if isinstance(state, State) else state
        except Exception:  # noqa: BLE001 - 观测失败即视为一个失败事实
            snap = None
        facts.setdefault("acted", True)
        if isinstance(snap, dict):
            facts.setdefault("state_ok", bool(snap.get("up", True)))
        return facts


__all__ = ["ExampleWorkerAgent"]


if __name__ == "__main__":  # 运行方式：python -m stability_harness_loop_multiagent.multi_agent.workers.example
    import time

    from ...core.bus import EventBus
    from ..adapter import Result, State

    class StubAdapter:
        def act(self, operation):
            return Result(ok=True, data={"op": operation})

        def observe(self):
            return State(snapshot={"up": True})

        def events(self, since):
            return []

    async def demo() -> None:
        bus = EventBus()
        collected = []

        def collect(_t, msg):
            collected.append(msg)

        for t in ("target/acted", "target/recovered", "target/checked",
                  "agent/example/done"):
            bus.subscribe(t, collect)

        worker = ExampleWorkerAgent(
            bus, AgentSpec(id="w1", role="example"), StubAdapter()
        )
        await worker.act({"round": 1, "operation": "demo"})
        await asyncio.sleep(0.05)  # 让发送即忘的处理器刷新
        print(f"已发布 {len(collected)} 条消息：")
        for m in collected:
            print("  ", m)
        worker._log.info("演示完成于 t=%.2f", time.time())

    asyncio.run(demo())
