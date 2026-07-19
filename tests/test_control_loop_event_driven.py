"""ControlLoop 事件驱动化表征测试（P2 / P2-b）。

验证两件事：
1. ``_run_round`` 在收齐 target/recovered + target/checked 后立即返回，
   不硬等满 recover_timeout/check_timeout。
2. ``_collect_votes`` 在收齐投票（静默期后）后立即返回，不硬等满 vote_timeout；
   无投票者时在 vote_timeout 上限内终止（防死锁不变量不变）。
"""

import asyncio
import time

import pytest

from stability_harness_loop_multiagent.core.agent import AgentSpec
from stability_harness_loop_multiagent.core.bus import EventBus
from stability_harness_loop_multiagent.loop.context import SharedContext
from stability_harness_loop_multiagent.loop.decision import DecisionAuthority
from stability_harness_loop_multiagent.loop.driver import ControlLoop
from stability_harness_loop_multiagent.loop.termination import (
    CountStop,
    TerminationPolicy,
)


def _make_loop(bus, *, recover_timeout=0.1, check_timeout=0.1,
               vote_timeout=0.2, vote_settle=0.05):
    ctx = SharedContext(baseline={})
    decision = DecisionAuthority()
    term = TerminationPolicy([CountStop(10 ** 9)])  # 永不 halt
    return ControlLoop(
        bus, ctx, decision, term,
        recover_timeout=recover_timeout, check_timeout=check_timeout,
        vote_timeout=vote_timeout, vote_settle=vote_settle,
    )


@pytest.mark.asyncio
async def test_run_round_returns_early_when_replies_arrive():
    bus = EventBus()
    loop = _make_loop(bus, vote_timeout=0.3)

    def on_tick(_t, msg):
        bus.publish("target/recovered",
                    {"recovered": True, "round": msg.get("round")})
        bus.publish("target/checked",
                    {"facts": {"checks_received": True}, "round": msg.get("round")})

    def on_vote(_t, _m):
        bus.publish("agent/vote/reply", {"risk": 30.0, "confidence": 0.7})

    bus.subscribe("loop/tick", on_tick)
    bus.subscribe("loop/vote/request", on_vote)

    start = time.monotonic()
    await loop._run_round()
    elapsed = time.monotonic() - start
    # 回复即时到达：应在 vote_timeout(0.3) 之前返回，而非硬等满超时。
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_run_round_respects_timeout_bound_without_replies():
    bus = EventBus()
    loop = _make_loop(bus, recover_timeout=0.1, check_timeout=0.1, vote_timeout=0.2)
    # 无任何订阅者 -> 无回复

    start = time.monotonic()
    await loop._run_round()
    elapsed = time.monotonic() - start
    # 必须在超时上限内终止（防死锁不变量）：recover(0.1) + vote(0.2) ≈ 0.3。
    assert 0.2 <= elapsed <= 0.6


@pytest.mark.asyncio
async def test_collect_votes_early_return():
    bus = EventBus()
    loop = _make_loop(bus, vote_timeout=0.3, vote_settle=0.05)
    bus.subscribe("loop/vote/request",
                  lambda _t, _m: bus.publish("agent/vote/reply", {"risk": 10.0}))

    start = time.monotonic()
    votes = await loop._collect_votes()
    elapsed = time.monotonic() - start
    assert votes, "应已收到投票"
    # 投票即时到达 -> 远小于 vote_timeout(0.3)（仅静默期 0.05 左右）。
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_collect_votes_timeout_bound_without_voters():
    bus = EventBus()
    loop = _make_loop(bus, vote_timeout=0.2, vote_settle=0.05)
    # 无投票者

    start = time.monotonic()
    votes = await loop._collect_votes()
    elapsed = time.monotonic() - start
    assert votes == []
    # 无投票者时仍须在 vote_timeout 上限内终止。
    assert 0.15 <= elapsed <= 0.6
