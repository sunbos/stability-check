"""Integration smoke test for the generic stability_harness_loop_multiagent framework.

Wires the three engines purely through the EventBus (no concrete scenario) and
asserts the core invariants:

  1. The loop TERMINATES (reaches its round cap; no deadlock / hang).
  2. VERDICTS are produced every round by the authoritative DecisionAuthority.
  3. OBSERVERS receive events (the bus fan-out works end-to-end).
  4. FACT DICTATORSHIP: an injected failing fact forces a 'fail' verdict even
     though the advisor confidently votes a low risk.

Standard library only. Run:
    python stability_harness_loop_multiagent/examples/smoke.py   # standalone asserts
    pytest tests/test_stability_harness_loop_multiagent_smoke.py # this file
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from stability_harness_loop_multiagent import DecisionAuthority
from stability_harness_loop_multiagent.examples.smoke import (
    FakeTargetAdapter,
    assert_failing_fact,
    assert_healthy,
    run_smoke,
)


@pytest.mark.asyncio
async def test_loop_terminates_and_produces_verdicts():
    result = await run_smoke(fail=False, max_rounds=5)
    # Invariant 1 + 2 + 3: termination, verdicts produced, observers saw events.
    assert_healthy(result)
    # No deadlock: the run returned (asyncio.wait_for would have timed out).
    assert result["ctx"].round_count == 5
    assert result["loop"].verdict is not None


@pytest.mark.asyncio
async def test_fact_dictatorship_failing_fact_forces_fail():
    result = await run_smoke(fail=True, max_rounds=5)
    # Invariant 4: fact dictatorship overrides the confident low-risk advisor vote.
    assert_failing_fact(result)


def test_decision_authority_fact_dictatorship_unit():
    """Pure unit check of the safety floor: any falsy fact => fail, period."""
    dec = DecisionAuthority()
    # Low risk + a False fact => fail (risk cannot upgrade a broken fact).
    v = dec.decide({"acted": True, "state_ok": False}, risk_score=30.0)
    assert v.decision == "fail"
    assert v.reason.startswith("fact failed")
    # All facts satisfied + low risk => pass.
    assert dec.decide({"acted": True, "state_ok": True}, risk_score=30.0).decision == "pass"
    # Empty facts dict is "nothing falsy" => pass (the loop injects a failing
    # 'checks_received' fact when no worker reports, see ControlLoop._merge_facts).
    assert dec.decide({}, risk_score=10.0).decision == "pass"


@pytest.mark.asyncio
async def test_fake_adapter_is_structural_target_adapter():
    """The fake adapter behaves like a TargetAdapter without subclassing it."""
    from stability_harness_loop_multiagent.multi_agent.adapter import TargetAdapter

    a = FakeTargetAdapter()
    assert isinstance(a, TargetAdapter)  # runtime_checkable protocol
    r = a.act("ping")
    assert r.ok and r.data["counter"] == 1
    assert a.observe().snapshot["up"] is True
    a.fail = True
    assert a.observe().snapshot["up"] is False
    assert any(e.kind == "degraded" for e in a.events(0.0))
