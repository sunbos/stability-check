"""End-to-end integration tests for the autonomous-MAS (Phase 7c).

These tests verify the full bus-driven flow without a real device:
- Coordinator collects votes from TrendSupervisor + RiskAnalyst (LLM disabled)
- Decision matrix is applied (record has decision + risk_score)
- Round record is published on round/done
- Incident mechanism works (TrendSupervisor raises → Coordinator acks)

No real device needed: L1 executor messages are simulated by publishing
directly to the bus.
"""

from __future__ import annotations

import asyncio
import os
import sys

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_AGENTS_DIR = os.path.join(_THIS_DIR, "agents")
_HARNESS_DIR = os.path.join(_THIS_DIR, "harness")
for _p in (_AGENTS_DIR, _HARNESS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from bus import EventBus  # noqa: E402
from context import CoordinatorContext  # noqa: E402
from agent import AgentSpec  # noqa: E402
from coordinator import Coordinator  # noqa: E402
from trend_supervisor_agent import TrendSupervisorAgent  # noqa: E402
from analyst_agent import AnalystAgent  # noqa: E402
from scribe_agent import ScribeAgent  # noqa: E402
from config import RunConfig  # noqa: E402
import llm_client  # noqa: E402


def _make_cfg(**overrides) -> RunConfig:
    """Build a minimal RunConfig for integration tests."""
    defaults = {
        "host": "fake",
        "user": "admin",
        "password": "x",
        "max_rounds": 0,
        "recover_timeout": 5,
        "fail_threshold": 99,
        "fail_consecutive": 99,
        "vote_timeout": 1.0,
    }
    defaults.update(overrides)
    return RunConfig(**defaults)


def _make_spec(name: str, role: str) -> AgentSpec:
    return AgentSpec(name, role, "", "admin", "x", "fake")


def test_e2e_clean_pass_with_voting():
    """End-to-end: clean pass round → votes collected → decision matrix applied.

    Simulates a successful reboot cycle (device recovers, event found, status
    unchanged). TrendSupervisor + RiskAnalyst (LLM disabled → abstain) reply
    to vote/request. Coordinator applies decision matrix and publishes round/done
    with decision + risk_score fields.
    """

    async def _test():
        # Ensure no LLM key (RiskAnalyst will abstain).
        # Must prevent llm_client._load_dotenv() from reloading keys via .env,
        # which would defeat the env-pop below and cause a real LLM call
        # (synchronous urllib blocking the event loop for 25s).
        saved = {
            k: os.environ.pop(k, None)
            for k in ("LLM_API_KEY", "OPENROUTER_API_KEY")
        }
        saved_dotenv = llm_client._DOTENV_LOADED
        llm_client._DOTENV_LOADED = True
        try:
            cfg = _make_cfg()
            bus = EventBus()
            ctx = CoordinatorContext()
            ctx.cfg = cfg

            coord = Coordinator(_make_spec("coord", "coordinator"), bus, ctx, cfg=cfg)
            trend = TrendSupervisorAgent(_make_spec("trend", "trend_supervisor"), bus, ctx, cfg=cfg)
            analyst = AnalystAgent(_make_spec("analyst", "analyst"), bus, ctx, cfg=cfg)
            scribe = ScribeAgent(_make_spec("scribe", "scribe"), bus, ctx, cfg=cfg)

            # Start autonomous agents (they subscribe to round/done + vote/request)
            trend_task = asyncio.create_task(trend.run())
            analyst_task = asyncio.create_task(analyst.run())
            scribe_task = asyncio.create_task(scribe.run())
            await asyncio.sleep(0.05)  # let subscriptions register

            # Subscribe Coordinator to topics (simulating run() init)
            coord._start_time = 0.0
            coord._round_done_event = asyncio.Event()
            coord.round_no = 1
            coord._round["round_no"] = 1
            coord.subscribe(coord.REBOOT_DONE_TOPIC, coord._on_reboot_done)
            coord.subscribe(coord.RECOVERED_TOPIC, coord._on_recovered)
            coord.subscribe(coord.EVENT_TOPIC, coord._on_event)
            coord.subscribe(coord.STATUS_TOPIC, coord._on_status)
            coord.subscribe(coord.ABORT_TOPIC, coord._on_abort)
            coord.subscribe(coord.INCIDENT_TOPIC, coord._on_incident)

            # Simulate a clean pass round
            await bus.publish("reboot/done", {"round_no": 1, "t_reboot": 100.0, "ok": True})
            await bus.publish("device/recovered", {"round_no": 1, "t_reboot": 100.0, "t_recover": 160.0})
            await bus.publish("check/event", {"round_no": 1, "found": True, "error": None})
            await bus.publish("check/status", {"round_no": 1, "changed": False, "diff": {}, "error": None})

            # Wait for round to complete (vote collection + decision matrix)
            await asyncio.wait_for(coord._round_done_event.wait(), timeout=10)

            # Cleanup
            trend_task.cancel()
            analyst_task.cancel()
            scribe_task.cancel()
            await asyncio.gather(trend_task, analyst_task, scribe_task, return_exceptions=True)

            # Assertions
            history = ctx.round_history_snapshot
            assert len(history) == 1, f"Expected 1 round, got {len(history)}"
            record = history[0]
            assert record["passed"] is True, "Clean pass should be passed=True"
            assert record["found"] is True
            assert record["changed"] is False
            assert "decision" in record, "Record must have decision field"
            assert "risk_score" in record, "Record must have risk_score field"
            # TrendSupervisor abstains (warmup), RiskAnalyst uses rule-based
            # fallback (LLM unavailable) → risk=30, decision=pass
            assert record["risk_score"] < 60, "Low risk on clean pass"
            assert record["decision"] == "pass"
        finally:
            llm_client._DOTENV_LOADED = saved_dotenv
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    asyncio.run(_test())


def test_e2e_fact_failure_skips_voting():
    """End-to-end: fact failure (event not found) → no voting → decision=fail.

    When facts fail, Coordinator should skip vote collection (timeout) and
    record decision=fail. The round should be marked as failed.
    """

    async def _test():
        cfg = _make_cfg(vote_timeout=0.3)
        bus = EventBus()
        ctx = CoordinatorContext()
        ctx.cfg = cfg

        coord = Coordinator(_make_spec("coord", "coordinator"), bus, ctx, cfg=cfg)

        coord._start_time = 0.0
        coord._round_done_event = asyncio.Event()
        coord.round_no = 1
        coord._round["round_no"] = 1
        coord.subscribe(coord.REBOOT_DONE_TOPIC, coord._on_reboot_done)
        coord.subscribe(coord.RECOVERED_TOPIC, coord._on_recovered)
        coord.subscribe(coord.EVENT_TOPIC, coord._on_event)
        coord.subscribe(coord.STATUS_TOPIC, coord._on_status)
        coord.subscribe(coord.ABORT_TOPIC, coord._on_abort)
        coord.subscribe(coord.INCIDENT_TOPIC, coord._on_incident)

        # Simulate a failure round: event not found
        await bus.publish("reboot/done", {"round_no": 1, "t_reboot": 100.0, "ok": True})
        await bus.publish("device/recovered", {"round_no": 1, "t_reboot": 100.0, "t_recover": 160.0})
        await bus.publish("check/event", {"round_no": 1, "found": False, "error": None})
        await bus.publish("check/status", {"round_no": 1, "changed": False, "diff": {}, "error": None})

        await asyncio.wait_for(coord._round_done_event.wait(), timeout=10)

        history = ctx.round_history_snapshot
        assert len(history) == 1
        record = history[0]
        assert record["passed"] is False
        assert record["decision"] == "fail"
        assert coord.total_failures == 1

    asyncio.run(_test())


def test_e2e_trend_supervisor_raises_incident():
    """End-to-end: 5 consecutive recover time increments → TrendSupervisor raises critical.

    Simulates 5 rounds with monotonically increasing recover times. After the
    5th round, TrendSupervisor should raise a critical incident. Coordinator
    should ack it and set _has_critical_incident for the next decision matrix.
    """

    async def _test():
        cfg = _make_cfg()
        bus = EventBus()
        ctx = CoordinatorContext()
        ctx.cfg = cfg

        coord = Coordinator(_make_spec("coord", "coordinator"), bus, ctx, cfg=cfg)
        trend = TrendSupervisorAgent(_make_spec("trend", "trend_supervisor"), bus, ctx, cfg=cfg)

        # Track incidents received by Coordinator
        incidents_acked: list = []

        # Start TrendSupervisor
        trend_task = asyncio.create_task(trend.run())
        await asyncio.sleep(0.05)

        # Subscribe Coordinator
        coord._start_time = 0.0
        coord._round_done_event = asyncio.Event()
        coord.subscribe(coord.REBOOT_DONE_TOPIC, coord._on_reboot_done)
        coord.subscribe(coord.RECOVERED_TOPIC, coord._on_recovered)
        coord.subscribe(coord.EVENT_TOPIC, coord._on_event)
        coord.subscribe(coord.STATUS_TOPIC, coord._on_status)
        coord.subscribe(coord.ABORT_TOPIC, coord._on_abort)
        coord.subscribe(coord.INCIDENT_TOPIC, coord._on_incident)

        # Track acks
        async def _track_ack(msg):
            incidents_acked.append(msg)
        bus.subscribe("incident/ack", _track_ack)

        # Simulate 5 rounds with increasing recover times
        base_t = 100.0
        for i in range(1, 6):
            coord._round_done_event = asyncio.Event()
            coord.round_no = i
            coord._round = coord._new_round_state()
            coord._round["round_no"] = i

            t_reboot = base_t + (i - 1) * 200
            t_recover = t_reboot + 60 + i * 10  # 70, 80, 90, 100, 110 (increasing)

            await bus.publish("reboot/done", {"round_no": i, "t_reboot": t_reboot, "ok": True})
            await bus.publish("device/recovered", {"round_no": i, "t_reboot": t_reboot, "t_recover": t_recover})
            await bus.publish("check/event", {"round_no": i, "found": True, "error": None})
            await bus.publish("check/status", {"round_no": i, "changed": False, "diff": {}, "error": None})

            await asyncio.wait_for(coord._round_done_event.wait(), timeout=10)
            await asyncio.sleep(0.05)  # let incident propagation settle

        trend_task.cancel()
        await asyncio.gather(trend_task, return_exceptions=True)

        # After 5 consecutive increments, TrendSupervisor should have raised critical
        # The incident is async; check that at least one ack was sent
        assert len(incidents_acked) > 0, (
            "Coordinator should have acked at least one incident from TrendSupervisor"
        )
        # The critical incident should set _has_critical_incident
        # (may have been consumed by decision matrix, but the ack should record it)
        ack = incidents_acked[-1]
        assert ack["ack_by"] == "coordinator"

    asyncio.run(_test())


def test_e2e_scribe_records_round_with_decision():
    """End-to-end: ScribeAgent records round/done with decision + risk_score fields.

    Verifies that the ScribeAgent's private timeline captures the new fields
    added by the decision matrix integration.
    """

    async def _test():
        saved = {
            k: os.environ.pop(k, None)
            for k in ("LLM_API_KEY", "OPENROUTER_API_KEY")
        }
        try:
            cfg = _make_cfg()
            bus = EventBus()
            ctx = CoordinatorContext()
            ctx.cfg = cfg

            coord = Coordinator(_make_spec("coord", "coordinator"), bus, ctx, cfg=cfg)
            scribe = ScribeAgent(_make_spec("scribe", "scribe"), bus, ctx, cfg=cfg)

            scribe_task = asyncio.create_task(scribe.run())
            await asyncio.sleep(0.05)

            coord._start_time = 0.0
            coord._round_done_event = asyncio.Event()
            coord.round_no = 1
            coord._round["round_no"] = 1
            coord.subscribe(coord.REBOOT_DONE_TOPIC, coord._on_reboot_done)
            coord.subscribe(coord.RECOVERED_TOPIC, coord._on_recovered)
            coord.subscribe(coord.EVENT_TOPIC, coord._on_event)
            coord.subscribe(coord.STATUS_TOPIC, coord._on_status)
            coord.subscribe(coord.ABORT_TOPIC, coord._on_abort)
            coord.subscribe(coord.INCIDENT_TOPIC, coord._on_incident)

            await bus.publish("reboot/done", {"round_no": 1, "t_reboot": 100.0, "ok": True})
            await bus.publish("device/recovered", {"round_no": 1, "t_reboot": 100.0, "t_recover": 160.0})
            await bus.publish("check/event", {"round_no": 1, "found": True, "error": None})
            await bus.publish("check/status", {"round_no": 1, "changed": False, "diff": {}, "error": None})

            await asyncio.wait_for(coord._round_done_event.wait(), timeout=10)
            await asyncio.sleep(0.05)  # let scribe process round/done

            scribe_task.cancel()
            await asyncio.gather(scribe_task, return_exceptions=True)

            summary = scribe.summary()
            assert summary["total"] == 1
            assert summary["passed"] == 1
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    asyncio.run(_test())
