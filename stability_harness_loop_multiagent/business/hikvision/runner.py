"""Assembly entry point for Hikvision door stability test.

Mirrors examples/smoke.py wiring: EventBus + Telemetry + SharedContext +
DecisionAuthority + RunConfig -> ControlLoop, plus HikvisionWorker (subscribed
to hikvision/plan) + HikvisionAdvisor + ScribeAgent + Watchdog.

LLM auto-detection: if LLM_API_KEY / OPENROUTER_API_KEY is set (or .env
loaded), advisor/diagnostic use real OpenRouter tencent/hy3:free; otherwise
they fall back to deterministic rule-based callables (tests use these).
"""

import asyncio
import json
from typing import Any, Callable, Dict

from ...harness.agent import AgentSpec
from ...harness.bus import EventBus
from ...harness.telemetry import MemorySink, Telemetry
from ...harness.watchdog import Watchdog
from ...loop.context import SharedContext
from ...loop.decision import DecisionAuthority
from ...loop.driver import ControlLoop, RunConfig
from ...loop.scheduler import Scheduler
from ...multi_agent.observers.scribe import ScribeAgent
from .adapter import HikvisionAdapter
from .advisor import HikvisionAdvisor
from .diagnostic import DiagnosticKernel, HEAL_TIME_SYNC, HEAL_RETRIGGER
from .llm import get_client
from .worker import HikvisionWorker


def _default_parse(instruction: str) -> Dict[str, Any]:
    """Deterministic fallback when no LLM available."""
    return {"skip_reboot": False, "event_check_delay_adjust": 0,
            "trigger_interval_adjust": 0,
            "diagnose_whitelist": [HEAL_TIME_SYNC, HEAL_RETRIGGER]}


def _default_llm_decide(env: dict) -> str:
    """Deterministic fallback when no LLM available."""
    if env.get("time_skew_seconds", 0) > 3.0:
        return HEAL_TIME_SYNC
    return HEAL_RETRIGGER


def _make_llm_parse() -> Callable[[str], Dict] | None:
    """Return real LLM parse callable if API key available, else None."""
    client = get_client()
    if client is None:
        return None

    system_prompt = (
        "You are a Hikvision door stability test planner. "
        "Parse the user instruction into JSON.")

    def _parse(instruction: str) -> Dict[str, Any]:
        if not instruction:
            return _default_parse(instruction)
        result = client.chat_json(system_prompt, instruction)
        if not isinstance(result, dict):
            return _default_parse(instruction)
        return result
    return _parse


def _make_llm_decide() -> Callable[[dict], str] | None:
    """Return real LLM decide callable if API key available, else None."""
    client = get_client()
    if client is None:
        return None

    system_prompt = (
        "You are a Hikvision door stability self-heal diagnostician. "
        "Given environment facts, choose one heal sub-flow: "
        "time_sync, wait_network, retrigger, abort. "
        'Return JSON {"decision": "<name>"}.')

    def _decide(env: dict) -> str:
        result = client.chat_json(system_prompt, json.dumps(env))
        if not isinstance(result, dict):
            return HEAL_RETRIGGER
        decision = result.get("decision", HEAL_RETRIGGER)
        return decision
    return _decide


def _patch_worker_plan_handler(worker: HikvisionWorker) -> None:
    """Override worker.handle to cache hikvision/plan into worker.state.

    The base Agent._dispatch looks up self.handle at call time, so replacing
    the instance attribute before start() is sufficient. The original handle
    is preserved for non-plan topics (loop/tick).
    """
    original_handle = worker.handle

    async def handle_with_plan(topic: str, message) -> None:
        if topic == "hikvision/plan":
            worker.state["plan"] = message or {}
            return
        await original_handle(topic, message)
    worker.handle = handle_with_plan  # type: ignore[assignment]


async def run_hikvision_stability(
    client,
    max_rounds: int = 5,
    run_timeout: float = 30.0,
    instruction: str = "",
    llm_parse: Callable[[str], Dict] | None = None,
    llm_decide: Callable[[dict], str] | None = None,
    *,
    run_reboot: bool = True,
    probe_interval: float = 5.0,
    probe_confirm_count: int = 2,
    warmup_time: float = 60.0,
    max_recover_timeout: float = 180.0,
    event_check_delay: float = 3.0,
) -> dict:
    """Run a Hikvision door stability test session end-to-end.

    Auto-detects real LLM if env / .env provides a key; explicit params win.
    Reboot/probe/warmup config (spec §4.1, §4.2, §6 worker.*) forwarded to
    HikvisionWorker. ``pre_loop_setup()`` is invoked AFTER the worker is
    started (so it can publish) but BEFORE the loop starts, so the baseline
    reboot duration is measured once and reused per round. Returns a dict
    with ctx/loop/worker/advisor/telemetry/config.
    """
    if llm_parse is None:
        llm_parse = _make_llm_parse() or _default_parse
    if llm_decide is None:
        llm_decide = _make_llm_decide() or _default_llm_decide

    bus = EventBus()
    mem = MemorySink()
    tel = Telemetry(bus=bus, sinks=[mem])
    ctx = SharedContext(baseline={"kind": "hikvision"}, strategy_text=instruction)
    decision = DecisionAuthority()
    # ControlLoop waits max(recover_timeout, check_timeout) for target/checked
    # (driver.py §round). When run_reboot=True, reboot+probe+warmup can take
    # max_recover_timeout + warmup_time; add buffer for probe + open + query.
    # When run_reboot=False, do_work is remote_open_door + event_check_delay
    # + event query; size timeout to cover the delay plus a small buffer.
    if run_reboot:
        round_act_timeout = max_recover_timeout + warmup_time + 30.0
    else:
        round_act_timeout = event_check_delay + 5.0
    cfg = RunConfig(max_rounds=max_rounds, max_duration=0.0, fail_threshold=10_000,
                    vote_timeout=0.1, recover_timeout=round_act_timeout,
                    check_timeout=round_act_timeout, recheck_limit=0)
    term = cfg.build_termination()
    loop = ControlLoop(
        bus, ctx, decision, term,
        vote_timeout=cfg.vote_timeout,
        recover_timeout=cfg.recover_timeout,
        check_timeout=cfg.check_timeout,
        recheck_limit=cfg.recheck_limit,
        # k=0.0 disables adaptive cooldown: each round's round_act_timeout
        # already waits for the full reboot+probe+warmup+open cycle. Without
        # this, recover_time (~180s) * k (1.5) = 270s extra sleep per round,
        # making 5-round reboot tests take 40+ minutes.
        scheduler=Scheduler(base=0.0, k=0.0, min_interval=0.0),
        telemetry=tel,
    )

    adapter = HikvisionAdapter(client)
    diagnostic = DiagnosticKernel(
        llm_decide=llm_decide,
        whitelist=[HEAL_TIME_SYNC, HEAL_RETRIGGER],
    )
    worker = HikvisionWorker(
        bus,
        AgentSpec(id="w1", role="hik",
                  subscriptions=["hikvision/plan"]),
        adapter, client, time_skew_threshold=3.0,
        diagnostic=diagnostic,
        run_reboot=run_reboot,
        probe_interval=probe_interval,
        probe_confirm_count=probe_confirm_count,
        warmup_time=warmup_time,
        max_recover_timeout=max_recover_timeout,
        event_check_delay=event_check_delay,
    )
    # Patch handle BEFORE start() so _dispatch picks up the new attribute
    _patch_worker_plan_handler(worker)

    advisor = HikvisionAdvisor(
        bus, AgentSpec(id="a1", role="risk"),
        instruction=instruction,
        llm_parse=llm_parse,
    )
    scribe = ScribeAgent(
        bus, AgentSpec(id="o1", role="scribe",
                       subscriptions=["loop/done", "agent/incident", "target/#"]),
    )
    # Watchdog stall_timeout must exceed max_recover_timeout + warmup_time
    # (reboot phase can block do_work for that long in a thread).
    stall_timeout = max(300.0, max_recover_timeout + warmup_time + 60.0)
    dog = Watchdog(bus, stall_timeout=stall_timeout, check_interval=0.05)

    for a in (worker, advisor, scribe, dog):
        await a.start()
    # Pre-loop setup: record baseline + baseline reboot + measure duration.
    # Done AFTER worker.start() (so it can publish) but BEFORE loop.start()
    # so the measured baseline_reboot_duration is available to do_work().
    # Run in a thread because pre_loop_setup may block 60-180s on the
    # baseline reboot probe; this must not stall the event loop.
    await asyncio.to_thread(worker.pre_loop_setup)
    # Advisor publishes hikvision/plan during start(); worker caches it.
    # Start loop AFTER advisor so the plan is already enqueued.
    await loop.start()
    try:
        await asyncio.wait_for(loop._task, run_timeout)
    finally:
        for a in (worker, advisor, scribe, dog):
            await a.stop()
    return {"ctx": ctx, "loop": loop, "worker": worker,
            "advisor": advisor, "telemetry": tel, "config": cfg}


__all__ = ["run_hikvision_stability"]
