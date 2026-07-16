"""Real-environment smoke for Hikvision door stability.

Runs the full stack against:
  - A real Hikvision device (HTTP ISAPI + Digest Auth via urllib)
  - A real LLM via OpenRouter (tencent/hy3:free), auto-detected from
    LLM_API_KEY / OPENROUTER_API_KEY / .env; falls back to deterministic
    rule-based callables when no key is available.

Device defaults come from configs/door_restart_stability.yaml (master branch
test scenario: 192.168.3.33/admin/121212..). Override via env vars:
    BURNIN_HOST / BURNIN_USER / BURNIN_PASSWORD / BURNIN_STRATEGY

Usage:
    python -m stability_harness_loop_multiagent.examples.hikvision_real_env
    python -m stability_harness_loop_multiagent.examples.hikvision_real_env --rounds 1
    python -m stability_harness_loop_multiagent.examples.hikvision_real_env --no-llm

Per-round progress is printed in real-time with clear separators
(verdict + facts + risk) so you can watch the loop run live.
"""

import argparse
import asyncio
import os
import sys

# Make package importable when run as a bare script.
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from stability_harness_loop_multiagent.business.hikvision.adapter import HikvisionAdapter
from stability_harness_loop_multiagent.business.hikvision.advisor import HikvisionAdvisor
from stability_harness_loop_multiagent.business.hikvision.client import HikvisionClient
from stability_harness_loop_multiagent.business.hikvision.diagnostic import (
    DiagnosticKernel, HEAL_RETRIGGER, HEAL_TIME_SYNC,
)
from stability_harness_loop_multiagent.business.hikvision.llm import get_client
from stability_harness_loop_multiagent.business.hikvision.runner import (
    _default_llm_decide, _default_parse, _make_llm_decide, _make_llm_parse,
    _patch_worker_plan_handler,
)
from stability_harness_loop_multiagent.business.hikvision.worker import HikvisionWorker
from stability_harness_loop_multiagent.harness.agent import AgentSpec
from stability_harness_loop_multiagent.harness.bus import EventBus
from stability_harness_loop_multiagent.harness.telemetry import MemorySink, Telemetry
from stability_harness_loop_multiagent.harness.watchdog import Watchdog
from stability_harness_loop_multiagent.loop.context import SharedContext
from stability_harness_loop_multiagent.loop.decision import DecisionAuthority
from stability_harness_loop_multiagent.loop.driver import ControlLoop, RunConfig
from stability_harness_loop_multiagent.loop.scheduler import Scheduler
from stability_harness_loop_multiagent.multi_agent.observers.scribe import ScribeAgent


def _print_header(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"=== {title}")
    print("=" * 72)


def _print_round(round_no: int, verdict: str, risk: float, facts: dict) -> None:
    print(f"\n--- Round {round_no} ---")
    print(f"  verdict: {verdict}")
    print(f"  risk:    {risk:.1f}")
    print(f"  facts:")
    for k, v in facts.items():
        shown = v if not isinstance(v, dict) else f"<meta: {v}>"
        print(f"    {k}: {shown}")


async def _run_with_progress(
    client: HikvisionClient,
    max_rounds: int,
    run_timeout: float,
    instruction: str,
    use_llm: bool,
    *,
    run_reboot: bool = True,
    probe_interval: float = 5.0,
    probe_confirm_count: int = 2,
    warmup_time: float = 60.0,
    max_recover_timeout: float = 180.0,
) -> dict:
    """Custom runner that prints per-round progress in real time.

    Mirrors run_hikvision_stability but subscribes a printer to loop/done
    BEFORE loop.start() so the user sees each round live. Reboot/probe/warmup
    config (spec §4.1, §4.2, §6) forwarded to HikvisionWorker.
    """
    if use_llm:
        llm_parse = _make_llm_parse() or _default_parse
        llm_decide = _make_llm_decide() or _default_llm_decide
    else:
        llm_parse = _default_parse
        llm_decide = _default_llm_decide

    bus = EventBus()
    mem = MemorySink()
    tel = Telemetry(bus=bus, sinks=[mem])
    ctx = SharedContext(baseline={"kind": "hikvision"}, strategy_text=instruction)
    decision = DecisionAuthority()
    cfg = RunConfig(max_rounds=max_rounds, max_duration=0.0, fail_threshold=10_000,
                    vote_timeout=0.1, recover_timeout=2.0, check_timeout=2.0,
                    recheck_limit=0)
    term = cfg.build_termination()
    loop = ControlLoop(
        bus, ctx, decision, term,
        vote_timeout=cfg.vote_timeout,
        recover_timeout=cfg.recover_timeout,
        check_timeout=cfg.check_timeout,
        recheck_limit=cfg.recheck_limit,
        scheduler=Scheduler(base=0.0, min_interval=0.0),
        telemetry=tel,
    )

    # Real-time progress printer: subscribe to loop/done BEFORE loop.start()
    def _on_loop_done(_topic, msg):
        if not isinstance(msg, dict):
            return
        _print_round(
            msg.get("round", 0),
            msg.get("verdict", "?"),
            float(msg.get("risk", 0.0)),
            msg.get("facts", {}) or {},
        )
    bus.subscribe("loop/done", _on_loop_done)

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
    )
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
    # Watchdog stall_timeout must exceed max_recover_timeout + warmup_time.
    stall_timeout = max(300.0, max_recover_timeout + warmup_time + 60.0)
    dog = Watchdog(bus, stall_timeout=stall_timeout, check_interval=0.05)

    for a in (worker, advisor, scribe, dog):
        await a.start()
    await loop.start()
    try:
        await asyncio.wait_for(loop._task, run_timeout)
    finally:
        for a in (worker, advisor, scribe, dog):
            await a.stop()
    return {"ctx": ctx, "loop": loop, "worker": worker,
            "advisor": advisor, "telemetry": tel, "config": cfg}


async def _main() -> int:
    parser = argparse.ArgumentParser(description="Hikvision real-env smoke")
    parser.add_argument("--rounds", type=int, default=3,
                        help="Max rounds (default 3)")
    parser.add_argument("--timeout", type=float, default=60.0,
                        help="Run timeout in seconds (default 60)")
    parser.add_argument("--no-llm", action="store_true",
                        help="Force rule-based fallback (skip LLM)")
    parser.add_argument("--host", default=None,
                        help="Override BURNIN_HOST")
    parser.add_argument("--user", default=None,
                        help="Override BURNIN_USER")
    parser.add_argument("--password", default=None,
                        help="Override BURNIN_PASSWORD")
    # Reboot + probe + warmup config (spec §4.1, §4.2, §6 worker.*)
    parser.add_argument("--no-reboot", action="store_true",
                        help="Skip reboot phase (only remote_open_door each round)")
    parser.add_argument("--warmup", type=float, default=60.0,
                        help="Warmup seconds after device online (spec §6, default 60)")
    parser.add_argument("--probe-interval", type=float, default=5.0,
                        help="Probe poll interval seconds (spec §4.1, default 5)")
    parser.add_argument("--probe-confirm", type=int, default=2,
                        help="Consecutive successes to confirm online (spec §4.1, default 2)")
    parser.add_argument("--max-recover", type=float, default=180.0,
                        help="Max seconds to wait for device online after reboot (default 180)")
    args = parser.parse_args()

    # Device config: env (BURNIN_*) -> CLI args -> master defaults
    host = args.host or os.environ.get("BURNIN_HOST") or "192.168.3.33"
    user = args.user or os.environ.get("BURNIN_USER") or "admin"
    pwd = args.password or os.environ.get("BURNIN_PASSWORD") or "121212.."
    instruction = os.environ.get("BURNIN_STRATEGY", "")
    run_reboot = not args.no_reboot

    _print_header("Hikvision real-env smoke")
    print(f"Device:    {host}:80 (user={user})")
    print(f"Rounds:    {args.rounds}")
    print(f"Timeout:   {args.timeout}s")
    print(f"LLM:       {'DISABLED (--no-llm)' if args.no_llm else 'auto-detect (LLM_API_KEY/.env)'}")
    print(f"Strategy:  {instruction!r}")
    print(f"Reboot:    {'ENABLED (reboot -> probe -> warmup -> open)' if run_reboot else 'DISABLED (--no-reboot, only open)'}")
    if run_reboot:
        print(f"  probe_interval={args.probe_interval}s, probe_confirm={args.probe_confirm}, "
              f"warmup={args.warmup}s, max_recover={args.max_recover}s")

    # LLM detection preview (does not call the API)
    if not args.no_llm:
        llm = get_client()
        if llm is None:
            print("  LLM status: NO KEY -> rule-based fallback (set LLM_API_KEY in .env)")
        else:
            print(f"  LLM status: ready (model={llm.model}, base={llm.base_url})")

    _print_header("Connecting to device")
    client = HikvisionClient(host=host, port=80, username=user, password=pwd,
                             http_timeout=5.0)
    try:
        status = client.get_work_status()
        print(f"  AcsWorkStatus: {status.get('AcsWorkStatus', {})}")
        t = client.get_time().get("Time", {})
        print(f"  Device time:   {t.get('localTime')} (tz={t.get('timeZone')})")
    except Exception as exc:  # noqa: BLE001
        print(f"  ERROR connecting: {exc}")
        print("  Check device reachability / credentials. Aborting.")
        return 2

    _print_header("Running stability loop (per-round progress below)")
    try:
        result = await _run_with_progress(
            client=client,
            max_rounds=args.rounds,
            run_timeout=args.timeout,
            instruction=instruction,
            use_llm=not args.no_llm,
            run_reboot=run_reboot,
            probe_interval=args.probe_interval,
            probe_confirm_count=args.probe_confirm,
            warmup_time=args.warmup,
            max_recover_timeout=args.max_recover,
        )
    except asyncio.TimeoutError:
        print("\nERROR: run timed out after", args.timeout, "s")
        return 3
    except Exception as exc:  # noqa: BLE001
        print(f"\nERROR during run: {exc}")
        return 4

    # Final summary
    _print_header("Summary")
    ctx = result["ctx"]
    history = ctx.snapshot().round_history
    print(f"  Total rounds:  {ctx.round_count}")
    print(f"  Aborted:       {ctx.aborted} (reason: {ctx.snapshot().abort_reason!r})")
    decisions = {}
    for r in history:
        decisions[r.verdict] = decisions.get(r.verdict, 0) + 1
    print(f"  Verdict dist:  {decisions}")
    print(f"  Final verdict: {result['loop'].verdict.decision}")
    print(f"  Worker state:  {result['worker'].state}")
    print(f"  Last stages:   {result['worker']._last_work_stages}")

    # Exit code: 0 if all pass, 1 if any fail
    return 0 if all(r.verdict == "pass" for r in history) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
