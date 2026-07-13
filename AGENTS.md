# AGENTS.md — Burn-in Stability Test Framework

> A pytest-based, event-bus-driven multi-agent framework for burn-in stability
> testing of Hikvision access-control devices (ISAPI + Digest auth).

## 1. Project Overview

This project repeatedly reboots a Hikvision access-control device
(`192.168.3.33`, ISAPI + Digest `admin / 121212..`) and verifies two invariants
after every reboot:

1. A remote-reboot event (`AcsEvent` with `major=3, minor=123`) is logged.
2. The device's work status (`AcsWorkStatus`) returns to the baseline snapshot.

A run is a single pytest session. A team of agents collaborates via an in-process
async event bus. The deterministic Loop Core (`Coordinator`) drives the main loop;
an autonomous policy layer (`AnalystAgent` / `RiskAnalyst` + `TrendSupervisorAgent`)
proactively monitors trends, votes on risk, and raises incidents — degrading
gracefully to a rule engine when LLM is unavailable. The Coordinator applies a
decision matrix that combines fact-layer dictatorship with risk-score modifiers,
ensuring the burn-in never deadlocks and safety is never compromised.

## 2. Architecture (Autonomous 4-Layer + Bus-Driven)

```
┌──────────────────────────────────────────────────────────────────┐
│ pytest harness (conftest + thin test entry; driving only)        │
├──────────────────────────────────────────────────────────────────┤
│ EventBus — the only inter-agent channel (pub/sub + req/resp)     │
├──────────────────────────────────────────────────────────────────┤
│ L1 — Executor Layer (deterministic; reuse DeviceClient)          │
│   RebootAgent / WatchAgent / EventCheckAgent / StatusCheckAgent  │
├──────────────────────────────────────────────────────────────────┤
│ L2 — Arbiter Layer (deterministic Loop Core)                     │
│   Coordinator (reboot → recover → check → vote → decide → abort) │
├──────────────────────────────────────────────────────────────────┤
│ L3 — Autonomous Layer (proactive; LLM-first + rule fallback)     │
│   TrendSupervisorAgent (rule-based trend detection + voting)     │
│   AnalystAgent / RiskAnalyst (LLM risk voting + proactive alert) │
├──────────────────────────────────────────────────────────────────┤
│ L4 — Output Layer (observability)                                │
│   ScribeAgent  (chronicle: private timeline + summary)           │
│   NotifierAgent(pluggable channel: print + webhook hook)         │
└──────────────────────────────────────────────────────────────────┘
```

**Key principle**: the deterministic Loop Core (L2, reproducible, low-maintenance)
is strictly separated from the autonomous policy layer (L3). L3 agents
proactively monitor trends, vote on risk, and raise incidents — but the
decision matrix ensures LLM/risk can **never** turn a fail into a pass (safety
bottom line). If LLM is unavailable (no key / rate-limited / timeout), the rule
engine takes over and the burn-in never deadlocks.

## 3. Directory Structure

```
.
├── AGENTS.md                       # this file
├── docs/plans/
│   ├── 2026-07-13-burnin-multiagent-design.md        # original design rationale
│   └── 2026-07-13-autonomous-multiagent-design.md    # autonomous-MAS redesign
└── tests/
    ├── conftest.py                 # reads os.environ → RunConfig + Baseline fixtures
    ├── test_burnin.py              # main session + policy-layer degradation tests
    ├── test_context.py             # ReadOnlyContext + CoordinatorContext unit tests
    ├── test_scribe.py              # ScribeAgent private timeline + summary tests
    ├── test_trend_supervisor.py    # TrendSupervisorAgent trend detection + voting tests
    ├── test_risk_analyst.py        # RiskAnalyst vote + proactive incident tests
    ├── test_coordinator_decisions.py # Coordinator decision matrix unit tests
    ├── agents/                     # early implementations (mostly superseded by harness/)
    │   ├── config.py               # RunConfig / RoundResult / Baseline
    │   ├── device_client.py        # Digest + 3 ISAPI calls (reboot / AcsEvent / AcsWorkStatus)
    │   ├── strategy.py             # parses BURNIN_STRATEGY
    │   ├── report.py               # Reporter (aggregate stats + alert) — reused by harness
    │   ├── supervisor.py           # early single-process loop (superseded by harness/coordinator)
    │   ├── reboot_agent.py         # early (superseded)
    │   ├── event_check_agent.py    # early (superseded)
    │   └── status_check_agent.py   # early (superseded)
    └── harness/                    # the live autonomous multi-agent framework
        ├── bus.py                  # async EventBus (pub/sub + request/response + '#' wildcard)
        ├── agent.py                # Agent base class + AgentSpec
        ├── context.py              # ReadOnlyContext + CoordinatorContext + TaskBoard
        ├── llm_client.py           # OpenAI-compatible chat client (stdlib; default OpenRouter)
        ├── loader.py               # build_system(cfg) → (bus, ctx, agents)
        ├── coordinator.py          # L2 Arbiter: Loop Core + decision matrix + incident ack
        ├── reboot_agent.py         # L1 Executor: executes reboot
        ├── watch_agent.py          # L1 Executor: watches DOWN→UP recovery cycle
        ├── event_check_agent.py    # L1 Executor: checks reboot event was logged
        ├── status_check_agent.py   # L1 Executor: diffs work status vs baseline
        ├── analyst_agent.py        # L3 Autonomous: RiskAnalyst (vote + advise + proactive incident)
        ├── trend_supervisor_agent.py # L3 Autonomous: rule-based trend detection + voting
        ├── scribe_agent.py         # L4 Output: chronicle (private timeline + summary)
        └── notifier_agent.py       # L4 Output: pluggable notification channel
```

> Note: `tests/agents/` contains the early single-process implementation. The
> live framework is under `tests/harness/`. `device_client.py`, `config.py`,
> `strategy.py`, and `report.py` in `tests/agents/` are still reused as
> utilities by `harness/`.

## 4. Agents — Roles and Topic Contracts

| Agent | Layer | Role | Subscribes | Publishes | Device calls | In pass/fail? |
|-------|-------|------|------------|-----------|--------------|----------------|
| `RebootAgent` | L1 | executor | `coord/reboot` | `reboot/done` | `reboot()` | no |
| `WatchAgent` | L1 | watcher | `reboot/done` | `device/recovered` | `get_work_status()` poll | no |
| `EventCheckAgent` | L1 | checker | `device/recovered` | `check/event` | `get_reboot_events()` | **yes** |
| `StatusCheckAgent` | L1 | checker | `device/recovered` | `check/status` | `get_work_status()` | **yes** |
| `Coordinator` | L2 | arbiter | `reboot/done`, `device/recovered`, `check/event`, `check/status`, `coord/abort`, `incident/raise`, `vote/reply` | `coord/reboot`, `round/done`, `incident/raise`, `coord/abort`, `analyst/advise`, `vote/request`, `incident/ack`, `coord/recheck` | none | **yes (decides)** |
| `TrendSupervisorAgent` | L3 | autonomous | `round/done`, `vote/request`, `coord/abort` | `incident/raise`, `vote/reply` | none | no (advisory) |
| `AnalystAgent` | L3 | autonomous | `analyst/advise`, `incident/raise`, `round/done`, `vote/request`, `coord/abort` | `analyst/advise/reply`, `analyst/decision`, `analyst/report`, `incident/raise`, `vote/reply` | none | no (advisory) |
| `ScribeAgent` | L4 | chronicle | `round/done`, `incident/raise`, `analyst/decision`, `analyst/report`, `coord/abort`, `scribe/summary/request` | `scribe/summary` | none | no |
| `NotifierAgent` | L4 | notifier | `coord/abort`, `analyst/decision`, `analyst/report`, `incident/raise`, `notify` | none | none | no |

### Per-round topic flow

```
Coordinator --coord/reboot--> RebootAgent
RebootAgent --reboot/done----> WatchAgent, Coordinator
WatchAgent  --device/recovered--> EventCheckAgent, StatusCheckAgent, Coordinator
EventCheckAgent --check/event--> Coordinator
StatusCheckAgent --check/status-> Coordinator
Coordinator --vote/request--> TrendSupervisorAgent, AnalystAgent (on clean pass)
TrendSupervisorAgent --vote/reply--> Coordinator
AnalystAgent --vote/reply--> Coordinator
Coordinator --round/done--> ScribeAgent, TrendSupervisorAgent, AnalystAgent
TrendSupervisorAgent --incident/raise--> Coordinator, ScribeAgent, NotifierAgent (proactive)
AnalystAgent --incident/raise--> Coordinator, ScribeAgent, NotifierAgent (proactive)
Coordinator --incident/ack--> (acknowledges non-self incidents)
Coordinator --incident/raise--> ScribeAgent, NotifierAgent (on no-recovery)
Coordinator --analyst/advise (req/resp)--> AnalystAgent --analyst/advise/reply--> Coordinator
Coordinator --coord/abort--> everyone listening (Scribe / Notifier / TrendSupervisor / Analyst)
```

### Decision matrix (design §5.4)

The Coordinator applies a decision matrix after collecting votes on clean passes:

| Fact layer | Risk score | Critical incident | Decision |
|------------|-----------|-------------------|----------|
| `found=False` or `changed=True` | any | any | **fail** (dictatorship) |
| `found=True` and `changed=False` | < 60 | no | **pass** |
| `found=True` and `changed=False` | 60–80 | no | **warn** |
| `found=True` and `changed=False` | > 80 | no | **recheck** |
| `found=True` and `changed=False` | any | yes | **recheck** |

**Safety bottom line**: risk score can **never** turn a fail into a pass. The
decision matrix is advisory — `passed` flag remains purely fact-based; `decision`
and `risk_score` fields are added to the round record for observability.

## 5. Communication — EventBus

File: [tests/harness/bus.py](tests/harness/bus.py)

- In-process async bus; stdlib only (`asyncio`, `secrets`).
- `publish(topic, message)` — broadcast to all matching handlers.
- `subscribe(topic, handler)` — handlers may be sync or async.
- `request(topic, message, timeout)` — publish + await first reply on
  `topic/reply` correlated by `req_id`. Raises `TimeoutError` on timeout.
- Topic matching: exact or trailing `#` wildcard (`a/#` matches `a`, `a/b`, ...).
- **Agents never call each other directly** — only via the bus. This keeps the
  door open to swapping the in-process bus for a network transport without
  touching agent code.

## 6. Shared State — ReadOnlyContext + CoordinatorContext + TaskBoard

File: [tests/harness/context.py](tests/harness/context.py)

- `ReadOnlyContext`: read-only view held by all agents. Contains baseline
  (injected at startup, immutable), strategy_text (immutable),
  round_history_snapshot (immutable tuple, refreshed by Coordinator after each
  round broadcast), aborted (read-only).
- `CoordinatorContext`: writable subclass held **only** by Coordinator.
  Provides `append_round` / `mark_aborted` / `publish_state` and other write
  methods. Each write refreshes `round_history_snapshot` (immutable tuple).
- `TaskBoard`: shared task list (statuses: `pending` / `doing` / `done` /
  `failed`). Maintained by the Coordinator; agents may read it directly.
- **Private state principle**: L3 autonomous agents maintain private state
  (windows, counters) and do **not** read `ctx.round_history` directly. They
  subscribe to `round/done` and accumulate their own private windows. This
  enforces the autonomous-MAS principle that agents are self-contained.
- `RunContext` is kept as a backward-compat alias for `CoordinatorContext`.

## 7. Configuration (Environment Variables)

Read by `tests/agents/config.py` via `load_config_from_env()`; injected through
`tests/conftest.py`.

### Run parameters
| Variable | Default | Meaning |
|----------|---------|---------|
| `BURNIN_STRATEGY` | `""` | Natural-language strategy hint (parsed by `strategy.py`) |
| `BURNIN_MAX_ROUNDS` | `0` (∞) | Max rounds |
| `BURNIN_MAX_DURATION` | `0` (∞) | Max total seconds |
| `BURNIN_BASE_INTERVAL` | `60` | Base cooldown between rounds (s) |
| `BURNIN_INTERVAL_MIN` / `BURNIN_INTERVAL_MAX` | `30` / `600` | Adaptive interval bounds (s) |
| `BURNIN_RECOVER_TIMEOUT` | `180` | Per-round recovery timeout (s) |
| `BURNIN_FAIL_THRESHOLD` | `5` | Cumulative failures → abort |
| `BURNIN_FAIL_CONSECUTIVE` | `3` | Consecutive failures → abort |
| `BURNIN_K` | `1.5` | Adaptive interval multiplier |
| `BURNIN_EVENT_WINDOW` | `30` | Event-check window (s) |
| `BURNIN_PER_ROUND_LLM` | `0` | If `1/true/on`, Analyst LLM comments every round |
| `BURNIN_NOTIFIER` | `print` | Notifier channel: `print` or `webhook` |
| `BURNIN_VOTE_TIMEOUT` | `1.0` | Vote collection timeout per round (s) |

### Device credentials
| Variable | Default | Meaning |
|----------|---------|---------|
| `BURNIN_HOST` | `192.168.3.33` | Device host |
| `BURNIN_USER` | `admin` | Digest user |
| `BURNIN_PASSWORD` | (required) | Digest password |

### LLM (OpenAI-compatible; default OpenRouter)
| Variable | Fallback | Meaning |
|----------|----------|---------|
| `LLM_API_KEY` | `OPENROUTER_API_KEY` | API key (preferred name) |
| `LLM_BASE_URL` | `OPENROUTER_BASE_URL` → `https://openrouter.ai/api/v1` | Base URL |
| `LLM_MODEL` | `OPENROUTER_MODEL` → `tencent/hy3:free` | Model name |

Switch platform by setting `LLM_BASE_URL` (e.g. DeepSeek `https://api.deepseek.com/v1`,
Moonshot `https://api.moonshot.cn/v1`, local Ollama `http://localhost:11434/v1`).
Keys are read from env or repo-root `.env` (`.env` is gitignored); never logged.

## 8. Running

### Full burn-in session (needs real device)
```powershell
$env:BURNIN_PASSWORD = "121212.."
python -m pytest tests/test_burnin.py::test_burnin_session -v -s
```

### Policy-layer tests (no device needed)
```powershell
python -m pytest tests/test_burnin.py::test_analyst_rulebased_degradation `
                  tests/test_burnin.py::test_coordinator_consults_analyst_on_no_recovery -v -s
```

### Autonomous-layer unit tests (no device needed)
```powershell
python -m pytest tests/test_context.py tests/test_scribe.py `
                  tests/test_trend_supervisor.py tests/test_risk_analyst.py `
                  tests/test_coordinator_decisions.py -v
```

### Run a single agent standalone
Every agent module has a `__main__` block, e.g.:
```powershell
python tests/harness/scribe_agent.py
```

## 9. Key Design Principles

1. **Strict layering**: deterministic Loop Core (L2) never depends on LLM; LLM
   never decides per-round pass/fail. L3 autonomous agents are advisory only.
2. **Bus-only inter-agent communication**: no direct method calls between
   agents; this enables future distributed deployment.
3. **Graceful degradation**: LLM unavailability → rule engine; device
   unreachable → recorded as failure; never deadlocks.
4. **Adaptive interval**: `next = clamp(recover_time × k + base, MIN, MAX)` —
   slow device → longer cooldown; fast device → tight loop. No manual tuning.
5. **Stdlib only**: no third-party dependencies (uses `urllib`, `asyncio`,
   `secrets`, `dataclasses`). The LLM client also uses `urllib`.
6. **Safety**: API keys only from env / `.env`; never hardcoded, never printed.
7. **Reproducibility**: per-round pass/fail is purely deterministic; LLM is
   advisory and overridable.
8. **Autonomous proactive monitoring**: L3 agents (TrendSupervisor +
   RiskAnalyst) proactively detect trends and raise incidents without being
   asked. This is the core autonomy property — agents don't just respond to
   queries, they independently monitor and alert.
9. **Decision matrix safety**: fact layer is dictatorial (found/changed →
   fail); risk score can only add warn/recheck markers, never override a fail.
   Critical incidents force recheck regardless of risk score.
10. **Mandatory incident ack**: Coordinator must ack every incident raised by
    other agents (forced echo), but never acks its own. This ensures no
    incident goes unnoticed.
11. **Private state isolation**: L3 agents maintain private windows/counters
    and do not read shared context's round_history directly. They subscribe to
    `round/done` and accumulate their own state.

## 10. Failure Modes and Degradation

| Failure | Detection | Response |
|---------|-----------|----------|
| Device doesn't recover | `WatchAgent` poll timeout | `device/recovered` with `t_recover=None` → Coordinator records failure |
| Reboot event missing | `EventCheckAgent` finds no `3/123` in window | `check/event` `found=False` → round fails |
| Status drift | `StatusCheckAgent` diff vs baseline | `check/status` `changed=True` → round fails |
| Cumulative failures ≥ threshold | Coordinator counter | `coord/abort` → graceful shutdown |
| Consecutive failures ≥ threshold | Coordinator counter | `coord/abort` → graceful shutdown |
| LLM unavailable / timeout | `AnalystAgent._ensure_llm` returns None | Rule engine decides; Coordinator's `_consult_analyst` returns None → deterministic fallback |
| Analyst advises stop | `analyst/advise/reply` `continue=False` | Coordinator aborts |
| Analyst advises continue | `analyst/advise/reply` `continue=True` | Coordinator records failure, threshold still applies |
| No voters reply | `_collect_votes` timeout | Default neutral risk (50) → decision matrix treats as pass |
| All voters abstain | `_combine_votes` returns `all_abstain` | Risk score = 50 → decision matrix treats as pass |
| TrendSupervisor detects increment streak | 3 consecutive → warn; 5 → critical | `incident/raise` → Coordinator acks; critical forces recheck |
| TrendSupervisor detects fail rate > 30% | ≥5 samples, upward crossing | `incident/raise` (warn) → Coordinator logs |
| TrendSupervisor detects recover time spike | > 2× avg(history) | `incident/raise` (warn) → Coordinator logs |
| RiskAnalyst: 3 consecutive high risk | risk > 80 for 3 rounds | `incident/raise` (critical) → Coordinator forces recheck |
| RiskAnalyst: single very high risk | risk ≥ 90 | `incident/raise` (warn) → Coordinator logs |
| Critical incident raised | L3 agent publishes `incident/raise` severity=critical | Coordinator acks + sets `_has_critical_incident` → decision matrix forces recheck |

## 11. Known Limitations

- `tests/agents/` retains early single-process implementations (`supervisor.py`,
  `reboot_agent.py`, etc.) that are **superseded** by `harness/`. Only
  `device_client.py`, `config.py`, `strategy.py`, `report.py` are still reused.
- `ReporterAgent` has been removed from the live framework (Phase 3); its
  functionality is absorbed by `ScribeAgent` + `NotifierAgent`.
- The `CoordinatorContext` is a single in-memory object; not safe across
  processes (would need rework for a distributed bus).
- `NotifierAgent`'s webhook channel is a stub (`_send_webhook` is a no-op).
- The `coord/recheck` topic is published but no agent currently subscribes to
  trigger an actual recheck round. The decision matrix marks rounds as
  `recheck` in the record, but the recheck mechanism itself is future work.
- LLM is consulted on incidents (advise) and per-round voting (vote); per-round
  LLM commentary is opt-in via `BURNIN_PER_ROUND_LLM=1`.
- `test_burnin_session` requires a real device and is not run in CI; only
  policy-layer and autonomous-layer unit tests are device-free.
