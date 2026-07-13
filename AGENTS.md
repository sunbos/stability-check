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
a policy layer (`AnalystAgent`) is consulted only on incidents (suspected power
loss / no recovery) and always degrades gracefully to a rule engine when LLM is
unavailable.

## 2. Architecture (Layered + Bus-Driven)

```
┌──────────────────────────────────────────────────────────────────┐
│ pytest harness (conftest + thin test entry; driving only)        │
├──────────────────────────────────────────────────────────────────┤
│ EventBus — the only inter-agent channel (pub/sub + req/resp)     │
├──────────────────────────────────────────────────────────────────┤
│ Deterministic Loop Core                                          │
│   Coordinator (reboot → recover → check → interval → abort)      │
├──────────────────────────────────────────────────────────────────┤
│ Role Agents (deterministic; reuse DeviceClient)                  │
│   RebootAgent / WatchAgent / EventCheckAgent / StatusCheckAgent  │
├──────────────────────────────────────────────────────────────────┤
│ Policy/Observability Agents (pluggable; LLM-first + rule fallback)│
│   AnalystAgent (incident decision + multi-angle analysis)        │
│   ScribeAgent  (chronicle: timeline + summary)                   │
│   NotifierAgent(pluggable channel: print + webhook hook)         │
│   ReporterAgent(aggregate stats + alert)                         │
└──────────────────────────────────────────────────────────────────┘
```

**Key principle**: the deterministic Loop Core (reproducible, low-maintenance)
is strictly separated from the LLM policy layer. The LLM never participates in
per-round pass/fail; it only decides "continue or stop" on incidents. If LLM is
unavailable (no key / rate-limited / timeout), the rule engine takes over and
the burn-in never deadlocks.

## 3. Directory Structure

```
.
├── AGENTS.md                       # this file
├── docs/plans/
│   └── 2026-07-13-burnin-multiagent-design.md   # full design rationale
└── tests/
    ├── conftest.py                 # reads os.environ → RunConfig + Baseline fixtures
    ├── test_burnin.py              # main session + policy-layer degradation tests
    ├── agents/                     # early implementations (mostly superseded by harness/)
    │   ├── config.py               # RunConfig / RoundResult / Baseline
    │   ├── device_client.py        # Digest + 3 ISAPI calls (reboot / AcsEvent / AcsWorkStatus)
    │   ├── strategy.py             # parses BURNIN_STRATEGY
    │   ├── report.py               # Reporter (aggregate stats + alert) — reused by harness
    │   ├── supervisor.py           # early single-process loop (superseded by harness/coordinator)
    │   ├── reboot_agent.py         # early (superseded)
    │   ├── event_check_agent.py    # early (superseded)
    │   └── status_check_agent.py   # early (superseded)
    └── harness/                    # the live multi-agent framework
        ├── bus.py                  # async EventBus (pub/sub + request/response + '#' wildcard)
        ├── agent.py                # Agent base class + AgentSpec
        ├── context.py              # RunContext (shared state) + TaskBoard (shared task list)
        ├── llm_client.py           # OpenAI-compatible chat client (stdlib; default OpenRouter)
        ├── loader.py               # build_system(cfg) → (bus, ctx, agents)
        ├── coordinator.py          # deterministic Loop Core (main driver)
        ├── reboot_agent.py         # executes reboot
        ├── watch_agent.py          # watches DOWN→UP recovery cycle
        ├── event_check_agent.py    # checks reboot event was logged
        ├── status_check_agent.py   # diffs work status vs baseline
        ├── reporter_agent.py       # bus observer: aggregates + alerts
        ├── analyst_agent.py        # policy layer: LLM decision + rule fallback + analysis
        ├── scribe_agent.py         # chronicle: timeline + summary
        └── notifier_agent.py       # pluggable notification channel
```

> Note: `tests/agents/` contains the early single-process implementation. The
> live framework is under `tests/harness/`. `device_client.py`, `config.py`,
> `strategy.py`, and `report.py` in `tests/agents/` are still reused as
> utilities by `harness/`.

## 4. Agents — Roles and Topic Contracts

| Agent | Role | Subscribes | Publishes | Device calls | In pass/fail? |
|-------|------|------------|-----------|--------------|----------------|
| `RebootAgent` | executor | `coord/reboot` | `reboot/done` | `reboot()` | no |
| `WatchAgent` | watcher | `reboot/done` | `device/recovered` | `get_work_status()` poll | no |
| `EventCheckAgent` | checker | `device/recovered` | `check/event` | `get_reboot_events()` | **yes** |
| `StatusCheckAgent` | checker | `device/recovered` | `check/status` | `get_work_status()` | **yes** |
| `Coordinator` | driver | `reboot/done`, `device/recovered`, `check/event`, `check/status`, `coord/abort` | `coord/reboot`, `round/done`, `incident/raise`, `coord/abort`, `analyst/advise` | none | **yes (decides)** |
| `ReporterAgent` | observer | `check/event`, `check/status`, `round/done`, `coord/abort` | none | none | no |
| `AnalystAgent` | policy | `analyst/advise`, `incident/raise`, `round/done` | `analyst/advise/reply`, `analyst/decision`, `analyst/report` | none | no (policy only) |
| `ScribeAgent` | chronicle | `round/done`, `incident/raise`, `analyst/decision`, `analyst/report`, `coord/abort`, `scribe/summary/request` | `scribe/summary` | none | no |
| `NotifierAgent` | notifier | `coord/abort`, `analyst/decision`, `analyst/report`, `notify` | none | none | no |

### Per-round topic flow

```
Coordinator --coord/reboot--> RebootAgent
RebootAgent --reboot/done----> WatchAgent, Coordinator
WatchAgent  --device/recovered--> EventCheckAgent, StatusCheckAgent, Coordinator
EventCheckAgent --check/event--> Coordinator, ReporterAgent
StatusCheckAgent --check/status-> Coordinator, ReporterAgent
Coordinator --round/done--> ReporterAgent, ScribeAgent, AnalystAgent
Coordinator --incident/raise--> ScribeAgent, NotifierAgent (on incidents)
Coordinator --analyst/advise (req/resp)--> AnalystAgent --analyst/advise/reply--> Coordinator
Coordinator --coord/abort--> everyone listening (Reporter / Scribe / Notifier)
```

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

## 6. Shared State — RunContext + TaskBoard

File: [tests/harness/context.py](tests/harness/context.py)

- `RunContext`: baseline snapshot, strategy_text, round_history, log, board.
  Single shared object held by all agents. Operations are async-safe by design
  (single event loop, no cross-loop access).
- `TaskBoard`: shared task list (statuses: `pending` / `doing` / `done` /
  `failed`). Maintained by the Coordinator; agents may read it directly.
- This is a "shared whiteboard" pattern: agents communicate state via the bus
  *and* read shared context. The Coordinator is the sole writer of authoritative
  round results and abort flags.

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

### Run a single agent standalone
Every agent module has a `__main__` block, e.g.:
```powershell
python tests/harness/scribe_agent.py
```

## 9. Key Design Principles

1. **Strict layering**: deterministic Loop Core never depends on LLM; LLM never
   decides per-round pass/fail.
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

## 11. Known Limitations

- `tests/agents/` retains early single-process implementations (`supervisor.py`,
  `reboot_agent.py`, etc.) that are **superseded** by `harness/`. Only
  `device_client.py`, `config.py`, `strategy.py`, `report.py` are still reused.
- The shared `RunContext` is a single in-memory object; not safe across
  processes (would need rework for a distributed bus).
- `NotifierAgent`'s webhook channel is a stub (`_send_webhook` is a no-op).
- LLM is consulted only on incidents; per-round LLM commentary is opt-in via
  `BURNIN_PER_ROUND_LLM=1` and only adds a `llm_note` field (no decision power).
