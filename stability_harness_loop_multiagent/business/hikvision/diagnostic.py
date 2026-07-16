"""LLM diagnostic kernel: Worker-internal submodule (NOT an independent Agent).

Called from HikvisionWorker.recover() to select a self-heal sub-flow.
LLM is injected as a callable to keep tests deterministic without mocking.
"""

from typing import Callable, Dict

HEAL_TIME_SYNC = "time_sync"
HEAL_WAIT_NETWORK = "wait_network"
HEAL_RETRIGGER = "retrigger"
HEAL_ABORT = "abort"


class DiagnosticKernel:
    """Selects a self-heal sub-flow from environment facts via LLM."""

    def __init__(self, llm_decide: Callable[[Dict], str],
                 whitelist: list) -> None:
        self._llm_decide = llm_decide
        self._whitelist = list(whitelist)

    def diagnose(self, env: Dict) -> str:
        """Return a whitelisted heal sub-flow, or HEAL_ABORT."""
        decision = self._llm_decide(dict(env))
        if decision in self._whitelist:
            return decision
        return HEAL_ABORT


__all__ = [
    "DiagnosticKernel",
    "HEAL_TIME_SYNC", "HEAL_WAIT_NETWORK", "HEAL_RETRIGGER", "HEAL_ABORT",
]
