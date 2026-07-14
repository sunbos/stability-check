"""Verification — guardrails, input/output validation hooks, eval hooks.

Generic, scenario-agnostic. Workers/harness invoke the Verifier around boundaries:
  - input guardrails  validate/transform an inbound request before it drives an agent.
  - output guardrails validate an agent's outgoing message/result before publish.
  - eval hooks        score a round/result against expectations (assertions).

A guardrail hook is a callable ``fn(item) -> None | (ok, reason)``. Returning a falsy
``ok`` (or raising ``VerifyError``) is a failure. By default the Verifier fails *closed*:
a blocking failure short-circuits the chain and raises ``VerifyError`` so the caller
cannot proceed. ``run_eval`` never raises; it aggregates ``EvalResult`` into an
``EvalReport`` (with a combined score and ``passed`` flag).

``VerificationAgent`` mounts the Verifier on the bus: it subscribes to
``harness/verify/request`` and replies allow/deny via req_id.

Engine isolation: imports only from this harness package (bus, agent).
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple

from .agent import Agent, AgentSpec
from .bus import EventBus


class VerifyError(Exception):
    """Raised when a guardrail blocks (fail-closed)."""

    def __init__(self, reason: str, stage: str = "", hook: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.stage = stage
        self.hook = hook


Guardrail = Callable[[Any], Any]


@dataclass
class VerifyResult:
    ok: bool
    reason: str = ""
    hook: str = ""
    stage: str = ""


@dataclass
class EvalResult:
    name: str
    ok: bool
    score: float = 0.0
    reason: str = ""


@dataclass
class EvalReport:
    results: List[EvalResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.ok for r in self.results)

    @property
    def score(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.score for r in self.results) / len(self.results)

    def as_dict(self) -> dict:
        return {
            "passed": self.passed,
            "score": self.score,
            "results": [r.__dict__ for r in self.results],
        }


class Verifier:
    def __init__(self, *, fail_closed: bool = True) -> None:
        self.fail_closed = fail_closed
        self._input: List[Tuple[str, Guardrail]] = []
        self._output: List[Tuple[str, Guardrail]] = []
        self._eval: List[Tuple[str, Guardrail]] = []
        self._log = logging.getLogger("stability_harness_loop_multiagent.verify")

    # ---- registration ----------------------------------------------
    def add_input_guardrail(self, name: str, fn: Guardrail) -> "Verifier":
        self._input.append((name, fn))
        return self

    def add_output_guardrail(self, name: str, fn: Guardrail) -> "Verifier":
        self._output.append((name, fn))
        return self

    def add_eval_hook(self, name: str, fn: Guardrail) -> "Verifier":
        self._eval.append((name, fn))
        return self

    # ---- validation -------------------------------------------------
    def _run_chain(self, chain: List[Tuple[str, Guardrail]], stage: str, item: Any):
        for name, fn in chain:
            try:
                res = fn(item)
            except VerifyError as e:
                e.stage = stage
                e.hook = name
                return False, e.reason, name
            except Exception as e:  # noqa: BLE001 - guardrail failures are policy failures
                return False, f"{name}: {e}", name
            if res is not None:
                if isinstance(res, tuple):
                    ok = bool(res[0])
                    reason = str(res[1]) if len(res) > 1 else ""
                else:
                    ok = bool(res)
                    reason = ""
                if not ok:
                    return False, reason or f"{name} rejected", name
        return True, "", ""

    def validate_input(self, item: Any) -> VerifyResult:
        ok, reason, hook = self._run_chain(self._input, "input", item)
        return self._result(ok, reason, hook, "input")

    def validate_output(self, item: Any) -> VerifyResult:
        ok, reason, hook = self._run_chain(self._output, "output", item)
        return self._result(ok, reason, hook, "output")

    def _result(self, ok: bool, reason: str, hook: str, stage: str) -> VerifyResult:
        if not ok and self.fail_closed:
            raise VerifyError(reason, stage=stage, hook=hook)
        return VerifyResult(ok=ok, reason=reason, hook=hook, stage=stage)

    # ---- evaluation -------------------------------------------------
    def run_eval(self, record: Any) -> EvalReport:
        results: List[EvalResult] = []
        for name, fn in self._eval:
            try:
                res = fn(record)
            except Exception as e:  # noqa: BLE001
                results.append(EvalResult(name=name, ok=False, reason=str(e), score=0.0))
                continue
            if isinstance(res, EvalResult):
                results.append(res)
            elif isinstance(res, (int, float)):
                results.append(EvalResult(name=name, ok=bool(res), reason="", score=float(res)))
            elif isinstance(res, tuple):
                score = float(res[0]) if len(res) > 0 else 0.0
                ok = bool(res[1]) if len(res) > 1 else True
                reason = str(res[2]) if len(res) > 2 else ""
                results.append(EvalResult(name=name, ok=ok, reason=reason, score=score))
            else:
                ok = bool(res)
                results.append(
                    EvalResult(name=name, ok=ok, reason="", score=1.0 if ok else 0.0)
                )
        return EvalReport(results)


class VerificationAgent(Agent):
    """Bus-native verifier. Replies allow/deny to ``harness/verify/request``."""

    def __init__(
        self,
        bus: EventBus,
        verifier: Verifier,
        *,
        topic: str = "harness/verify/request",
    ) -> None:
        super().__init__(
            bus,
            AgentSpec(
                id="verify",
                role="verify",
                capabilities={"guardrails", "validation", "eval"},
                subscriptions=[topic],
            ),
        )
        self.verifier = verifier
        self.topic = topic

    async def handle(self, topic: str, message) -> None:
        if topic != self.topic:
            return
        req = message if isinstance(message, dict) else {}
        stage = req.get("stage", "input")
        item = req.get("item", req)
        try:
            if stage == "output":
                res = self.verifier.validate_output(item)
            else:
                res = self.verifier.validate_input(item)
            self.respond(message, {"allowed": res.ok, "reason": res.reason, "hook": res.hook})
        except VerifyError as e:
            self.respond(message, {"allowed": False, "reason": e.reason, "hook": e.hook})


__all__ = [
    "Verifier",
    "VerifyError",
    "VerificationAgent",
    "VerifyResult",
    "EvalResult",
    "EvalReport",
]
