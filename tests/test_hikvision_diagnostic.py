# tests/test_hikvision_diagnostic.py
from stability_harness_loop_multiagent.business.hikvision.diagnostic import (
    DiagnosticKernel, HEAL_TIME_SYNC, HEAL_RETRIGGER, HEAL_ABORT,
)


def test_diagnostic_selects_time_sync_when_skew_high():
    def fake_llm(env: dict) -> str:
        return HEAL_TIME_SYNC
    kernel = DiagnosticKernel(llm_decide=fake_llm,
                              whitelist=[HEAL_TIME_SYNC, HEAL_RETRIGGER])
    decision = kernel.diagnose({
        "time_skew_seconds": 10.5,
        "missing": ["remote_open", "lock_open"],
        "http_error": None,
    })
    assert decision == HEAL_TIME_SYNC


def test_diagnostic_aborts_when_decision_not_in_whitelist():
    def fake_llm(env: dict) -> str:
        return HEAL_TIME_SYNC
    kernel = DiagnosticKernel(llm_decide=fake_llm, whitelist=[HEAL_RETRIGGER])
    decision = kernel.diagnose({"time_skew_seconds": 10.0, "missing": []})
    assert decision == HEAL_ABORT


def test_diagnostic_passes_environment_to_llm():
    received = {}

    def capturing_llm(env: dict) -> str:
        received.update(env)
        return HEAL_RETRIGGER
    kernel = DiagnosticKernel(llm_decide=capturing_llm,
                              whitelist=[HEAL_RETRIGGER])
    kernel.diagnose({"time_skew_seconds": 2.0, "missing": ["lock_open"]})
    assert received["time_skew_seconds"] == 2.0
    assert received["missing"] == ["lock_open"]
