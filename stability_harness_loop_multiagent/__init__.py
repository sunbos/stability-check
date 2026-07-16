"""stability_harness_loop_multiagent —— 通用的、与领域无关的“三引擎”自治循环框架。

三个引擎，一个接缝（即 EventBus）：
  - harness/  运行时 + 治理（事件总线、智能体生命周期、看门狗、遥测）
  - loop/     确定性的控制循环 + 决策权 + 终止条件
  - multi_agent/    多智能体系统（TargetAdapter、工作者、顾问、观察者）

各引擎之间从不互相 import；所有跨引擎通信都通过事件总线完成。
"""

from .harness.bus import EventBus
from .harness.agent import Agent, AgentSpec
from .harness.watchdog import Watchdog
from .harness.runtime import Runtime
from .harness.governance import (
    Governance,
    GovernanceAgent,
    AccessControl,
    Quota,
    Budget,
    CircuitBreaker,
)
from .harness.verify import (
    Verifier,
    VerifyError,
    VerificationAgent,
    VerifyResult,
    EvalResult,
    EvalReport,
)
from .harness.telemetry import Telemetry, Sink, PrintSink, MemorySink, NullSink
from .loop.context import SharedContext, ReadOnlyContext, RoundRecord
from .loop.termination import (
    StopCondition,
    TerminationPolicy,
    CountStop,
    DurationStop,
    FailThresholdStop,
    ExternalAbortStop,
    ExternalStop,
)
from .loop.decision import (
    DecisionAuthority,
    Verdict,
    NEUTRAL_RISK,
    CONSERVATIVE_RISK,
)
from .loop.scheduler import Scheduler, RetryBudget, clamp
from .loop.driver import ControlLoop, RunConfig
from .multi_agent.adapter import TargetAdapter, Event, Result, State
from .multi_agent.protocols import AdvisorContract, ObserverContract, combine_votes
from .multi_agent.workers.base import WorkerAgent
from .multi_agent.workers.example import ExampleWorkerAgent
from .multi_agent.advisors.base import AdvisorAgent
from .multi_agent.advisors.trend_supervisor import TrendSupervisorAgent
from .multi_agent.advisors.risk_analyst import RiskAnalyst
from .multi_agent.observers.base import ObserverAgent
from .multi_agent.observers.scribe import ScribeAgent
from .multi_agent.observers.notifier import NotifierAgent

__all__ = [
    "EventBus",
    "Agent",
    "AgentSpec",
    "Watchdog",
    "Runtime",
    "Governance",
    "GovernanceAgent",
    "AccessControl",
    "Quota",
    "Budget",
    "CircuitBreaker",
    "Verifier",
    "VerifyError",
    "VerificationAgent",
    "VerifyResult",
    "EvalResult",
    "EvalReport",
    "Telemetry",
    "Sink",
    "PrintSink",
    "MemorySink",
    "NullSink",
    "SharedContext",
    "ReadOnlyContext",
    "RoundRecord",
    "StopCondition",
    "TerminationPolicy",
    "CountStop",
    "DurationStop",
    "FailThresholdStop",
    "ExternalAbortStop",
    "ExternalStop",
    "DecisionAuthority",
    "Verdict",
    "NEUTRAL_RISK",
    "CONSERVATIVE_RISK",
    "Scheduler",
    "RetryBudget",
    "clamp",
    "ControlLoop",
    "RunConfig",
    "TargetAdapter",
    "Event",
    "Result",
    "State",
    "AdvisorContract",
    "ObserverContract",
    "combine_votes",
    "WorkerAgent",
    "ExampleWorkerAgent",
    "AdvisorAgent",
    "TrendSupervisorAgent",
    "RiskAnalyst",
    "ObserverAgent",
    "ScribeAgent",
    "NotifierAgent",
]
