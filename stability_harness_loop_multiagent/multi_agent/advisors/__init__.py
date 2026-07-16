"""MAS 顾问角色（自主的监控/分析智能体；仅具建议性）。"""

from .base import AdvisorAgent
from .trend_supervisor import TrendSupervisorAgent
from .risk_analyst import RiskAnalyst

__all__ = ["AdvisorAgent", "TrendSupervisorAgent", "RiskAnalyst"]
