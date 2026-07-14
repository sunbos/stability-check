"""MAS advisor roles (autonomous monitoring/analysis agents; advisory only)."""

from .base import AdvisorAgent
from .trend_supervisor import TrendSupervisorAgent
from .risk_analyst import RiskAnalyst

__all__ = ["AdvisorAgent", "TrendSupervisorAgent", "RiskAnalyst"]
