"""Execution module for dual-leg order management."""

from .executor import PairedExecutor, ExecutionResult, LegStatus

__all__ = ["PairedExecutor", "ExecutionResult", "LegStatus"]
