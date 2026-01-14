"""
Metrics collection for monitoring bot performance.
"""

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


@dataclass
class TradeMetrics:
    """Metrics for a single trade."""
    execution_id: str
    condition_id: str
    entry_time: float
    exit_time: Optional[float] = None
    entry_cost: Decimal = Decimal("0")
    exit_proceeds: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    holding_time_seconds: float = 0
    success: bool = False


@dataclass
class SessionMetrics:
    """Metrics for a trading session."""
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    # Trade counts
    signals_detected: int = 0
    trades_attempted: int = 0
    trades_successful: int = 0
    trades_failed: int = 0
    trades_partial: int = 0
    
    # P&L
    total_realized_pnl: Decimal = Decimal("0")
    total_expected_pnl: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    peak_pnl: Decimal = Decimal("0")
    
    # Volume
    total_volume: Decimal = Decimal("0")
    
    # Timing
    avg_execution_time_ms: float = 0
    avg_holding_time_seconds: float = 0
    
    # Connection
    ws_reconnects: int = 0
    api_errors: int = 0


class MetricsCollector:
    """
    Collects and aggregates metrics for the arbitrage bot.
    """
    
    def __init__(self):
        self._session = SessionMetrics()
        self._trades: list[TradeMetrics] = []
        self._execution_times: list[float] = []
        self._holding_times: list[float] = []
    
    def record_signal(self) -> None:
        """Record a signal detection."""
        self._session.signals_detected += 1
    
    def record_trade_attempt(self) -> None:
        """Record a trade attempt."""
        self._session.trades_attempted += 1
    
    def record_trade_success(
        self,
        execution_id: str,
        condition_id: str,
        entry_cost: Decimal,
        expected_pnl: Decimal,
        execution_time_ms: float,
    ) -> None:
        """Record a successful trade."""
        self._session.trades_successful += 1
        self._session.total_volume += entry_cost
        self._session.total_expected_pnl += expected_pnl
        
        self._execution_times.append(execution_time_ms)
        self._update_avg_execution_time()
        
        trade = TradeMetrics(
            execution_id=execution_id,
            condition_id=condition_id,
            entry_time=time.time(),
            entry_cost=entry_cost,
            success=True,
        )
        self._trades.append(trade)
    
    def record_trade_failure(self, execution_id: str, condition_id: str) -> None:
        """Record a failed trade."""
        self._session.trades_failed += 1
        self._session.api_errors += 1
        
        trade = TradeMetrics(
            execution_id=execution_id,
            condition_id=condition_id,
            entry_time=time.time(),
            success=False,
        )
        self._trades.append(trade)
    
    def record_trade_partial(self, execution_id: str, condition_id: str) -> None:
        """Record a partial fill."""
        self._session.trades_partial += 1
    
    def record_position_closed(
        self,
        execution_id: str,
        realized_pnl: Decimal,
        holding_time_seconds: float,
    ) -> None:
        """Record a position closure."""
        self._session.total_realized_pnl += realized_pnl
        
        # Update peak and drawdown
        if self._session.total_realized_pnl > self._session.peak_pnl:
            self._session.peak_pnl = self._session.total_realized_pnl
        
        drawdown = self._session.peak_pnl - self._session.total_realized_pnl
        if drawdown > self._session.max_drawdown:
            self._session.max_drawdown = drawdown
        
        self._holding_times.append(holding_time_seconds)
        self._update_avg_holding_time()
        
        # Update trade record
        for trade in reversed(self._trades):
            if trade.execution_id == execution_id:
                trade.exit_time = time.time()
                trade.realized_pnl = realized_pnl
                trade.holding_time_seconds = holding_time_seconds
                break
    
    def record_ws_reconnect(self) -> None:
        """Record a WebSocket reconnection."""
        self._session.ws_reconnects += 1
    
    def record_api_error(self) -> None:
        """Record an API error."""
        self._session.api_errors += 1
    
    def _update_avg_execution_time(self) -> None:
        """Update average execution time."""
        if self._execution_times:
            self._session.avg_execution_time_ms = (
                sum(self._execution_times) / len(self._execution_times)
            )
    
    def _update_avg_holding_time(self) -> None:
        """Update average holding time."""
        if self._holding_times:
            self._session.avg_holding_time_seconds = (
                sum(self._holding_times) / len(self._holding_times)
            )
    
    def get_session_metrics(self) -> dict:
        """Get current session metrics as dict."""
        uptime = time.time() - self._session.start_time
        
        return {
            "uptime_seconds": uptime,
            "signals_detected": self._session.signals_detected,
            "trades_attempted": self._session.trades_attempted,
            "trades_successful": self._session.trades_successful,
            "trades_failed": self._session.trades_failed,
            "trades_partial": self._session.trades_partial,
            "success_rate": (
                self._session.trades_successful / self._session.trades_attempted
                if self._session.trades_attempted > 0 else 0
            ),
            "total_realized_pnl": str(self._session.total_realized_pnl),
            "total_expected_pnl": str(self._session.total_expected_pnl),
            "max_drawdown": str(self._session.max_drawdown),
            "total_volume": str(self._session.total_volume),
            "avg_execution_time_ms": self._session.avg_execution_time_ms,
            "avg_holding_time_seconds": self._session.avg_holding_time_seconds,
            "ws_reconnects": self._session.ws_reconnects,
            "api_errors": self._session.api_errors,
        }
    
    def get_recent_trades(self, limit: int = 10) -> list[dict]:
        """Get recent trades."""
        recent = self._trades[-limit:] if self._trades else []
        return [
            {
                "execution_id": t.execution_id,
                "condition_id": t.condition_id,
                "entry_time": t.entry_time,
                "exit_time": t.exit_time,
                "entry_cost": str(t.entry_cost),
                "realized_pnl": str(t.realized_pnl),
                "holding_time_seconds": t.holding_time_seconds,
                "success": t.success,
            }
            for t in recent
        ]
    
    def reset_session(self) -> None:
        """Reset session metrics."""
        self._session = SessionMetrics()
        self._trades = []
        self._execution_times = []
        self._holding_times = []
