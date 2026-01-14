"""
Risk management for parity arbitrage bot.
Handles limits, cooldowns, kill-switch, and health checks.
"""

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import RiskConfig, TradingConfig
    from ..positions import PositionManager
    from ..monitor import Logger


class RiskViolation(Enum):
    """Types of risk violations."""
    MAX_DAILY_LOSS = "max_daily_loss"
    MAX_POSITION_VALUE = "max_position_value"
    MAX_OPEN_PAIRS = "max_open_pairs"
    COOLDOWN_ACTIVE = "cooldown_active"
    KILL_SWITCH_TRIGGERED = "kill_switch_triggered"
    CONSECUTIVE_FAILURES = "consecutive_failures"
    STALE_DATA = "stale_data"
    CONNECTION_LOST = "connection_lost"


@dataclass
class RiskCheck:
    """Result of a risk check."""
    passed: bool
    violation: Optional[RiskViolation] = None
    message: str = ""
    
    @classmethod
    def ok(cls) -> "RiskCheck":
        return cls(passed=True)
    
    @classmethod
    def fail(cls, violation: RiskViolation, message: str = "") -> "RiskCheck":
        return cls(passed=False, violation=violation, message=message)


@dataclass
class DailyStats:
    """Daily trading statistics."""
    date: str  # YYYY-MM-DD
    trades_count: int = 0
    total_volume: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    peak_pnl: Decimal = Decimal("0")


class RiskManager:
    """
    Manages risk controls for the arbitrage bot.
    
    Controls:
    - Daily loss limits
    - Position limits
    - Trade cooldowns
    - Kill switch
    - Health monitoring
    """
    
    def __init__(
        self,
        risk_config: "RiskConfig",
        trading_config: "TradingConfig",
        position_manager: "PositionManager",
        logger: Optional["Logger"] = None,
    ):
        self.config = risk_config
        self.trading = trading_config
        self.positions = position_manager
        self.logger = logger
        
        # State
        self._kill_switch_active = False
        self._kill_switch_reason: Optional[str] = None
        self._last_trade_time: float = 0
        self._consecutive_failures: int = 0
        self._daily_stats: Optional[DailyStats] = None
        
        # Health check state
        self._last_health_check: float = 0
        self._ws_connected: bool = False
        self._last_ws_message: float = 0
        
        # Callbacks
        self._on_kill_switch: list[Callable[[str], None]] = []
    
    def on_kill_switch(self, callback: Callable[[str], None]) -> None:
        """Register callback for kill switch activation."""
        self._on_kill_switch.append(callback)
    
    def _trigger_kill_switch(self, reason: str) -> None:
        """Activate kill switch."""
        self._kill_switch_active = True
        self._kill_switch_reason = reason
        
        if self.logger:
            self.logger.critical("kill_switch_triggered", reason=reason)
        
        for callback in self._on_kill_switch:
            try:
                callback(reason)
            except Exception:
                pass
    
    def reset_kill_switch(self) -> None:
        """Reset kill switch (manual intervention required)."""
        self._kill_switch_active = False
        self._kill_switch_reason = None
        
        if self.logger:
            self.logger.info("kill_switch_reset")
    
    @property
    def is_kill_switch_active(self) -> bool:
        return self._kill_switch_active
    
    def check_can_trade(self) -> RiskCheck:
        """
        Comprehensive check if trading is allowed.
        Returns RiskCheck with pass/fail and reason.
        """
        # Kill switch check
        if self._kill_switch_active:
            return RiskCheck.fail(
                RiskViolation.KILL_SWITCH_TRIGGERED,
                f"Kill switch active: {self._kill_switch_reason}"
            )
        
        # Cooldown check
        if not self._check_cooldown():
            remaining = self._cooldown_remaining_ms()
            return RiskCheck.fail(
                RiskViolation.COOLDOWN_ACTIVE,
                f"Cooldown active: {remaining}ms remaining"
            )
        
        # Position limit check
        if not self.positions.can_open_new_position:
            return RiskCheck.fail(
                RiskViolation.MAX_OPEN_PAIRS,
                f"Max open pairs reached: {self.positions.open_position_count}"
            )
        
        # Daily loss check
        daily_pnl = self._get_daily_pnl()
        if daily_pnl < -Decimal(str(self.config.max_daily_loss)):
            self._trigger_kill_switch(f"Daily loss limit exceeded: {daily_pnl}")
            return RiskCheck.fail(
                RiskViolation.MAX_DAILY_LOSS,
                f"Daily loss limit exceeded: {daily_pnl}"
            )
        
        # Position value check
        total_exposure = self.positions.total_exposure
        if total_exposure >= Decimal(str(self.config.max_position_value)):
            return RiskCheck.fail(
                RiskViolation.MAX_POSITION_VALUE,
                f"Max position value reached: {total_exposure}"
            )
        
        # Consecutive failures check
        if self._consecutive_failures >= self.config.max_consecutive_failures:
            return RiskCheck.fail(
                RiskViolation.CONSECUTIVE_FAILURES,
                f"Too many consecutive failures: {self._consecutive_failures}"
            )
        
        return RiskCheck.ok()
    
    def check_trade_size(self, size: Decimal, price: Decimal) -> RiskCheck:
        """Check if a specific trade size is allowed."""
        notional = size * price
        max_notional = Decimal(str(self.trading.max_notional_per_trade))
        
        if notional > max_notional:
            return RiskCheck.fail(
                RiskViolation.MAX_POSITION_VALUE,
                f"Trade notional {notional} exceeds max {max_notional}"
            )
        
        # Check if this would exceed total position limit
        new_total = self.positions.total_exposure + notional
        if new_total > Decimal(str(self.config.max_position_value)):
            return RiskCheck.fail(
                RiskViolation.MAX_POSITION_VALUE,
                f"Trade would exceed max position value"
            )
        
        return RiskCheck.ok()
    
    def _check_cooldown(self) -> bool:
        """Check if cooldown period has passed."""
        if self._last_trade_time == 0:
            return True
        
        elapsed_ms = (time.time() - self._last_trade_time) * 1000
        return elapsed_ms >= self.trading.cooldown_ms
    
    def _cooldown_remaining_ms(self) -> int:
        """Get remaining cooldown time in milliseconds."""
        if self._last_trade_time == 0:
            return 0
        
        elapsed_ms = (time.time() - self._last_trade_time) * 1000
        remaining = self.trading.cooldown_ms - elapsed_ms
        return max(0, int(remaining))
    
    def record_trade(self, success: bool, pnl: Decimal = Decimal("0")) -> None:
        """Record a trade attempt."""
        self._last_trade_time = time.time()
        
        if success:
            self._consecutive_failures = 0
            self._update_daily_stats(pnl)
        else:
            self._consecutive_failures += 1
            
            if self._consecutive_failures >= self.config.max_consecutive_failures:
                self._trigger_kill_switch(
                    f"Consecutive failures: {self._consecutive_failures}"
                )
    
    def record_pnl(self, pnl: Decimal) -> None:
        """Record realized P&L."""
        self._update_daily_stats(pnl)
        
        # Check for kill switch threshold
        daily_pnl = self._get_daily_pnl()
        if daily_pnl < -Decimal(str(self.config.kill_switch_loss_threshold)):
            self._trigger_kill_switch(f"Loss threshold exceeded: {daily_pnl}")
    
    def _get_daily_pnl(self) -> Decimal:
        """Get today's realized P&L."""
        if self._daily_stats is None:
            self._init_daily_stats()
        return self._daily_stats.realized_pnl
    
    def _init_daily_stats(self) -> None:
        """Initialize daily stats for today."""
        today = time.strftime("%Y-%m-%d")
        self._daily_stats = DailyStats(date=today)
    
    def _update_daily_stats(self, pnl: Decimal) -> None:
        """Update daily statistics."""
        today = time.strftime("%Y-%m-%d")
        
        if self._daily_stats is None or self._daily_stats.date != today:
            self._init_daily_stats()
        
        self._daily_stats.trades_count += 1
        self._daily_stats.realized_pnl += pnl
        
        # Track peak and drawdown
        if self._daily_stats.realized_pnl > self._daily_stats.peak_pnl:
            self._daily_stats.peak_pnl = self._daily_stats.realized_pnl
        
        drawdown = self._daily_stats.peak_pnl - self._daily_stats.realized_pnl
        if drawdown > self._daily_stats.max_drawdown:
            self._daily_stats.max_drawdown = drawdown
    
    # === Health Monitoring ===
    
    def update_ws_status(self, connected: bool, last_message_time: float = 0) -> None:
        """Update WebSocket connection status."""
        self._ws_connected = connected
        if last_message_time > 0:
            self._last_ws_message = last_message_time
    
    def run_health_check(self) -> dict:
        """
        Run comprehensive health check.
        Returns dict with health status and any issues.
        """
        self._last_health_check = time.time()
        
        issues = []
        
        # Check WebSocket connection
        if not self._ws_connected:
            issues.append("WebSocket disconnected")
        
        # Check data freshness
        if self._last_ws_message > 0:
            data_age = time.time() - self._last_ws_message
            if data_age > 60:
                issues.append(f"Stale data: {data_age:.0f}s since last update")
        
        # Check kill switch
        if self._kill_switch_active:
            issues.append(f"Kill switch active: {self._kill_switch_reason}")
        
        # Check consecutive failures
        if self._consecutive_failures > 0:
            issues.append(f"Consecutive failures: {self._consecutive_failures}")
        
        # Check daily P&L
        daily_pnl = self._get_daily_pnl()
        if daily_pnl < Decimal("0"):
            issues.append(f"Daily P&L negative: {daily_pnl}")
        
        return {
            "healthy": len(issues) == 0,
            "issues": issues,
            "ws_connected": self._ws_connected,
            "kill_switch_active": self._kill_switch_active,
            "consecutive_failures": self._consecutive_failures,
            "daily_pnl": str(daily_pnl),
            "open_positions": self.positions.open_position_count,
            "total_exposure": str(self.positions.total_exposure),
            "timestamp": self._last_health_check,
        }
    
    async def health_check_loop(self, interval: Optional[int] = None) -> None:
        """Run periodic health checks."""
        interval = interval or self.config.health_check_interval_seconds
        
        while True:
            try:
                health = self.run_health_check()
                
                if not health["healthy"] and self.logger:
                    self.logger.warning("health_check_issues", issues=health["issues"])
                
            except Exception as e:
                if self.logger:
                    self.logger.error("health_check_error", error=str(e))
            
            await asyncio.sleep(interval)
    
    def get_status(self) -> dict:
        """Get current risk manager status."""
        return {
            "kill_switch_active": self._kill_switch_active,
            "kill_switch_reason": self._kill_switch_reason,
            "consecutive_failures": self._consecutive_failures,
            "cooldown_remaining_ms": self._cooldown_remaining_ms(),
            "daily_stats": {
                "date": self._daily_stats.date if self._daily_stats else None,
                "trades_count": self._daily_stats.trades_count if self._daily_stats else 0,
                "realized_pnl": str(self._daily_stats.realized_pnl) if self._daily_stats else "0",
                "max_drawdown": str(self._daily_stats.max_drawdown) if self._daily_stats else "0",
            },
            "position_count": self.positions.open_position_count,
            "total_exposure": str(self.positions.total_exposure),
        }
