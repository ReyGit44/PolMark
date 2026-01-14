"""
Structured JSON logging for the arbitrage bot.
All logs are JSON for easy parsing and analysis.
"""

import json
import logging
import sys
import time
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class LogLevel(Enum):
    """Log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class JSONFormatter(logging.Formatter):
    """Format log records as JSON."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "event": record.msg,
            "logger": record.name,
        }
        
        # Add extra fields
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class Logger:
    """
    Structured JSON logger for the arbitrage bot.
    
    All log entries are JSON objects with:
    - timestamp: ISO 8601 UTC timestamp
    - level: Log level
    - event: Event name/type
    - Additional context fields
    """
    
    def __init__(
        self,
        name: str = "arb_bot",
        level: str = "INFO",
        log_file: Optional[str] = None,
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.handlers = []  # Clear existing handlers
        
        # JSON formatter
        formatter = JSONFormatter()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (optional)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def _log(self, level: int, event: str, **kwargs: Any) -> None:
        """Internal log method."""
        record = self.logger.makeRecord(
            self.logger.name,
            level,
            "",
            0,
            event,
            (),
            None,
        )
        record.extra_fields = kwargs
        self.logger.handle(record)
    
    def debug(self, event: str, **kwargs: Any) -> None:
        """Log debug message."""
        self._log(logging.DEBUG, event, **kwargs)
    
    def info(self, event: str, **kwargs: Any) -> None:
        """Log info message."""
        self._log(logging.INFO, event, **kwargs)
    
    def warning(self, event: str, **kwargs: Any) -> None:
        """Log warning message."""
        self._log(logging.WARNING, event, **kwargs)
    
    def error(self, event: str, **kwargs: Any) -> None:
        """Log error message."""
        self._log(logging.ERROR, event, **kwargs)
    
    def critical(self, event: str, **kwargs: Any) -> None:
        """Log critical message."""
        self._log(logging.CRITICAL, event, **kwargs)
    
    # === Convenience methods for common events ===
    
    def trade_signal(
        self,
        condition_id: str,
        yes_ask: str,
        no_ask: str,
        edge: str,
        size: str,
    ) -> None:
        """Log a trade signal."""
        self.info(
            "trade_signal",
            condition_id=condition_id,
            yes_ask=yes_ask,
            no_ask=no_ask,
            edge=edge,
            size=size,
        )
    
    def trade_executed(
        self,
        execution_id: str,
        condition_id: str,
        size: str,
        entry_cost: str,
        expected_profit: str,
    ) -> None:
        """Log a successful trade execution."""
        self.info(
            "trade_executed",
            execution_id=execution_id,
            condition_id=condition_id,
            size=size,
            entry_cost=entry_cost,
            expected_profit=expected_profit,
        )
    
    def trade_failed(
        self,
        execution_id: str,
        condition_id: str,
        error: str,
    ) -> None:
        """Log a failed trade."""
        self.error(
            "trade_failed",
            execution_id=execution_id,
            condition_id=condition_id,
            error=error,
        )
    
    def position_opened(
        self,
        position_id: str,
        condition_id: str,
        size: str,
        entry_cost: str,
    ) -> None:
        """Log position opened."""
        self.info(
            "position_opened",
            position_id=position_id,
            condition_id=condition_id,
            size=size,
            entry_cost=entry_cost,
        )
    
    def position_closed(
        self,
        position_id: str,
        condition_id: str,
        realized_pnl: str,
        holding_time_seconds: float,
    ) -> None:
        """Log position closed."""
        self.info(
            "position_closed",
            position_id=position_id,
            condition_id=condition_id,
            realized_pnl=realized_pnl,
            holding_time_seconds=holding_time_seconds,
        )
    
    def ws_connected(self, url: str) -> None:
        """Log WebSocket connected."""
        self.info("ws_connected", url=url)
    
    def ws_disconnected(self, reason: str = "") -> None:
        """Log WebSocket disconnected."""
        self.warning("ws_disconnected", reason=reason)
    
    def ws_message(self, event_type: str, asset_id: str) -> None:
        """Log WebSocket message (debug level)."""
        self.debug("ws_message", event_type=event_type, asset_id=asset_id)
    
    def risk_check_failed(self, violation: str, message: str) -> None:
        """Log risk check failure."""
        self.warning("risk_check_failed", violation=violation, message=message)
    
    def health_check(self, healthy: bool, issues: list[str]) -> None:
        """Log health check result."""
        if healthy:
            self.debug("health_check", healthy=True)
        else:
            self.warning("health_check", healthy=False, issues=issues)
    
    def startup(self, config: dict) -> None:
        """Log bot startup."""
        self.info("bot_startup", config=config)
    
    def shutdown(self, reason: str = "normal") -> None:
        """Log bot shutdown."""
        self.info("bot_shutdown", reason=reason)
