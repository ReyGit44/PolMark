"""
Configuration management for Polymarket parity arbitrage bot.
All secrets via environment variables. All tunable parameters externalized.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MarketConfig:
    """Configuration for a single market to monitor."""
    condition_id: str
    yes_token_id: str
    no_token_id: str
    tick_size: str = "0.01"
    neg_risk: bool = False


@dataclass
class FeeConfig:
    """Fee model configuration. Polymarket currently has 0 maker/taker fees for most markets."""
    maker_fee_bps: float = 0.0
    taker_fee_bps: float = 0.0
    
    @property
    def maker_fee_rate(self) -> float:
        return self.maker_fee_bps / 10000.0
    
    @property
    def taker_fee_rate(self) -> float:
        return self.taker_fee_bps / 10000.0


@dataclass
class TradingConfig:
    """Trading parameters."""
    min_edge: float = 0.005  # Minimum edge to execute (0.5%)
    slippage_buffer: float = 0.002  # Slippage buffer (0.2%)
    max_notional_per_trade: float = 100.0  # Max USDC per trade
    max_open_pairs: int = 5  # Max concurrent paired positions
    cooldown_ms: int = 1000  # Cooldown between trades in ms
    order_timeout_seconds: int = 30  # Order timeout
    convergence_threshold: float = 0.001  # Exit when spread < this


@dataclass
class RiskConfig:
    """Risk management parameters."""
    max_daily_loss: float = 500.0  # Max daily loss in USDC
    max_position_value: float = 1000.0  # Max total position value
    health_check_interval_seconds: int = 30
    kill_switch_loss_threshold: float = 200.0  # Immediate halt threshold
    max_consecutive_failures: int = 3


@dataclass
class ConnectionConfig:
    """API connection configuration."""
    clob_rest_url: str = "https://clob.polymarket.com"
    clob_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    chain_id: int = 137  # Polygon mainnet
    ws_reconnect_delay_seconds: int = 5
    ws_ping_interval_seconds: int = 30
    rest_timeout_seconds: int = 10
    max_retries: int = 3
    retry_backoff_base: float = 1.5


@dataclass
class Config:
    """Main configuration container."""
    # Secrets from environment
    private_key: str = field(default_factory=lambda: os.environ.get("POLYMARKET_PRIVATE_KEY", ""))
    funder_address: str = field(default_factory=lambda: os.environ.get("POLYMARKET_FUNDER_ADDRESS", ""))
    signature_type: int = field(default_factory=lambda: int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "2")))
    
    # API credentials (derived from private key)
    api_key: Optional[str] = field(default_factory=lambda: os.environ.get("POLYMARKET_API_KEY"))
    api_secret: Optional[str] = field(default_factory=lambda: os.environ.get("POLYMARKET_API_SECRET"))
    api_passphrase: Optional[str] = field(default_factory=lambda: os.environ.get("POLYMARKET_API_PASSPHRASE"))
    
    # Sub-configs
    fees: FeeConfig = field(default_factory=FeeConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    
    # Markets to monitor
    markets: list[MarketConfig] = field(default_factory=list)
    
    # Logging
    log_level: str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))
    log_file: str = field(default_factory=lambda: os.environ.get("LOG_FILE", "arb_bot.log"))
    
    # Database
    db_path: str = field(default_factory=lambda: os.environ.get("DB_PATH", "arb_bot.db"))
    
    def validate(self) -> list[str]:
        """Validate configuration. Returns list of errors."""
        errors = []
        
        if not self.private_key:
            errors.append("POLYMARKET_PRIVATE_KEY is required")
        if not self.funder_address:
            errors.append("POLYMARKET_FUNDER_ADDRESS is required")
        if not self.markets:
            errors.append("At least one market must be configured")
        if self.trading.min_edge <= 0:
            errors.append("min_edge must be positive")
        if self.trading.slippage_buffer < 0:
            errors.append("slippage_buffer cannot be negative")
        if self.trading.max_notional_per_trade <= 0:
            errors.append("max_notional_per_trade must be positive")
            
        return errors


def load_config_from_env() -> Config:
    """Load configuration from environment variables."""
    config = Config()
    
    # Parse markets from environment
    # Format: MARKETS=condition_id1:yes_token1:no_token1,condition_id2:yes_token2:no_token2
    markets_str = os.environ.get("POLYMARKET_MARKETS", "")
    if markets_str:
        for market_def in markets_str.split(","):
            parts = market_def.strip().split(":")
            if len(parts) >= 3:
                config.markets.append(MarketConfig(
                    condition_id=parts[0],
                    yes_token_id=parts[1],
                    no_token_id=parts[2],
                    tick_size=parts[3] if len(parts) > 3 else "0.01",
                    neg_risk=parts[4].lower() == "true" if len(parts) > 4 else False
                ))
    
    # Override trading params from env
    if os.environ.get("MIN_EDGE"):
        config.trading.min_edge = float(os.environ["MIN_EDGE"])
    if os.environ.get("SLIPPAGE_BUFFER"):
        config.trading.slippage_buffer = float(os.environ["SLIPPAGE_BUFFER"])
    if os.environ.get("MAX_NOTIONAL_PER_TRADE"):
        config.trading.max_notional_per_trade = float(os.environ["MAX_NOTIONAL_PER_TRADE"])
    if os.environ.get("MAX_OPEN_PAIRS"):
        config.trading.max_open_pairs = int(os.environ["MAX_OPEN_PAIRS"])
    if os.environ.get("COOLDOWN_MS"):
        config.trading.cooldown_ms = int(os.environ["COOLDOWN_MS"])
    
    # Override fee params from env
    if os.environ.get("MAKER_FEE_BPS"):
        config.fees.maker_fee_bps = float(os.environ["MAKER_FEE_BPS"])
    if os.environ.get("TAKER_FEE_BPS"):
        config.fees.taker_fee_bps = float(os.environ["TAKER_FEE_BPS"])
    
    # Override risk params from env
    if os.environ.get("MAX_DAILY_LOSS"):
        config.risk.max_daily_loss = float(os.environ["MAX_DAILY_LOSS"])
    if os.environ.get("KILL_SWITCH_LOSS_THRESHOLD"):
        config.risk.kill_switch_loss_threshold = float(os.environ["KILL_SWITCH_LOSS_THRESHOLD"])
    
    return config
