"""
Parity arbitrage signal detector.
Identifies opportunities where YES_ask + NO_ask < 1 - fees - slippage.
"""

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import FeeConfig, TradingConfig
    from ..orderbook import MarketBook, OrderBookManager


@dataclass
class ParitySignal:
    """
    Parity arbitrage opportunity signal.
    
    When YES_ask + NO_ask < 1, buying both sides guarantees profit at resolution.
    """
    condition_id: str
    yes_token_id: str
    no_token_id: str
    yes_ask: Decimal
    no_ask: Decimal
    combined_cost: Decimal  # yes_ask + no_ask
    gross_edge: Decimal  # 1 - combined_cost
    net_edge: Decimal  # gross_edge - fees - slippage
    max_size: Decimal  # Maximum executable size
    timestamp: float
    
    @property
    def is_profitable(self) -> bool:
        """Check if signal is profitable after costs."""
        return self.net_edge > 0
    
    @property
    def expected_profit_per_share(self) -> Decimal:
        """Expected profit per share at resolution."""
        return self.net_edge
    
    def expected_total_profit(self, size: Decimal) -> Decimal:
        """Expected total profit for given size."""
        return self.net_edge * size


class ParityDetector:
    """
    Detects parity arbitrage opportunities across markets.
    
    Parity arbitrage: In a binary market, YES + NO must equal 1 at resolution.
    If we can buy YES at ask_yes and NO at ask_no where ask_yes + ask_no < 1,
    we lock in a guaranteed profit of (1 - ask_yes - ask_no) per share.
    """
    
    def __init__(
        self,
        orderbook_manager: "OrderBookManager",
        fee_config: "FeeConfig",
        trading_config: "TradingConfig",
    ):
        self.orderbook = orderbook_manager
        self.fees = fee_config
        self.trading = trading_config
        
        self._callbacks: list[Callable[[ParitySignal], None]] = []
        self._last_signals: dict[str, ParitySignal] = {}
    
    def on_signal(self, callback: Callable[[ParitySignal], None]) -> None:
        """Register callback for new signals."""
        self._callbacks.append(callback)
    
    def _emit_signal(self, signal: ParitySignal) -> None:
        """Emit signal to all registered callbacks."""
        for callback in self._callbacks:
            try:
                callback(signal)
            except Exception:
                pass  # Don't let callback errors break detection
    
    def calculate_fees(self, yes_price: Decimal, no_price: Decimal, size: Decimal) -> Decimal:
        """
        Calculate total fees for buying both sides.
        
        Polymarket fee formula for buying:
        fee = baseRate * min(price, 1-price) * (size/price)
        
        For most markets, fees are 0. For 15-min crypto markets, taker fees apply.
        """
        if self.fees.taker_fee_rate == 0:
            return Decimal("0")
        
        # Fee for YES side
        yes_fee_factor = min(yes_price, Decimal("1") - yes_price)
        yes_fee = self.fees.taker_fee_rate * yes_fee_factor * size
        
        # Fee for NO side
        no_fee_factor = min(no_price, Decimal("1") - no_price)
        no_fee = self.fees.taker_fee_rate * no_fee_factor * size
        
        return Decimal(str(yes_fee + no_fee))
    
    def check_market(self, market: "MarketBook") -> Optional[ParitySignal]:
        """
        Check a single market for parity arbitrage opportunity.
        
        Returns signal if profitable opportunity exists, None otherwise.
        """
        # Skip stale books
        if market.is_stale:
            return None
        
        yes_ask = market.yes_best_ask
        no_ask = market.no_best_ask
        
        if yes_ask is None or no_ask is None:
            return None
        
        # Combined cost to buy both sides
        combined_cost = yes_ask + no_ask
        
        # Gross edge before fees and slippage
        gross_edge = Decimal("1") - combined_cost
        
        # Skip if no gross edge
        if gross_edge <= 0:
            return None
        
        # Get executable size (limited by smaller side)
        max_size = market.get_executable_size()
        if max_size is None or max_size <= 0:
            return None
        
        # Cap size by max notional
        max_notional = Decimal(str(self.trading.max_notional_per_trade))
        max_size_by_notional = max_notional / combined_cost
        max_size = min(max_size, max_size_by_notional)
        
        # Calculate fees
        fees = self.calculate_fees(yes_ask, no_ask, max_size)
        fee_per_share = fees / max_size if max_size > 0 else Decimal("0")
        
        # Apply slippage buffer
        slippage = Decimal(str(self.trading.slippage_buffer))
        
        # Net edge after all costs
        net_edge = gross_edge - fee_per_share - slippage
        
        signal = ParitySignal(
            condition_id=market.condition_id,
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
            yes_ask=yes_ask,
            no_ask=no_ask,
            combined_cost=combined_cost,
            gross_edge=gross_edge,
            net_edge=net_edge,
            max_size=max_size,
            timestamp=time.time(),
        )
        
        return signal
    
    def scan_all_markets(self) -> list[ParitySignal]:
        """
        Scan all markets for parity opportunities.
        Returns list of profitable signals sorted by edge (highest first).
        """
        signals = []
        min_edge = Decimal(str(self.trading.min_edge))
        
        for market in self.orderbook.get_all_markets():
            signal = self.check_market(market)
            
            if signal and signal.net_edge >= min_edge:
                signals.append(signal)
                self._last_signals[market.condition_id] = signal
                self._emit_signal(signal)
        
        # Sort by net edge descending
        signals.sort(key=lambda s: s.net_edge, reverse=True)
        
        return signals
    
    def get_best_opportunity(self) -> Optional[ParitySignal]:
        """Get the single best opportunity across all markets."""
        signals = self.scan_all_markets()
        return signals[0] if signals else None
    
    def get_last_signal(self, condition_id: str) -> Optional[ParitySignal]:
        """Get the last signal for a specific market."""
        return self._last_signals.get(condition_id)
    
    def clear_signals(self) -> None:
        """Clear cached signals."""
        self._last_signals.clear()


class ConvergenceDetector:
    """
    Detects when a paired position should be exited.
    
    Exit conditions:
    1. Spread converges (YES_bid + NO_bid >= 1 - threshold)
    2. One side becomes highly liquid for exit
    3. Market resolution approaching
    """
    
    def __init__(
        self,
        orderbook_manager: "OrderBookManager",
        convergence_threshold: Decimal = Decimal("0.001"),
    ):
        self.orderbook = orderbook_manager
        self.threshold = convergence_threshold
    
    def should_exit(self, condition_id: str) -> tuple[bool, str]:
        """
        Check if a paired position should be exited.
        
        Returns (should_exit, reason).
        """
        market = self.orderbook.get_market(condition_id)
        if not market:
            return True, "market_not_found"
        
        yes_bid = market.yes_book.best_bid
        no_bid = market.no_book.best_bid
        
        if yes_bid is None or no_bid is None:
            return False, "no_bids"
        
        # Check if we can exit profitably
        # If YES_bid + NO_bid >= 1 - threshold, spread has converged
        combined_bid = yes_bid + no_bid
        
        if combined_bid >= Decimal("1") - self.threshold:
            return True, "spread_converged"
        
        # Check for stale data
        if market.is_stale:
            return False, "stale_data"
        
        return False, "hold"
    
    def get_exit_value(self, condition_id: str) -> Optional[Decimal]:
        """
        Get the current exit value for a paired position.
        
        Exit value = YES_bid + NO_bid (what we'd get selling both sides).
        """
        market = self.orderbook.get_market(condition_id)
        if not market:
            return None
        
        yes_bid = market.yes_book.best_bid
        no_bid = market.no_book.best_bid
        
        if yes_bid is None or no_bid is None:
            return None
        
        return yes_bid + no_bid
