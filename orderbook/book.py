"""
In-memory orderbook management with delta updates.
Maintains synchronized YES/NO books for parity arbitrage detection.
"""

import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional
from sortedcontainers import SortedDict


@dataclass
class PriceLevel:
    """Single price level with size."""
    price: Decimal
    size: Decimal
    
    def __post_init__(self):
        if isinstance(self.price, str):
            self.price = Decimal(self.price)
        if isinstance(self.size, str):
            self.size = Decimal(self.size)


@dataclass
class BookSide:
    """One side of an orderbook (bids or asks)."""
    is_bid: bool
    levels: SortedDict = field(default_factory=SortedDict)
    
    def __post_init__(self):
        # Bids sorted descending (highest first), asks ascending (lowest first)
        if self.is_bid:
            self.levels = SortedDict(lambda x: -x)
        else:
            self.levels = SortedDict()
    
    def update(self, price: Decimal, size: Decimal) -> None:
        """Update a price level. Size of 0 removes the level."""
        if size <= 0:
            self.levels.pop(price, None)
        else:
            self.levels[price] = size
    
    def set_snapshot(self, levels: list[tuple[Decimal, Decimal]]) -> None:
        """Replace all levels with a snapshot."""
        self.levels.clear()
        for price, size in levels:
            if size > 0:
                self.levels[price] = size
    
    @property
    def best(self) -> Optional[PriceLevel]:
        """Get best price level."""
        if not self.levels:
            return None
        price = self.levels.keys()[0]
        return PriceLevel(price, self.levels[price])
    
    @property
    def best_price(self) -> Optional[Decimal]:
        """Get best price."""
        if not self.levels:
            return None
        return self.levels.keys()[0]
    
    @property
    def best_size(self) -> Optional[Decimal]:
        """Get size at best price."""
        if not self.levels:
            return None
        return self.levels[self.levels.keys()[0]]
    
    def get_depth(self, max_levels: int = 10) -> list[PriceLevel]:
        """Get top N price levels."""
        result = []
        for i, price in enumerate(self.levels.keys()):
            if i >= max_levels:
                break
            result.append(PriceLevel(price, self.levels[price]))
        return result
    
    def get_liquidity_at_price(self, target_price: Decimal) -> Decimal:
        """Get total liquidity available at or better than target price."""
        total = Decimal("0")
        for price in self.levels.keys():
            if self.is_bid:
                if price < target_price:
                    break
            else:
                if price > target_price:
                    break
            total += self.levels[price]
        return total


@dataclass
class TokenBook:
    """Orderbook for a single token (YES or NO)."""
    token_id: str
    bids: BookSide = field(default_factory=lambda: BookSide(is_bid=True))
    asks: BookSide = field(default_factory=lambda: BookSide(is_bid=False))
    last_update: float = 0
    hash: str = ""
    
    def update_level(self, side: str, price: Decimal, size: Decimal) -> None:
        """Update a single price level."""
        if side.upper() == "BUY":
            self.bids.update(price, size)
        else:
            self.asks.update(price, size)
        self.last_update = time.time()
    
    def set_snapshot(
        self,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
        book_hash: str = "",
    ) -> None:
        """Set full book snapshot."""
        self.bids.set_snapshot(bids)
        self.asks.set_snapshot(asks)
        self.hash = book_hash
        self.last_update = time.time()
    
    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids.best_price
    
    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks.best_price
    
    @property
    def spread(self) -> Optional[Decimal]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid
    
    @property
    def midpoint(self) -> Optional[Decimal]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2
    
    @property
    def age_seconds(self) -> float:
        """Seconds since last update."""
        if self.last_update == 0:
            return float("inf")
        return time.time() - self.last_update


@dataclass
class MarketBook:
    """
    Combined YES/NO orderbook for a binary market.
    Tracks both sides for parity arbitrage detection.
    """
    condition_id: str
    yes_token_id: str
    no_token_id: str
    yes_book: TokenBook = field(default_factory=lambda: TokenBook(""))
    no_book: TokenBook = field(default_factory=lambda: TokenBook(""))
    tick_size: Decimal = Decimal("0.01")
    neg_risk: bool = False
    
    def __post_init__(self):
        self.yes_book = TokenBook(self.yes_token_id)
        self.no_book = TokenBook(self.no_token_id)
    
    @property
    def yes_best_ask(self) -> Optional[Decimal]:
        """Best ask price for YES token."""
        return self.yes_book.best_ask
    
    @property
    def no_best_ask(self) -> Optional[Decimal]:
        """Best ask price for NO token."""
        return self.no_book.best_ask
    
    @property
    def combined_ask(self) -> Optional[Decimal]:
        """Sum of YES and NO best asks."""
        if self.yes_best_ask is None or self.no_best_ask is None:
            return None
        return self.yes_best_ask + self.no_best_ask
    
    @property
    def parity_edge(self) -> Optional[Decimal]:
        """
        Raw edge from parity: 1 - (yes_ask + no_ask).
        Positive means buying both sides costs less than $1.
        """
        combined = self.combined_ask
        if combined is None:
            return None
        return Decimal("1") - combined
    
    def get_executable_size(self) -> Optional[Decimal]:
        """
        Get the maximum size that can be executed on both sides.
        Limited by the smaller of YES ask size and NO ask size.
        """
        yes_size = self.yes_book.asks.best_size
        no_size = self.no_book.asks.best_size
        
        if yes_size is None or no_size is None:
            return None
        
        return min(yes_size, no_size)
    
    @property
    def is_stale(self) -> bool:
        """Check if either book is stale (>60 seconds old)."""
        return self.yes_book.age_seconds > 60 or self.no_book.age_seconds > 60
    
    @property
    def last_update(self) -> float:
        """Most recent update time across both books."""
        return max(self.yes_book.last_update, self.no_book.last_update)


class OrderBookManager:
    """
    Manages orderbooks for multiple markets.
    Thread-safe for concurrent updates from WebSocket.
    """
    
    def __init__(self):
        self._markets: dict[str, MarketBook] = {}
        self._token_to_market: dict[str, str] = {}  # token_id -> condition_id
        self._lock = threading.RLock()
    
    def add_market(
        self,
        condition_id: str,
        yes_token_id: str,
        no_token_id: str,
        tick_size: str = "0.01",
        neg_risk: bool = False,
    ) -> MarketBook:
        """Add a market to track."""
        with self._lock:
            market = MarketBook(
                condition_id=condition_id,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                tick_size=Decimal(tick_size),
                neg_risk=neg_risk,
            )
            self._markets[condition_id] = market
            self._token_to_market[yes_token_id] = condition_id
            self._token_to_market[no_token_id] = condition_id
            return market
    
    def get_market(self, condition_id: str) -> Optional[MarketBook]:
        """Get market by condition ID."""
        with self._lock:
            return self._markets.get(condition_id)
    
    def get_market_by_token(self, token_id: str) -> Optional[MarketBook]:
        """Get market by token ID."""
        with self._lock:
            condition_id = self._token_to_market.get(token_id)
            if condition_id:
                return self._markets.get(condition_id)
            return None
    
    def update_book_snapshot(
        self,
        token_id: str,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
        book_hash: str = "",
    ) -> None:
        """Update a token's book with a full snapshot."""
        with self._lock:
            market = self.get_market_by_token(token_id)
            if not market:
                return
            
            if token_id == market.yes_token_id:
                market.yes_book.set_snapshot(bids, asks, book_hash)
            elif token_id == market.no_token_id:
                market.no_book.set_snapshot(bids, asks, book_hash)
    
    def update_price_level(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
    ) -> None:
        """Update a single price level."""
        with self._lock:
            market = self.get_market_by_token(token_id)
            if not market:
                return
            
            if token_id == market.yes_token_id:
                market.yes_book.update_level(side, price, size)
            elif token_id == market.no_token_id:
                market.no_book.update_level(side, price, size)
    
    def update_best_bid_ask(
        self,
        token_id: str,
        best_bid: Decimal,
        best_ask: Decimal,
    ) -> None:
        """
        Update best bid/ask from WebSocket message.
        This is a lightweight update that only touches top of book.
        """
        with self._lock:
            market = self.get_market_by_token(token_id)
            if not market:
                return
            
            if token_id == market.yes_token_id:
                book = market.yes_book
            elif token_id == market.no_token_id:
                book = market.no_book
            else:
                return
            
            # Update best levels if they changed
            if best_bid > 0:
                current_best = book.bids.best_price
                if current_best is None or current_best != best_bid:
                    # We don't know the size, so use a placeholder
                    # Full book update will correct this
                    book.bids.update(best_bid, Decimal("1"))
            
            if best_ask > 0:
                current_best = book.asks.best_price
                if current_best is None or current_best != best_ask:
                    book.asks.update(best_ask, Decimal("1"))
            
            book.last_update = time.time()
    
    def get_all_markets(self) -> list[MarketBook]:
        """Get all tracked markets."""
        with self._lock:
            return list(self._markets.values())
    
    def get_all_token_ids(self) -> list[str]:
        """Get all token IDs being tracked."""
        with self._lock:
            return list(self._token_to_market.keys())
    
    def remove_market(self, condition_id: str) -> None:
        """Remove a market from tracking."""
        with self._lock:
            market = self._markets.pop(condition_id, None)
            if market:
                self._token_to_market.pop(market.yes_token_id, None)
                self._token_to_market.pop(market.no_token_id, None)
