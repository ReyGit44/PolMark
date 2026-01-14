"""
WebSocket client for Polymarket CLOB API.
Handles real-time orderbook updates and order status.
"""

import asyncio
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .auth import AuthManager


class WSMessageType(Enum):
    """WebSocket message types."""
    BOOK = "book"
    PRICE_CHANGE = "price_change"
    LAST_TRADE_PRICE = "last_trade_price"
    BEST_BID_ASK = "best_bid_ask"
    TICK_SIZE_CHANGE = "tick_size_change"
    ORDER_UPDATE = "order"
    TRADE = "trade"


@dataclass
class BookUpdate:
    """Full orderbook snapshot from WebSocket."""
    asset_id: str
    market: str
    bids: list[tuple[Decimal, Decimal]]  # (price, size)
    asks: list[tuple[Decimal, Decimal]]
    timestamp: int
    hash: str


@dataclass
class PriceChange:
    """Price level change update."""
    asset_id: str
    market: str
    price: Decimal
    size: Decimal
    side: str
    best_bid: Decimal
    best_ask: Decimal
    timestamp: int


@dataclass
class BestBidAsk:
    """Best bid/ask update."""
    asset_id: str
    market: str
    best_bid: Decimal
    best_ask: Decimal
    spread: Decimal
    timestamp: int


class PolymarketWebSocketClient:
    """WebSocket client for real-time market data."""
    
    def __init__(
        self,
        auth_manager: Optional[AuthManager] = None,
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/",
        reconnect_delay: int = 5,
        ping_interval: int = 30,
    ):
        self.auth = auth_manager
        self.ws_url = ws_url
        self.reconnect_delay = reconnect_delay
        self.ping_interval = ping_interval
        
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._subscribed_assets: set[str] = set()
        self._subscribed_markets: set[str] = set()
        
        # Callbacks
        self._on_book: Optional[Callable[[BookUpdate], None]] = None
        self._on_price_change: Optional[Callable[[PriceChange], None]] = None
        self._on_best_bid_ask: Optional[Callable[[BestBidAsk], None]] = None
        self._on_error: Optional[Callable[[Exception], None]] = None
        self._on_connected: Optional[Callable[[], None]] = None
        self._on_disconnected: Optional[Callable[[], None]] = None
        
        self._last_message_time = 0
        self._reconnect_task: Optional[asyncio.Task] = None
    
    def on_book(self, callback: Callable[[BookUpdate], None]) -> None:
        """Register callback for book updates."""
        self._on_book = callback
    
    def on_price_change(self, callback: Callable[[PriceChange], None]) -> None:
        """Register callback for price changes."""
        self._on_price_change = callback
    
    def on_best_bid_ask(self, callback: Callable[[BestBidAsk], None]) -> None:
        """Register callback for best bid/ask updates."""
        self._on_best_bid_ask = callback
    
    def on_error(self, callback: Callable[[Exception], None]) -> None:
        """Register callback for errors."""
        self._on_error = callback
    
    def on_connected(self, callback: Callable[[], None]) -> None:
        """Register callback for connection established."""
        self._on_connected = callback
    
    def on_disconnected(self, callback: Callable[[], None]) -> None:
        """Register callback for disconnection."""
        self._on_disconnected = callback
    
    async def connect(self, asset_ids: list[str]) -> None:
        """Connect to WebSocket and subscribe to assets."""
        self._running = True
        self._subscribed_assets = set(asset_ids)
        
        while self._running:
            try:
                await self._connect_and_subscribe()
                await self._message_loop()
            except ConnectionClosed as e:
                if self._on_disconnected:
                    self._on_disconnected()
                if self._running:
                    await asyncio.sleep(self.reconnect_delay)
            except Exception as e:
                if self._on_error:
                    self._on_error(e)
                if self._running:
                    await asyncio.sleep(self.reconnect_delay)
    
    async def _connect_and_subscribe(self) -> None:
        """Establish connection and send subscription message."""
        self._ws = await websockets.connect(
            self.ws_url,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_interval * 2,
        )
        
        # Build subscription message for market channel
        subscribe_msg = {
            "type": "MARKET",
            "assets_ids": list(self._subscribed_assets),
            "custom_feature_enabled": True,  # Enable best_bid_ask messages
        }
        
        # Add auth if available (for user channel)
        if self.auth and self.auth.has_l2_credentials():
            subscribe_msg["auth"] = {
                "apiKey": self.auth.api_key,
                "secret": self.auth.api_secret,
                "passphrase": self.auth.api_passphrase,
            }
        
        await self._ws.send(json.dumps(subscribe_msg))
        
        if self._on_connected:
            self._on_connected()
    
    async def _message_loop(self) -> None:
        """Process incoming messages."""
        async for message in self._ws:
            self._last_message_time = time.time()
            
            try:
                data = json.loads(message)
                await self._handle_message(data)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                if self._on_error:
                    self._on_error(e)
    
    async def _handle_message(self, data: dict[str, Any]) -> None:
        """Route message to appropriate handler."""
        event_type = data.get("event_type", "")
        
        if event_type == "book":
            if self._on_book:
                update = BookUpdate(
                    asset_id=data.get("asset_id", ""),
                    market=data.get("market", ""),
                    bids=[
                        (Decimal(b["price"]), Decimal(b["size"]))
                        for b in data.get("bids", [])
                    ],
                    asks=[
                        (Decimal(a["price"]), Decimal(a["size"]))
                        for a in data.get("asks", [])
                    ],
                    timestamp=int(data.get("timestamp", 0)),
                    hash=data.get("hash", ""),
                )
                self._on_book(update)
        
        elif event_type == "price_change":
            if self._on_price_change:
                for change in data.get("price_changes", []):
                    update = PriceChange(
                        asset_id=change.get("asset_id", ""),
                        market=data.get("market", ""),
                        price=Decimal(change.get("price", "0")),
                        size=Decimal(change.get("size", "0")),
                        side=change.get("side", ""),
                        best_bid=Decimal(change.get("best_bid", "0")),
                        best_ask=Decimal(change.get("best_ask", "0")),
                        timestamp=int(data.get("timestamp", 0)),
                    )
                    self._on_price_change(update)
        
        elif event_type == "best_bid_ask":
            if self._on_best_bid_ask:
                update = BestBidAsk(
                    asset_id=data.get("asset_id", ""),
                    market=data.get("market", ""),
                    best_bid=Decimal(data.get("best_bid", "0")),
                    best_ask=Decimal(data.get("best_ask", "0")),
                    spread=Decimal(data.get("spread", "0")),
                    timestamp=int(data.get("timestamp", 0)),
                )
                self._on_best_bid_ask(update)
    
    async def subscribe(self, asset_ids: list[str]) -> None:
        """Subscribe to additional assets."""
        if not self._ws or self._ws.closed:
            self._subscribed_assets.update(asset_ids)
            return
        
        new_assets = set(asset_ids) - self._subscribed_assets
        if not new_assets:
            return
        
        self._subscribed_assets.update(new_assets)
        
        msg = {
            "assets_ids": list(new_assets),
            "operation": "subscribe",
            "custom_feature_enabled": True,
        }
        await self._ws.send(json.dumps(msg))
    
    async def unsubscribe(self, asset_ids: list[str]) -> None:
        """Unsubscribe from assets."""
        if not self._ws or self._ws.closed:
            self._subscribed_assets -= set(asset_ids)
            return
        
        to_remove = set(asset_ids) & self._subscribed_assets
        if not to_remove:
            return
        
        self._subscribed_assets -= to_remove
        
        msg = {
            "assets_ids": list(to_remove),
            "operation": "unsubscribe",
        }
        await self._ws.send(json.dumps(msg))
    
    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        self._running = False
        
        if self._ws and not self._ws.closed:
            await self._ws.close()
        
        if self._on_disconnected:
            self._on_disconnected()
    
    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._ws is not None and not self._ws.closed
    
    @property
    def last_message_age(self) -> float:
        """Seconds since last message received."""
        if self._last_message_time == 0:
            return float("inf")
        return time.time() - self._last_message_time
