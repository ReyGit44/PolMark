"""
REST client for Polymarket CLOB API.
Handles orderbook queries, order placement, and account operations.
"""

import asyncio
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

import aiohttp

from .auth import AuthManager


@dataclass
class OrderBookLevel:
    """Single price level in orderbook."""
    price: Decimal
    size: Decimal


@dataclass
class OrderBook:
    """Orderbook snapshot."""
    asset_id: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    timestamp: int
    hash: str


@dataclass
class Order:
    """Order representation."""
    order_id: str
    token_id: str
    side: str  # BUY or SELL
    price: Decimal
    size: Decimal
    status: str
    created_at: Optional[int] = None


@dataclass
class Trade:
    """Trade representation."""
    trade_id: str
    order_id: str
    token_id: str
    side: str
    price: Decimal
    size: Decimal
    fee: Decimal
    timestamp: int


class RateLimiter:
    """Simple rate limiter with exponential backoff."""
    
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: list[float] = []
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Wait until a request can be made."""
        async with self._lock:
            now = time.time()
            # Remove old requests outside window
            self.requests = [t for t in self.requests if now - t < self.window_seconds]
            
            if len(self.requests) >= self.max_requests:
                # Wait until oldest request expires
                sleep_time = self.window_seconds - (now - self.requests[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                self.requests = self.requests[1:]
            
            self.requests.append(time.time())


class PolymarketRestClient:
    """REST client for Polymarket CLOB API."""
    
    def __init__(
        self,
        auth_manager: AuthManager,
        base_url: str = "https://clob.polymarket.com",
        gamma_url: str = "https://gamma-api.polymarket.com",
        timeout_seconds: int = 10,
        max_retries: int = 3,
        retry_backoff_base: float = 1.5,
    ):
        self.auth = auth_manager
        self.base_url = base_url.rstrip("/")
        self.gamma_url = gamma_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiters per endpoint category
        self._book_limiter = RateLimiter(150, 10)  # 1500/10s = 150/s
        self._order_limiter = RateLimiter(350, 10)  # 3500/10s burst
        self._general_limiter = RateLimiter(900, 10)  # 9000/10s
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session
    
    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def _request(
        self,
        method: str,
        url: str,
        authenticated: bool = False,
        body: Optional[dict] = None,
        limiter: Optional[RateLimiter] = None,
    ) -> dict[str, Any]:
        """Make HTTP request with retry logic."""
        session = await self._get_session()
        limiter = limiter or self._general_limiter
        
        for attempt in range(self.max_retries):
            await limiter.acquire()
            
            headers = {"Content-Type": "application/json"}
            body_str = json.dumps(body) if body else ""
            
            if authenticated:
                path = url.replace(self.base_url, "")
                auth_headers = self.auth.get_l2_headers(method, path, body_str)
                headers.update(auth_headers)
            
            try:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    data=body_str if body else None,
                ) as response:
                    if response.status == 429:
                        # Rate limited - exponential backoff
                        wait_time = self.retry_backoff_base ** attempt
                        await asyncio.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    return await response.json()
                    
            except aiohttp.ClientError as e:
                if attempt == self.max_retries - 1:
                    raise
                wait_time = self.retry_backoff_base ** attempt
                await asyncio.sleep(wait_time)
        
        raise RuntimeError(f"Request failed after {self.max_retries} attempts")
    
    # === Public Endpoints ===
    
    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Get orderbook for a token."""
        url = f"{self.base_url}/book?token_id={token_id}"
        data = await self._request("GET", url, limiter=self._book_limiter)
        
        return OrderBook(
            asset_id=data.get("asset_id", token_id),
            bids=[
                OrderBookLevel(Decimal(b["price"]), Decimal(b["size"]))
                for b in data.get("bids", [])
            ],
            asks=[
                OrderBookLevel(Decimal(a["price"]), Decimal(a["size"]))
                for a in data.get("asks", [])
            ],
            timestamp=int(data.get("timestamp", 0)),
            hash=data.get("hash", ""),
        )
    
    async def get_price(self, token_id: str) -> dict[str, Decimal]:
        """Get current price for a token."""
        url = f"{self.base_url}/price?token_id={token_id}"
        data = await self._request("GET", url, limiter=self._book_limiter)
        
        return {
            "bid": Decimal(data.get("bid", "0")),
            "ask": Decimal(data.get("ask", "0")),
            "mid": Decimal(data.get("mid", "0")),
        }
    
    async def get_midpoint(self, token_id: str) -> Decimal:
        """Get midpoint price for a token."""
        url = f"{self.base_url}/midpoint?token_id={token_id}"
        data = await self._request("GET", url, limiter=self._book_limiter)
        return Decimal(data.get("mid", "0"))
    
    async def get_tick_size(self, token_id: str) -> str:
        """Get tick size for a token."""
        url = f"{self.base_url}/tick-size?token_id={token_id}"
        data = await self._request("GET", url)
        return data.get("minimum_tick_size", "0.01")
    
    # === Authenticated Endpoints ===
    
    async def derive_api_key(self, nonce: int = 0) -> dict[str, str]:
        """Derive API credentials using L1 authentication."""
        url = f"{self.base_url}/auth/derive-api-key"
        session = await self._get_session()
        
        headers = self.auth.get_l1_headers(nonce)
        headers["Content-Type"] = "application/json"
        
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            
            # Store credentials in auth manager
            self.auth.set_api_credentials(
                data["apiKey"],
                data["secret"],
                data["passphrase"],
            )
            
            return {
                "api_key": data["apiKey"],
                "api_secret": data["secret"],
                "api_passphrase": data["passphrase"],
            }
    
    async def create_api_key(self, nonce: int = 0) -> dict[str, str]:
        """Create new API credentials using L1 authentication."""
        url = f"{self.base_url}/auth/api-key"
        session = await self._get_session()
        
        headers = self.auth.get_l1_headers(nonce)
        headers["Content-Type"] = "application/json"
        
        async with session.post(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            
            self.auth.set_api_credentials(
                data["apiKey"],
                data["secret"],
                data["passphrase"],
            )
            
            return {
                "api_key": data["apiKey"],
                "api_secret": data["secret"],
                "api_passphrase": data["passphrase"],
            }
    
    async def get_balance_allowance(self, asset_type: str = "USDC") -> dict[str, Decimal]:
        """Get balance and allowance for an asset."""
        url = f"{self.base_url}/balance-allowance?asset_type={asset_type}"
        data = await self._request("GET", url, authenticated=True)
        
        return {
            "balance": Decimal(data.get("balance", "0")),
            "allowance": Decimal(data.get("allowance", "0")),
        }
    
    async def post_order(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        order_type: str = "GTC",
        tick_size: str = "0.01",
        neg_risk: bool = False,
        funder: Optional[str] = None,
        signature_type: int = 2,
    ) -> Order:
        """
        Post a new order.
        
        Note: This is a simplified implementation. The actual py-clob-client
        handles order signing with EIP-712. For production, use the official client.
        """
        url = f"{self.base_url}/order"
        
        body = {
            "tokenID": token_id,
            "price": str(price),
            "size": str(size),
            "side": side.upper(),
            "orderType": order_type,
            "tickSize": tick_size,
            "negRisk": neg_risk,
        }
        
        if funder:
            body["funder"] = funder
        
        data = await self._request(
            "POST", url, authenticated=True, body=body, limiter=self._order_limiter
        )
        
        return Order(
            order_id=data.get("orderID", ""),
            token_id=token_id,
            side=side.upper(),
            price=price,
            size=size,
            status=data.get("status", "PENDING"),
        )
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        url = f"{self.base_url}/order"
        body = {"orderID": order_id}
        
        try:
            await self._request(
                "DELETE", url, authenticated=True, body=body, limiter=self._order_limiter
            )
            return True
        except Exception:
            return False
    
    async def cancel_all_orders(self) -> bool:
        """Cancel all open orders."""
        url = f"{self.base_url}/cancel-all"
        
        try:
            await self._request("DELETE", url, authenticated=True)
            return True
        except Exception:
            return False
    
    async def get_open_orders(self, market: Optional[str] = None) -> list[Order]:
        """Get all open orders."""
        url = f"{self.base_url}/orders"
        if market:
            url += f"?market={market}"
        
        data = await self._request("GET", url, authenticated=True)
        
        orders = []
        for o in data:
            orders.append(Order(
                order_id=o.get("id", ""),
                token_id=o.get("asset_id", ""),
                side=o.get("side", ""),
                price=Decimal(o.get("price", "0")),
                size=Decimal(o.get("original_size", "0")),
                status=o.get("status", ""),
                created_at=o.get("created_at"),
            ))
        
        return orders
    
    async def get_trades(
        self,
        market: Optional[str] = None,
        limit: int = 100,
    ) -> list[Trade]:
        """Get trade history."""
        url = f"{self.base_url}/trades?limit={limit}"
        if market:
            url += f"&market={market}"
        
        data = await self._request("GET", url, authenticated=True)
        
        trades = []
        for t in data:
            trades.append(Trade(
                trade_id=t.get("id", ""),
                order_id=t.get("order_id", ""),
                token_id=t.get("asset_id", ""),
                side=t.get("side", ""),
                price=Decimal(t.get("price", "0")),
                size=Decimal(t.get("size", "0")),
                fee=Decimal(t.get("fee", "0")),
                timestamp=int(t.get("match_time", 0)),
            ))
        
        return trades
    
    # === Gamma API (Market Discovery) ===
    
    async def get_market_info(self, condition_id: str) -> dict[str, Any]:
        """Get market information from Gamma API."""
        url = f"{self.gamma_url}/markets/{condition_id}"
        return await self._request("GET", url)
    
    async def search_markets(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search for markets."""
        url = f"{self.gamma_url}/markets?_q={query}&_limit={limit}"
        return await self._request("GET", url)
