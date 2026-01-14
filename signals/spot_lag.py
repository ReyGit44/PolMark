"""
Spot-Lag Probability Signal Detector.

Detects when Polymarket prices lag behind actual spot price movements.
When spot moves significantly but Polymarket hasn't repriced, there's
an edge in betting on the direction spot has already moved.

Strategy:
1. Monitor real-time spot prices (Binance)
2. Compare to Polymarket implied probability
3. When spot moves but PM lags, bet in direction of spot move
"""

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional
import aiohttp


@dataclass
class SpotData:
    """Current spot price data."""
    symbol: str
    price: Decimal
    open_price: Decimal  # Candle open
    change_pct: Decimal  # % change from open
    timestamp: float


@dataclass
class SpotLagSignal:
    """
    Signal when Polymarket lags spot price movement.
    
    If BTC is up 0.5% in current candle but PM still prices UP at 50%,
    there's edge in buying UP (should be priced higher).
    """
    condition_id: str
    symbol: str
    spot_change_pct: Decimal  # Actual spot move
    pm_up_price: Decimal  # Polymarket UP price
    pm_down_price: Decimal  # Polymarket DOWN price
    implied_up_prob: Decimal  # What PM thinks
    fair_up_prob: Decimal  # What it should be based on spot
    edge: Decimal  # fair - implied
    direction: str  # "UP" or "DOWN"
    recommended_price: Decimal  # Price to buy at
    token_id: str  # Token to buy
    max_size: Decimal
    timestamp: float
    
    @property
    def is_profitable(self) -> bool:
        return abs(self.edge) > Decimal("0.02")  # 2% edge minimum


class BinanceSpotFeed:
    """
    Real-time spot price feed.
    Uses multiple sources: Kraken (primary), CoinGecko (fallback).
    Tracks current candle open and live price.
    """
    
    KRAKEN_SYMBOLS = {
        "btc": "XXBTZUSD",
        "eth": "XETHZUSD",
        "sol": "SOLUSD",
    }
    
    COINGECKO_IDS = {
        "btc": "bitcoin",
        "eth": "ethereum",
        "sol": "solana",
    }
    
    def __init__(self):
        self._prices: dict[str, SpotData] = {}
        self._ws = None
        self._running = False
        self._callbacks: list[Callable[[SpotData], None]] = []
        self._candle_opens: dict[str, Decimal] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_candle_time: dict[str, int] = {}
    
    def on_price(self, callback: Callable[[SpotData], None]) -> None:
        """Register callback for price updates."""
        self._callbacks.append(callback)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def get_current_price(self, symbol: str) -> Optional[SpotData]:
        """Fetch current price via REST - tries Kraken first, then CoinGecko."""
        symbol = symbol.lower()
        
        # Try Kraken first
        spot_data = await self._get_kraken_price(symbol)
        if spot_data:
            return spot_data
        
        # Fallback to CoinGecko
        spot_data = await self._get_coingecko_price(symbol)
        return spot_data
    
    async def _get_kraken_price(self, symbol: str) -> Optional[SpotData]:
        """Fetch from Kraken."""
        kraken_symbol = self.KRAKEN_SYMBOLS.get(symbol)
        if not kraken_symbol:
            return None
        
        try:
            session = await self._get_session()
            
            # Get current ticker
            async with session.get(
                f"https://api.kraken.com/0/public/Ticker?pair={kraken_symbol}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                data = await resp.json()
                if data.get("error"):
                    return None
                
                result = data.get("result", {})
                ticker_data = list(result.values())[0] if result else None
                if not ticker_data:
                    return None
                
                current_price = Decimal(ticker_data["c"][0])  # Last trade price
                open_price = Decimal(ticker_data["o"])  # Today's open
            
            # Get 15-min OHLC for candle open
            current_15min = (int(time.time()) // 900) * 900
            if symbol not in self._candle_opens or self._last_candle_time.get(symbol, 0) != current_15min:
                async with session.get(
                    f"https://api.kraken.com/0/public/OHLC?pair={kraken_symbol}&interval=15",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    ohlc_data = await resp.json()
                    if not ohlc_data.get("error"):
                        ohlc_result = ohlc_data.get("result", {})
                        candles = list(ohlc_result.values())[0] if ohlc_result else []
                        if candles and isinstance(candles, list) and len(candles) > 0:
                            # Last candle open price
                            last_candle = candles[-1]
                            if len(last_candle) > 1:
                                self._candle_opens[symbol] = Decimal(str(last_candle[1]))
                                self._last_candle_time[symbol] = current_15min
            
            candle_open = self._candle_opens.get(symbol, current_price)
            change_pct = ((current_price - candle_open) / candle_open) * 100 if candle_open > 0 else Decimal("0")
            
            spot_data = SpotData(
                symbol=symbol,
                price=current_price,
                open_price=candle_open,
                change_pct=change_pct,
                timestamp=time.time(),
            )
            
            self._prices[symbol] = spot_data
            return spot_data
            
        except Exception as e:
            return None
    
    async def _get_coingecko_price(self, symbol: str) -> Optional[SpotData]:
        """Fetch from CoinGecko (rate limited, use as fallback)."""
        cg_id = self.COINGECKO_IDS.get(symbol)
        if not cg_id:
            return None
        
        try:
            session = await self._get_session()
            
            async with session.get(
                f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd&include_24hr_change=true",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                data = await resp.json()
                
                if cg_id not in data:
                    return None
                
                current_price = Decimal(str(data[cg_id]["usd"]))
                # CoinGecko doesn't give 15-min candle, estimate from 24h change
                change_24h = Decimal(str(data[cg_id].get("usd_24h_change", 0)))
                # Rough estimate: 15-min is ~1% of 24h volatility
                change_pct = change_24h / Decimal("96")  # 96 15-min periods in 24h
                
                # Use cached open or current price
                candle_open = self._candle_opens.get(symbol, current_price)
                
                spot_data = SpotData(
                    symbol=symbol,
                    price=current_price,
                    open_price=candle_open,
                    change_pct=change_pct,
                    timestamp=time.time(),
                )
                
                self._prices[symbol] = spot_data
                return spot_data
                
        except Exception:
            return None
    
    async def start_ws_feed(self) -> None:
        """Start WebSocket feed for real-time prices (Kraken)."""
        self._running = True
        
        url = "wss://ws.kraken.com"
        
        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        # Subscribe to tickers
                        subscribe_msg = {
                            "event": "subscribe",
                            "pair": list(self.KRAKEN_SYMBOLS.values()),
                            "subscription": {"name": "ticker"}
                        }
                        await ws.send_json(subscribe_msg)
                        
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = msg.json()
                                if isinstance(data, list) and len(data) >= 4:
                                    await self._handle_kraken_ticker(data)
            except Exception:
                if self._running:
                    await asyncio.sleep(5)
    
    async def _handle_kraken_ticker(self, data: list) -> None:
        """Handle ticker update from Kraken WS."""
        try:
            ticker_data = data[1]
            pair = data[3]
            
            symbol_map = {v: k for k, v in self.KRAKEN_SYMBOLS.items()}
            symbol = symbol_map.get(pair)
            if not symbol:
                return
            
            current_price = Decimal(ticker_data["c"][0])
            open_price = Decimal(ticker_data["o"][0]) if "o" in ticker_data else current_price
            
            if open_price > 0:
                change_pct = ((current_price - open_price) / open_price) * 100
            else:
                change_pct = Decimal("0")
            
            spot_data = SpotData(
                symbol=symbol,
                price=current_price,
                open_price=open_price,
                change_pct=change_pct,
                timestamp=time.time(),
            )
            
            self._prices[symbol] = spot_data
            
            for callback in self._callbacks:
                try:
                    callback(spot_data)
                except Exception:
                    pass
        except Exception:
            pass
    
    def get_cached_price(self, symbol: str) -> Optional[SpotData]:
        """Get cached price data."""
        return self._prices.get(symbol.lower())
    
    async def stop(self) -> None:
        """Stop the feed."""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()


class SpotLagDetector:
    """
    Detects spot-lag opportunities.
    
    When spot price moves but Polymarket prices haven't adjusted,
    there's edge in betting the direction spot has moved.
    
    Example:
    - BTC spot up 0.3% in current 15-min candle
    - PM still prices UP at $0.50 (50% probability)
    - Fair value for UP should be ~55-60% given spot move
    - Edge = buy UP at $0.50, fair value ~$0.57
    """
    
    # Empirical mapping: spot move -> fair UP probability adjustment
    # Based on historical data of how spot moves correlate with candle outcomes
    SPOT_MOVE_TO_PROB = {
        # (min_move%, max_move%): prob_adjustment
        (0.0, 0.1): Decimal("0.00"),
        (0.1, 0.2): Decimal("0.05"),
        (0.2, 0.3): Decimal("0.10"),
        (0.3, 0.5): Decimal("0.15"),
        (0.5, 0.75): Decimal("0.20"),
        (0.75, 1.0): Decimal("0.25"),
        (1.0, 1.5): Decimal("0.30"),
        (1.5, 2.0): Decimal("0.35"),
        (2.0, 100.0): Decimal("0.40"),
    }
    
    def __init__(
        self,
        spot_feed: BinanceSpotFeed,
        min_edge: Decimal = Decimal("0.02"),
        min_spot_move: Decimal = Decimal("0.1"),
    ):
        self.spot_feed = spot_feed
        self.min_edge = min_edge
        self.min_spot_move = min_spot_move
        
        self._market_tokens: dict[str, dict] = {}  # symbol -> {condition_id, up_token, down_token}
        self._pm_prices: dict[str, tuple[Decimal, Decimal]] = {}  # symbol -> (up_price, down_price)
        self._callbacks: list[Callable[[SpotLagSignal], None]] = []
    
    def on_signal(self, callback: Callable[[SpotLagSignal], None]) -> None:
        """Register callback for signals."""
        self._callbacks.append(callback)
    
    def register_market(
        self,
        symbol: str,
        condition_id: str,
        up_token_id: str,
        down_token_id: str,
    ) -> None:
        """Register a market to monitor."""
        self._market_tokens[symbol.lower()] = {
            "condition_id": condition_id,
            "up_token": up_token_id,
            "down_token": down_token_id,
        }
    
    def update_pm_prices(
        self,
        symbol: str,
        up_bid: Decimal,
        up_ask: Decimal,
        down_bid: Decimal,
        down_ask: Decimal,
    ) -> None:
        """Update Polymarket prices for a symbol."""
        # Use mid prices for implied probability
        up_mid = (up_bid + up_ask) / 2 if up_bid > 0 and up_ask < 1 else up_ask
        down_mid = (down_bid + down_ask) / 2 if down_bid > 0 and down_ask < 1 else down_ask
        
        self._pm_prices[symbol.lower()] = (up_mid, down_mid)
    
    def _get_fair_prob_adjustment(self, spot_change_pct: Decimal) -> Decimal:
        """
        Get fair probability adjustment based on spot move.
        Positive spot move -> positive adjustment to UP probability.
        """
        abs_move = abs(spot_change_pct)
        sign = Decimal("1") if spot_change_pct >= 0 else Decimal("-1")
        
        for (min_move, max_move), adjustment in self.SPOT_MOVE_TO_PROB.items():
            if Decimal(str(min_move)) <= abs_move < Decimal(str(max_move)):
                return adjustment * sign
        
        return Decimal("0")
    
    def check_opportunity(self, symbol: str) -> Optional[SpotLagSignal]:
        """
        Check for spot-lag opportunity on a symbol.
        """
        symbol = symbol.lower()
        
        # Get spot data
        spot_data = self.spot_feed.get_cached_price(symbol)
        if not spot_data:
            return None
        
        # Check minimum spot move
        if abs(spot_data.change_pct) < self.min_spot_move:
            return None
        
        # Get PM prices
        pm_prices = self._pm_prices.get(symbol)
        if not pm_prices:
            return None
        
        up_price, down_price = pm_prices
        
        # Get market info
        market_info = self._market_tokens.get(symbol)
        if not market_info:
            return None
        
        # Calculate implied probability (from PM prices)
        # In a binary market, UP price ≈ UP probability
        implied_up_prob = up_price
        
        # Calculate fair probability based on spot move
        base_prob = Decimal("0.50")  # Assume 50/50 at candle open
        prob_adjustment = self._get_fair_prob_adjustment(spot_data.change_pct)
        fair_up_prob = base_prob + prob_adjustment
        
        # Clamp to valid range
        fair_up_prob = max(Decimal("0.05"), min(Decimal("0.95"), fair_up_prob))
        
        # Calculate edge
        if spot_data.change_pct > 0:
            # Spot is up -> should buy UP
            edge = fair_up_prob - implied_up_prob
            direction = "UP"
            token_id = market_info["up_token"]
            recommended_price = up_price
        else:
            # Spot is down -> should buy DOWN
            fair_down_prob = Decimal("1") - fair_up_prob
            implied_down_prob = down_price
            edge = fair_down_prob - implied_down_prob
            direction = "DOWN"
            token_id = market_info["down_token"]
            recommended_price = down_price
        
        # Check minimum edge
        if edge < self.min_edge:
            return None
        
        signal = SpotLagSignal(
            condition_id=market_info["condition_id"],
            symbol=symbol,
            spot_change_pct=spot_data.change_pct,
            pm_up_price=up_price,
            pm_down_price=down_price,
            implied_up_prob=implied_up_prob,
            fair_up_prob=fair_up_prob,
            edge=edge,
            direction=direction,
            recommended_price=recommended_price,
            token_id=token_id,
            max_size=Decimal("100"),  # Will be updated with actual liquidity
            timestamp=time.time(),
        )
        
        # Emit signal
        for callback in self._callbacks:
            try:
                callback(signal)
            except Exception:
                pass
        
        return signal
    
    def scan_all(self) -> list[SpotLagSignal]:
        """Scan all registered markets for opportunities."""
        signals = []
        for symbol in self._market_tokens.keys():
            signal = self.check_opportunity(symbol)
            if signal and signal.is_profitable:
                signals.append(signal)
        
        return sorted(signals, key=lambda s: s.edge, reverse=True)
