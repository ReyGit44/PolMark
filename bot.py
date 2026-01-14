"""
Main arbitrage bot orchestration.
Coordinates all modules for parity arbitrage execution.
"""

import asyncio
import signal
import time
from decimal import Decimal
from typing import Optional

from .config import Config, load_config_from_env
from .connector import AuthManager, PolymarketRestClient, PolymarketWebSocketClient
from .connector.ws_client import BookUpdate, PriceChange, BestBidAsk
from .orderbook import OrderBookManager
from .signals import ParityDetector, ParitySignal, ConvergenceDetector
from .exec import PairedExecutor, ExecutionResult, ExecutionStatus
from .positions import PositionManager, PairedPosition
from .risk import RiskManager
from .monitor import Logger, MetricsCollector
from .storage import Database


class ArbitrageBot:
    """
    Parity arbitrage bot for Polymarket.
    
    Strategy:
    1. Monitor YES/NO orderbooks via WebSocket
    2. Detect parity opportunities (YES_ask + NO_ask < 1 - costs)
    3. Execute paired trades atomically
    4. Hold until convergence or resolution
    5. Exit to lock profit
    """
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or load_config_from_env()
        
        # Validate configuration
        errors = self.config.validate()
        if errors:
            raise ValueError(f"Configuration errors: {errors}")
        
        # Initialize components
        self.logger = Logger(
            name="arb_bot",
            level=self.config.log_level,
            log_file=self.config.log_file,
        )
        
        self.auth = AuthManager(
            private_key=self.config.private_key,
            api_key=self.config.api_key,
            api_secret=self.config.api_secret,
            api_passphrase=self.config.api_passphrase,
            chain_id=self.config.connection.chain_id,
        )
        
        self.rest_client = PolymarketRestClient(
            auth_manager=self.auth,
            base_url=self.config.connection.clob_rest_url,
            gamma_url=self.config.connection.gamma_api_url,
            timeout_seconds=self.config.connection.rest_timeout_seconds,
            max_retries=self.config.connection.max_retries,
        )
        
        self.ws_client = PolymarketWebSocketClient(
            auth_manager=self.auth,
            ws_url=self.config.connection.clob_ws_url,
            reconnect_delay=self.config.connection.ws_reconnect_delay_seconds,
            ping_interval=self.config.connection.ws_ping_interval_seconds,
        )
        
        self.orderbook = OrderBookManager()
        
        self.parity_detector = ParityDetector(
            orderbook_manager=self.orderbook,
            fee_config=self.config.fees,
            trading_config=self.config.trading,
        )
        
        self.convergence_detector = ConvergenceDetector(
            orderbook_manager=self.orderbook,
            convergence_threshold=Decimal(str(self.config.trading.convergence_threshold)),
        )
        
        self.position_manager = PositionManager(
            max_open_pairs=self.config.trading.max_open_pairs,
        )
        
        self.executor = PairedExecutor(
            rest_client=self.rest_client,
            funder_address=self.config.funder_address,
            signature_type=self.config.signature_type,
            order_timeout_seconds=self.config.trading.order_timeout_seconds,
            logger=self.logger,
        )
        
        self.risk_manager = RiskManager(
            risk_config=self.config.risk,
            trading_config=self.config.trading,
            position_manager=self.position_manager,
            logger=self.logger,
        )
        
        self.metrics = MetricsCollector()
        self.database = Database(self.config.db_path)
        
        # State
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._main_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start the arbitrage bot."""
        self._running = True
        
        self.logger.startup({
            "markets": len(self.config.markets),
            "min_edge": str(self.config.trading.min_edge),
            "max_notional": str(self.config.trading.max_notional_per_trade),
        })
        
        try:
            # Derive API credentials if not provided
            if not self.auth.has_l2_credentials():
                self.logger.info("deriving_api_credentials")
                await self.rest_client.derive_api_key()
            
            # Load existing positions from database
            await self._load_state()
            
            # Initialize markets
            await self._init_markets()
            
            # Setup WebSocket callbacks
            self._setup_ws_callbacks()
            
            # Setup risk callbacks
            self.risk_manager.on_kill_switch(self._on_kill_switch)
            
            # Start main loops
            await asyncio.gather(
                self._ws_loop(),
                self._trading_loop(),
                self._exit_monitor_loop(),
                self._health_check_loop(),
                self._state_save_loop(),
            )
            
        except asyncio.CancelledError:
            self.logger.info("bot_cancelled")
        except Exception as e:
            self.logger.error("bot_error", error=str(e))
            raise
        finally:
            await self._cleanup()
    
    async def stop(self) -> None:
        """Stop the arbitrage bot gracefully."""
        self.logger.info("bot_stopping")
        self._running = False
        self._shutdown_event.set()
        
        # Cancel all open orders
        try:
            await self.rest_client.cancel_all_orders()
        except Exception as e:
            self.logger.error("cancel_orders_failed", error=str(e))
        
        # Save state
        await self._save_state()
        
        self.logger.shutdown()
    
    async def _load_state(self) -> None:
        """Load state from database."""
        # Load open positions
        positions = self.database.get_open_positions()
        for pos in positions:
            self.position_manager.add_position(pos)
        
        self.logger.info("state_loaded", open_positions=len(positions))
    
    async def _save_state(self) -> None:
        """Save state to database."""
        # Save all positions
        for pos in self.position_manager.get_all_positions():
            self.database.save_position(pos)
        
        # Save metrics
        metrics = self.metrics.get_session_metrics()
        self.database.save_state("last_session_metrics", metrics)
        
        self.logger.info("state_saved")
    
    async def _init_markets(self) -> None:
        """Initialize markets from configuration."""
        for market_config in self.config.markets:
            self.orderbook.add_market(
                condition_id=market_config.condition_id,
                yes_token_id=market_config.yes_token_id,
                no_token_id=market_config.no_token_id,
                tick_size=market_config.tick_size,
                neg_risk=market_config.neg_risk,
            )
            
            self.logger.info(
                "market_added",
                condition_id=market_config.condition_id,
            )
    
    def _setup_ws_callbacks(self) -> None:
        """Setup WebSocket event callbacks."""
        
        def on_book(update: BookUpdate) -> None:
            self.orderbook.update_book_snapshot(
                token_id=update.asset_id,
                bids=update.bids,
                asks=update.asks,
                book_hash=update.hash,
            )
            self.logger.debug(
                "book_update",
                asset_id=update.asset_id,
                bids=len(update.bids),
                asks=len(update.asks),
            )
        
        def on_price_change(update: PriceChange) -> None:
            self.orderbook.update_price_level(
                token_id=update.asset_id,
                side=update.side,
                price=update.price,
                size=update.size,
            )
        
        def on_best_bid_ask(update: BestBidAsk) -> None:
            self.orderbook.update_best_bid_ask(
                token_id=update.asset_id,
                best_bid=update.best_bid,
                best_ask=update.best_ask,
            )
            self.risk_manager.update_ws_status(True, time.time())
        
        def on_connected() -> None:
            self.logger.ws_connected(self.config.connection.clob_ws_url)
            self.risk_manager.update_ws_status(True, time.time())
        
        def on_disconnected() -> None:
            self.logger.ws_disconnected()
            self.risk_manager.update_ws_status(False)
            self.metrics.record_ws_reconnect()
        
        def on_error(e: Exception) -> None:
            self.logger.error("ws_error", error=str(e))
            self.metrics.record_api_error()
        
        self.ws_client.on_book(on_book)
        self.ws_client.on_price_change(on_price_change)
        self.ws_client.on_best_bid_ask(on_best_bid_ask)
        self.ws_client.on_connected(on_connected)
        self.ws_client.on_disconnected(on_disconnected)
        self.ws_client.on_error(on_error)
    
    def _on_kill_switch(self, reason: str) -> None:
        """Handle kill switch activation."""
        self.logger.critical("kill_switch_activated", reason=reason)
        # Don't stop the bot, just halt trading
        # Manual intervention required to reset
    
    async def _ws_loop(self) -> None:
        """WebSocket connection loop."""
        token_ids = self.orderbook.get_all_token_ids()
        
        while self._running:
            try:
                await self.ws_client.connect(token_ids)
            except Exception as e:
                self.logger.error("ws_loop_error", error=str(e))
                await asyncio.sleep(self.config.connection.ws_reconnect_delay_seconds)
    
    async def _trading_loop(self) -> None:
        """Main trading loop - scan for opportunities and execute."""
        scan_interval = 0.1  # 100ms between scans
        
        while self._running:
            try:
                # Check if trading is allowed
                risk_check = self.risk_manager.check_can_trade()
                
                if not risk_check.passed:
                    self.logger.debug(
                        "trading_blocked",
                        violation=risk_check.violation.value if risk_check.violation else None,
                        message=risk_check.message,
                    )
                    await asyncio.sleep(scan_interval)
                    continue
                
                # Scan for opportunities
                signal = self.parity_detector.get_best_opportunity()
                
                if signal and signal.is_profitable:
                    self.metrics.record_signal()
                    await self._execute_signal(signal)
                
                await asyncio.sleep(scan_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("trading_loop_error", error=str(e))
                await asyncio.sleep(1)
    
    async def _execute_signal(self, signal: ParitySignal) -> None:
        """Execute a parity arbitrage signal."""
        self.logger.trade_signal(
            condition_id=signal.condition_id,
            yes_ask=str(signal.yes_ask),
            no_ask=str(signal.no_ask),
            edge=str(signal.net_edge),
            size=str(signal.max_size),
        )
        
        # Check trade size
        size_check = self.risk_manager.check_trade_size(
            signal.max_size,
            signal.combined_cost,
        )
        
        if not size_check.passed:
            self.logger.risk_check_failed(
                size_check.violation.value if size_check.violation else "unknown",
                size_check.message,
            )
            return
        
        # Execute trade
        self.metrics.record_trade_attempt()
        start_time = time.time()
        
        try:
            result = await self.executor.execute_parity_trade(signal)
            execution_time_ms = (time.time() - start_time) * 1000
            
            if result.status == ExecutionStatus.COMPLETE:
                # Success - create position
                position = PairedPosition.from_execution(result)
                self.position_manager.add_position(position)
                self.database.save_position(position)
                
                self.metrics.record_trade_success(
                    execution_id=result.execution_id,
                    condition_id=result.condition_id,
                    entry_cost=result.entry_cost,
                    expected_pnl=result.expected_profit,
                    execution_time_ms=execution_time_ms,
                )
                
                self.risk_manager.record_trade(True)
                
                self.logger.trade_executed(
                    execution_id=result.execution_id,
                    condition_id=result.condition_id,
                    size=str(result.actual_filled_size),
                    entry_cost=str(result.entry_cost),
                    expected_profit=str(result.expected_profit),
                )
                
            elif result.status == ExecutionStatus.PARTIAL:
                # Partial fill - still create position for filled amount
                if result.actual_filled_size > 0:
                    position = PairedPosition.from_execution(result)
                    self.position_manager.add_position(position)
                    self.database.save_position(position)
                
                self.metrics.record_trade_partial(
                    result.execution_id,
                    result.condition_id,
                )
                self.risk_manager.record_trade(True)
                
            else:
                # Failed
                self.metrics.record_trade_failure(
                    result.execution_id,
                    result.condition_id,
                )
                self.risk_manager.record_trade(False)
                
                self.logger.trade_failed(
                    execution_id=result.execution_id,
                    condition_id=result.condition_id,
                    error=result.error or "Unknown error",
                )
                
        except Exception as e:
            self.risk_manager.record_trade(False)
            self.logger.error("execution_error", error=str(e))
    
    async def _exit_monitor_loop(self) -> None:
        """Monitor open positions for exit opportunities."""
        check_interval = 1.0  # Check every second
        
        while self._running:
            try:
                for position in self.position_manager.get_open_positions():
                    should_exit, reason = self.convergence_detector.should_exit(
                        position.condition_id
                    )
                    
                    if should_exit:
                        self.logger.info(
                            "exit_triggered",
                            position_id=position.position_id,
                            reason=reason,
                        )
                        await self._exit_position(position)
                
                await asyncio.sleep(check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("exit_monitor_error", error=str(e))
                await asyncio.sleep(check_interval)
    
    async def _exit_position(self, position: PairedPosition) -> None:
        """Exit a paired position."""
        try:
            result = await self.executor.exit_position(
                condition_id=position.condition_id,
                yes_token_id=position.yes_token_id,
                no_token_id=position.no_token_id,
                size=position.size,
            )
            
            if result.status == ExecutionStatus.COMPLETE:
                exit_proceeds = (
                    result.yes_leg.price * result.yes_leg.filled_size +
                    result.no_leg.price * result.no_leg.filled_size
                )
                
                position.close(
                    yes_exit_price=result.yes_leg.price,
                    no_exit_price=result.no_leg.price,
                    exit_proceeds=exit_proceeds,
                )
                
                self.database.save_position(position)
                
                self.metrics.record_position_closed(
                    execution_id=position.position_id,
                    realized_pnl=position.realized_pnl,
                    holding_time_seconds=position.holding_time_seconds,
                )
                
                self.risk_manager.record_pnl(position.realized_pnl)
                
                self.logger.position_closed(
                    position_id=position.position_id,
                    condition_id=position.condition_id,
                    realized_pnl=str(position.realized_pnl),
                    holding_time_seconds=position.holding_time_seconds,
                )
                
        except Exception as e:
            self.logger.error(
                "exit_position_error",
                position_id=position.position_id,
                error=str(e),
            )
    
    async def _health_check_loop(self) -> None:
        """Periodic health checks."""
        interval = self.config.risk.health_check_interval_seconds
        
        while self._running:
            try:
                health = self.risk_manager.run_health_check()
                self.logger.health_check(health["healthy"], health.get("issues", []))
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("health_check_error", error=str(e))
                await asyncio.sleep(interval)
    
    async def _state_save_loop(self) -> None:
        """Periodic state saves."""
        interval = 60  # Save every minute
        
        while self._running:
            try:
                await asyncio.sleep(interval)
                await self._save_state()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("state_save_error", error=str(e))
    
    async def _cleanup(self) -> None:
        """Cleanup resources."""
        await self.ws_client.disconnect()
        await self.rest_client.close()
        self.logger.info("cleanup_complete")
    
    def get_status(self) -> dict:
        """Get current bot status."""
        return {
            "running": self._running,
            "risk": self.risk_manager.get_status(),
            "positions": self.position_manager.get_summary(),
            "metrics": self.metrics.get_session_metrics(),
            "ws_connected": self.ws_client.is_connected,
        }


async def run_bot(config: Optional[Config] = None) -> None:
    """Run the arbitrage bot with signal handling."""
    bot = ArbitrageBot(config)
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        asyncio.create_task(bot.stop())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        await bot.stop()
