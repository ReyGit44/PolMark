"""
Dual-leg execution engine for parity arbitrage.
Handles paired order placement, partial fills, and unwinding.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..connector import PolymarketRestClient
    from ..signals import ParitySignal
    from ..monitor import Logger


class LegStatus(Enum):
    """Status of a single leg."""
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ExecutionStatus(Enum):
    """Status of paired execution."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"
    UNWINDING = "unwinding"


@dataclass
class LegOrder:
    """Single leg of a paired trade."""
    leg_id: str
    token_id: str
    side: str
    price: Decimal
    size: Decimal
    order_id: Optional[str] = None
    filled_size: Decimal = Decimal("0")
    status: LegStatus = LegStatus.PENDING
    error: Optional[str] = None
    submitted_at: Optional[float] = None
    filled_at: Optional[float] = None


@dataclass
class ExecutionResult:
    """Result of a paired execution attempt."""
    execution_id: str
    condition_id: str
    yes_leg: LegOrder
    no_leg: LegOrder
    status: ExecutionStatus
    entry_cost: Decimal = Decimal("0")  # Total cost to enter
    expected_profit: Decimal = Decimal("0")
    actual_filled_size: Decimal = Decimal("0")
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    error: Optional[str] = None
    
    @property
    def is_complete(self) -> bool:
        return self.status == ExecutionStatus.COMPLETE
    
    @property
    def needs_unwind(self) -> bool:
        """Check if we have unbalanced exposure that needs unwinding."""
        return (
            self.yes_leg.filled_size != self.no_leg.filled_size
            and (self.yes_leg.filled_size > 0 or self.no_leg.filled_size > 0)
        )


class PairedExecutor:
    """
    Executes paired YES+NO orders for parity arbitrage.
    
    Key principles:
    1. Both legs must be sized equally
    2. Partial fills on one leg require immediate hedge/unwind
    3. No naked exposure allowed
    """
    
    def __init__(
        self,
        rest_client: "PolymarketRestClient",
        funder_address: str,
        signature_type: int = 2,
        order_timeout_seconds: int = 30,
        logger: Optional["Logger"] = None,
    ):
        self.client = rest_client
        self.funder = funder_address
        self.signature_type = signature_type
        self.order_timeout = order_timeout_seconds
        self.logger = logger
        
        self._active_executions: dict[str, ExecutionResult] = {}
        self._lock = asyncio.Lock()
    
    async def execute_parity_trade(
        self,
        signal: "ParitySignal",
        size: Optional[Decimal] = None,
    ) -> ExecutionResult:
        """
        Execute a parity arbitrage trade.
        
        Places orders for both YES and NO sides simultaneously.
        If either fails or partially fills, attempts to unwind.
        """
        execution_id = str(uuid.uuid4())
        trade_size = size or signal.max_size
        
        # Create leg orders
        yes_leg = LegOrder(
            leg_id=f"{execution_id}-yes",
            token_id=signal.yes_token_id,
            side="BUY",
            price=signal.yes_ask,
            size=trade_size,
        )
        
        no_leg = LegOrder(
            leg_id=f"{execution_id}-no",
            token_id=signal.no_token_id,
            side="BUY",
            price=signal.no_ask,
            size=trade_size,
        )
        
        result = ExecutionResult(
            execution_id=execution_id,
            condition_id=signal.condition_id,
            yes_leg=yes_leg,
            no_leg=no_leg,
            status=ExecutionStatus.PENDING,
            expected_profit=signal.expected_total_profit(trade_size),
        )
        
        async with self._lock:
            self._active_executions[execution_id] = result
        
        try:
            # Submit both orders concurrently
            result.status = ExecutionStatus.IN_PROGRESS
            
            yes_task = self._submit_leg(yes_leg, signal.condition_id)
            no_task = self._submit_leg(no_leg, signal.condition_id)
            
            await asyncio.gather(yes_task, no_task, return_exceptions=True)
            
            # Check results
            if yes_leg.status == LegStatus.FILLED and no_leg.status == LegStatus.FILLED:
                # Both filled - success
                result.status = ExecutionStatus.COMPLETE
                result.actual_filled_size = min(yes_leg.filled_size, no_leg.filled_size)
                result.entry_cost = (
                    yes_leg.price * yes_leg.filled_size +
                    no_leg.price * no_leg.filled_size
                )
                result.completed_at = time.time()
                
                if self.logger:
                    self.logger.info(
                        "parity_trade_complete",
                        execution_id=execution_id,
                        size=str(result.actual_filled_size),
                        cost=str(result.entry_cost),
                    )
            
            elif result.needs_unwind:
                # Partial fill - need to unwind
                result.status = ExecutionStatus.UNWINDING
                await self._unwind_partial(result)
            
            else:
                # Both failed
                result.status = ExecutionStatus.FAILED
                result.error = "Both legs failed to execute"
            
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.error = str(e)
            
            if self.logger:
                self.logger.error(
                    "parity_trade_failed",
                    execution_id=execution_id,
                    error=str(e),
                )
            
            # Attempt cleanup
            await self._cleanup_failed_execution(result)
        
        return result
    
    async def _submit_leg(self, leg: LegOrder, condition_id: str) -> None:
        """Submit a single leg order."""
        try:
            leg.status = LegStatus.SUBMITTED
            leg.submitted_at = time.time()
            
            # Get market info for tick size
            market_info = await self.client.get_market_info(condition_id)
            tick_size = market_info.get("minimum_tick_size", "0.01")
            neg_risk = market_info.get("neg_risk", False)
            
            order = await self.client.post_order(
                token_id=leg.token_id,
                side=leg.side,
                price=leg.price,
                size=leg.size,
                order_type="GTC",
                tick_size=tick_size,
                neg_risk=neg_risk,
                funder=self.funder,
                signature_type=self.signature_type,
            )
            
            leg.order_id = order.order_id
            
            # Wait for fill with timeout
            filled = await self._wait_for_fill(leg)
            
            if filled:
                leg.status = LegStatus.FILLED
                leg.filled_size = leg.size
                leg.filled_at = time.time()
            else:
                # Timeout - cancel and check partial
                await self._cancel_and_check_partial(leg)
                
        except Exception as e:
            leg.status = LegStatus.FAILED
            leg.error = str(e)
    
    async def _wait_for_fill(self, leg: LegOrder, poll_interval: float = 0.5) -> bool:
        """Wait for order to fill with timeout."""
        if not leg.order_id:
            return False
        
        deadline = time.time() + self.order_timeout
        
        while time.time() < deadline:
            try:
                orders = await self.client.get_open_orders()
                
                # Check if order is still open
                order_found = False
                for order in orders:
                    if order.order_id == leg.order_id:
                        order_found = True
                        break
                
                if not order_found:
                    # Order no longer open - check if filled
                    trades = await self.client.get_trades(limit=10)
                    for trade in trades:
                        if trade.order_id == leg.order_id:
                            leg.filled_size += trade.size
                    
                    if leg.filled_size >= leg.size:
                        return True
                
                await asyncio.sleep(poll_interval)
                
            except Exception:
                await asyncio.sleep(poll_interval)
        
        return False
    
    async def _cancel_and_check_partial(self, leg: LegOrder) -> None:
        """Cancel order and determine partial fill amount."""
        if leg.order_id:
            await self.client.cancel_order(leg.order_id)
        
        # Check for partial fills
        try:
            trades = await self.client.get_trades(limit=20)
            for trade in trades:
                if trade.order_id == leg.order_id:
                    leg.filled_size += trade.size
        except Exception:
            pass
        
        if leg.filled_size > 0:
            if leg.filled_size >= leg.size:
                leg.status = LegStatus.FILLED
            else:
                leg.status = LegStatus.PARTIAL
        else:
            leg.status = LegStatus.CANCELLED
    
    async def _unwind_partial(self, result: ExecutionResult) -> None:
        """
        Unwind a partial fill to eliminate naked exposure.
        
        If YES filled but NO didn't, sell YES.
        If NO filled but YES didn't, sell NO.
        """
        yes_filled = result.yes_leg.filled_size
        no_filled = result.no_leg.filled_size
        
        if self.logger:
            self.logger.warning(
                "unwinding_partial",
                execution_id=result.execution_id,
                yes_filled=str(yes_filled),
                no_filled=str(no_filled),
            )
        
        try:
            if yes_filled > no_filled:
                # Sell excess YES
                excess = yes_filled - no_filled
                await self._sell_position(
                    result.yes_leg.token_id,
                    excess,
                    result.condition_id,
                )
            elif no_filled > yes_filled:
                # Sell excess NO
                excess = no_filled - yes_filled
                await self._sell_position(
                    result.no_leg.token_id,
                    excess,
                    result.condition_id,
                )
            
            # Update result
            result.actual_filled_size = min(yes_filled, no_filled)
            if result.actual_filled_size > 0:
                result.status = ExecutionStatus.PARTIAL
            else:
                result.status = ExecutionStatus.FAILED
            
            result.completed_at = time.time()
            
        except Exception as e:
            result.error = f"Unwind failed: {e}"
            if self.logger:
                self.logger.error(
                    "unwind_failed",
                    execution_id=result.execution_id,
                    error=str(e),
                )
    
    async def _sell_position(
        self,
        token_id: str,
        size: Decimal,
        condition_id: str,
    ) -> None:
        """Sell a position to unwind exposure."""
        # Get current best bid
        price_info = await self.client.get_price(token_id)
        bid_price = price_info["bid"]
        
        if bid_price <= 0:
            raise ValueError(f"No bid available for {token_id}")
        
        market_info = await self.client.get_market_info(condition_id)
        tick_size = market_info.get("minimum_tick_size", "0.01")
        neg_risk = market_info.get("neg_risk", False)
        
        await self.client.post_order(
            token_id=token_id,
            side="SELL",
            price=bid_price,
            size=size,
            order_type="GTC",
            tick_size=tick_size,
            neg_risk=neg_risk,
            funder=self.funder,
            signature_type=self.signature_type,
        )
    
    async def _cleanup_failed_execution(self, result: ExecutionResult) -> None:
        """Cancel any open orders from a failed execution."""
        for leg in [result.yes_leg, result.no_leg]:
            if leg.order_id and leg.status in [LegStatus.SUBMITTED, LegStatus.PENDING]:
                try:
                    await self.client.cancel_order(leg.order_id)
                except Exception:
                    pass
    
    async def exit_position(
        self,
        condition_id: str,
        yes_token_id: str,
        no_token_id: str,
        size: Decimal,
    ) -> ExecutionResult:
        """
        Exit a paired position by selling both sides.
        
        Used when spread converges or for manual exit.
        """
        execution_id = str(uuid.uuid4())
        
        # Get current bids
        yes_price = await self.client.get_price(yes_token_id)
        no_price = await self.client.get_price(no_token_id)
        
        yes_leg = LegOrder(
            leg_id=f"{execution_id}-yes-exit",
            token_id=yes_token_id,
            side="SELL",
            price=yes_price["bid"],
            size=size,
        )
        
        no_leg = LegOrder(
            leg_id=f"{execution_id}-no-exit",
            token_id=no_token_id,
            side="SELL",
            price=no_price["bid"],
            size=size,
        )
        
        result = ExecutionResult(
            execution_id=execution_id,
            condition_id=condition_id,
            yes_leg=yes_leg,
            no_leg=no_leg,
            status=ExecutionStatus.IN_PROGRESS,
        )
        
        # Submit both sell orders
        yes_task = self._submit_leg(yes_leg, condition_id)
        no_task = self._submit_leg(no_leg, condition_id)
        
        await asyncio.gather(yes_task, no_task, return_exceptions=True)
        
        if yes_leg.status == LegStatus.FILLED and no_leg.status == LegStatus.FILLED:
            result.status = ExecutionStatus.COMPLETE
            result.actual_filled_size = min(yes_leg.filled_size, no_leg.filled_size)
        else:
            result.status = ExecutionStatus.PARTIAL
        
        result.completed_at = time.time()
        return result
    
    def get_active_executions(self) -> list[ExecutionResult]:
        """Get all active executions."""
        return list(self._active_executions.values())
    
    def get_execution(self, execution_id: str) -> Optional[ExecutionResult]:
        """Get a specific execution by ID."""
        return self._active_executions.get(execution_id)
