"""
Position manager for paired YES+NO positions.
Tracks entry, exit, and P&L for parity arbitrage trades.
"""

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..exec import ExecutionResult


class PositionStatus(Enum):
    """Status of a paired position."""
    OPEN = "open"
    EXITING = "exiting"
    CLOSED = "closed"
    RESOLVED = "resolved"  # Market resolved, position settled


@dataclass
class PairedPosition:
    """
    A paired YES+NO position from parity arbitrage.
    
    Entry: Buy YES at yes_entry_price, Buy NO at no_entry_price
    Exit: Either sell both sides or wait for market resolution
    """
    position_id: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    
    # Entry details
    size: Decimal
    yes_entry_price: Decimal
    no_entry_price: Decimal
    entry_cost: Decimal  # Total USDC spent
    entry_time: float
    
    # Exit details
    yes_exit_price: Optional[Decimal] = None
    no_exit_price: Optional[Decimal] = None
    exit_proceeds: Optional[Decimal] = None
    exit_time: Optional[float] = None
    
    # Status
    status: PositionStatus = PositionStatus.OPEN
    
    # P&L tracking
    realized_pnl: Decimal = Decimal("0")
    
    # Metadata
    execution_id: Optional[str] = None
    notes: str = ""
    
    @property
    def combined_entry_price(self) -> Decimal:
        """Total price paid per share (YES + NO)."""
        return self.yes_entry_price + self.no_entry_price
    
    @property
    def expected_pnl_at_resolution(self) -> Decimal:
        """
        Expected P&L if held to resolution.
        At resolution, one side pays $1, other pays $0.
        Total payout = $1 per share.
        """
        return (Decimal("1") - self.combined_entry_price) * self.size
    
    @property
    def holding_time_seconds(self) -> float:
        """Time position has been held."""
        end_time = self.exit_time or time.time()
        return end_time - self.entry_time
    
    def calculate_exit_pnl(
        self,
        yes_exit_price: Decimal,
        no_exit_price: Decimal,
    ) -> Decimal:
        """Calculate P&L for exiting at given prices."""
        exit_proceeds = (yes_exit_price + no_exit_price) * self.size
        return exit_proceeds - self.entry_cost
    
    def close(
        self,
        yes_exit_price: Decimal,
        no_exit_price: Decimal,
        exit_proceeds: Decimal,
    ) -> None:
        """Mark position as closed with exit details."""
        self.yes_exit_price = yes_exit_price
        self.no_exit_price = no_exit_price
        self.exit_proceeds = exit_proceeds
        self.exit_time = time.time()
        self.realized_pnl = exit_proceeds - self.entry_cost
        self.status = PositionStatus.CLOSED
    
    def resolve(self, payout: Decimal) -> None:
        """Mark position as resolved (market settled)."""
        self.exit_proceeds = payout
        self.exit_time = time.time()
        self.realized_pnl = payout - self.entry_cost
        self.status = PositionStatus.RESOLVED
    
    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "position_id": self.position_id,
            "condition_id": self.condition_id,
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "size": str(self.size),
            "yes_entry_price": str(self.yes_entry_price),
            "no_entry_price": str(self.no_entry_price),
            "entry_cost": str(self.entry_cost),
            "entry_time": self.entry_time,
            "yes_exit_price": str(self.yes_exit_price) if self.yes_exit_price else None,
            "no_exit_price": str(self.no_exit_price) if self.no_exit_price else None,
            "exit_proceeds": str(self.exit_proceeds) if self.exit_proceeds else None,
            "exit_time": self.exit_time,
            "status": self.status.value,
            "realized_pnl": str(self.realized_pnl),
            "execution_id": self.execution_id,
            "notes": self.notes,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "PairedPosition":
        """Create from dictionary."""
        return cls(
            position_id=data["position_id"],
            condition_id=data["condition_id"],
            yes_token_id=data["yes_token_id"],
            no_token_id=data["no_token_id"],
            size=Decimal(data["size"]),
            yes_entry_price=Decimal(data["yes_entry_price"]),
            no_entry_price=Decimal(data["no_entry_price"]),
            entry_cost=Decimal(data["entry_cost"]),
            entry_time=data["entry_time"],
            yes_exit_price=Decimal(data["yes_exit_price"]) if data.get("yes_exit_price") else None,
            no_exit_price=Decimal(data["no_exit_price"]) if data.get("no_exit_price") else None,
            exit_proceeds=Decimal(data["exit_proceeds"]) if data.get("exit_proceeds") else None,
            exit_time=data.get("exit_time"),
            status=PositionStatus(data["status"]),
            realized_pnl=Decimal(data.get("realized_pnl", "0")),
            execution_id=data.get("execution_id"),
            notes=data.get("notes", ""),
        )
    
    @classmethod
    def from_execution(cls, result: "ExecutionResult") -> "PairedPosition":
        """Create position from execution result."""
        return cls(
            position_id=result.execution_id,
            condition_id=result.condition_id,
            yes_token_id=result.yes_leg.token_id,
            no_token_id=result.no_leg.token_id,
            size=result.actual_filled_size,
            yes_entry_price=result.yes_leg.price,
            no_entry_price=result.no_leg.price,
            entry_cost=result.entry_cost,
            entry_time=result.created_at,
            execution_id=result.execution_id,
        )


class PositionManager:
    """
    Manages all paired positions.
    
    Responsibilities:
    - Track open positions
    - Calculate aggregate exposure
    - Manage position limits
    - Track P&L
    """
    
    def __init__(self, max_open_pairs: int = 5):
        self.max_open_pairs = max_open_pairs
        self._positions: dict[str, PairedPosition] = {}
        self._positions_by_market: dict[str, list[str]] = {}  # condition_id -> position_ids
    
    def add_position(self, position: PairedPosition) -> None:
        """Add a new position."""
        self._positions[position.position_id] = position
        
        if position.condition_id not in self._positions_by_market:
            self._positions_by_market[position.condition_id] = []
        self._positions_by_market[position.condition_id].append(position.position_id)
    
    def get_position(self, position_id: str) -> Optional[PairedPosition]:
        """Get position by ID."""
        return self._positions.get(position_id)
    
    def get_positions_for_market(self, condition_id: str) -> list[PairedPosition]:
        """Get all positions for a market."""
        position_ids = self._positions_by_market.get(condition_id, [])
        return [self._positions[pid] for pid in position_ids if pid in self._positions]
    
    def get_open_positions(self) -> list[PairedPosition]:
        """Get all open positions."""
        return [p for p in self._positions.values() if p.status == PositionStatus.OPEN]
    
    def get_all_positions(self) -> list[PairedPosition]:
        """Get all positions."""
        return list(self._positions.values())
    
    @property
    def open_position_count(self) -> int:
        """Number of open positions."""
        return len(self.get_open_positions())
    
    @property
    def can_open_new_position(self) -> bool:
        """Check if we can open a new position."""
        return self.open_position_count < self.max_open_pairs
    
    @property
    def total_exposure(self) -> Decimal:
        """Total USDC exposure across all open positions."""
        return sum(p.entry_cost for p in self.get_open_positions())
    
    @property
    def total_expected_pnl(self) -> Decimal:
        """Total expected P&L if all positions held to resolution."""
        return sum(p.expected_pnl_at_resolution for p in self.get_open_positions())
    
    @property
    def total_realized_pnl(self) -> Decimal:
        """Total realized P&L from closed positions."""
        return sum(p.realized_pnl for p in self._positions.values() if p.status != PositionStatus.OPEN)
    
    def close_position(
        self,
        position_id: str,
        yes_exit_price: Decimal,
        no_exit_price: Decimal,
        exit_proceeds: Decimal,
    ) -> Optional[PairedPosition]:
        """Close a position with exit details."""
        position = self._positions.get(position_id)
        if position:
            position.close(yes_exit_price, no_exit_price, exit_proceeds)
        return position
    
    def resolve_position(self, position_id: str, payout: Decimal) -> Optional[PairedPosition]:
        """Mark position as resolved (market settled)."""
        position = self._positions.get(position_id)
        if position:
            position.resolve(payout)
        return position
    
    def remove_position(self, position_id: str) -> Optional[PairedPosition]:
        """Remove a position from tracking."""
        position = self._positions.pop(position_id, None)
        if position:
            market_positions = self._positions_by_market.get(position.condition_id, [])
            if position_id in market_positions:
                market_positions.remove(position_id)
        return position
    
    def get_market_exposure(self, condition_id: str) -> Decimal:
        """Get total exposure for a specific market."""
        positions = self.get_positions_for_market(condition_id)
        return sum(p.entry_cost for p in positions if p.status == PositionStatus.OPEN)
    
    def get_summary(self) -> dict:
        """Get summary statistics."""
        open_positions = self.get_open_positions()
        closed_positions = [p for p in self._positions.values() if p.status != PositionStatus.OPEN]
        
        return {
            "open_count": len(open_positions),
            "closed_count": len(closed_positions),
            "total_exposure": str(self.total_exposure),
            "total_expected_pnl": str(self.total_expected_pnl),
            "total_realized_pnl": str(self.total_realized_pnl),
            "markets_with_positions": list(self._positions_by_market.keys()),
        }
    
    def load_positions(self, positions_data: list[dict]) -> None:
        """Load positions from storage."""
        for data in positions_data:
            position = PairedPosition.from_dict(data)
            self.add_position(position)
    
    def export_positions(self) -> list[dict]:
        """Export all positions for storage."""
        return [p.to_dict() for p in self._positions.values()]
