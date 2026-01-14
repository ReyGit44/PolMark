"""
SQLite database for persistent storage.
Stores positions, trades, P&L history, and bot state for restarts.
"""

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Generator, Optional


class PositionStatus(Enum):
    """Position status enum."""
    OPEN = "open"
    EXITING = "exiting"
    CLOSED = "closed"
    RESOLVED = "resolved"


@dataclass
class StoredPosition:
    """Position data for storage (avoids circular imports)."""
    position_id: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    size: Decimal
    yes_entry_price: Decimal
    no_entry_price: Decimal
    entry_cost: Decimal
    entry_time: float
    yes_exit_price: Optional[Decimal] = None
    no_exit_price: Optional[Decimal] = None
    exit_proceeds: Optional[Decimal] = None
    exit_time: Optional[float] = None
    status: PositionStatus = PositionStatus.OPEN
    realized_pnl: Decimal = Decimal("0")
    execution_id: Optional[str] = None
    notes: str = ""


class Database:
    """
    SQLite database for arbitrage bot persistence.
    
    Tables:
    - positions: Paired position records
    - trades: Individual trade/fill records
    - daily_pnl: Daily P&L summaries
    - bot_state: Bot state for restarts
    """
    
    def __init__(self, db_path: str = "arb_bot.db"):
        self.db_path = Path(db_path)
        self._init_db()
    
    @contextmanager
    def _get_conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Get database connection with context manager."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            
            # Positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    yes_token_id TEXT NOT NULL,
                    no_token_id TEXT NOT NULL,
                    size TEXT NOT NULL,
                    yes_entry_price TEXT NOT NULL,
                    no_entry_price TEXT NOT NULL,
                    entry_cost TEXT NOT NULL,
                    entry_time REAL NOT NULL,
                    yes_exit_price TEXT,
                    no_exit_price TEXT,
                    exit_proceeds TEXT,
                    exit_time REAL,
                    status TEXT NOT NULL,
                    realized_pnl TEXT DEFAULT '0',
                    execution_id TEXT,
                    notes TEXT DEFAULT '',
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)
            
            # Trades table (individual fills)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    position_id TEXT,
                    execution_id TEXT,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price TEXT NOT NULL,
                    size TEXT NOT NULL,
                    fee TEXT DEFAULT '0',
                    timestamp REAL NOT NULL,
                    order_id TEXT,
                    FOREIGN KEY (position_id) REFERENCES positions(position_id)
                )
            """)
            
            # Daily P&L table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_pnl (
                    date TEXT PRIMARY KEY,
                    trades_count INTEGER DEFAULT 0,
                    total_volume TEXT DEFAULT '0',
                    realized_pnl TEXT DEFAULT '0',
                    expected_pnl TEXT DEFAULT '0',
                    max_drawdown TEXT DEFAULT '0',
                    peak_pnl TEXT DEFAULT '0',
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)
            
            # Bot state table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)
            
            # Indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_positions_status 
                ON positions(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_positions_condition 
                ON positions(condition_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_position 
                ON trades(position_id)
            """)
    
    # === Position Operations ===
    
    def save_position(self, position: Any) -> None:
        """Save or update a position."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO positions (
                    position_id, condition_id, yes_token_id, no_token_id,
                    size, yes_entry_price, no_entry_price, entry_cost,
                    entry_time, yes_exit_price, no_exit_price, exit_proceeds,
                    exit_time, status, realized_pnl, execution_id, notes,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                position.position_id,
                position.condition_id,
                position.yes_token_id,
                position.no_token_id,
                str(position.size),
                str(position.yes_entry_price),
                str(position.no_entry_price),
                str(position.entry_cost),
                position.entry_time,
                str(position.yes_exit_price) if position.yes_exit_price else None,
                str(position.no_exit_price) if position.no_exit_price else None,
                str(position.exit_proceeds) if position.exit_proceeds else None,
                position.exit_time,
                position.status.value,
                str(position.realized_pnl),
                position.execution_id,
                position.notes,
                time.time(),
            ))
    
    def get_position(self, position_id: str) -> Optional[StoredPosition]:
        """Get a position by ID."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM positions WHERE position_id = ?",
                (position_id,)
            )
            row = cursor.fetchone()
            
            if row:
                return self._row_to_position(row)
            return None
    
    def get_open_positions(self) -> list[StoredPosition]:
        """Get all open positions."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM positions WHERE status = ?",
                (PositionStatus.OPEN.value,)
            )
            return [self._row_to_position(row) for row in cursor.fetchall()]
    
    def get_positions_by_market(self, condition_id: str) -> list[StoredPosition]:
        """Get all positions for a market."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM positions WHERE condition_id = ?",
                (condition_id,)
            )
            return [self._row_to_position(row) for row in cursor.fetchall()]
    
    def get_all_positions(self, limit: int = 100) -> list[StoredPosition]:
        """Get all positions with limit."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM positions ORDER BY entry_time DESC LIMIT ?",
                (limit,)
            )
            return [self._row_to_position(row) for row in cursor.fetchall()]
    
    def _row_to_position(self, row: sqlite3.Row) -> StoredPosition:
        """Convert database row to StoredPosition."""
        return StoredPosition(
            position_id=row["position_id"],
            condition_id=row["condition_id"],
            yes_token_id=row["yes_token_id"],
            no_token_id=row["no_token_id"],
            size=Decimal(row["size"]),
            yes_entry_price=Decimal(row["yes_entry_price"]),
            no_entry_price=Decimal(row["no_entry_price"]),
            entry_cost=Decimal(row["entry_cost"]),
            entry_time=row["entry_time"],
            yes_exit_price=Decimal(row["yes_exit_price"]) if row["yes_exit_price"] else None,
            no_exit_price=Decimal(row["no_exit_price"]) if row["no_exit_price"] else None,
            exit_proceeds=Decimal(row["exit_proceeds"]) if row["exit_proceeds"] else None,
            exit_time=row["exit_time"],
            status=PositionStatus(row["status"]),
            realized_pnl=Decimal(row["realized_pnl"]),
            execution_id=row["execution_id"],
            notes=row["notes"] or "",
        )
    
    # === Trade Operations ===
    
    def save_trade(
        self,
        trade_id: str,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        timestamp: float,
        position_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        order_id: Optional[str] = None,
        fee: Decimal = Decimal("0"),
    ) -> None:
        """Save a trade record."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO trades (
                    trade_id, position_id, execution_id, token_id,
                    side, price, size, fee, timestamp, order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_id,
                position_id,
                execution_id,
                token_id,
                side,
                str(price),
                str(size),
                str(fee),
                timestamp,
                order_id,
            ))
    
    def get_trades_for_position(self, position_id: str) -> list[dict]:
        """Get all trades for a position."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM trades WHERE position_id = ? ORDER BY timestamp",
                (position_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_recent_trades(self, limit: int = 50) -> list[dict]:
        """Get recent trades."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    # === Daily P&L Operations ===
    
    def update_daily_pnl(
        self,
        date: str,
        trades_count: int = 0,
        volume: Decimal = Decimal("0"),
        realized_pnl: Decimal = Decimal("0"),
        expected_pnl: Decimal = Decimal("0"),
        max_drawdown: Decimal = Decimal("0"),
        peak_pnl: Decimal = Decimal("0"),
    ) -> None:
        """Update daily P&L record."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO daily_pnl (
                    date, trades_count, total_volume, realized_pnl,
                    expected_pnl, max_drawdown, peak_pnl, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    trades_count = trades_count + excluded.trades_count,
                    total_volume = CAST(
                        CAST(total_volume AS REAL) + CAST(excluded.total_volume AS REAL) 
                        AS TEXT
                    ),
                    realized_pnl = excluded.realized_pnl,
                    expected_pnl = excluded.expected_pnl,
                    max_drawdown = excluded.max_drawdown,
                    peak_pnl = excluded.peak_pnl,
                    updated_at = excluded.updated_at
            """, (
                date,
                trades_count,
                str(volume),
                str(realized_pnl),
                str(expected_pnl),
                str(max_drawdown),
                str(peak_pnl),
                time.time(),
            ))
    
    def get_daily_pnl(self, date: str) -> Optional[dict]:
        """Get daily P&L for a specific date."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM daily_pnl WHERE date = ?",
                (date,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_pnl_history(self, days: int = 30) -> list[dict]:
        """Get P&L history for last N days."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM daily_pnl ORDER BY date DESC LIMIT ?",
                (days,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    # === Bot State Operations ===
    
    def save_state(self, key: str, value: Any) -> None:
        """Save bot state value."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO bot_state (key, value, updated_at)
                VALUES (?, ?, ?)
            """, (key, json.dumps(value), time.time()))
    
    def get_state(self, key: str, default: Any = None) -> Any:
        """Get bot state value."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT value FROM bot_state WHERE key = ?",
                (key,)
            )
            row = cursor.fetchone()
            if row:
                return json.loads(row["value"])
            return default
    
    def delete_state(self, key: str) -> None:
        """Delete bot state value."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM bot_state WHERE key = ?", (key,))
    
    # === Utility Operations ===
    
    def get_total_realized_pnl(self) -> Decimal:
        """Get total realized P&L across all positions."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COALESCE(SUM(CAST(realized_pnl AS REAL)), 0) as total FROM positions"
            )
            row = cursor.fetchone()
            return Decimal(str(row["total"])) if row else Decimal("0")
    
    def get_statistics(self) -> dict:
        """Get overall statistics."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            
            # Position stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_positions,
                    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_positions,
                    SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) as closed_positions,
                    SUM(CAST(realized_pnl AS REAL)) as total_realized_pnl,
                    SUM(CAST(entry_cost AS REAL)) as total_volume
                FROM positions
            """)
            pos_stats = dict(cursor.fetchone())
            
            # Trade stats
            cursor.execute("SELECT COUNT(*) as total_trades FROM trades")
            trade_stats = dict(cursor.fetchone())
            
            return {
                **pos_stats,
                **trade_stats,
            }
    
    def vacuum(self) -> None:
        """Optimize database."""
        with self._get_conn() as conn:
            conn.execute("VACUUM")
