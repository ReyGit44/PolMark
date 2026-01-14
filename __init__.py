"""
Polymarket YES+NO Parity Arbitrage Bot

Executes when YES_best_ask + NO_best_ask < 1.00 - fees - slippage_buffer,
buys both sides atomically, holds until convergence, and exits to lock profit.
"""

__version__ = "1.0.0"
