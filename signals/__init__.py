"""Signals module for arbitrage detection."""

from .parity_detector import ParityDetector, ParitySignal, ConvergenceDetector
from .spot_lag import SpotLagDetector, SpotLagSignal, BinanceSpotFeed, SpotData

__all__ = [
    "ParityDetector", 
    "ParitySignal", 
    "ConvergenceDetector",
    "SpotLagDetector",
    "SpotLagSignal", 
    "BinanceSpotFeed",
    "SpotData",
]
