"""
Entry point for running the arbitrage bot.
Usage: python -m polymarket_arb_bot
"""

import asyncio
import sys

from .bot import run_bot
from .config import load_config_from_env


def main() -> int:
    """Main entry point."""
    try:
        config = load_config_from_env()
        
        # Validate config
        errors = config.validate()
        if errors:
            print(f"Configuration errors:", file=sys.stderr)
            for error in errors:
                print(f"  - {error}", file=sys.stderr)
            return 1
        
        # Run bot
        asyncio.run(run_bot(config))
        return 0
        
    except KeyboardInterrupt:
        print("\nShutdown requested")
        return 0
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
