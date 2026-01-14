#!/bin/bash
# Run the Polymarket Parity Arbitrage Bot

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check for .env file
if [ ! -f ".env" ]; then
    echo "Error: .env file not found. Copy .env.example to .env and configure."
    exit 1
fi

# Load environment variables
set -a
source .env
set +a

# Check for virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Run the bot
exec python -m polymarket_arb_bot "$@"
