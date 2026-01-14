# Polymarket Parity Arbitrage Bot

YES+NO parity arbitrage bot for Polymarket binary markets.

## Strategy

In binary prediction markets, YES + NO must equal $1 at resolution. When `YES_ask + NO_ask < 1.00 - fees - slippage`, buying both sides locks in guaranteed profit.

**Execution flow:**
1. Monitor orderbooks via WebSocket
2. Detect parity edge: `edge = 1 - (yes_ask + no_ask) - fees - slippage`
3. Execute when `edge >= min_edge`
4. Buy YES and NO atomically (paired orders)
5. Hold until spread converges or market resolves
6. Exit to realize profit

## Requirements

- Python 3.11+
- Polygon wallet with USDC
- Polymarket account (for proxy wallet)

## Installation

```bash
# Clone and enter directory
cd polymarket_arb_bot

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials
```

## Configuration

Edit `.env` with your settings:

```bash
# Required
POLYMARKET_PRIVATE_KEY=0x...        # Wallet private key
POLYMARKET_FUNDER_ADDRESS=0x...     # Proxy wallet from polymarket.com/settings
POLYMARKET_SIGNATURE_TYPE=2         # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE

# Markets (condition_id:yes_token:no_token:tick_size:neg_risk)
POLYMARKET_MARKETS=0xabc:111:222:0.01:false

# Trading parameters
MIN_EDGE=0.005                      # 0.5% minimum edge
SLIPPAGE_BUFFER=0.002               # 0.2% slippage buffer
MAX_NOTIONAL_PER_TRADE=100          # Max $100 per trade
MAX_OPEN_PAIRS=5                    # Max 5 concurrent positions

# Risk limits
MAX_DAILY_LOSS=500                  # Daily loss limit
KILL_SWITCH_LOSS_THRESHOLD=200      # Immediate halt threshold
```

### Finding Market IDs

1. Go to polymarket.com and find your target market
2. Use the Gamma API to get token IDs:
   ```bash
   curl "https://gamma-api.polymarket.com/markets?slug=your-market-slug"
   ```
3. Extract `conditionId`, `clobTokenIds[0]` (YES), `clobTokenIds[1]` (NO)

## Running

### Development

```bash
# Load environment and run
source venv/bin/activate
python -m polymarket_arb_bot
```

### Production (systemd)

```bash
# Create user and directories
sudo useradd -r -s /bin/false polymarket
sudo mkdir -p /opt/polymarket-arb-bot /var/log/polymarket-arb-bot
sudo chown polymarket:polymarket /opt/polymarket-arb-bot /var/log/polymarket-arb-bot

# Copy files
sudo cp -r . /opt/polymarket-arb-bot/
sudo cp polymarket-arb-bot.service /etc/systemd/system/

# Setup virtual environment
sudo -u polymarket python -m venv /opt/polymarket-arb-bot/venv
sudo -u polymarket /opt/polymarket-arb-bot/venv/bin/pip install -r /opt/polymarket-arb-bot/requirements.txt

# Configure
sudo cp .env.example /opt/polymarket-arb-bot/.env
sudo nano /opt/polymarket-arb-bot/.env  # Edit with your values
sudo chmod 600 /opt/polymarket-arb-bot/.env
sudo chown polymarket:polymarket /opt/polymarket-arb-bot/.env

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable polymarket-arb-bot
sudo systemctl start polymarket-arb-bot

# Check status
sudo systemctl status polymarket-arb-bot
sudo journalctl -u polymarket-arb-bot -f
```

## Architecture

```
polymarket_arb_bot/
├── connector/          # REST/WebSocket API adapters
│   ├── auth.py         # L1 (EIP-712) and L2 (HMAC) authentication
│   ├── rest_client.py  # CLOB REST API client
│   └── ws_client.py    # WebSocket client for real-time data
├── orderbook/          # In-memory orderbook management
│   └── book.py         # YES/NO book synchronization
├── signals/            # Parity detection
│   └── parity_detector.py  # Edge calculation and signal generation
├── exec/               # Order execution
│   └── executor.py     # Paired order placement and unwind logic
├── positions/          # Position tracking
│   └── manager.py      # Paired position state management
├── risk/               # Risk controls
│   └── manager.py      # Limits, cooldowns, kill-switch
├── monitor/            # Observability
│   ├── logger.py       # Structured JSON logging
│   └── metrics.py      # Performance metrics
├── storage/            # Persistence
│   └── database.py     # SQLite for state, fills, P&L
├── config.py           # Configuration management
├── bot.py              # Main orchestration
└── __main__.py         # Entry point
```

## Execution Rules

1. **Edge calculation**: `edge = 1 - (yes_ask + no_ask) - fees - slippage_buffer`
2. **Execute only if**: `edge >= min_edge`
3. **Paired orders**: YES and NO sized equally, treated as one trade
4. **Partial fill handling**: Immediate hedge or unwind
5. **No naked exposure**: Always balanced or flat
6. **Rate limiting**: Respects Polymarket API limits with backoff

## Monitoring

Logs are JSON-formatted for parsing:

```json
{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","event":"trade_executed","execution_id":"abc123","size":"50","entry_cost":"48.50","expected_profit":"1.50"}
```

View logs:
```bash
# Systemd
sudo journalctl -u polymarket-arb-bot -f

# Log file
tail -f /opt/polymarket-arb-bot/arb_bot.log | jq .
```

## Risk Controls

- **Daily loss limit**: Halts trading when exceeded
- **Kill switch**: Immediate halt on threshold breach
- **Position limits**: Max concurrent paired positions
- **Trade cooldown**: Minimum time between trades
- **Consecutive failure limit**: Halts after N failures
- **Health checks**: Periodic connection and data freshness checks

## Troubleshooting

**"Configuration errors: POLYMARKET_PRIVATE_KEY is required"**
- Ensure `.env` file exists and contains your private key

**"API credentials required for L2 authentication"**
- Bot will auto-derive credentials on first run
- Or manually set `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, `POLYMARKET_API_PASSPHRASE`

**"Kill switch active"**
- Check logs for reason
- Reset requires manual intervention (restart bot)

**WebSocket disconnections**
- Normal during network issues
- Bot auto-reconnects with backoff

## Disclaimer

This software is for educational purposes. Trading involves risk. No guarantee of profit. Use at your own risk.
