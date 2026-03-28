# Polymarket Latency Arbitrage Bot

A Python bot that monitors BTC/ETH up/down prediction markets on Polymarket and executes trades when Polymarket odds lag Binance CEX prices by a configurable threshold.

---

## How It Works

1. **Price feed** — connects to Binance via WebSocket and streams real-time BTC/ETH ticker prices.
2. **Market monitor** — polls Polymarket's CLOB API for live order books on BTC/ETH 5-min and 15-min up/down contracts.
3. **Signal engine** — computes a fair-value probability from recent CEX price momentum and compares it against Polymarket's mid-price. When the lag exceeds the threshold and confidence is high enough, a trade signal is generated.
4. **Sizer** — applies half-Kelly criterion to size the position, capped at 8% of portfolio.
5. **Executor** — places the order on Polymarket (or simulates it in paper mode).
6. **Kill switch** — halts all trading automatically if the daily drawdown exceeds 20%.
7. **Dashboard** — renders a live terminal UI showing prices, P&L, open positions, and recent trades.

---

## Requirements

- Python 3.11+
- A funded Polymarket account on Polygon with API credentials
- A Telegram bot (optional, for alerts)

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Configuration

Copy the example env file and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `POLYMARKET_PRIVATE_KEY` | Your Polygon wallet private key (`0x...`) |
| `POLYMARKET_API_KEY` | Polymarket CLOB API key |
| `POLYMARKET_API_SECRET` | Polymarket CLOB API secret |
| `POLYMARKET_API_PASSPHRASE` | Polymarket CLOB API passphrase |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | Your Telegram chat ID from [@userinfobot](https://t.me/userinfobot) |

To get Polymarket API credentials, go to your [Polymarket profile](https://polymarket.com/profile) → API section.

---

## Usage

### Paper trading (default — no real funds at risk)

```bash
python polymarket_arb_bot.py
```

### Live trading

All three flags are required. This is intentional — it forces a conscious decision before real funds are used.

```bash
python polymarket_arb_bot.py --live --confirm-live --i-understand-risks
```

### Options

```
--live                  Enable live trading (flag 1 of 3)
--confirm-live          Confirm live trading intent (flag 2 of 3)
--i-understand-risks    Acknowledge financial risks (flag 3 of 3)
--portfolio FLOAT       Portfolio value in USDC (default: 1000.0)
```

---

## Market IDs

Before running, replace the placeholder market IDs in `PolymarketMonitor.MARKET_IDS` with real Polymarket condition IDs. Polymarket's short-term up/down contracts rotate frequently, so check for active markets at:

```
https://clob.polymarket.com/markets
```

Look for markets matching descriptions like *"Will BTC be higher in 5 minutes?"* and copy their `condition_id` into the dict:

```python
MARKET_IDS: dict[str, dict] = {
    "0xabc123...": {"asset": "BTC", "direction": "UP",   "duration": "5min"},
    "0xdef456...": {"asset": "BTC", "direction": "DOWN", "duration": "5min"},
    # ...
}
```

---

## Trading Parameters

All thresholds are defined in the `Config` dataclass at the top of `polymarket_arb_bot.py`:

| Parameter | Default | Description |
|---|---|---|
| `min_edge_pct` | `5.0` | Minimum edge (%) required to place a trade |
| `lag_threshold_pct` | `3.0` | Minimum Polymarket lag vs CEX to flag an opportunity |
| `max_position_pct` | `8.0` | Maximum position size as % of portfolio |
| `confidence_threshold` | `85.0` | Minimum confidence score (0–100) to trade |
| `kelly_fraction` | `0.5` | Kelly multiplier (0.5 = half-Kelly) |
| `kill_switch_drawdown` | `20.0` | Daily drawdown % that triggers the kill switch |
| `order_cooldown_sec` | `1.0` | Minimum seconds between order submissions |
| `ws_reconnect_delay` | `5.0` | Seconds before reconnecting a dropped WebSocket |
| `max_retries` | `5` | Retry attempts for failed API calls (exponential backoff) |

---

## Database

All trades and signals are logged to `trades.db` (SQLite). Three tables are created automatically:

- **`trades`** — every executed (or paper) trade with entry price, size, P&L, and status
- **`signals`** — every generated signal, whether acted on or not
- **`price_log`** — Binance price ticks (for replay/analysis)

Query recent trades directly:

```bash
sqlite3 trades.db "SELECT * FROM trades ORDER BY opened_at DESC LIMIT 20;"
```

---

## Kill Switch

If the bot's cumulative daily P&L falls below **−20%** of portfolio value, the kill switch activates:

- All new trade execution is halted for the remainder of the day
- A Telegram alert is sent immediately
- The dashboard displays a prominent warning

The bot must be restarted to reset the kill switch after the next calendar day.

---

## Terminal Dashboard

The live dashboard refreshes twice per second and displays:

- Current BTC and ETH prices from Binance
- Daily P&L and overall win rate
- All open positions with entry price, size, and edge
- Last 10 trades with P&L per trade
- Trading mode (PAPER / LIVE) and kill switch status

---

## Project Structure

```
.
├── polymarket_arb_bot.py   # Main bot (single file)
├── requirements.txt
├── .env.example            # Credential template
├── .env                    # Your credentials (never commit this)
├── trades.db               # SQLite database (auto-created)
└── arb_bot.log             # Rolling log file (auto-created)
```

---

## Disclaimer

This software is provided for educational and research purposes only. Trading prediction markets involves significant financial risk. Paper trade and validate your setup thoroughly before committing real funds. The authors accept no responsibility for financial losses.
