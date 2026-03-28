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
| `max_slippage_pct` | `1.5` | Maximum VWAP slippage (%) allowed before rejecting a trade |
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

## Slippage Guard

Before any trade is submitted, the bot walks the live Polymarket order book to estimate the actual fill price for the intended position size. Trades are rejected if the estimated slippage exceeds `max_slippage_pct` (default: 1.5%).

**How it works:**

- For a **YES buy**, the bot consumes ask levels from lowest price upward.
- For a **NO buy**, it mirrors the bid side (since NO = 1 − YES).
- It computes the **volume-weighted average price (VWAP)** across all levels needed to fill the order.
- Slippage is then `(VWAP − best_price) / best_price × 100`.
- If the full order size can't be filled by available liquidity, the trade is also rejected.

**What gets logged on rejection:**

```
SLIPPAGE REJECTED BTC UP YES – slippage=3.21% (max 1.50%), fillable=$42.00 of $80.00
```

This protects against thin markets where posting even a modest order would move the price significantly against you, eroding the edge that triggered the signal in the first place.

To relax or tighten the guard, adjust `max_slippage_pct` in the `Config` dataclass.

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
├── arb_bot.log             # Rolling log file (auto-created)
└── arb-bot.service         # systemd service file (optional)
```

---

## Keeping the Bot Alive (Process Management)

The bot handles internal failures well — dropped WebSocket connections reconnect automatically, and API errors retry with exponential backoff. But none of that helps if **the process itself dies** because your SSH session closed, your laptop went to sleep, or the server rebooted. Use one of the options below to protect against that.

---

### Option 1 — tmux (simplest, recommended for development)

`tmux` keeps your terminal session alive after you disconnect.

```bash
# Start a named session
tmux new -s arb-bot

# Run the bot inside it
python polymarket_arb_bot.py --live --confirm-live --i-understand-risks

# Detach (bot keeps running): Ctrl+B then D

# Reattach later from any SSH session
tmux attach -t arb-bot

# List all sessions
tmux ls
```

If your server doesn't have tmux: `sudo apt install tmux` (Ubuntu/Debian) or `brew install tmux` (macOS).

---

### Option 2 — nohup (quick one-liner, no reattach)

Redirects output to a file and keeps the process running after logout. You won't be able to see the live dashboard, but logs and the database still work.

```bash
nohup python polymarket_arb_bot.py --live --confirm-live --i-understand-risks \
  >> nohup_arb.log 2>&1 &

# Note the printed PID, then check it anytime:
tail -f nohup_arb.log

# Stop the bot:
kill <PID>
```

---

### Option 3 — systemd (recommended for production servers)

systemd auto-restarts the bot on crash and brings it back up after a server reboot. Create the service file at `/etc/systemd/system/arb-bot.service`:

```ini
[Unit]
Description=Polymarket Latency Arbitrage Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=YOUR_LINUX_USER
WorkingDirectory=/path/to/your/bot
EnvironmentFile=/path/to/your/bot/.env
ExecStart=/usr/bin/python3 polymarket_arb_bot.py --live --confirm-live --i-understand-risks
Restart=on-failure
RestartSec=10
StandardOutput=append:/path/to/your/bot/arb_bot.log
StandardError=append:/path/to/your/bot/arb_bot.log

[Install]
WantedBy=multi-user.target
```

Then enable and start it:

```bash
# Replace placeholders first, then:
sudo systemctl daemon-reload
sudo systemctl enable arb-bot
sudo systemctl start arb-bot

# Check status and live logs
sudo systemctl status arb-bot
sudo journalctl -u arb-bot -f
```

> **Note:** The Rich terminal dashboard won't render under systemd since there's no TTY. The bot still runs fully — trades execute, Telegram alerts fire, and everything logs to `trades.db` and `arb_bot.log` as normal. Remove the `--live` flags if running in paper mode.

---

### Which option to choose?

| | tmux | nohup | systemd |
|---|---|---|---|
| Survives SSH disconnect | ✅ | ✅ | ✅ |
| Survives server reboot | ❌ | ❌ | ✅ |
| Auto-restarts on crash | ❌ | ❌ | ✅ |
| Live dashboard works | ✅ | ❌ | ❌ |
| Setup complexity | Low | Minimal | Medium |

For a VPS running 24/7, use **systemd**. For local development or occasional runs, use **tmux**.

---

## Disclaimer

This software is provided for educational and research purposes only. Trading prediction markets involves significant financial risk. Paper trade and validate your setup thoroughly before committing real funds. The authors accept no responsibility for financial losses.
