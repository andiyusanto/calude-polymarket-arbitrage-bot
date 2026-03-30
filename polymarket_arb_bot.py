"""
Polymarket Latency Arbitrage Bot
=================================
Monitors BTC/ETH up/down contracts on Polymarket and executes trades
when Polymarket odds lag CEX (Binance) prices.

Requirements:
    pip install py-clob-client websockets aiohttp rich sqlite-utils python-telegram-bot python-dotenv

Usage:
    # Paper trading (default, safe):
    python polymarket_arb_bot.py

    # Live trading (requires all three flags):
    python polymarket_arb_bot.py --live --confirm-live --i-understand-risks
"""

import asyncio
import json
import logging
import sqlite3
import time
import argparse
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from typing import Optional
import os

import aiohttp
import websockets
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

# ── Polymarket CLOB client ──────────────────────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
except ImportError as e:
    raise SystemExit(f"py_clob_client import failed: {e}\n  pip install py-clob-client")

# POLYGON chain ID
try:
    from py_clob_client.constants import POLYGON
except ImportError:
    POLYGON = 137  # Polygon Mainnet chain ID

load_dotenv()
console = Console()

# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    # Polymarket
    polymarket_key: str = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    polymarket_api_key: str = os.getenv("POLYMARKET_API_KEY", "")
    polymarket_secret: str = os.getenv("POLYMARKET_API_SECRET", "")
    polymarket_passphrase: str = os.getenv("POLYMARKET_API_PASSPHRASE", "")
    clob_host: str = "https://clob.polymarket.com"

    # Telegram
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # Trading parameters
    min_edge_pct: float = 2.0           #3.5           # Lowered — fee filter now handles the real floor
    lag_threshold_pct: float =  1.0     #3.0      # Polymarket lag vs CEX to flag (%)
    max_position_pct: float = 8.0       # Max position as % of portfolio
    confidence_threshold: float = 65.0  #90.0  # Raised — fewer but higher-quality signals
    kelly_fraction: float = 0.5         # Half-Kelly
    kill_switch_drawdown: float = 20.0  # Daily drawdown % to halt all trading
    max_slippage_pct: float = 1.5       # Max acceptable VWAP slippage vs best price (%)

    # Fee-aware trading
    # Polymarket taker fee is ~1.8% peak. Only trade if edge > fee + buffer.
    # Set to 0 to disable (e.g. if you have a maker rebate).
    taker_fee_pct: float = 1.8          # Current Polymarket peak taker fee (%)
    fee_buffer_pct: float = 0.8         # Extra buffer above fee before trading
    # Effective min edge = taker_fee_pct + fee_buffer_pct = 2.6% minimum net edge

    # Post-spike cooldown: avoid trading N seconds after a violent CEX move
    # Violent = BTC/ETH moves more than spike_threshold_pct in a single tick
    spike_threshold_pct: float = 0.3    # % single-tick move that triggers cooldown
    spike_cooldown_sec: float = 15.0    # Seconds to pause after a spike

    # Polymarket WebSocket
    poly_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Binance WebSocket — use stream.binance.us if your server gets HTTP 451
    binance_ws_url: str = "wss://stream.binance.us:9443/stream"
    binance_ws_fallback: str = "wss://data-stream.binance.com/stream"

    # SQLite
    db_path: str = "trades.db"

    # Rate limiting
    order_cooldown_sec: float = 1.0
    ws_reconnect_delay: float = 5.0
    max_retries: int = 5


CONFIG = Config()

# ═══════════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("arb_bot.log"),
        logging.StreamHandler(),
    ],
)
# Keep arb_bot at DEBUG for our own messages, suppress noisy libraries
logging.getLogger("arb_bot").setLevel(logging.DEBUG)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("hpack").setLevel(logging.WARNING)
logging.getLogger("h2").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
log = logging.getLogger("arb_bot")

# ═══════════════════════════════════════════════════════════════════════════
# Data models
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class MarketSnapshot:
    market_id: str
    asset: str          # BTC or ETH
    direction: str      # UP or DOWN
    duration: str       # 5min or 15min
    yes_price: float    # Polymarket YES token price (0-1)
    no_price: float
    # Raw order book levels: list of (price, size) tuples, asks sorted low→high
    asks: list[tuple[float, float]] = field(default_factory=list)
    bids: list[tuple[float, float]] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)


@dataclass
class PriceSnapshot:
    asset: str
    price: float
    prev_price: float
    timestamp: float = field(default_factory=time.time)

    @property
    def change_pct(self) -> float:
        if self.prev_price == 0:
            return 0.0
        return (self.price - self.prev_price) / self.prev_price * 100


@dataclass
class TradeSignal:
    market_id: str
    asset: str
    direction: str
    duration: str
    side: str           # YES or NO
    poly_price: float
    fair_value: float
    edge_pct: float
    confidence: float
    kelly_size: float   # fraction of portfolio
    timestamp: float = field(default_factory=time.time)


@dataclass
class Position:
    trade_id: str
    market_id: str
    asset: str
    direction: str
    side: str
    entry_price: float
    size_usdc: float
    fair_value_at_entry: float
    edge_at_entry: float
    status: str = "OPEN"    # OPEN | CLOSED | PAPER
    pnl: float = 0.0
    opened_at: float = field(default_factory=time.time)
    closed_at: Optional[float] = None


# ═══════════════════════════════════════════════════════════════════════════
# Database
# ═══════════════════════════════════════════════════════════════════════════

class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id    TEXT PRIMARY KEY,
                market_id   TEXT NOT NULL,
                asset       TEXT NOT NULL,
                direction   TEXT NOT NULL,
                side        TEXT NOT NULL,
                entry_price REAL NOT NULL,
                size_usdc   REAL NOT NULL,
                fair_value  REAL NOT NULL,
                edge_pct    REAL NOT NULL,
                confidence  REAL NOT NULL,
                status      TEXT NOT NULL DEFAULT 'OPEN',
                pnl         REAL NOT NULL DEFAULT 0,
                mode        TEXT NOT NULL DEFAULT 'PAPER',
                opened_at   REAL NOT NULL,
                closed_at   REAL
            );

            CREATE TABLE IF NOT EXISTS price_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                asset     TEXT NOT NULL,
                price     REAL NOT NULL,
                logged_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id   TEXT NOT NULL,
                asset       TEXT NOT NULL,
                direction   TEXT NOT NULL,
                side        TEXT NOT NULL,
                poly_price  REAL NOT NULL,
                fair_value  REAL NOT NULL,
                edge_pct    REAL NOT NULL,
                confidence  REAL NOT NULL,
                acted_on    INTEGER NOT NULL DEFAULT 0,
                logged_at   REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_stats (
                market_id   TEXT PRIMARY KEY,
                asset       TEXT NOT NULL,
                direction   TEXT NOT NULL,
                duration    TEXT NOT NULL,
                total       INTEGER NOT NULL DEFAULT 0,
                wins        INTEGER NOT NULL DEFAULT 0,
                total_pnl   REAL NOT NULL DEFAULT 0,
                last_trade  REAL NOT NULL DEFAULT 0,
                blacklisted INTEGER NOT NULL DEFAULT 0
            );
        """)
        self.conn.commit()

    def insert_trade(self, pos: Position, mode: str):
        self.conn.execute("""
            INSERT OR REPLACE INTO trades VALUES (
                :trade_id, :market_id, :asset, :direction, :side,
                :entry_price, :size_usdc, :fair_value, :edge_pct,
                :confidence, :status, :pnl, :mode, :opened_at, :closed_at
            )
        """, {
            "trade_id": pos.trade_id,
            "market_id": pos.market_id,
            "asset": pos.asset,
            "direction": pos.direction,
            "side": pos.side,
            "entry_price": pos.entry_price,
            "size_usdc": pos.size_usdc,
            "fair_value": pos.fair_value_at_entry,
            "edge_pct": pos.edge_at_entry,
            "confidence": 0,
            "status": pos.status,
            "pnl": pos.pnl,
            "mode": mode,
            "opened_at": pos.opened_at,
            "closed_at": pos.closed_at,
        })
        self.conn.commit()

    def update_trade(self, trade_id: str, status: str, pnl: float, closed_at: float):
        self.conn.execute(
            "UPDATE trades SET status=?, pnl=?, closed_at=? WHERE trade_id=?",
            (status, pnl, closed_at, trade_id),
        )
        self.conn.commit()

    def log_signal(self, sig: TradeSignal, acted_on: bool):
        self.conn.execute("""
            INSERT INTO signals (market_id, asset, direction, side,
                poly_price, fair_value, edge_pct, confidence, acted_on, logged_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (sig.market_id, sig.asset, sig.direction, sig.side,
              sig.poly_price, sig.fair_value, sig.edge_pct,
              sig.confidence, int(acted_on), sig.timestamp))
        self.conn.commit()

    def recent_trades(self, n: int = 10) -> list[dict]:
        cur = self.conn.execute(
            "SELECT * FROM trades ORDER BY opened_at DESC LIMIT ?", (n,)
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def daily_pnl(self) -> float:
        today_start = datetime.combine(date.today(), datetime.min.time()).timestamp()
        cur = self.conn.execute(
            "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE opened_at >= ?",
            (today_start,),
        )
        return cur.fetchone()[0]

    def win_rate(self) -> float:
        cur = self.conn.execute(
            "SELECT COUNT(*) as total, SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins "
            "FROM trades WHERE status='CLOSED'"
        )
        row = cur.fetchone()
        total, wins = row
        if not total:
            return 0.0
        return wins / total * 100

    def update_market_stats(self, market_id: str, asset: str, direction: str,
                             duration: str, pnl: float):
        """Upsert per-market stats and auto-blacklist losers."""
        self.conn.execute("""
            INSERT INTO market_stats (market_id, asset, direction, duration,
                total, wins, total_pnl, last_trade)
            VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
                total      = total + 1,
                wins       = wins + (CASE WHEN excluded.total_pnl > 0 THEN 1 ELSE 0 END),
                total_pnl  = total_pnl + excluded.total_pnl,
                last_trade = excluded.last_trade
        """, (market_id, asset, direction, duration,
              1 if pnl > 0 else 0, pnl, time.time()))

        # Blacklist: ≥5 trades and win rate < 35%
        self.conn.execute("""
            UPDATE market_stats
            SET blacklisted = 1
            WHERE market_id = ? AND total >= 5
              AND CAST(wins AS REAL) / total < 0.35
        """, (market_id,))
        self.conn.commit()

    def is_blacklisted(self, market_id: str) -> bool:
        cur = self.conn.execute(
            "SELECT blacklisted FROM market_stats WHERE market_id = ?", (market_id,)
        )
        row = cur.fetchone()
        return bool(row and row[0])

    def market_win_rates(self) -> list[dict]:
        cur = self.conn.execute("""
            SELECT market_id, asset, direction, duration,
                   total, wins,
                   ROUND(CAST(wins AS REAL) / total * 100, 1) as win_rate,
                   ROUND(total_pnl, 2) as total_pnl,
                   blacklisted
            FROM market_stats
            ORDER BY win_rate DESC
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════════════════
# Telegram notifier
# ═══════════════════════════════════════════════════════════════════════════

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def send(self, message: str, retries: int = 3):
        if not self.enabled:
            log.debug("Telegram disabled (no token/chat_id)")
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "Markdown"}
        for attempt in range(retries):
            try:
                session = await self._get_session()
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return
                    log.warning("Telegram HTTP %s", resp.status)
            except Exception as e:
                log.warning("Telegram send failed (attempt %d): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ═══════════════════════════════════════════════════════════════════════════
# Binance WebSocket price feed
# ═══════════════════════════════════════════════════════════════════════════

class BinancePriceFeed:
    # bookTicker fires instantly on any best bid/ask change — much faster than @ticker
    # depth20@100ms for OFI, ticker as fallback price source
    STREAMS = (
        "btcusdt@bookTicker/ethusdt@bookTicker"
        "/btcusdt@depth20@100ms/ethusdt@depth20@100ms"
        "/btcusdt@ticker/ethusdt@ticker"
    )

    def __init__(self):
        self.prices: dict[str, float] = {"BTC": 0.0, "ETH": 0.0}
        self._prev: dict[str, float] = {"BTC": 0.0, "ETH": 0.0}
        self.ofi: dict[str, float] = {"BTC": 0.5, "ETH": 0.5}
        # Spike cooldown: timestamp of last violent move per asset
        self.last_spike: dict[str, float] = {"BTC": 0.0, "ETH": 0.0}
        self._running = False
        self._callbacks: list = []

    def is_in_cooldown(self, asset: str) -> bool:
        """Returns True if we should skip trading due to a recent price spike."""
        elapsed = time.time() - self.last_spike.get(asset, 0)
        return elapsed < CONFIG.spike_cooldown_sec

    def on_update(self, cb):
        self._callbacks.append(cb)

    def _parse_depth(self, asset: str, data: dict):
        """Compute order flow imbalance from top-20 depth snapshot."""
        try:
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            bid_vol = sum(float(b[1]) for b in bids[:10])
            ask_vol = sum(float(a[1]) for a in asks[:10])
            total = bid_vol + ask_vol
            self.ofi[asset] = bid_vol / total if total > 0 else 0.5
        except (ValueError, IndexError, ZeroDivisionError):
            pass

    async def run(self):
        self._running = True
        urls = [
            f"{CONFIG.binance_ws_url}?streams={self.STREAMS}",
            f"{CONFIG.binance_ws_fallback}?streams={self.STREAMS}",
        ]
        url_index = 0
        while self._running:
            url = urls[url_index % len(urls)]
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    log.info("Binance WebSocket connected: %s", url)
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            data = msg.get("data", {})
                            stream = msg.get("stream", "")

                            # Determine asset
                            if "btcusdt" in stream:
                                asset = "BTC"
                            elif "ethusdt" in stream:
                                asset = "ETH"
                            else:
                                continue

                            # Depth stream → update OFI only
                            if "@depth" in stream:
                                self._parse_depth(asset, data)
                                continue

                            # bookTicker stream → instant best bid/ask, compute mid
                            if "bookticker" in stream or "@bookTicker" in stream.lower():
                                best_bid = float(data.get("b", 0) or data.get("B", 0))
                                best_ask = float(data.get("a", 0) or data.get("A", 0))
                                if best_bid > 0 and best_ask > 0:
                                    new_price = (best_bid + best_ask) / 2
                                else:
                                    continue
                            else:
                                # Ticker stream → last trade price
                                new_price = float(data.get("c", 0))
                            if new_price <= 0:
                                continue
                            snap = PriceSnapshot(
                                asset=asset,
                                price=new_price,
                                prev_price=self.prices[asset],
                            )
                            # Spike detection: large single-tick move triggers cooldown
                            if self.prices[asset] > 0:
                                tick_chg = abs(new_price - self.prices[asset]) / self.prices[asset] * 100
                                if tick_chg >= CONFIG.spike_threshold_pct:
                                    self.last_spike[asset] = time.time()
                                    log.debug("Spike detected %s: %.3f%% — cooldown %ds",
                                              asset, tick_chg, CONFIG.spike_cooldown_sec)
                            self._prev[asset] = self.prices[asset]
                            self.prices[asset] = new_price
                            for cb in self._callbacks:
                                asyncio.create_task(cb(snap))
                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            log.debug("Parse error: %s", e)
            except (websockets.exceptions.ConnectionClosed,
                    websockets.exceptions.WebSocketException,
                    OSError) as e:
                log.warning("Binance WS disconnected (%s): %s – trying next endpoint in %ds",
                            url, e, CONFIG.ws_reconnect_delay)
                url_index += 1
                await asyncio.sleep(CONFIG.ws_reconnect_delay)

    def stop(self):
        self._running = False


# ═══════════════════════════════════════════════════════════════════════════
# Polymarket market monitor
# ═══════════════════════════════════════════════════════════════════════════

class PolymarketMonitor:
    """
    Polls Polymarket CLOB for BTC/ETH 5-min and 15-min up/down markets.
    Market IDs are discovered automatically from the CLOB markets endpoint,
    filtering by keyword. If none are found, falls back to FALLBACK_MARKET_IDS.
    """

    # Fallback hardcoded IDs — replace these with real condition IDs from
    # https://clob.polymarket.com/markets if auto-discovery fails.
    FALLBACK_MARKET_IDS: dict[str, dict] = {}

    # Keywords used to match active markets from the CLOB
    SEARCH_TERMS = [
        ("BTC", "UP",   "5min",  ["btc", "bitcoin", "higher", "5 min", "5min"]),
        ("BTC", "DOWN", "5min",  ["btc", "bitcoin", "lower",  "5 min", "5min"]),
        ("BTC", "UP",   "15min", ["btc", "bitcoin", "higher", "15 min", "15min"]),
        ("BTC", "DOWN", "15min", ["btc", "bitcoin", "lower",  "15 min", "15min"]),
        ("ETH", "UP",   "5min",  ["eth", "ether",   "higher", "5 min", "5min"]),
        ("ETH", "DOWN", "5min",  ["eth", "ether",   "lower",  "5 min", "5min"]),
        ("ETH", "UP",   "15min", ["eth", "ether",   "higher", "15 min", "15min"]),
        ("ETH", "DOWN", "15min", ["eth", "ether",   "lower",  "15 min", "15min"]),
    ]

    def __init__(self):
        self._client: Optional[ClobClient] = None
        self.snapshots: dict[str, MarketSnapshot] = {}
        self.market_ids: dict[str, dict] = {}   # populated by _discover_markets
        self._callbacks: list = []
        self._running = False
        self.poly_ws = None   # injected by ArbBot.run() for staleness checks
        self._executor = __import__("concurrent.futures", fromlist=["ThreadPoolExecutor"]).ThreadPoolExecutor(max_workers=6)

    def _get_client(self) -> ClobClient:
        if self._client is None:
            self._client = ClobClient(
                host=CONFIG.clob_host,
                key=CONFIG.polymarket_key,
                chain_id=POLYGON,
                creds={
                    "apiKey": CONFIG.polymarket_api_key,
                    "secret": CONFIG.polymarket_secret,
                    "passphrase": CONFIG.polymarket_passphrase,
                },
            )
        return self._client

    async def _discover_markets(self):
        """
        Discover active BTC/ETH short-duration markets using the Gamma API
        events endpoint as recommended by Polymarket docs:
          GET /events?active=true&closed=false&order=end_date&ascending=true

        Only includes markets where:
          - enableOrderBook is true (CLOB tradeable)
          - endDate is in the future with at least 60s remaining
          - question matches BTC/ETH + up-or-down short-duration pattern
        """
        log.info("Discovering active Polymarket markets via Gamma events API…")
        GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

        now = time.time()

        ASSET_KEYWORDS = {
            "BTC": ["btc", "bitcoin"],
            "ETH": ["eth", "ethereum", "ether"],
            # Only BTC and ETH — signal engine prices these from Binance.
            # Altcoins (SOL, XRP, BNB, DOGE, HYPE etc.) are intentionally excluded.
        }
        # Skip if question contains any of these — altcoins and non-price markets
        SKIP_KEYWORDS = [
            "solana", "xrp", "ripple", "bnb", "dogecoin", "doge",
            "hyperliquid", "avax", "avalanche", "matic", "polygon",
            "ada", "cardano", "link", "chainlink", "map 1", "map 2",
            "kills", "odd/even", "esport", "sol up", "sol down",
        ]
        # Polymarket 5M/15M canonical patterns seen in production:
        #   "bitcoin up or down - march 29, 5:15am-5:20am et"
        #   "bitcoin up or down on march 29?"
        #   "btc up or down - march 29, 4am et"
        SHORT_DUR_SIGNALS = [
            "up or down", "am et", "pm et",
            "5 min", "5min", "15 min", "15min",
            "5 minutes", "15 minutes",
        ]

        found: dict[str, dict] = {}

        try:
            async with aiohttp.ClientSession() as session:
                offset = 0
                limit  = 100
                while len(found) < 50:   # stop after finding enough or exhausting pages
                    params = {
                        "archived": "false",
                        "active":   "true",
                        "closed":   "false",
                        "order":    "endDate",    # camelCase per Gamma API
                        "ascending":"true",
                        "limit":    limit,
                        "offset":   offset,
                    }
                    async with session.get(
                        GAMMA_EVENTS_URL, params=params,
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as resp:
                        if resp.status != 200:
                            # Fallback: try without order param which may be causing 422
                            params2 = {
                                "archived": "false",
                                "active":   "true",
                                "closed":   "false",
                                "limit":    limit,
                                "offset":   offset,
                            }
                            async with session.get(
                                GAMMA_EVENTS_URL, params=params2,
                                timeout=aiohttp.ClientTimeout(total=15)
                            ) as resp2:
                                if resp2.status != 200:
                                    log.error("Gamma events API HTTP %s (fallback also failed %s)",
                                              resp.status, resp2.status)
                                    break
                                events = await resp2.json(content_type=None)
                        else:
                            events = await resp.json(content_type=None)

                    if not isinstance(events, list):
                        events = events.get("data", events.get("events", []))

                    if not events:
                        break   # no more pages

                    for event in events:
                        # Each event contains a list of markets
                        markets = event.get("markets") or []
                        for m in markets:
                            # ── CLOB order book must be enabled ────────────
                            if not m.get("enableOrderBook", False):
                                continue

                            # ── Must not be closed/resolved ────────────────
                            if m.get("closed") or m.get("resolved"):
                                continue

                            # ── endDate must be at least 60s in the future ─
                            end_date_str = m.get("endDate") or m.get("end_date") or ""
                            if end_date_str:
                                try:
                                    from datetime import timezone
                                    end_dt = datetime.fromisoformat(
                                        end_date_str.replace("Z", "+00:00")
                                    )
                                    end_ts = end_dt.timestamp()
                                    if end_ts < now + 120:   # need at least 2 min remaining
                                        continue
                                    # Also cap at 20 minutes — beyond that it's not a 5M/15M
                                    if end_ts > now + 1200:
                                        continue
                                except Exception:
                                    pass

                            question = (m.get("question") or "").lower().strip()
                            if not question:
                                continue

                            # ── Skip altcoins and non-price markets ────────
                            if any(skip in question for skip in SKIP_KEYWORDS):
                                continue

                            log.debug("CANDIDATE: %s", question)

                            # ── Asset match (BTC/ETH only) ─────────────────
                            asset = None
                            for a, kws in ASSET_KEYWORDS.items():
                                if any(k in question for k in kws):
                                    asset = a
                                    break
                            if not asset:
                                continue

                            # ── Short-duration signal ──────────────────────
                            if not any(p in question for p in SHORT_DUR_SIGNALS):
                                continue

                            # ── Duration classification ────────────────────
                            if any(p in question for p in ["15 min", "15min", "15 minutes"]):
                                duration = "15min"
                            else:
                                duration = "5min"

                            # ── Extract token IDs ──────────────────────────
                            clob_token_ids = m.get("clobTokenIds") or []
                            if isinstance(clob_token_ids, str):
                                import json as _json
                                try:
                                    clob_token_ids = _json.loads(clob_token_ids)
                                except Exception:
                                    clob_token_ids = []

                            outcomes = m.get("outcomes") or []
                            if isinstance(outcomes, str):
                                import json as _json
                                try:
                                    outcomes = _json.loads(outcomes)
                                except Exception:
                                    outcomes = []

                            for i, token_id in enumerate(clob_token_ids):
                                tid = str(token_id)
                                if tid in found:
                                    continue  # already discovered, skip duplicate

                                outcome = str(outcomes[i]).lower() if i < len(outcomes) else ""
                                if any(k in outcome for k in ["up", "higher", "yes", "above"]):
                                    direction = "UP"
                                elif any(k in outcome for k in ["down", "lower", "no", "below"]):
                                    direction = "DOWN"
                                else:
                                    direction = "UP" if i == 0 else "DOWN"

                                found[tid] = {
                                    "asset":     asset,
                                    "direction": direction,
                                    "duration":  duration,
                                    "end_ts":    end_ts if end_date_str else now + 300,
                                }
                                log.info(
                                    "Discovered: %s → %s %s %s  outcome=%s  (q: %.55s)",
                                    tid[:14], asset, direction, duration,
                                    outcomes[i] if i < len(outcomes) else "?",
                                    question,
                                )

                    offset += limit
                    # Stop once we've passed the 20-min expiry window
                    if isinstance(events, list) and events:
                        last = events[-1]
                        last_end = last.get("endDate", "") or last.get("end_date", "")
                        if last_end:
                            try:
                                ts = datetime.fromisoformat(
                                    last_end.replace("Z", "+00:00")
                                ).timestamp()
                                if ts > now + 1200:
                                    break
                            except Exception:
                                pass
                    if not events:
                        break

            if found:
                self.market_ids = found
                log.info("Discovered %d tradeable short-duration markets", len(found))
            else:
                log.warning(
                    "No active BTC/ETH short-duration markets found with order books. "
                    "Check https://polymarket.com/crypto/5M for currently active markets."
                )
                self.market_ids = self.FALLBACK_MARKET_IDS

        except Exception as e:
            log.error("Market discovery failed: %s", e, exc_info=True)
            self.market_ids = self.FALLBACK_MARKET_IDS

    def on_snapshot(self, cb):
        self._callbacks.append(cb)

    async def _fetch_market(self, market_id: str, meta: dict) -> Optional[MarketSnapshot]:
        # Skip tokens that have already expired
        end_ts = meta.get("end_ts", 0)
        if end_ts and time.time() > end_ts:
            log.debug("Skipping expired token %s", market_id[:14])
            return None

        for attempt in range(CONFIG.max_retries):
            try:
                client = self._get_client()
                loop = asyncio.get_running_loop()
                book = await loop.run_in_executor(
                    self._executor, lambda mid=market_id: client.get_order_book(mid)
                )
                best_ask = float(book.asks[0].price) if book.asks else 0.5
                best_bid = float(book.bids[0].price) if book.bids else 0.5
                yes_price = (best_ask + best_bid) / 2

                asks = [(float(l.price), float(l.size)) for l in book.asks]
                bids = [(float(l.price), float(l.size)) for l in book.bids]

                return MarketSnapshot(
                    market_id=market_id,
                    asset=meta["asset"],
                    direction=meta["direction"],
                    duration=meta["duration"],
                    yes_price=yes_price,
                    no_price=1.0 - yes_price,
                    asks=asks,
                    bids=bids,
                )
            except Exception as e:
                err_str = str(e)

                # Hard error: token expired/invalid — drop immediately, no retry
                if "400" in err_str or "status_code=400" in err_str:
                    log.debug("Token %s returned 400 (expired), dropping", market_id[:14])
                    meta["end_ts"] = 0
                    return None

                # Fix 3: gentle backoff for transient "Request exception" (network hiccup)
                # These don't need exponential backoff — a short wait is enough
                is_transient = "Request exception" in err_str or "request exception" in err_str.lower()
                if is_transient:
                    # 0.5s, 1.0s, 1.5s, 2.0s — linear and short
                    backoff = 0.5 * (attempt + 1) + random.uniform(0, 0.3)
                    log.debug("Transient error %s (attempt %d) – retry in %.1fs",
                              market_id[:14], attempt + 1, backoff)
                else:
                    # True exponential for real errors (auth, server error, etc.)
                    backoff = 2 ** attempt + random.uniform(0, 0.5)
                    log.warning("Polymarket fetch failed for %s (attempt %d): %s – retry in %.1fs",
                                market_id[:14], attempt + 1, e, backoff)

                await asyncio.sleep(backoff)
        return None

    async def run(self, poll_interval: float = 1.5, poly_ws: Optional["PolymarketWSFeed"] = None):
        self._running = True
        self.poly_ws = poly_ws   # reference for staleness check
        await self._discover_markets()
        last_discovery = time.time()
        REDISCOVER_INTERVAL = 180  # re-discover every 3 minutes (5M markets rotate)

        # Fix 4: cooldown after discovery — let the WS feed warm up before
        # hammering REST, avoids the burst of Request exceptions on startup
        log.info("Waiting 12s after discovery before starting REST polling…")
        await asyncio.sleep(12)

        while self._running:
            # Re-discover periodically since 5M markets expire and new ones open
            if time.time() - last_discovery > REDISCOVER_INTERVAL:
                await self._discover_markets()
                last_discovery = time.time()
                # Cooldown after re-discovery too
                await asyncio.sleep(12)

            if not self.market_ids:
                log.warning("No market IDs — retrying discovery in 30s")
                await asyncio.sleep(30)
                await self._discover_markets()
                last_discovery = time.time()
                continue

            # Fix 1: only REST-poll when WS is stale (>25s without message)
            # When WS is healthy, REST is purely a no-op heartbeat
            ws_healthy = self.poly_ws.is_healthy if self.poly_ws else False
            if ws_healthy:
                log.debug("WS healthy — skipping REST poll cycle")
            else:
                if not ws_healthy:
                    log.warning("WS stale — falling back to REST for one cycle")
                active_ids = {
                    mid: meta for mid, meta in self.market_ids.items()
                    if not meta.get("end_ts") or meta["end_ts"] > time.time() + 30
                }

                items = list(active_ids.items())
                BATCH_SIZE = 2
                for i in range(0, len(items), BATCH_SIZE):
                    batch = items[i:i + BATCH_SIZE]
                    results = await asyncio.gather(
                        *[self._fetch_market(mid, meta) for mid, meta in batch],
                        return_exceptions=True
                    )
                    for snap in results:
                        if isinstance(snap, MarketSnapshot):
                            self.snapshots[snap.market_id] = snap
                            for cb in self._callbacks:
                                asyncio.create_task(cb(snap))
                    await asyncio.sleep(0.1)

            # Fix 2: random jitter prevents synchronised rate-limit hits
            await asyncio.sleep(poll_interval + random.uniform(0.1, 0.3))

    def stop(self):
        self._running = False


# ═══════════════════════════════════════════════════════════════════════════
# Polymarket WebSocket order book feed
# ═══════════════════════════════════════════════════════════════════════════

class PolymarketWSFeed:
    """
    Hybrid WS primary + REST fallback for Polymarket order books.

    Key design decisions based on observed WS behaviour:
      - Subscribe in batches of 15 with 0.8s spacing (bulk subscribe gets dropped/rejected)
      - WS sends bids/asks as [[price, size], ...] arrays, NOT {"price":…} dicts
      - Control messages ("INVALID OPERATION", "subscription", "welcome") must be
        filtered before json.loads to avoid spurious parse errors
      - _last_msg_time drives REST fallback — if WS silent >25s, REST takes over
    """

    WS_STALE_SEC   = 25.0
    SUB_BATCH_SIZE = 12    # smaller batches = more stable per Polymarket server
    SUB_BATCH_DELAY = 1.2  # seconds between subscription batches

    def __init__(self):
        self._books: dict[str, dict] = {}
        self.snapshots: dict[str, MarketSnapshot] = {}   # mirrors PolymarketMonitor.snapshots
        self._callbacks: list = []
        self._market_meta: dict[str, dict] = {}
        self._running = False
        self._ws = None
        self._last_msg_time: float = 0.0
        self._connected: bool = False
        self._debug_logged: bool = False

    def on_snapshot(self, cb):
        self._callbacks.append(cb)

    def set_markets(self, market_ids: dict[str, dict]):
        self._market_meta = market_ids

    @property
    def is_healthy(self) -> bool:
        if not self._connected:
            return False
        return (time.time() - self._last_msg_time) < self.WS_STALE_SEC

    def get_snapshot(self, market_id: str) -> Optional[MarketSnapshot]:
        book = self._books.get(market_id)
        meta = self._market_meta.get(market_id)
        if not book or not meta:
            return None
        return MarketSnapshot(
            market_id=market_id,
            asset=meta["asset"],
            direction=meta["direction"],
            duration=meta["duration"],
            yes_price=book.get("yes_price", 0.5),
            no_price=1.0 - book.get("yes_price", 0.5),
            asks=book.get("asks", []),
            bids=book.get("bids", []),
        )

    def _parse_level(self, level) -> Optional[tuple[float, float]]:
        """
        Parse a single order book level.
        Polymarket sends levels as [price, size] arrays.
        Some endpoints use {"price": x, "size": y} dicts — handle both.
        """
        try:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                return float(level[0]), float(level[1])
            elif isinstance(level, dict):
                p = level.get("price") or level.get("p") or level.get("px")
                s = level.get("size") or level.get("s") or level.get("sz")
                if p is not None and s is not None:
                    return float(p), float(s)
        except (ValueError, TypeError):
            pass
        return None

    def _parse_levels(self, raw) -> list[tuple[float, float]]:
        if not isinstance(raw, list):
            return []
        parsed = [self._parse_level(l) for l in raw]
        return [x for x in parsed if x is not None]

    async def _process_ws_update(self, data: dict):
        """
        Parse Polymarket WS update into a MarketSnapshot.

        Polymarket sends best_bid / best_ask as top-level fields for speed.
        Full bids/asks arrays are also included when present — we use them
        for the slippage guard. When only best prices are available we
        construct a single-level synthetic book with a realistic size cap
        so the slippage guard doesn't assume infinite liquidity.
        """
        try:
            asset_id = (data.get("asset_id") or data.get("market") or
                        data.get("asset") or data.get("token_id"))
            if not asset_id:
                return
            asset_id = str(asset_id)
            if asset_id not in self._market_meta:
                return

            meta = self._market_meta[asset_id]

            # ── Best bid/ask (fast path — always present) ──────────────────
            best_bid_raw = data.get("best_bid") or data.get("bid") or 0
            best_ask_raw = data.get("best_ask") or data.get("ask") or 0
            best_bid = float(best_bid_raw) if best_bid_raw else 0.0
            best_ask = float(best_ask_raw) if best_ask_raw else 0.0

            # ── Full order book levels (when present, use them) ─────────────
            bids = self._parse_levels(data.get("bids") or data.get("bid_levels") or [])
            asks = self._parse_levels(data.get("asks") or data.get("ask_levels") or [])

            # If full levels missing, build minimal synthetic book from best prices
            # Use a realistic synthetic size ($50) not 1000 — avoids fooling slippage guard
            if not bids and best_bid > 0:
                bids = [(best_bid, 50.0)]
            if not asks and best_ask > 0:
                asks = [(best_ask, 50.0)]

            if not bids or not asks:
                return

            bids = sorted(bids, key=lambda x: -x[0])
            asks = sorted(asks, key=lambda x:  x[0])

            yes_price = (bids[0][0] + asks[0][0]) / 2

            # Skip contracts that are already near resolution (price outside 0.15–0.85)
            # These are expired/almost-expired and have no trading value
            if yes_price < 0.15 or yes_price > 0.85:
                log.debug("WS skip near-resolved %s @ %.4f", asset_id[:12], yes_price)
                return

            snap = MarketSnapshot(
                market_id=asset_id,
                asset=meta["asset"],
                direction=meta["direction"],
                duration=meta["duration"],
                yes_price=yes_price,
                no_price=1.0 - yes_price,
                bids=bids[:10],
                asks=asks[:10],
            )

            self._books[asset_id] = {
                "bids": bids[:10], "asks": asks[:10], "yes_price": yes_price
            }
            self.snapshots[asset_id] = snap

            for cb in self._callbacks:
                asyncio.create_task(cb(snap))

            log.debug("WS snap %s %s %s @ %.4f",
                      meta["asset"], meta["direction"], asset_id[:12], yes_price)

        except Exception as e:
            log.debug("WS parse failed: %s — data keys: %s", e,
                      list(data.keys())[:6] if isinstance(data, dict) else type(data))

    async def _subscribe(self, ws):
        """Subscribe using the correct Polymarket WS protocol format."""
        token_ids = list(self._market_meta.keys())
        total = len(token_ids)
        n_batches = -(-total // self.SUB_BATCH_SIZE)  # ceiling division

        for i in range(0, total, self.SUB_BATCH_SIZE):
            batch = token_ids[i:i + self.SUB_BATCH_SIZE]
            # Correct Polymarket format: type="market", key="assets_ids" (with underscore)
            msg = {
                "type": "market",
                "assets_ids": batch,
                "custom_feature_enabled": True,
            }
            await ws.send(json.dumps(msg))
            log.info("WS subscribed batch %d/%d (%d tokens)",
                     i // self.SUB_BATCH_SIZE + 1, n_batches, len(batch))
            if i + self.SUB_BATCH_SIZE < total:
                await asyncio.sleep(1.2)  # give server time to process each batch

        log.info("WS all %d tokens subscribed", total)
    async def _handle_message(self, raw: str):
        """Parse and dispatch one raw WS message."""
        self._last_msg_time = time.time()

        # Log first real message for parser calibration
        if not self._debug_logged and raw.strip().startswith("{"):
            log.debug("WS first message (raw): %s", raw[:400])
            self._debug_logged = True

        # Fast-path: filter known non-JSON control strings before parsing
        if not raw or not raw.strip():
            return
        if "INVALID OPERATION" in raw:
            log.debug("WS control message: INVALID OPERATION")
            return

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.debug("WS non-JSON: %s", raw[:120])
            return

        # Normalise to list
        if not isinstance(data, list):
            data = [data]

        for msg in data:
            if not isinstance(msg, dict):
                continue

            # Skip anything that looks like a control/error response
            msg_type = (msg.get("type") or msg.get("event") or
                        msg.get("event_type") or "").lower()
            if msg_type in ("subscription", "subscribed", "welcome",
                            "pong", "heartbeat", "connected", ""):
                log.debug("WS control: type=%s", msg_type or "(empty)")
                continue

            # Catch INVALID OPERATION buried inside a parsed dict
            if "INVALID" in str(msg).upper():
                log.debug("WS server rejection: %s", str(msg)[:120])
                continue

            # Book data arrives under several event type names
            if msg_type in ("book", "price_change", "last_trade_price",
                            "update", "orderbook", "tick", "market",
                            "best_bid_ask"):   # ← primary real-time feed from Polymarket
                await self._process_ws_update(msg)

            elif msg_type == "price_changes":
                # Batch format: {"type":"price_changes", "price_changes":[{asset_id, best_bid, best_ask}, ...]}
                changes = msg.get("price_changes") or []
                for change in changes:
                    if isinstance(change, dict):
                        await self._process_ws_update(change)
            else:
                log.debug("WS unhandled type=%s keys=%s", msg_type,
                          list(msg.keys())[:6])

    async def run(self):
        self._running = True
        while self._running:
            if not self._market_meta:
                await asyncio.sleep(1)
                continue
            try:
                async with websockets.connect(
                    CONFIG.poly_ws_url,
                    ping_interval=20,
                    ping_timeout=30,
                    max_size=2**20,
                    additional_headers={"User-Agent": "polymarket-arb-bot/1.0"},
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._last_msg_time = time.time()
                    self._debug_logged = False

                    await self._subscribe(ws)

                    async for raw in ws:
                        await self._handle_message(raw)

            except (websockets.exceptions.ConnectionClosed,
                    websockets.exceptions.WebSocketException,
                    OSError) as e:
                log.warning("Polymarket WS disconnected: %s – reconnecting in %ds",
                            e, CONFIG.ws_reconnect_delay)
            except Exception as e:
                log.error("Polymarket WS unexpected error: %s", e, exc_info=True)
            finally:
                self._ws = None
                self._connected = False
                await asyncio.sleep(CONFIG.ws_reconnect_delay)

    async def resubscribe(self, market_ids: dict[str, dict]):
        self.set_markets(market_ids)
        if self._ws and self._connected:
            try:
                await self._subscribe(self._ws)
            except Exception as e:
                log.warning("WS resubscribe failed: %s", e)

    def stop(self):
        self._running = False


class SignalEngine:
    """
    Computes fair value from CEX prices using:
      1. Multi-timeframe momentum (1min + 5min + 15min must agree)
      2. Order flow imbalance (Binance top-10 bid/ask volume ratio)
    Confidence score scales position size via KellySizer.
    """

    def __init__(self, price_feed: BinancePriceFeed):
        self.feed = price_feed
        self._price_history: dict[str, list[float]] = {"BTC": [], "ETH": []}

    def record_price(self, asset: str, price: float):
        hist = self._price_history[asset]
        hist.append(price)
        if len(hist) > 1000:  # ~16 min at 1 tick/sec
            hist.pop(0)

    def _momentum(self, hist: list[float], window: int) -> float:
        """Returns momentum % over the last `window` ticks."""
        if len(hist) < window:
            return 0.0
        start = hist[-window]
        current = hist[-1]
        return (current - start) / start * 100 if start > 0 else 0.0

    def _compute_fair_value(self, asset: str, direction: str, duration: str) -> tuple[float, float]:
        """
        Returns (fair_value_prob, confidence_score 0-100).

        Three signals are combined:
          - Short-term momentum (1min window)
          - Medium-term momentum matching duration
          - Order flow imbalance from Binance depth stream

        All three must agree directionally for max confidence.
        """
        import math
        hist = self._price_history[asset]
        current = self.feed.prices.get(asset, 0)
        ofi = self.feed.ofi.get(asset, 0.5)

        if current == 0 or len(hist) < 10:
            return 0.5, 0.0

        # ── Multi-timeframe momentum ────────────────────────────────────────
        mom_1min  = self._momentum(hist, min(60,  len(hist)))
        mom_5min  = self._momentum(hist, min(300, len(hist)))
        mom_15min = self._momentum(hist, min(900, len(hist)))

        duration_mom = mom_5min if duration == "5min" else mom_15min

        # All three timeframes must agree in direction
        signs = [
            1 if m > 0 else (-1 if m < 0 else 0)
            for m in [mom_1min, mom_5min, mom_15min]
        ]
        timeframe_agreement = len(set(s for s in signs if s != 0)) == 1

        # ── OFI signal ─────────────────────────────────────────────────────
        # OFI > 0.55 = buy pressure, < 0.45 = sell pressure
        ofi_bullish  = ofi > 0.55
        ofi_bearish  = ofi < 0.45
        ofi_neutral  = not ofi_bullish and not ofi_bearish

        # ── Combined fair value probability ────────────────────────────────
        # Sigmoid on duration momentum
        scale = 2.0
        raw_prob = 1 / (1 + math.exp(-duration_mom * scale / 100 * 10))

        # OFI nudge: push probability 3% toward the OFI-implied direction
        if ofi_bullish:
            raw_prob = min(raw_prob + 0.03, 0.99)
        elif ofi_bearish:
            raw_prob = max(raw_prob - 0.03, 0.01)

        fair_value = (1.0 - raw_prob) if direction == "DOWN" else raw_prob

        # ── Confidence score (0-100) ────────────────────────────────────────
        # Base: data sufficiency
        tick_conf = min(len(hist) / 300, 1.0) * 30          # up to 30 pts

        # Momentum strength
        mom_conf = min(abs(duration_mom) / 2.0, 1.0) * 30   # up to 30 pts

        # Timeframe agreement bonus
        agreement_conf = 25.0 if timeframe_agreement else 0.0

        # OFI agreement bonus: OFI direction matches momentum direction
        mom_is_up = duration_mom > 0
        ofi_agrees = (mom_is_up and ofi_bullish) or (not mom_is_up and ofi_bearish)
        ofi_conf = 15.0 if ofi_agrees else (0.0 if ofi_neutral else -10.0)

        confidence = min(tick_conf + mom_conf + agreement_conf + ofi_conf, 100.0)

        return fair_value, confidence
    
    def evaluate(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        fair_value, confidence = self._compute_fair_value(
            snap.asset, snap.direction, snap.duration
        )
        edge = (fair_value - snap.yes_price) * 100 if snap.direction == "UP" else ((1 - fair_value) - snap.no_price) * 100
    
        log.debug(f"Eval {snap.asset} {snap.direction} | "
              f"Fair={fair_value:.4f} | Poly={snap.yes_price:.4f} | "
              f"Edge={edge:+.2f}% | Conf={confidence:.1f}%")
        
        if confidence < CONFIG.confidence_threshold:
            return None

        yes_edge = (fair_value - snap.yes_price) * 100
        no_edge  = ((1 - fair_value) - snap.no_price) * 100

        if yes_edge >= CONFIG.min_edge_pct and abs(yes_edge) > abs(no_edge):
            side, poly_price, edge = "YES", snap.yes_price, yes_edge
        elif no_edge >= CONFIG.min_edge_pct:
            side, poly_price, edge = "NO", snap.no_price, no_edge
        else:
            return None

        lag = abs((fair_value - snap.yes_price) * 100)
        if lag < CONFIG.lag_threshold_pct:
            return None

        return TradeSignal(
            market_id=snap.market_id,
            asset=snap.asset,
            direction=snap.direction,
            duration=snap.duration,
            side=side,
            poly_price=poly_price,
            fair_value=fair_value,
            edge_pct=edge,
            confidence=confidence,
            kelly_size=0.0,  # filled by KellySizer
        )


# ═══════════════════════════════════════════════════════════════════════════
# Position sizer (half-Kelly)
# ═══════════════════════════════════════════════════════════════════════════

class KellySizer:
    def __init__(self, portfolio_value: float = 1000.0):
        self.portfolio_value = portfolio_value
        self._recent_results: list[bool] = []   # True=win, False=loss

    def record_result(self, win: bool):
        self._recent_results.append(win)
        if len(self._recent_results) > 10:
            self._recent_results.pop(0)

    def _effective_kelly_fraction(self, confidence: float) -> float:
        """
        Scale Kelly fraction with confidence (85→100 maps to 0.25→0.5).
        Also cuts to 0.25 after 3 consecutive losses (losing streak protection).
        """
        # Confidence scaling: linearly interpolate between 0.25 and 0.5
        conf_clamped = max(CONFIG.confidence_threshold, min(confidence, 100.0))
        conf_range   = 100.0 - CONFIG.confidence_threshold
        conf_ratio   = (conf_clamped - CONFIG.confidence_threshold) / conf_range if conf_range > 0 else 1.0
        scaled_kelly = 0.25 + conf_ratio * (CONFIG.kelly_fraction - 0.25)

        # Losing streak: last 3 trades all losses → halve the fraction
        if len(self._recent_results) >= 3 and not any(self._recent_results[-3:]):
            scaled_kelly *= 0.5
            log.info("Losing streak detected — Kelly fraction halved to %.3f", scaled_kelly)

        return scaled_kelly

    def size(self, sig: TradeSignal) -> float:
        """Returns USDC size to trade, scaled by confidence."""
        p = sig.fair_value
        b = (1.0 / sig.poly_price) - 1.0  # net odds
        if b <= 0 or p <= 0 or p >= 1:
            return 0.0

        kelly_f  = self._effective_kelly_fraction(sig.confidence)
        kelly    = (p * b - (1 - p)) / b
        sized    = kelly * kelly_f

        capped = min(sized, CONFIG.max_position_pct / 100)
        capped = max(capped, 0.0)

        return round(self.portfolio_value * capped, 2)


# ═══════════════════════════════════════════════════════════════════════════
# Slippage guard
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SlippageResult:
    vwap: float             # Volume-weighted average fill price
    best_price: float       # Top-of-book price (best ask for buys, best bid for sells)
    slippage_pct: float     # (vwap - best_price) / best_price * 100
    fillable_usdc: float    # Total liquidity available up to size requested
    passes: bool            # True if slippage <= CONFIG.max_slippage_pct


class SlippageGuard:
    """
    Walks the order book to compute the VWAP fill price for a given USDC
    size, then rejects the trade if estimated slippage exceeds the threshold.

    For a YES buy we consume asks (lowest price first).
    For a NO buy we consume bids inverted (highest price first, since
    NO = 1 - YES, so buying NO is equivalent to selling YES).
    """

    @staticmethod
    def check(snap: MarketSnapshot, side: str, size_usdc: float) -> SlippageResult:
        # Choose the correct side of the book
        if side == "YES":
            # Buying YES tokens → walk asks, sorted low→high (already sorted)
            levels = sorted(snap.asks, key=lambda x: x[0])
            best_price = levels[0][0] if levels else snap.yes_price
        else:
            # Buying NO tokens → walk bids inverted; NO price = 1 - bid
            levels = [(1.0 - p, s) for p, s in sorted(snap.bids, key=lambda x: -x[0])]
            best_price = levels[0][0] if levels else snap.no_price

        if not levels or best_price <= 0:
            # No book data — conservatively reject
            return SlippageResult(
                vwap=best_price,
                best_price=best_price,
                slippage_pct=999.0,
                fillable_usdc=0.0,
                passes=False,
            )

        remaining = size_usdc
        total_cost = 0.0
        total_tokens = 0.0

        for price, token_size in levels:
            if remaining <= 0:
                break
            level_usdc = price * token_size       # cost to fill this entire level
            fill_usdc = min(remaining, level_usdc)
            fill_tokens = fill_usdc / price
            total_cost += fill_usdc
            total_tokens += fill_tokens
            remaining -= fill_usdc

        fillable_usdc = size_usdc - remaining
        vwap = total_cost / total_tokens if total_tokens > 0 else best_price
        slippage_pct = abs(vwap - best_price) / best_price * 100 if best_price > 0 else 999.0
        passes = slippage_pct <= CONFIG.max_slippage_pct and remaining == 0

        return SlippageResult(
            vwap=vwap,
            best_price=best_price,
            slippage_pct=slippage_pct,
            fillable_usdc=fillable_usdc,
            passes=passes,
        )



class OrderExecutor:
    def __init__(self, live: bool = False):
        self.live = live
        self._client: Optional[ClobClient] = None
        self._last_order_time: float = 0.0

    def _get_client(self) -> ClobClient:
        if self._client is None:
            self._client = ClobClient(
                host=CONFIG.clob_host,
                key=CONFIG.polymarket_key,
                chain_id=POLYGON,
            )
        return self._client

    def _limit_price(self, sig: TradeSignal, snap: MarketSnapshot) -> float:
        """
        Post a limit order at mid-price rather than hitting the ask.
        For YES buys: mid = (best_bid + best_ask) / 2, rounded to 2 decimals.
        For NO buys:  mid on the NO side = 1 - YES mid.
        """
        if snap.asks and snap.bids:
            best_ask = snap.asks[0][0]
            best_bid = snap.bids[0][0]
            yes_mid  = (best_bid + best_ask) / 2
        else:
            yes_mid = sig.poly_price

        price = yes_mid if sig.side == "YES" else (1.0 - yes_mid)
        return round(price, 4)

    async def execute(self, sig: TradeSignal, size_usdc: float,
                      snap: Optional[MarketSnapshot] = None) -> Optional[Position]:
        # Rate limit
        elapsed = time.time() - self._last_order_time
        if elapsed < CONFIG.order_cooldown_sec:
            await asyncio.sleep(CONFIG.order_cooldown_sec - elapsed)

        trade_id   = f"{sig.market_id}-{int(time.time()*1000)}"
        limit_price = self._limit_price(sig, snap) if snap else sig.poly_price

        if not self.live:
            pos = Position(
                trade_id=trade_id,
                market_id=sig.market_id,
                asset=sig.asset,
                direction=sig.direction,
                side=sig.side,
                entry_price=limit_price,
                size_usdc=size_usdc,
                fair_value_at_entry=sig.fair_value,
                edge_at_entry=sig.edge_pct,
                status="PAPER",
            )
            log.info("[PAPER] LIMIT %s %s %s @ %.4f (mid)  size=$%.2f  edge=%.1f%%  conf=%.0f%%",
                     sig.side, sig.asset, sig.direction,
                     limit_price, size_usdc, sig.edge_pct, sig.confidence)
            self._last_order_time = time.time()
            return pos

        # Live limit order with re-entry retry (up to 3 attempts, 500ms apart)
        for attempt in range(CONFIG.max_retries):
            try:
                client  = self._get_client()
                loop    = asyncio.get_running_loop()
                order_args = OrderArgs(
                    token_id=sig.market_id,
                    price=limit_price,
                    size=size_usdc,
                    side="BUY",
                )
                resp = await loop.run_in_executor(
                    None, lambda: client.create_and_post_order(order_args)
                )
                log.info("[LIVE] Limit order placed @ %.4f: %s", limit_price, resp)
                self._last_order_time = time.time()
                return Position(
                    trade_id=trade_id,
                    market_id=sig.market_id,
                    asset=sig.asset,
                    direction=sig.direction,
                    side=sig.side,
                    entry_price=limit_price,
                    size_usdc=size_usdc,
                    fair_value_at_entry=sig.fair_value,
                    edge_at_entry=sig.edge_pct,
                    status="OPEN",
                )
            except Exception as e:
                backoff = 0.5 if attempt < 3 else 2 ** attempt
                log.error("Order failed (attempt %d): %s – retry in %.1fs", attempt + 1, e, backoff)
                await asyncio.sleep(backoff)

        return None


# ═══════════════════════════════════════════════════════════════════════════
# Terminal Dashboard
# ═══════════════════════════════════════════════════════════════════════════

class Dashboard:
    def __init__(self, db: Database, price_feed: BinancePriceFeed, live: bool):
        self.db = db
        self.feed = price_feed
        self.live = live
        self.positions: list[Position] = []
        self.last_signal: Optional[TradeSignal] = None
        self.kill_switch_active = False
        self._portfolio_value = 1000.0

    def update_portfolio(self, value: float):
        self._portfolio_value = value

    def build(self) -> Panel:
        mode_label = "[bold red]LIVE[/bold red]" if self.live else "[bold yellow]PAPER[/bold yellow]"
        kill_label = " [bold red]⛔ KILL SWITCH ACTIVE[/bold red]" if self.kill_switch_active else ""

        # ── Header ─────────────────────────────────────────────
        header = Text.from_markup(
            f"  Polymarket Arb Bot  |  Mode: {mode_label}{kill_label}  |  "
            f"Portfolio: [green]${self._portfolio_value:,.2f}[/green]  |  "
            f"{datetime.now().strftime('%H:%M:%S')}"
        )

        # ── Prices ─────────────────────────────────────────────
        btc = self.feed.prices.get("BTC", 0)
        eth = self.feed.prices.get("ETH", 0)
        price_line = Text.from_markup(
            f"  BTC: [cyan]${btc:,.2f}[/cyan]   ETH: [cyan]${eth:,.2f}[/cyan]"
        )

        # ── Stats ──────────────────────────────────────────────
        daily_pnl = self.db.daily_pnl()
        wr = self.db.win_rate()
        pnl_color = "green" if daily_pnl >= 0 else "red"
        stats_line = Text.from_markup(
            f"  Daily P&L: [{pnl_color}]${daily_pnl:+.2f}[/{pnl_color}]   "
            f"Win Rate: [white]{wr:.1f}%[/white]   "
            f"Open Positions: [white]{sum(1 for p in self.positions if p.status in ('OPEN','PAPER'))}[/white]"
        )

        # ── Open positions ─────────────────────────────────────
        pos_table = Table(title="Open Positions", expand=True, show_lines=False)
        pos_table.add_column("ID", style="dim", max_width=20)
        pos_table.add_column("Asset")
        pos_table.add_column("Dir")
        pos_table.add_column("Side")
        pos_table.add_column("Entry")
        pos_table.add_column("Size")
        pos_table.add_column("Edge")
        pos_table.add_column("Status")

        for p in self.positions:
            if p.status not in ("OPEN", "PAPER"):
                continue
            pos_table.add_row(
                p.trade_id[-12:],
                p.asset,
                p.direction,
                p.side,
                f"${p.entry_price:.4f}",
                f"${p.size_usdc:.2f}",
                f"{p.edge_at_entry:.1f}%",
                f"[yellow]{p.status}[/yellow]",
            )

        # ── Recent trades ──────────────────────────────────────
        trades_table = Table(title="Last 10 Trades", expand=True, show_lines=False)
        trades_table.add_column("ID", style="dim", max_width=16)
        trades_table.add_column("Asset")
        trades_table.add_column("Dir")
        trades_table.add_column("Side")
        trades_table.add_column("Size")
        trades_table.add_column("P&L")
        trades_table.add_column("Status")

        for t in self.db.recent_trades(10):
            pnl_str = f"[green]${t['pnl']:+.2f}[/green]" if t['pnl'] >= 0 else f"[red]${t['pnl']:+.2f}[/red]"
            trades_table.add_row(
                str(t['trade_id'])[-12:],
                t['asset'],
                t['direction'],
                t['side'],
                f"${t['size_usdc']:.2f}",
                pnl_str,
                t['status'],
            )

        from rich.columns import Columns
        content = "\n".join([
            header.plain,
            price_line.plain,
            stats_line.plain,
        ])

        # ── Per-market win rates ───────────────────────────────
        mkt_table = Table(title="Market Win Rates", expand=True, show_lines=False)
        mkt_table.add_column("Market", style="dim", max_width=18)
        mkt_table.add_column("Asset")
        mkt_table.add_column("Dir")
        mkt_table.add_column("Trades")
        mkt_table.add_column("Win%")
        mkt_table.add_column("P&L")
        mkt_table.add_column("Status")

        for m in self.db.market_win_rates():
            wr_color = "green" if m["win_rate"] >= 50 else "red"
            pnl_color = "green" if m["total_pnl"] >= 0 else "red"
            status = "[red]BLACKLISTED[/red]" if m["blacklisted"] else "[green]active[/green]"
            mkt_table.add_row(
                str(m["market_id"])[-14:],
                m["asset"],
                m["direction"],
                str(m["total"]),
                f"[{wr_color}]{m['win_rate']}%[/{wr_color}]",
                f"[{pnl_color}]${m['total_pnl']:+.2f}[/{pnl_color}]",
                status,
            )

        layout = Layout()
        layout.split_column(
            Layout(Panel(header), size=3),
            Layout(Panel(price_line), size=3),
            Layout(Panel(stats_line), size=3),
            Layout(pos_table, size=10),
            Layout(trades_table, size=14),
            Layout(mkt_table),
        )
        return Panel(layout, title="[bold blue]Polymarket Latency Arb[/bold blue]", border_style="blue")


# ═══════════════════════════════════════════════════════════════════════════
# Main Bot
# ═══════════════════════════════════════════════════════════════════════════

class ArbBot:
    def __init__(self, live: bool = False, portfolio_value: float = 1000.0):
        self.live = live
        self.db = Database(CONFIG.db_path)
        self.price_feed = BinancePriceFeed()
        self.poly_monitor = PolymarketMonitor()
        self.poly_ws = PolymarketWSFeed()        # WebSocket book feed (primary)
        self.signal_engine = SignalEngine(self.price_feed)
        self.sizer = KellySizer(portfolio_value)
        self.executor = OrderExecutor(live=live)
        self.notifier = TelegramNotifier(CONFIG.telegram_token, CONFIG.telegram_chat_id)
        self.dashboard = Dashboard(self.db, self.price_feed, live)
        self.kill_switch = False
        self._positions: list[Position] = []

    async def _on_price_update(self, snap: PriceSnapshot):
        self.signal_engine.record_price(snap.asset, snap.price)

    async def _on_market_snapshot(self, snap: MarketSnapshot):
        if self.kill_switch:
            return

        # Per-market blacklist check
        if self.db.is_blacklisted(snap.market_id):
            log.debug("Skipping blacklisted market %s", snap.market_id)
            return

        # Check daily drawdown kill switch
        daily_pnl = self.db.daily_pnl()
        portfolio = self.sizer.portfolio_value
        if portfolio > 0 and (-daily_pnl / portfolio * 100) >= CONFIG.kill_switch_drawdown:
            if not self.kill_switch:
                self.kill_switch = True
                self.dashboard.kill_switch_active = True
                msg = f"⛔ KILL SWITCH ACTIVATED – Daily drawdown exceeded {CONFIG.kill_switch_drawdown}%"
                log.critical(msg)
                await self.notifier.send(f"*{msg}*")
            return

        sig = self.signal_engine.evaluate(snap)
        if sig is None:
            return

        # ── Spike cooldown ──────────────────────────────────────────────────
        if self.price_feed.is_in_cooldown(sig.asset):
            log.debug("COOLDOWN %s – skipping signal after spike", sig.asset)
            return

        # ── Fee-aware edge filter ───────────────────────────────────────────
        # Net edge must exceed taker fee + buffer to be worth trading.
        # This replaces the raw min_edge_pct check — we still keep min_edge_pct
        # as a secondary floor for very low-fee scenarios.
        net_edge_floor = CONFIG.taker_fee_pct + CONFIG.fee_buffer_pct
        effective_min  = max(CONFIG.min_edge_pct, net_edge_floor)
        if sig.edge_pct < effective_min:
            log.debug("EDGE BELOW FEE FLOOR %.1f%% (need %.1f%%): %s %s",
                      sig.edge_pct, effective_min, sig.asset, sig.direction)
            return

        size = self.sizer.size(sig)
        if size < 1.0:
            return

        # ── Slippage guard ─────────────────────────────────────────────────
        slip = SlippageGuard.check(snap, sig.side, size)
        if not slip.passes:
            log.info(
                "SLIPPAGE REJECTED %s %s %s – slippage=%.2f%% (max %.2f%%), "
                "fillable=$%.2f of $%.2f",
                sig.asset, sig.direction, sig.side,
                slip.slippage_pct, CONFIG.max_slippage_pct,
                slip.fillable_usdc, size,
            )
            return

        acted = False
        if sig.edge_pct >= CONFIG.min_edge_pct and sig.confidence >= CONFIG.confidence_threshold:
            pos = await self.executor.execute(sig, size, snap=snap)
            if pos:
                self._positions.append(pos)
                self.dashboard.positions = self._positions
                mode = "LIVE" if self.live else "PAPER"
                self.db.insert_trade(pos, mode)
                acted = True

                # Simulate close for paper trades and update market stats
                if pos.status == "PAPER":
                    pnl = (sig.fair_value - pos.entry_price) * size
                    self.db.update_trade(pos.trade_id, "CLOSED", pnl, time.time())
                    self.db.update_market_stats(
                        pos.market_id, pos.asset, pos.direction, snap.duration, pnl
                    )
                    self.sizer.record_result(pnl > 0)
                    pos.pnl = pnl
                    pos.status = "CLOSED"

                ofi = self.price_feed.ofi.get(sig.asset, 0.5)
                msg = (
                    f"{'🟢' if self.live else '📋'} *{'LIVE' if self.live else 'PAPER'} TRADE*\n"
                    f"Market: {sig.asset} {sig.direction} {snap.duration}\n"
                    f"Side: {sig.side} @ limit={pos.entry_price:.4f}  (VWAP={slip.vwap:.4f})\n"
                    f"Slippage: {slip.slippage_pct:.2f}%  Edge: {sig.edge_pct:.1f}%\n"
                    f"Fair Value: {sig.fair_value:.4f}  Confidence: {sig.confidence:.0f}%\n"
                    f"OFI: {ofi:.2f}  Size: ${size:.2f}"
                )
                await self.notifier.send(msg)

        self.db.log_signal(sig, acted)

    async def run(self):
        mode_str = "LIVE TRADING" if self.live else "PAPER TRADING"
        log.info("Starting Polymarket Arb Bot in %s mode", mode_str)
        if self.live:
            log.warning("⚠️  LIVE TRADING ENABLED – real funds at risk")

        # Wire callbacks — WS feed is primary, REST monitor is fallback/discovery
        self.price_feed.on_update(self._on_price_update)
        self.poly_ws.on_snapshot(self._on_market_snapshot)    # WS = primary
        self.poly_monitor.on_snapshot(self._on_market_snapshot)  # REST = fallback

        # After discovery, sync market meta to WS feed only when markets change
        async def _sync_ws_markets():
            last_market_ids: set = set()
            while True:
                await asyncio.sleep(10)
                current_ids = set(self.poly_monitor.market_ids.keys())
                if current_ids and current_ids != last_market_ids:
                    log.info("Market IDs changed (%d → %d) — resyncing WS",
                             len(last_market_ids), len(current_ids))
                    self.poly_ws.set_markets(self.poly_monitor.market_ids)
                    await self.poly_ws.resubscribe(self.poly_monitor.market_ids)
                    last_market_ids = current_ids

        tasks = [
            asyncio.create_task(self.price_feed.run(),                        name="binance_ws"),
            asyncio.create_task(self.poly_ws.run(),                           name="poly_ws"),
            asyncio.create_task(self.poly_monitor.run(poly_ws=self.poly_ws),  name="poly_rest"),
            asyncio.create_task(_sync_ws_markets(),                           name="ws_sync"),
            asyncio.create_task(self._dashboard_loop(),                       name="dashboard"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Shutting down…")
        finally:
            self.price_feed.stop()
            self.poly_monitor.stop()
            self.poly_ws.stop()
            await self.notifier.close()

    async def _dashboard_loop(self):
        with Live(self.dashboard.build(), refresh_per_second=2, console=console) as live:
            while True:
                live.update(self.dashboard.build())
                await asyncio.sleep(0.5)


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Polymarket Latency Arbitrage Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--live", action="store_true",
                        help="Enable live trading (flag 1 of 3)")
    parser.add_argument("--confirm-live", action="store_true",
                        help="Confirm live trading intent (flag 2 of 3)")
    parser.add_argument("--i-understand-risks", action="store_true",
                        help="Acknowledge financial risks (flag 3 of 3)")
    parser.add_argument("--portfolio", type=float, default=1000.0,
                        help="Portfolio value in USDC (default: 1000)")
    return parser.parse_args()


def main():
    args = parse_args()

    live = args.live and args.confirm_live and args.i_understand_risks

    if args.live and not live:
        console.print("[bold red]ERROR:[/bold red] All three flags are required to enable live trading:")
        console.print("  --live  --confirm-live  --i-understand-risks")
        console.print("\nRunning in PAPER mode instead.")

    bot = ArbBot(live=live, portfolio_value=args.portfolio)

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Bot stopped by user.[/yellow]")


if __name__ == "__main__":
    main()
