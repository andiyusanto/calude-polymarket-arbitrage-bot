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
    min_edge_pct: float = 5.0          # Minimum edge to trade (%)
    lag_threshold_pct: float = 3.0     # Polymarket lag vs CEX to flag (%)
    max_position_pct: float = 8.0      # Max position as % of portfolio
    confidence_threshold: float = 85.0 # Min confidence score to trade
    kelly_fraction: float = 0.5        # Half-Kelly
    kill_switch_drawdown: float = 20.0 # Daily drawdown % to halt all trading
    max_slippage_pct: float = 1.5      # Max acceptable VWAP slippage vs best price (%)

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
    STREAMS = "btcusdt@ticker/ethusdt@ticker"

    def __init__(self):
        self.prices: dict[str, float] = {"BTC": 0.0, "ETH": 0.0}
        self._prev: dict[str, float] = {"BTC": 0.0, "ETH": 0.0}
        self._running = False
        self._callbacks: list = []

    def on_update(self, cb):
        self._callbacks.append(cb)

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
                            if "btcusdt" in stream:
                                asset = "BTC"
                            elif "ethusdt" in stream:
                                asset = "ETH"
                            else:
                                continue
                            new_price = float(data.get("c", 0))
                            if new_price <= 0:
                                continue
                            snap = PriceSnapshot(
                                asset=asset,
                                price=new_price,
                                prev_price=self.prices[asset],
                            )
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
        """Fetch active markets from CLOB and match against BTC/ETH up/down keywords."""
        log.info("Discovering active Polymarket markets…")
        try:
            client = self._get_client()
            loop = asyncio.get_event_loop()
            # get_markets returns paginated results; fetch first page
            resp = await loop.run_in_executor(None, lambda: client.get_markets())
            markets = resp.data if hasattr(resp, "data") else (resp if isinstance(resp, list) else [])

            found: dict[str, dict] = {}
            for m in markets:
                question = (getattr(m, "question", "") or "").lower()
                tokens = getattr(m, "tokens", []) or []
                for token in tokens:
                    token_id = getattr(token, "token_id", None)
                    outcome = (getattr(token, "outcome", "") or "").lower()
                    if not token_id:
                        continue
                    for asset, direction, duration, keywords in self.SEARCH_TERMS:
                        asset_kw = keywords[:2]   # e.g. ["btc", "bitcoin"]
                        dir_kw   = keywords[2]     # e.g. "higher"
                        dur_kws  = keywords[3:]    # e.g. ["5 min", "5min"]
                        if (any(k in question for k in asset_kw) and
                                dir_kw in question and
                                any(k in question for k in dur_kws) and
                                "yes" in outcome):
                            found[token_id] = {"asset": asset, "direction": direction, "duration": duration}
                            log.info("Discovered market: %s → %s %s %s", token_id[:16], asset, direction, duration)

            if found:
                self.market_ids = found
                log.info("Discovered %d markets", len(found))
            else:
                log.warning("No markets matched search terms — check Polymarket for active BTC/ETH up/down markets")
                self.market_ids = self.FALLBACK_MARKET_IDS

        except Exception as e:
            log.error("Market discovery failed: %s — using fallback IDs", e)
            self.market_ids = self.FALLBACK_MARKET_IDS

    def on_snapshot(self, cb):
        self._callbacks.append(cb)

    async def _fetch_market(self, market_id: str, meta: dict) -> Optional[MarketSnapshot]:
        for attempt in range(CONFIG.max_retries):
            try:
                client = self._get_client()
                loop = asyncio.get_event_loop()
                # py-clob-client is synchronous; run in executor
                book = await loop.run_in_executor(
                    None, lambda: client.get_order_book(market_id)
                )
                # Best ask = cheapest YES token offer
                best_ask = float(book.asks[0].price) if book.asks else 0.5
                best_bid = float(book.bids[0].price) if book.bids else 0.5
                yes_price = (best_ask + best_bid) / 2

                # Preserve raw levels for slippage analysis
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
                backoff = 2 ** attempt
                log.warning("Polymarket fetch failed for %s (attempt %d): %s – retry in %ds",
                            market_id, attempt + 1, e, backoff)
                await asyncio.sleep(backoff)
        return None

    async def run(self, poll_interval: float = 2.0):
        self._running = True
        await self._discover_markets()
        while self._running:
            if not self.market_ids:
                log.warning("No market IDs — retrying discovery in 30s")
                await asyncio.sleep(30)
                await self._discover_markets()
                continue
            tasks = [
                self._fetch_market(mid, meta)
                for mid, meta in self.market_ids.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for snap in results:
                if isinstance(snap, MarketSnapshot):
                    self.snapshots[snap.market_id] = snap
                    for cb in self._callbacks:
                        asyncio.create_task(cb(snap))
            await asyncio.sleep(poll_interval)

    def stop(self):
        self._running = False


# ═══════════════════════════════════════════════════════════════════════════
# Signal engine
# ═══════════════════════════════════════════════════════════════════════════

class SignalEngine:
    """
    Computes fair value from CEX prices and detects lag opportunities.
    """

    def __init__(self, price_feed: BinancePriceFeed):
        self.feed = price_feed
        self._price_history: dict[str, list[float]] = {"BTC": [], "ETH": []}

    def record_price(self, asset: str, price: float):
        hist = self._price_history[asset]
        hist.append(price)
        if len(hist) > 300:   # keep last 300 ticks
            hist.pop(0)

    def _compute_fair_value(self, asset: str, direction: str, duration: str) -> tuple[float, float]:
        """
        Returns (fair_value_prob, confidence_score).
        Uses recent price momentum to estimate UP probability.
        """
        hist = self._price_history[asset]
        current = self.feed.prices.get(asset, 0)
        if current == 0 or len(hist) < 10:
            return 0.5, 0.0

        # Short-term momentum signal
        window = 20 if duration == "5min" else 60
        lookback = hist[-min(window, len(hist)):]
        start = lookback[0]
        if start == 0:
            return 0.5, 0.0

        momentum_pct = (current - start) / start * 100

        # Sigmoid-like mapping: strong upward momentum → higher UP probability
        import math
        scale = 2.0  # sensitivity
        raw_prob = 1 / (1 + math.exp(-momentum_pct * scale / 100 * 10))

        if direction == "DOWN":
            fair_value = 1.0 - raw_prob
        else:
            fair_value = raw_prob

        # Confidence: how many ticks we have, how extreme the momentum
        tick_confidence = min(len(hist) / window, 1.0) * 60
        momentum_confidence = min(abs(momentum_pct) / 2.0, 1.0) * 40
        confidence = tick_confidence + momentum_confidence

        return fair_value, confidence

    def evaluate(self, snap: MarketSnapshot) -> Optional[TradeSignal]:
        fair_value, confidence = self._compute_fair_value(
            snap.asset, snap.direction, snap.duration
        )
        if confidence < CONFIG.confidence_threshold:
            return None

        # YES side
        yes_edge = (fair_value - snap.yes_price) * 100
        no_edge = ((1 - fair_value) - snap.no_price) * 100

        if yes_edge >= CONFIG.min_edge_pct and abs(yes_edge) > abs(no_edge):
            side = "YES"
            poly_price = snap.yes_price
            edge = yes_edge
        elif no_edge >= CONFIG.min_edge_pct:
            side = "NO"
            poly_price = snap.no_price
            edge = no_edge
        else:
            return None

        # Check lag threshold
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
        )


# ═══════════════════════════════════════════════════════════════════════════
# Position sizer (half-Kelly)
# ═══════════════════════════════════════════════════════════════════════════

class KellySizer:
    def __init__(self, portfolio_value: float = 1000.0):
        self.portfolio_value = portfolio_value

    def size(self, sig: TradeSignal) -> float:
        """Returns USDC size to trade."""
        p = sig.fair_value
        b = (1.0 / sig.poly_price) - 1.0  # net odds
        if b <= 0 or p <= 0 or p >= 1:
            return 0.0

        # Kelly fraction
        kelly = (p * b - (1 - p)) / b
        half_kelly = kelly * CONFIG.kelly_fraction

        # Cap at max position %
        capped = min(half_kelly, CONFIG.max_position_pct / 100)
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

    async def execute(self, sig: TradeSignal, size_usdc: float) -> Optional[Position]:
        # Rate limit
        elapsed = time.time() - self._last_order_time
        if elapsed < CONFIG.order_cooldown_sec:
            await asyncio.sleep(CONFIG.order_cooldown_sec - elapsed)

        trade_id = f"{sig.market_id}-{int(time.time()*1000)}"

        if not self.live:
            # Paper trade
            pos = Position(
                trade_id=trade_id,
                market_id=sig.market_id,
                asset=sig.asset,
                direction=sig.direction,
                side=sig.side,
                entry_price=sig.poly_price,
                size_usdc=size_usdc,
                fair_value_at_entry=sig.fair_value,
                edge_at_entry=sig.edge_pct,
                status="PAPER",
            )
            log.info("[PAPER] %s %s %s @ %.4f  size=$%.2f  edge=%.1f%%",
                     sig.side, sig.asset, sig.direction,
                     sig.poly_price, size_usdc, sig.edge_pct)
            self._last_order_time = time.time()
            return pos

        # Live order
        for attempt in range(CONFIG.max_retries):
            try:
                client = self._get_client()
                loop = asyncio.get_event_loop()
                order_args = OrderArgs(
                    token_id=sig.market_id,
                    price=sig.poly_price,
                    size=size_usdc,
                    side="BUY",
                )
                resp = await loop.run_in_executor(
                    None,
                    lambda: client.create_and_post_order(order_args),
                )
                log.info("[LIVE] Order placed: %s", resp)
                self._last_order_time = time.time()
                return Position(
                    trade_id=trade_id,
                    market_id=sig.market_id,
                    asset=sig.asset,
                    direction=sig.direction,
                    side=sig.side,
                    entry_price=sig.poly_price,
                    size_usdc=size_usdc,
                    fair_value_at_entry=sig.fair_value,
                    edge_at_entry=sig.edge_pct,
                    status="OPEN",
                )
            except Exception as e:
                backoff = 2 ** attempt
                log.error("Order failed (attempt %d): %s – retry in %ds", attempt + 1, e, backoff)
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

        layout = Layout()
        layout.split_column(
            Layout(Panel(header), size=3),
            Layout(Panel(price_line), size=3),
            Layout(Panel(stats_line), size=3),
            Layout(pos_table, size=10),
            Layout(trades_table),
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

        size = self.sizer.size(sig)
        if size < 1.0:   # minimum $1
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
            pos = await self.executor.execute(sig, size)
            if pos:
                self._positions.append(pos)
                self.dashboard.positions = self._positions
                mode = "LIVE" if self.live else "PAPER"
                self.db.insert_trade(pos, mode)
                acted = True
                msg = (
                    f"{'🟢' if self.live else '📋'} *{'LIVE' if self.live else 'PAPER'} TRADE*\n"
                    f"Market: {sig.asset} {sig.direction} {sig.duration}\n"
                    f"Side: {sig.side} @ best={slip.best_price:.4f} VWAP={slip.vwap:.4f}\n"
                    f"Slippage: {slip.slippage_pct:.2f}%  Edge: {sig.edge_pct:.1f}%\n"
                    f"Fair Value: {sig.fair_value:.4f}  Confidence: {sig.confidence:.0f}%\n"
                    f"Size: ${size:.2f}"
                )
                await self.notifier.send(msg)

        self.db.log_signal(sig, acted)

    async def run(self):
        mode_str = "LIVE TRADING" if self.live else "PAPER TRADING"
        log.info("Starting Polymarket Arb Bot in %s mode", mode_str)

        if self.live:
            log.warning("⚠️  LIVE TRADING ENABLED – real funds at risk")

        # Wire callbacks
        self.price_feed.on_update(self._on_price_update)
        self.poly_monitor.on_snapshot(self._on_market_snapshot)

        # Start tasks
        tasks = [
            asyncio.create_task(self.price_feed.run(), name="binance_ws"),
            asyncio.create_task(self.poly_monitor.run(), name="poly_monitor"),
            asyncio.create_task(self._dashboard_loop(), name="dashboard"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Shutting down…")
        finally:
            self.price_feed.stop()
            self.poly_monitor.stop()
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
