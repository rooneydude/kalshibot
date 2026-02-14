"""
Universal YES+NO arbitrage scanner.

Scans ALL open markets on Kalshi for the fundamental invariant:
    yes_ask + no_ask + fees < $1.00

Architecture (optimised for speed):
  - Background thread paginates through every open market and caches
    prices in memory.  Refresh cycle is configurable (default 30s).
  - Hot loop scans the in-memory cache each cycle -- pure arithmetic,
    zero API calls, sub-millisecond.
  - When a candidate arb is found, ONE fresh get_market() call confirms
    the price before signalling the opportunity.
"""

import time
import threading
import logging
from dataclasses import dataclass

from .kalshi_client import KalshiClient
from .fees import taker_fee
from . import config

logger = logging.getLogger(__name__)


@dataclass
class ArbOpportunity:
    """A single contract where YES + NO < $1.00 after fees."""
    event_ticker: str
    ticker: str
    title: str
    yes_ask: float          # dollars (0-1)
    no_ask: float           # dollars (0-1)
    total_cost: float       # yes_ask + no_ask
    total_fees: float       # taker fees for both sides
    profit_per_contract: float  # $1.00 - total_cost - total_fees
    profit_cents: float     # profit in cents


# ---------------------------------------------------------------------------
# Background market cache
# ---------------------------------------------------------------------------

class MarketCache:
    """Background-refreshed cache of every open market on Kalshi.

    A daemon thread paginates ``GET /markets`` and atomically swaps the
    in-memory dict so the hot loop never blocks on I/O.
    """

    def __init__(self, client: KalshiClient, refresh_seconds: int = 30):
        self._client = client
        self._refresh_seconds = refresh_seconds
        self._markets: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_refresh: float = 0
        self._total_markets: int = 0

    # -- lifecycle -----------------------------------------------------------

    def start(self):
        """Synchronously load the first snapshot, then start the daemon."""
        self._do_refresh()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="market-cache",
        )
        self._thread.start()
        logger.info(
            "Market cache started (%d markets, refresh every %ds)",
            self._total_markets, self._refresh_seconds,
        )

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Market cache stopped")

    # -- public API ----------------------------------------------------------

    def snapshot(self) -> dict[str, dict]:
        """Return current cache (shallow copy for thread safety)."""
        with self._lock:
            return dict(self._markets)

    @property
    def size(self) -> int:
        return self._total_markets

    @property
    def age(self) -> float:
        """Seconds since last successful refresh."""
        if self._last_refresh == 0:
            return float("inf")
        return time.monotonic() - self._last_refresh

    # -- internals -----------------------------------------------------------

    def _loop(self):
        while not self._stop.is_set():
            self._stop.wait(self._refresh_seconds)
            if self._stop.is_set():
                break
            try:
                self._do_refresh()
            except Exception as e:
                logger.error("Market cache refresh failed: %s", e)

    def _do_refresh(self):
        start = time.monotonic()
        new_markets: dict[str, dict] = {}
        cursor = None
        pages = 0

        while True:
            try:
                data = self._client.get_markets(
                    status="open", limit=200, cursor=cursor,
                )
            except Exception:
                # Some Kalshi environments use "active" instead of "open"
                try:
                    data = self._client.get_markets(
                        status="active", limit=200, cursor=cursor,
                    )
                except Exception as e:
                    logger.error("Cannot fetch markets page %d: %s", pages + 1, e)
                    break

            markets = data.get("markets", [])
            for m in markets:
                t = m.get("ticker", "")
                if t:
                    new_markets[t] = m

            cursor = data.get("cursor")
            pages += 1
            if not cursor or not markets:
                break

        # Atomic swap
        with self._lock:
            self._markets = new_markets
        self._total_markets = len(new_markets)
        self._last_refresh = time.monotonic()

        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.info(
            "Market cache refreshed: %d markets (%d pages, %dms)",
            len(new_markets), pages, elapsed_ms,
        )


# ---------------------------------------------------------------------------
# Module-level cache singleton
# ---------------------------------------------------------------------------

_cache: MarketCache | None = None


def start_cache(client: KalshiClient):
    """Create and start the background market cache."""
    global _cache
    _cache = MarketCache(client, refresh_seconds=config.CACHE_REFRESH_SECONDS)
    _cache.start()


def stop_cache():
    """Stop the background market cache."""
    global _cache
    if _cache:
        _cache.stop()
        _cache = None


def cache_info() -> tuple[int, float]:
    """Return (market_count, age_seconds) for logging."""
    if _cache is None:
        return 0, float("inf")
    return _cache.size, _cache.age


# ---------------------------------------------------------------------------
# Hot-path scanner (no API calls)
# ---------------------------------------------------------------------------

def _normalise(price: float) -> float:
    """Kalshi may return cents (1-99) or dollars (0.01-0.99)."""
    if price > 1:
        return price / 100.0
    return price


def scan_contracts(client: KalshiClient) -> list[ArbOpportunity]:
    """Scan the in-memory cache for YES+NO arbitrage.

    Pure arithmetic -- no network I/O on the hot path.
    Candidates are verified with one fresh ``get_market()`` call each.
    """
    if _cache is None or _cache.size == 0:
        return []

    markets = _cache.snapshot()
    opportunities: list[ArbOpportunity] = []
    contracts_checked = 0

    for ticker, m in markets.items():
        yes_ask = m.get("yes_ask", 0) or 0
        no_ask = m.get("no_ask", 0) or 0

        yes_ask = _normalise(yes_ask)
        no_ask = _normalise(no_ask)

        if yes_ask <= 0 or no_ask <= 0:
            continue

        contracts_checked += 1
        total_cost = yes_ask + no_ask
        fees = taker_fee(1, yes_ask) + taker_fee(1, no_ask)
        profit = 1.00 - total_cost - fees
        profit_cents = profit * 100

        if profit_cents < config.MIN_PROFIT_CENTS:
            continue

        # ---------- candidate found -- verify with a live price ----------
        try:
            fresh = client.get_market(ticker)
            fy = _normalise(fresh.get("yes_ask", 0) or 0)
            fn = _normalise(fresh.get("no_ask", 0) or 0)

            if fy <= 0 or fn <= 0:
                logger.debug("Stale arb %s: side missing on fresh fetch", ticker)
                continue

            fresh_cost = fy + fn
            fresh_fees = taker_fee(1, fy) + taker_fee(1, fn)
            fresh_profit = 1.00 - fresh_cost - fresh_fees
            fresh_cents = fresh_profit * 100

            if fresh_cents < config.MIN_PROFIT_CENTS:
                logger.debug(
                    "Stale arb %s: cached=%.1f¢ fresh=%.1f¢",
                    ticker, profit_cents, fresh_cents,
                )
                continue

            # ---- confirmed ----
            opp = ArbOpportunity(
                event_ticker=m.get("event_ticker", ""),
                ticker=ticker,
                title=m.get("title", m.get("subtitle", "")),
                yes_ask=fy,
                no_ask=fn,
                total_cost=fresh_cost,
                total_fees=fresh_fees,
                profit_per_contract=fresh_profit,
                profit_cents=fresh_cents,
            )
            opportunities.append(opp)
            logger.info(
                "ARB CONFIRMED: %s  YES=$%.2f + NO=$%.2f = $%.4f  "
                "fees=$%.4f  profit=%.1f¢",
                ticker, fy, fn, fresh_cost, fresh_fees, fresh_cents,
            )
        except Exception as e:
            logger.warning("Fresh price fetch failed for %s: %s", ticker, e)

    opportunities.sort(key=lambda o: o.profit_cents, reverse=True)
    return opportunities
