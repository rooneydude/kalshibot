"""
Partition scanner -- finds crypto events where buying all YES sides
costs less than $1.00 after fees, guaranteeing a profit.
"""

import logging
from dataclasses import dataclass, field

from .kalshi_client import KalshiClient
from .fees import taker_fee, total_partition_fees
from . import config

logger = logging.getLogger(__name__)


@dataclass
class ArbOpportunity:
    """A profitable partition arbitrage."""
    event_ticker: str
    markets: list[dict] = field(default_factory=list)   # [{ticker, yes_ask, ...}, ...]
    total_ask: float = 0.0          # sum of YES ask prices (dollars)
    total_fees: float = 0.0         # total taker fees (dollars)
    profit_per_set: float = 0.0     # $1.00 - total_ask - total_fees
    profit_cents: float = 0.0       # profit in cents

    @property
    def num_markets(self) -> int:
        return len(self.markets)


def scan_partitions(client: KalshiClient) -> list[ArbOpportunity]:
    """Scan all crypto partition events for arbitrage.

    For each event:
        1. Fetch all child markets
        2. Sum the YES ask prices
        3. Calculate fees for buying 1 contract of each
        4. If profit > min_profit_cents, flag it

    Returns a list of profitable ArbOpportunity objects.
    """
    opportunities: list[ArbOpportunity] = []

    # Fetch all active events, filter to crypto partitions
    try:
        all_events = client.get_all_events(status="open")
    except Exception:
        logger.warning("Failed to fetch events with status=open, trying status=active")
        all_events = client.get_all_events(status="active")

    crypto_events = [
        e for e in all_events
        if any(e.get("event_ticker", "").startswith(prefix)
               for prefix in config.CRYPTO_EVENT_PREFIXES)
    ]

    logger.info("Found %d crypto partition events out of %d total",
                len(crypto_events), len(all_events))

    for event in crypto_events:
        evt = event.get("event_ticker", "")
        try:
            opp = _check_event(client, evt, event)
            if opp and opp.profit_cents >= config.MIN_PROFIT_CENTS:
                opportunities.append(opp)
                logger.info(
                    "ARB FOUND: %s  %d markets  total_ask=$%.4f  fees=$%.4f  profit=%.1f¢",
                    evt, opp.num_markets, opp.total_ask, opp.total_fees, opp.profit_cents,
                )
        except Exception as e:
            logger.warning("Error checking event %s: %s", evt, e)

    logger.info("Scan complete: %d profitable partitions found", len(opportunities))
    return opportunities


def _check_event(client: KalshiClient, event_ticker: str,
                 event_data: dict) -> ArbOpportunity | None:
    """Check a single event for partition arbitrage.

    Returns an ArbOpportunity if profitable, else None.
    """
    # Get markets for this event -- try from event data first, else fetch
    markets_data = event_data.get("markets", [])

    if not markets_data:
        # Fetch markets by event_ticker
        try:
            resp = client.get_markets(event_ticker=event_ticker, status="open")
            markets_data = resp.get("markets", [])
        except Exception:
            pass
        if not markets_data:
            try:
                resp = client.get_markets(event_ticker=event_ticker, status="active")
                markets_data = resp.get("markets", [])
            except Exception:
                pass

    if len(markets_data) < 2:
        # Not a partition (need at least 2 outcomes)
        return None

    # Extract YES ask prices
    market_legs = []
    for m in markets_data:
        ticker = m.get("ticker", "")
        # yes_ask can be in dollars (0-1) or cents (0-100) depending on API version
        yes_ask = m.get("yes_ask", 0) or 0

        # Kalshi API returns prices in cents (0-100); convert to dollars
        if yes_ask > 1:
            yes_ask = yes_ask / 100.0

        if yes_ask <= 0:
            # No ask available -- can't buy this side, so partition arb is impossible
            # (unless we use the orderbook for a better price)
            # For now, try the orderbook
            try:
                book = client.get_orderbook(ticker)
                asks = book.get("orderbook", {}).get("yes", [])
                if asks:
                    # asks are [[price_cents, quantity], ...] sorted by price asc
                    best_ask = asks[0][0] / 100.0 if asks[0][0] > 1 else asks[0][0]
                    yes_ask = best_ask
            except Exception:
                pass

        if yes_ask <= 0:
            # Still no price -- skip this event
            return None

        market_legs.append({
            "ticker": ticker,
            "yes_ask": yes_ask,
            "title": m.get("title", ""),
        })

    # Calculate totals
    prices = [leg["yes_ask"] for leg in market_legs]
    total_ask = sum(prices)
    fees = total_partition_fees(1, prices)
    profit = 1.00 - total_ask - fees
    profit_cents = profit * 100

    if profit_cents < config.MIN_PROFIT_CENTS:
        return None

    return ArbOpportunity(
        event_ticker=event_ticker,
        markets=market_legs,
        total_ask=total_ask,
        total_fees=fees,
        profit_per_set=profit,
        profit_cents=profit_cents,
    )
