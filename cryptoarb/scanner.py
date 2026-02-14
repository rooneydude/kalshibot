"""
YES+NO arbitrage scanner.

For each individual crypto contract, checks if:
    yes_ask + no_ask + fees < $1.00

If so, buying both YES and NO guarantees profit since exactly one
settles at $1.00.

Optimised for speed: caches the crypto event list and only fetches
fresh market prices for those events each cycle.
"""

import time
import logging
from dataclasses import dataclass

from .kalshi_client import KalshiClient
from .fees import taker_fee
from . import config

logger = logging.getLogger(__name__)

# Cached crypto event tickers -- refreshed every EVENT_REFRESH_SECONDS
_cached_event_tickers: list[str] = []
_last_event_refresh: float = 0
EVENT_REFRESH_SECONDS = 60  # re-discover new events every 60s


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


def _refresh_event_list(client: KalshiClient) -> list[str]:
    """Fetch all events and return just the crypto event tickers."""
    global _cached_event_tickers, _last_event_refresh

    try:
        all_events = client.get_all_events(status="open")
    except Exception:
        try:
            all_events = client.get_all_events(status="active")
        except Exception as e:
            logger.error("Cannot fetch events: %s", e)
            return _cached_event_tickers  # return stale cache

    crypto_tickers = [
        e.get("event_ticker", "")
        for e in all_events
        if any(e.get("event_ticker", "").startswith(prefix)
               for prefix in config.CRYPTO_EVENT_PREFIXES)
    ]

    _cached_event_tickers = crypto_tickers
    _last_event_refresh = time.monotonic()
    logger.info("Refreshed event list: %d crypto events (%d total)",
                len(crypto_tickers), len(all_events))
    return crypto_tickers


def scan_contracts(client: KalshiClient) -> list[ArbOpportunity]:
    """Scan all crypto contracts for YES+NO arbitrage.

    Fast path: only fetches markets for known crypto events.
    Event list is refreshed every 60 seconds.
    """
    global _cached_event_tickers, _last_event_refresh

    # Refresh event list if stale or empty
    now = time.monotonic()
    if not _cached_event_tickers or (now - _last_event_refresh) > EVENT_REFRESH_SECONDS:
        _refresh_event_list(client)

    event_tickers = _cached_event_tickers
    if not event_tickers:
        return []

    opportunities: list[ArbOpportunity] = []
    contracts_checked = 0

    # Fetch fresh market prices for each crypto event directly
    for evt in event_tickers:
        try:
            resp = client.get_markets(event_ticker=evt, status="open")
            markets = resp.get("markets", [])
        except Exception:
            try:
                resp = client.get_markets(event_ticker=evt, status="active")
                markets = resp.get("markets", [])
            except Exception:
                continue

        for m in markets:
            ticker = m.get("ticker", "")
            yes_ask = m.get("yes_ask", 0) or 0
            no_ask = m.get("no_ask", 0) or 0

            # Kalshi API may return prices in cents (0-100); normalise to dollars
            if yes_ask > 1:
                yes_ask /= 100.0
            if no_ask > 1:
                no_ask /= 100.0

            # Need both sides to have an ask
            if yes_ask <= 0 or no_ask <= 0:
                continue

            contracts_checked += 1
            total_cost = yes_ask + no_ask
            fees = taker_fee(1, yes_ask) + taker_fee(1, no_ask)
            profit = 1.00 - total_cost - fees
            profit_cents = profit * 100

            if profit_cents >= config.MIN_PROFIT_CENTS:
                opp = ArbOpportunity(
                    event_ticker=evt,
                    ticker=ticker,
                    title=m.get("title", m.get("subtitle", "")),
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    total_cost=total_cost,
                    total_fees=fees,
                    profit_per_contract=profit,
                    profit_cents=profit_cents,
                )
                opportunities.append(opp)
                logger.info(
                    "ARB FOUND: %s  YES=$%.2f + NO=$%.2f = $%.4f  fees=$%.4f  profit=%.1f¢",
                    ticker, yes_ask, no_ask, total_cost, fees, profit_cents,
                )

    opportunities.sort(key=lambda o: o.profit_cents, reverse=True)

    if opportunities:
        logger.info("Found %d arb opportunities across %d contracts",
                    len(opportunities), contracts_checked)
    else:
        logger.debug("No arb (%d contracts, %d events)", contracts_checked, len(event_tickers))

    return opportunities
