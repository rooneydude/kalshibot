"""
YES+NO arbitrage scanner.

For each individual crypto contract, checks if:
    yes_ask + no_ask + fees < $1.00

If so, buying both YES and NO guarantees profit since exactly one
settles at $1.00.
"""

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


def scan_contracts(client: KalshiClient) -> list[ArbOpportunity]:
    """Scan all crypto contracts for YES+NO arbitrage.

    For each contract:
        1. Get yes_ask and no_ask
        2. If both > 0, calculate total cost + fees
        3. If $1.00 - cost - fees > min_profit, flag it

    Returns profitable ArbOpportunity objects sorted by profit desc.
    """
    opportunities: list[ArbOpportunity] = []

    # Fetch all active events
    try:
        all_events = client.get_all_events(status="open")
    except Exception:
        logger.warning("Failed with status=open, trying status=active")
        try:
            all_events = client.get_all_events(status="active")
        except Exception as e:
            logger.error("Cannot fetch events: %s", e)
            return []

    # Filter to crypto events
    crypto_events = [
        e for e in all_events
        if any(e.get("event_ticker", "").startswith(prefix)
               for prefix in config.CRYPTO_EVENT_PREFIXES)
    ]

    logger.info("Scanning %d crypto events (%d total) for YES+NO arb",
                len(crypto_events), len(all_events))

    contracts_checked = 0

    for event in crypto_events:
        evt = event.get("event_ticker", "")

        # Get markets from event data or fetch separately
        markets = event.get("markets", [])
        if not markets:
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
        logger.info("No arb found (%d contracts checked, %d events)",
                    contracts_checked, len(crypto_events))

    return opportunities
