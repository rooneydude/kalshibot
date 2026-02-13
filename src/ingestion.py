"""
Layer 1 -- Market Ingestion.

Pulls all active markets and events from the Kalshi API and caches
them in PostgreSQL. Records price snapshots for historical tracking.
"""

import json
import logging

from src.kalshi_client import KalshiClient
from src import db

logger = logging.getLogger(__name__)


def ingest_markets(client: KalshiClient) -> int:
    """Pull all open markets and upsert into the database.

    Returns the number of markets processed.
    """
    markets = client.get_all_markets(status="open")
    count = 0
    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            for m in markets:
                # Kalshi returns prices in cents (integer); normalise to dollars
                market_row = {
                    "ticker": m.get("ticker"),
                    "event_ticker": m.get("event_ticker"),
                    "title": m.get("title"),
                    "subtitle": m.get("subtitle"),
                    "category": m.get("category"),
                    "status": m.get("status"),
                    "yes_ask": _cents_to_dollars(m.get("yes_ask")),
                    "yes_bid": _cents_to_dollars(m.get("yes_bid")),
                    "no_ask": _cents_to_dollars(m.get("no_ask")),
                    "no_bid": _cents_to_dollars(m.get("no_bid")),
                    "volume": m.get("volume", 0),
                    "open_interest": m.get("open_interest", 0),
                    "close_time": m.get("close_time") or m.get("expiration_time"),
                    "rules_primary": m.get("rules_primary", ""),
                }
                db.upsert_market(cur, market_row)

                # Price snapshot
                if market_row["yes_ask"] is not None and market_row["yes_bid"] is not None:
                    db.insert_price_snapshot(
                        cur,
                        market_row["ticker"],
                        market_row["yes_ask"],
                        market_row["yes_bid"],
                    )
                count += 1

    logger.info("Ingested %d markets", count)
    return count


def ingest_events(client: KalshiClient) -> int:
    """Pull all open events and cache them.

    Returns the number of events processed.
    """
    events = client.get_all_events(status="open")
    count = 0
    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            for e in events:
                event_row = {
                    "event_ticker": e.get("event_ticker"),
                    "title": e.get("title"),
                    "category": e.get("category"),
                    "market_tickers": e.get("markets", []),
                }
                # If the API returns full market objects, extract just tickers
                if event_row["market_tickers"] and isinstance(event_row["market_tickers"][0], dict):
                    event_row["market_tickers"] = [
                        mk.get("ticker") for mk in event_row["market_tickers"]
                    ]
                db.upsert_event(cur, event_row)
                count += 1

    logger.info("Ingested %d events", count)
    return count


def ingest_all(client: KalshiClient) -> dict:
    """Run full ingestion of markets and events.

    Returns a summary dict.
    """
    n_markets = ingest_markets(client)
    n_events = ingest_events(client)
    return {"markets": n_markets, "events": n_events}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cents_to_dollars(value) -> float | None:
    """Convert Kalshi's cent-based prices to dollars (0.0 – 1.0)."""
    if value is None:
        return None
    return round(int(value) / 100.0, 2)
