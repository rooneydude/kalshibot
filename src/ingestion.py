"""
Layer 1 -- Market Ingestion.

Pulls all active markets and events from the Kalshi API and caches
them in PostgreSQL. Records price snapshots for historical tracking.

Uses batch inserts for performance (66k+ markets on Kalshi).
"""

import json
import logging
from datetime import datetime, timezone

from psycopg2.extras import execute_values

from src.kalshi_client import KalshiClient
from src import db

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


def ingest_markets(client: KalshiClient) -> int:
    """Pull all open markets and upsert into the database.

    Uses batch upserts for performance.
    Returns the number of markets processed.
    """
    markets = client.get_all_markets(status="open")
    now_iso = datetime.now(timezone.utc).isoformat()
    count = 0

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            # Batch upsert markets
            for i in range(0, len(markets), BATCH_SIZE):
                batch = markets[i : i + BATCH_SIZE]
                values = []
                for m in batch:
                    values.append((
                        m.get("ticker"),
                        m.get("event_ticker"),
                        m.get("title"),
                        m.get("subtitle"),
                        m.get("category"),
                        m.get("status"),
                        _cents_to_dollars(m.get("yes_ask")),
                        _cents_to_dollars(m.get("yes_bid")),
                        _cents_to_dollars(m.get("no_ask")),
                        _cents_to_dollars(m.get("no_bid")),
                        m.get("volume", 0),
                        m.get("open_interest", 0),
                        m.get("close_time") or m.get("expiration_time"),
                        m.get("rules_primary", ""),
                        now_iso,
                    ))

                execute_values(
                    cur,
                    """
                    INSERT INTO markets (ticker, event_ticker, title, subtitle, category,
                                         status, yes_ask, yes_bid, no_ask, no_bid,
                                         volume, open_interest, close_time, rules_primary, updated_at)
                    VALUES %s
                    ON CONFLICT (ticker) DO UPDATE SET
                        event_ticker  = EXCLUDED.event_ticker,
                        title         = EXCLUDED.title,
                        subtitle      = EXCLUDED.subtitle,
                        category      = EXCLUDED.category,
                        status        = EXCLUDED.status,
                        yes_ask       = EXCLUDED.yes_ask,
                        yes_bid       = EXCLUDED.yes_bid,
                        no_ask        = EXCLUDED.no_ask,
                        no_bid        = EXCLUDED.no_bid,
                        volume        = EXCLUDED.volume,
                        open_interest = EXCLUDED.open_interest,
                        close_time    = EXCLUDED.close_time,
                        rules_primary = EXCLUDED.rules_primary,
                        updated_at    = EXCLUDED.updated_at
                    """,
                    values,
                    page_size=BATCH_SIZE,
                )
                count += len(batch)
                if count % 10000 == 0:
                    logger.info("Markets upserted: %d / %d", count, len(markets))

            # Batch insert price snapshots (skip markets with no price data)
            snapshot_values = []
            for m in markets:
                ya = _cents_to_dollars(m.get("yes_ask"))
                yb = _cents_to_dollars(m.get("yes_bid"))
                if ya is not None and yb is not None:
                    snapshot_values.append((m.get("ticker"), ya, yb, now_iso))

            for i in range(0, len(snapshot_values), BATCH_SIZE):
                batch = snapshot_values[i : i + BATCH_SIZE]
                execute_values(
                    cur,
                    """
                    INSERT INTO price_snapshots (ticker, yes_ask, yes_bid, snapshot_time)
                    VALUES %s
                    """,
                    batch,
                    page_size=BATCH_SIZE,
                )

    logger.info("Ingested %d markets (%d snapshots)", count, len(snapshot_values))
    return count


def ingest_events(client: KalshiClient) -> int:
    """Pull all open events and cache them.

    Returns the number of events processed.
    """
    events = client.get_all_events(status="open")
    count = 0
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            values = []
            for e in events:
                market_tickers = e.get("markets", [])
                if market_tickers and isinstance(market_tickers[0], dict):
                    market_tickers = [mk.get("ticker") for mk in market_tickers]

                values.append((
                    e.get("event_ticker"),
                    e.get("title"),
                    e.get("category"),
                    json.dumps(market_tickers),
                ))

            for i in range(0, len(values), BATCH_SIZE):
                batch = values[i : i + BATCH_SIZE]
                execute_values(
                    cur,
                    """
                    INSERT INTO events (event_ticker, title, category, market_tickers)
                    VALUES %s
                    ON CONFLICT (event_ticker) DO UPDATE SET
                        title          = EXCLUDED.title,
                        category       = EXCLUDED.category,
                        market_tickers = EXCLUDED.market_tickers
                    """,
                    batch,
                    page_size=BATCH_SIZE,
                )
                count += len(batch)

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
