#!/usr/bin/env python3
"""
One-shot scan -- ingest all markets and events, print a summary.

Usage:
    python -m scripts.scan_once
"""

import sys
import os
import logging

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.kalshi_client import KalshiClient
from src.ingestion import ingest_all
from src import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Initialising database...")
    db.init_db()

    logger.info("Connecting to Kalshi API...")
    client = KalshiClient()

    logger.info("Running full ingestion...")
    summary = ingest_all(client)
    logger.info("Ingestion complete: %s", summary)

    # Print market summary
    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM markets WHERE status = 'open'")
            row = cur.fetchone()
            total = row["cnt"] if isinstance(row, dict) else row[0]
            logger.info("Total open markets in DB: %d", total)

            cur.execute(
                """
                SELECT category, COUNT(*) AS cnt
                FROM markets
                WHERE status = 'open'
                GROUP BY category
                ORDER BY cnt DESC
                LIMIT 20
                """
            )
            rows = cur.fetchall()
            logger.info("Markets by category:")
            for r in rows:
                cat = r["category"] if isinstance(r, dict) else r[0]
                cnt = r["cnt"] if isinstance(r, dict) else r[1]
                logger.info("  %-30s %d", cat, cnt)

            cur.execute("SELECT COUNT(*) AS cnt FROM events")
            row = cur.fetchone()
            total_events = row["cnt"] if isinstance(row, dict) else row[0]
            logger.info("Total events in DB: %d", total_events)


if __name__ == "__main__":
    main()
