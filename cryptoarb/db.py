"""
Database module for the crypto arb bot.

Uses the shared PostgreSQL database but with its own tables:
    - arb_scans:  log of each scan cycle
    - arb_trades: individual order legs placed
"""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from psycopg2 import pool

from . import config

logger = logging.getLogger(__name__)

_pool: pool.SimpleConnectionPool | None = None


def _get_pool() -> pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(1, 5, config.DATABASE_URL)
        logger.info("Crypto-arb DB pool created")
    return _pool


@contextmanager
def get_conn():
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def init_db():
    """Create arb-specific tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS arb_scans (
                    id              SERIAL PRIMARY KEY,
                    event_ticker    TEXT NOT NULL,
                    num_markets     INTEGER NOT NULL,
                    total_ask       REAL NOT NULL,
                    total_fees      REAL NOT NULL,
                    profit_cents    REAL NOT NULL,
                    acted           BOOLEAN DEFAULT FALSE,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS arb_trades (
                    id              SERIAL PRIMARY KEY,
                    scan_id         INTEGER REFERENCES arb_scans(id),
                    event_ticker    TEXT NOT NULL,
                    ticker          TEXT NOT NULL,
                    side            TEXT NOT NULL DEFAULT 'yes',
                    action          TEXT NOT NULL DEFAULT 'buy',
                    price           REAL NOT NULL,
                    count           INTEGER NOT NULL,
                    order_id        TEXT,
                    order_status    TEXT,
                    fees            REAL DEFAULT 0,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_arb_scans_event ON arb_scans(event_ticker);
                CREATE INDEX IF NOT EXISTS idx_arb_scans_profit ON arb_scans(profit_cents DESC);
                CREATE INDEX IF NOT EXISTS idx_arb_trades_scan ON arb_trades(scan_id);
            """)
    logger.info("Crypto-arb DB tables initialised")


def log_scan(event_ticker: str, num_markets: int, total_ask: float,
             total_fees: float, profit_cents: float, acted: bool = False) -> int:
    """Log a scan result. Returns the scan ID."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO arb_scans (event_ticker, num_markets, total_ask, total_fees, profit_cents, acted)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (event_ticker, num_markets, total_ask, total_fees, profit_cents, acted))
            return cur.fetchone()[0]


def log_trade(scan_id: int, event_ticker: str, ticker: str, side: str,
              price: float, count: int, order_id: str, order_status: str,
              fees: float) -> int:
    """Log an individual trade leg (YES or NO)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO arb_trades
                    (scan_id, event_ticker, ticker, side, action, price, count, order_id, order_status, fees)
                VALUES (%s, %s, %s, %s, 'buy', %s, %s, %s, %s, %s)
                RETURNING id
            """, (scan_id, event_ticker, ticker, side, price, count, order_id, order_status, fees))
            return cur.fetchone()[0]


def mark_scan_acted(scan_id: int):
    """Mark a scan as acted upon."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE arb_scans SET acted = TRUE WHERE id = %s", (scan_id,))


def get_recent_scans(limit: int = 20) -> list[dict]:
    """Fetch recent profitable scans."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM arb_scans
                WHERE profit_cents > 0
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]


def get_trades_for_scan(scan_id: int) -> list[dict]:
    """Fetch all trade legs for a scan."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM arb_trades WHERE scan_id = %s ORDER BY id", (scan_id,))
            return [dict(r) for r in cur.fetchall()]
