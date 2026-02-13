"""
Database module -- PostgreSQL connection pooling, schema management, and query helpers.

Reads DATABASE_URL from environment (auto-provided by Railway Postgres addon).
"""

import os
import json
import logging
from datetime import datetime, timezone
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import psycopg2.extras

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

_pool = None


def get_pool():
    """Return the global connection pool, creating it on first call."""
    global _pool
    if _pool is None:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=database_url,
        )
        logger.info("PostgreSQL connection pool created")
    return _pool


@contextmanager
def get_conn():
    """Context manager that checks out a connection and returns it on exit."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


@contextmanager
def get_cursor(conn=None):
    """Context manager for a dict-cursor. If *conn* is None a fresh one is checked out."""
    if conn is not None:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
        finally:
            cur.close()
    else:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                yield cur
            finally:
                cur.close()


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS markets (
    ticker          TEXT PRIMARY KEY,
    event_ticker    TEXT,
    title           TEXT,
    subtitle        TEXT,
    category        TEXT,
    status          TEXT,
    yes_ask         REAL,
    yes_bid         REAL,
    no_ask          REAL,
    no_bid          REAL,
    volume          INTEGER,
    open_interest   INTEGER,
    close_time      TEXT,
    rules_primary   TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id              SERIAL PRIMARY KEY,
    ticker          TEXT REFERENCES markets(ticker) ON DELETE CASCADE,
    yes_ask         REAL,
    yes_bid         REAL,
    snapshot_time   TEXT
);
CREATE INDEX IF NOT EXISTS idx_snapshots_ticker ON price_snapshots(ticker);

CREATE TABLE IF NOT EXISTS events (
    event_ticker    TEXT PRIMARY KEY,
    title           TEXT,
    category        TEXT,
    market_tickers  TEXT   -- JSON array
);

CREATE TABLE IF NOT EXISTS relationships (
    id                      SERIAL PRIMARY KEY,
    type                    TEXT,
    tickers                 TEXT,   -- JSON array
    constraint_description  TEXT,
    constraint_formula      TEXT,
    confidence              REAL,
    reasoning               TEXT,
    last_validated          TEXT,
    created_at              TEXT
);

CREATE TABLE IF NOT EXISTS opportunities (
    id                  SERIAL PRIMARY KEY,
    relationship_id     INTEGER REFERENCES relationships(id) ON DELETE SET NULL,
    signal              TEXT,
    magnitude           REAL,
    confidence          REAL,
    score               REAL,
    legs                TEXT,   -- JSON array
    status              TEXT DEFAULT 'DETECTED',
    detected_at         TEXT,
    expires_at          TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    id                  SERIAL PRIMARY KEY,
    opportunity_id      INTEGER REFERENCES opportunities(id) ON DELETE SET NULL,
    ticker              TEXT,
    side                TEXT,
    action              TEXT,
    price               REAL,
    count               INTEGER,
    order_id            TEXT,
    order_status        TEXT,
    filled_count        INTEGER DEFAULT 0,
    fees                REAL DEFAULT 0,
    created_at          TEXT,
    updated_at          TEXT
);

CREATE TABLE IF NOT EXISTS portfolio_state (
    id                  SERIAL PRIMARY KEY,
    balance             REAL,
    daily_pnl           REAL DEFAULT 0,
    open_positions      INTEGER DEFAULT 0,
    kill_switch         BOOLEAN DEFAULT FALSE,
    last_updated        TEXT
);
"""


def init_db():
    """Create all tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    logger.info("Database schema initialised")


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def upsert_market(cur, market: dict):
    """Insert or update a market row from Kalshi API data."""
    cur.execute(
        """
        INSERT INTO markets (ticker, event_ticker, title, subtitle, category,
                             status, yes_ask, yes_bid, no_ask, no_bid,
                             volume, open_interest, close_time, rules_primary, updated_at)
        VALUES (%(ticker)s, %(event_ticker)s, %(title)s, %(subtitle)s, %(category)s,
                %(status)s, %(yes_ask)s, %(yes_bid)s, %(no_ask)s, %(no_bid)s,
                %(volume)s, %(open_interest)s, %(close_time)s, %(rules_primary)s, %(updated_at)s)
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
        {
            "ticker": market.get("ticker"),
            "event_ticker": market.get("event_ticker"),
            "title": market.get("title"),
            "subtitle": market.get("subtitle"),
            "category": market.get("category"),
            "status": market.get("status"),
            "yes_ask": market.get("yes_ask"),
            "yes_bid": market.get("yes_bid"),
            "no_ask": market.get("no_ask"),
            "no_bid": market.get("no_bid"),
            "volume": market.get("volume"),
            "open_interest": market.get("open_interest"),
            "close_time": market.get("close_time"),
            "rules_primary": market.get("rules_primary"),
            "updated_at": _now_iso(),
        },
    )


def insert_price_snapshot(cur, ticker: str, yes_ask: float, yes_bid: float):
    """Record a price snapshot."""
    cur.execute(
        """
        INSERT INTO price_snapshots (ticker, yes_ask, yes_bid, snapshot_time)
        VALUES (%s, %s, %s, %s)
        """,
        (ticker, yes_ask, yes_bid, _now_iso()),
    )


def upsert_event(cur, event: dict):
    """Insert or update an event row."""
    market_tickers_json = json.dumps(event.get("market_tickers", []))
    cur.execute(
        """
        INSERT INTO events (event_ticker, title, category, market_tickers)
        VALUES (%(event_ticker)s, %(title)s, %(category)s, %(market_tickers)s)
        ON CONFLICT (event_ticker) DO UPDATE SET
            title          = EXCLUDED.title,
            category       = EXCLUDED.category,
            market_tickers = EXCLUDED.market_tickers
        """,
        {
            "event_ticker": event.get("event_ticker"),
            "title": event.get("title"),
            "category": event.get("category"),
            "market_tickers": market_tickers_json,
        },
    )


def insert_relationship(cur, rel: dict) -> int:
    """Insert a new relationship and return its id."""
    cur.execute(
        """
        INSERT INTO relationships (type, tickers, constraint_description,
                                   constraint_formula, confidence, reasoning,
                                   last_validated, created_at)
        VALUES (%(type)s, %(tickers)s, %(constraint_description)s,
                %(constraint_formula)s, %(confidence)s, %(reasoning)s,
                %(last_validated)s, %(created_at)s)
        RETURNING id
        """,
        {
            "type": rel.get("type"),
            "tickers": json.dumps(rel.get("tickers", [])),
            "constraint_description": rel.get("constraint_description"),
            "constraint_formula": rel.get("constraint_formula"),
            "confidence": rel.get("confidence"),
            "reasoning": rel.get("reasoning"),
            "last_validated": _now_iso(),
            "created_at": _now_iso(),
        },
    )
    row = cur.fetchone()
    return row["id"] if isinstance(row, dict) else row[0]


def insert_opportunity(cur, opp: dict) -> int:
    """Insert an opportunity and return its id."""
    cur.execute(
        """
        INSERT INTO opportunities (relationship_id, signal, magnitude, confidence,
                                   score, legs, status, detected_at, expires_at)
        VALUES (%(relationship_id)s, %(signal)s, %(magnitude)s, %(confidence)s,
                %(score)s, %(legs)s, %(status)s, %(detected_at)s, %(expires_at)s)
        RETURNING id
        """,
        {
            "relationship_id": opp.get("relationship_id"),
            "signal": opp.get("signal"),
            "magnitude": opp.get("magnitude"),
            "confidence": opp.get("confidence"),
            "score": opp.get("score"),
            "legs": json.dumps(opp.get("legs", [])),
            "status": opp.get("status", "DETECTED"),
            "detected_at": _now_iso(),
            "expires_at": opp.get("expires_at"),
        },
    )
    row = cur.fetchone()
    return row["id"] if isinstance(row, dict) else row[0]


def insert_trade(cur, trade: dict) -> int:
    """Insert a trade record and return its id."""
    cur.execute(
        """
        INSERT INTO trades (opportunity_id, ticker, side, action, price, count,
                            order_id, order_status, filled_count, fees,
                            created_at, updated_at)
        VALUES (%(opportunity_id)s, %(ticker)s, %(side)s, %(action)s, %(price)s,
                %(count)s, %(order_id)s, %(order_status)s, %(filled_count)s,
                %(fees)s, %(created_at)s, %(updated_at)s)
        RETURNING id
        """,
        {
            "opportunity_id": trade.get("opportunity_id"),
            "ticker": trade.get("ticker"),
            "side": trade.get("side"),
            "action": trade.get("action"),
            "price": trade.get("price"),
            "count": trade.get("count"),
            "order_id": trade.get("order_id"),
            "order_status": trade.get("order_status", "pending"),
            "filled_count": trade.get("filled_count", 0),
            "fees": trade.get("fees", 0),
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        },
    )
    row = cur.fetchone()
    return row["id"] if isinstance(row, dict) else row[0]


def get_all_markets(cur, status: str = "open") -> list:
    """Fetch all markets with the given status."""
    cur.execute("SELECT * FROM markets WHERE status = %s", (status,))
    return cur.fetchall()


def get_market(cur, ticker: str) -> dict | None:
    """Fetch a single market by ticker."""
    cur.execute("SELECT * FROM markets WHERE ticker = %s", (ticker,))
    return cur.fetchone()


def get_active_relationships(cur) -> list:
    """Fetch all relationships that reference at least one open market."""
    cur.execute(
        """
        SELECT r.* FROM relationships r
        WHERE EXISTS (
            SELECT 1 FROM markets m
            WHERE m.status = 'open'
              AND r.tickers LIKE '%%' || m.ticker || '%%'
        )
        """
    )
    return cur.fetchall()


def get_all_events(cur) -> list:
    """Fetch all events."""
    cur.execute("SELECT * FROM events")
    return cur.fetchall()


def update_opportunity_status(cur, opp_id: int, status: str):
    """Update the status of an opportunity."""
    cur.execute(
        "UPDATE opportunities SET status = %s WHERE id = %s",
        (status, opp_id),
    )


def update_trade(cur, trade_id: int, updates: dict):
    """Update fields on a trade record."""
    set_clauses = []
    params = []
    for key, value in updates.items():
        set_clauses.append(f"{key} = %s")
        params.append(value)
    params.append(_now_iso())
    params.append(trade_id)
    cur.execute(
        f"UPDATE trades SET {', '.join(set_clauses)}, updated_at = %s WHERE id = %s",
        params,
    )


def get_portfolio_state(cur) -> dict | None:
    """Get the latest portfolio state."""
    cur.execute("SELECT * FROM portfolio_state ORDER BY id DESC LIMIT 1")
    return cur.fetchone()


def upsert_portfolio_state(cur, state: dict):
    """Insert or update portfolio state (single-row table by convention)."""
    cur.execute(
        """
        INSERT INTO portfolio_state (id, balance, daily_pnl, open_positions, kill_switch, last_updated)
        VALUES (1, %(balance)s, %(daily_pnl)s, %(open_positions)s, %(kill_switch)s, %(last_updated)s)
        ON CONFLICT (id) DO UPDATE SET
            balance        = EXCLUDED.balance,
            daily_pnl      = EXCLUDED.daily_pnl,
            open_positions = EXCLUDED.open_positions,
            kill_switch    = EXCLUDED.kill_switch,
            last_updated   = EXCLUDED.last_updated
        """,
        {
            "balance": state.get("balance", 0),
            "daily_pnl": state.get("daily_pnl", 0),
            "open_positions": state.get("open_positions", 0),
            "kill_switch": state.get("kill_switch", False),
            "last_updated": _now_iso(),
        },
    )


def close_pool():
    """Shut down the connection pool."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("PostgreSQL connection pool closed")
