"""
Discord alerts for the YES+NO arb bot.
"""

import time
import logging
from collections import deque
from datetime import datetime, timezone

import requests

from . import config
from .scanner import ArbOpportunity

logger = logging.getLogger(__name__)

COLOUR_GREEN = 0x2ECC71
COLOUR_GOLD = 0xF1C40F
COLOUR_RED = 0xE74C3C
COLOUR_BLUE = 0x3498DB

_send_timestamps: deque = deque()
_MAX_PER_MINUTE = 10


def _rate_limited() -> bool:
    now = time.monotonic()
    while _send_timestamps and now - _send_timestamps[0] > 60:
        _send_timestamps.popleft()
    return len(_send_timestamps) >= _MAX_PER_MINUTE


def _send(payload: dict, force: bool = False):
    url = config.DISCORD_WEBHOOK_URL
    if not url:
        return
    if not force and _rate_limited():
        return
    try:
        resp = requests.post(url, json=payload, timeout=10)
        _send_timestamps.append(time.monotonic())
        if resp.status_code == 429:
            time.sleep(min(resp.json().get("retry_after", 2), 3))
        elif resp.status_code not in (200, 204):
            logger.warning("Discord returned %d", resp.status_code)
    except Exception as e:
        logger.warning("Discord alert failed: %s", e)


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def send_arb_found(opp: ArbOpportunity, contracts: int):
    """Alert when a YES+NO arb is found and executed."""
    total_profit = opp.profit_per_contract * contracts

    embed = {
        "title": f"YES+NO ARB: {opp.ticker}",
        "description": (
            f"**{opp.title}**\n"
            f"**BUY YES** @ ${opp.yes_ask:.2f}\n"
            f"**BUY NO** @ ${opp.no_ask:.2f}\n"
            f"**Total cost:** ${opp.total_cost:.4f}\n"
            f"**Fees:** ${opp.total_fees:.4f}\n"
            f"**Profit/contract:** {opp.profit_cents:.1f}¢\n"
            f"**Contracts:** {contracts}\n"
            f"**Total profit:** ${total_profit:.4f}"
        ),
        "color": COLOUR_GREEN,
        "timestamp": _ts(),
        "footer": {"text": "YES+NO Arb Bot"},
    }
    _send({"embeds": [embed]})


def send_scan_summary(markets_cached: int, cache_age_s: float,
                      opportunities: int, cycle_time_ms: int):
    """Periodic scan summary."""
    embed = {
        "title": "Arb Scanner Status",
        "description": (
            f"**Markets cached:** {markets_cached:,}\n"
            f"**Cache age:** {cache_age_s:.0f}s\n"
            f"**Total arbs found:** {opportunities}\n"
            f"**Last cycle:** {cycle_time_ms}ms"
        ),
        "color": COLOUR_BLUE,
        "timestamp": _ts(),
        "footer": {"text": "YES+NO Arb Bot"},
    }
    _send({"embeds": [embed]})


def send_error(title: str, msg: str):
    embed = {
        "title": f"Arb Bot Error: {title}",
        "description": f"```\n{msg[:1800]}\n```",
        "color": COLOUR_RED,
        "timestamp": _ts(),
        "footer": {"text": "YES+NO Arb Bot"},
    }
    _send({"embeds": [embed]})


def send_startup(dry_run: bool, balance: float | None = None):
    bal_text = f"  Balance: ${balance:.2f}" if balance is not None else ""
    mode = "DRY RUN" if dry_run else "LIVE"
    embed = {
        "title": f"YES+NO Arb Bot Started ({mode})",
        "description": (
            f"Strategy: buy YES + NO on any contract where total < $1 after fees.\n"
            f"Scanning **all** open markets on Kalshi.\n"
            f"Cache refresh: {config.CACHE_REFRESH_SECONDS}s\n"
            f"Poll interval: {config.POLL_INTERVAL_SECONDS}s\n"
            f"Min profit: {config.MIN_PROFIT_CENTS}¢\n"
            f"Contracts/trade: {config.MAX_CONTRACTS_PER_LEG}\n"
            f"{bal_text}"
        ),
        "color": COLOUR_GOLD,
        "timestamp": _ts(),
        "footer": {"text": "YES+NO Arb Bot"},
    }
    _send({"embeds": [embed]}, force=True)
