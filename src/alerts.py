"""
Discord webhook alert system.

Sends structured embeds for:
    - New opportunities detected (high-score only)
    - Trades executed (or dry-run logged, high-score only)
    - Errors and warnings
    - Daily P&L summary

Includes rate limiting to avoid Discord 429s.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from collections import deque

import requests

logger = logging.getLogger(__name__)

WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# Discord embed colour codes
COLOUR_GREEN = 0x2ECC71
COLOUR_RED = 0xE74C3C
COLOUR_BLUE = 0x3498DB
COLOUR_YELLOW = 0xF1C40F
COLOUR_ORANGE = 0xE67E22

# Rate limiter: track send timestamps
_send_timestamps: deque = deque()
_MAX_PER_MINUTE = int(os.environ.get("ALERT_MAX_PER_MINUTE", "5"))
_MIN_SCORE_FOR_ALERT = float(os.environ.get("ALERT_MIN_SCORE", "1.0"))


def configure(max_per_minute: int = 5, min_score: float = 1.0):
    """Update alert configuration at runtime."""
    global _MAX_PER_MINUTE, _MIN_SCORE_FOR_ALERT
    _MAX_PER_MINUTE = max_per_minute
    _MIN_SCORE_FOR_ALERT = min_score


def _rate_limited() -> bool:
    """Return True if we've hit the per-minute send limit."""
    now = time.monotonic()
    # Purge timestamps older than 60s
    while _send_timestamps and now - _send_timestamps[0] > 60:
        _send_timestamps.popleft()
    return len(_send_timestamps) >= _MAX_PER_MINUTE


def _send(payload: dict, force: bool = False):
    """Send a payload to the Discord webhook with rate limiting."""
    if not WEBHOOK_URL:
        logger.debug("DISCORD_WEBHOOK_URL not set, skipping alert")
        return

    if not force and _rate_limited():
        logger.debug("Discord rate limit reached (%d/min), skipping alert", _MAX_PER_MINUTE)
        return

    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        _send_timestamps.append(time.monotonic())
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 1)
            logger.debug("Discord 429, backing off %.1fs", retry_after)
            time.sleep(min(retry_after, 2))
        elif resp.status_code not in (200, 204):
            logger.warning("Discord webhook returned %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("Failed to send Discord alert: %s", e)


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Alert types
# ---------------------------------------------------------------------------

def send_opportunity_alert(opportunity: dict):
    """Notify about a newly detected opportunity (high-score only)."""
    score = opportunity.get("score", 0)
    if score < _MIN_SCORE_FOR_ALERT:
        return  # Skip low-score noise

    legs = opportunity.get("legs", [])
    if isinstance(legs, str):
        legs = json.loads(legs)

    legs_text = "\n".join(
        f"  {'BUY' if l.get('side') == 'buy' else 'SELL'} {l.get('ticker')} @ ${l.get('price', 0):.2f}"
        for l in legs
    )

    embed = {
        "title": f"Opportunity: {opportunity.get('signal', 'UNKNOWN')}",
        "description": (
            f"**Magnitude:** ${opportunity.get('magnitude', 0):.4f}\n"
            f"**Confidence:** {opportunity.get('confidence', 0):.2f}\n"
            f"**Score:** {opportunity.get('score', 0):.4f}\n\n"
            f"**Legs:**\n```\n{legs_text}\n```"
        ),
        "color": COLOUR_BLUE,
        "timestamp": _timestamp(),
        "footer": {"text": "Kalshi Mispricing Bot"},
    }

    _send({"embeds": [embed]})


def send_trade_alert(trade: dict, dry_run: bool = True):
    """Notify about an executed (or dry-run) trade (only for high-score)."""
    # Trade alerts follow the same rate limit -- called from main loop
    prefix = "DRY RUN " if dry_run else ""
    colour = COLOUR_YELLOW if dry_run else COLOUR_GREEN

    embed = {
        "title": f"{prefix}Trade: {trade.get('action', '').upper()} {trade.get('ticker', '')}",
        "description": (
            f"**Side:** {trade.get('side', '')}\n"
            f"**Count:** {trade.get('count', 0)}\n"
            f"**Price:** ${trade.get('price', 0):.2f}\n"
            f"**Fees:** ${trade.get('fees', 0):.2f}\n"
            f"**Order ID:** `{trade.get('order_id', 'N/A')}`\n"
            f"**Status:** {trade.get('order_status', 'N/A')}"
        ),
        "color": colour,
        "timestamp": _timestamp(),
        "footer": {"text": "Kalshi Mispricing Bot"},
    }

    _send({"embeds": [embed]})


def send_error_alert(title: str, error_msg: str):
    """Notify about an error (always sent, bypasses score filter)."""
    embed = {
        "title": f"Error: {title}",
        "description": f"```\n{error_msg[:1800]}\n```",
        "color": COLOUR_RED,
        "timestamp": _timestamp(),
        "footer": {"text": "Kalshi Mispricing Bot"},
    }

    _send({"embeds": [embed]})


def send_daily_summary(portfolio_summary: dict, opportunities_today: int = 0, trades_today: int = 0):
    """Send end-of-day portfolio summary (always sent)."""
    pnl = portfolio_summary.get("daily_pnl", 0)
    colour = COLOUR_GREEN if pnl >= 0 else COLOUR_RED

    embed = {
        "title": "Daily Summary",
        "description": (
            f"**Balance:** ${portfolio_summary.get('balance', 0):.2f}\n"
            f"**Daily P&L:** ${pnl:+.2f}\n"
            f"**Open Positions:** {portfolio_summary.get('open_positions', 0)}\n"
            f"**Kill Switch:** {'ON' if portfolio_summary.get('kill_switch') else 'OFF'}\n\n"
            f"**Opportunities Detected:** {opportunities_today}\n"
            f"**Trades Executed:** {trades_today}"
        ),
        "color": colour,
        "timestamp": _timestamp(),
        "footer": {"text": "Kalshi Mispricing Bot"},
    }

    _send({"embeds": [embed]}, force=True)


def send_startup_alert():
    """Notify that the bot has started (always sent)."""
    embed = {
        "title": "Bot Started",
        "description": "Kalshi Mispricing Bot is online and scanning markets.",
        "color": COLOUR_GREEN,
        "timestamp": _timestamp(),
        "footer": {"text": "Kalshi Mispricing Bot"},
    }

    _send({"embeds": [embed]}, force=True)


def send_shutdown_alert(reason: str = "normal"):
    """Notify that the bot is shutting down (always sent)."""
    embed = {
        "title": "Bot Shutting Down",
        "description": f"Reason: {reason}",
        "color": COLOUR_ORANGE,
        "timestamp": _timestamp(),
        "footer": {"text": "Kalshi Mispricing Bot"},
    }

    _send({"embeds": [embed]}, force=True)
