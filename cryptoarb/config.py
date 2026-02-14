"""
Configuration for the crypto partition arbitrage bot.

All secrets come from environment variables (prefixed LIVE_KALSHI_*).
"""

import os

# Kalshi LIVE API credentials (separate from demo bot)
KALSHI_API_KEY_ID = os.environ.get("LIVE_KALSHI_API_KEY_ID", "")
KALSHI_RSA_PRIVATE_KEY = os.environ.get("LIVE_KALSHI_RSA_PRIVATE_KEY", "")
KALSHI_BASE_URL = os.environ.get(
    "LIVE_KALSHI_BASE_URL", "https://trading-api.kalshi.co/trade-api/v2"
)

# Database (shared with bot #1)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Discord alerts
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Trading parameters
MIN_PROFIT_CENTS = int(os.environ.get("ARB_MIN_PROFIT_CENTS", "2"))
POLL_INTERVAL_SECONDS = int(os.environ.get("ARB_POLL_INTERVAL", "5"))
MAX_CONTRACTS_PER_LEG = int(os.environ.get("ARB_MAX_CONTRACTS", "10"))
DRY_RUN = os.environ.get("ARB_DRY_RUN", "false").lower() in ("true", "1", "yes")

# Crypto event ticker prefixes to scan (partition markets)
CRYPTO_EVENT_PREFIXES = [
    "KXBTC-", "KXBTCD-",
    "KXETH-", "KXETHD-",
    "KXSOL-", "KXSOLD-",
    "KXXRP-", "KXXRPD-",
]
