"""
Main entry point for the YES+NO partition arbitrage bot.

Usage:  python -m cryptoarb
"""

import time
import signal
import logging
import sys

from . import config
from .kalshi_client import KalshiClient
from .scanner import scan_contracts, start_cache, stop_cache, cache_info
from .executor import execute_arb
from . import alerts, db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("cryptoarb")

running = True


def _shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received (%s)", signum)
    running = False


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)


def main():
    global running

    logger.info("=== YES+NO Arb Bot starting ===")
    logger.info("Base URL: %s", config.KALSHI_BASE_URL)
    logger.info("Dry run: %s", config.DRY_RUN)
    logger.info("Min profit: %d¢", config.MIN_PROFIT_CENTS)
    logger.info("Contracts/trade: %d", config.MAX_CONTRACTS_PER_LEG)
    logger.info("Poll interval: %ds", config.POLL_INTERVAL_SECONDS)
    logger.info("Cache refresh: %ds", config.CACHE_REFRESH_SECONDS)

    # Init
    client = KalshiClient()
    db.init_db()

    # Check balance
    balance = None
    try:
        bal_data = client.get_balance()
        balance = bal_data.get("balance", 0) / 100.0
        logger.info("Account balance: $%.2f", balance)
    except Exception as e:
        logger.warning("Could not fetch balance: %s", e)

    # Start background market cache (blocks until first snapshot is loaded)
    start_cache(client)

    alerts.send_startup(dry_run=config.DRY_RUN, balance=balance)

    scan_count = 0
    total_arbs = 0

    while running:
        cycle_start = time.monotonic()

        try:
            opportunities = scan_contracts(client)
            scan_count += 1

            for opp in opportunities:
                execute_arb(client, opp)
                alerts.send_arb_found(opp, config.MAX_CONTRACTS_PER_LEG)
                total_arbs += 1

            cycle_ms = int((time.monotonic() - cycle_start) * 1000)
            mkt_count, cache_age = cache_info()

            # Log every cycle
            logger.info(
                "Cycle %d: %dms, %d markets, cache %.0fs old, "
                "%d arbs found (total: %d)",
                scan_count, cycle_ms, mkt_count, cache_age,
                len(opportunities), total_arbs,
            )

            # Discord summary every 100 cycles
            if scan_count % 100 == 0:
                alerts.send_scan_summary(
                    markets_cached=mkt_count,
                    cache_age_s=cache_age,
                    opportunities=total_arbs,
                    cycle_time_ms=cycle_ms,
                )

        except Exception as e:
            logger.error("Scan cycle failed: %s", e, exc_info=True)
            alerts.send_error("Scan cycle failed", str(e))

        # Wait for next cycle
        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0, config.POLL_INTERVAL_SECONDS - elapsed)
        if sleep_time > 0 and running:
            time.sleep(sleep_time)

    stop_cache()
    logger.info("Arb Bot shutting down (total arbs: %d)", total_arbs)


if __name__ == "__main__":
    main()
