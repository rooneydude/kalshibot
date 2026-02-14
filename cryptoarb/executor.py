"""
Executor -- buys YES and NO on a single contract for guaranteed arb profit.
"""

import time
import logging

from .kalshi_client import KalshiClient
from .scanner import ArbOpportunity
from .fees import taker_fee
from . import config, db

logger = logging.getLogger(__name__)


def execute_arb(client: KalshiClient, opp: ArbOpportunity) -> bool:
    """Buy YES and NO on a single contract.

    Places limit orders at the current ask price for both sides.
    Returns True if both orders were placed successfully.
    """
    count = config.MAX_CONTRACTS_PER_LEG
    dry_run = config.DRY_RUN

    total_fees = (taker_fee(count, opp.yes_ask) + taker_fee(count, opp.no_ask))
    total_profit = opp.profit_per_contract * count

    # Log the scan
    scan_id = db.log_scan(
        event_ticker=opp.event_ticker,
        num_markets=1,
        total_ask=opp.total_cost,
        total_fees=total_fees,
        profit_cents=opp.profit_cents,
        acted=True,
    )

    success = True

    # Leg 1: Buy YES
    yes_price_cents = int(round(opp.yes_ask * 100))
    if dry_run:
        yes_order_id = f"DRY-YES-{int(time.time() * 1000)}"
        yes_status = "dry_run"
        logger.info("[DRY RUN] BUY %d YES %s @ $%.2f", count, opp.ticker, opp.yes_ask)
    else:
        try:
            result = client.place_order(
                ticker=opp.ticker,
                action="buy",
                side="yes",
                order_type="limit",
                count=count,
                yes_price=yes_price_cents,
            )
            yes_order_id = result.get("order", {}).get("order_id", "unknown")
            yes_status = result.get("order", {}).get("status", "placed")
            logger.info("YES ORDER: BUY %d %s @ $%.2f -> %s",
                        count, opp.ticker, opp.yes_ask, yes_order_id)
        except Exception as e:
            logger.error("FAILED YES order for %s: %s", opp.ticker, e)
            yes_order_id = "FAILED"
            yes_status = f"error: {e}"
            success = False

    db.log_trade(
        scan_id=scan_id,
        event_ticker=opp.event_ticker,
        ticker=opp.ticker,
        side="yes",
        price=opp.yes_ask,
        count=count,
        order_id=yes_order_id,
        order_status=yes_status,
        fees=taker_fee(count, opp.yes_ask),
    )

    # Leg 2: Buy NO
    # For NO orders, Kalshi uses yes_price to represent the complement:
    # buying NO at no_ask means setting yes_price = 100 - no_ask_cents
    no_price_cents = int(round(opp.no_ask * 100))
    no_yes_price = 100 - no_price_cents  # Kalshi's NO pricing convention

    if dry_run:
        no_order_id = f"DRY-NO-{int(time.time() * 1000)}"
        no_status = "dry_run"
        logger.info("[DRY RUN] BUY %d NO %s @ $%.2f", count, opp.ticker, opp.no_ask)
    else:
        try:
            result = client.place_order(
                ticker=opp.ticker,
                action="buy",
                side="no",
                order_type="limit",
                count=count,
                yes_price=no_yes_price,
            )
            no_order_id = result.get("order", {}).get("order_id", "unknown")
            no_status = result.get("order", {}).get("status", "placed")
            logger.info("NO ORDER: BUY %d NO %s @ $%.2f -> %s",
                        count, opp.ticker, opp.no_ask, no_order_id)
        except Exception as e:
            logger.error("FAILED NO order for %s: %s", opp.ticker, e)
            no_order_id = "FAILED"
            no_status = f"error: {e}"
            success = False

    db.log_trade(
        scan_id=scan_id,
        event_ticker=opp.event_ticker,
        ticker=opp.ticker,
        side="no",
        price=opp.no_ask,
        count=count,
        order_id=no_order_id,
        order_status=no_status,
        fees=taker_fee(count, opp.no_ask),
    )

    if success and not dry_run:
        logger.info("ARB EXECUTED: %s  profit=%.1f¢ x %d = $%.4f",
                     opp.ticker, opp.profit_cents, count, total_profit)

    return success
