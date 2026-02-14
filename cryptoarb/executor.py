"""
Executor -- places buy-YES orders on all legs of a profitable partition.
"""

import time
import logging

from .kalshi_client import KalshiClient
from .scanner import ArbOpportunity
from .fees import taker_fee
from . import config, db

logger = logging.getLogger(__name__)


def execute_arb(client: KalshiClient, opp: ArbOpportunity) -> bool:
    """Buy YES on every leg of the partition.

    Places limit orders at the current ask price for each market.
    Logs everything to the arb_trades table.

    Returns True if all orders were placed successfully.
    """
    count = config.MAX_CONTRACTS_PER_LEG
    dry_run = config.DRY_RUN

    # Log the scan
    scan_id = db.log_scan(
        event_ticker=opp.event_ticker,
        num_markets=opp.num_markets,
        total_ask=opp.total_ask,
        total_fees=opp.total_fees,
        profit_cents=opp.profit_cents,
        acted=True,
    )

    all_success = True
    orders_placed = []

    for leg in opp.markets:
        ticker = leg["ticker"]
        price = leg["yes_ask"]
        price_cents = int(round(price * 100))
        fee = taker_fee(count, price)

        if dry_run:
            order_id = f"DRY-{int(time.time() * 1000)}"
            order_status = "dry_run"
            logger.info("[DRY RUN] BUY %d YES %s @ $%.2f (fee=$%.2f)",
                        count, ticker, price, fee)
        else:
            try:
                result = client.place_order(
                    ticker=ticker,
                    action="buy",
                    side="yes",
                    order_type="limit",
                    count=count,
                    yes_price=price_cents,
                )
                order_id = result.get("order", {}).get("order_id", "unknown")
                order_status = result.get("order", {}).get("status", "placed")
                orders_placed.append(order_id)
                logger.info("ORDER PLACED: BUY %d YES %s @ $%.2f -> %s (%s)",
                            count, ticker, price, order_id, order_status)
            except Exception as e:
                logger.error("FAILED to place order for %s: %s", ticker, e)
                order_id = "FAILED"
                order_status = f"error: {e}"
                all_success = False

        db.log_trade(
            scan_id=scan_id,
            event_ticker=opp.event_ticker,
            ticker=ticker,
            price=price,
            count=count,
            order_id=order_id,
            order_status=order_status,
            fees=fee,
        )

    if not dry_run and all_success:
        logger.info("All %d orders placed for %s (profit=%.1f¢ x %d contracts = $%.4f)",
                     opp.num_markets, opp.event_ticker, opp.profit_cents,
                     count, opp.profit_per_set * count)

    return all_success
