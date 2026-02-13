"""
Layer 5 -- Execution.

Converts validated opportunities into Kalshi orders.  Handles:
    - Dry-run mode (log without placing)
    - Two-leg execution (SUBSET / THRESHOLD)
    - Multi-leg execution (PARTITION)
    - Partial fill handling
    - Position sizing
"""

import json
import time
import logging
from datetime import datetime, timezone

from src.kalshi_client import KalshiClient
from src.portfolio import Portfolio
from src.fees import taker_fee
from src import db

logger = logging.getLogger(__name__)


class Executor:
    """Trade executor with dry-run support and safety controls."""

    def __init__(self, client: KalshiClient, portfolio: Portfolio, dry_run: bool = True):
        self.client = client
        self.portfolio = portfolio
        self.dry_run = dry_run

    # -----------------------------------------------------------------
    # Public entry point
    # -----------------------------------------------------------------

    def execute_opportunity(self, opportunity: dict) -> bool:
        """Execute an opportunity. Returns True if successful."""
        signal = opportunity.get("signal", "")
        legs = opportunity.get("legs", [])
        if isinstance(legs, str):
            legs = json.loads(legs)

        if not legs:
            logger.warning("Opportunity %s has no legs, skipping", opportunity.get("id"))
            return False

        # Pre-flight safety checks
        if not self.portfolio.can_trade():
            logger.warning("Portfolio safety check failed – skipping trade")
            return False

        # Calculate position size
        count = self.portfolio.calculate_position_size(opportunity)
        if count <= 0:
            logger.info("Position size is 0 – opportunity not worth trading")
            return False

        logger.info(
            "Executing opportunity %s: signal=%s count=%d dry_run=%s",
            opportunity.get("id"), signal, count, self.dry_run,
        )

        # Update opportunity status
        opp_id = opportunity.get("id")
        if opp_id:
            with db.get_conn() as conn:
                with db.get_cursor(conn) as cur:
                    db.update_opportunity_status(cur, opp_id, "EXECUTING")

        try:
            if signal in ("BUY_ALL_PARTITION", "SELL_ALL_PARTITION"):
                success = self._execute_multi_leg(opportunity, legs, count)
            else:
                success = self._execute_two_leg(opportunity, legs, count)

            # Update final status
            final_status = "FILLED" if success else "FAILED"
            if opp_id:
                with db.get_conn() as conn:
                    with db.get_cursor(conn) as cur:
                        db.update_opportunity_status(cur, opp_id, final_status)

            return success

        except Exception as e:
            logger.error("Execution error for opportunity %s: %s", opp_id, e)
            if opp_id:
                with db.get_conn() as conn:
                    with db.get_cursor(conn) as cur:
                        db.update_opportunity_status(cur, opp_id, "FAILED")
            return False

    # -----------------------------------------------------------------
    # Two-leg execution (SUBSET, THRESHOLD, IMPLICATION)
    # -----------------------------------------------------------------

    def _execute_two_leg(self, opportunity: dict, legs: list[dict], count: int) -> bool:
        """Place the harder-to-fill leg first, then the second."""
        if len(legs) < 2:
            logger.error("Two-leg execution requires 2 legs, got %d", len(legs))
            return False

        leg1, leg2 = legs[0], legs[1]

        # Leg 1: place and wait
        order1 = self._place_leg(opportunity, leg1, count)
        if not order1:
            return False

        if not self.dry_run:
            filled1 = self._wait_for_fill(order1["order_id"], timeout=30)
            if not filled1:
                logger.warning("Leg 1 not filled, cancelling")
                self._cancel_order(order1["order_id"])
                return False
            filled_count = filled1.get("filled_count", count)
        else:
            filled_count = count

        # Leg 2: place with matched quantity, slightly more aggressive
        price_cents = int(leg2.get("price", 0) * 100)
        if leg2.get("side") == "buy":
            price_cents += 1  # 1 cent more aggressive for buy
        else:
            price_cents = max(1, price_cents - 1)  # 1 cent more aggressive for sell

        leg2_adjusted = dict(leg2)
        leg2_adjusted["price"] = price_cents / 100.0

        order2 = self._place_leg(opportunity, leg2_adjusted, filled_count)
        if not order2:
            logger.warning("Leg 2 placement failed – holding directional position from leg 1")
            return False

        if not self.dry_run:
            filled2 = self._wait_for_fill(order2["order_id"], timeout=30)
            if not filled2:
                logger.warning("Leg 2 not filled – holding directional position")
                # Try to cancel and accept the directional exposure
                self._cancel_order(order2["order_id"])
                return False

        logger.info("Two-leg execution complete for opportunity %s", opportunity.get("id"))
        return True

    # -----------------------------------------------------------------
    # Multi-leg execution (PARTITION)
    # -----------------------------------------------------------------

    def _execute_multi_leg(self, opportunity: dict, legs: list[dict], count: int) -> bool:
        """Place all legs simultaneously for partition trades."""
        orders = []
        for leg in legs:
            order = self._place_leg(opportunity, leg, count)
            if order:
                orders.append(order)

        if not orders:
            return False

        if self.dry_run:
            logger.info("DRY RUN: would have placed %d partition legs", len(orders))
            return True

        # Wait for fills
        time.sleep(5)  # Give orders time to match

        filled = []
        unfilled = []
        for order in orders:
            status = self._check_order_status(order["order_id"])
            if status and status.get("status") == "filled":
                filled.append(order)
            else:
                unfilled.append(order)

        if unfilled:
            logger.warning(
                "Partial fill on partition: %d/%d legs filled, cancelling rest",
                len(filled), len(orders),
            )
            for order in unfilled:
                self._cancel_order(order["order_id"])

        return len(filled) == len(orders)

    # -----------------------------------------------------------------
    # Low-level order helpers
    # -----------------------------------------------------------------

    def _place_leg(self, opportunity: dict, leg: dict, count: int) -> dict | None:
        """Place a single order leg. Returns trade record dict or None."""
        ticker = leg.get("ticker")
        side_str = leg.get("side", "buy")  # "buy" or "sell"
        price = leg.get("price", 0)
        price_cents = int(price * 100)

        # Determine action and contract side
        if side_str == "buy":
            action = "buy"
            contract_side = "yes"
        else:
            action = "sell"
            contract_side = "yes"

        if self.dry_run:
            fee = taker_fee(count, price)
            logger.info(
                "DRY RUN: %s %d x %s @ $%.2f (fee ~$%.2f) [%s]",
                action.upper(), count, ticker, price, fee, opportunity.get("signal"),
            )
            # Record the dry-run trade
            trade = {
                "opportunity_id": opportunity.get("id"),
                "ticker": ticker,
                "side": contract_side,
                "action": action,
                "price": price,
                "count": count,
                "order_id": f"DRY-{int(time.time()*1000)}",
                "order_status": "dry_run",
                "filled_count": count,
                "fees": fee,
            }
            with db.get_conn() as conn:
                with db.get_cursor(conn) as cur:
                    db.insert_trade(cur, trade)
            return trade

        try:
            expiration_ts = int(time.time()) + 30  # 30-second expiry
            result = self.client.place_order(
                ticker=ticker,
                action=action,
                side=contract_side,
                order_type="limit",
                count=count,
                yes_price=price_cents,
                expiration_ts=expiration_ts,
            )

            order = result.get("order", result)
            order_id = order.get("order_id", "unknown")

            trade = {
                "opportunity_id": opportunity.get("id"),
                "ticker": ticker,
                "side": contract_side,
                "action": action,
                "price": price,
                "count": count,
                "order_id": order_id,
                "order_status": order.get("status", "pending"),
                "filled_count": 0,
                "fees": taker_fee(count, price),
            }
            with db.get_conn() as conn:
                with db.get_cursor(conn) as cur:
                    db.insert_trade(cur, trade)

            logger.info("Placed order %s: %s %d x %s @ %d cents", order_id, action, count, ticker, price_cents)
            return trade

        except Exception as e:
            logger.error("Failed to place order for %s: %s", ticker, e)
            return None

    def _wait_for_fill(self, order_id: str, timeout: int = 30) -> dict | None:
        """Poll for order fill. Returns order dict if filled, None if timed out."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self._check_order_status(order_id)
            if status:
                if status.get("status") == "filled":
                    return status
                if status.get("status") in ("canceled", "cancelled", "expired"):
                    return None
            time.sleep(1)
        return None

    def _check_order_status(self, order_id: str) -> dict | None:
        """Get current order status from Kalshi."""
        try:
            data = self.client.get_order(order_id)
            return data.get("order", data)
        except Exception as e:
            logger.warning("Failed to check order %s: %s", order_id, e)
            return None

    def _cancel_order(self, order_id: str):
        """Cancel an order and update the trade record."""
        try:
            self.client.cancel_order(order_id)
            logger.info("Cancelled order %s", order_id)
            with db.get_conn() as conn:
                with db.get_cursor(conn) as cur:
                    cur.execute(
                        "UPDATE trades SET order_status = 'cancelled', updated_at = %s WHERE order_id = %s",
                        (datetime.now(timezone.utc).isoformat(), order_id),
                    )
        except Exception as e:
            logger.warning("Failed to cancel order %s: %s", order_id, e)
