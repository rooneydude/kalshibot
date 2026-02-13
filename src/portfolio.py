"""
Portfolio management -- position tracking, P&L, and risk limits.

Enforces:
    - Kill switch
    - Max daily loss
    - Max open positions
    - Max contracts per trade
    - Position size calculation
"""

import json
import logging
from datetime import datetime, timezone, date

from src.kalshi_client import KalshiClient
from src import db
from src.fees import taker_fee

logger = logging.getLogger(__name__)


class Portfolio:
    """Manages portfolio state and risk controls."""

    def __init__(
        self,
        client: KalshiClient,
        max_risk_per_trade_pct: float = 0.02,
        max_daily_loss: float = 50.0,
        max_open_positions: int = 10,
        max_contracts_per_trade: int = 50,
        fee_safety_multiplier: float = 2.0,
    ):
        self.client = client
        self.max_risk_per_trade_pct = max_risk_per_trade_pct
        self.max_daily_loss = max_daily_loss
        self.max_open_positions = max_open_positions
        self.max_contracts_per_trade = max_contracts_per_trade
        self.fee_safety_multiplier = fee_safety_multiplier

        self._balance: float = 0.0
        self._daily_pnl: float = 0.0
        self._open_positions: int = 0
        self._kill_switch: bool = False
        self._last_pnl_date: str = ""

    # -----------------------------------------------------------------
    # Sync with Kalshi + DB
    # -----------------------------------------------------------------

    def sync(self):
        """Refresh portfolio state from Kalshi API and local DB."""
        try:
            balance_data = self.client.get_balance()
            self._balance = balance_data.get("balance", 0) / 100.0  # cents -> dollars
        except Exception as e:
            logger.warning("Failed to sync balance from Kalshi: %s", e)

        try:
            positions_data = self.client.get_positions()
            positions = positions_data.get("market_positions", [])
            self._open_positions = len(positions)
        except Exception as e:
            logger.warning("Failed to sync positions from Kalshi: %s", e)

        # Reset daily P&L at midnight
        today = date.today().isoformat()
        if self._last_pnl_date != today:
            self._daily_pnl = 0.0
            self._last_pnl_date = today

        # Load kill switch state from DB
        with db.get_conn() as conn:
            with db.get_cursor(conn) as cur:
                state = db.get_portfolio_state(cur)
                if state:
                    self._kill_switch = bool(state.get("kill_switch", False))

        # Persist current state
        self._save_state()

        logger.info(
            "Portfolio synced: balance=$%.2f daily_pnl=$%.2f positions=%d kill=%s",
            self._balance, self._daily_pnl, self._open_positions, self._kill_switch,
        )

    def _save_state(self):
        """Persist portfolio state to DB."""
        with db.get_conn() as conn:
            with db.get_cursor(conn) as cur:
                db.upsert_portfolio_state(cur, {
                    "balance": self._balance,
                    "daily_pnl": self._daily_pnl,
                    "open_positions": self._open_positions,
                    "kill_switch": self._kill_switch,
                })

    # -----------------------------------------------------------------
    # Risk checks
    # -----------------------------------------------------------------

    def can_trade(self) -> bool:
        """Return True if all safety checks pass."""
        if self._kill_switch:
            logger.warning("KILL SWITCH is active – no trading")
            return False

        if self._daily_pnl <= -self.max_daily_loss:
            logger.warning(
                "Daily loss limit reached ($%.2f <= -$%.2f) – no trading",
                self._daily_pnl, self.max_daily_loss,
            )
            return False

        if self._open_positions >= self.max_open_positions:
            logger.warning(
                "Max open positions reached (%d >= %d) – no trading",
                self._open_positions, self.max_open_positions,
            )
            return False

        return True

    def calculate_position_size(self, opportunity: dict) -> int:
        """Calculate the number of contracts to trade.

        Based on: min(risk-based size, liquidity, hard cap).
        """
        magnitude = opportunity.get("magnitude", 0)
        if magnitude <= 0:
            return 0

        legs = opportunity.get("legs", [])
        if isinstance(legs, str):
            legs = json.loads(legs)

        # Risk-based sizing: max 2% of balance at risk per trade
        max_risk = self._balance * self.max_risk_per_trade_pct
        risk_based = int(max_risk / magnitude) if magnitude > 0 else 0

        # Liquidity-based sizing: limited by shallowest depth
        depths = [leg.get("depth", 0) for leg in legs]
        min_depth = min(depths) if depths else 0

        count = min(
            risk_based,
            min_depth,
            self.max_contracts_per_trade,
        )

        return max(count, 0)

    # -----------------------------------------------------------------
    # P&L tracking
    # -----------------------------------------------------------------

    def record_fill(self, action: str, price: float, count: int, fees: float):
        """Record a fill and update daily P&L.

        For buys, cost = price * count + fees.
        For sells, proceeds = price * count - fees.
        """
        if action == "sell":
            self._daily_pnl += (price * count) - fees
        else:  # buy
            self._daily_pnl -= (price * count) + fees
        self._save_state()

    def record_settlement(self, payout: float):
        """Record a settlement payout."""
        self._daily_pnl += payout
        self._save_state()

    # -----------------------------------------------------------------
    # Kill switch
    # -----------------------------------------------------------------

    def activate_kill_switch(self):
        """Activate the kill switch to halt all trading."""
        self._kill_switch = True
        self._save_state()
        logger.critical("KILL SWITCH ACTIVATED")

    def deactivate_kill_switch(self):
        """Deactivate the kill switch."""
        self._kill_switch = False
        self._save_state()
        logger.info("Kill switch deactivated")

    # -----------------------------------------------------------------
    # Properties
    # -----------------------------------------------------------------

    @property
    def balance(self) -> float:
        return self._balance

    @property
    def daily_pnl(self) -> float:
        return self._daily_pnl

    @property
    def open_positions(self) -> int:
        return self._open_positions

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch

    def summary(self) -> dict:
        """Return a summary dict for logging / alerts."""
        return {
            "balance": round(self._balance, 2),
            "daily_pnl": round(self._daily_pnl, 2),
            "open_positions": self._open_positions,
            "kill_switch": self._kill_switch,
        }
