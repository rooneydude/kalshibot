"""Mock/paper trading simulation tests.

Tests the full execution pipeline with a mocked Kalshi client and mocked DB,
verifying that the executor correctly handles dry-run trades, two-leg and
multi-leg execution, and safety controls.
"""

import unittest
from unittest.mock import MagicMock, patch

from src.executor import Executor
from src.portfolio import Portfolio


def _mock_client():
    """Create a mock KalshiClient."""
    client = MagicMock()
    client.get_balance.return_value = {"balance": 10000}  # $100 in cents
    client.get_positions.return_value = {"market_positions": []}
    client.place_order.return_value = {
        "order": {"order_id": "mock-order-123", "status": "open"}
    }
    client.get_order.return_value = {
        "order": {"order_id": "mock-order-123", "status": "filled", "filled_count": 5}
    }
    client.cancel_order.return_value = {}
    return client


def _mock_portfolio(client):
    """Create a portfolio with the mock client."""
    portfolio = Portfolio(
        client=client,
        max_risk_per_trade_pct=0.02,
        max_daily_loss=50.0,
        max_open_positions=10,
        max_contracts_per_trade=50,
    )
    portfolio._balance = 100.0
    portfolio._open_positions = 0
    portfolio._kill_switch = False
    portfolio._daily_pnl = 0.0
    return portfolio


class TestDryRunExecution(unittest.TestCase):
    """Test that dry-run mode logs trades without placing real orders."""

    @patch("src.executor.db")
    def test_two_leg_dry_run(self, mock_db):
        """Two-leg trade in dry-run mode should not call place_order."""
        client = _mock_client()
        portfolio = _mock_portfolio(client)
        executor = Executor(client=client, portfolio=portfolio, dry_run=True)

        # Mock DB interactions
        mock_db.get_conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_db.get_cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_db.insert_trade.return_value = 1

        opportunity = {
            "id": 1,
            "signal": "BUY_SUPERSET_SELL_SUBSET",
            "magnitude": 0.10,
            "confidence": 0.9,
            "score": 0.05,
            "legs": [
                {"ticker": "SUPERSET", "side": "buy", "price": 0.50, "depth": 20},
                {"ticker": "SUBSET", "side": "sell", "price": 0.60, "depth": 20},
            ],
        }

        result = executor.execute_opportunity(opportunity)
        self.assertTrue(result)
        # Should NOT have called the real place_order
        client.place_order.assert_not_called()


class TestSafetyChecks(unittest.TestCase):
    """Test that safety controls prevent trading when limits are hit."""

    @patch("src.executor.db")
    def test_kill_switch_prevents_trading(self, mock_db):
        client = _mock_client()
        portfolio = _mock_portfolio(client)
        portfolio._kill_switch = True
        executor = Executor(client=client, portfolio=portfolio, dry_run=True)

        mock_db.get_conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_db.get_cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_cursor.return_value.__exit__ = MagicMock(return_value=False)

        opportunity = {
            "id": 2,
            "signal": "BUY_SUPERSET_SELL_SUBSET",
            "magnitude": 0.10,
            "confidence": 0.9,
            "score": 0.05,
            "legs": [
                {"ticker": "A", "side": "buy", "price": 0.50, "depth": 20},
                {"ticker": "B", "side": "sell", "price": 0.60, "depth": 20},
            ],
        }

        result = executor.execute_opportunity(opportunity)
        self.assertFalse(result)

    @patch("src.executor.db")
    def test_daily_loss_prevents_trading(self, mock_db):
        client = _mock_client()
        portfolio = _mock_portfolio(client)
        portfolio._daily_pnl = -60.0  # Exceeds $50 limit
        executor = Executor(client=client, portfolio=portfolio, dry_run=True)

        mock_db.get_conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_db.get_cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_cursor.return_value.__exit__ = MagicMock(return_value=False)

        opportunity = {
            "id": 3,
            "signal": "BUY_ALL_PARTITION",
            "magnitude": 0.10,
            "confidence": 0.9,
            "score": 0.05,
            "legs": [
                {"ticker": "A", "side": "buy", "price": 0.30, "depth": 20},
                {"ticker": "B", "side": "buy", "price": 0.30, "depth": 20},
            ],
        }

        result = executor.execute_opportunity(opportunity)
        self.assertFalse(result)

    @patch("src.executor.db")
    def test_max_positions_prevents_trading(self, mock_db):
        client = _mock_client()
        portfolio = _mock_portfolio(client)
        portfolio._open_positions = 10  # At the limit
        executor = Executor(client=client, portfolio=portfolio, dry_run=True)

        mock_db.get_conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_db.get_cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.get_cursor.return_value.__exit__ = MagicMock(return_value=False)

        opportunity = {
            "id": 4,
            "signal": "BUY_SUPERSET_SELL_SUBSET",
            "magnitude": 0.10,
            "confidence": 0.9,
            "score": 0.05,
            "legs": [
                {"ticker": "A", "side": "buy", "price": 0.50, "depth": 20},
                {"ticker": "B", "side": "sell", "price": 0.60, "depth": 20},
            ],
        }

        result = executor.execute_opportunity(opportunity)
        self.assertFalse(result)


class TestPositionSizing(unittest.TestCase):
    """Test position size calculation."""

    def test_basic_sizing(self):
        client = _mock_client()
        portfolio = _mock_portfolio(client)
        portfolio._balance = 1000.0

        opp = {
            "magnitude": 0.10,
            "legs": [
                {"ticker": "A", "depth": 30},
                {"ticker": "B", "depth": 25},
            ],
        }

        size = portfolio.calculate_position_size(opp)
        # Max risk = 1000 * 0.02 = 20, / 0.10 = 200
        # But depth limit = 25, and hard cap = 50
        self.assertEqual(size, 25)

    def test_zero_magnitude(self):
        client = _mock_client()
        portfolio = _mock_portfolio(client)

        opp = {"magnitude": 0.0, "legs": [{"ticker": "A", "depth": 30}]}
        size = portfolio.calculate_position_size(opp)
        self.assertEqual(size, 0)


if __name__ == "__main__":
    unittest.main()
