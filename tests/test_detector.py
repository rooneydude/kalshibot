"""Tests for the constraint violation detector.

These tests use synthetic data (no DB or API calls) to verify that
the detection logic correctly identifies violations for each relationship type.
"""

import unittest
from unittest.mock import patch, MagicMock

from src.detector import (
    _check_subset,
    _check_threshold,
    _check_partition,
    _check_implication,
)


def _make_market(ticker: str, yes_ask: float, yes_bid: float, open_interest: int = 50) -> dict:
    """Helper to create a synthetic market dict."""
    return {
        "ticker": ticker,
        "yes_ask": yes_ask,
        "yes_bid": yes_bid,
        "open_interest": open_interest,
    }


class TestSubsetDetection(unittest.TestCase):
    """SUBSET: P(subset) <= P(superset)."""

    def test_no_violation(self):
        """Subset priced lower than superset -- no violation."""
        rel = {"id": 1, "type": "SUBSET"}
        tickers = ["SUBSET_TICKER", "SUPERSET_TICKER"]
        markets = {
            "SUBSET_TICKER": _make_market("SUBSET_TICKER", yes_ask=0.30, yes_bid=0.28),
            "SUPERSET_TICKER": _make_market("SUPERSET_TICKER", yes_ask=0.60, yes_bid=0.58),
        }
        opps = _check_subset(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 0)

    def test_violation_detected(self):
        """Subset ask > superset bid -- violation."""
        rel = {"id": 1, "type": "SUBSET"}
        tickers = ["SUBSET_TICKER", "SUPERSET_TICKER"]
        markets = {
            "SUBSET_TICKER": _make_market("SUBSET_TICKER", yes_ask=0.65, yes_bid=0.63),
            "SUPERSET_TICKER": _make_market("SUPERSET_TICKER", yes_ask=0.52, yes_bid=0.50),
        }
        opps = _check_subset(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 1)
        self.assertEqual(opps[0]["signal"], "BUY_SUPERSET_SELL_SUBSET")
        self.assertGreater(opps[0]["magnitude"], 0)

    def test_violation_below_fee_threshold(self):
        """Tiny violation that doesn't survive fees."""
        rel = {"id": 1, "type": "SUBSET"}
        tickers = ["A", "B"]
        markets = {
            "A": _make_market("A", yes_ask=0.52, yes_bid=0.51),
            "B": _make_market("B", yes_ask=0.50, yes_bid=0.50),
        }
        opps = _check_subset(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 0)


class TestThresholdDetection(unittest.TestCase):
    """THRESHOLD: P(lower threshold) >= P(higher threshold)."""

    def test_no_violation(self):
        """Correctly ordered: P(>3%) > P(>4%) > P(>5%)."""
        rel = {"id": 2, "type": "THRESHOLD"}
        tickers = ["GT3", "GT4", "GT5"]
        markets = {
            "GT3": _make_market("GT3", yes_ask=0.70, yes_bid=0.68),
            "GT4": _make_market("GT4", yes_ask=0.50, yes_bid=0.48),
            "GT5": _make_market("GT5", yes_ask=0.20, yes_bid=0.18),
        }
        opps = _check_threshold(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 0)

    def test_single_violation(self):
        """P(>4%) ask > P(>3%) bid -- violation between first pair."""
        rel = {"id": 2, "type": "THRESHOLD"}
        tickers = ["GT3", "GT4"]
        markets = {
            "GT3": _make_market("GT3", yes_ask=0.40, yes_bid=0.38),
            "GT4": _make_market("GT4", yes_ask=0.55, yes_bid=0.53),
        }
        opps = _check_threshold(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 1)
        self.assertIn("GT3", opps[0]["signal"])
        self.assertIn("GT4", opps[0]["signal"])


class TestPartitionDetection(unittest.TestCase):
    """PARTITION: Sum of YES prices should ≈ $1.00."""

    def test_no_violation(self):
        """Asks sum to ~1.00 -- no violation."""
        rel = {"id": 3, "type": "PARTITION"}
        tickers = ["A", "B", "C"]
        markets = {
            "A": _make_market("A", yes_ask=0.35, yes_bid=0.33),
            "B": _make_market("B", yes_ask=0.35, yes_bid=0.33),
            "C": _make_market("C", yes_ask=0.30, yes_bid=0.28),
        }
        opps = _check_partition(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 0)

    def test_buy_all_violation(self):
        """Asks sum to well under 1.00 -- buy all."""
        rel = {"id": 3, "type": "PARTITION"}
        tickers = ["A", "B", "C"]
        markets = {
            "A": _make_market("A", yes_ask=0.20, yes_bid=0.18),
            "B": _make_market("B", yes_ask=0.20, yes_bid=0.18),
            "C": _make_market("C", yes_ask=0.20, yes_bid=0.18),
        }
        opps = _check_partition(rel, tickers, markets, confidence=0.9, safety=2.0)
        buy_opps = [o for o in opps if o["signal"] == "BUY_ALL_PARTITION"]
        self.assertGreater(len(buy_opps), 0)

    def test_sell_all_violation(self):
        """Bids sum to well over 1.00 -- sell all."""
        rel = {"id": 3, "type": "PARTITION"}
        tickers = ["A", "B", "C"]
        markets = {
            "A": _make_market("A", yes_ask=0.45, yes_bid=0.43),
            "B": _make_market("B", yes_ask=0.45, yes_bid=0.43),
            "C": _make_market("C", yes_ask=0.45, yes_bid=0.43),
        }
        opps = _check_partition(rel, tickers, markets, confidence=0.9, safety=2.0)
        sell_opps = [o for o in opps if o["signal"] == "SELL_ALL_PARTITION"]
        self.assertGreater(len(sell_opps), 0)

    def test_missing_market(self):
        """If any partition member is missing, skip."""
        rel = {"id": 3, "type": "PARTITION"}
        tickers = ["A", "B", "C"]
        markets = {
            "A": _make_market("A", yes_ask=0.20, yes_bid=0.18),
            "B": _make_market("B", yes_ask=0.20, yes_bid=0.18),
            # C is missing
        }
        opps = _check_partition(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 0)


class TestImplicationDetection(unittest.TestCase):
    """IMPLICATION: P(if) should roughly imply P(then)."""

    def test_no_violation(self):
        """If priced lower than then -- consistent."""
        rel = {"id": 4, "type": "IMPLICATION"}
        tickers = ["IF_TICKER", "THEN_TICKER"]
        markets = {
            "IF_TICKER": _make_market("IF_TICKER", yes_ask=0.30, yes_bid=0.28),
            "THEN_TICKER": _make_market("THEN_TICKER", yes_ask=0.60, yes_bid=0.58),
        }
        opps = _check_implication(rel, tickers, markets, confidence=0.8, safety=2.0)
        self.assertEqual(len(opps), 0)

    def test_violation(self):
        """If bid >> then ask with high confidence -- violation."""
        rel = {"id": 4, "type": "IMPLICATION"}
        tickers = ["IF_TICKER", "THEN_TICKER"]
        markets = {
            "IF_TICKER": _make_market("IF_TICKER", yes_ask=0.75, yes_bid=0.73),
            "THEN_TICKER": _make_market("THEN_TICKER", yes_ask=0.40, yes_bid=0.38),
        }
        opps = _check_implication(rel, tickers, markets, confidence=0.9, safety=2.0)
        self.assertEqual(len(opps), 1)
        self.assertEqual(opps[0]["signal"], "BUY_THEN_SELL_IF")

    def test_low_confidence_skipped(self):
        """Low-confidence implications should be skipped even with large spread."""
        rel = {"id": 4, "type": "IMPLICATION"}
        tickers = ["IF_TICKER", "THEN_TICKER"]
        markets = {
            "IF_TICKER": _make_market("IF_TICKER", yes_ask=0.75, yes_bid=0.73),
            "THEN_TICKER": _make_market("THEN_TICKER", yes_ask=0.40, yes_bid=0.38),
        }
        opps = _check_implication(rel, tickers, markets, confidence=0.3, safety=2.0)
        self.assertEqual(len(opps), 0)


if __name__ == "__main__":
    unittest.main()
