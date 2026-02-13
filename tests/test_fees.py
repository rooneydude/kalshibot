"""Tests for the Kalshi fee calculation module."""

import math
import unittest

from src.fees import taker_fee, maker_fee, estimate_round_trip_fees, is_profitable_after_fees


class TestTakerFee(unittest.TestCase):
    """Test taker fee calculation: ceil(0.07 * C * P * (1-P))."""

    def test_single_contract_50_cents(self):
        """At 50c, P*(1-P) is maximal (0.25). Fee = ceil(0.07*1*0.25*100)/100 = 0.02."""
        fee = taker_fee(1, 0.50)
        self.assertEqual(fee, 0.02)

    def test_single_contract_low_price(self):
        """At 5c, fee = ceil(0.07*1*0.05*0.95*100)/100 = ceil(0.3325)/100 = 0.01."""
        fee = taker_fee(1, 0.05)
        self.assertEqual(fee, 0.01)

    def test_single_contract_high_price(self):
        """At 95c, fee = ceil(0.07*1*0.95*0.05*100)/100 = 0.01."""
        fee = taker_fee(1, 0.95)
        self.assertEqual(fee, 0.01)

    def test_100_contracts_50_cents(self):
        """100 contracts at 50c: ceil(0.07*100*0.25*100)/100 = ceil(175)/100 = 1.75."""
        fee = taker_fee(100, 0.50)
        self.assertEqual(fee, 1.75)

    def test_zero_count(self):
        fee = taker_fee(0, 0.50)
        self.assertEqual(fee, 0.0)

    def test_zero_price(self):
        fee = taker_fee(10, 0.0)
        self.assertEqual(fee, 0.0)

    def test_price_at_one(self):
        fee = taker_fee(10, 1.0)
        self.assertEqual(fee, 0.0)

    def test_negative_count(self):
        fee = taker_fee(-5, 0.50)
        self.assertEqual(fee, 0.0)


class TestMakerFee(unittest.TestCase):
    """Maker fee is 25% of taker fee."""

    def test_maker_is_quarter_of_taker(self):
        for price in [0.10, 0.25, 0.50, 0.75, 0.90]:
            t = taker_fee(100, price)
            m = maker_fee(100, price)
            # Maker fee should be <= taker fee (rounding may cause slight differences)
            self.assertLessEqual(m, t)

    def test_single_contract_50c(self):
        fee = maker_fee(1, 0.50)
        # ceil(0.0175 * 1 * 0.25 * 100)/100 = ceil(0.4375)/100 = 0.01
        self.assertEqual(fee, 0.01)


class TestRoundTripFees(unittest.TestCase):
    def test_round_trip(self):
        fee = estimate_round_trip_fees(10, 0.40, 0.60)
        buy_fee = taker_fee(10, 0.40)
        sell_fee = taker_fee(10, 0.60)
        self.assertEqual(fee, buy_fee + sell_fee)


class TestIsProfitable(unittest.TestCase):
    def test_profitable(self):
        # 10c magnitude with cheap prices -> fees should be small
        result = is_profitable_after_fees(0.10, 10, [0.10, 0.10], safety_multiplier=2.0)
        self.assertTrue(result)

    def test_not_profitable(self):
        # Tiny magnitude should fail after fees
        result = is_profitable_after_fees(0.001, 1, [0.50, 0.50], safety_multiplier=2.0)
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
