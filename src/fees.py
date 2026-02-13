"""
Kalshi fee calculation.

Fee schedule (updated Feb 2026):
    Taker fee  = ceil(0.07  * C * P * (1 - P))
    Maker fee  = ceil(0.0175 * C * P * (1 - P))   (25 % of taker)

Where:
    C = number of contracts
    P = contract price in dollars (0.0 – 1.0)

No settlement fee, no membership fee.
"""

import math


def taker_fee(count: int, price: float) -> float:
    """Calculate the taker fee in dollars for *count* contracts at *price*."""
    if count <= 0 or price <= 0 or price >= 1:
        return 0.0
    # Compute in cents to avoid floating-point ceil() errors
    raw_cents = 7 * count * price * (1 - price)
    return math.ceil(round(raw_cents, 8)) / 100  # round up to nearest cent


def maker_fee(count: int, price: float) -> float:
    """Calculate the maker (resting) fee in dollars."""
    if count <= 0 or price <= 0 or price >= 1:
        return 0.0
    raw_cents = 1.75 * count * price * (1 - price)
    return math.ceil(round(raw_cents, 8)) / 100


def estimate_round_trip_fees(count: int, buy_price: float, sell_price: float) -> float:
    """Estimate total fees for a buy + sell round trip, both as taker."""
    return taker_fee(count, buy_price) + taker_fee(count, sell_price)


def max_fee_per_contract(price: float) -> float:
    """Maximum taker fee for a single contract at a given price."""
    return taker_fee(1, price)


def is_profitable_after_fees(
    magnitude: float,
    count: int,
    prices: list[float],
    safety_multiplier: float = 2.0,
) -> bool:
    """Check whether a trade is profitable after fees with safety margin.

    *magnitude* is the raw mispricing spread in dollars per contract.
    *prices* is the list of leg prices involved (to estimate worst-case fees).
    """
    total_fees = sum(taker_fee(count, p) for p in prices)
    fee_per_contract = total_fees / count if count > 0 else 0
    return magnitude > fee_per_contract * safety_multiplier
