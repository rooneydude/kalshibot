"""
Kalshi fee calculation (self-contained copy for cryptoarb).

Taker fee = ceil(0.07  * C * P * (1 - P))  in cents
Maker fee = ceil(0.0175 * C * P * (1 - P)) in cents
"""

import math


def taker_fee(count: int, price: float) -> float:
    """Taker fee in dollars for *count* contracts at *price* (0-1 scale)."""
    if count <= 0 or price <= 0 or price >= 1:
        return 0.0
    raw_cents = 7 * count * price * (1 - price)
    return math.ceil(round(raw_cents, 8)) / 100


def total_partition_fees(contracts_per_leg: int, prices: list[float]) -> float:
    """Total taker fees for buying YES on every leg of a partition.

    *prices* is a list of YES ask prices (0-1 scale) for each market in the
    partition.  Returns total fees in dollars.
    """
    return sum(taker_fee(contracts_per_leg, p) for p in prices)


def partition_profit(contracts_per_leg: int, prices: list[float]) -> float:
    """Net profit per set of contracts if buying all sides of a partition.

    Exactly one leg settles at $1.00, all others at $0.00.
    Profit = $1.00 * contracts - total_cost - total_fees

    Returns profit in dollars (negative if unprofitable).
    """
    total_cost = sum(prices) * contracts_per_leg
    fees = total_partition_fees(contracts_per_leg, prices)
    settlement = 1.00 * contracts_per_leg
    return settlement - total_cost - fees
