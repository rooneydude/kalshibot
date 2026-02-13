"""
Layer 3 -- Inconsistency Detection.

On each price cycle, scans all known relationships for constraint violations.
Scores violations by magnitude, confidence, and liquidity, then inserts
them into the opportunities table.
"""

import json
import logging
from datetime import datetime, timezone, timedelta

from src import db
from src.fees import taker_fee, is_profitable_after_fees

logger = logging.getLogger(__name__)

# Minimum magnitude (in dollars) before we even look at it
MIN_MAGNITUDE = 0.02

# Extra-wide threshold for soft IMPLICATION constraints
SOFT_THRESHOLD = 0.08

# Default liquidity factor when depth data is unavailable
DEFAULT_DEPTH = 20


def scan_for_violations(min_score: float = 0.01, fee_safety_multiplier: float = 2.0) -> list[dict]:
    """Scan all active relationships for constraint violations.

    Returns a list of opportunity dicts (also inserted into the DB).
    """
    opportunities = []

    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            relationships = db.get_active_relationships(cur)

            for rel in relationships:
                rel = dict(rel)
                rel_type = rel["type"]
                tickers = json.loads(rel["tickers"])
                confidence = rel.get("confidence", 0.5)

                # Fetch current prices for all tickers
                markets = {}
                for t in tickers:
                    m = db.get_market(cur, t)
                    if m:
                        markets[t] = dict(m)

                if len(markets) < 2:
                    continue

                if rel_type == "SUBSET":
                    opps = _check_subset(rel, tickers, markets, confidence, fee_safety_multiplier)
                elif rel_type == "THRESHOLD":
                    opps = _check_threshold(rel, tickers, markets, confidence, fee_safety_multiplier)
                elif rel_type == "PARTITION":
                    opps = _check_partition(rel, tickers, markets, confidence, fee_safety_multiplier)
                elif rel_type == "IMPLICATION":
                    opps = _check_implication(rel, tickers, markets, confidence, fee_safety_multiplier)
                else:
                    continue

                # Filter by min_score and insert
                for opp in opps:
                    if opp["score"] >= min_score:
                        opp_id = db.insert_opportunity(cur, opp)
                        opp["id"] = opp_id
                        opportunities.append(opp)
                        logger.info(
                            "Opportunity detected: %s | score=%.4f mag=%.4f | %s",
                            opp["signal"], opp["score"], opp["magnitude"],
                            json.dumps(opp["legs"]),
                        )

    logger.info("Scan complete: %d new opportunities", len(opportunities))
    return opportunities


# ---------------------------------------------------------------------------
# Constraint checkers
# ---------------------------------------------------------------------------

def _check_subset(rel: dict, tickers: list, markets: dict, confidence: float, safety: float) -> list[dict]:
    """SUBSET: P(subset) <= P(superset).

    tickers[0] = subset, tickers[1] = superset.
    Violation: subset yes_ask > superset yes_bid.
    Trade: buy superset, sell subset.
    """
    subset_ticker, superset_ticker = tickers[0], tickers[1]
    subset = markets.get(subset_ticker)
    superset = markets.get(superset_ticker)
    if not subset or not superset:
        return []

    sub_ask = subset.get("yes_ask") or 0
    sup_bid = superset.get("yes_bid") or 0

    magnitude = sub_ask - sup_bid
    if magnitude <= MIN_MAGNITUDE:
        return []

    prices = [sub_ask, sup_bid]
    if not is_profitable_after_fees(magnitude, 1, prices, safety):
        return []

    depth = min(
        subset.get("open_interest", DEFAULT_DEPTH),
        superset.get("open_interest", DEFAULT_DEPTH),
    ) or DEFAULT_DEPTH
    liquidity_factor = min(depth / 50.0, 1.0)  # normalise to [0, 1]
    score = magnitude * confidence * liquidity_factor

    return [{
        "relationship_id": rel["id"],
        "signal": "BUY_SUPERSET_SELL_SUBSET",
        "magnitude": round(magnitude, 4),
        "confidence": confidence,
        "score": round(score, 6),
        "legs": [
            {"ticker": superset_ticker, "side": "buy", "price": sup_bid, "depth": depth},
            {"ticker": subset_ticker, "side": "sell", "price": sub_ask, "depth": depth},
        ],
        "status": "DETECTED",
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    }]


def _check_threshold(rel: dict, tickers: list, markets: dict, confidence: float, safety: float) -> list[dict]:
    """THRESHOLD: For ascending thresholds, probabilities should descend.

    e.g. P(>3%) >= P(>4%) >= P(>5%)
    tickers_ascending = [lowest_threshold, ..., highest_threshold]
    """
    opps = []
    for i in range(len(tickers) - 1):
        lower_t = tickers[i]   # e.g. >3%
        higher_t = tickers[i + 1]  # e.g. >4%
        lower = markets.get(lower_t)
        higher = markets.get(higher_t)
        if not lower or not higher:
            continue

        lower_bid = lower.get("yes_bid") or 0
        higher_ask = higher.get("yes_ask") or 0

        magnitude = higher_ask - lower_bid
        if magnitude <= MIN_MAGNITUDE:
            continue

        prices = [lower_bid, higher_ask]
        if not is_profitable_after_fees(magnitude, 1, prices, safety):
            continue

        depth = min(
            lower.get("open_interest", DEFAULT_DEPTH),
            higher.get("open_interest", DEFAULT_DEPTH),
        ) or DEFAULT_DEPTH
        liquidity_factor = min(depth / 50.0, 1.0)
        score = magnitude * confidence * liquidity_factor

        opps.append({
            "relationship_id": rel["id"],
            "signal": f"BUY_{lower_t}_SELL_{higher_t}",
            "magnitude": round(magnitude, 4),
            "confidence": confidence,
            "score": round(score, 6),
            "legs": [
                {"ticker": lower_t, "side": "buy", "price": lower_bid, "depth": depth},
                {"ticker": higher_t, "side": "sell", "price": higher_ask, "depth": depth},
            ],
            "status": "DETECTED",
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        })
    return opps


def _check_partition(rel: dict, tickers: list, markets: dict, confidence: float, safety: float) -> list[dict]:
    """PARTITION: Sum of YES prices should ≈ $1.00.

    If sum of asks < 1.00 (significantly), buy all.
    If sum of bids > 1.00 (significantly), sell all.
    """
    opps = []
    available = [(t, markets[t]) for t in tickers if t in markets]
    if len(available) < len(tickers):
        return []  # Need all partition members

    total_ask = sum(m.get("yes_ask", 0) or 0 for _, m in available)
    total_bid = sum(m.get("yes_bid", 0) or 0 for _, m in available)

    # BUY ALL: total ask < 1.00
    buy_magnitude = 1.00 - total_ask
    if buy_magnitude > MIN_MAGNITUDE:
        prices = [m.get("yes_ask", 0) or 0 for _, m in available]
        if is_profitable_after_fees(buy_magnitude, 1, prices, safety):
            depth = min(m.get("open_interest", DEFAULT_DEPTH) or DEFAULT_DEPTH for _, m in available)
            liquidity_factor = min(depth / 50.0, 1.0)
            score = buy_magnitude * confidence * liquidity_factor
            opps.append({
                "relationship_id": rel["id"],
                "signal": "BUY_ALL_PARTITION",
                "magnitude": round(buy_magnitude, 4),
                "confidence": confidence,
                "score": round(score, 6),
                "legs": [
                    {"ticker": t, "side": "buy", "price": m.get("yes_ask", 0), "depth": depth}
                    for t, m in available
                ],
                "status": "DETECTED",
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            })

    # SELL ALL: total bid > 1.00
    sell_magnitude = total_bid - 1.00
    if sell_magnitude > MIN_MAGNITUDE:
        prices = [m.get("yes_bid", 0) or 0 for _, m in available]
        if is_profitable_after_fees(sell_magnitude, 1, prices, safety):
            depth = min(m.get("open_interest", DEFAULT_DEPTH) or DEFAULT_DEPTH for _, m in available)
            liquidity_factor = min(depth / 50.0, 1.0)
            score = sell_magnitude * confidence * liquidity_factor
            opps.append({
                "relationship_id": rel["id"],
                "signal": "SELL_ALL_PARTITION",
                "magnitude": round(sell_magnitude, 4),
                "confidence": confidence,
                "score": round(score, 6),
                "legs": [
                    {"ticker": t, "side": "sell", "price": m.get("yes_bid", 0), "depth": depth}
                    for t, m in available
                ],
                "status": "DETECTED",
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            })

    return opps


def _check_implication(rel: dict, tickers: list, markets: dict, confidence: float, safety: float) -> list[dict]:
    """IMPLICATION: Soft constraint — P(if) implies P(then).

    Only flag large mispricings with high confidence.
    tickers[0] = if_ticker, tickers[1] = then_ticker.
    """
    if_ticker, then_ticker = tickers[0], tickers[1]
    if_market = markets.get(if_ticker)
    then_market = markets.get(then_ticker)
    if not if_market or not then_market:
        return []

    if_bid = if_market.get("yes_bid") or 0
    then_ask = then_market.get("yes_ask") or 0

    # "if" implies "then", so P(if) should be <= P(then) (roughly)
    magnitude = if_bid - then_ask
    if magnitude <= SOFT_THRESHOLD:
        return []

    if confidence < 0.7:
        return []  # Only trade implications with high confidence

    prices = [if_bid, then_ask]
    if not is_profitable_after_fees(magnitude, 1, prices, safety):
        return []

    depth = min(
        if_market.get("open_interest", DEFAULT_DEPTH),
        then_market.get("open_interest", DEFAULT_DEPTH),
    ) or DEFAULT_DEPTH
    liquidity_factor = min(depth / 50.0, 1.0)
    score = magnitude * confidence * liquidity_factor

    return [{
        "relationship_id": rel["id"],
        "signal": "BUY_THEN_SELL_IF",
        "magnitude": round(magnitude, 4),
        "confidence": confidence,
        "score": round(score, 6),
        "legs": [
            {"ticker": then_ticker, "side": "buy", "price": then_ask, "depth": depth},
            {"ticker": if_ticker, "side": "sell", "price": if_bid, "depth": depth},
        ],
        "status": "DETECTED",
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    }]
