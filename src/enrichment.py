"""
Layer 4 -- Signal Enrichment.

For high-scoring opportunities, pulls external data to validate which side
is mispriced and to add directional conviction.

Data sources:
    - Polymarket CLOB API (cross-platform price comparison)
    - FRED (Federal Reserve Economic Data)
    - Yahoo Finance (via yfinance)
    - Claude Opus (LLM assessment for IMPLICATION-type trades)
"""

import os
import json
import logging
from typing import Any

import requests
import anthropic

logger = logging.getLogger(__name__)

VALIDATE_MODEL = os.environ.get("ANTHROPIC_VALIDATE_MODEL", "claude-opus-4-6")
FRED_API_KEY = os.environ.get("FRED_API_KEY")

# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

POLYMARKET_CLOB_URL = "https://clob.polymarket.com"


def fetch_polymarket_markets(query: str, limit: int = 10) -> list[dict]:
    """Search Polymarket for markets matching a query string."""
    try:
        resp = requests.get(
            f"{POLYMARKET_CLOB_URL}/markets",
            params={"next_cursor": "MA==", "limit": limit},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        markets = data if isinstance(data, list) else data.get("data", [])

        # Simple keyword filter since Polymarket doesn't always have search
        query_lower = query.lower()
        matched = [
            m for m in markets
            if query_lower in (m.get("question", "") + m.get("description", "")).lower()
        ]
        return matched[:limit]
    except Exception as e:
        logger.warning("Polymarket fetch failed: %s", e)
        return []


def get_polymarket_price(market: dict) -> float | None:
    """Extract the current YES price from a Polymarket market object."""
    tokens = market.get("tokens", [])
    for token in tokens:
        if token.get("outcome", "").lower() == "yes":
            return token.get("price")
    return None


# ---------------------------------------------------------------------------
# FRED (Federal Reserve Economic Data)
# ---------------------------------------------------------------------------

FRED_BASE_URL = "https://api.stlouisfed.org/fred"


def fetch_fred_series(series_id: str, limit: int = 10) -> list[dict]:
    """Fetch recent observations of a FRED data series.

    Requires FRED_API_KEY environment variable.
    """
    if not FRED_API_KEY:
        logger.debug("FRED_API_KEY not set, skipping FRED enrichment")
        return []

    try:
        resp = requests.get(
            f"{FRED_BASE_URL}/series/observations",
            params={
                "series_id": series_id,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "sort_order": "desc",
                "limit": limit,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("observations", [])
    except Exception as e:
        logger.warning("FRED fetch for %s failed: %s", series_id, e)
        return []


# Common FRED series for macro markets
FRED_SERIES = {
    "fed_funds": "FEDFUNDS",
    "cpi": "CPIAUCSL",
    "unemployment": "UNRATE",
    "gdp": "GDP",
    "pce": "PCEPI",
    "sp500": "SP500",
}


def fetch_macro_snapshot() -> dict[str, Any]:
    """Fetch latest values for common macro indicators."""
    snapshot = {}
    for name, series_id in FRED_SERIES.items():
        obs = fetch_fred_series(series_id, limit=1)
        if obs:
            snapshot[name] = {
                "value": obs[0].get("value"),
                "date": obs[0].get("date"),
            }
    return snapshot


# ---------------------------------------------------------------------------
# Yahoo Finance
# ---------------------------------------------------------------------------

def fetch_yahoo_price(symbol: str) -> float | None:
    """Fetch the latest price for a Yahoo Finance symbol.

    Uses yfinance if available, falls back to a simple API call.
    """
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except ImportError:
        logger.debug("yfinance not installed, skipping Yahoo Finance enrichment")
    except Exception as e:
        logger.warning("Yahoo Finance fetch for %s failed: %s", symbol, e)
    return None


# ---------------------------------------------------------------------------
# LLM Enrichment (Claude Opus for IMPLICATION validation)
# ---------------------------------------------------------------------------

LLM_ENRICHMENT_PROMPT = """\
Given these two Kalshi markets:
- Market A: "{title_a}" currently at {price_a} YES
- Market B: "{title_b}" currently at {price_b} YES

The identified relationship is: {relationship_description}

Given current macroeconomic conditions and the following data:
{external_data_summary}

Answer these questions in valid JSON:
{{
    "relationship_still_valid": true/false,
    "validity_reasoning": "...",
    "more_mispriced_market": "A" or "B",
    "fair_price_a": 0.0-1.0,
    "fair_price_b": 0.0-1.0,
    "confidence": 1-10,
    "reasoning": "..."
}}

Return ONLY the JSON object, no other text.
"""


def llm_validate_opportunity(
    market_a: dict,
    market_b: dict,
    relationship_desc: str,
    external_data: dict | None = None,
) -> dict | None:
    """Use Claude Opus to assess an IMPLICATION-type opportunity.

    Returns the parsed LLM assessment or None on failure.
    """
    try:
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

        ext_summary = json.dumps(external_data, indent=2) if external_data else "No external data available."

        prompt = LLM_ENRICHMENT_PROMPT.format(
            title_a=market_a.get("title", ""),
            price_a=market_a.get("yes_ask", "N/A"),
            title_b=market_b.get("title", ""),
            price_b=market_b.get("yes_ask", "N/A"),
            relationship_description=relationship_desc,
            external_data_summary=ext_summary,
        )

        response = client.messages.create(
            model=VALIDATE_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
            text = text.strip()

        return json.loads(text)
    except Exception as e:
        logger.error("LLM enrichment failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Full enrichment pipeline
# ---------------------------------------------------------------------------

def enrich_opportunity(opportunity: dict, markets: dict[str, dict]) -> dict:
    """Run the full enrichment pipeline for a single opportunity.

    *opportunity* is the opportunity dict from the DB.
    *markets* is a dict of ticker -> market data for all legs.

    Returns an enrichment_data dict to attach to the opportunity.
    """
    enrichment: dict[str, Any] = {
        "polymarket": {},
        "fred": {},
        "yahoo": {},
        "llm_assessment": None,
    }

    legs = opportunity.get("legs", [])
    if isinstance(legs, str):
        legs = json.loads(legs)

    # 1. Polymarket cross-reference
    for leg in legs:
        ticker = leg.get("ticker", "")
        market = markets.get(ticker, {})
        title = market.get("title", "")
        if title:
            poly_matches = fetch_polymarket_markets(title, limit=3)
            for pm in poly_matches:
                price = get_polymarket_price(pm)
                if price is not None:
                    enrichment["polymarket"][ticker] = {
                        "polymarket_question": pm.get("question"),
                        "polymarket_price": price,
                        "kalshi_price": market.get("yes_ask"),
                    }
                    break

    # 2. FRED macro data (only for macro-related markets)
    categories = set(markets[leg["ticker"]].get("category", "") for leg in legs if leg["ticker"] in markets)
    macro_cats = {"Economics", "Fed", "Inflation", "GDP", "Employment", "Politics"}
    if categories & macro_cats or any(
        kw in str(categories).lower()
        for kw in ["fed", "inflation", "gdp", "unemployment", "recession", "rate"]
    ):
        enrichment["fred"] = fetch_macro_snapshot()

    # 3. LLM validation for IMPLICATION-type
    if opportunity.get("signal") == "BUY_THEN_SELL_IF" and len(legs) >= 2:
        market_a = markets.get(legs[0]["ticker"], {})
        market_b = markets.get(legs[1]["ticker"], {})
        rel_desc = f"Signal: {opportunity.get('signal')}"

        external = {}
        if enrichment["fred"]:
            external["fred_data"] = enrichment["fred"]
        if enrichment["polymarket"]:
            external["polymarket_data"] = enrichment["polymarket"]

        assessment = llm_validate_opportunity(market_a, market_b, rel_desc, external or None)
        enrichment["llm_assessment"] = assessment

    return enrichment
