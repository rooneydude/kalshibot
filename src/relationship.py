"""
Layer 2 -- Relationship Mapping.

Uses Claude (Sonnet for bulk, Opus for ambiguous) to discover logical
relationships between Kalshi markets: SUBSET, THRESHOLD, PARTITION,
and IMPLICATION.

Three-pass batching strategy:
  1. Within-event  (markets grouped by the same event)
  2. Within-category  (markets in the same category, across events)
  3. Cross-category  (all remaining, run infrequently)
"""

import os
import json
import logging
from datetime import datetime, timezone

import anthropic

from src import db

logger = logging.getLogger(__name__)

SCAN_MODEL = os.environ.get("ANTHROPIC_SCAN_MODEL", "claude-sonnet-4-5-20250929")
VALIDATE_MODEL = os.environ.get("ANTHROPIC_VALIDATE_MODEL", "claude-opus-4-6")

SYSTEM_PROMPT = """\
You are analyzing prediction markets on Kalshi to find logically related markets
whose prices should be mathematically constrained relative to each other.

For each batch of markets I give you, identify ALL pairs or groups where a
logical/mathematical relationship exists. Classify each relationship as:

1. SUBSET: Market A's YES outcome is a strict subset of Market B's YES outcome.
   Output: { "type": "SUBSET", "subset_ticker": "...", "superset_ticker": "...",
   "confidence": 0.0-1.0, "reasoning": "..." }

2. THRESHOLD: Markets on the same underlying with ordered thresholds.
   Output: { "type": "THRESHOLD", "tickers_ascending": ["...", "..."],
   "confidence": 0.0-1.0, "reasoning": "..." }

3. PARTITION: Markets that should sum to ~100%.
   Output: { "type": "PARTITION", "tickers": ["...", "..."],
   "confidence": 0.0-1.0, "reasoning": "..." }

4. IMPLICATION: One event logically or empirically implies another.
   Output: { "type": "IMPLICATION", "if_ticker": "...", "then_ticker": "...",
   "estimated_conditional_prob": 0.0-1.0, "confidence": 0.0-1.0,
   "reasoning": "..." }

CRITICAL: Read the settlement rules carefully. Sometimes markets that LOOK
related have different settlement criteria that break the logical link.
Only flag relationships you are confident about. False positives waste money.

Return ONLY a valid JSON array of relationships. If no relationships exist, return [].
Do not include any text outside the JSON array.
"""

MAX_MARKETS_PER_BATCH = 40  # Keep context manageable for Sonnet


def _get_client() -> anthropic.Anthropic:
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def _format_market_for_prompt(m: dict) -> str:
    """Format a single market dict for inclusion in the LLM prompt."""
    parts = [
        f"Ticker: {m['ticker']}",
        f"  Title: {m['title']}",
    ]
    if m.get("subtitle"):
        parts.append(f"  Subtitle: {m['subtitle']}")
    parts.append(f"  Category: {m.get('category', 'N/A')}")
    parts.append(f"  YES ask: {m.get('yes_ask', 'N/A')}  YES bid: {m.get('yes_bid', 'N/A')}")
    if m.get("rules_primary"):
        rules = m["rules_primary"][:500]  # truncate long rules
        parts.append(f"  Settlement rules: {rules}")
    return "\n".join(parts)


def _call_llm(markets: list[dict], model: str = SCAN_MODEL) -> list[dict]:
    """Send a batch of markets to Claude and parse the JSON response."""
    client = _get_client()

    market_text = "\n\n".join(_format_market_for_prompt(m) for m in markets)
    user_msg = f"Analyze these {len(markets)} markets for logical relationships:\n\n{market_text}"

    logger.info("Sending %d markets to %s for relationship analysis", len(markets), model)

    response = client.messages.create(
        model=model,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )

    text = response.content[0].text.strip()

    # Extract JSON from the response (handle possible markdown fences)
    if text.startswith("```"):
        # Strip markdown code fences
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
        text = text.strip()

    try:
        relationships = json.loads(text)
        if not isinstance(relationships, list):
            logger.warning("LLM returned non-list JSON: %s", type(relationships))
            return []
        logger.info("LLM found %d relationships in batch", len(relationships))
        return relationships
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM response as JSON: %s\nResponse: %s", e, text[:500])
        return []


def _normalise_relationship(raw: dict) -> dict | None:
    """Convert raw LLM output into a standardised relationship dict for the DB."""
    rel_type = raw.get("type", "").upper()
    confidence = raw.get("confidence", 0.5)
    reasoning = raw.get("reasoning", "")

    if rel_type == "SUBSET":
        subset = raw.get("subset_ticker")
        superset = raw.get("superset_ticker")
        if not subset or not superset:
            return None
        return {
            "type": "SUBSET",
            "tickers": [subset, superset],
            "constraint_description": f"P({subset}) <= P({superset})",
            "constraint_formula": f"P({subset}) <= P({superset})",
            "confidence": confidence,
            "reasoning": reasoning,
        }

    elif rel_type == "THRESHOLD":
        tickers = raw.get("tickers_ascending", [])
        if len(tickers) < 2:
            return None
        desc = " >= ".join(f"P({t})" for t in tickers)
        return {
            "type": "THRESHOLD",
            "tickers": tickers,
            "constraint_description": desc,
            "constraint_formula": desc,
            "confidence": confidence,
            "reasoning": reasoning,
        }

    elif rel_type == "PARTITION":
        tickers = raw.get("tickers", [])
        if len(tickers) < 2:
            return None
        return {
            "type": "PARTITION",
            "tickers": tickers,
            "constraint_description": f"SUM(P({', '.join(tickers)})) ≈ 1.00",
            "constraint_formula": "SUM_EQUALS_1",
            "confidence": confidence,
            "reasoning": reasoning,
        }

    elif rel_type == "IMPLICATION":
        if_ticker = raw.get("if_ticker")
        then_ticker = raw.get("then_ticker")
        if not if_ticker or not then_ticker:
            return None
        cond_prob = raw.get("estimated_conditional_prob", 0.8)
        return {
            "type": "IMPLICATION",
            "tickers": [if_ticker, then_ticker],
            "constraint_description": f"P({if_ticker}) implies P({then_ticker}) with prob ~{cond_prob}",
            "constraint_formula": f"IMPLIES({if_ticker},{then_ticker},{cond_prob})",
            "confidence": confidence,
            "reasoning": reasoning,
        }

    else:
        logger.warning("Unknown relationship type: %s", rel_type)
        return None


# ---------------------------------------------------------------------------
# Batching strategies
# ---------------------------------------------------------------------------

def _batch_by_event(markets: list[dict]) -> list[list[dict]]:
    """Group markets by event_ticker."""
    groups: dict[str, list[dict]] = {}
    for m in markets:
        key = m.get("event_ticker") or "__no_event__"
        groups.setdefault(key, []).append(m)
    # Only send groups with 2+ markets (single markets can't have internal relationships)
    return [g for g in groups.values() if len(g) >= 2]


def _batch_by_category(markets: list[dict]) -> list[list[dict]]:
    """Group markets by category, chunk large groups."""
    groups: dict[str, list[dict]] = {}
    for m in markets:
        key = m.get("category") or "__no_category__"
        groups.setdefault(key, []).append(m)

    batches = []
    for group in groups.values():
        if len(group) < 2:
            continue
        # Chunk large groups
        for i in range(0, len(group), MAX_MARKETS_PER_BATCH):
            chunk = group[i : i + MAX_MARKETS_PER_BATCH]
            if len(chunk) >= 2:
                batches.append(chunk)
    return batches


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def discover_relationships(pass_type: str = "event") -> int:
    """Run relationship discovery and store results.

    *pass_type*: "event" (pass 1), "category" (pass 2), or "cross" (pass 3).
    Returns number of new relationships stored.
    """
    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            markets = db.get_all_markets(cur)

    if not markets:
        logger.info("No open markets – skipping relationship discovery")
        return 0

    # Convert RealDictRow to plain dicts
    markets = [dict(m) for m in markets]

    if pass_type == "event":
        batches = _batch_by_event(markets)
    elif pass_type == "category":
        batches = _batch_by_category(markets)
    else:
        # Cross-category: just chunk everything
        batches = [
            markets[i : i + MAX_MARKETS_PER_BATCH]
            for i in range(0, len(markets), MAX_MARKETS_PER_BATCH)
            if len(markets[i : i + MAX_MARKETS_PER_BATCH]) >= 2
        ]

    logger.info("Running %s-pass relationship discovery: %d batches", pass_type, len(batches))

    total_new = 0
    for idx, batch in enumerate(batches, 1):
        logger.info("Processing batch %d/%d (%d markets)", idx, len(batches), len(batch))
        raw_rels = _call_llm(batch)

        for raw in raw_rels:
            normalised = _normalise_relationship(raw)
            if normalised is None:
                continue

            # Check for duplicate (same type + same tickers)
            with db.get_conn() as conn:
                with db.get_cursor(conn) as cur:
                    tickers_json = json.dumps(sorted(normalised["tickers"]))
                    cur.execute(
                        "SELECT id FROM relationships WHERE type = %s AND tickers = %s",
                        (normalised["type"], tickers_json),
                    )
                    existing = cur.fetchone()
                    if existing:
                        # Update last_validated
                        cur.execute(
                            "UPDATE relationships SET last_validated = %s, confidence = %s WHERE id = %s",
                            (datetime.now(timezone.utc).isoformat(), normalised["confidence"],
                             existing["id"] if isinstance(existing, dict) else existing[0]),
                        )
                        logger.debug("Re-validated existing relationship %s", existing)
                        continue

                    # Store with sorted tickers for dedup consistency
                    normalised["tickers"] = sorted(normalised["tickers"])
                    db.insert_relationship(cur, normalised)
                    total_new += 1

    logger.info("Relationship discovery (%s pass) complete: %d new relationships", pass_type, total_new)
    return total_new


def validate_relationship(rel_id: int) -> bool:
    """Re-validate a specific relationship using the stronger model.

    Returns True if the relationship is still valid.
    """
    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            cur.execute("SELECT * FROM relationships WHERE id = %s", (rel_id,))
            rel = cur.fetchone()
            if not rel:
                return False

            tickers = json.loads(rel["tickers"])
            markets = []
            for t in tickers:
                m = db.get_market(cur, t)
                if m:
                    markets.append(dict(m))

    if len(markets) < 2:
        logger.warning("Relationship %d: not enough active markets, marking stale", rel_id)
        return False

    raw_rels = _call_llm(markets, model=VALIDATE_MODEL)

    # Check if the relationship type + tickers still appear
    for raw in raw_rels:
        normalised = _normalise_relationship(raw)
        if normalised and normalised["type"] == rel["type"]:
            normalised_tickers = set(normalised["tickers"])
            rel_tickers = set(tickers)
            if normalised_tickers == rel_tickers:
                # Still valid — update
                with db.get_conn() as conn:
                    with db.get_cursor(conn) as cur:
                        cur.execute(
                            "UPDATE relationships SET last_validated = %s, confidence = %s WHERE id = %s",
                            (datetime.now(timezone.utc).isoformat(), normalised["confidence"], rel_id),
                        )
                logger.info("Relationship %d re-validated successfully", rel_id)
                return True

    logger.warning("Relationship %d could not be re-validated", rel_id)
    return False


def cleanup_stale_relationships():
    """Remove relationships where all referenced markets are closed."""
    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            cur.execute("SELECT id, tickers FROM relationships")
            rows = cur.fetchall()
            removed = 0
            for row in rows:
                tickers = json.loads(row["tickers"])
                # Check if any ticker is still open
                any_open = False
                for t in tickers:
                    cur.execute("SELECT status FROM markets WHERE ticker = %s", (t,))
                    m = cur.fetchone()
                    if m and m["status"] in ("open", "active"):
                        any_open = True
                        break
                if not any_open:
                    cur.execute("DELETE FROM relationships WHERE id = %s", (row["id"],))
                    removed += 1
            logger.info("Cleaned up %d stale relationships", removed)
            return removed
