# Kalshi Cross-Market Mispricing Detector & Trader

## Project Overview

Build a Python-based system that detects logical pricing inconsistencies across related Kalshi prediction markets and executes trades against mispricings. This is NOT a simple arbitrage bot — it uses semantic analysis to find markets that are logically linked but priced inconsistently, then trades the mispriced side.

### Core Thesis

Prediction markets on Kalshi frequently contain cross-market mispricings that persist for hours or days because they require *reasoning* about the logical relationships between contracts in different categories. Example: "Will the Fed cut rates in March?" at $0.60 while "Will the Fed cut rates by June?" is at $0.50 — this is incoherent because March is a subset of possible cut dates before June.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR (main.py)                │
│              Scheduling, logging, state mgmt             │
├──────────┬──────────┬───────────┬───────────┬───────────┤
│  LAYER 1 │  LAYER 2 │  LAYER 3  │  LAYER 4  │  LAYER 5  │
│  Ingest  │  Mapping │  Detect   │  Enrich   │  Execute  │
│          │          │           │           │           │
│ Kalshi   │ Claude   │ Price vs  │ External  │ Order     │
│ API pull │ clusters │ constraint│ data pull │ placement │
│ + cache  │ related  │ violation │ + scoring │ + sizing  │
│          │ markets  │ scanning  │           │           │
└──────────┴──────────┴───────────┴───────────┴───────────┘
```

### Tech Stack

- **Language**: Python 3.11+
- **Kalshi interaction**: `requests` + Kalshi REST API v2
- **LLM**: Anthropic Claude API (claude-sonnet-4-5-20250929 for relationship mapping — fast and cheap for bulk analysis; claude-opus-4-6 for high-confidence trade signal validation)
- **Data storage**: SQLite for market cache, trade log, and relationship mappings
- **Scheduling**: `schedule` library or simple `while True` loop with `time.sleep`
- **Config**: `.env` file for API keys, `config.yaml` for trading parameters
- **Logging**: Python `logging` module to file + console

---

## Layer 1: Market Ingestion

### Responsibilities

- Pull all active markets from Kalshi's API on a regular cadence
- Cache market metadata and current prices in SQLite
- Track price history for each contract

### Kalshi API Details

**Base URL**: `https://trading-api.kalshi.com/trade-api/v2`

**Authentication**: Kalshi uses email/password login to obtain an auth token. The token is passed as a Bearer token in subsequent requests.

```
POST /login
Body: { "email": "...", "password": "..." }
Response: { "token": "...", "member_id": "..." }
```

**Key Endpoints**:

- `GET /markets` — List all markets. Supports pagination via `cursor` param and filtering via `status=open`. Returns market ticker, title, subtitle, category, yes_ask, yes_bid, no_ask, no_bid, volume, open_interest, close_time, etc.
- `GET /markets/{ticker}` — Single market details
- `GET /markets/{ticker}/orderbook` — Full order book with depth
- `GET /events` — List events (an event groups multiple related markets, e.g., "Fed Rate Decision March 2026" might contain multiple strike-level markets)
- `GET /events/{event_ticker}` — Event details with associated market tickers

**Rate Limits**: Kalshi imposes rate limits (typically 10 requests/second). Implement exponential backoff and respect `Retry-After` headers.

### Implementation Notes

- Poll `GET /markets?status=open` every 60 seconds for the full market scan
- For flagged opportunities, poll the specific orderbook every 10-15 seconds for real-time depth
- Store in SQLite tables: `markets`, `price_snapshots`, `events`
- Cache the event-to-market groupings — these are critical for Layer 2

### SQLite Schema

```sql
CREATE TABLE markets (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT,
    title TEXT,
    subtitle TEXT,
    category TEXT,
    status TEXT,
    yes_ask REAL,
    yes_bid REAL,
    no_ask REAL,
    no_bid REAL,
    volume INTEGER,
    open_interest INTEGER,
    close_time TEXT,
    rules_primary TEXT,  -- settlement rules, very important
    updated_at TEXT
);

CREATE TABLE price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT,
    yes_ask REAL,
    yes_bid REAL,
    snapshot_time TEXT,
    FOREIGN KEY (ticker) REFERENCES markets(ticker)
);

CREATE TABLE events (
    event_ticker TEXT PRIMARY KEY,
    title TEXT,
    category TEXT,
    market_tickers TEXT  -- JSON array of associated market tickers
);
```

---

## Layer 2: Relationship Mapping

### Responsibilities

- Identify which markets are semantically related across different events/categories
- Define the logical constraints between related markets
- Store relationships and constraints in a structured format

### Types of Relationships to Detect

#### 1. Subset/Superset (MUST detect)
Market A is a strict subset of Market B's outcome space.
- Example: "Fed cuts in March" ⊂ "Fed cuts by June"
- Constraint: `P(A) <= P(B)` always
- Trade: If P(A) > P(B), buy B and sell A

#### 2. Hierarchical Thresholds (MUST detect)
Markets on the same underlying with different strike levels.
- Example: "Inflation > 3%" and "Inflation > 4%"
- Constraint: `P(>4%) <= P(>3%)` always
- Trade: If P(>4%) > P(>3%), buy >3% and sell >4%

#### 3. Exhaustive Partition (MUST detect)
A set of markets that covers all possible outcomes.
- Example: GDP buckets: "0-1%", "1-2%", "2-3%", "3%+"
- Constraint: Sum of YES prices should ≈ $1.00
- Trade: If sum < $1.00 significantly (after fees), buy all. If sum > $1.00, sell all.

#### 4. Logical Implication (SHOULD detect)
One event strongly implies another based on domain knowledge.
- Example: "US enters recession in 2026" → "Unemployment > 5% in 2026" (strong but not certain link)
- Constraint: Soft — P(recession) should be somewhat bounded by P(unemployment > 5%)
- Trade: Only with high confidence and larger mispricings

#### 5. Cross-Platform (NICE TO HAVE)
Same or equivalent event priced differently on Kalshi vs. Polymarket or implied by traditional financial instruments (fed funds futures, options).
- This layer is optional for MVP but very powerful

### LLM-Powered Relationship Discovery

**Prompt strategy**: Feed Claude batches of market titles + descriptions and ask it to identify relationships.

```
System prompt for relationship mapping:

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

Return valid JSON array of relationships. If no relationships exist, return [].
```

**Batching strategy**:
- First pass: Group markets by event — markets within the same event are most likely related
- Second pass: Group markets by category and scan across events
- Third pass: Cross-category scan (most expensive, run less frequently)
- Use Sonnet for bulk scanning, escalate ambiguous cases to Opus

### Storage

```sql
CREATE TABLE relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT,  -- SUBSET, THRESHOLD, PARTITION, IMPLICATION
    tickers TEXT,  -- JSON array of involved tickers
    constraint_description TEXT,  -- human-readable constraint
    constraint_formula TEXT,  -- parseable formula like "P(A) <= P(B)"
    confidence REAL,  -- LLM confidence score
    reasoning TEXT,
    last_validated TEXT,  -- re-validate periodically as markets change
    created_at TEXT
);
```

### Re-validation

- Re-run relationship mapping when new markets are added
- Re-validate existing relationships weekly (settlement rules can change)
- Remove relationships when either market closes

---

## Layer 3: Inconsistency Detection

### Responsibilities

- On every price update cycle, check all known relationships for constraint violations
- Score violations by magnitude, confidence, and expected value
- Maintain a live opportunity queue

### Detection Logic

For each relationship type:

**SUBSET** (`P(A) <= P(B)`):
```python
violation = market_a.yes_ask - market_b.yes_bid  # cost to buy A, sell B
if violation > FEE_THRESHOLD:
    signal = "BUY_SUPERSET_SELL_SUBSET"
    magnitude = violation - estimated_fees
```

**THRESHOLD** (descending probability for ascending thresholds):
```python
for i in range(len(tickers) - 1):
    lower = markets[tickers[i]]  # e.g., >3%
    higher = markets[tickers[i+1]]  # e.g., >4%
    if higher.yes_ask > lower.yes_bid:
        signal = f"BUY_{tickers[i]}_SELL_{tickers[i+1]}"
        magnitude = higher.yes_ask - lower.yes_bid - estimated_fees
```

**PARTITION** (sum should ≈ $1.00):
```python
total_ask = sum(m.yes_ask for m in partition_markets)
total_bid = sum(m.yes_bid for m in partition_markets)
if total_ask < 1.00 - FEE_THRESHOLD:
    signal = "BUY_ALL"
    magnitude = 1.00 - total_ask - estimated_fees
if total_bid > 1.00 + FEE_THRESHOLD:
    signal = "SELL_ALL"
    magnitude = total_bid - 1.00 - estimated_fees
```

**IMPLICATION** (soft constraint, wider threshold):
```python
# Only flag if the mispricing is large AND confidence is high
if market_if.yes_bid > market_then.yes_ask + SOFT_THRESHOLD:
    # "If" event is priced higher than "then" event, but "if" implies "then"
    signal = "BUY_THEN_SELL_IF"
    magnitude = market_if.yes_bid - market_then.yes_ask - estimated_fees
```

### Fee Calculation

Kalshi's fee structure (verify current rates via API or docs):
- Taker fee: typically 1-2 cents per contract or a percentage
- Build a `calculate_fees(contracts, price)` function
- **Only flag opportunities where magnitude > 2x estimated fees** as a safety margin

### Opportunity Scoring

```python
score = magnitude * confidence * liquidity_factor

# liquidity_factor: min(depth_at_price across all legs) / desired_position_size
# A huge mispricing with 1 contract of depth is worse than a small one with 100
```

### Output

```sql
CREATE TABLE opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relationship_id INTEGER,
    signal TEXT,
    magnitude REAL,
    confidence REAL,
    score REAL,
    legs TEXT,  -- JSON: [{"ticker": "...", "side": "buy/sell", "price": ..., "depth": ...}]
    status TEXT,  -- DETECTED, VALIDATED, EXECUTING, FILLED, EXPIRED, FAILED
    detected_at TEXT,
    expires_at TEXT,
    FOREIGN KEY (relationship_id) REFERENCES relationships(id)
);
```

---

## Layer 4: Signal Enrichment (Optional for MVP)

### Responsibilities

- For high-scoring opportunities, pull external data to validate which side is mispriced
- Determine directional conviction beyond just "these two prices are inconsistent"

### Data Sources

1. **Polymarket API** — Compare equivalent market prices
   - `GET https://clob.polymarket.com/markets` or their CLOB API
   - Match by event description similarity

2. **FRED (Federal Reserve Economic Data)** — For macro markets
   - Free API: `https://api.stlouisfed.org/fred/`
   - Fed funds futures implied probabilities
   - Latest economic indicators

3. **CME FedWatch** — Fed rate decision probabilities implied by futures
   - Scrape from CME website or use their data feeds

4. **Yahoo Finance** — For market-related contracts
   - `yfinance` Python package for quick pulls

### LLM Enrichment

For IMPLICATION-type relationships, use Claude to assess the current probability:

```
Given these two Kalshi markets:
- Market A: "{title_a}" currently at {price_a} YES
- Market B: "{title_b}" currently at {price_b} YES

The identified relationship is: {relationship_description}

Given current macroeconomic conditions and the following data:
{external_data_summary}

1. Is the logical relationship still valid? (Yes/No + reasoning)
2. Which market appears more mispriced?
3. What would you estimate as fair prices for each?
4. Confidence level (1-10) in this assessment?
```

### MVP Skip

For the MVP, you can skip Layer 4 entirely and only trade:
- SUBSET violations (risk-free if relationship is correct)
- THRESHOLD violations (risk-free if relationship is correct)
- PARTITION violations where sum deviates significantly from $1.00

These don't need external enrichment because the constraint is mathematical, not probabilistic.

---

## Layer 5: Execution

### Responsibilities

- Convert validated opportunities into Kalshi orders
- Handle partial fills and execution risk
- Manage position sizing and portfolio risk

### Kalshi Order API

```
POST /portfolio/orders
Headers: { "Authorization": "Bearer {token}" }
Body: {
    "ticker": "...",
    "action": "buy" | "sell",
    "side": "yes" | "no",
    "type": "limit" | "market",
    "count": 10,
    "yes_price": 45,  // in cents, for limit orders
    "expiration_ts": 1234567890  // optional, GTC if omitted
}
```

**Response includes**: order_id, status, remaining_count, etc.

```
GET /portfolio/orders/{order_id}  — Check order status
DELETE /portfolio/orders/{order_id}  — Cancel order
GET /portfolio/positions  — Current positions
```

### Execution Strategy

#### For SUBSET/THRESHOLD (two-leg trades):

```python
async def execute_two_leg(opportunity):
    # 1. Place the harder-to-fill leg first (less liquid side)
    leg1 = place_limit_order(
        ticker=opportunity.legs[0].ticker,
        side=opportunity.legs[0].side,
        price=opportunity.legs[0].price,
        count=position_size,
        expiration=30_seconds_from_now  # short expiry
    )

    # 2. Wait for fill confirmation
    if not wait_for_fill(leg1, timeout=30):
        cancel_order(leg1.order_id)
        log("Leg 1 not filled, aborting")
        return

    # 3. Immediately place leg 2 at market or aggressive limit
    leg2 = place_limit_order(
        ticker=opportunity.legs[1].ticker,
        side=opportunity.legs[1].side,
        price=opportunity.legs[1].price + 1,  # 1 cent more aggressive
        count=leg1.filled_count,  # match filled quantity
        expiration=30_seconds_from_now
    )

    # 4. If leg 2 doesn't fill, we have directional risk — handle it
    if not wait_for_fill(leg2, timeout=30):
        log("WARNING: Leg 2 not filled, holding directional position")
        # Option: widen price until filled, or hold and monitor
```

#### For PARTITION (multi-leg):

```python
async def execute_partition(opportunity):
    # Place all legs simultaneously as limit orders
    orders = []
    for leg in opportunity.legs:
        order = place_limit_order(
            ticker=leg.ticker,
            side=leg.side,
            price=leg.price,
            count=position_size,
            expiration=60_seconds_from_now
        )
        orders.append(order)

    # Wait and check fills
    await asyncio.sleep(60)

    filled = [o for o in orders if check_fill(o)]
    unfilled = [o for o in orders if not check_fill(o)]

    if unfilled:
        # Cancel unfilled orders
        for o in unfilled:
            cancel_order(o.order_id)
        # We now have partial fills — log and decide whether to unwind
        log(f"Partial fill: {len(filled)}/{len(orders)} legs")
```

### Position Sizing

```python
def calculate_position_size(opportunity, portfolio):
    max_risk_per_trade = portfolio.balance * 0.02  # 2% max risk per opportunity
    max_contracts = min(
        int(max_risk_per_trade / opportunity.magnitude),
        min(leg.depth for leg in opportunity.legs),  # limited by liquidity
        100  # hard cap per trade
    )
    return max_contracts
```

### Safety Controls

- **Kill switch**: Global boolean in config that halts all trading
- **Max daily loss**: Stop trading if daily P&L drops below threshold (e.g., -$50)
- **Max open positions**: Limit total number of concurrent open position sets
- **Max position per market**: Never hold more than N contracts in any single market
- **Dry run mode**: Log all trades without executing (ESSENTIAL for testing)
- **Alerting**: Send notifications (email/Telegram/Discord webhook) on trades and errors

---

## Config File

```yaml
# config.yaml

kalshi:
  email: "${KALSHI_EMAIL}"
  password: "${KALSHI_PASSWORD}"
  base_url: "https://trading-api.kalshi.com/trade-api/v2"
  # Use demo API for testing:
  # base_url: "https://demo-api.kalshi.co/trade-api/v2"

anthropic:
  api_key: "${ANTHROPIC_API_KEY}"
  scan_model: "claude-sonnet-4-5-20250929"
  validate_model: "claude-opus-4-6"

trading:
  dry_run: true  # SET TO TRUE UNTIL FULLY TESTED
  max_risk_per_trade_pct: 0.02
  max_daily_loss: 50.00
  max_open_positions: 10
  max_contracts_per_trade: 50
  min_score_threshold: 0.05  # minimum magnitude after fees
  fee_safety_multiplier: 2.0  # only trade if profit > 2x fees

scanning:
  full_scan_interval_seconds: 60
  opportunity_recheck_seconds: 15
  relationship_rescan_hours: 24

logging:
  level: "INFO"
  file: "logs/trader.log"
  trade_log: "logs/trades.log"

alerts:
  enabled: false
  webhook_url: "${ALERT_WEBHOOK_URL}"
```

---

## File Structure

```
kalshi-mispricing-bot/
├── main.py                  # Entry point, orchestrator loop
├── config.yaml              # Trading parameters
├── .env                     # API keys (gitignored)
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── kalshi_client.py     # Kalshi API wrapper (auth, markets, orders)
│   ├── ingestion.py         # Layer 1: market data pulling + caching
│   ├── relationship.py      # Layer 2: LLM-powered relationship mapping
│   ├── detector.py          # Layer 3: constraint violation scanning
│   ├── enrichment.py        # Layer 4: external data + LLM validation
│   ├── executor.py          # Layer 5: order placement + management
│   ├── portfolio.py         # Position tracking, P&L, risk limits
│   ├── fees.py              # Fee calculation logic
│   ├── alerts.py            # Notification system
│   └── db.py                # SQLite helpers
├── logs/
│   ├── trader.log
│   └── trades.log
├── tests/
│   ├── test_detector.py     # Unit tests for constraint checking
│   ├── test_fees.py
│   └── test_mock_trades.py  # Paper trading simulation
└── scripts/
    ├── scan_once.py          # One-shot scan for debugging
    ├── backtest.py           # Replay historical prices against detector
    └── dashboard.py          # Simple CLI dashboard showing opportunities
```

---

## Implementation Order

### Phase 1: Foundation (Day 1-2)
1. `kalshi_client.py` — Auth, fetch markets, fetch orderbook
2. `db.py` — SQLite setup, insert/query helpers
3. `ingestion.py` — Full market pull, store in DB
4. `scan_once.py` — Verify you can pull and display all markets

### Phase 2: Brain (Day 3-4)
5. `relationship.py` — LLM-powered relationship discovery
6. `detector.py` — Constraint violation scanning
7. `fees.py` — Fee calculation
8. Run full scan → relationship mapping → violation detection pipeline
9. **Manually review detected opportunities to validate correctness**

### Phase 3: Execution (Day 5-6)
10. `executor.py` — Order placement in DRY RUN mode
11. `portfolio.py` — Position and risk tracking
12. `main.py` — Full orchestrator loop
13. Run in dry run mode for 48+ hours, review all "would have traded" logs

### Phase 4: Go Live (Day 7+)
14. Fund Kalshi account with small amount ($100-200)
15. Set conservative limits (max 5 contracts per trade)
16. Switch `dry_run: false`
17. Monitor closely for first week
18. Tune thresholds based on real execution data

---

## Critical Warnings

1. **START IN DRY RUN MODE.** Do not set `dry_run: false` until you've reviewed at least 48 hours of simulated trades and confirmed the logic is sound.

2. **Read Kalshi settlement rules for EVERY market you trade.** The LLM might identify a relationship that looks valid but breaks due to quirky settlement definitions. Always store and review `rules_primary` from the API.

3. **Kalshi has a demo/sandbox API.** Use `https://demo-api.kalshi.co/trade-api/v2` for all development and testing. Switch to production only when ready.

4. **Partial fill risk is real.** A two-leg trade where only one leg fills leaves you with directional exposure. The executor MUST handle this gracefully.

5. **LLM hallucination risk.** The relationship mapper might confidently assert a constraint that doesn't hold. Always validate SUBSET and THRESHOLD relationships manually before enabling auto-trading on them. Consider requiring human approval for the first N trades of each new relationship.

6. **Start with hard constraints only.** SUBSET, THRESHOLD, and PARTITION violations are mathematically provable (if the relationship is correctly identified). IMPLICATION trades are probabilistic — only add those after the core system is proven.

7. **Monitor your Kalshi account balance independently.** Don't rely solely on your bot's P&L tracking. Cross-reference with Kalshi's portfolio endpoint regularly.

---

## Testing Checklist Before Going Live

- [ ] Full market ingestion runs without errors for 24+ hours
- [ ] Relationship mapper produces sensible results (manually review ALL relationships)
- [ ] Detector correctly identifies known test cases (create synthetic test data)
- [ ] Fee calculation matches Kalshi's actual fee schedule
- [ ] Dry run trades log correctly with proper sizing
- [ ] Kill switch works immediately
- [ ] Max daily loss circuit breaker triggers correctly
- [ ] Partial fill handling doesn't leave orphaned positions
- [ ] Alert system sends notifications
- [ ] Auth token refresh works (tokens expire)
- [ ] Rate limiting / backoff works under sustained polling
