#!/usr/bin/env python3
"""
Kalshi Cross-Market Mispricing Detector & Trader
=================================================
Main orchestrator loop.

Cycle structure:
    - Every 60s:  Full market ingestion
    - Every 15s:  Opportunity detection scan
    - Every 24h:  Relationship re-discovery (event pass)
    - Every 72h:  Relationship re-discovery (category pass)
    - On detection: Enrichment + execution pipeline
    - On shutdown:  Graceful cleanup
"""

import os
import sys
import time
import signal
import logging
import traceback
from datetime import datetime, timezone, timedelta

import yaml

from src import db
from src.kalshi_client import KalshiClient
from src.ingestion import ingest_all
from src.relationship import discover_relationships, cleanup_stale_relationships
from src.detector import scan_for_violations
from src.enrichment import enrich_opportunity
from src.executor import Executor
from src.portfolio import Portfolio
from src import alerts

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(level: str = "INFO", log_file: str | None = None):
    """Configure root logger."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(path: str = "config.yaml") -> dict:
    """Load config from YAML, falling back to sensible defaults."""
    defaults = {
        "trading": {
            "dry_run": True,
            "max_risk_per_trade_pct": 0.02,
            "max_daily_loss": 50.0,
            "max_open_positions": 10,
            "max_contracts_per_trade": 50,
            "min_score_threshold": 0.05,
            "fee_safety_multiplier": 2.0,
        },
        "scanning": {
            "full_scan_interval_seconds": 60,
            "opportunity_recheck_seconds": 15,
            "relationship_rescan_hours": 24,
        },
        "logging": {
            "level": "INFO",
            "file": None,
        },
    }

    if os.path.exists(path):
        with open(path) as f:
            file_cfg = yaml.safe_load(f) or {}
        # Merge file config over defaults
        for section, values in file_cfg.items():
            if isinstance(values, dict) and section in defaults:
                defaults[section].update(values)
            else:
                defaults[section] = values

    return defaults


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class Orchestrator:
    """Main bot loop."""

    def __init__(self, config: dict):
        self.config = config
        self.running = False

        # Timing state
        self._last_ingest = datetime.min.replace(tzinfo=timezone.utc)
        self._last_scan = datetime.min.replace(tzinfo=timezone.utc)
        self._last_rel_event = datetime.min.replace(tzinfo=timezone.utc)
        self._last_rel_category = datetime.min.replace(tzinfo=timezone.utc)
        self._last_daily_summary = datetime.min.replace(tzinfo=timezone.utc)

        # Counters for daily summary
        self._opportunities_today = 0
        self._trades_today = 0

        # Components
        self.client = KalshiClient()
        trading_cfg = config.get("trading", {})
        self.portfolio = Portfolio(
            client=self.client,
            max_risk_per_trade_pct=trading_cfg.get("max_risk_per_trade_pct", 0.02),
            max_daily_loss=trading_cfg.get("max_daily_loss", 50.0),
            max_open_positions=trading_cfg.get("max_open_positions", 10),
            max_contracts_per_trade=trading_cfg.get("max_contracts_per_trade", 50),
            fee_safety_multiplier=trading_cfg.get("fee_safety_multiplier", 2.0),
        )
        self.executor = Executor(
            client=self.client,
            portfolio=self.portfolio,
            dry_run=trading_cfg.get("dry_run", True),
        )

        self.scan_cfg = config.get("scanning", {})

    def run(self):
        """Start the main loop."""
        self.running = True
        logger = logging.getLogger(__name__)

        logger.info("=== Kalshi Mispricing Bot starting ===")
        logger.info("Dry run: %s", self.executor.dry_run)

        # Init DB
        db.init_db()

        # Initial portfolio sync
        try:
            self.portfolio.sync()
        except Exception as e:
            logger.warning("Initial portfolio sync failed (expected on demo): %s", e)

        alerts.send_startup_alert()

        try:
            while self.running:
                now = datetime.now(timezone.utc)
                self._tick(now)
                time.sleep(1)  # Tight loop with 1s resolution
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.critical("Fatal error: %s", e)
            logger.critical(traceback.format_exc())
            alerts.send_error_alert("Fatal error", traceback.format_exc())
        finally:
            self.shutdown()

    def shutdown(self):
        """Graceful shutdown."""
        logger = logging.getLogger(__name__)
        self.running = False
        logger.info("Shutting down...")
        alerts.send_shutdown_alert("normal")
        db.close_pool()
        logger.info("Goodbye.")

    def _tick(self, now: datetime):
        """Single iteration of the main loop."""
        logger = logging.getLogger(__name__)
        scan_cfg = self.scan_cfg

        ingest_interval = timedelta(seconds=scan_cfg.get("full_scan_interval_seconds", 60))
        scan_interval = timedelta(seconds=scan_cfg.get("opportunity_recheck_seconds", 15))
        rel_interval = timedelta(hours=scan_cfg.get("relationship_rescan_hours", 24))
        category_interval = timedelta(hours=scan_cfg.get("relationship_rescan_hours", 24) * 3)

        # 1. Market ingestion
        if now - self._last_ingest >= ingest_interval:
            try:
                ingest_all(self.client)
                self._last_ingest = now
            except Exception as e:
                logger.error("Ingestion failed: %s", e)
                alerts.send_error_alert("Ingestion failed", str(e))

        # 2. Relationship discovery (event pass)
        if now - self._last_rel_event >= rel_interval:
            try:
                new = discover_relationships(pass_type="event")
                logger.info("Event-pass relationship discovery: %d new", new)
                cleanup_stale_relationships()
                self._last_rel_event = now
            except Exception as e:
                logger.error("Relationship event-pass failed: %s", e)
                alerts.send_error_alert("Relationship discovery failed", str(e))

        # 3. Relationship discovery (category pass – less frequent)
        if now - self._last_rel_category >= category_interval:
            try:
                new = discover_relationships(pass_type="category")
                logger.info("Category-pass relationship discovery: %d new", new)
                self._last_rel_category = now
            except Exception as e:
                logger.error("Relationship category-pass failed: %s", e)

        # 4. Opportunity detection
        if now - self._last_scan >= scan_interval:
            try:
                trading_cfg = self.config.get("trading", {})
                opportunities = scan_for_violations(
                    min_score=trading_cfg.get("min_score_threshold", 0.05),
                    fee_safety_multiplier=trading_cfg.get("fee_safety_multiplier", 2.0),
                )
                self._last_scan = now

                for opp in opportunities:
                    self._opportunities_today += 1
                    alerts.send_opportunity_alert(opp)

                    # Enrichment (optional, skip on failure)
                    try:
                        # Fetch market data for enrichment
                        legs = opp.get("legs", [])
                        market_data = {}
                        with db.get_conn() as conn:
                            with db.get_cursor(conn) as cur:
                                for leg in legs:
                                    m = db.get_market(cur, leg["ticker"])
                                    if m:
                                        market_data[leg["ticker"]] = dict(m)

                        enrichment = enrich_opportunity(opp, market_data)
                        logger.info("Enrichment for opp %s: %s", opp.get("id"), enrichment)

                        # If LLM says relationship is no longer valid, skip
                        llm = enrichment.get("llm_assessment")
                        if llm and not llm.get("relationship_still_valid", True):
                            logger.info("LLM invalidated relationship for opp %s, skipping", opp.get("id"))
                            with db.get_conn() as conn:
                                with db.get_cursor(conn) as cur:
                                    db.update_opportunity_status(cur, opp["id"], "EXPIRED")
                            continue
                    except Exception as e:
                        logger.warning("Enrichment failed for opp %s (proceeding anyway): %s", opp.get("id"), e)

                    # Execute
                    success = self.executor.execute_opportunity(opp)
                    if success:
                        self._trades_today += 1
                        # Send trade alerts for each leg
                        legs = opp.get("legs", [])
                        for leg in legs:
                            alerts.send_trade_alert(
                                {
                                    "action": leg.get("side"),
                                    "ticker": leg.get("ticker"),
                                    "side": "yes",
                                    "count": self.portfolio.calculate_position_size(opp),
                                    "price": leg.get("price", 0),
                                    "fees": 0,
                                    "order_id": "see-trades-table",
                                    "order_status": "dry_run" if self.executor.dry_run else "placed",
                                },
                                dry_run=self.executor.dry_run,
                            )

            except Exception as e:
                logger.error("Detection/execution cycle failed: %s", e)
                alerts.send_error_alert("Detection cycle failed", str(e))

        # 5. Portfolio sync (every ingestion cycle)
        if now - self._last_ingest < timedelta(seconds=2):
            try:
                self.portfolio.sync()
            except Exception as e:
                logger.warning("Portfolio sync failed: %s", e)

        # 6. Daily summary (once per day around midnight UTC)
        if now - self._last_daily_summary >= timedelta(hours=24):
            try:
                alerts.send_daily_summary(
                    self.portfolio.summary(),
                    opportunities_today=self._opportunities_today,
                    trades_today=self._trades_today,
                )
                self._opportunities_today = 0
                self._trades_today = 0
                self._last_daily_summary = now
            except Exception as e:
                logger.warning("Daily summary failed: %s", e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    config = load_config()
    log_cfg = config.get("logging", {})
    setup_logging(level=log_cfg.get("level", "INFO"), log_file=log_cfg.get("file"))

    orchestrator = Orchestrator(config)

    # Graceful shutdown on SIGTERM (Railway sends this)
    def handle_signal(signum, frame):
        logging.getLogger(__name__).info("Received signal %d", signum)
        orchestrator.running = False

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    orchestrator.run()


if __name__ == "__main__":
    main()
