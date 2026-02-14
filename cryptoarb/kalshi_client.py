"""
Self-contained Kalshi API client for the LIVE exchange.

Identical auth logic (RSA-PSS) but reads from LIVE_KALSHI_* env vars
so it can run alongside the demo bot without interference.
"""

import os
import time
import base64
import logging
from typing import Any
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from . import config

logger = logging.getLogger(__name__)


class _RateLimiter:
    """Simple token-bucket rate limiter (10 req/s)."""

    def __init__(self, rate: float = 10.0):
        self._rate = rate
        self._tokens = rate
        self._last = time.monotonic()

    def acquire(self):
        now = time.monotonic()
        elapsed = now - self._last
        self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
        self._last = now
        if self._tokens < 1:
            sleep_time = (1 - self._tokens) / self._rate
            time.sleep(sleep_time)
            self._tokens = 0
        else:
            self._tokens -= 1


class KalshiClient:
    """Authenticated Kalshi REST API v2 client (live exchange)."""

    def __init__(self):
        self.api_key_id = config.KALSHI_API_KEY_ID
        if not self.api_key_id:
            raise RuntimeError("LIVE_KALSHI_API_KEY_ID not set")

        self.base_url = config.KALSHI_BASE_URL.rstrip("/")

        raw_pem = config.KALSHI_RSA_PRIVATE_KEY
        if not raw_pem:
            raise RuntimeError("LIVE_KALSHI_RSA_PRIVATE_KEY not set")
        raw_pem = raw_pem.replace("\\n", "\n")
        self._private_key = serialization.load_pem_private_key(
            raw_pem.encode(), password=None
        )

        self._session = requests.Session()
        self._limiter = _RateLimiter(rate=10.0)
        self._max_retries = 3

    # ----- auth -----

    def _sign(self, timestamp_ms: int, method: str, full_path: str) -> str:
        path_no_query = full_path.split("?")[0]
        message = f"{timestamp_ms}{method}{path_no_query}".encode("utf-8")
        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _auth_headers(self, method: str, full_path: str) -> dict:
        ts = int(time.time() * 1000)
        sig = self._sign(ts, method, full_path)
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(ts),
            "KALSHI-ACCESS-SIGNATURE": sig,
            "Content-Type": "application/json",
        }

    # ----- low-level request -----

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        full_path = urlparse(url).path
        headers = self._auth_headers(method.upper(), full_path)

        backoff = 1
        for attempt in range(1, self._max_retries + 1):
            self._limiter.acquire()
            try:
                resp = self._session.request(
                    method, url, headers=headers, params=params,
                    json=json_body, timeout=15,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", backoff))
                    logger.warning("Rate limited, sleeping %ss (attempt %d)", retry_after, attempt)
                    time.sleep(retry_after)
                    backoff *= 2
                    continue
                resp.raise_for_status()
                return resp.json() if resp.content else {}
            except requests.exceptions.RequestException as exc:
                if attempt == self._max_retries:
                    logger.error("Request failed after %d attempts: %s %s – %s",
                                 self._max_retries, method, path, exc)
                    raise
                logger.warning("Request error (attempt %d): %s – retrying in %ss",
                               attempt, exc, backoff)
                time.sleep(backoff)
                backoff *= 2

    # =====================================================================
    # Market data
    # =====================================================================

    def get_event(self, event_ticker: str) -> dict:
        """Fetch a single event with its markets."""
        data = self._request("GET", f"/events/{event_ticker}")
        return data.get("event", data)

    def get_events(self, status: str = "open", limit: int = 200,
                   cursor: str | None = None) -> dict:
        params: dict = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/events", params=params)

    def get_all_events(self, status: str = "open") -> list[dict]:
        all_events: list[dict] = []
        cursor = None
        while True:
            data = self.get_events(status=status, cursor=cursor)
            events = data.get("events", [])
            all_events.extend(events)
            cursor = data.get("cursor")
            if not cursor or not events:
                break
        logger.info("Fetched %d events (status=%s)", len(all_events), status)
        return all_events

    def get_market(self, ticker: str) -> dict:
        data = self._request("GET", f"/markets/{ticker}")
        return data.get("market", data)

    def get_markets(self, event_ticker: str | None = None,
                    status: str = "open", limit: int = 200,
                    cursor: str | None = None) -> dict:
        params: dict = {"status": status, "limit": limit}
        if event_ticker:
            params["event_ticker"] = event_ticker
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/markets", params=params)

    def get_orderbook(self, ticker: str) -> dict:
        return self._request("GET", f"/markets/{ticker}/orderbook")

    # =====================================================================
    # Portfolio / Trading
    # =====================================================================

    def get_balance(self) -> dict:
        return self._request("GET", "/portfolio/balance")

    def place_order(
        self,
        ticker: str,
        action: str,
        side: str,
        order_type: str,
        count: int,
        yes_price: int | None = None,
    ) -> dict:
        body: dict = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "type": order_type,
            "count": count,
        }
        if yes_price is not None:
            body["yes_price"] = yes_price
        logger.info("Placing order: %s", body)
        return self._request("POST", "/portfolio/orders", json_body=body)

    def get_order(self, order_id: str) -> dict:
        return self._request("GET", f"/portfolio/orders/{order_id}")

    def cancel_order(self, order_id: str) -> dict:
        return self._request("DELETE", f"/portfolio/orders/{order_id}")
