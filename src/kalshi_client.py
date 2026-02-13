"""
Kalshi API client with RSA-PSS signature authentication.

Kalshi v2 API requires three custom headers on every authenticated request:
    KALSHI-ACCESS-KEY       – API Key ID (UUID)
    KALSHI-ACCESS-TIMESTAMP – current time in ms since epoch
    KALSHI-ACCESS-SIGNATURE – RSA-PSS / SHA-256 signature of (timestamp + method + path)

Environment variables:
    KALSHI_API_KEY_ID        – the key UUID
    KALSHI_RSA_PRIVATE_KEY   – PEM-encoded RSA private key string
    KALSHI_BASE_URL          – e.g. https://demo-api.kalshi.co/trade-api/v2
"""

import os
import time
import base64
import logging
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate-limiter (token bucket – 10 requests/s)
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Simple token-bucket rate limiter."""

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


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class KalshiClient:
    """Authenticated Kalshi REST API v2 client."""

    def __init__(
        self,
        api_key_id: str | None = None,
        private_key_pem: str | None = None,
        base_url: str | None = None,
    ):
        self.api_key_id = api_key_id or os.environ["KALSHI_API_KEY_ID"]
        self.base_url = (base_url or os.environ.get(
            "KALSHI_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2"
        )).rstrip("/")

        raw_pem = private_key_pem or os.environ["KALSHI_RSA_PRIVATE_KEY"]
        # Handle escaped newlines from env vars
        raw_pem = raw_pem.replace("\\n", "\n")
        self._private_key = serialization.load_pem_private_key(
            raw_pem.encode(), password=None
        )

        self._session = requests.Session()
        self._limiter = _RateLimiter(rate=10.0)
        self._max_retries = 3

    # ----- auth helpers -----

    def _sign(self, timestamp_ms: int, method: str, path: str) -> str:
        """Create the RSA-PSS signature required by Kalshi."""
        message = f"{timestamp_ms}{method}{path}".encode()
        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _auth_headers(self, method: str, path: str) -> dict:
        ts = int(time.time() * 1000)
        sig = self._sign(ts, method, path)
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
        authenticated: bool = True,
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers = self._auth_headers(method.upper(), path) if authenticated else {
            "Content-Type": "application/json"
        }

        backoff = 1
        for attempt in range(1, self._max_retries + 1):
            self._limiter.acquire()
            try:
                resp = self._session.request(
                    method, url, headers=headers, params=params, json=json_body, timeout=30
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
                    logger.error("Request failed after %d attempts: %s %s – %s", self._max_retries, method, path, exc)
                    raise
                logger.warning("Request error (attempt %d): %s – retrying in %ss", attempt, exc, backoff)
                time.sleep(backoff)
                backoff *= 2

    # =====================================================================
    # Market data (public-ish, but we authenticate anyway)
    # =====================================================================

    def get_markets(self, status: str = "open", limit: int = 200, cursor: str | None = None) -> dict:
        """Fetch a page of markets. Returns {markets: [...], cursor: '...'}."""
        params: dict = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/markets", params=params)

    def get_all_markets(self, status: str = "open") -> list[dict]:
        """Page through all markets with the given status."""
        all_markets: list[dict] = []
        cursor = None
        while True:
            data = self.get_markets(status=status, cursor=cursor)
            markets = data.get("markets", [])
            all_markets.extend(markets)
            cursor = data.get("cursor")
            if not cursor or not markets:
                break
            logger.debug("Fetched %d markets so far (cursor=%s)", len(all_markets), cursor[:20])
        logger.info("Fetched %d total markets (status=%s)", len(all_markets), status)
        return all_markets

    def get_market(self, ticker: str) -> dict:
        """Fetch a single market by ticker."""
        data = self._request("GET", f"/markets/{ticker}")
        return data.get("market", data)

    def get_orderbook(self, ticker: str) -> dict:
        """Fetch the order book for a market."""
        return self._request("GET", f"/markets/{ticker}/orderbook")

    def get_events(self, status: str = "open", limit: int = 200, cursor: str | None = None) -> dict:
        """Fetch a page of events."""
        params: dict = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/events", params=params)

    def get_all_events(self, status: str = "open") -> list[dict]:
        """Page through all events."""
        all_events: list[dict] = []
        cursor = None
        while True:
            data = self.get_events(status=status, cursor=cursor)
            events = data.get("events", [])
            all_events.extend(events)
            cursor = data.get("cursor")
            if not cursor or not events:
                break
        logger.info("Fetched %d total events (status=%s)", len(all_events), status)
        return all_events

    def get_event(self, event_ticker: str) -> dict:
        """Fetch a single event."""
        data = self._request("GET", f"/events/{event_ticker}")
        return data.get("event", data)

    # =====================================================================
    # Portfolio / Trading
    # =====================================================================

    def get_balance(self) -> dict:
        """Get account balance."""
        return self._request("GET", "/portfolio/balance")

    def get_positions(self, limit: int = 200, cursor: str | None = None) -> dict:
        """Fetch open positions."""
        params: dict = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/portfolio/positions", params=params)

    def place_order(
        self,
        ticker: str,
        action: str,      # "buy" | "sell"
        side: str,         # "yes" | "no"
        order_type: str,   # "limit" | "market"
        count: int,
        yes_price: int | None = None,   # cents, for limit orders
        expiration_ts: int | None = None,
    ) -> dict:
        """Place an order on Kalshi."""
        body: dict = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "type": order_type,
            "count": count,
        }
        if yes_price is not None:
            body["yes_price"] = yes_price
        if expiration_ts is not None:
            body["expiration_ts"] = expiration_ts

        logger.info("Placing order: %s", body)
        return self._request("POST", "/portfolio/orders", json_body=body)

    def get_order(self, order_id: str) -> dict:
        """Get order status."""
        return self._request("GET", f"/portfolio/orders/{order_id}")

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an order."""
        return self._request("DELETE", f"/portfolio/orders/{order_id}")

    def get_fills(self, limit: int = 200, cursor: str | None = None) -> dict:
        """Fetch recent fills."""
        params: dict = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/portfolio/fills", params=params)
