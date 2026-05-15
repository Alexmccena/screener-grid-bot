from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import socket
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from ..models import Candle, ExchangeName, Instrument, MarketSnapshot
from .rate_limiter import RateLimiter

LOGGER = logging.getLogger(__name__)


class ExchangeApiError(RuntimeError):
    def __init__(self, exchange: ExchangeName, status: int | None, message: str) -> None:
        super().__init__(f"{exchange.value}: {message}")
        self.exchange = exchange
        self.status = status


@dataclass(frozen=True)
class HttpTransport:
    exchange: ExchangeName
    base_url: str
    limiter: RateLimiter
    timeout_seconds: int = 8
    max_retries: int = 1

    async def get_json(
        self,
        path: str,
        params: Mapping[str, object | None] | None = None,
    ) -> Any:
        query_params = {
            key: value
            for key, value in (params or {}).items()
            if value is not None
        }
        query = urllib.parse.urlencode(query_params)
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        for attempt in range(self.max_retries + 1):
            await self.limiter.acquire()
            try:
                return await asyncio.to_thread(self._get_json_sync, url)
            except ExchangeApiError as exc:
                if exc.status in {403, 418, 429}:
                    self.limiter.penalize(10 * (attempt + 1))
                if attempt >= self.max_retries:
                    raise
                await asyncio.sleep(2**attempt)

    def _get_json_sync(self, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "telegram-oi-volume-screener/0.1",
                "Accept": "application/json",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ExchangeApiError(self.exchange, exc.code, _compact_error_body(exc.code, body)) from exc
        except urllib.error.URLError as exc:
            raise ExchangeApiError(self.exchange, None, _compact_network_error(exc.reason)) from exc
        except (TimeoutError, socket.timeout, OSError) as exc:
            raise ExchangeApiError(self.exchange, None, _compact_network_error(exc)) from exc
        return json.loads(payload)


class BaseExchangeClient(ABC):
    exchange: ExchangeName

    @abstractmethod
    async def get_instruments(self) -> dict[str, Instrument]:
        raise NotImplementedError

    @abstractmethod
    async def get_ticker_snapshots(self) -> dict[str, MarketSnapshot]:
        raise NotImplementedError

    @abstractmethod
    async def get_klines(self, symbol: str, limit: int = 180) -> list[Candle]:
        raise NotImplementedError

    @abstractmethod
    async def enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        raise NotImplementedError

    async def get_open_interest_history(
        self,
        symbol: str,
        limit: int = 30,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[MarketSnapshot]:
        return []

    async def get_klines_history(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> list[Candle]:
        return await self.get_klines(symbol)


def decimal_from(value: object, default: Decimal | None = None) -> Decimal | None:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    return Decimal(text)


def funding_fraction_to_pct(value: object) -> Decimal | None:
    raw = decimal_from(value)
    if raw is None:
        return None
    return raw * Decimal("100")


def _compact_error_body(status: int, body: str) -> str:
    if "<TITLE>ERROR:" in body or "<H1>403 ERROR</H1>" in body or "Request blocked" in body:
        if status == 403:
            return "403 Request blocked"
        return f"{status} HTTP error"
    text = " ".join(body.replace("\r", " ").replace("\n", " ").split())
    return text[:180] or f"{status} HTTP error"


def _compact_network_error(error: BaseException) -> str:
    text = str(error)
    if "handshake operation timed out" in text:
        return "SSL handshake timed out"
    if "read operation timed out" in text:
        return "read timed out"
    if "timed out" in text.lower():
        return "network timed out"
    return text[:180]


def ms_to_datetime(ms: object):
    from datetime import UTC, datetime

    return datetime.fromtimestamp(int(str(ms)) / 1000, tz=UTC)
