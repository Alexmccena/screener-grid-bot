from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ..models import Candle, ExchangeName, Instrument, MarketSnapshot, utc_now
from ..normalizer import estimate_oi_value_usdt, normalize_symbol
from .base import BaseExchangeClient, ExchangeApiError, HttpTransport, decimal_from, funding_fraction_to_pct, ms_to_datetime
from .rate_limiter import RateLimiter


BYBIT_INTERVALS = {"1m": "1", "5m": "5"}


class BybitClient(BaseExchangeClient):
    exchange = ExchangeName.BYBIT

    def __init__(self, base_url: str = "https://api.bybit.com") -> None:
        self.transport = HttpTransport(
            exchange=self.exchange,
            base_url=base_url,
            limiter=RateLimiter(max_calls=600, period_seconds=60),
        )

    async def get_instruments(self) -> dict[str, Instrument]:
        payload = await self.transport.get_json(
            "/v5/market/instruments-info",
            {"category": "linear", "status": "Trading"},
        )
        self._ensure_ok(payload)
        instruments: dict[str, Instrument] = {}
        for item in payload.get("result", {}).get("list", []):
            if item.get("quoteCoin") != "USDT":
                continue
            symbol = normalize_symbol(self.exchange, str(item["symbol"]))
            instruments[symbol] = Instrument(
                exchange=self.exchange,
                symbol=symbol,
                exchange_symbol=str(item["symbol"]),
                base_asset=str(item.get("baseCoin", symbol.removesuffix("USDT"))),
                quote_asset="USDT",
            )
        return instruments

    async def get_ticker_snapshots(self) -> dict[str, MarketSnapshot]:
        payload = await self.transport.get_json("/v5/market/tickers", {"category": "linear"})
        self._ensure_ok(payload)
        snapshots: dict[str, MarketSnapshot] = {}
        now = utc_now()
        for item in payload.get("result", {}).get("list", []):
            exchange_symbol = str(item.get("symbol", ""))
            if not exchange_symbol.endswith("USDT"):
                continue
            symbol = normalize_symbol(self.exchange, exchange_symbol)
            price = decimal_from(item.get("lastPrice"))
            oi = decimal_from(item.get("openInterest"))
            oi_value = decimal_from(item.get("openInterestValue"))
            snapshots[symbol] = MarketSnapshot(
                exchange=self.exchange,
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                timestamp=now,
                price=price,
                open_interest=oi,
                open_interest_value_usdt=oi_value or estimate_oi_value_usdt(oi, price),
                open_interest_value_estimated=oi_value is None,
                volume_24h_usdt=decimal_from(item.get("turnover24h")),
                price_change_24h_pct=_bybit_price_change_pct(item.get("price24hPcnt")),
                funding_rate_pct=funding_fraction_to_pct(item.get("fundingRate")),
                source="rest:ticker",
                raw=item,
            )
        return snapshots

    async def get_klines(self, symbol: str, limit: int = 180) -> list[Candle]:
        exchange_symbol = normalize_symbol(self.exchange, symbol)
        payload = await self.transport.get_json(
            "/v5/market/kline",
            {"category": "linear", "symbol": exchange_symbol, "interval": "1", "limit": limit},
        )
        self._ensure_ok(payload)
        candles: list[Candle] = []
        for item in payload.get("result", {}).get("list", []):
            candles.append(
                Candle(
                    exchange=self.exchange,
                    symbol=exchange_symbol,
                    open_time=ms_to_datetime(item[0]),
                    open=decimal_from(item[1], Decimal("0")) or Decimal("0"),
                    high=decimal_from(item[2], Decimal("0")) or Decimal("0"),
                    low=decimal_from(item[3], Decimal("0")) or Decimal("0"),
                    close=decimal_from(item[4], Decimal("0")) or Decimal("0"),
                    volume_usdt=decimal_from(item[6], Decimal("0")) or Decimal("0"),
                )
            )
        return sorted(candles, key=lambda candle: candle.open_time)

    async def get_klines_history(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> list[Candle]:
        exchange_symbol = normalize_symbol(self.exchange, symbol)
        api_interval = BYBIT_INTERVALS.get(interval, interval)
        candles: list[Candle] = []
        cursor = start
        step = _interval_delta(api_interval) * 999
        while cursor < end:
            chunk_end = min(cursor + step, end)
            payload = await self.transport.get_json(
                "/v5/market/kline",
                {
                    "category": "linear",
                    "symbol": exchange_symbol,
                    "interval": api_interval,
                    "start": int(cursor.timestamp() * 1000),
                    "end": int(chunk_end.timestamp() * 1000),
                    "limit": 1000,
                },
            )
            self._ensure_ok(payload)
            parsed = _parse_bybit_klines(exchange_symbol, payload.get("result", {}).get("list", []))
            candles.extend(parsed)
            if not parsed:
                cursor = chunk_end + _interval_delta(api_interval)
            else:
                cursor = parsed[-1].open_time + _interval_delta(api_interval)
        return _dedupe_candles(candles)

    async def enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        if snapshot.open_interest is not None and snapshot.funding_rate_pct is not None:
            return snapshot
        funding = snapshot.funding_rate_pct or await self.get_funding_rate_pct(snapshot.exchange_symbol)
        oi = snapshot.open_interest
        oi_value = snapshot.open_interest_value_usdt
        estimated = snapshot.open_interest_value_estimated
        if oi is None:
            oi, oi_value = await self.get_open_interest_latest(snapshot.exchange_symbol, snapshot.price)
            estimated = oi_value is not None
        return snapshot.with_updates(
            timestamp=datetime.now(UTC),
            open_interest=oi,
            open_interest_value_usdt=oi_value,
            open_interest_value_estimated=estimated,
            funding_rate_pct=funding,
            source=f"{snapshot.source}+rest:fallback",
        )

    async def get_open_interest_latest(
        self,
        exchange_symbol: str,
        price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal | None]:
        payload = await self.transport.get_json(
            "/v5/market/open-interest",
            {
                "category": "linear",
                "symbol": exchange_symbol,
                "intervalTime": "5min",
                "limit": 1,
            },
        )
        self._ensure_ok(payload)
        items = payload.get("result", {}).get("list", [])
        if not items:
            return None, None
        oi = decimal_from(items[0].get("openInterest"))
        return oi, estimate_oi_value_usdt(oi, price)

    async def get_open_interest_history(
        self,
        symbol: str,
        limit: int = 30,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[MarketSnapshot]:
        exchange_symbol = normalize_symbol(self.exchange, symbol)
        snapshots: list[MarketSnapshot] = []
        if start is None or end is None:
            payload = await self.transport.get_json(
                "/v5/market/open-interest",
                {
                    "category": "linear",
                    "symbol": exchange_symbol,
                    "intervalTime": "5min",
                    "limit": limit,
                },
            )
            self._ensure_ok(payload)
            snapshots.extend(
                _parse_bybit_oi_history(exchange_symbol, payload.get("result", {}).get("list", []))
            )
        else:
            page_cursor: str | None = None
            iterations = 0
            while True:
                iterations += 1
                if iterations > 100:
                    break
                payload = await self.transport.get_json(
                    "/v5/market/open-interest",
                    {
                        "category": "linear",
                        "symbol": exchange_symbol,
                        "intervalTime": "5min",
                        "startTime": int(start.timestamp() * 1000),
                        "endTime": int(end.timestamp() * 1000),
                        "limit": 200,
                        "cursor": page_cursor,
                    },
                )
                self._ensure_ok(payload)
                result = payload.get("result", {})
                parsed = _parse_bybit_oi_history(
                    exchange_symbol,
                    result.get("list", []),
                )
                snapshots.extend(
                    snapshot for snapshot in parsed if start <= snapshot.timestamp <= end
                )
                if not parsed:
                    break
                if parsed[0].timestamp <= start:
                    break
                next_cursor = result.get("nextPageCursor")
                if not next_cursor or next_cursor == page_cursor:
                    break
                page_cursor = str(next_cursor)
        deduped = _dedupe_snapshots(snapshots)
        return deduped[-limit:] if start is None else deduped

    async def get_funding_rate_pct(self, exchange_symbol: str) -> Decimal | None:
        payload = await self.transport.get_json(
            "/v5/market/funding/history",
            {"category": "linear", "symbol": exchange_symbol, "limit": 1},
        )
        self._ensure_ok(payload)
        items = payload.get("result", {}).get("list", [])
        if not items:
            return None
        return funding_fraction_to_pct(items[0].get("fundingRate"))

    def _ensure_ok(self, payload: dict[str, object]) -> None:
        if int(payload.get("retCode", 0)) != 0:
            raise ExchangeApiError(self.exchange, None, str(payload))


def _bybit_price_change_pct(value: object) -> Decimal | None:
    raw = decimal_from(value)
    if raw is None:
        return None
    return raw * Decimal("100")


def _parse_bybit_klines(exchange_symbol: str, payload: list[list[object]]) -> list[Candle]:
    candles: list[Candle] = []
    for item in payload:
        candles.append(
            Candle(
                exchange=ExchangeName.BYBIT,
                symbol=exchange_symbol,
                open_time=ms_to_datetime(item[0]),
                open=decimal_from(item[1], Decimal("0")) or Decimal("0"),
                high=decimal_from(item[2], Decimal("0")) or Decimal("0"),
                low=decimal_from(item[3], Decimal("0")) or Decimal("0"),
                close=decimal_from(item[4], Decimal("0")) or Decimal("0"),
                volume_usdt=decimal_from(item[6], Decimal("0")) or Decimal("0"),
            )
        )
    return sorted(candles, key=lambda candle: candle.open_time)


def _parse_bybit_oi_history(
    exchange_symbol: str,
    payload: list[dict[str, object]],
) -> list[MarketSnapshot]:
    return [
        MarketSnapshot(
            exchange=ExchangeName.BYBIT,
            symbol=exchange_symbol,
            exchange_symbol=exchange_symbol,
            timestamp=ms_to_datetime(item.get("timestamp")),
            open_interest=decimal_from(item.get("openInterest")),
            source="rest:oi_history",
            raw=item,
        )
        for item in payload
    ]


def _interval_delta(interval: str) -> timedelta:
    return timedelta(minutes=int(interval))


def _dedupe_candles(candles: list[Candle]) -> list[Candle]:
    by_time = {candle.open_time: candle for candle in candles}
    return [by_time[key] for key in sorted(by_time)]


def _dedupe_snapshots(snapshots: list[MarketSnapshot]) -> list[MarketSnapshot]:
    by_time = {snapshot.timestamp: snapshot for snapshot in snapshots}
    return [by_time[key] for key in sorted(by_time)]
