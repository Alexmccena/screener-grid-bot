from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ..models import Candle, ExchangeName, Instrument, MarketSnapshot, utc_now
from ..normalizer import estimate_oi_value_usdt, normalize_okx_symbol, okx_exchange_symbol
from .base import BaseExchangeClient, ExchangeApiError, HttpTransport, decimal_from, funding_fraction_to_pct, ms_to_datetime
from .rate_limiter import RateLimiter


OKX_INTERVALS = {"1m": "1m", "5m": "5m"}


class OKXClient(BaseExchangeClient):
    exchange = ExchangeName.OKX

    def __init__(self, base_url: str = "https://www.okx.com") -> None:
        self.transport = HttpTransport(
            exchange=self.exchange,
            base_url=base_url,
            limiter=RateLimiter(max_calls=300, period_seconds=60),
        )
        self._instruments: dict[str, Instrument] = {}

    async def get_instruments(self) -> dict[str, Instrument]:
        payload = await self.transport.get_json(
            "/api/v5/public/instruments",
            {"instType": "SWAP"},
        )
        self._ensure_ok(payload)
        instruments: dict[str, Instrument] = {}
        for item in payload.get("data", []):
            if item.get("settleCcy") != "USDT" and item.get("quoteCcy") != "USDT":
                continue
            if item.get("state") not in {None, "live"}:
                continue
            exchange_symbol = str(item["instId"])
            symbol = normalize_okx_symbol(exchange_symbol)
            instruments[symbol] = Instrument(
                exchange=self.exchange,
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                base_asset=str(item.get("baseCcy", symbol.removesuffix("USDT"))),
                quote_asset="USDT",
                contract_multiplier=decimal_from(item.get("ctVal"), Decimal("1")) or Decimal("1"),
            )
        self._instruments = instruments
        return instruments

    async def get_ticker_snapshots(self) -> dict[str, MarketSnapshot]:
        payload = await self.transport.get_json("/api/v5/market/tickers", {"instType": "SWAP"})
        self._ensure_ok(payload)
        snapshots: dict[str, MarketSnapshot] = {}
        now = utc_now()
        for item in payload.get("data", []):
            exchange_symbol = str(item.get("instId", ""))
            if not exchange_symbol.endswith("-USDT-SWAP"):
                continue
            symbol = normalize_okx_symbol(exchange_symbol)
            price = decimal_from(item.get("last"))
            snapshots[symbol] = MarketSnapshot(
                exchange=self.exchange,
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                timestamp=now,
                price=price,
                volume_24h_usdt=_okx_turnover_usdt(item, price),
                source="rest:ticker",
                raw=item,
            )
        return snapshots

    async def get_klines(self, symbol: str, limit: int = 180) -> list[Candle]:
        exchange_symbol = okx_exchange_symbol(symbol)
        payload = await self.transport.get_json(
            "/api/v5/market/candles",
            {"instId": exchange_symbol, "bar": "1m", "limit": limit},
        )
        self._ensure_ok(payload)
        candles: list[Candle] = []
        for item in payload.get("data", []):
            close = decimal_from(item[4], Decimal("0")) or Decimal("0")
            volume_usdt = decimal_from(item[7] if len(item) > 7 else None)
            if volume_usdt is None:
                volume_ccy = decimal_from(item[6] if len(item) > 6 else None, Decimal("0")) or Decimal("0")
                volume_usdt = volume_ccy * close
            candles.append(
                Candle(
                    exchange=self.exchange,
                    symbol=normalize_okx_symbol(exchange_symbol),
                    open_time=ms_to_datetime(item[0]),
                    open=decimal_from(item[1], Decimal("0")) or Decimal("0"),
                    high=decimal_from(item[2], Decimal("0")) or Decimal("0"),
                    low=decimal_from(item[3], Decimal("0")) or Decimal("0"),
                    close=close,
                    volume_usdt=volume_usdt,
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
        exchange_symbol = okx_exchange_symbol(symbol)
        api_interval = OKX_INTERVALS.get(interval, interval)
        candles: list[Candle] = []
        cursor = start
        step = _interval_delta(api_interval) * 299
        while cursor < end:
            chunk_end = min(cursor + step, end)
            payload = await self.transport.get_json(
                "/api/v5/market/history-candles",
                {
                    "instId": exchange_symbol,
                    "bar": api_interval,
                    "before": int(cursor.timestamp() * 1000),
                    "after": int(chunk_end.timestamp() * 1000),
                    "limit": 300,
                },
            )
            self._ensure_ok(payload)
            parsed = _parse_okx_candles(exchange_symbol, payload.get("data", []))
            candles.extend(parsed)
            if not parsed:
                cursor = chunk_end + _interval_delta(api_interval)
            else:
                cursor = parsed[-1].open_time + _interval_delta(api_interval)
        return _dedupe_candles(candles)

    async def enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        if not self._instruments:
            await self.get_instruments()
        oi, oi_value, estimated = await self.get_open_interest(
            snapshot.exchange_symbol,
            snapshot.price,
        )
        funding = await self.get_funding_rate_pct(snapshot.exchange_symbol)
        return snapshot.with_updates(
            timestamp=datetime.now(UTC),
            open_interest=oi,
            open_interest_value_usdt=oi_value,
            open_interest_value_estimated=estimated,
            funding_rate_pct=funding,
            source=f"{snapshot.source}+rest:oi+funding",
        )

    async def get_open_interest(
        self,
        exchange_symbol: str,
        price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal | None, bool]:
        payload = await self.transport.get_json(
            "/api/v5/public/open-interest",
            {"instType": "SWAP", "instId": exchange_symbol},
        )
        self._ensure_ok(payload)
        items = payload.get("data", [])
        if not items:
            return None, None, False
        item = items[0]
        oi = decimal_from(item.get("oi"))
        oi_ccy = decimal_from(item.get("oiCcy"))
        if oi_ccy is not None and price is not None:
            return oi_ccy, oi_ccy * price, True
        instrument = self._instruments.get(normalize_okx_symbol(exchange_symbol))
        return oi, estimate_oi_value_usdt(oi, price, instrument), True

    async def get_open_interest_history(
        self,
        symbol: str,
        limit: int = 30,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[MarketSnapshot]:
        exchange_symbol = okx_exchange_symbol(symbol)
        limited_latest = start is None or end is None
        snapshots: list[MarketSnapshot] = []
        if start is None or end is None:
            end = datetime.now(UTC)
            start = end - timedelta(minutes=max(limit * 5 + 30, 90))
        cursor = start
        step = timedelta(minutes=5 * 300)
        while cursor < end:
            chunk_end = min(cursor + step, end)
            payload = await self.transport.get_json(
                "/api/v5/rubik/stat/contracts/open-interest-history",
                {
                    "instType": "SWAP",
                    "instId": exchange_symbol,
                    "period": "5m",
                    "begin": int(cursor.timestamp() * 1000),
                    "end": int(chunk_end.timestamp() * 1000),
                },
            )
            self._ensure_ok(payload)
            parsed = [
                snapshot
                for item in payload.get("data", [])
                if (snapshot := _okx_oi_history_snapshot(item, exchange_symbol)) is not None
            ]
            parsed = _dedupe_snapshots(parsed)
            snapshots.extend(parsed)
            if not parsed:
                cursor = chunk_end + timedelta(minutes=5)
            else:
                cursor = parsed[-1].timestamp + timedelta(minutes=5)
        deduped = _dedupe_snapshots(snapshots)
        return deduped[-limit:] if limited_latest else deduped

    async def get_funding_rate_pct(self, exchange_symbol: str) -> Decimal | None:
        payload = await self.transport.get_json(
            "/api/v5/public/funding-rate",
            {"instId": exchange_symbol},
        )
        self._ensure_ok(payload)
        items = payload.get("data", [])
        if not items:
            return None
        return funding_fraction_to_pct(items[0].get("fundingRate"))

    def _ensure_ok(self, payload: dict[str, object]) -> None:
        if str(payload.get("code", "0")) != "0":
            raise ExchangeApiError(self.exchange, None, str(payload))


def _okx_turnover_usdt(item: dict[str, object], price: Decimal | None) -> Decimal | None:
    direct = decimal_from(item.get("volCcyQuote24h"))
    if direct is not None:
        return direct
    fallback = decimal_from(item.get("volCcy24h"))
    if fallback is not None and price is not None:
        return fallback * price
    return decimal_from(item.get("vol24h"))


def _parse_okx_candles(exchange_symbol: str, payload: list[list[object]]) -> list[Candle]:
    candles: list[Candle] = []
    for item in payload:
        close = decimal_from(item[4], Decimal("0")) or Decimal("0")
        volume_usdt = decimal_from(item[7] if len(item) > 7 else None)
        if volume_usdt is None:
            volume_ccy = decimal_from(item[6] if len(item) > 6 else None, Decimal("0")) or Decimal("0")
            volume_usdt = volume_ccy * close
        candles.append(
            Candle(
                exchange=ExchangeName.OKX,
                symbol=normalize_okx_symbol(exchange_symbol),
                open_time=ms_to_datetime(item[0]),
                open=decimal_from(item[1], Decimal("0")) or Decimal("0"),
                high=decimal_from(item[2], Decimal("0")) or Decimal("0"),
                low=decimal_from(item[3], Decimal("0")) or Decimal("0"),
                close=close,
                volume_usdt=volume_usdt,
            )
        )
    return sorted(candles, key=lambda candle: candle.open_time)


def _okx_oi_history_snapshot(
    item: object,
    exchange_symbol: str,
) -> MarketSnapshot | None:
    symbol = normalize_okx_symbol(exchange_symbol)
    if isinstance(item, list):
        if len(item) < 2:
            return None
        timestamp = item[0]
        oi = decimal_from(item[1])
        oi_ccy = decimal_from(item[2] if len(item) > 2 else None)
        oi_value = decimal_from(item[3] if len(item) > 3 else None)
    elif isinstance(item, dict):
        timestamp = item.get("ts") or item.get("timestamp")
        oi = decimal_from(item.get("oi"))
        oi_ccy = decimal_from(item.get("oiCcy"))
        oi_value = decimal_from(item.get("oiUsd") or item.get("oiValue"))
    else:
        return None
    if timestamp is None:
        return None
    return MarketSnapshot(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        exchange_symbol=exchange_symbol,
        timestamp=ms_to_datetime(timestamp),
        open_interest=oi_ccy or oi,
        open_interest_value_usdt=oi_value,
        open_interest_value_estimated=oi_value is None,
        source="rest:oi_history",
        raw={"raw": item},
    )


def _interval_delta(interval: str) -> timedelta:
    if interval.endswith("m"):
        return timedelta(minutes=int(interval.removesuffix("m")))
    if interval.endswith("H"):
        return timedelta(hours=int(interval.removesuffix("H")))
    raise ValueError(f"Unsupported interval: {interval}")


def _dedupe_candles(candles: list[Candle]) -> list[Candle]:
    by_time = {candle.open_time: candle for candle in candles}
    return [by_time[key] for key in sorted(by_time)]


def _dedupe_snapshots(snapshots: list[MarketSnapshot]) -> list[MarketSnapshot]:
    by_time = {snapshot.timestamp: snapshot for snapshot in snapshots}
    return [by_time[key] for key in sorted(by_time)]
