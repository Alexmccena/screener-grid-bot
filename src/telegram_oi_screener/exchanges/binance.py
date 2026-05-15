from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ..models import Candle, ExchangeName, Instrument, MarketSnapshot, utc_now
from ..normalizer import estimate_oi_value_usdt, normalize_symbol
from .base import BaseExchangeClient, HttpTransport, decimal_from, funding_fraction_to_pct, ms_to_datetime
from .rate_limiter import RateLimiter


BINANCE_INTERVALS = {"1m": "1m", "5m": "5m"}


class BinanceClient(BaseExchangeClient):
    exchange = ExchangeName.BINANCE

    def __init__(self, base_url: str = "https://fapi.binance.com") -> None:
        self.transport = HttpTransport(
            exchange=self.exchange,
            base_url=base_url,
            limiter=RateLimiter(max_calls=900, period_seconds=60),
        )

    async def get_instruments(self) -> dict[str, Instrument]:
        payload = await self.transport.get_json("/fapi/v1/exchangeInfo")
        instruments: dict[str, Instrument] = {}
        for item in payload.get("symbols", []):
            if (
                item.get("contractType") == "PERPETUAL"
                and item.get("status") == "TRADING"
                and item.get("quoteAsset") == "USDT"
            ):
                symbol = normalize_symbol(self.exchange, str(item["symbol"]))
                instruments[symbol] = Instrument(
                    exchange=self.exchange,
                    symbol=symbol,
                    exchange_symbol=str(item["symbol"]),
                    base_asset=str(item.get("baseAsset", symbol.removesuffix("USDT"))),
                    quote_asset="USDT",
                )
        return instruments

    async def get_ticker_snapshots(self) -> dict[str, MarketSnapshot]:
        payload = await self.transport.get_json("/fapi/v1/ticker/24hr")
        snapshots: dict[str, MarketSnapshot] = {}
        now = utc_now()
        for item in payload:
            exchange_symbol = str(item.get("symbol", ""))
            if not exchange_symbol.endswith("USDT"):
                continue
            symbol = normalize_symbol(self.exchange, exchange_symbol)
            price = decimal_from(item.get("lastPrice"))
            snapshots[symbol] = MarketSnapshot(
                exchange=self.exchange,
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                timestamp=now,
                price=price,
                volume_24h_usdt=decimal_from(item.get("quoteVolume")),
                price_change_24h_pct=decimal_from(item.get("priceChangePercent")),
                source="rest:ticker24h",
                raw=item,
            )
        return snapshots

    async def get_klines(self, symbol: str, limit: int = 180) -> list[Candle]:
        exchange_symbol = normalize_symbol(self.exchange, symbol)
        payload = await self.transport.get_json(
            "/fapi/v1/klines",
            {"symbol": exchange_symbol, "interval": "1m", "limit": limit},
        )
        candles: list[Candle] = []
        for item in payload:
            candles.append(
                Candle(
                    exchange=self.exchange,
                    symbol=exchange_symbol,
                    open_time=ms_to_datetime(item[0]),
                    open=decimal_from(item[1], Decimal("0")) or Decimal("0"),
                    high=decimal_from(item[2], Decimal("0")) or Decimal("0"),
                    low=decimal_from(item[3], Decimal("0")) or Decimal("0"),
                    close=decimal_from(item[4], Decimal("0")) or Decimal("0"),
                    volume_usdt=decimal_from(item[7], Decimal("0")) or Decimal("0"),
                )
            )
        return candles

    async def get_klines_history(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> list[Candle]:
        exchange_symbol = normalize_symbol(self.exchange, symbol)
        api_interval = BINANCE_INTERVALS.get(interval, interval)
        candles: list[Candle] = []
        cursor = start
        step = _interval_delta(api_interval) * 1499
        while cursor < end:
            chunk_end = min(cursor + step, end)
            payload = await self.transport.get_json(
                "/fapi/v1/klines",
                {
                    "symbol": exchange_symbol,
                    "interval": api_interval,
                    "startTime": int(cursor.timestamp() * 1000),
                    "endTime": int(chunk_end.timestamp() * 1000),
                    "limit": 1500,
                },
            )
            parsed = [
                Candle(
                    exchange=self.exchange,
                    symbol=exchange_symbol,
                    open_time=ms_to_datetime(item[0]),
                    open=decimal_from(item[1], Decimal("0")) or Decimal("0"),
                    high=decimal_from(item[2], Decimal("0")) or Decimal("0"),
                    low=decimal_from(item[3], Decimal("0")) or Decimal("0"),
                    close=decimal_from(item[4], Decimal("0")) or Decimal("0"),
                    volume_usdt=decimal_from(item[7], Decimal("0")) or Decimal("0"),
                )
                for item in payload
            ]
            candles.extend(parsed)
            if not parsed:
                cursor = chunk_end + _interval_delta(api_interval)
            else:
                cursor = parsed[-1].open_time + _interval_delta(api_interval)
        return _dedupe_candles(candles)

    async def enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        oi = await self.get_open_interest(snapshot.exchange_symbol)
        funding = await self.get_funding_rate_pct(snapshot.exchange_symbol)
        oi_value = estimate_oi_value_usdt(oi, snapshot.price)
        return snapshot.with_updates(
            timestamp=datetime.now(UTC),
            open_interest=oi,
            open_interest_value_usdt=oi_value,
            open_interest_value_estimated=oi_value is not None,
            funding_rate_pct=funding,
            source=f"{snapshot.source}+rest:oi+funding",
        )

    async def get_open_interest(self, exchange_symbol: str) -> Decimal | None:
        payload = await self.transport.get_json("/fapi/v1/openInterest", {"symbol": exchange_symbol})
        return decimal_from(payload.get("openInterest"))

    async def get_open_interest_history_value(
        self,
        exchange_symbol: str,
        period: str = "5m",
        limit: int = 30,
    ) -> Decimal | None:
        payload = await self.transport.get_json(
            "/futures/data/openInterestHist",
            {"symbol": exchange_symbol, "period": period, "limit": limit},
        )
        if not payload:
            return None
        return decimal_from(payload[-1].get("sumOpenInterestValue"))

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
                "/futures/data/openInterestHist",
                {"symbol": exchange_symbol, "period": "5m", "limit": limit},
            )
            snapshots.extend(_parse_binance_oi_history(exchange_symbol, payload, self.exchange))
        else:
            cursor = start
            step = timedelta(minutes=5 * 499)
            while cursor < end:
                chunk_end = min(cursor + step, end)
                payload = await self.transport.get_json(
                    "/futures/data/openInterestHist",
                    {
                        "symbol": exchange_symbol,
                        "period": "5m",
                        "limit": 500,
                        "startTime": int(cursor.timestamp() * 1000),
                        "endTime": int(chunk_end.timestamp() * 1000),
                    },
                )
                parsed = _parse_binance_oi_history(exchange_symbol, payload, self.exchange)
                snapshots.extend(parsed)
                if not parsed:
                    cursor = chunk_end + timedelta(minutes=5)
                else:
                    cursor = parsed[-1].timestamp + timedelta(minutes=5)
        return _dedupe_snapshots(snapshots)[-limit:] if start is None else _dedupe_snapshots(snapshots)

    async def get_funding_rate_pct(self, exchange_symbol: str) -> Decimal | None:
        payload = await self.transport.get_json(
            "/fapi/v1/fundingRate",
            {"symbol": exchange_symbol, "limit": 1},
        )
        if not payload:
            return None
        return funding_fraction_to_pct(payload[-1].get("fundingRate"))


def _parse_binance_oi_history(
    exchange_symbol: str,
    payload: list[dict[str, object]],
    exchange: ExchangeName,
) -> list[MarketSnapshot]:
    return [
        MarketSnapshot(
            exchange=exchange,
            symbol=exchange_symbol,
            exchange_symbol=exchange_symbol,
            timestamp=ms_to_datetime(item.get("timestamp")),
            open_interest=decimal_from(item.get("sumOpenInterest")),
            open_interest_value_usdt=decimal_from(item.get("sumOpenInterestValue")),
            open_interest_value_estimated=False,
            source="rest:oi_history",
            raw=item,
        )
        for item in payload
    ]


def _interval_delta(interval: str) -> timedelta:
    if interval.endswith("m"):
        return timedelta(minutes=int(interval.removesuffix("m")))
    if interval.endswith("h"):
        return timedelta(hours=int(interval.removesuffix("h")))
    raise ValueError(f"Unsupported interval: {interval}")


def _dedupe_candles(candles: list[Candle]) -> list[Candle]:
    by_time = {candle.open_time: candle for candle in candles}
    return [by_time[key] for key in sorted(by_time)]


def _dedupe_snapshots(snapshots: list[MarketSnapshot]) -> list[MarketSnapshot]:
    by_time = {snapshot.timestamp: snapshot for snapshot in snapshots}
    return [by_time[key] for key in sorted(by_time)]
