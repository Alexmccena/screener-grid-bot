from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .exchanges.rate_limiter import RateLimiter
from .models import ExchangeName, MarketSnapshot


COINALYZE_BASE_URL = "https://api.coinalyze.net/v1"


class CoinalyzeApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class CoinalyzeMarket:
    symbol: str
    exchange: str
    base_asset: str
    quote_asset: str


@dataclass(frozen=True)
class CoinalyzeSnapshot:
    symbol: str
    timestamp: datetime | None = None
    open_interest: Decimal | None = None
    open_interest_value_usdt: Decimal | None = None
    funding_rate_pct: Decimal | None = None
    raw: dict[str, Any] | None = None


@dataclass(frozen=True)
class ComparisonRow:
    exchange: ExchangeName
    symbol: str
    coinalyze_symbol: str | None
    exchange_oi: Decimal | None = None
    coinalyze_oi: Decimal | None = None
    exchange_oi_value: Decimal | None = None
    coinalyze_oi_value: Decimal | None = None
    exchange_funding_pct: Decimal | None = None
    coinalyze_funding_pct: Decimal | None = None
    exchange_error: str | None = None
    coinalyze_error: str | None = None


class CoinalyzeClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = COINALYZE_BASE_URL,
        timeout_seconds: int = 12,
        max_symbol_calls_per_minute: int = 30,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.limiter = RateLimiter(max_calls=max(1, max_symbol_calls_per_minute), period_seconds=60)

    async def get_future_markets(self) -> list[CoinalyzeMarket]:
        await self.limiter.acquire()
        exchange_map = await self.get_exchanges()
        await self.limiter.acquire()
        payload = await self._get_json("/future-markets")
        if not isinstance(payload, list):
            raise CoinalyzeApiError("unexpected future-markets response")
        return [
            market
            for item in payload
            if isinstance(item, dict)
            and (market := parse_market(item, exchange_map=exchange_map)) is not None
        ]

    async def get_exchanges(self) -> dict[str, str]:
        await self.limiter.acquire()
        payload = await self._get_json("/exchanges")
        if not isinstance(payload, list):
            return {}
        result: dict[str, str] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            code = _first_text(item, "code", "id", "exchange", "symbol")
            name = _first_text(item, "name", "exchange_name", "exchangeName")
            if code and name:
                result[code.upper()] = name
        return result

    async def get_open_interest(self, symbols: list[str]) -> dict[str, CoinalyzeSnapshot]:
        await self._acquire_symbol_slots(symbols)
        payload = await self._get_json("/open-interest", {"symbols": ",".join(symbols)})
        return _parse_snapshots(payload, value_kind="oi")

    async def get_open_interest_usd(self, symbols: list[str]) -> dict[str, CoinalyzeSnapshot]:
        await self._acquire_symbol_slots(symbols)
        payload = await self._get_json(
            "/open-interest",
            {"symbols": ",".join(symbols), "convert_to_usd": "true"},
        )
        parsed = _parse_snapshots(payload, value_kind="oi_usd")
        return {
            symbol: CoinalyzeSnapshot(
                symbol=snapshot.symbol,
                timestamp=snapshot.timestamp,
                open_interest_value_usdt=snapshot.open_interest or snapshot.open_interest_value_usdt,
                raw=snapshot.raw,
            )
            for symbol, snapshot in parsed.items()
        }

    async def get_funding_rates(self, symbols: list[str]) -> dict[str, CoinalyzeSnapshot]:
        await self._acquire_symbol_slots(symbols)
        payload = await self._get_json("/funding-rate", {"symbols": ",".join(symbols)})
        return _parse_snapshots(payload, value_kind="funding")

    async def _acquire_symbol_slots(self, symbols: list[str]) -> None:
        for _ in symbols:
            await self.limiter.acquire()

    async def _get_json(self, path: str, params: dict[str, object] | None = None) -> Any:
        query = urllib.parse.urlencode(params or {})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        return await asyncio.to_thread(self._get_json_sync, url)

    def _get_json_sync(self, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "telegram-oi-volume-screener/0.1",
                "api_key": self.api_key,
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise CoinalyzeApiError(f"coinalyze HTTP {exc.code}: {_compact_body(body)}") from exc
        except urllib.error.URLError as exc:
            raise CoinalyzeApiError(f"coinalyze network error: {exc.reason}") from exc
        except TimeoutError as exc:
            raise CoinalyzeApiError("coinalyze timed out") from exc
        return json.loads(payload)


class CoinalyzeFallbackProvider:
    def __init__(self, client: CoinalyzeClient) -> None:
        self.client = client
        self._markets: list[CoinalyzeMarket] | None = None

    async def enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot | None:
        market = await self._find_market(snapshot.exchange, snapshot.symbol)
        if market is None:
            return None
        oi_data = await self.client.get_open_interest([market.symbol])
        oi_usd_data = await self.client.get_open_interest_usd([market.symbol])
        funding_data = await self.client.get_funding_rates([market.symbol])
        oi_snapshot = merge_oi_value(oi_data.get(market.symbol), oi_usd_data.get(market.symbol))
        coinalyze_snapshot = merge_snapshots(oi_snapshot, funding_data.get(market.symbol))
        if coinalyze_snapshot is None:
            return None
        return snapshot.with_updates(
            timestamp=coinalyze_snapshot.timestamp or datetime.now(UTC),
            open_interest=coinalyze_snapshot.open_interest,
            open_interest_value_usdt=coinalyze_snapshot.open_interest_value_usdt,
            open_interest_value_estimated=False,
            funding_rate_pct=coinalyze_snapshot.funding_rate_pct,
            source=f"{snapshot.source}+coinalyze:fallback",
            raw={**snapshot.raw, "coinalyze": coinalyze_snapshot.raw or {}},
        )

    async def _find_market(self, exchange: ExchangeName, symbol: str) -> CoinalyzeMarket | None:
        if self._markets is None:
            self._markets = await self.client.get_future_markets()
        return find_market(self._markets, exchange, symbol)


def find_market(
    markets: list[CoinalyzeMarket],
    exchange: ExchangeName,
    symbol: str,
) -> CoinalyzeMarket | None:
    base = symbol.upper().removesuffix("USDT")
    quote = "USDT"
    candidates = [
        market
        for market in markets
        if market.base_asset.upper() == base
        and market.quote_asset.upper() in {quote, "USD"}
        and _exchange_matches(exchange, market.exchange)
    ]
    if not candidates:
        compact = symbol.upper()
        candidates = [
            market
            for market in markets
            if compact in market.symbol.upper()
            and _exchange_matches(exchange, market.exchange)
        ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item.quote_asset.upper() != quote, item.symbol))
    return candidates[0]


def parse_market(item: dict[str, Any], exchange_map: dict[str, str] | None = None) -> CoinalyzeMarket | None:
    symbol = _first_text(item, "symbol", "market", "instrument")
    if not symbol:
        return None
    exchange = _first_text(item, "exchange", "exchange_name", "exchangeName") or ""
    exchange_code = _first_text(item, "exchange_code", "exchangeCode", "code")
    if exchange_code and exchange_map:
        exchange = exchange_map.get(exchange_code.upper(), exchange)
    if exchange and exchange_map:
        exchange = exchange_map.get(exchange.upper(), exchange)
    if not exchange:
        suffix = _coinalyze_suffix(symbol)
        exchange = (exchange_map or {}).get(suffix.upper(), suffix)
    return CoinalyzeMarket(
        symbol=symbol,
        exchange=exchange,
        base_asset=_first_text(item, "base_asset", "baseAsset", "base", "base_currency") or _base_from_symbol(symbol),
        quote_asset=_first_text(item, "quote_asset", "quoteAsset", "quote", "quote_currency") or _quote_from_symbol(symbol),
    )


def parse_snapshot(item: dict[str, Any], *, value_kind: str) -> CoinalyzeSnapshot | None:
    symbol = _first_text(item, "symbol", "market", "instrument")
    if not symbol:
        return None
    timestamp = _parse_timestamp(_first(item, "timestamp", "time", "t"))
    oi = _first_decimal(item, "open_interest", "openInterest", "oi", "value")
    oi_value = _first_decimal(
        item,
        "open_interest_usd",
        "openInterestUsd",
        "open_interest_value",
        "openInterestValue",
        "oi_usd",
        "oiUsd",
    )
    funding = _first_decimal(item, "funding_rate", "fundingRate", "rate", "value")
    if value_kind in {"oi", "oi_usd"}:
        funding = None
    elif value_kind == "funding":
        oi = None
        oi_value = None
    return CoinalyzeSnapshot(
        symbol=symbol,
        timestamp=timestamp,
        open_interest=oi,
        open_interest_value_usdt=oi_value,
        funding_rate_pct=funding,
        raw=item,
    )


def merge_snapshots(
    left: CoinalyzeSnapshot | None,
    right: CoinalyzeSnapshot | None,
) -> CoinalyzeSnapshot | None:
    if left is None:
        return right
    if right is None:
        return left
    return CoinalyzeSnapshot(
        symbol=left.symbol,
        timestamp=right.timestamp or left.timestamp,
        open_interest=left.open_interest if left.open_interest is not None else right.open_interest,
        open_interest_value_usdt=(
            left.open_interest_value_usdt
            if left.open_interest_value_usdt is not None
            else right.open_interest_value_usdt
        ),
        funding_rate_pct=right.funding_rate_pct if right.funding_rate_pct is not None else left.funding_rate_pct,
        raw={"oi": left.raw, "funding": right.raw},
    )


def merge_oi_value(
    snapshot: CoinalyzeSnapshot | None,
    usd_snapshot: CoinalyzeSnapshot | None,
) -> CoinalyzeSnapshot | None:
    if snapshot is None:
        return usd_snapshot
    if usd_snapshot is None:
        return snapshot
    return CoinalyzeSnapshot(
        symbol=snapshot.symbol,
        timestamp=snapshot.timestamp or usd_snapshot.timestamp,
        open_interest=snapshot.open_interest,
        open_interest_value_usdt=usd_snapshot.open_interest_value_usdt or usd_snapshot.open_interest,
        funding_rate_pct=snapshot.funding_rate_pct,
        raw={"native": snapshot.raw, "usd": usd_snapshot.raw},
    )


def format_comparison_table(rows: list[ComparisonRow]) -> str:
    headers = [
        "exchange",
        "symbol",
        "cg_symbol",
        "oi_ex",
        "oi_cg",
        "oi_diff",
        "oi_usd_ex",
        "oi_usd_cg",
        "usd_diff",
        "fund_ex",
        "fund_cg",
        "status",
    ]
    data = [headers]
    for row in rows:
        status = row.exchange_error or row.coinalyze_error or "ok"
        data.append(
            [
                row.exchange.value,
                row.symbol,
                row.coinalyze_symbol or "n/a",
                _fmt_num(row.exchange_oi),
                _fmt_num(row.coinalyze_oi),
                _fmt_pct(_diff_pct(row.exchange_oi, row.coinalyze_oi)),
                _fmt_money(row.exchange_oi_value),
                _fmt_money(row.coinalyze_oi_value),
                _fmt_pct(_diff_pct(row.exchange_oi_value, row.coinalyze_oi_value)),
                _fmt_pct(row.exchange_funding_pct, digits=4),
                _fmt_pct(row.coinalyze_funding_pct, digits=4),
                status[:40],
            ]
        )
    widths = [max(len(str(row[index])) for row in data) for index in range(len(headers))]
    lines = [
        "  ".join(str(value).ljust(widths[index]) for index, value in enumerate(row))
        for row in data
    ]
    return "\n".join(lines)


def _parse_snapshots(payload: Any, *, value_kind: str) -> dict[str, CoinalyzeSnapshot]:
    if not isinstance(payload, list):
        raise CoinalyzeApiError(f"unexpected {value_kind} response")
    result: dict[str, CoinalyzeSnapshot] = {}
    for item in payload:
        if isinstance(item, dict) and (snapshot := parse_snapshot(item, value_kind=value_kind)) is not None:
            result[snapshot.symbol] = snapshot
    return result


def _exchange_matches(exchange: ExchangeName, name: str) -> bool:
    normalized = name.lower().replace("_", "").replace("-", "").replace(" ", "")
    aliases = {
        ExchangeName.BINANCE: {"a", "binance", "binancefutures"},
        ExchangeName.BYBIT: {"6", "bybit", "bybitfutures"},
        ExchangeName.OKX: {"3", "okx", "okex", "okxfutures", "okexfutures"},
    }
    return normalized in aliases[exchange] or exchange.value in normalized


def _first(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in item:
            return item[key]
    return None


def _first_text(item: dict[str, Any], *keys: str) -> str | None:
    value = _first(item, *keys)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_decimal(item: dict[str, Any], *keys: str) -> Decimal | None:
    value = _first(item, *keys)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        raw = int(str(value))
    except ValueError:
        return None
    if raw > 10_000_000_000:
        raw //= 1000
    return datetime.fromtimestamp(raw, tz=UTC)


def _base_from_symbol(symbol: str) -> str:
    compact = symbol.upper().split("_")[0].replace("-", "")
    return compact.removesuffix("USDT").removesuffix("USD")


def _quote_from_symbol(symbol: str) -> str:
    compact = symbol.upper().split("_")[0].replace("-", "")
    if compact.endswith("USDT"):
        return "USDT"
    if compact.endswith("USD"):
        return "USD"
    return ""


def _coinalyze_suffix(symbol: str) -> str:
    _, separator, suffix = symbol.rpartition(".")
    return suffix if separator else ""


def _diff_pct(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None or right is None or left == 0:
        return None
    return (right - left) / left * Decimal("100")


def _fmt_num(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.2f}"


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / Decimal('1000000'):,.2f}M"


def _fmt_pct(value: Decimal | None, *, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}%"


def _compact_body(body: str) -> str:
    return " ".join(body.replace("\r", " ").replace("\n", " ").split())[:180]
