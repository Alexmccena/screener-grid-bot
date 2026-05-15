from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


ZERO = Decimal("0")

SIGNAL_FILTER_NAMES = (
    "oi_change_pct",
    "oi_value_change_usdt",
    "price_change_pct",
    "volatility_pct",
    "volume_spike_ratio",
    "volume_24h_usdt",
    "funding_rate_pct",
    "min_score",
)


class ExchangeName(StrEnum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"


class AggregationMode(StrEnum):
    ANY_SELECTED = "any_selected"
    ALL_SELECTED = "all_selected"
    PRIMARY_CONFIRMED = "primary_confirmed"


class Direction(StrEnum):
    LONG = "long"


class ExecutionMode(StrEnum):
    MANUAL = "manual"


class ExecutionExchange(StrEnum):
    NONE = "none"
    BYBIT = "bybit"
    OKX = "okx"


@dataclass(frozen=True)
class Instrument:
    exchange: ExchangeName
    symbol: str
    exchange_symbol: str
    base_asset: str
    quote_asset: str
    contract_multiplier: Decimal = Decimal("1")
    active: bool = True


@dataclass(frozen=True)
class Candle:
    exchange: ExchangeName
    symbol: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_usdt: Decimal


@dataclass(frozen=True)
class MarketSnapshot:
    exchange: ExchangeName
    symbol: str
    exchange_symbol: str
    timestamp: datetime
    price: Decimal | None = None
    open_interest: Decimal | None = None
    open_interest_value_usdt: Decimal | None = None
    open_interest_value_estimated: bool = False
    volume_24h_usdt: Decimal | None = None
    recent_volume_usdt: Decimal | None = None
    price_change_24h_pct: Decimal | None = None
    funding_rate_pct: Decimal | None = None
    source: str = "rest"
    raw: dict[str, Any] = field(default_factory=dict)

    def with_updates(self, **updates: Any) -> MarketSnapshot:
        data = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "exchange_symbol": self.exchange_symbol,
            "timestamp": self.timestamp,
            "price": self.price,
            "open_interest": self.open_interest,
            "open_interest_value_usdt": self.open_interest_value_usdt,
            "open_interest_value_estimated": self.open_interest_value_estimated,
            "volume_24h_usdt": self.volume_24h_usdt,
            "recent_volume_usdt": self.recent_volume_usdt,
            "price_change_24h_pct": self.price_change_24h_pct,
            "funding_rate_pct": self.funding_rate_pct,
            "source": self.source,
            "raw": self.raw,
        }
        data.update(updates)
        return MarketSnapshot(**data)


@dataclass(frozen=True)
class SignalThresholds:
    min_oi_change_pct: Decimal
    min_oi_value_change_usdt: Decimal
    min_24h_volume_usdt: Decimal
    min_volume_spike_ratio: Decimal
    min_price_change_pct: Decimal
    min_volatility_pct: Decimal
    max_volatility_pct: Decimal
    max_funding_rate_pct: Decimal
    min_score_to_alert: int
    cooldown_minutes: int


@dataclass(frozen=True)
class SignalSettings:
    profile: str
    direction: Direction
    aggregation_mode: AggregationMode
    primary_exchange: ExchangeName
    enabled_exchanges: tuple[ExchangeName, ...]
    thresholds: SignalThresholds
    oi_period_minutes: int
    volume_spike_period_minutes: int
    volume_baseline_period_minutes: int
    price_change_period_minutes: int
    volatility_period_minutes: int
    volatility_display_mode: str = "market"
    oi_bias_enabled: bool = False
    whitelist_symbols: tuple[str, ...] = ()
    blacklist_symbols: tuple[str, ...] = ()
    max_symbols_per_refresh: int = 40
    oi_scan_max_symbols: int = 250
    oi_scan_min_24h_volume_usdt: Decimal = Decimal("1000000")
    min_secondary_oi_change_pct: Decimal = Decimal("3")
    min_secondary_volume_spike_ratio: Decimal = Decimal("1.2")
    max_price_divergence_pct: Decimal = Decimal("1.5")
    min_confirming_exchanges: int = 1
    enabled_filters: tuple[str, ...] = SIGNAL_FILTER_NAMES
    native_grid_only_exchanges: tuple[ExchangeName, ...] = ()
    grid_eligible_symbols: dict[ExchangeName, frozenset[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class FilterResult:
    name: str
    passed: bool
    value: Decimal | int | str | None
    threshold: Decimal | int | str | None
    reason: str
    previous_value: Decimal | int | str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SignalMetrics:
    oi_change_pct: Decimal | None = None
    oi_value_change_usdt: Decimal | None = None
    oi_bullish_value_usdt: Decimal | None = None
    oi_bearish_value_usdt: Decimal | None = None
    volume_spike_ratio: Decimal | None = None
    price_change_pct: Decimal | None = None
    volatility_pct: Decimal | None = None
    funding_rate_pct: Decimal | None = None
    volume_24h_usdt: Decimal | None = None


@dataclass(frozen=True)
class ExchangeSignalEvaluation:
    exchange: ExchangeName
    symbol: str
    snapshot: MarketSnapshot | None
    filters: tuple[FilterResult, ...]
    metrics: SignalMetrics
    score: int
    passed: bool

    @property
    def failed_filters(self) -> tuple[FilterResult, ...]:
        return tuple(result for result in self.filters if not result.passed)

    @property
    def passed_filters(self) -> tuple[FilterResult, ...]:
        return tuple(result for result in self.filters if result.passed)


@dataclass(frozen=True)
class AggregatedSignal:
    symbol: str
    direction: Direction
    aggregation_mode: AggregationMode
    primary_exchange: ExchangeName
    evaluations: tuple[ExchangeSignalEvaluation, ...]
    score: int
    passed: bool
    reason: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class GridDryRun:
    exchange: ExecutionExchange
    symbol: str
    direction: Direction
    current_price: Decimal
    lower_price: Decimal
    upper_price: Decimal
    investment_usdt: Decimal
    leverage: int
    grid_count: int
    status: str = "dry_run"


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    text = str(value).strip()
    if text == "":
        return None
    return Decimal(text)


def utc_now() -> datetime:
    return datetime.now(UTC)


def decimal_to_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, tuple | list):
        return [decimal_to_json(item) for item in value]
    if isinstance(value, dict):
        return {key: decimal_to_json(item) for key, item in value.items()}
    if hasattr(value, "__dict__"):
        return decimal_to_json(value.__dict__)
    return value
