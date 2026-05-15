from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, DivisionByZero, InvalidOperation

from .models import Candle, SignalThresholds, ZERO


@dataclass(frozen=True)
class VolumeSpike:
    recent_volume: Decimal
    baseline_average: Decimal
    ratio: Decimal | None


def pct_change(now: Decimal | None, old: Decimal | None) -> Decimal | None:
    if now is None or old is None or old == ZERO:
        return None
    return _safe_decimal((now - old) / old * Decimal("100"))


def absolute_change(now: Decimal | None, old: Decimal | None) -> Decimal | None:
    if now is None or old is None:
        return None
    return now - old


def calculate_oi_change_pct(now: Decimal | None, old: Decimal | None) -> Decimal | None:
    return pct_change(now, old)


def calculate_oi_value_change(now: Decimal | None, old: Decimal | None) -> Decimal | None:
    return absolute_change(now, old)


def calculate_price_change_pct(now: Decimal | None, old: Decimal | None) -> Decimal | None:
    return pct_change(now, old)


def calculate_volatility_pct(
    highest_high: Decimal | None,
    lowest_low: Decimal | None,
    close_now: Decimal | None,
) -> Decimal | None:
    if highest_high is None or lowest_low is None or close_now is None or close_now == ZERO:
        return None
    return _safe_decimal((highest_high - lowest_low) / close_now * Decimal("100"))


def calculate_volatility_from_candles(candles: Iterable[Candle]) -> Decimal | None:
    candle_list = list(candles)
    if not candle_list:
        return None
    highest = max(candle.high for candle in candle_list)
    lowest = min(candle.low for candle in candle_list)
    close = candle_list[-1].close
    return calculate_volatility_pct(highest, lowest, close)


def calculate_volume_spike(
    candles: Iterable[Candle],
    now: datetime | None,
    recent_minutes: int,
    baseline_minutes: int,
) -> VolumeSpike:
    candle_list = sorted(candles, key=lambda candle: candle.open_time)
    if not candle_list:
        return VolumeSpike(ZERO, ZERO, None)
    current_time = now or candle_list[-1].open_time
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)
    recent_start = current_time - timedelta(minutes=recent_minutes)
    baseline_start = recent_start - timedelta(minutes=baseline_minutes)

    recent_volume = sum(
        (candle.volume_usdt for candle in candle_list if candle.open_time >= recent_start),
        ZERO,
    )
    baseline_volume = sum(
        (
            candle.volume_usdt
            for candle in candle_list
            if baseline_start <= candle.open_time < recent_start
        ),
        ZERO,
    )
    if baseline_volume <= ZERO:
        return VolumeSpike(recent_volume, ZERO, None)

    windows = Decimal(str(max(baseline_minutes / recent_minutes, 1)))
    baseline_average = baseline_volume / windows
    if baseline_average <= ZERO:
        return VolumeSpike(recent_volume, ZERO, None)
    return VolumeSpike(recent_volume, baseline_average, _safe_decimal(recent_volume / baseline_average))


def calculate_score(
    thresholds: SignalThresholds,
    oi_change_pct: Decimal | None,
    oi_value_change_usdt: Decimal | None,
    volume_24h_usdt: Decimal | None,
    volume_spike_ratio: Decimal | None,
    price_change_pct: Decimal | None,
    volatility_pct: Decimal | None,
    funding_rate_pct: Decimal | None,
    volatility_passed: bool | None = None,
) -> int:
    weights = {
        "oi_change": 25,
        "oi_value": 20,
        "volume_24h": 15,
        "volume_spike": 15,
        "price_change": 10,
        "volatility": 10,
        "funding": 5,
    }
    score = 0
    if _gte(oi_change_pct, thresholds.min_oi_change_pct):
        score += weights["oi_change"]
    if _gte(oi_value_change_usdt, thresholds.min_oi_value_change_usdt):
        score += weights["oi_value"]
    if _gte(volume_24h_usdt, thresholds.min_24h_volume_usdt):
        score += weights["volume_24h"]
    if _gte(volume_spike_ratio, thresholds.min_volume_spike_ratio):
        score += weights["volume_spike"]
    if price_change_pct is not None and price_change_pct >= thresholds.min_price_change_pct:
        score += weights["price_change"]
    if volatility_passed is True or (
        volatility_passed is None
        and volatility_pct is not None
        and thresholds.min_volatility_pct <= volatility_pct <= thresholds.max_volatility_pct
    ):
        score += weights["volatility"]
    if funding_rate_pct is not None and funding_rate_pct <= thresholds.max_funding_rate_pct:
        score += weights["funding"]
    return min(score, 100)


def _gte(value: Decimal | None, threshold: Decimal) -> bool:
    return value is not None and value >= threshold


def _safe_decimal(value: Decimal) -> Decimal | None:
    try:
        if value.is_nan() or value.is_infinite():
            return None
    except (InvalidOperation, DivisionByZero):
        return None
    return value
