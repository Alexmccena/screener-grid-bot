from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from .calculations import (
    calculate_oi_change_pct,
    calculate_oi_value_change,
    calculate_price_change_pct,
    calculate_score,
    calculate_volatility_from_candles,
    calculate_volatility_pct,
    calculate_volume_spike,
)
from .models import (
    AggregatedSignal,
    AggregationMode,
    Candle,
    ExchangeName,
    ExchangeSignalEvaluation,
    FilterResult,
    MarketSnapshot,
    SignalMetrics,
    SignalSettings,
    ZERO,
)
from .rolling_buffer import RollingBuffer


class SignalEngine:
    def __init__(self, buffer: RollingBuffer) -> None:
        self.buffer = buffer

    def evaluate_symbol(
        self,
        symbol: str,
        snapshots: Mapping[ExchangeName, MarketSnapshot],
        settings: SignalSettings,
        candles: Mapping[ExchangeName, Iterable[Candle]] | None = None,
        avg24h_candles: Mapping[ExchangeName, Iterable[Candle]] | None = None,
    ) -> AggregatedSignal:
        if not symbol_allowed(symbol, settings):
            return AggregatedSignal(
                symbol=symbol,
                direction=settings.direction,
                aggregation_mode=settings.aggregation_mode,
                primary_exchange=settings.primary_exchange,
                evaluations=(),
                score=0,
                passed=False,
                reason="symbol_filtered",
            )

        candle_map = candles or {}
        avg24h_candle_map = avg24h_candles or {}
        evaluations = tuple(
            self.evaluate_exchange(
                exchange=exchange,
                symbol=symbol,
                snapshot=snapshots.get(exchange),
                settings=settings,
                candles=tuple(candle_map.get(exchange, ())),
                avg24h_candles=tuple(avg24h_candle_map.get(exchange, ())),
            )
            for exchange in settings.enabled_exchanges
        )
        return aggregate_evaluations(symbol, evaluations, settings)

    def evaluate_exchange(
        self,
        exchange: ExchangeName,
        symbol: str,
        snapshot: MarketSnapshot | None,
        settings: SignalSettings,
        candles: Iterable[Candle] = (),
        avg24h_candles: Iterable[Candle] = (),
    ) -> ExchangeSignalEvaluation:
        if snapshot is None:
            return _missing_evaluation(exchange, symbol, "missing_snapshot")
        if exchange in settings.native_grid_only_exchanges:
            eligible = settings.grid_eligible_symbols.get(exchange, frozenset())
            if eligible and symbol not in eligible:
                return _missing_evaluation(exchange, symbol, "not_native_grid_symbol")

        now = snapshot.timestamp
        oi_old = self.buffer.get_point_ago(exchange, symbol, settings.oi_period_minutes, now)
        price_old = self.buffer.get_point_ago(exchange, symbol, settings.price_change_period_minutes, now)
        candle_list = tuple(candles)
        avg24h_candle_list = tuple(avg24h_candles)

        oi_change_pct = calculate_oi_change_pct(
            snapshot.open_interest,
            oi_old.open_interest if oi_old else None,
        )
        oi_value_change = calculate_oi_value_change(
            snapshot.open_interest_value_usdt,
            oi_old.open_interest_value_usdt if oi_old else None,
        )
        oi_bias = self._oi_bias(snapshot, settings, oi_old)
        price_change_pct = calculate_price_change_pct(
            snapshot.price,
            price_old.price if price_old else None,
        )
        volume_spike_ratio = self._volume_spike(snapshot, settings, candle_list)
        volatility = self._volatility(snapshot, settings, candle_list, avg24h_candle_list)
        volatility_pct = volatility["market_pct"]
        volatility_passed = volatility["passed"]
        funding_rate_pct = snapshot.funding_rate_pct
        volume_24h_usdt = snapshot.volume_24h_usdt
        metrics = SignalMetrics(
            oi_change_pct=oi_change_pct,
            oi_value_change_usdt=oi_value_change,
            oi_bullish_value_usdt=oi_bias["bullish"],
            oi_bearish_value_usdt=oi_bias["bearish"],
            volume_spike_ratio=volume_spike_ratio,
            price_change_pct=price_change_pct,
            volatility_pct=volatility_pct,
            funding_rate_pct=funding_rate_pct,
            volume_24h_usdt=volume_24h_usdt,
        )

        thresholds = settings.thresholds
        filters = (
            _filter(
                "oi_change_pct",
                oi_change_pct,
                thresholds.min_oi_change_pct,
                oi_change_pct is not None and oi_change_pct >= thresholds.min_oi_change_pct,
                "OI growth over selected period",
                previous_value=oi_old.open_interest if oi_old else None,
            ),
            _filter(
                "oi_value_change_usdt",
                oi_value_change,
                thresholds.min_oi_value_change_usdt,
                oi_value_change is not None and oi_value_change >= thresholds.min_oi_value_change_usdt,
                "OI value growth in USDT",
                previous_value=oi_old.open_interest_value_usdt if oi_old else None,
            ),
            *(
                (
                    _filter(
                        "oi_bullish_value_usdt",
                        oi_bias["bullish"],
                        "info",
                        True,
                        "positive OI USD steps while price was rising",
                    ),
                    _filter(
                        "oi_bearish_value_usdt",
                        oi_bias["bearish"],
                        "info",
                        True,
                        "positive OI USD steps while price was falling",
                    ),
                )
                if settings.oi_bias_enabled
                else ()
            ),
            _filter(
                "price_change_pct",
                price_change_pct,
                thresholds.min_price_change_pct,
                price_change_pct is not None and price_change_pct >= thresholds.min_price_change_pct,
                "price started moving",
            ),
            _filter(
                "volatility_pct",
                volatility_pct,
                f"{thresholds.min_volatility_pct}..{thresholds.max_volatility_pct}",
                bool(volatility_passed),
                "grid-friendly volatility",
                metadata={
                    **volatility,
                    "display_mode": settings.volatility_display_mode,
                    "period_minutes": settings.volatility_period_minutes,
                },
            ),
            _filter(
                "volume_spike_ratio",
                volume_spike_ratio,
                thresholds.min_volume_spike_ratio,
                volume_spike_ratio is not None
                and volume_spike_ratio >= thresholds.min_volume_spike_ratio,
                "local volume spike",
            ),
            _filter(
                "volume_24h_usdt",
                volume_24h_usdt,
                thresholds.min_24h_volume_usdt,
                volume_24h_usdt is not None and volume_24h_usdt >= thresholds.min_24h_volume_usdt,
                "24h turnover/liquidity",
            ),
            _filter(
                "funding_rate_pct",
                funding_rate_pct,
                thresholds.max_funding_rate_pct,
                funding_rate_pct is not None
                and funding_rate_pct <= thresholds.max_funding_rate_pct,
                "funding is not overheated",
            ),
        )
        score = calculate_score(
            thresholds=thresholds,
            oi_change_pct=oi_change_pct,
            oi_value_change_usdt=oi_value_change,
            volume_24h_usdt=volume_24h_usdt,
            volume_spike_ratio=volume_spike_ratio,
            price_change_pct=price_change_pct,
            volatility_pct=volatility_pct,
            funding_rate_pct=funding_rate_pct,
            volatility_passed=bool(volatility_passed),
        )
        required_filters = set(settings.enabled_filters)
        score_filter = _filter(
            "min_score",
            score,
            thresholds.min_score_to_alert,
            score >= thresholds.min_score_to_alert,
            "overall setup score",
        )
        filters = filters + (score_filter,)
        required_real_filters = [item for item in filters if item.name in required_filters]
        filters_passed = bool(required_real_filters) and all(item.passed for item in required_real_filters)
        passed = filters_passed
        return ExchangeSignalEvaluation(
            exchange=exchange,
            symbol=symbol,
            snapshot=snapshot,
            filters=filters,
            metrics=metrics,
            score=score,
            passed=passed,
        )

    def _volume_spike(
        self,
        snapshot: MarketSnapshot,
        settings: SignalSettings,
        candles: tuple[Candle, ...],
    ) -> Decimal | None:
        if candles:
            spike = calculate_volume_spike(
                candles,
                snapshot.timestamp,
                settings.volume_spike_period_minutes,
                settings.volume_baseline_period_minutes,
            )
            return spike.ratio
        recent_end = snapshot.timestamp
        recent_start = recent_end - timedelta(minutes=settings.volume_spike_period_minutes)
        baseline_start = recent_start - timedelta(minutes=settings.volume_baseline_period_minutes)
        recent_volume = self.buffer.sum_volume(
            snapshot.exchange,
            snapshot.symbol,
            recent_start,
            recent_end,
        )
        baseline_volume = self.buffer.sum_volume(
            snapshot.exchange,
            snapshot.symbol,
            baseline_start,
            recent_start,
        )
        if baseline_volume <= ZERO:
            return None
        windows = Decimal(
            str(max(settings.volume_baseline_period_minutes / settings.volume_spike_period_minutes, 1))
        )
        baseline_average = baseline_volume / windows
        if baseline_average <= ZERO:
            return None
        return recent_volume / baseline_average

    def _oi_bias(
        self,
        snapshot: MarketSnapshot,
        settings: SignalSettings,
        oi_old: object | None,
    ) -> dict[str, Decimal | None]:
        if not settings.oi_bias_enabled:
            return {"bullish": None, "bearish": None}
        start = snapshot.timestamp - timedelta(minutes=settings.oi_period_minutes)
        points = list(self.buffer.points_between(snapshot.exchange, snapshot.symbol, start, snapshot.timestamp))
        if oi_old is not None and getattr(oi_old, "timestamp", snapshot.timestamp) < start:
            points.insert(0, oi_old)
        points.append(
            SimpleNamespace(
                timestamp=snapshot.timestamp,
                price=snapshot.price,
                open_interest_value_usdt=snapshot.open_interest_value_usdt,
            )
        )
        points = sorted(
            (
                point
                for point in points
                if getattr(point, "price", None) is not None
                and getattr(point, "open_interest_value_usdt", None) is not None
            ),
            key=lambda item: item.timestamp,
        )
        if len(points) < 2:
            return {"bullish": None, "bearish": None}
        bullish = ZERO
        bearish = ZERO
        previous = points[0]
        for point in points[1:]:
            oi_delta = point.open_interest_value_usdt - previous.open_interest_value_usdt
            price_delta = point.price - previous.price
            if oi_delta > ZERO and price_delta > ZERO:
                bullish += oi_delta
            elif oi_delta > ZERO and price_delta < ZERO:
                bearish += oi_delta
            previous = point
        return {"bullish": bullish, "bearish": bearish}

    def _volatility(
        self,
        snapshot: MarketSnapshot,
        settings: SignalSettings,
        candles: tuple[Candle, ...],
        avg24h_candles: tuple[Candle, ...] = (),
    ) -> dict[str, object]:
        if candles:
            cutoff = snapshot.timestamp - timedelta(minutes=settings.volatility_period_minutes)
            period_candles = tuple(candle for candle in candles if candle.open_time >= cutoff)
            market_pct = calculate_volatility_from_candles(period_candles)
            minute_stats = _minute_volatility_stats(
                period_candles,
                settings.thresholds.min_volatility_pct,
                settings.thresholds.max_volatility_pct,
                settings.volatility_period_minutes,
            )
            avg24h_pct = _average_period_volatility(
                avg24h_candles or candles,
                snapshot.timestamp,
                settings.volatility_period_minutes,
            )
            avg_period_minutes = _avg24h_period_minutes(settings.volatility_period_minutes)
            market_passed = (
                market_pct is not None
                and settings.thresholds.min_volatility_pct
                <= market_pct
                <= settings.thresholds.max_volatility_pct
            )
            passed = minute_stats["passed"] if settings.volatility_display_mode == "diagnostic" else market_passed
            return {
                "market_pct": market_pct,
                "passed": passed,
                "passed_candles": minute_stats["passed_candles"],
                "total_candles": minute_stats["total_candles"],
                "min_per_candle_pct": minute_stats["min_per_candle_pct"],
                "max_per_candle_pct": minute_stats["max_per_candle_pct"],
                "required_ratio": minute_stats["required_ratio"],
                "avg24h_pct": avg24h_pct,
                "avg24h_period_minutes": avg_period_minutes,
            }
        avg24h_pct = _average_period_volatility(
            avg24h_candles,
            snapshot.timestamp,
            settings.volatility_period_minutes,
        )
        avg_period_minutes = _avg24h_period_minutes(settings.volatility_period_minutes)
        start = snapshot.timestamp - timedelta(minutes=settings.volatility_period_minutes)
        low, high = self.buffer.min_max_price(snapshot.exchange, snapshot.symbol, start, snapshot.timestamp)
        market_pct = calculate_volatility_pct(high, low, snapshot.price)
        return {
            "market_pct": market_pct,
            "passed": market_pct is not None
            and settings.thresholds.min_volatility_pct
            <= market_pct
            <= settings.thresholds.max_volatility_pct,
            "passed_candles": None,
            "total_candles": None,
            "min_per_candle_pct": None,
            "max_per_candle_pct": None,
            "required_ratio": Decimal("0.6"),
            "avg24h_pct": avg24h_pct,
            "avg24h_period_minutes": avg_period_minutes,
        }


class AntiSpamRegistry:
    def __init__(self) -> None:
        self._last_sent: dict[tuple[int | None, str], datetime] = {}

    def should_alert(
        self,
        symbol: str,
        cooldown_minutes: int,
        now: datetime,
        telegram_user_id: int | None = None,
    ) -> bool:
        key = (telegram_user_id, symbol)
        last_sent = self._last_sent.get(key)
        if last_sent is None or now - last_sent >= timedelta(minutes=cooldown_minutes):
            self._last_sent[key] = now
            return True
        return False


def aggregate_evaluations(
    symbol: str,
    evaluations: Iterable[ExchangeSignalEvaluation],
    settings: SignalSettings,
) -> AggregatedSignal:
    evaluation_tuple = tuple(evaluations)
    if not evaluation_tuple:
        return AggregatedSignal(
            symbol=symbol,
            direction=settings.direction,
            aggregation_mode=settings.aggregation_mode,
            primary_exchange=settings.primary_exchange,
            evaluations=(),
            score=0,
            passed=False,
            reason="no_evaluations",
        )

    if settings.aggregation_mode is AggregationMode.ANY_SELECTED:
        passing = [item for item in evaluation_tuple if item.passed]
        score = max((item.score for item in evaluation_tuple), default=0)
        return AggregatedSignal(
            symbol=symbol,
            direction=settings.direction,
            aggregation_mode=settings.aggregation_mode,
            primary_exchange=settings.primary_exchange,
            evaluations=evaluation_tuple,
            score=score,
            passed=bool(passing),
            reason="any_selected_passed" if passing else "no_exchange_passed",
        )

    if settings.aggregation_mode is AggregationMode.ALL_SELECTED:
        passed = all(item.passed for item in evaluation_tuple)
        score = min((item.score for item in evaluation_tuple), default=0)
        return AggregatedSignal(
            symbol=symbol,
            direction=settings.direction,
            aggregation_mode=settings.aggregation_mode,
            primary_exchange=settings.primary_exchange,
            evaluations=evaluation_tuple,
            score=score,
            passed=passed,
            reason="all_selected_passed" if passed else "not_all_exchanges_passed",
        )

    primary = next(
        (item for item in evaluation_tuple if item.exchange is settings.primary_exchange),
        None,
    )
    if primary is None:
        return AggregatedSignal(
            symbol=symbol,
            direction=settings.direction,
            aggregation_mode=settings.aggregation_mode,
            primary_exchange=settings.primary_exchange,
            evaluations=evaluation_tuple,
            score=0,
            passed=False,
            reason="missing_primary_exchange",
        )
    confirming = sum(
        1
        for item in evaluation_tuple
        if item.exchange is not settings.primary_exchange
        and secondary_confirms(primary, item, settings)
    )
    passed = primary.passed and confirming >= settings.min_confirming_exchanges
    return AggregatedSignal(
        symbol=symbol,
        direction=settings.direction,
        aggregation_mode=settings.aggregation_mode,
        primary_exchange=settings.primary_exchange,
        evaluations=evaluation_tuple,
        score=primary.score,
        passed=passed,
        reason=(
            "primary_confirmed_passed"
            if passed
            else f"primary_or_confirmations_failed:{confirming}/{settings.min_confirming_exchanges}"
        ),
    )


def secondary_confirms(
    primary: ExchangeSignalEvaluation,
    secondary: ExchangeSignalEvaluation,
    settings: SignalSettings,
) -> bool:
    metrics = secondary.metrics
    enabled = set(settings.enabled_filters)
    if (
        "oi_change_pct" in enabled
        and (metrics.oi_change_pct is None or metrics.oi_change_pct < settings.min_secondary_oi_change_pct)
    ):
        return False
    if (
        "volume_spike_ratio" in enabled
        and (
        metrics.volume_spike_ratio is None
        or metrics.volume_spike_ratio < settings.min_secondary_volume_spike_ratio
        )
    ):
        return False
    if "price_change_pct" in enabled:
        if primary.snapshot is None or secondary.snapshot is None:
            return False
        if primary.snapshot.price is None or secondary.snapshot.price is None:
            return False
        divergence = abs(primary.snapshot.price - secondary.snapshot.price) / primary.snapshot.price * Decimal("100")
        return divergence <= settings.max_price_divergence_pct
    return True


def symbol_allowed(symbol: str, settings: SignalSettings) -> bool:
    normalized = symbol.upper()
    if settings.whitelist_symbols and normalized not in settings.whitelist_symbols:
        return False
    return normalized not in settings.blacklist_symbols


def _missing_evaluation(
    exchange: ExchangeName,
    symbol: str,
    reason: str,
) -> ExchangeSignalEvaluation:
    return ExchangeSignalEvaluation(
        exchange=exchange,
        symbol=symbol,
        snapshot=None,
        filters=(FilterResult("snapshot", False, None, "present", reason),),
        metrics=SignalMetrics(),
        score=0,
        passed=False,
    )


def _filter(
    name: str,
    value: Decimal | int | str | None,
    threshold: Decimal | int | str | None,
    passed: bool,
    reason: str,
    previous_value: Decimal | int | str | None = None,
    metadata: dict[str, object] | None = None,
) -> FilterResult:
    detail = reason if passed else f"{reason}: missing_or_out_of_range"
    return FilterResult(
        name=name,
        passed=passed,
        value=value,
        threshold=threshold,
        reason=detail,
        previous_value=previous_value,
        metadata=metadata or {},
    )


def _minute_volatility_stats(
    candles: tuple[Candle, ...],
    min_total_pct: Decimal,
    max_total_pct: Decimal,
    period_minutes: int,
) -> dict[str, object]:
    del min_total_pct
    per_candle_threshold = max_total_pct / Decimal(str(period_minutes))
    total = len(candles)
    if total == 0:
        return {
            "passed": False,
            "passed_candles": 0,
            "total_candles": 0,
            "min_per_candle_pct": per_candle_threshold,
            "max_per_candle_pct": per_candle_threshold,
            "required_ratio": Decimal("0.6"),
        }
    passed_candles = 0
    for candle in candles:
        value = calculate_volatility_pct(candle.high, candle.low, candle.close)
        if value is not None and value >= per_candle_threshold:
            passed_candles += 1
    required_ratio = Decimal("0.6")
    passed = Decimal(str(passed_candles)) / Decimal(str(total)) >= required_ratio
    return {
        "passed": passed,
        "passed_candles": passed_candles,
        "total_candles": total,
        "min_per_candle_pct": per_candle_threshold,
        "max_per_candle_pct": per_candle_threshold,
        "required_ratio": required_ratio,
    }


def _average_period_volatility(
    candles: tuple[Candle, ...],
    now: datetime,
    period_minutes: int,
) -> Decimal | None:
    cutoff = now - timedelta(hours=24)
    source = tuple(sorted((candle for candle in candles if candle.open_time >= cutoff), key=lambda item: item.open_time))
    if not source:
        return None
    candle_minutes = _infer_candle_minutes(source)
    candles_per_period = max(1, period_minutes // candle_minutes) if period_minutes >= candle_minutes else 1
    if len(source) < candles_per_period:
        return None
    values: list[Decimal] = []
    for index in range(0, len(source) - candles_per_period + 1, candles_per_period):
        value = calculate_volatility_from_candles(source[index : index + candles_per_period])
        if value is not None:
            values.append(value)
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(str(len(values)))


def _infer_candle_minutes(candles: tuple[Candle, ...]) -> int:
    if len(candles) < 2:
        return 5
    deltas = [
        int((right.open_time - left.open_time).total_seconds() // 60)
        for left, right in zip(candles, candles[1:])
        if right.open_time > left.open_time
    ]
    if not deltas:
        return 5
    return max(1, min(deltas))


def _avg24h_period_minutes(volatility_period_minutes: int) -> int:
    if volatility_period_minutes >= 5:
        return volatility_period_minutes if volatility_period_minutes % 5 == 0 else 5
    return 5
