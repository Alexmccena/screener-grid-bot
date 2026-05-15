from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from telegram_oi_screener.models import (
    AggregationMode,
    Candle,
    Direction,
    ExchangeName,
    MarketSnapshot,
    SignalSettings,
    SignalThresholds,
)
from telegram_oi_screener.rolling_buffer import RollingBuffer
from telegram_oi_screener.signal_engine import SignalEngine, aggregate_evaluations


def test_exchange_evaluation_passes_all_filters() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    settings = _settings(AggregationMode.ANY_SELECTED)
    buffer = RollingBuffer(max_age_minutes=180)
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=20),
            price=Decimal("100"),
            open_interest=Decimal("1000"),
            open_interest_value_usdt=Decimal("1000000"),
            recent_volume_usdt=Decimal("10"),
        )
    )
    current = MarketSnapshot(
        exchange=ExchangeName.BINANCE,
        symbol="BTCUSDT",
        exchange_symbol="BTCUSDT",
        timestamp=now,
        price=Decimal("103"),
        open_interest=Decimal("1120"),
        open_interest_value_usdt=Decimal("1600000"),
        volume_24h_usdt=Decimal("30000000"),
        funding_rate_pct=Decimal("0.01"),
    )
    evaluation = SignalEngine(buffer).evaluate_exchange(
        ExchangeName.BINANCE,
        "BTCUSDT",
        current,
        settings,
        candles=_candles(now),
    )

    assert evaluation.passed is True
    assert evaluation.score == 100
    assert not evaluation.failed_filters


def test_exchange_evaluation_filter_order_matches_telegram_output() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    settings = _settings(AggregationMode.ANY_SELECTED)
    buffer = RollingBuffer(max_age_minutes=180)
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=20),
            price=Decimal("100"),
            open_interest=Decimal("1000"),
            open_interest_value_usdt=Decimal("1000000"),
            recent_volume_usdt=Decimal("10"),
        )
    )
    evaluation = SignalEngine(buffer).evaluate_exchange(
        ExchangeName.BINANCE,
        "BTCUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now,
            price=Decimal("103"),
            open_interest=Decimal("1120"),
            open_interest_value_usdt=Decimal("1600000"),
            volume_24h_usdt=Decimal("30000000"),
            funding_rate_pct=Decimal("0.01"),
        ),
        settings,
        candles=_candles(now),
    )

    assert [item.name for item in evaluation.filters] == [
        "oi_change_pct",
        "oi_value_change_usdt",
        "price_change_pct",
        "volatility_pct",
        "volume_spike_ratio",
        "volume_24h_usdt",
        "funding_rate_pct",
        "min_score",
    ]


def test_price_change_filter_requires_minimum_positive_move() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    settings = _settings(AggregationMode.ANY_SELECTED)
    buffer = RollingBuffer(max_age_minutes=180)
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=20),
            price=Decimal("100"),
            open_interest=Decimal("1000"),
            open_interest_value_usdt=Decimal("1000000"),
            recent_volume_usdt=Decimal("10"),
        )
    )
    evaluation = SignalEngine(buffer).evaluate_exchange(
        ExchangeName.BINANCE,
        "BTCUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now,
            price=Decimal("92"),
            open_interest=Decimal("1120"),
            open_interest_value_usdt=Decimal("1600000"),
            volume_24h_usdt=Decimal("30000000"),
            funding_rate_pct=Decimal("0.01"),
        ),
        settings,
        candles=_candles(now),
    )

    price_filter = next(item for item in evaluation.filters if item.name == "price_change_pct")
    assert price_filter.value == Decimal("-8.00")
    assert price_filter.passed is False


def test_oi_bias_splits_positive_oi_steps_by_price_direction() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    base_settings = _settings(AggregationMode.ANY_SELECTED)
    settings = replace(
        base_settings,
        oi_period_minutes=15,
        oi_bias_enabled=True,
        enabled_filters=("oi_change_pct", "oi_value_change_usdt"),
    )
    buffer = RollingBuffer(max_age_minutes=180)
    for minutes_ago, price, oi_value in (
        (15, "100", "1000000"),
        (10, "102", "1100000"),
        (5, "101", "1250000"),
    ):
        buffer.add_snapshot(
            MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=now - timedelta(minutes=minutes_ago),
                price=Decimal(price),
                open_interest=Decimal("1000"),
                open_interest_value_usdt=Decimal(oi_value),
            )
        )

    evaluation = SignalEngine(buffer).evaluate_exchange(
        ExchangeName.BINANCE,
        "BTCUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now,
            price=Decimal("103"),
            open_interest=Decimal("1300"),
            open_interest_value_usdt=Decimal("1300000"),
            volume_24h_usdt=Decimal("30000000"),
            funding_rate_pct=Decimal("0.01"),
        ),
        settings,
    )

    bullish = next(item for item in evaluation.filters if item.name == "oi_bullish_value_usdt")
    bearish = next(item for item in evaluation.filters if item.name == "oi_bearish_value_usdt")
    assert bullish.value == Decimal("150000")
    assert bearish.value == Decimal("150000")
    assert evaluation.metrics.oi_bullish_value_usdt == Decimal("150000")
    assert evaluation.metrics.oi_bearish_value_usdt == Decimal("150000")


def test_volatility_requires_sixty_percent_of_minute_candles() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    base_settings = _settings(AggregationMode.ANY_SELECTED)
    settings = replace(
        base_settings,
        volatility_period_minutes=5,
        volatility_display_mode="diagnostic",
        thresholds=replace(
            base_settings.thresholds,
            min_volatility_pct=Decimal("0"),
            max_volatility_pct=Decimal("2.5"),
        ),
    )
    buffer = RollingBuffer(max_age_minutes=180)
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=20),
            price=Decimal("100"),
            open_interest=Decimal("1000"),
            open_interest_value_usdt=Decimal("1000000"),
            recent_volume_usdt=Decimal("10"),
        )
    )
    candles = tuple(
        Candle(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            open_time=now - timedelta(minutes=offset),
            open=Decimal("100"),
            high=Decimal("100.6") if offset in {1, 2, 3} else Decimal("100.2"),
            low=Decimal("100"),
            close=Decimal("100"),
            volume_usdt=Decimal("200"),
        )
        for offset in range(5, 0, -1)
    )
    evaluation = SignalEngine(buffer).evaluate_exchange(
        ExchangeName.BINANCE,
        "BTCUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now,
            price=Decimal("103"),
            open_interest=Decimal("1120"),
            open_interest_value_usdt=Decimal("1600000"),
            volume_24h_usdt=Decimal("30000000"),
            funding_rate_pct=Decimal("0.01"),
        ),
        settings,
        candles=candles,
    )

    volatility_filter = next(item for item in evaluation.filters if item.name == "volatility_pct")
    assert volatility_filter.passed is True
    assert volatility_filter.metadata["passed_candles"] == 3
    assert volatility_filter.metadata["total_candles"] == 5
    assert volatility_filter.metadata["max_per_candle_pct"] == Decimal("0.5")


def test_volatility_market_mode_passes_by_period_range() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    base_settings = _settings(AggregationMode.ANY_SELECTED)
    settings = replace(
        base_settings,
        volatility_period_minutes=15,
        volatility_display_mode="market",
        thresholds=replace(
            base_settings.thresholds,
            min_volatility_pct=Decimal("0.7"),
            max_volatility_pct=Decimal("6"),
        ),
    )
    candles = tuple(
        Candle(
            exchange=ExchangeName.BINANCE,
            symbol="BILLUSDT",
            open_time=now - timedelta(minutes=offset),
            open=Decimal("100"),
            high=Decimal("102") if offset == 1 else Decimal("100.1"),
            low=Decimal("100"),
            close=Decimal("100"),
            volume_usdt=Decimal("200"),
        )
        for offset in range(15, 0, -1)
    )
    evaluation = SignalEngine(RollingBuffer(max_age_minutes=180)).evaluate_exchange(
        ExchangeName.BINANCE,
        "BILLUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BILLUSDT",
            exchange_symbol="BILLUSDT",
            timestamp=now,
            price=Decimal("100"),
            open_interest=Decimal("1120"),
            open_interest_value_usdt=Decimal("1600000"),
            volume_24h_usdt=Decimal("30000000"),
            funding_rate_pct=Decimal("0.01"),
        ),
        settings,
        candles=candles,
    )

    volatility_filter = next(item for item in evaluation.filters if item.name == "volatility_pct")
    assert volatility_filter.value == Decimal("2.00")
    assert volatility_filter.passed is True
    assert volatility_filter.metadata["passed_candles"] == 1


def test_volatility_avg24h_can_use_5m_candles() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    base_settings = _settings(AggregationMode.ANY_SELECTED)
    settings = replace(base_settings, volatility_period_minutes=15)
    buffer = RollingBuffer(max_age_minutes=180)
    current = MarketSnapshot(
        exchange=ExchangeName.BINANCE,
        symbol="BTCUSDT",
        exchange_symbol="BTCUSDT",
        timestamp=now,
        price=Decimal("103"),
        open_interest=Decimal("1120"),
        open_interest_value_usdt=Decimal("1600000"),
        volume_24h_usdt=Decimal("30000000"),
        funding_rate_pct=Decimal("0.01"),
    )
    avg_candles = tuple(
        Candle(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            open_time=now - timedelta(minutes=offset),
            open=Decimal("100"),
            high=Decimal("103"),
            low=Decimal("100"),
            close=Decimal("100"),
            volume_usdt=Decimal("1000"),
        )
        for offset in range(120, 0, -5)
    )

    evaluation = SignalEngine(buffer).evaluate_exchange(
        ExchangeName.BINANCE,
        "BTCUSDT",
        current,
        settings,
        candles=(),
        avg24h_candles=avg_candles,
    )

    volatility_filter = next(item for item in evaluation.filters if item.name == "volatility_pct")
    assert volatility_filter.metadata["avg24h_pct"] == Decimal("3.00")
    assert volatility_filter.metadata["avg24h_period_minutes"] == 15


def test_aggregation_primary_confirmed_needs_secondary_confirmation() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    settings = _settings(AggregationMode.PRIMARY_CONFIRMED)
    buffer = RollingBuffer(max_age_minutes=180)
    for exchange in (ExchangeName.BINANCE, ExchangeName.BYBIT):
        buffer.add_snapshot(
            MarketSnapshot(
                exchange=exchange,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=now - timedelta(minutes=20),
                price=Decimal("100"),
                open_interest=Decimal("1000"),
                open_interest_value_usdt=Decimal("1000000"),
            )
        )
    engine = SignalEngine(buffer)
    primary = engine.evaluate_exchange(
        ExchangeName.BINANCE,
        "BTCUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now,
            price=Decimal("103"),
            open_interest=Decimal("1120"),
            open_interest_value_usdt=Decimal("1600000"),
            volume_24h_usdt=Decimal("30000000"),
            funding_rate_pct=Decimal("0.01"),
        ),
        settings,
        candles=_candles(now),
    )
    secondary = engine.evaluate_exchange(
        ExchangeName.BYBIT,
        "BTCUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BYBIT,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now,
            price=Decimal("103.5"),
            open_interest=Decimal("1040"),
            open_interest_value_usdt=Decimal("1300000"),
            volume_24h_usdt=Decimal("30000000"),
            funding_rate_pct=Decimal("0.01"),
        ),
        settings,
        candles=_candles(now),
    )

    signal = aggregate_evaluations("BTCUSDT", (primary, secondary), settings)
    assert signal.passed is True
    assert signal.reason == "primary_confirmed_passed"


def _settings(mode: AggregationMode) -> SignalSettings:
    return SignalSettings(
        profile="normal",
        direction=Direction.LONG,
        aggregation_mode=mode,
        primary_exchange=ExchangeName.BINANCE,
        enabled_exchanges=(ExchangeName.BINANCE, ExchangeName.BYBIT),
        thresholds=SignalThresholds(
            min_oi_change_pct=Decimal("10"),
            min_oi_value_change_usdt=Decimal("500000"),
            min_24h_volume_usdt=Decimal("20000000"),
            min_volume_spike_ratio=Decimal("2"),
            min_price_change_pct=Decimal("0.5"),
            min_volatility_pct=Decimal("1"),
            max_volatility_pct=Decimal("4"),
            max_funding_rate_pct=Decimal("0.05"),
            min_score_to_alert=70,
            cooldown_minutes=60,
        ),
        oi_period_minutes=20,
        volume_spike_period_minutes=15,
        volume_baseline_period_minutes=120,
        price_change_period_minutes=15,
        volatility_period_minutes=30,
    )


def _candles(now: datetime) -> tuple[Candle, ...]:
    candles = []
    for offset in range(135, 0, -1):
        ts = now - timedelta(minutes=offset)
        volume = Decimal("20") if offset > 15 else Decimal("200")
        candles.append(
            Candle(
                exchange=ExchangeName.BINANCE,
                symbol="BTCUSDT",
                open_time=ts,
                open=Decimal("100"),
                high=Decimal("103"),
                low=Decimal("100"),
                close=Decimal("103"),
                volume_usdt=volume,
            )
        )
    return tuple(candles)
