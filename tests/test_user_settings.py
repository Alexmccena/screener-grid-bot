from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from telegram_oi_screener.models import ExchangeName, MarketSnapshot
from telegram_oi_screener.rolling_buffer import RollingBuffer
from telegram_oi_screener.signal_engine import SignalEngine
from telegram_oi_screener.user_settings import effective_settings
from telegram_oi_screener.config import load_config
from tests.test_signal_engine import _settings
from telegram_oi_screener.models import AggregationMode


def test_only_oi_filter_can_pass_without_volume_score_or_funding() -> None:
    now = datetime(2026, 5, 6, 12, tzinfo=UTC)
    settings = replace(
        _settings(AggregationMode.ANY_SELECTED),
        enabled_filters=("oi_change_pct",),
    )
    buffer = RollingBuffer(max_age_minutes=60)
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=20),
            price=Decimal("100"),
            open_interest=Decimal("100"),
            open_interest_value_usdt=Decimal("10000"),
        )
    )
    signal = SignalEngine(buffer).evaluate_symbol(
        "BTCUSDT",
        {
            ExchangeName.BINANCE: MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=now,
                price=Decimal("100"),
                open_interest=Decimal("120"),
                open_interest_value_usdt=Decimal("12000"),
            )
        },
        settings,
    )

    assert signal.passed is True
    assert signal.evaluations[0].score == 25
    min_score = next(item for item in signal.evaluations[0].filters if item.name == "min_score")
    assert min_score.value == 25


def test_primary_confirmation_respects_enabled_filters() -> None:
    from telegram_oi_screener.signal_engine import aggregate_evaluations

    now = datetime(2026, 5, 6, 12, tzinfo=UTC)
    settings = replace(
        _settings(AggregationMode.PRIMARY_CONFIRMED),
        enabled_filters=("oi_change_pct",),
    )
    buffer = RollingBuffer(max_age_minutes=60)
    for exchange in (ExchangeName.BINANCE, ExchangeName.BYBIT):
        buffer.add_snapshot(
            MarketSnapshot(
                exchange=exchange,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=now - timedelta(minutes=20),
                price=Decimal("100"),
                open_interest=Decimal("100"),
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
            price=Decimal("100"),
            open_interest=Decimal("120"),
        ),
        settings,
    )
    secondary = engine.evaluate_exchange(
        ExchangeName.BYBIT,
        "BTCUSDT",
        MarketSnapshot(
            exchange=ExchangeName.BYBIT,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now,
            price=Decimal("90"),
            open_interest=Decimal("104"),
        ),
        settings,
    )

    signal = aggregate_evaluations("BTCUSDT", (primary, secondary), settings)
    assert signal.passed is True


def test_user_settings_select_exchanges_and_any_selected_mode() -> None:
    config = load_config("config.yaml")
    settings = effective_settings(
        config,
        {"enabled_exchanges": ["binance", "okx"], "enabled_filters": ["oi_change_pct"]},
    )

    assert settings.aggregation_mode is AggregationMode.ANY_SELECTED
    assert tuple(exchange.value for exchange in settings.enabled_exchanges) == ("bybit",)
    assert settings.enabled_filters[:2] == ("oi_change_pct", "oi_value_change_usdt")


def test_user_settings_override_volatility_period() -> None:
    config = load_config("config.yaml")
    settings = effective_settings(
        config,
        {
            "volatility_period_minutes": 45,
            "volatility_display_mode": "diagnostic",
            "min_volatility_pct": "1.5",
            "max_volatility_pct": "5",
        },
    )

    assert settings.volatility_period_minutes == 45
    assert settings.volatility_display_mode == "diagnostic"
    assert settings.thresholds.min_volatility_pct == Decimal("1.5")
    assert settings.thresholds.max_volatility_pct == Decimal("5")


def test_native_grid_only_filters_exchange_symbol_universe() -> None:
    now = datetime(2026, 5, 6, 12, tzinfo=UTC)
    settings = replace(
        _settings(AggregationMode.ANY_SELECTED),
        enabled_filters=("oi_change_pct",),
        native_grid_only_exchanges=(ExchangeName.BINANCE,),
        grid_eligible_symbols={ExchangeName.BINANCE: frozenset({"ETHUSDT"})},
    )
    buffer = RollingBuffer(max_age_minutes=60)
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=20),
            open_interest=Decimal("100"),
        )
    )
    signal = SignalEngine(buffer).evaluate_symbol(
        "BTCUSDT",
        {
            ExchangeName.BINANCE: MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=now,
                open_interest=Decimal("120"),
            )
        },
        settings,
    )

    assert signal.passed is False
    assert signal.evaluations[0].failed_filters[0].reason == "not_native_grid_symbol"
