from datetime import UTC, datetime
from decimal import Decimal

from telegram_oi_screener.models import ExchangeName, ExchangeSignalEvaluation, FilterResult, MarketSnapshot, SignalMetrics
from telegram_oi_screener.telegram.formatting import (
    _fmt_filter_threshold,
    _fmt_filter_value,
    _format_filter_table,
    _market_links_line,
)


def test_filter_value_formatting_is_compact() -> None:
    assert _fmt_filter_value("funding_rate_pct", Decimal("-0.008243")) == "-0.0082%"
    assert _fmt_filter_value("volatility_pct", Decimal("0.332747")) == "0.33%"
    assert _fmt_filter_value("volume_spike_ratio", Decimal("2.860388")) == "2.86x"
    assert _fmt_filter_value("volume_24h_usdt", Decimal("12697117075.26")) == "12697.12M $"
    assert _fmt_filter_value("volume_24h_usdt", Decimal("950000")) == "0.95M $"


def test_filter_threshold_formatting_supports_volatility_range() -> None:
    result = FilterResult(
        name="volatility_pct",
        passed=False,
        value=Decimal("0.332747"),
        threshold="1..4",
        reason="grid-friendly volatility",
    )
    assert _fmt_filter_threshold(result.name, result.threshold) == "1.00..4.00%"


def test_info_money_threshold_is_not_parsed_as_decimal() -> None:
    table = _format_filter_table(
        (
            FilterResult(
                name="oi_bullish_value_usdt",
                passed=True,
                value=Decimal("150000"),
                threshold="info",
                reason="",
            ),
            FilterResult(
                name="oi_bearish_value_usdt",
                passed=True,
                value=Decimal("120000"),
                threshold="info",
                reason="",
            ),
        ),
        oi_period_minutes=15,
    )

    assert "0.15M $" in table
    assert "0.12M $" in table
    assert "info" in table


def test_filter_table_has_setting_column_and_no_per_row_off_suffix() -> None:
    table = _format_filter_table(
        (
            FilterResult(
                name="oi_change_pct",
                passed=True,
                value=Decimal("1.543"),
                threshold=Decimal("1"),
                reason="",
            ),
            FilterResult(
                name="funding_rate_pct",
                passed=False,
                value=Decimal("0.18055"),
                threshold=Decimal("0.08"),
                reason="",
            ),
            FilterResult(
                name="price_change_pct",
                passed=True,
                value=Decimal("0.75"),
                threshold=Decimal("0.5"),
                reason="",
            ),
        ),
        oi_period_minutes=5,
        price_change_period_minutes=10,
    )

    assert "Настройка" in table
    assert "+1.54%" in table
    assert "1.00% / 5m" in table
    assert "0.50% / 10m" in table
    assert "OFF" not in table


def test_oi_filters_show_previous_and_current_values() -> None:
    table = _format_filter_table(
        (
            FilterResult(
                name="oi_change_pct",
                passed=True,
                value=Decimal("1.44"),
                threshold=Decimal("1"),
                reason="",
                previous_value=Decimal("85400000"),
            ),
            FilterResult(
                name="oi_value_change_usdt",
                passed=True,
                value=Decimal("20000"),
                threshold=Decimal("10000"),
                reason="",
                previous_value=Decimal("1240000"),
            ),
        ),
        oi_period_minutes=5,
    )

    assert "+1.44% (85.40M-&gt;86.63M)" in table
    assert "+1.61% (1.24M $-&gt;1.26M $)" in table


def test_volatility_market_display_includes_avg24h_for_same_period() -> None:
    table = _format_filter_table(
        (
            FilterResult(
                name="volatility_pct",
                passed=True,
                value=Decimal("3.6"),
                threshold="1..5",
                reason="",
                metadata={
                    "display_mode": "market",
                    "avg24h_pct": Decimal("1.8"),
                    "period_minutes": 5,
                },
            ),
        ),
        oi_period_minutes=5,
    )

    assert "3.60% | avg24h 1.80%/5m" in table


def test_volatility_diagnostic_display_shows_passing_candles() -> None:
    table = _format_filter_table(
        (
            FilterResult(
                name="volatility_pct",
                passed=True,
                value=Decimal("3.6"),
                threshold="1..5",
                reason="",
                metadata={
                    "display_mode": "diagnostic",
                    "passed_candles": 3,
                    "total_candles": 5,
                    "max_per_candle_pct": Decimal("1"),
                    "avg24h_pct": Decimal("1.8"),
                    "period_minutes": 5,
                },
            ),
        ),
        oi_period_minutes=5,
    )

    assert "3/5 &gt;= 1.00% | avg24h 1.80%/5m" in table


def test_market_links_line_matches_bybit_reference_shape() -> None:
    evaluation = ExchangeSignalEvaluation(
        exchange=ExchangeName.BYBIT,
        symbol="OXTUSDT",
        snapshot=MarketSnapshot(
            exchange=ExchangeName.BYBIT,
            symbol="OXTUSDT",
            exchange_symbol="OXTUSDT",
            timestamp=datetime(2026, 5, 7, tzinfo=UTC),
        ),
        filters=(),
        metrics=SignalMetrics(),
        score=0,
        passed=True,
    )

    line = _market_links_line(evaluation, oi_period_minutes=20)

    assert "BYBIT" in line
    assert "20мин" in line
    assert "OXT" in line
    assert "https://www.bybit.com/trade/usdt/OXTUSDT" in line
    assert "https://www.coinglass.com/tv/ru/Bybit_OXTUSDT" in line
    assert "symbol=BYBIT%3AOXTUSDT.P" in line
