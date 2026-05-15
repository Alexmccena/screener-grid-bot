from datetime import UTC, datetime, timedelta
from decimal import Decimal

from telegram_oi_screener.models import ExchangeName, MarketSnapshot
from telegram_oi_screener.rolling_buffer import RollingBuffer


def test_get_point_ago_returns_closest_point_at_or_before_target() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    buffer = RollingBuffer(max_age_minutes=60)
    for minutes_ago, price in [(30, "90"), (20, "100"), (10, "110")]:
        buffer.add_snapshot(
            MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=now - timedelta(minutes=minutes_ago),
                price=Decimal(price),
                open_interest=Decimal(price),
                open_interest_value_usdt=Decimal(price) * Decimal("100"),
            )
        )

    point = buffer.get_point_ago(ExchangeName.BINANCE, "BTCUSDT", 20, now)
    assert point is not None
    assert point.price == Decimal("100")


def test_get_point_ago_merges_latest_available_fields_before_target() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    buffer = RollingBuffer(max_age_minutes=60)
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=25),
            open_interest=Decimal("1000"),
            open_interest_value_usdt=Decimal("1000000"),
        )
    )
    buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=now - timedelta(minutes=20),
            price=Decimal("100"),
        )
    )

    point = buffer.get_point_ago(ExchangeName.BINANCE, "BTCUSDT", 20, now)
    assert point is not None
    assert point.price == Decimal("100")
    assert point.open_interest == Decimal("1000")
    assert point.open_interest_value_usdt == Decimal("1000000")


def test_points_between_returns_window() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
    buffer = RollingBuffer(max_age_minutes=60)
    for offset in (20, 10, 5):
        buffer.add_snapshot(
            MarketSnapshot(
                exchange=ExchangeName.BYBIT,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=now - timedelta(minutes=offset),
                price=Decimal("100"),
            )
        )

    points = buffer.points_between(
        ExchangeName.BYBIT,
        "BTCUSDT",
        now - timedelta(minutes=15),
        now,
    )

    assert [point.timestamp for point in points] == [
        now - timedelta(minutes=10),
        now - timedelta(minutes=5),
    ]
