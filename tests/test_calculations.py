from datetime import UTC, datetime, timedelta
from decimal import Decimal

from telegram_oi_screener.calculations import (
    calculate_oi_change_pct,
    calculate_oi_value_change,
    calculate_price_change_pct,
    calculate_volatility_pct,
    calculate_volume_spike,
)
from telegram_oi_screener.models import Candle, ExchangeName


def test_decimal_calculations() -> None:
    assert calculate_oi_change_pct(Decimal("110"), Decimal("100")) == Decimal("10.0")
    assert calculate_oi_value_change(Decimal("1500"), Decimal("900")) == Decimal("600")
    assert calculate_price_change_pct(Decimal("105"), Decimal("100")) == Decimal("5.00")
    assert calculate_volatility_pct(Decimal("110"), Decimal("100"), Decimal("105")) == (
        Decimal("10") / Decimal("105") * Decimal("100")
    )


def test_volume_spike_uses_recent_window_against_baseline_average() -> None:
    now = datetime(2026, 5, 3, 12, tzinfo=UTC)
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
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume_usdt=volume,
            )
        )
    spike = calculate_volume_spike(candles, now, recent_minutes=15, baseline_minutes=120)
    assert spike.recent_volume == Decimal("3000")
    assert spike.baseline_average == Decimal("300")
    assert spike.ratio == Decimal("10")
