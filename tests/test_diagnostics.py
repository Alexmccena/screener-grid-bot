from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from telegram_oi_screener.diagnostics import build_market_diagnostics, build_market_diagnostics_24h, load_sector_map
from telegram_oi_screener.models import Candle, ExchangeName, MarketSnapshot
from telegram_oi_screener.rolling_buffer import RollingBuffer
from telegram_oi_screener.storage import SQLiteStorage
from tests.test_signal_engine import _settings
from telegram_oi_screener.models import AggregationMode


def test_load_sector_map_supports_1000_prefix(tmp_path: Path) -> None:
    path = tmp_path / "sectors.yaml"
    path.write_text("MEME:\n  - 1000PEPE\n", encoding="utf-8")

    sector_map = load_sector_map(path)

    assert sector_map["1000PEPE"] == "MEME"
    assert sector_map["PEPE"] == "MEME"


def test_market_diagnostics_formats_sector_and_tops(tmp_path: Path) -> None:
    now = datetime(2026, 5, 13, 12, tzinfo=UTC)
    path = tmp_path / "sectors.yaml"
    path.write_text("MEME:\n  - DOGE\nAI:\n  - TAO\n", encoding="utf-8")
    settings = _settings(AggregationMode.ANY_SELECTED)
    buffer = RollingBuffer(max_age_minutes=60)
    for symbol, old_price, old_oi, old_oi_value in (
        ("DOGEUSDT", "100", "1000", "1000000"),
        ("TAOUSDT", "100", "1000", "1000000"),
    ):
        buffer.add_snapshot(
            MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol=symbol,
                exchange_symbol=symbol,
                timestamp=now - timedelta(minutes=20),
                price=Decimal(old_price),
                open_interest=Decimal(old_oi),
                open_interest_value_usdt=Decimal(old_oi_value),
            )
        )
    snapshots = {
        "DOGEUSDT": {
            ExchangeName.BINANCE: MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="DOGEUSDT",
                exchange_symbol="DOGEUSDT",
                timestamp=now,
                price=Decimal("110"),
                open_interest=Decimal("1200"),
                open_interest_value_usdt=Decimal("1400000"),
                volume_24h_usdt=Decimal("10000000"),
            )
        },
        "TAOUSDT": {
            ExchangeName.BINANCE: MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="TAOUSDT",
                exchange_symbol="TAOUSDT",
                timestamp=now,
                price=Decimal("105"),
                open_interest=Decimal("1150"),
                open_interest_value_usdt=Decimal("1200000"),
                volume_24h_usdt=Decimal("8000000"),
            )
        },
    }

    text = build_market_diagnostics(snapshots, {}, buffer, settings, sectors_path=path)

    assert "Диагностика рынка" in text
    assert "MEME" in text
    assert "AI" in text
    assert "DOGEUSDT" in text
    assert "Топ OI" in text


def test_market_diagnostics_24h_uses_sqlite_history(tmp_path: Path) -> None:
    now = datetime(2026, 5, 13, 12, tzinfo=UTC)
    sector_path = tmp_path / "sectors.yaml"
    sector_path.write_text("MEME:\n  - DOGE\n", encoding="utf-8")
    db_path = tmp_path / "diagnostics.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    settings = _settings(AggregationMode.ANY_SELECTED)
    old = MarketSnapshot(
        exchange=ExchangeName.BINANCE,
        symbol="DOGEUSDT",
        exchange_symbol="DOGEUSDT",
        timestamp=now - timedelta(minutes=20),
        price=Decimal("100"),
        open_interest=Decimal("1000"),
        open_interest_value_usdt=Decimal("1000000"),
    )
    current = MarketSnapshot(
        exchange=ExchangeName.BINANCE,
        symbol="DOGEUSDT",
        exchange_symbol="DOGEUSDT",
        timestamp=now - timedelta(minutes=5),
        price=Decimal("110"),
        open_interest=Decimal("1200"),
        open_interest_value_usdt=Decimal("1400000"),
    )
    storage.save_snapshot(old)
    storage.save_snapshot(current)
    storage.save_candles(
        (
            Candle(
                exchange=ExchangeName.BINANCE,
                symbol="DOGEUSDT",
                open_time=now - timedelta(minutes=10),
                open=Decimal("100"),
                high=Decimal("112"),
                low=Decimal("99"),
                close=Decimal("110"),
                volume_usdt=Decimal("100000"),
            ),
        ),
        "5m",
    )

    text = build_market_diagnostics_24h(storage, settings, sectors_path=sector_path, now=now)

    assert "Диагностика рынка за 24h" in text
    assert "OI" in text
    assert "DOGEUSDT" in text
    assert "MEME" in text
    assert "Vol median" in text


def test_market_diagnostics_24h_hides_btc_eth_from_bias_and_uses_top_10(tmp_path: Path) -> None:
    now = datetime(2026, 5, 13, 12, tzinfo=UTC)
    db_path = tmp_path / "diagnostics.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    settings = _settings(AggregationMode.ANY_SELECTED)
    symbols = ["BTCUSDT", "ETHUSDT", *(f"ALT{index:02d}USDT" for index in range(12))]
    for index, symbol in enumerate(symbols):
        old_value = Decimal("1000000")
        new_value = old_value + Decimal(index + 1) * Decimal("100000")
        storage.save_snapshot(
            MarketSnapshot(
                exchange=ExchangeName.BYBIT,
                symbol=symbol,
                exchange_symbol=symbol,
                timestamp=now - timedelta(minutes=20),
                price=Decimal("100"),
                open_interest=Decimal("1000"),
                open_interest_value_usdt=old_value,
            )
        )
        storage.save_snapshot(
            MarketSnapshot(
                exchange=ExchangeName.BYBIT,
                symbol=symbol,
                exchange_symbol=symbol,
                timestamp=now - timedelta(minutes=5),
                price=Decimal("110"),
                open_interest=Decimal("1100"),
                open_interest_value_usdt=new_value,
            )
        )

    text = build_market_diagnostics_24h(storage, settings, now=now)
    bullish_section = text.split("Bullish OI", 1)[1].split("Bearish OI", 1)[0]

    assert "BTCUSDT" not in bullish_section
    assert "ETHUSDT" not in bullish_section
    assert bullish_section.count("ALT") == 10
