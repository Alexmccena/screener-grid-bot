from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from telegram_oi_screener.models import Candle, ExchangeName, MarketSnapshot
from telegram_oi_screener.storage import SQLiteStorage


def test_prune_history_removes_only_expired_rows() -> None:
    db_path = Path(f"pytest-cache-files-storage-{uuid4().hex}.sqlite3")
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    now = datetime(2026, 5, 9, 12, tzinfo=UTC)
    old = now - timedelta(days=10)
    fresh = now - timedelta(days=1)

    storage.save_snapshot(_snapshot(old, "OLDUSDT"))
    storage.save_snapshot(_snapshot(fresh, "NEWUSDT"))
    storage.save_open_interest_history((_snapshot(old, "OLDUSDT"), _snapshot(fresh, "NEWUSDT")))
    storage.save_candles((_candle(old, "OLDUSDT"), _candle(fresh, "NEWUSDT")), "1m")
    storage.save_candles((_candle(old, "OLD5USDT"), _candle(fresh, "NEW5USDT")), "5m")
    with storage.connect() as conn:
        conn.execute(
            """
            INSERT INTO funding_history (exchange, symbol, exchange_symbol, timestamp, funding_rate_pct, source, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("binance", "OLDUSDT", "OLDUSDT", old.isoformat(), "0.01", "test", "{}"),
        )
        conn.execute(
            """
            INSERT INTO funding_history (exchange, symbol, exchange_symbol, timestamp, funding_rate_pct, source, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("binance", "NEWUSDT", "NEWUSDT", fresh.isoformat(), "0.01", "test", "{}"),
        )
        conn.execute(
            """
            INSERT INTO signals (
                symbol, aggregation_mode, primary_exchange, direction, score,
                passed_filters_json, failed_filters_json, snapshots_json, message_text, sent, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("OLDUSDT", "any_selected", "binance", "long", 10, "[]", "[]", "[]", "old", 1, old.isoformat()),
        )
        conn.execute(
            """
            INSERT INTO signals (
                symbol, aggregation_mode, primary_exchange, direction, score,
                passed_filters_json, failed_filters_json, snapshots_json, message_text, sent, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("NEWUSDT", "any_selected", "binance", "long", 10, "[]", "[]", "[]", "new", 1, fresh.isoformat()),
        )
        conn.commit()

    try:
        result = storage.prune_history(
            now=now,
            market_snapshot_days=7,
            oi_history_days=7,
            candle_1m_days=7,
            candle_5m_days=7,
            funding_history_days=7,
            signal_history_days=7,
        )

        assert result.total == 6
        assert result.market_snapshots == 1
        assert result.open_interest_history == 1
        assert result.market_candles_1m == 1
        assert result.market_candles_5m == 1
        assert result.funding_history == 1
        assert result.signals == 1
        with storage.connect() as conn:
            assert _count(conn, "market_snapshots") == 1
            assert _count(conn, "open_interest_history") == 1
            assert _count(conn, "market_candles") == 2
            assert _count(conn, "funding_history") == 1
            assert _count(conn, "signals") == 1
    finally:
        db_path.unlink(missing_ok=True)


def _snapshot(timestamp: datetime, symbol: str) -> MarketSnapshot:
    return MarketSnapshot(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        exchange_symbol=symbol,
        timestamp=timestamp,
        price=Decimal("100"),
        open_interest=Decimal("1000"),
        open_interest_value_usdt=Decimal("100000"),
        volume_24h_usdt=Decimal("1000000"),
        funding_rate_pct=Decimal("0.01"),
    )


def _candle(open_time: datetime, symbol: str) -> Candle:
    return Candle(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        open_time=open_time,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume_usdt=Decimal("1000"),
    )


def _count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
