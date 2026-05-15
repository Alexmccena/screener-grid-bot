from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from telegram_oi_screener.config import load_config
from telegram_oi_screener.market_data import MarketDataRefresh
from telegram_oi_screener.models import (
    AggregatedSignal,
    Candle,
    ExchangeName,
    ExchangeSignalEvaluation,
    FilterResult,
    MarketSnapshot,
    SignalMetrics,
)
from telegram_oi_screener.realtime.scheduler import ScreenerRuntime
from telegram_oi_screener.storage import SQLiteStorage


class RenamingClient:
    async def get_instruments(self):
        return {}

    async def get_ticker_snapshots(self):
        return {}

    async def get_klines(self, symbol: str, limit: int = 180):
        return []

    async def enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        return snapshot.with_updates(symbol="AVAXUSDT", exchange_symbol="AVAXUSDT")


class CandleClient:
    async def get_klines(self, symbol: str, limit: int = 180):
        return [
            Candle(
                exchange=ExchangeName.BINANCE,
                symbol=symbol,
                open_time=datetime(2026, 5, 5, 0, 0, tzinfo=UTC),
                open=Decimal("10"),
                high=Decimal("11"),
                low=Decimal("9"),
                close=Decimal("10.5"),
                volume_usdt=Decimal("1000"),
            )
        ]

    async def get_klines_history(self, symbol: str, interval: str, start, end):
        return [
            Candle(
                exchange=ExchangeName.BINANCE,
                symbol=symbol,
                open_time=start,
                open=Decimal("10"),
                high=Decimal("11"),
                low=Decimal("9"),
                close=Decimal("10.5"),
                volume_usdt=Decimal("1000"),
            )
        ]


def test_oi_refresh_handles_enriched_symbol_missing_from_state() -> None:
    import asyncio

    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    runtime.market_data.clients = {ExchangeName.BINANCE: RenamingClient()}
    runtime.state.latest_refresh = MarketDataRefresh(
        snapshots={
            "OLDUSDT": {
                ExchangeName.BINANCE: MarketSnapshot(
                    exchange=ExchangeName.BINANCE,
                    symbol="OLDUSDT",
                    exchange_symbol="OLDUSDT",
                    timestamp=datetime(2026, 5, 5, tzinfo=UTC),
                    price=Decimal("10"),
                )
            }
        }
    )

    asyncio.run(runtime.refresh_open_interest_for_current(ExchangeName.BINANCE))

    try:
        assert "AVAXUSDT" in runtime.state.latest_refresh.snapshots
    finally:
        db_path.unlink(missing_ok=True)


def test_send_evaluation_runs_only_after_data_revision_changes() -> None:
    import asyncio

    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)

    try:
        assert runtime._has_unevaluated_data()

        asyncio.run(runtime.evaluate_current(send=True))

        assert not runtime._has_unevaluated_data()

        asyncio.run(
            runtime.on_websocket_snapshot(
                MarketSnapshot(
                    exchange=ExchangeName.BINANCE,
                    symbol="BTCUSDT",
                    exchange_symbol="BTCUSDT",
                    timestamp=datetime(2026, 5, 5, tzinfo=UTC),
                    price=Decimal("90000"),
                )
            )
        )

        assert runtime._has_unevaluated_data()
    finally:
        db_path.unlink(missing_ok=True)


def test_evaluate_current_tolerates_snapshot_mutation_during_iteration() -> None:
    import asyncio

    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    runtime.state.latest_refresh = MarketDataRefresh(
        snapshots={
            "BTCUSDT": {
                ExchangeName.BINANCE: MarketSnapshot(
                    exchange=ExchangeName.BINANCE,
                    symbol="BTCUSDT",
                    exchange_symbol="BTCUSDT",
                    timestamp=datetime(2026, 5, 5, tzinfo=UTC),
                    price=Decimal("10"),
                )
            }
        }
    )
    original_evaluate_symbol = runtime.engine.evaluate_symbol

    def mutating_evaluate_symbol(*args, **kwargs):
        runtime.state.latest_refresh.snapshots["ETHUSDT"] = {
            ExchangeName.BINANCE: MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="ETHUSDT",
                exchange_symbol="ETHUSDT",
                timestamp=datetime(2026, 5, 5, tzinfo=UTC),
                price=Decimal("20"),
            )
        }
        return original_evaluate_symbol(*args, **kwargs)

    runtime.engine.evaluate_symbol = mutating_evaluate_symbol

    try:
        asyncio.run(runtime.evaluate_current(send=False))

        assert "ETHUSDT" in runtime.state.latest_refresh.snapshots
    finally:
        db_path.unlink(missing_ok=True)


def test_hot_candidate_is_registered_when_oi_is_close_to_threshold() -> None:
    import asyncio

    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    old_time = datetime(2026, 5, 5, 0, 0, tzinfo=UTC)
    current_time = datetime(2026, 5, 5, 0, 20, tzinfo=UTC)
    runtime.buffer.add_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BYBIT,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=old_time,
            price=Decimal("100"),
            open_interest=Decimal("1000"),
            open_interest_value_usdt=Decimal("100000"),
        )
    )
    runtime.state.latest_refresh = MarketDataRefresh(
        snapshots={
            "BTCUSDT": {
                ExchangeName.BYBIT: MarketSnapshot(
                    exchange=ExchangeName.BYBIT,
                    symbol="BTCUSDT",
                    exchange_symbol="BTCUSDT",
                    timestamp=current_time,
                    price=Decimal("100"),
                    open_interest=Decimal("1080"),
                    open_interest_value_usdt=Decimal("108000"),
                )
            }
        }
    )

    try:
        asyncio.run(runtime.evaluate_current(send=False))

        assert "BTCUSDT" in runtime._hot_symbols(ExchangeName.BYBIT, current_time)
    finally:
        db_path.unlink(missing_ok=True)


def test_hot_candle_refresh_updates_latest_refresh() -> None:
    import asyncio

    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    runtime.market_data.clients = {ExchangeName.BINANCE: CandleClient()}
    runtime.state.latest_refresh = MarketDataRefresh(
        snapshots={
            "BTCUSDT": {
                ExchangeName.BINANCE: MarketSnapshot(
                    exchange=ExchangeName.BINANCE,
                    symbol="BTCUSDT",
                    exchange_symbol="BTCUSDT",
                    timestamp=datetime(2026, 5, 5, tzinfo=UTC),
                    price=Decimal("10"),
                )
            }
        }
    )

    try:
        asyncio.run(runtime.refresh_candles_for_symbols(ExchangeName.BINANCE, {"BTCUSDT"}))

        assert runtime.state.latest_refresh.candles["BTCUSDT"][ExchangeName.BINANCE][0].close == Decimal(
            "10.5"
        )
        assert runtime._has_unevaluated_data()
    finally:
        db_path.unlink(missing_ok=True)


def test_avg24h_candle_refresh_saves_5m_candles() -> None:
    import asyncio

    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    runtime.market_data.clients = {ExchangeName.BINANCE: CandleClient()}
    runtime.state.latest_refresh = MarketDataRefresh(
        snapshots={
            "BTCUSDT": {
                ExchangeName.BINANCE: MarketSnapshot(
                    exchange=ExchangeName.BINANCE,
                    symbol="BTCUSDT",
                    exchange_symbol="BTCUSDT",
                    timestamp=datetime(2026, 5, 5, tzinfo=UTC),
                    price=Decimal("10"),
                )
            }
        }
    )

    try:
        asyncio.run(runtime.refresh_avg24h_candles_for_current(ExchangeName.BINANCE))

        assert runtime.state.latest_refresh.candles_5m["BTCUSDT"][ExchangeName.BINANCE][0].close == Decimal(
            "10.5"
        )
        saved = storage.load_candles(ExchangeName.BINANCE, "BTCUSDT", "5m", datetime(2026, 5, 4, tzinfo=UTC))
        assert saved
        assert runtime._has_unevaluated_data()
    finally:
        db_path.unlink(missing_ok=True)


def test_regular_oi_refresh_uses_rotating_batches() -> None:
    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    snapshots = [
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol=f"SYM{index}USDT",
            exchange_symbol=f"SYM{index}USDT",
            timestamp=datetime(2026, 5, 5, tzinfo=UTC),
        )
        for index in range(5)
    ]

    try:
        runtime.config.realtime.binance.values["oi_refresh_batch_size"] = 2

        first = runtime._oi_refresh_batch(ExchangeName.BINANCE, snapshots, hot=False)
        second = runtime._oi_refresh_batch(ExchangeName.BINANCE, snapshots, hot=False)
        hot = runtime._oi_refresh_batch(ExchangeName.BINANCE, snapshots, hot=True)

        assert [item.symbol for item in first] == ["SYM0USDT", "SYM1USDT"]
        assert [item.symbol for item in second] == ["SYM2USDT", "SYM3USDT"]
        assert len(hot) == 5
    finally:
        db_path.unlink(missing_ok=True)


def test_degraded_oi_refresh_uses_smaller_batch() -> None:
    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    snapshots = [
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol=f"SYM{index}USDT",
            exchange_symbol=f"SYM{index}USDT",
            timestamp=datetime(2026, 5, 5, tzinfo=UTC),
        )
        for index in range(10)
    ]

    try:
        runtime.config.realtime.binance.values["oi_refresh_batch_size"] = 8
        runtime.config.realtime.binance.values["candidate_oi_rest_poll_interval_seconds"] = 10
        runtime.circuit_breaker.record_failure(ExchangeName.BINANCE, "timeout")

        batch = runtime._oi_refresh_batch(ExchangeName.BINANCE, snapshots, hot=False)
        hot_batch = runtime._oi_refresh_batch(ExchangeName.BINANCE, snapshots, hot=True)

        assert len(batch) == config.realtime.degraded_oi_batch_size
        assert len(hot_batch) == config.realtime.degraded_oi_batch_size
    finally:
        db_path.unlink(missing_ok=True)


def test_handle_signal_sends_one_message_per_passing_exchange() -> None:
    import asyncio

    config = load_config("config.yaml")
    db_path = Path("data") / f"test_scheduler_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    sent: list[tuple[int | None, ExchangeName, str]] = []

    async def notifier(user_id, signal, message):
        sent.append((user_id, signal.primary_exchange, message))
        return True

    runtime = ScreenerRuntime(config=config, storage=storage, notifier=notifier)
    signal = AggregatedSignal(
        symbol="DYDXUSDT",
        direction=config.signal.direction,
        aggregation_mode=config.signal.aggregation_mode,
        primary_exchange=ExchangeName.BINANCE,
        evaluations=(
            _passed_evaluation(ExchangeName.BYBIT, "DYDXUSDT", 80),
            _passed_evaluation(ExchangeName.OKX, "DYDXUSDT", 85),
        ),
        score=85,
        passed=True,
        reason="any_selected_passed",
        created_at=datetime(2026, 5, 5, tzinfo=UTC),
    )

    try:
        asyncio.run(runtime._handle_signal(signal, send=True, telegram_user_id=123, settings=config.signal))

        assert [item[1] for item in sent] == [ExchangeName.BYBIT, ExchangeName.OKX]
        assert "BYBIT: ПРОШЕЛ" in sent[0][2]
        assert "OKX" not in sent[0][2]
        assert "OKX: ПРОШЕЛ" in sent[1][2]
        assert "BYBIT" not in sent[1][2]
        with storage.connect() as conn:
            rows = conn.execute(
                "SELECT symbol, primary_exchange FROM signals ORDER BY id"
            ).fetchall()
        assert [(row["symbol"], row["primary_exchange"]) for row in rows] == [
            ("DYDXUSDT", "bybit"),
            ("DYDXUSDT", "okx"),
        ]
    finally:
        db_path.unlink(missing_ok=True)


def _passed_evaluation(exchange: ExchangeName, symbol: str, score: int) -> ExchangeSignalEvaluation:
    return ExchangeSignalEvaluation(
        exchange=exchange,
        symbol=symbol,
        snapshot=MarketSnapshot(
            exchange=exchange,
            symbol=symbol,
            exchange_symbol=symbol if exchange is not ExchangeName.OKX else "DYDX-USDT-SWAP",
            timestamp=datetime(2026, 5, 5, tzinfo=UTC),
            price=Decimal("1"),
        ),
        filters=(
            FilterResult(
                name="oi_change_pct",
                passed=True,
                value=Decimal("2"),
                threshold=Decimal("1"),
                reason="test",
            ),
        ),
        metrics=SignalMetrics(oi_change_pct=Decimal("2")),
        score=score,
        passed=True,
    )
