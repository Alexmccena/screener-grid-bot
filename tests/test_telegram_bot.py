from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from telegram_oi_screener.config import load_config
from telegram_oi_screener.models import AggregatedSignal, Direction, ExchangeName, MarketSnapshot
from telegram_oi_screener.realtime.scheduler import ScreenerRuntime
from telegram_oi_screener.storage import SQLiteStorage
from telegram_oi_screener.telegram.bot import TelegramScreenerBot, _is_stale_callback_error


class FakeMessage:
    def __init__(self, text: str = "") -> None:
        self.text = text
        self.replies: list[tuple[str, dict[str, object]]] = []

    async def reply_text(self, text: str, **kwargs: object) -> None:
        self.replies.append((text, kwargs))


def _bot() -> tuple[TelegramScreenerBot, SQLiteStorage, Path]:
    config = load_config("config.yaml")
    db_path = Path("data") / f"test_telegram_{uuid4().hex}.sqlite3"
    storage = SQLiteStorage(db_path)
    storage.init_schema()
    runtime = ScreenerRuntime(config=config, storage=storage)
    return TelegramScreenerBot(config=config, runtime=runtime, storage=storage), storage, db_path


def _update(text: str = "", chat_id: int = 123) -> SimpleNamespace:
    return SimpleNamespace(
        effective_chat=SimpleNamespace(id=chat_id),
        message=FakeMessage(text),
        callback_query=None,
    )


def test_menu_button_cancels_pending_input(monkeypatch) -> None:
    bot, _, db_path = _bot()
    update = _update("Инфо")
    called = {"info": False}

    async def fake_info_command(update, context):
        called["info"] = True

    monkeypatch.setattr(bot, "info_command", fake_info_command)
    bot._pending[123] = {"kind": "oi_period"}

    try:
        asyncio.run(bot.text_message(update, SimpleNamespace()))

        assert called["info"]
        assert 123 not in bot._pending
    finally:
        db_path.unlink(missing_ok=True)


def test_diagnostics_menu_button_cancels_pending_input(monkeypatch) -> None:
    bot, _, db_path = _bot()
    update = _update("Диагностика")
    called = {"diagnostics": False}

    async def fake_diagnostics_command(update, context):
        called["diagnostics"] = True

    monkeypatch.setattr(bot, "diagnostics_command", fake_diagnostics_command)
    bot._pending[123] = {"kind": "oi_period"}

    try:
        asyncio.run(bot.text_message(update, SimpleNamespace()))

        assert called["diagnostics"]
        assert 123 not in bot._pending
    finally:
        db_path.unlink(missing_ok=True)


def test_test_button_asks_for_symbol_and_runs_signal(monkeypatch) -> None:
    bot, _, db_path = _bot()
    chat_id = 123
    called: list[str] = []

    async def fake_send_test_signal(update, symbol):
        called.append(symbol)

    monkeypatch.setattr(bot, "_send_test_signal", fake_send_test_signal)
    monkeypatch.setattr("telegram_oi_screener.telegram.bot._main_keyboard", lambda: None)

    try:
        test_update = _update("Тест", chat_id)
        asyncio.run(bot.text_message(test_update, SimpleNamespace()))

        assert bot._pending[chat_id] == {"kind": "test_symbol"}
        assert "Введите тикер монеты без USDT" in test_update.message.replies[-1][0]

        asyncio.run(bot.text_message(_update("btc", chat_id), SimpleNamespace()))

        assert called == ["BTCUSDT"]
        assert chat_id not in bot._pending
    finally:
        db_path.unlink(missing_ok=True)


def test_status_separates_full_refresh_and_latest_db_snapshot(monkeypatch) -> None:
    bot, storage, db_path = _bot()
    update = _update("Статус")
    storage.save_snapshot(
        MarketSnapshot(
            exchange=ExchangeName.BINANCE,
            symbol="BTCUSDT",
            exchange_symbol="BTCUSDT",
            timestamp=datetime(2026, 5, 8, 1, 0, tzinfo=UTC),
            price=Decimal("100000"),
        )
    )
    monkeypatch.setattr("telegram_oi_screener.telegram.bot._main_keyboard", lambda: None)

    try:
        asyncio.run(bot.status_command(update, SimpleNamespace()))

        text = update.message.replies[-1][0]
        assert "📡 Статус скринера" in text
        assert "Состояние: 🟢 работает" in text
        assert "🔄 Полное обновление: нет данных" in text
        assert "💾 Последняя запись в базе: 08.05.2026 04:00:00 МСК" in text
    finally:
        db_path.unlink(missing_ok=True)


def test_status_shows_refresh_in_progress(monkeypatch) -> None:
    bot, _, db_path = _bot()
    update = _update("Статус")
    bot.runtime.state.refresh_in_progress = True
    bot.runtime.state.refresh_started_at = datetime(2026, 5, 8, 1, 0, tzinfo=UTC)
    monkeypatch.setattr("telegram_oi_screener.telegram.bot._main_keyboard", lambda: None)

    try:
        asyncio.run(bot.status_command(update, SimpleNamespace()))

        text = update.message.replies[-1][0]
        assert "🔄 Полное обновление: идет с 08.05.2026 04:00:00 МСК" in text
    finally:
        db_path.unlink(missing_ok=True)


def test_volatility_dialog_saves_period_min_max_and_display() -> None:
    bot, storage, db_path = _bot()
    chat_id = 123
    bot._pending[chat_id] = {"kind": "volatility_period"}

    try:
        asyncio.run(bot._handle_pending_value(_update("45", chat_id), chat_id, bot._pending[chat_id], "45"))
        assert bot._pending[chat_id] == {"kind": "volatility_min", "period": 45}

        asyncio.run(bot._handle_pending_value(_update("1.5", chat_id), chat_id, bot._pending[chat_id], "1.5"))
        assert bot._pending[chat_id] == {
            "kind": "volatility_max",
            "period": 45,
            "min": Decimal("1.5"),
        }

        asyncio.run(bot._handle_pending_value(_update("5", chat_id), chat_id, bot._pending[chat_id], "5"))
        assert bot._pending[chat_id] == {
            "kind": "volatility_display",
            "period": 45,
            "min": Decimal("1.5"),
            "max": Decimal("5"),
        }

        asyncio.run(bot._handle_pending_value(_update("2", chat_id), chat_id, bot._pending[chat_id], "2"))
        raw = storage.load_user_settings(chat_id)

        assert raw is not None
        assert raw["volatility_period_minutes"] == 45
        assert raw["min_volatility_pct"] == "1.5"
        assert raw["max_volatility_pct"] == "5"
        assert raw["volatility_display_mode"] == "diagnostic"
        assert chat_id not in bot._pending
    finally:
        db_path.unlink(missing_ok=True)


def test_settings_text_is_visual_and_normalized() -> None:
    bot, storage, db_path = _bot()
    chat_id = 123
    raw = {
        "profile": "aggressive",
        "enabled_exchanges": ["binance", "bybit", "okx"],
        "native_grid_only_exchanges": [],
        "enabled_filters": [
            "oi_change_pct",
            "oi_value_change_usdt",
            "price_change_pct",
            "volatility_pct",
            "volume_24h_usdt",
        ],
        "oi_period_minutes": 15,
        "min_oi_change_pct": "3",
        "min_oi_value_change_usdt": "100000",
        "price_change_period_minutes": 5,
        "min_price_change_pct": "4",
        "volatility_period_minutes": 5,
        "min_volatility_pct": "2",
        "max_volatility_pct": "20",
        "volatility_display_mode": "market",
        "min_volume_spike_ratio": "1.5",
        "min_24h_volume_usdt": "10000000",
        "max_funding_rate_pct": "-0.1",
        "min_score_to_alert": 60,
        "cooldown_minutes": 30,
    }
    storage.save_user_settings(chat_id, raw)

    try:
        text = bot._settings_text(chat_id)

        assert "⚙️ Настройки скринера" in text
        assert "👤 Профиль: aggressive" in text
        assert "🏦 Биржи: bybit" in text
        assert "✅ Активные фильтры" in text
        assert "📈 OI: 3.00% / 15m · min 0.10M $ · bias OFF" in text
        assert "📊 Price change: 4.00% / 5m" in text
        assert "🌊 Volatility: 2.00%..20.00% / 5m · market" in text
        assert "💵 24h volume: 10.00M $" in text
        assert "Неактивные" in text
        assert "Volume spike: 1.50x" in text
        assert "Funding: -0.1000%" in text
        assert "Min score: 60" in text
        assert "⬜" not in text
        assert "⏱ Cooldown: 30m" in text
    finally:
        db_path.unlink(missing_ok=True)


def test_top_command_sends_candidates_as_separate_messages(monkeypatch) -> None:
    bot, _, db_path = _bot()
    update = _update("Кандидаты")
    bot.runtime.state.latest_signals = [
        AggregatedSignal(
            symbol=f"TEST{index}USDT",
            direction=Direction.LONG,
            aggregation_mode=bot.config.signal.aggregation_mode,
            primary_exchange=ExchangeName.BINANCE,
            evaluations=(),
            score=100 - index,
            passed=True,
            reason="test",
        )
        for index in range(5)
    ]
    monkeypatch.setattr("telegram_oi_screener.telegram.bot._main_keyboard", lambda: None)

    try:
        asyncio.run(bot.top_command(update, SimpleNamespace()))

        assert len(update.message.replies) == 6
        assert "Кандидаты: топ 5" in update.message.replies[0][0]
        assert update.message.replies[1][0].startswith("#1")
        assert update.message.replies[-1][0].startswith("#5")
    finally:
        db_path.unlink(missing_ok=True)


def test_diagnostics_command_sends_market_summary(monkeypatch) -> None:
    bot, storage, db_path = _bot()
    update = _update("Диагностика")
    now = datetime(2026, 5, 13, 12, tzinfo=UTC)
    old_snapshot = MarketSnapshot(
        exchange=ExchangeName.BYBIT,
        symbol="DOGEUSDT",
        exchange_symbol="DOGEUSDT",
        timestamp=now - timedelta(minutes=15),
        price=Decimal("0.20"),
        open_interest=Decimal("1000000"),
        open_interest_value_usdt=Decimal("200000"),
    )
    current_snapshot = MarketSnapshot(
        exchange=ExchangeName.BYBIT,
        symbol="DOGEUSDT",
        exchange_symbol="DOGEUSDT",
        timestamp=now,
        price=Decimal("0.22"),
        open_interest=Decimal("1100000"),
        open_interest_value_usdt=Decimal("242000"),
        volume_24h_usdt=Decimal("10000000"),
    )
    storage.save_snapshot(old_snapshot)
    storage.save_snapshot(current_snapshot)
    monkeypatch.setattr("telegram_oi_screener.telegram.bot._main_keyboard", lambda: None)

    try:
        asyncio.run(bot.diagnostics_command(update, SimpleNamespace()))

        text = update.message.replies[-1][0]
        assert "Диагностика рынка за 24h" in text
        assert "Биржи:" in text
        assert "Фильтры за 24h" in text
        assert "DOGEUSDT" in text
    finally:
        db_path.unlink(missing_ok=True)


def test_oi_dialog_saves_period_pct_and_value() -> None:
    bot, storage, db_path = _bot()
    chat_id = 123
    bot._pending[chat_id] = {"kind": "oi_period"}

    try:
        asyncio.run(bot._handle_pending_value(_update("5", chat_id), chat_id, bot._pending[chat_id], "5"))
        assert bot._pending[chat_id] == {"kind": "oi_pct", "period": 5}

        asyncio.run(bot._handle_pending_value(_update("1.2", chat_id), chat_id, bot._pending[chat_id], "1.2"))
        assert bot._pending[chat_id] == {"kind": "oi_value", "period": 5, "pct": Decimal("1.2")}

        value_update = _update("500000", chat_id)
        asyncio.run(bot._handle_pending_value(value_update, chat_id, bot._pending[chat_id], "500000"))
        assert bot._pending[chat_id] == {
            "kind": "oi_bias",
            "period": 5,
            "pct": Decimal("1.2"),
            "value": Decimal("500000"),
        }
        assert (
            "Показывать Bullish/Bearish OI в сигнале? 1 - да, 2 - нет."
            in value_update.message.replies[-1][0]
        )

        asyncio.run(bot._handle_pending_value(_update("1", chat_id), chat_id, bot._pending[chat_id], "1"))
        raw = storage.load_user_settings(chat_id)

        assert raw is not None
        assert raw["oi_period_minutes"] == 5
        assert raw["min_oi_change_pct"] == "1.2"
        assert raw["min_oi_value_change_usdt"] == "500000"
        assert raw["oi_bias_enabled"] is True
        assert chat_id not in bot._pending
    finally:
        db_path.unlink(missing_ok=True)


def test_stale_callback_bad_request_is_recognized() -> None:
    class BadRequestLike(Exception):
        pass

    BadRequestLike.__name__ = "BadRequest"

    assert _is_stale_callback_error(
        BadRequestLike("Query is too old and response timeout expired or query id is invalid")
    )
