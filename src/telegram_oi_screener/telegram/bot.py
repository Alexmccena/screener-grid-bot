from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ..config import ScreenerConfig
from ..diagnostics import build_market_diagnostics_24h
from ..grid import calculate_grid_dry_run
from ..models import AggregatedSignal, ExchangeName, ExecutionExchange, SIGNAL_FILTER_NAMES
from ..realtime.scheduler import ScreenerRuntime
from ..storage import SQLiteStorage
from ..user_settings import (
    FILTER_LABELS,
    NUMERIC_FIELDS,
    apply_profile,
    default_user_settings,
    effective_settings,
    merge_user_settings,
)
from .formatting import format_grid_dry_run, format_signal

LOGGER = logging.getLogger(__name__)
DISPLAY_TZ = timezone(timedelta(hours=3), name="MSK")
DISPLAY_TZ_LABEL = "МСК"
TELEGRAM_MESSAGE_LIMIT = 4096
TELEGRAM_SAFE_MESSAGE_LIMIT = 3900


class TelegramUnavailable(RuntimeError):
    pass


class TelegramScreenerBot:
    def __init__(
        self,
        config: ScreenerConfig,
        runtime: ScreenerRuntime,
        storage: SQLiteStorage,
    ) -> None:
        self.config = config
        self.runtime = runtime
        self.storage = storage
        self.application: Any | None = None
        self._known_chat_ids: set[int] = set(config.telegram.allowed_user_ids)
        self._pending: dict[int, dict[str, Any]] = {}
        self._watchdog_task: asyncio.Task[None] | None = None
        self._restart_lock = asyncio.Lock()
        self._stopping = False

    async def start(self) -> None:
        token = self.config.telegram.token
        if not token:
            raise TelegramUnavailable("TELEGRAM_BOT_TOKEN is not set")
        telegram = _import_telegram()
        pool_timeout = _env_int("TELEGRAM_POOL_TIMEOUT_SECONDS", default=30, minimum=5)
        builder = (
            telegram["Application"]
            .builder()
            .token(token)
            .connection_pool_size(_env_int("TELEGRAM_CONNECTION_POOL_SIZE", default=32, minimum=4))
            .get_updates_connection_pool_size(_env_int("TELEGRAM_GET_UPDATES_POOL_SIZE", default=8, minimum=2))
            .concurrent_updates(_env_int("TELEGRAM_CONCURRENT_UPDATES", default=8, minimum=1))
            .connect_timeout(30)
            .read_timeout(30)
            .write_timeout(30)
            .pool_timeout(pool_timeout)
            .get_updates_connect_timeout(30)
            .get_updates_read_timeout(30)
            .get_updates_write_timeout(30)
            .get_updates_pool_timeout(pool_timeout)
        )
        proxy_url = os.getenv("TELEGRAM_PROXY_URL")
        if proxy_url:
            builder = builder.proxy(proxy_url).get_updates_proxy(proxy_url)
        application = builder.build()
        application.add_handler(telegram["CommandHandler"]("start", self.start_command))
        application.add_handler(telegram["CommandHandler"]("status", self.status_command))
        application.add_handler(telegram["CommandHandler"]("settings", self.settings_command))
        application.add_handler(telegram["CommandHandler"]("info", self.info_command))
        application.add_handler(telegram["CommandHandler"]("pause", self.pause_command))
        application.add_handler(telegram["CommandHandler"]("resume", self.resume_command))
        application.add_handler(telegram["CommandHandler"]("top", self.top_command))
        application.add_handler(telegram["CommandHandler"]("diagnostics", self.diagnostics_command))
        application.add_handler(telegram["CommandHandler"]("test_signal", self.test_signal_command))
        application.add_handler(telegram["CommandHandler"]("test_grid", self.test_grid_command))
        application.add_handler(telegram["CallbackQueryHandler"](self.callback_query))
        application.add_handler(
            telegram["MessageHandler"](telegram["filters"].TEXT & ~telegram["filters"].COMMAND, self.text_message)
        )
        application.add_error_handler(self.error_handler)
        self.application = application
        initialized = False
        started = False
        try:
            await application.initialize()
            initialized = True
            await application.bot.set_my_commands(
                [
                    telegram["BotCommand"]("start", "Запустить меню"),
                    telegram["BotCommand"]("status", "Статус скринера"),
                    telegram["BotCommand"]("settings", "Настройки фильтров"),
                    telegram["BotCommand"]("info", "Справка по показателям"),
                    telegram["BotCommand"]("test_signal", "Проверить сигнал по символу"),
                    telegram["BotCommand"]("test_grid", "Dry-run grid"),
                    telegram["BotCommand"]("top", "Последние кандидаты"),
                    telegram["BotCommand"]("diagnostics", "Диагностика рынка"),
                    telegram["BotCommand"]("pause", "Пауза"),
                    telegram["BotCommand"]("resume", "Продолжить"),
                ]
            )
            await application.start()
            started = True
            await application.updater.start_polling()
        except Exception as exc:
            if started:
                with suppress(Exception):
                    await application.stop()
            if initialized:
                with suppress(Exception):
                    await application.shutdown()
            self.application = None
            if _is_telegram_network_error(exc):
                raise TelegramUnavailable(
                    "Telegram API is unavailable from this network. "
                    "Check VPN/proxy access to api.telegram.org or set TELEGRAM_PROXY_URL."
                ) from exc
            raise
        self._stopping = False
        self._ensure_watchdog()
        LOGGER.info("telegram bot started")

    async def stop(self) -> None:
        self._stopping = True
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            await asyncio.gather(self._watchdog_task, return_exceptions=True)
            self._watchdog_task = None
        if self.application is None:
            return
        with suppress(Exception):
            await self.application.updater.stop()
        with suppress(Exception):
            await self.application.stop()
        with suppress(Exception):
            await self.application.shutdown()
        self.application = None

    def _ensure_watchdog(self) -> None:
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.create_task(self._telegram_watchdog_loop())

    async def _telegram_watchdog_loop(self) -> None:
        interval_seconds = _env_int("TELEGRAM_WATCHDOG_INTERVAL_SECONDS", default=45, minimum=5)
        failure_limit = _env_int("TELEGRAM_WATCHDOG_FAILURES", default=3, minimum=1)
        failures = 0
        while not self._stopping:
            try:
                await asyncio.sleep(interval_seconds)
                await self._check_telegram_polling()
                if failures:
                    LOGGER.info("telegram watchdog recovered after %s failed checks", failures)
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                if _is_telegram_network_error(exc) or isinstance(exc, TelegramUnavailable):
                    LOGGER.warning("telegram watchdog check failed (%s): %s", failures, exc)
                else:
                    LOGGER.exception("telegram watchdog check failed (%s)", failures)
                if failures >= failure_limit:
                    await self._restart_telegram_polling(exc)
                    failures = 0

    async def _check_telegram_polling(self) -> None:
        application = self.application
        if application is None:
            raise TelegramUnavailable("telegram application is not running")
        updater = getattr(application, "updater", None)
        if updater is None:
            raise TelegramUnavailable("telegram updater is not available")
        if not getattr(updater, "running", True):
            raise TelegramUnavailable("telegram polling is not running")
        await application.bot.get_me(
            connect_timeout=15,
            read_timeout=15,
            write_timeout=15,
            pool_timeout=15,
        )

    async def _restart_telegram_polling(self, reason: object) -> None:
        async with self._restart_lock:
            if self._stopping or self.application is None:
                return
            LOGGER.warning("telegram polling reconnect started after: %s", reason)
            with suppress(Exception):
                await asyncio.wait_for(self.application.updater.stop(), timeout=20)
            backoffs = _env_backoffs("TELEGRAM_RECONNECT_BACKOFF_SECONDS", default=(5, 15, 30, 60, 120))
            attempt = 0
            while not self._stopping and self.application is not None:
                try:
                    await self.application.bot.get_me(
                        connect_timeout=15,
                        read_timeout=15,
                        write_timeout=15,
                        pool_timeout=15,
                    )
                    await self.application.updater.start_polling()
                    LOGGER.info("telegram polling reconnected")
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    delay = backoffs[min(attempt, len(backoffs) - 1)]
                    attempt += 1
                    if _is_telegram_network_error(exc):
                        LOGGER.warning("telegram reconnect failed; retry in %ss: %s", delay, exc)
                    else:
                        LOGGER.exception("telegram reconnect failed; retry in %ss", delay)
                    await asyncio.sleep(delay)

    async def notify(self, telegram_user_id: int | None, signal: AggregatedSignal, message: str) -> bool:
        if self.application is None:
            return False
        chat_ids = [telegram_user_id] if telegram_user_id is not None else list(self._known_chat_ids)
        if not chat_ids:
            LOGGER.info("signal %s not sent: no known Telegram chat ids", signal.symbol)
            return False
        sent_any = False
        for chat_id in chat_ids:
            try:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                sent_any = True
            except Exception as exc:
                if _is_telegram_network_error(exc):
                    LOGGER.warning("telegram send failed for chat %s: %s", chat_id, exc)
                    continue
                LOGGER.exception("telegram send failed for chat %s", chat_id)
                continue
        return sent_any

    async def error_handler(self, update: Any, context: Any) -> None:
        error = getattr(context, "error", None)
        if _is_telegram_network_error(error):
            LOGGER.warning("telegram network timeout while handling update: %s", error)
            return
        LOGGER.error("telegram update failed", exc_info=error)

    async def start_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        chat_id = update.effective_chat.id
        self._pending.pop(chat_id, None)
        self._known_chat_ids.add(chat_id)
        self._ensure_user_settings(chat_id)
        await update.message.reply_text(
            "Скринер запущен. Основные действия доступны кнопками ниже.",
            reply_markup=_main_keyboard(),
        )

    async def status_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        chat_id = update.effective_chat.id
        self._pending.pop(chat_id, None)
        state = self.runtime.state
        settings = self._effective(chat_id)
        latest_db_snapshot = self.storage.latest_market_snapshot_time()
        if state.refresh_in_progress:
            refresh_line = f"🔄 Полное обновление: идет с {_format_status_time(state.refresh_started_at)}"
        else:
            refresh_line = f"🔄 Полное обновление: {_format_status_time(state.last_refresh)}"
        status_text = "⏸ пауза" if state.paused else "🟢 работает"
        lines = [
            "📡 Статус скринера",
            "",
            f"Состояние: {status_text}",
            f"👤 Профиль: {settings.profile}",
            "🏦 Биржи: " + ", ".join(exchange.value for exchange in settings.enabled_exchanges),
            "",
            "⏱ Время",
            refresh_line,
            f"💾 Последняя запись в базе: {_format_status_time(latest_db_snapshot)}",
            f"🔎 Последняя проверка: {_format_status_time(state.last_evaluation)}",
        ]
        await update.message.reply_text("\n".join(lines), reply_markup=_main_keyboard())

    async def settings_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        self._pending.pop(update.effective_chat.id, None)
        await update.message.reply_text(
            self._settings_text(update.effective_chat.id),
            reply_markup=_settings_keyboard(update.effective_chat.id, self.storage, self.config),
        )

    async def info_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        self._pending.pop(update.effective_chat.id, None)
        await update.message.reply_text(_info_text(), reply_markup=_main_keyboard(), parse_mode="HTML")

    async def pause_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        self._pending.pop(update.effective_chat.id, None)
        self.runtime.pause()
        await update.message.reply_text("Скринер поставлен на паузу.", reply_markup=_main_keyboard())

    async def resume_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        self._pending.pop(update.effective_chat.id, None)
        self.runtime.resume()
        await update.message.reply_text("Скринер снова активен.", reply_markup=_main_keyboard())

    async def top_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        self._pending.pop(update.effective_chat.id, None)
        signals = self.runtime.state.latest_signals[:5]
        if not signals:
            await update.message.reply_text("Пока нет рассчитанных кандидатов.", reply_markup=_main_keyboard())
            return
        settings = self._effective(update.effective_chat.id)
        await update.message.reply_text(
            f"📋 Кандидаты: топ {len(signals)} из последней проверки.",
            reply_markup=_main_keyboard(),
        )
        for index, signal in enumerate(signals, start=1):
            message = format_signal(
                signal,
                enabled_filters=settings.enabled_filters,
                oi_period_minutes=settings.oi_period_minutes,
                price_change_period_minutes=settings.price_change_period_minutes,
            )
            await update.message.reply_text(
                _trim_telegram_message(f"#{index}\n{message}"),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )

    async def diagnostics_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        chat_id = update.effective_chat.id
        self._pending.pop(chat_id, None)
        settings = self._effective(chat_id)
        message = build_market_diagnostics_24h(
            storage=self.storage,
            settings=settings,
        )
        chunks = _split_telegram_message(message)
        for index, chunk in enumerate(chunks):
            await update.message.reply_text(
                chunk,
                reply_markup=_main_keyboard() if index == len(chunks) - 1 else None,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )

    async def test_signal_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        self._pending.pop(update.effective_chat.id, None)
        args = context.args or []
        if not args:
            await update.message.reply_text("Пример: /test_signal BTCUSDT", reply_markup=_main_keyboard())
            return
        await self._send_test_signal(update, args[0].upper())

    async def _send_test_signal(self, update: Any, symbol: str) -> None:
        chat_id = update.effective_chat.id
        signals = await self.runtime.scan_once(
            {symbol},
            send=False,
            settings=self._effective(chat_id),
            telegram_user_id=chat_id,
        )
        signal = next((item for item in signals if item.symbol == symbol), None)
        if signal is None:
            await update.message.reply_text(f"Нет данных по {symbol}.", reply_markup=_main_keyboard())
            return
        settings = self._effective(chat_id)
        await update.message.reply_text(
            format_signal(
                signal,
                enabled_filters=settings.enabled_filters,
                oi_period_minutes=settings.oi_period_minutes,
                price_change_period_minutes=settings.price_change_period_minutes,
            ),
            reply_markup=_main_keyboard(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    async def test_grid_command(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        self._pending.pop(update.effective_chat.id, None)
        args = context.args or []
        if len(args) < 2:
            await update.message.reply_text("Пример: /test_grid BTCUSDT bybit", reply_markup=_main_keyboard())
            return
        await self._send_test_grid(update, args[0].upper(), ExecutionExchange(args[1].lower()))

    async def _send_test_grid(self, update: Any, symbol: str, exchange: ExecutionExchange) -> None:
        if exchange is ExecutionExchange.NONE:
            await update.message.reply_text("Для dry-run укажи bybit или okx.", reply_markup=_main_keyboard())
            return
        chat_id = update.effective_chat.id
        signals = await self.runtime.scan_once(
            {symbol},
            send=False,
            settings=self._effective(chat_id),
            telegram_user_id=chat_id,
        )
        current_price = _first_price(signals, symbol)
        if current_price is None:
            await update.message.reply_text(f"Нет цены по {symbol}.", reply_markup=_main_keyboard())
            return
        dry_run = calculate_grid_dry_run(symbol, exchange, current_price, self.config.grid)
        self.storage.save_grid_action(
            dry_run,
            request={"symbol": symbol, "exchange": exchange.value, "source": "telegram"},
            telegram_user_id=chat_id,
        )
        await update.message.reply_text(format_grid_dry_run(dry_run), reply_markup=_main_keyboard())

    async def callback_query(self, update: Any, context: Any) -> None:
        query = update.callback_query
        try:
            await query.answer()
        except Exception as exc:
            if _is_stale_callback_error(exc):
                LOGGER.warning("telegram callback answer skipped: %s", exc)
            elif _is_telegram_network_error(exc):
                LOGGER.warning("telegram callback answer failed: %s", exc)
            else:
                raise
        chat_id = update.effective_chat.id
        if not await self._authorized(update):
            return
        data = str(query.data)
        if not data.startswith("edit:"):
            self._pending.pop(chat_id, None)
        if data == "settings:main":
            await query.edit_message_text(
                self._settings_text(chat_id),
                reply_markup=_settings_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data == "settings:profiles":
            await query.edit_message_text(
                "Выбери профиль риска. Профиль меняет значения фильтров, но не список включенных фильтров.",
                reply_markup=_profiles_keyboard(),
            )
            return
        if data == "settings:custom":
            await query.edit_message_text(
                "🛠 Пользовательские настройки\n\nВыбери параметр для изменения.",
                reply_markup=_custom_settings_keyboard(),
            )
            return
        if data == "settings:filters":
            await query.edit_message_text(
                "Выбери фильтры, которые должны быть обязательными для сигнала.",
                reply_markup=_filters_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data == "settings:exchanges":
            await query.edit_message_text(
                "Выбери биржи, которые могут независимо давать сигнал.",
                reply_markup=_exchanges_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data == "settings:gridonly":
            await query.edit_message_text(
                "Фильтр native grid pairs. ON = сигналы только по парам из grid universe биржи.",
                reply_markup=_grid_only_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data.startswith("filter:"):
            self._toggle_filter(chat_id, data.removeprefix("filter:"))
            await query.edit_message_text(
                "Выбери фильтры, которые должны быть обязательными для сигнала.",
                reply_markup=_filters_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data.startswith("exchange:"):
            self._toggle_exchange(chat_id, data.removeprefix("exchange:"))
            await query.edit_message_text(
                "Выбери биржи, которые могут независимо давать сигнал.",
                reply_markup=_exchanges_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data.startswith("gridonly:"):
            self._toggle_grid_only(chat_id, data.removeprefix("gridonly:"))
            await query.edit_message_text(
                "Фильтр native grid pairs. ON = сигналы только по парам из grid universe биржи.",
                reply_markup=_grid_only_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data.startswith("profile:"):
            profile = data.removeprefix("profile:")
            raw = self.storage.load_user_settings(chat_id)
            self.storage.save_user_settings(chat_id, apply_profile(self.config, profile, raw))
            await query.edit_message_text(
                self._settings_text(chat_id),
                reply_markup=_settings_keyboard(chat_id, self.storage, self.config),
            )
            return
        if data == "edit:oi":
            self._pending[chat_id] = {"kind": "oi_period"}
            await query.message.reply_text("Введите период OI от 1 до 30 минут.")
            return
        if data == "edit:volatility":
            self._pending[chat_id] = {"kind": "volatility_period"}
            await query.message.reply_text("Введите период Volatility от 1 до 120 минут.")
            return
        if data == "edit:price_change":
            self._pending[chat_id] = {"kind": "price_change_period"}
            await query.message.reply_text("Введите период Price change от 1 до 120 минут.")
            return
        if data.startswith("edit:"):
            field = data.removeprefix("edit:")
            if field not in NUMERIC_FIELDS:
                return
            label, minimum, maximum = NUMERIC_FIELDS[field]
            self._pending[chat_id] = {"kind": "numeric", "field": field}
            hint = f"Введите значение: {label}."
            if maximum is not None:
                hint += f" Диапазон {minimum}..{maximum}."
            await query.message.reply_text(hint)

    async def text_message(self, update: Any, context: Any) -> None:
        if not await self._authorized(update):
            return
        chat_id = update.effective_chat.id
        text = update.message.text.strip()
        if await self._handle_menu_text(update, context, text):
            return
        pending = self._pending.get(chat_id)
        if not pending:
            return
        text = text.replace(",", ".")
        try:
            await self._handle_pending_value(update, chat_id, pending, text)
        except ValueError as exc:
            await update.message.reply_text(str(exc))

    async def _handle_menu_text(self, update: Any, context: Any, text: str) -> bool:
        chat_id = update.effective_chat.id
        if text == "Статус":
            self._pending.pop(chat_id, None)
            await self.status_command(update, context)
            return True
        if text == "Настройки":
            self._pending.pop(chat_id, None)
            await self.settings_command(update, context)
            return True
        if text == "Инфо":
            self._pending.pop(chat_id, None)
            await self.info_command(update, context)
            return True
        if text == "Кандидаты":
            self._pending.pop(chat_id, None)
            await self.top_command(update, context)
            return True
        if text == "Диагностика":
            self._pending.pop(chat_id, None)
            await self.diagnostics_command(update, context)
            return True
        if text == "Пауза":
            self._pending.pop(chat_id, None)
            await self.pause_command(update, context)
            return True
        if text == "Продолжить":
            self._pending.pop(chat_id, None)
            await self.resume_command(update, context)
            return True
        if text == "Тест":
            self._pending[chat_id] = {"kind": "test_symbol"}
            await update.message.reply_text(
                "Введите тикер монеты без USDT и без биржи. Например: BTC",
                reply_markup=_main_keyboard(),
            )
            return True
        if text == "Тест сигнала BTCUSDT":
            self._pending.pop(chat_id, None)
            await self._send_test_signal(update, "BTCUSDT")
            return True
        if text == "Тест grid Bybit":
            self._pending.pop(chat_id, None)
            await self._send_test_grid(update, "BTCUSDT", ExecutionExchange.BYBIT)
            return True
        if text == "Тест grid OKX":
            self._pending.pop(chat_id, None)
            await self._send_test_grid(update, "BTCUSDT", ExecutionExchange.OKX)
            return True
        if text == "Назад":
            self._pending.pop(chat_id, None)
            await update.message.reply_text(
                "Основное меню.",
                reply_markup=_main_keyboard(),
            )
            return True
        return False

    async def _handle_pending_value(self, update: Any, chat_id: int, pending: dict[str, Any], text: str) -> None:
        kind = pending["kind"]
        if kind == "test_symbol":
            symbol = _normalize_test_symbol(text)
            self._pending.pop(chat_id, None)
            await self._send_test_signal(update, symbol)
            return
        if kind == "oi_period":
            value = _int_range(text, 1, 30, "Период OI")
            pending["period"] = value
            pending["kind"] = "oi_pct"
            await update.message.reply_text("Теперь введите минимальный рост OI в %, например 10.")
            return
        if kind == "oi_pct":
            value = _decimal_range(text, Decimal("0"), Decimal("1000"), "Рост OI")
            pending["pct"] = value
            pending["kind"] = "oi_value"
            await update.message.reply_text("Теперь введите минимальный прирост OI value в USDT, например 500000.")
            return
        if kind == "oi_value":
            value = _decimal_range(text, Decimal("0"), None, "Min OI value")
            pending["value"] = value
            pending["kind"] = "oi_bias"
            await update.message.reply_text(
                "\u041f\u043e\u043a\u0430\u0437\u044b\u0432\u0430\u0442\u044c Bullish/Bearish OI "
                "\u0432 \u0441\u0438\u0433\u043d\u0430\u043b\u0435? 1 - \u0434\u0430, 2 - \u043d\u0435\u0442."
            )
            return
        if kind == "oi_bias":
            enabled = _yes_no(text, "Bullish/Bearish OI")
            updates = {
                "oi_period_minutes": pending["period"],
                "min_oi_change_pct": str(pending["pct"]),
                "min_oi_value_change_usdt": str(pending["value"]),
                "oi_bias_enabled": enabled,
            }
            self._save_updates(chat_id, updates)
            self._pending.pop(chat_id, None)
            await update.message.reply_text(
                "OI-настройки сохранены.\n\n" + self._settings_text(chat_id),
                reply_markup=_settings_keyboard(chat_id, self.storage, self.config),
            )
            return
        if kind == "volatility_period":
            value = _int_range(text, 1, 120, "Период Volatility")
            pending["period"] = value
            pending["kind"] = "volatility_min"
            await update.message.reply_text("Теперь введите минимальную волатильность в %, например 1.")
            return
        if kind == "price_change_period":
            value = _int_range(text, 1, 120, "Период Price change")
            pending["period"] = value
            pending["kind"] = "price_change_pct"
            await update.message.reply_text("Теперь введите минимальный рост цены в %, например 0.5.")
            return
        if kind == "price_change_pct":
            value = _decimal_range(text, Decimal("0"), Decimal("1000"), "Минимальный рост Price change")
            self._save_updates(
                chat_id,
                {
                    "price_change_period_minutes": pending["period"],
                    "min_price_change_pct": str(value),
                },
            )
            self._pending.pop(chat_id, None)
            await update.message.reply_text(
                "Price change-настройки сохранены.\n\n" + self._settings_text(chat_id),
                reply_markup=_settings_keyboard(chat_id, self.storage, self.config),
            )
            return
        if kind == "volatility_min":
            value = _decimal_range(text, Decimal("0"), Decimal("1000"), "Минимальная волатильность")
            pending["min"] = value
            pending["kind"] = "volatility_max"
            await update.message.reply_text("Теперь введите максимальную волатильность в %, например 4.")
            return
        if kind == "volatility_max":
            value = _decimal_range(text, pending["min"], Decimal("1000"), "Максимальная волатильность")
            pending["max"] = value
            pending["kind"] = "volatility_display"
            await update.message.reply_text(
                "Выберите отображение Volatility: 1 - рыночная волатильность периода, 2 - свечи внутри периода."
            )
            return
        if kind == "volatility_display":
            display_mode = _volatility_display_mode(text)
            self._save_updates(
                chat_id,
                {
                    "volatility_period_minutes": pending["period"],
                    "min_volatility_pct": str(pending["min"]),
                    "max_volatility_pct": str(pending["max"]),
                    "volatility_display_mode": display_mode,
                },
            )
            self._pending.pop(chat_id, None)
            await update.message.reply_text(
                "Volatility-настройки сохранены.\n\n" + self._settings_text(chat_id),
                reply_markup=_settings_keyboard(chat_id, self.storage, self.config),
            )
            return
        if kind == "numeric":
            field = pending["field"]
            label, minimum, maximum = NUMERIC_FIELDS[field]
            if isinstance(minimum, int):
                value = _int_range(text, minimum, int(maximum), label)
            else:
                value = _decimal_range(text, minimum, maximum, label)
            self._save_updates(chat_id, {field: str(value) if isinstance(value, Decimal) else value})
            self._pending.pop(chat_id, None)
            await update.message.reply_text(
                "Настройка сохранена.\n\n" + self._settings_text(chat_id),
                reply_markup=_settings_keyboard(chat_id, self.storage, self.config),
            )

    async def _authorized(self, update: Any) -> bool:
        allowed = set(self.config.telegram.allowed_user_ids)
        chat_id = update.effective_chat.id
        if allowed and chat_id not in allowed:
            target = update.message or update.callback_query.message
            await target.reply_text("Доступ к этому боту ограничен.")
            return False
        self._known_chat_ids.add(chat_id)
        self._ensure_user_settings(chat_id)
        return True

    def _ensure_user_settings(self, chat_id: int) -> None:
        if self.storage.load_user_settings(chat_id) is None:
            self.storage.save_user_settings(chat_id, default_user_settings(self.config))

    def _effective(self, chat_id: int):
        return effective_settings(self.config, self.storage.load_user_settings(chat_id))

    def _save_updates(self, chat_id: int, updates: dict[str, Any]) -> None:
        raw = self.storage.load_user_settings(chat_id)
        self.storage.save_user_settings(chat_id, merge_user_settings(self.config, raw, updates))

    def _toggle_filter(self, chat_id: int, filter_name: str) -> None:
        if filter_name not in SIGNAL_FILTER_NAMES:
            return
        raw = self.storage.load_user_settings(chat_id) or default_user_settings(self.config)
        enabled = set(raw.get("enabled_filters", SIGNAL_FILTER_NAMES))
        if filter_name in {"oi_change_pct", "oi_value_change_usdt"}:
            has_oi = "oi_change_pct" in enabled or "oi_value_change_usdt" in enabled
            if has_oi:
                enabled.difference_update({"oi_change_pct", "oi_value_change_usdt"})
            else:
                enabled.update({"oi_change_pct", "oi_value_change_usdt"})
            if not enabled:
                enabled.update({"oi_change_pct", "oi_value_change_usdt"})
            raw["enabled_filters"] = [item for item in SIGNAL_FILTER_NAMES if item in enabled]
            self.storage.save_user_settings(chat_id, raw)
            return
        if filter_name in enabled:
            enabled.remove(filter_name)
        else:
            enabled.add(filter_name)
        if not enabled:
            enabled.update({"oi_change_pct", "oi_value_change_usdt"})
        raw["enabled_filters"] = [item for item in SIGNAL_FILTER_NAMES if item in enabled]
        self.storage.save_user_settings(chat_id, raw)

    def _toggle_exchange(self, chat_id: int, exchange_name: str) -> None:
        if exchange_name not in {exchange.value for exchange in ExchangeName}:
            return
        raw = self.storage.load_user_settings(chat_id) or default_user_settings(self.config)
        enabled = set(
            raw.get(
                "enabled_exchanges",
                [exchange.value for exchange in self.config.signal.enabled_exchanges],
            )
        )
        if exchange_name in enabled:
            enabled.remove(exchange_name)
        else:
            enabled.add(exchange_name)
        if not enabled:
            enabled.add(ExchangeName.BINANCE.value)
        raw["enabled_exchanges"] = [
            exchange.value for exchange in ExchangeName if exchange.value in enabled
        ]
        self.storage.save_user_settings(chat_id, raw)

    def _toggle_grid_only(self, chat_id: int, exchange_name: str) -> None:
        if exchange_name not in {exchange.value for exchange in ExchangeName}:
            return
        raw = self.storage.load_user_settings(chat_id) or default_user_settings(self.config)
        enabled = set(raw.get("native_grid_only_exchanges", []))
        if exchange_name in enabled:
            enabled.remove(exchange_name)
        else:
            enabled.add(exchange_name)
        raw["native_grid_only_exchanges"] = [
            exchange.value for exchange in ExchangeName if exchange.value in enabled
        ]
        self.storage.save_user_settings(chat_id, raw)

    def _settings_text(self, chat_id: int) -> str:
        settings = self._effective(chat_id)
        thresholds = settings.thresholds
        enabled = set(settings.enabled_filters)
        exchanges = ", ".join(exchange.value for exchange in settings.enabled_exchanges)
        grid_only = (
            ", ".join(exchange.value for exchange in settings.native_grid_only_exchanges)
            if settings.native_grid_only_exchanges
            else "OFF"
        )
        active_lines, inactive_lines = _settings_filter_lines(settings)
        lines = [
            "⚙️ Настройки скринера",
            "",
            f"👤 Профиль: {settings.profile}",
            f"🏦 Биржи: {exchanges}",
            f"🧩 Native grid pairs: {grid_only}",
            "",
            "✅ Активные фильтры",
            *(active_lines or ["нет"]),
        ]
        if inactive_lines:
            lines.extend(["", "Неактивные", *inactive_lines])
        lines.extend(["", f"⏱ Cooldown: {thresholds.cooldown_minutes}m"])
        return "\n".join(lines)


def _settings_filter_lines(settings: Any) -> tuple[list[str], list[str]]:
    thresholds = settings.thresholds
    enabled = set(settings.enabled_filters)
    filter_rows = [
        (
            "oi_change_pct",
            {"oi_change_pct", "oi_value_change_usdt"}.issubset(enabled),
            f"📈 OI: {_fmt_pct2(thresholds.min_oi_change_pct)} / {settings.oi_period_minutes}m · min {_fmt_money_m(thresholds.min_oi_value_change_usdt)} · bias {_on_off(settings.oi_bias_enabled)}",
            f"OI: {_fmt_pct2(thresholds.min_oi_change_pct)} / {settings.oi_period_minutes}m · min {_fmt_money_m(thresholds.min_oi_value_change_usdt)} · bias {_on_off(settings.oi_bias_enabled)}",
        ),
        (
            "price_change_pct",
            "price_change_pct" in enabled,
            f"📊 Price change: {_fmt_pct2(thresholds.min_price_change_pct)} / {settings.price_change_period_minutes}m",
            f"Price change: {_fmt_pct2(thresholds.min_price_change_pct)} / {settings.price_change_period_minutes}m",
        ),
        (
            "volatility_pct",
            "volatility_pct" in enabled,
            f"🌊 Volatility: {_fmt_pct2(thresholds.min_volatility_pct)}..{_fmt_pct2(thresholds.max_volatility_pct)} / {settings.volatility_period_minutes}m · {_volatility_display_label(settings.volatility_display_mode)}",
            f"Volatility: {_fmt_pct2(thresholds.min_volatility_pct)}..{_fmt_pct2(thresholds.max_volatility_pct)} / {settings.volatility_period_minutes}m · {_volatility_display_label(settings.volatility_display_mode)}",
        ),
        (
            "volume_spike_ratio",
            "volume_spike_ratio" in enabled,
            f"📦 Volume spike: {_fmt_ratio(thresholds.min_volume_spike_ratio)}",
            f"Volume spike: {_fmt_ratio(thresholds.min_volume_spike_ratio)}",
        ),
        (
            "volume_24h_usdt",
            "volume_24h_usdt" in enabled,
            f"💵 24h volume: {_fmt_money_m(thresholds.min_24h_volume_usdt)}",
            f"24h volume: {_fmt_money_m(thresholds.min_24h_volume_usdt)}",
        ),
        (
            "funding_rate_pct",
            "funding_rate_pct" in enabled,
            f"💸 Funding: {_fmt_pct4(thresholds.max_funding_rate_pct)}",
            f"Funding: {_fmt_pct4(thresholds.max_funding_rate_pct)}",
        ),
        (
            "min_score",
            "min_score" in enabled,
            f"⭐ Min score: {thresholds.min_score_to_alert}",
            f"Min score: {thresholds.min_score_to_alert}",
        ),
    ]
    active = [active_text for _, is_enabled, active_text, _ in filter_rows if is_enabled]
    inactive = [inactive_text for _, is_enabled, _, inactive_text in filter_rows if not is_enabled]
    return active, inactive


def _first_price(signals: list[AggregatedSignal], symbol: str):
    for signal in signals:
        if signal.symbol != symbol:
            continue
        for evaluation in signal.evaluations:
            if evaluation.snapshot and evaluation.snapshot.price is not None:
                return evaluation.snapshot.price
    return None


def _is_telegram_network_error(error: object) -> bool:
    if error is None:
        return False
    return error.__class__.__name__ in {"TimedOut", "NetworkError"}


def _env_int(name: str, *, default: int, minimum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        LOGGER.warning("invalid %s=%r, using %s", name, raw, default)
        return default
    return max(minimum, value)


def _env_backoffs(name: str, *, default: tuple[int, ...]) -> tuple[int, ...]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    values: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(max(1, int(part)))
        except ValueError:
            LOGGER.warning("invalid %s=%r, using %s", name, raw, ",".join(str(item) for item in default))
            return default
    return tuple(values) or default


def _is_stale_callback_error(error: object) -> bool:
    if error is None or error.__class__.__name__ != "BadRequest":
        return False
    message = str(error).lower()
    return "query is too old" in message or "query id is invalid" in message


def _volatility_display_mode(text: str) -> str:
    normalized = text.strip().lower()
    if normalized in {"1", "market", "рынок", "рыночная"}:
        return "market"
    if normalized in {"2", "diagnostic", "candles", "свечи", "свечная"}:
        return "diagnostic"
    raise ValueError("Отображение Volatility: введите 1 для рыночной волатильности или 2 для свечей.")


def _volatility_display_label(mode: str) -> str:
    if mode == "diagnostic":
        return "candles"
    return "market"


def _filter_mark(enabled: bool) -> str:
    return "✅" if enabled else "⬜"


def _fmt_pct2(value: object) -> str:
    return f"{_fmt_decimal(value, '0.01')}%"


def _fmt_pct4(value: object) -> str:
    return f"{_fmt_decimal(value, '0.0001')}%"


def _fmt_ratio(value: object) -> str:
    return f"{_fmt_decimal(value, '0.01')}x"


def _fmt_money_m(value: object) -> str:
    return f"{_fmt_decimal(Decimal(str(value)) / Decimal('1000000'), '0.01')}M $"


def _on_off(value: object) -> str:
    return "ON" if bool(value) else "OFF"


def _fmt_decimal(value: object, quant: str) -> str:
    return str(Decimal(str(value)).quantize(Decimal(quant)))


def _info_text() -> str:
    return "\n".join(
        [
            "ℹ️ Быстрый гид по скринеру",
            "",
            "Сигнал приходит, когда монета проходит все включенные фильтры. OFF-фильтры остаются в сообщении только как справка.",
            "",
            "📌 Кнопки",
            "• <b>Статус</b> - здоровье бота, база, последнее обновление и проверка.",
            "• <b>Настройки</b> - профиль, фильтры, биржи и параметры сигналов.",
            "• <b>Кандидаты</b> - последние найденные сетапы из последней проверки.",
            "• <b>Диагностика</b> - картина рынка за 24ч: сектора, OI/Price events, volatility.",
            "• <b>Тест</b> - ручная проверка тикера: введи BTC, SOL, BILL и т.п.",
            "• <b>Пауза / Продолжить</b> - остановить или включить отправку сигналов.",
            "",
            "⚙️ Настройки",
            "• <b>Профиль</b> - быстрые пресеты Conservative / Normal / Aggressive.",
            "• <b>Пользовательские настройки</b> - ручная настройка каждого фильтра.",
            "• <b>Фильтры ON/OFF</b> - что обязательно для сигнала. Выключенное попадает в Инфо.",
            "• <b>Биржи ON/OFF</b> - откуда брать сигналы. Сейчас можно оставить одну рабочую биржу.",
            "• <b>Grid pairs only</b> - отсекать пары не из списка native grid, если список доступен.",
            "",
            "📊 Фильтры",
            "• <b>OI</b> - рост открытого интереса за период + min OI USD. Главный фильтр интереса.",
            "• <b>Price change</b> - минимальный рост цены за период. Полезно вместе с OI.",
            "• <b>Volatility</b> - диапазон движения за период. Market = проверка общей volatility.",
            "• <b>Volume spike</b> - объем выше обычного, например 1.5x или 2.0x.",
            "• <b>24h volume</b> - ликвидность пары за сутки. Отсекает тонкие инструменты.",
            "• <b>Funding</b> - не брать перегретые контракты.",
            "• <b>Score</b> - общий рейтинг сетапа 0-100. <b>Cooldown</b> - пауза повторного сигнала.",
            "",
            "🚀 Быстрый старт",
            "1) Включи OI + 24h volume.",
            "2) Добавь Price change, если нужен импульс цены.",
            "3) Диагностика подскажет, рынок сегодня тихий или горячий.",
        ]
    )


def _format_status_time(value: datetime | None) -> str:
    if value is None:
        return "нет данных"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    local = value.astimezone(DISPLAY_TZ)
    return f"{local:%d.%m.%Y %H:%M:%S} {DISPLAY_TZ_LABEL} ({_ago(value)})"


def _trim_telegram_message(text: str, limit: int = TELEGRAM_SAFE_MESSAGE_LIMIT) -> str:
    if len(text) <= limit:
        return text
    suffix = "\n\n… сообщение сокращено"
    return text[: max(0, limit - len(suffix))].rstrip() + suffix


def _split_telegram_message(text: str, limit: int = TELEGRAM_SAFE_MESSAGE_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current = ""
    for section in text.split("\n\n"):
        candidate = section if not current else f"{current}\n\n{section}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
        current = _trim_telegram_message(section, limit) if len(section) > limit else section
    if current:
        chunks.append(current)
    return chunks or [""]


def _ago(value: datetime) -> str:
    now = datetime.now(UTC)
    seconds = max(0, int((now - value.astimezone(UTC)).total_seconds()))
    if seconds < 60:
        return f"{seconds} сек назад"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    minutes = minutes % 60
    if hours < 24:
        if minutes:
            return f"{hours} ч {minutes} мин назад"
        return f"{hours} ч назад"
    days = hours // 24
    hours = hours % 24
    if hours:
        return f"{days} д {hours} ч назад"
    return f"{days} д назад"


def _settings_keyboard(chat_id: int, storage: SQLiteStorage, config: ScreenerConfig):
    InlineKeyboardButton, InlineKeyboardMarkup = _inline_classes()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Профиль", callback_data="settings:profiles")],
            [InlineKeyboardButton("Пользовательские настройки", callback_data="settings:custom")],
            [
                InlineKeyboardButton("Фильтры ON/OFF", callback_data="settings:filters"),
                InlineKeyboardButton("Биржи ON/OFF", callback_data="settings:exchanges"),
            ],
            [InlineKeyboardButton("Grid pairs only", callback_data="settings:gridonly")],
        ]
    )


def _profiles_keyboard():
    InlineKeyboardButton, InlineKeyboardMarkup = _inline_classes()
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Conservative", callback_data="profile:conservative"),
                InlineKeyboardButton("Normal", callback_data="profile:normal"),
                InlineKeyboardButton("Aggressive", callback_data="profile:aggressive"),
            ],
            [InlineKeyboardButton("Назад", callback_data="settings:main")],
        ]
    )


def _custom_settings_keyboard():
    InlineKeyboardButton, InlineKeyboardMarkup = _inline_classes()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("OI", callback_data="edit:oi")],
            [
                InlineKeyboardButton("Price change", callback_data="edit:price_change"),
                InlineKeyboardButton("Volatility", callback_data="edit:volatility"),
            ],
            [
                InlineKeyboardButton("Volume spike", callback_data="edit:min_volume_spike_ratio"),
                InlineKeyboardButton("24h volume", callback_data="edit:min_24h_volume_usdt"),
            ],
            [
                InlineKeyboardButton("Funding", callback_data="edit:max_funding_rate_pct"),
                InlineKeyboardButton("Score", callback_data="edit:min_score_to_alert"),
                InlineKeyboardButton("Cooldown", callback_data="edit:cooldown_minutes"),
            ],
            [InlineKeyboardButton("Фильтры ON/OFF", callback_data="settings:filters")],
            [InlineKeyboardButton("Биржи ON/OFF", callback_data="settings:exchanges")],
            [InlineKeyboardButton("Grid pairs only", callback_data="settings:gridonly")],
            [InlineKeyboardButton("Назад", callback_data="settings:main")],
        ]
    )


def _active_filter_labels(enabled_filters: tuple[str, ...]) -> str:
    labels: list[str] = []
    added_oi = False
    for name in enabled_filters:
        if name in {"oi_change_pct", "oi_value_change_usdt"}:
            if not added_oi:
                labels.append("OI")
                added_oi = True
            continue
        labels.append(FILTER_LABELS[name])
    return ", ".join(labels)


def _filters_keyboard(chat_id: int, storage: SQLiteStorage, config: ScreenerConfig):
    InlineKeyboardButton, InlineKeyboardMarkup = _inline_classes()
    settings = effective_settings(config, storage.load_user_settings(chat_id))
    enabled = set(settings.enabled_filters)
    rows = []
    for name in SIGNAL_FILTER_NAMES:
        if name == "oi_value_change_usdt":
            continue
        mark = "✓" if name in enabled else "□"
        rows.append([InlineKeyboardButton(f"{mark} {FILTER_LABELS[name]}", callback_data=f"filter:{name}")])
    rows.append([InlineKeyboardButton("Назад", callback_data="settings:main")])
    return InlineKeyboardMarkup(rows)


def _exchanges_keyboard(chat_id: int, storage: SQLiteStorage, config: ScreenerConfig):
    InlineKeyboardButton, InlineKeyboardMarkup = _inline_classes()
    settings = effective_settings(config, storage.load_user_settings(chat_id))
    enabled = {exchange.value for exchange in settings.enabled_exchanges}
    rows = []
    for exchange in ExchangeName:
        mark = "✓" if exchange.value in enabled else "□"
        rows.append(
            [InlineKeyboardButton(f"{mark} {exchange.value}", callback_data=f"exchange:{exchange.value}")]
        )
    rows.append([InlineKeyboardButton("Назад", callback_data="settings:main")])
    return InlineKeyboardMarkup(rows)


def _grid_only_keyboard(chat_id: int, storage: SQLiteStorage, config: ScreenerConfig):
    InlineKeyboardButton, InlineKeyboardMarkup = _inline_classes()
    settings = effective_settings(config, storage.load_user_settings(chat_id))
    enabled = {exchange.value for exchange in settings.native_grid_only_exchanges}
    rows = []
    for exchange in ExchangeName:
        mark = "✓" if exchange.value in enabled else "□"
        rows.append(
            [InlineKeyboardButton(f"{mark} {exchange.value}", callback_data=f"gridonly:{exchange.value}")]
        )
    rows.append([InlineKeyboardButton("Назад", callback_data="settings:main")])
    return InlineKeyboardMarkup(rows)


def _inline_classes():
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    except ModuleNotFoundError as exc:
        raise TelegramUnavailable("python-telegram-bot is not installed") from exc
    return InlineKeyboardButton, InlineKeyboardMarkup


def _import_telegram() -> dict[str, object]:
    try:
        from telegram import BotCommand
        from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters
    except ModuleNotFoundError as exc:
        raise TelegramUnavailable("python-telegram-bot is not installed") from exc
    return {
        "Application": Application,
        "CallbackQueryHandler": CallbackQueryHandler,
        "CommandHandler": CommandHandler,
        "MessageHandler": MessageHandler,
        "BotCommand": BotCommand,
        "filters": filters,
    }


def _main_keyboard():
    try:
        from telegram import KeyboardButton, ReplyKeyboardMarkup
    except ModuleNotFoundError as exc:
        raise TelegramUnavailable("python-telegram-bot is not installed") from exc
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Статус"), KeyboardButton("Настройки")],
            [KeyboardButton("Кандидаты"), KeyboardButton("Тест")],
            [KeyboardButton("Диагностика"), KeyboardButton("Инфо")],
            [KeyboardButton("Пауза"), KeyboardButton("Продолжить")],
        ],
        resize_keyboard=True,
        is_persistent=False,
    )


def _test_keyboard():
    try:
        from telegram import KeyboardButton, ReplyKeyboardMarkup
    except ModuleNotFoundError as exc:
        raise TelegramUnavailable("python-telegram-bot is not installed") from exc
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Тест сигнала BTCUSDT")],
            [KeyboardButton("Тест grid Bybit"), KeyboardButton("Тест grid OKX")],
            [KeyboardButton("Назад")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def _decimal_range(
    text: str,
    minimum: Decimal,
    maximum: Decimal | None,
    label: str,
) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{label}: нужно число.") from exc
    if value < minimum:
        raise ValueError(f"{label}: значение должно быть >= {minimum}.")
    if maximum is not None and value > maximum:
        raise ValueError(f"{label}: значение должно быть <= {maximum}.")
    return value


def _int_range(text: str, minimum: int, maximum: int, label: str) -> int:
    try:
        value = int(text)
    except ValueError as exc:
        raise ValueError(f"{label}: нужно целое число.") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{label}: значение должно быть {minimum}..{maximum}.")
    return value


def _yes_no(text: str, label: str) -> bool:
    normalized = text.strip().lower()
    if normalized in {"1", "yes", "y", "true", "on", "да", "д"}:
        return True
    if normalized in {"2", "0", "no", "n", "false", "off", "нет", "н"}:
        return False
    raise ValueError(f"{label}: введите 1 или 2.")


def _normalize_test_symbol(text: str) -> str:
    symbol = (
        text.strip()
        .upper()
        .replace(" ", "")
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
    )
    if symbol.endswith("USDT"):
        symbol = symbol[:-4]
    if not symbol or not symbol.isalnum():
        raise ValueError("Введите тикер монеты латиницей, например BTC или SOL.")
    return f"{symbol}USDT"
