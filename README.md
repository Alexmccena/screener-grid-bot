# Telegram OI + Volume + Volatility Screener

MVP скринера из плана: public market data по Binance USD-M, Bybit Linear и OKX USDT SWAP, единый `MarketSnapshot`, rolling buffer, 7 фильтров, scoring, SQLite, Telegram-команды и dry-run расчёт grid-параметров.

Реальная торговля в MVP не выполняется. API keys для бирж не нужны.

## Быстрый старт

```powershell
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -e ".[dev]"
Copy-Item .env.example .env
oi-screener validate-config --config config.yaml
oi-screener init-db --config config.yaml
oi-screener scan-once --config config.yaml
```

Для Telegram-бота заполните `TELEGRAM_BOT_TOKEN` в `.env` или переменных окружения:

```powershell
$env:TELEGRAM_BOT_TOKEN = "..."
oi-screener run --config config.yaml
```

## Команды

- `validate-config` - проверить конфиг и вывести активный профиль.
- `init-db` - создать SQLite schema.
- `scan-once` - выполнить один REST refresh и оценку сигналов.
- `run` - запустить realtime scheduler и Telegram bot, если токен задан.
- `test-signal SYMBOL` - dry-run расчёт фильтров по текущим REST-данным.
- `test-grid SYMBOL --exchange bybit|okx` - расчёт grid-параметров без создания бота.
- `backfill [SYMBOL...] --days N` - заполнить SQLite историей OI/candles для выбранных symbols или top symbols.

## Telegram

Доступные команды:

- `/start`
- `/status`
- `/settings`
- `/pause`
- `/resume`
- `/top`
- `/test_signal BTCUSDT`
- `/test_grid BTCUSDT bybit`

## Архитектура

Ключевые слои:

- `config.py` - загрузка профилей и настроек.
- `models.py` - доменные dataclass-модели.
- `exchanges/` - public REST clients и rate limiter.
- `realtime/` - WebSocket managers и scheduler.
- `rolling_buffer.py` - временной буфер OI, цены и объёма.
- `signal_engine.py` - фильтры, score, агрегация бирж и anti-spam.
- `storage.py` - SQLite schema/repositories.
- `telegram/` - тонкий интерфейс команд без бизнес-логики фильтров.
- `grid.py` - dry-run расчёт будущих grid-действий.

## Проверки

```powershell
$env:PYTHONPATH = "src"
python -m pytest
python -m compileall src tests
```

## Исторический кэш

Скринер хранит длинную историю в `data/screener.sqlite3`:

- `market_candles` - свечи `1m` и `5m`;
- `open_interest_history` - OI history с шагом `5m`;
- `funding_history` - место под funding history;
- `backfill_jobs` - журнал запусков backfill.

При расчёте сигналов бот сначала прогревает `RollingBuffer` последними часами из SQLite, затем добирает свежее через API и сохраняет новые точки.

Примеры:

```powershell
oi-screener backfill BTCUSDT ETHUSDT --days 14 --config config.yaml
oi-screener backfill --days 7 --config config.yaml
```

Без списка symbols команда берёт top symbols по 24h volume, количество задаётся в `history.backfill_top_symbols`.
