# Telegram OI + Volume + Volatility Screener

Telegram-бот для мониторинга публичных фьючерсных рынков Binance, Bybit и OKX. Сервис собирает market data в реальном времени, нормализует данные разных бирж, рассчитывает фильтры по open interest, объему, волатильности, funding и изменению цены, после чего отправляет пользователю сигналы в Telegram.

Pet-project реализован в AI-assisted workflow с использованием ChatGPT и VS Code: асинхронный realtime-сбор данных, работа с внешними API, SQLite-хранилище, конфигурация через YAML, Telegram UI, тесты и защитная логика для нестабильной сети/API.

Реальная торговля не выполняется. API-ключи бирж не нужны: используются только публичные данные.

## Demo

Ниже несколько экранов Telegram-интерфейса: настройки фильтров, рыночная диагностика, рейтинги событий и пример найденного кандидата.

<table>
  <tr>
    <td width="50%" valign="top">
      <img src="docs/screenshots/telegram_settings.png" alt="Настройки Telegram screener" width="100%"><br>
      <sub>Настройки профиля, фильтров и cooldown.</sub>
    </td>
    <td width="50%" valign="top">
      <img src="docs/screenshots/telegram_rankings.png" alt="Рейтинги price events и volatility" width="100%"><br>
      <sub>Top Price events и Top Volatility 5m.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <img src="docs/screenshots/telegram_diagnostics.png" alt="Диагностика рынка за 24 часа" width="100%"><br>
      <sub>Диагностика рынка за 24 часа и статистика по секторам.</sub>
    </td>
    <td width="50%" valign="top">
      <img src="docs/screenshots/telegram_signal.png" alt="Статус screener и пример сигнала" width="100%"><br>
      <sub>Статус сервиса и пример кандидата LONG grid.</sub>
    </td>
  </tr>
</table>

## Что умеет проект

- получает публичные данные Binance USD-M, Bybit Linear и OKX USDT SWAP;
- приводит данные бирж к единой модели `MarketSnapshot`;
- хранит историю в SQLite;
- прогревает rolling buffer из базы после перезапуска;
- рассчитывает сигналы по OI, OI value, price change, volatility, volume spike, 24h volume, funding и score;
- поддерживает cooldown, профили риска и включение/отключение фильтров через Telegram;
- показывает диагностическую сводку рынка за 24 часа;
- умеет делать dry-run расчет grid-параметров без выставления ордеров;
- содержит тесты для расчетов, конфигурации, форматирования, storage, scheduler и Telegram-логики.

## Архитектура

```text
Binance ─┐
Bybit   ─┼─> Market Data ─> Normalization ─> Rolling Buffer ─> Signal Engine
OKX     ─┘                                            │
                                                      ├─> SQLite
                                                      └─> Telegram Bot
```

Ключевые слои:

- `config.py` - загрузка YAML-конфига, профилей и env-настроек;
- `models.py` - доменные dataclass-модели;
- `exchanges/` - public REST clients, нормализация ошибок и rate limiter;
- `realtime/` - WebSocket managers, REST refresh loops, scheduler и circuit breaker;
- `rolling_buffer.py` - временной буфер OI, цены и объема;
- `signal_engine.py` - фильтры, score, агрегация бирж и anti-spam;
- `storage.py` - SQLite schema и repository-слой;
- `telegram/` - Telegram UI и форматирование сообщений;
- `diagnostics.py` - рыночная диагностика по данным SQLite;
- `grid.py` - dry-run расчет grid-параметров.

## Быстрый старт

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"

Copy-Item .env.example .env
Copy-Item config.example.yaml config.yaml

oi-screener validate-config --config config.yaml
oi-screener init-db --config config.yaml
oi-screener scan-once --config config.yaml
```

### Linux / VPS

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
cp config.example.yaml config.yaml

oi-screener validate-config --config config.yaml
oi-screener init-db --config config.yaml
oi-screener scan-once --config config.yaml
```

Для запуска Telegram-бота заполните `TELEGRAM_BOT_TOKEN` в `.env` или переменных окружения:

```powershell
$env:TELEGRAM_BOT_TOKEN = "..."
oi-screener run --config config.yaml
```

На Linux:

```bash
export TELEGRAM_BOT_TOKEN="..."
oi-screener run --config config.yaml
```

## Конфигурация

Основной пример настроек находится в `config.example.yaml`.

Чаще всего меняются:

- `exchanges.*.enabled` - какие биржи включены;
- `signal.primary_exchange` - основная биржа для сигналов;
- `signal.oi_scan_max_symbols` - размер рабочей вселенной монет;
- `signal.oi_scan_min_24h_volume_usdt` - минимальный 24h volume для попадания в сканер;
- `profiles.*` - пороги фильтров для профилей `conservative`, `normal`, `aggressive`;
- `realtime.*` - интервалы REST/WebSocket-обновлений и circuit breaker;
- `history.*` - срок хранения SQLite-истории.

Локальные файлы `.env`, `config.yaml` и SQLite-базы не коммитятся.

Подробнее о логике отбора монет, hot candidates, периодах обновления данных и настройках фильтров: [docs/screener_logic.md](docs/screener_logic.md).

## Telegram UI

Бот предоставляет кнопочное меню:

- `Статус` - текущее состояние сервиса и актуальность данных;
- `Настройки` - профиль, фильтры, биржи, cooldown и grid-only режим;
- `Кандидаты` - последние найденные кандидаты;
- `Тест` - проверка конкретного тикера;
- `Диагностика` - сводка рынка за 24 часа;
- `Инфо` - краткая справка по кнопкам и фильтрам;
- `Пауза` / `Продолжить` - управление отправкой сигналов.

## CLI-команды

- `validate-config` - проверить конфиг и вывести активный профиль;
- `init-db` - создать SQLite schema;
- `scan-once` - выполнить один REST refresh и оценку сигналов;
- `run` - запустить realtime scheduler и Telegram bot;
- `test-signal SYMBOL` - dry-run расчет фильтров по текущим REST-данным;
- `test-grid SYMBOL --exchange bybit|okx` - расчет grid-параметров без создания ордера;
- `backfill [SYMBOL...] --days N` - заполнить SQLite историей OI/candles;
- `compare-coinalyze` - сравнить данные биржи с Coinalyze для диагностики fallback-источника.

## SQLite и исторический кэш

Скринер хранит историю в `data/screener.sqlite3`:

- `market_snapshots` - нормализованные market snapshots;
- `open_interest_history` - история OI;
- `market_candles` - свечи `1m` и `5m`;
- `funding_history` - история funding;
- `signals` - история отправленных сигналов;
- `backfill_jobs` - журнал запусков backfill.

После перезапуска бот сначала прогревает `RollingBuffer` последними данными из SQLite, затем добирает свежие точки через API.

Примеры:

```powershell
oi-screener backfill BTCUSDT ETHUSDT --days 14 --config config.yaml
oi-screener backfill --days 7 --config config.yaml
```

Без списка symbols команда берет top symbols по 24h volume. Количество задается в `history.backfill_top_symbols`.

## Проверки

```powershell
$env:PYTHONPATH = "src"
python -m pytest
python -m compileall src tests
```

На Linux:

```bash
PYTHONPATH=src python -m pytest
python -m compileall src tests
```

## Что стоит посмотреть в коде

Для быстрого ревью проекта:

- `src/telegram_oi_screener/realtime/scheduler.py` - основной realtime loop;
- `src/telegram_oi_screener/signal_engine.py` - логика фильтров и scoring;
- `src/telegram_oi_screener/storage.py` - SQLite-слой;
- `src/telegram_oi_screener/telegram/bot.py` - Telegram UI;
- `tests/` - покрытие ключевой бизнес-логики.
