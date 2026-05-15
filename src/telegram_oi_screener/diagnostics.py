from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from html import escape
from statistics import median
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from .calculations import calculate_oi_change_pct, calculate_price_change_pct, calculate_volatility_from_candles
from .models import Candle, ExchangeName, MarketSnapshot, SignalSettings, ZERO
from .rolling_buffer import RollingBuffer
from .storage import SQLiteStorage

DIAGNOSTIC_TABLE_WIDTH = 76


@dataclass(frozen=True)
class MarketDiagnosticRow:
    exchange: ExchangeName
    symbol: str
    sector: str | None
    oi_change_pct: Decimal | None
    oi_value_change_usdt: Decimal | None
    price_change_pct: Decimal | None
    volatility_pct: Decimal | None
    volume_24h_usdt: Decimal | None
    bullish_oi_usdt: Decimal | None
    bearish_oi_usdt: Decimal | None


@dataclass(frozen=True)
class DailyDiagnosticRow:
    exchange: ExchangeName
    symbol: str
    sector: str
    oi_events: int
    price_events: int
    best_oi_change_pct: Decimal | None
    best_oi_value_change_usdt: Decimal | None
    best_price_change_pct: Decimal | None
    bullish_oi_usdt: Decimal
    bearish_oi_usdt: Decimal
    volatility_values: tuple[Decimal, ...] = ()

    @property
    def key(self) -> str:
        return f"{self.exchange.value.upper()} {self.symbol}"


def build_market_diagnostics_24h(
    storage: SQLiteStorage,
    settings: SignalSettings,
    sectors_path: Path | str = Path("data/sectors.yaml"),
    limit: int = 10,
    now: datetime | None = None,
) -> str:
    sector_map = load_sector_map(sectors_path)
    end = now or storage.latest_market_snapshot_time() or datetime.now(UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    window_start = end - timedelta(hours=24)
    lookback_start = window_start - timedelta(
        minutes=max(settings.oi_period_minutes, settings.price_change_period_minutes) + 5
    )
    snapshots = storage.load_market_snapshots_since(lookback_start, settings.enabled_exchanges)
    candles = storage.load_candles_since(window_start, "5m", settings.enabled_exchanges)
    if not snapshots and not candles:
        return (
            "📊 <b>Диагностика рынка за 24h</b>\n\n"
            "В базе пока нет данных за последние 24 часа."
        )

    rows = _daily_rows(
        snapshots=snapshots,
        candles=candles,
        settings=settings,
        sector_map=sector_map,
        window_start=window_start,
    )
    all_volatility = [value for row in rows for value in row.volatility_values]
    sectors = _daily_sector_rows(rows)
    lines = [
        "📊 <b>Диагностика рынка за 24h</b>",
        "",
        f"🕘 Период: <b>{_fmt_time(window_start)} - {_fmt_time(end)} UTC</b>",
        f"🏦 Биржи: <b>{escape(', '.join(exchange.value for exchange in settings.enabled_exchanges))}</b>",
        f"🧭 Монет в базе: <b>{len({row.symbol for row in rows})}</b>",
        f"🕯 Монет с 5m candles: <b>{len({candle.symbol for candle in candles})}</b>",
        f"🗂 Секторов: <b>{len(sectors)}</b>",
        "",
        _table(
            ("Метрика", "Событий", "Настройка"),
            (
                (
                    "OI",
                    str(sum(row.oi_events for row in rows)),
                    f"{_fmt_pct(settings.thresholds.min_oi_change_pct)} / "
                    f"{settings.oi_period_minutes}m + {_fmt_money(settings.thresholds.min_oi_value_change_usdt)}",
                ),
                (
                    "Price",
                    str(sum(row.price_events for row in rows)),
                    f"{_fmt_pct(settings.thresholds.min_price_change_pct)} / "
                    f"{settings.price_change_period_minutes}m",
                ),
                (
                    "Vol avg",
                    _fmt_pct(_avg_decimal(all_volatility)),
                    "5m candles",
                ),
                (
                    "Vol median",
                    _fmt_pct(_median_decimal(all_volatility)),
                    "5m candles",
                ),
            ),
            title="🎯 Фильтры за 24h",
        ),
        "ℹ️ Событие = историческая точка в базе, которая прошла текущий пользовательский порог.",
        "",
    ]
    if sectors:
        lines.extend([_format_daily_sector_table(sectors), ""])
    lines.extend(
        [
            _format_daily_top_table(
                rows,
                title="📈 Топ OI events",
                key=lambda row: row.best_oi_change_pct,
                value=lambda row: _fmt_pct(row.best_oi_change_pct),
                extra=lambda row: f"{row.oi_events} ev / {_fmt_money(row.best_oi_value_change_usdt)}",
                limit=limit,
            ),
            "",
            _format_daily_top_table(
                rows,
                title="💹 Топ Price events",
                key=lambda row: row.best_price_change_pct,
                value=lambda row: _fmt_pct(row.best_price_change_pct),
                extra=lambda row: f"{row.price_events} ev",
                limit=limit,
            ),
            "",
            _format_daily_top_table(
                rows,
                title="🌊 Топ Volatility 5m",
                key=lambda row: max(row.volatility_values) if row.volatility_values else None,
                value=lambda row: _fmt_pct(max(row.volatility_values) if row.volatility_values else None),
                extra=lambda row: (
                    f"avg {_fmt_pct(_avg_decimal(list(row.volatility_values)))} / "
                    f"med {_fmt_pct(_median_decimal(list(row.volatility_values)))}"
                ),
                limit=limit,
            ),
            "",
            _format_daily_top_table(
                _without_major_symbols(rows),
                title="🟢 Bullish OI за 24h",
                key=lambda row: row.bullish_oi_usdt if row.bullish_oi_usdt > ZERO else None,
                value=lambda row: _fmt_money(row.bullish_oi_usdt),
                extra=lambda row: "",
                limit=limit,
            ),
            "",
            _format_daily_top_table(
                _without_major_symbols(rows),
                title="🔴 Bearish OI за 24h",
                key=lambda row: row.bearish_oi_usdt if row.bearish_oi_usdt > ZERO else None,
                value=lambda row: _fmt_money(row.bearish_oi_usdt),
                extra=lambda row: "",
                limit=limit,
            ),
        ]
    )
    return "\n".join(lines).strip()


def build_market_diagnostics(
    snapshots: dict[str, dict[ExchangeName, MarketSnapshot]],
    candles: dict[str, dict[ExchangeName, tuple[Candle, ...]]],
    buffer: RollingBuffer,
    settings: SignalSettings,
    sectors_path: Path | str = Path("data/sectors.yaml"),
    limit: int = 10,
) -> str:
    sector_map = load_sector_map(sectors_path)
    rows: list[MarketDiagnosticRow] = []
    for symbol, by_exchange in snapshots.items():
        for exchange in settings.enabled_exchanges:
            snapshot = by_exchange.get(exchange)
            if snapshot is None:
                continue
            row = _diagnostic_row(
                snapshot=snapshot,
                candle_list=tuple(candles.get(symbol, {}).get(exchange, ())),
                buffer=buffer,
                settings=settings,
                sector=sector_map.get(_base_symbol(symbol), "Прочее"),
            )
            rows.append(row)

    if not rows:
        return "📊 <b>Диагностика рынка</b>\n\nПока нет текущих данных для расчета."

    lines = [
        "📊 <b>Диагностика рынка</b>",
        "",
        f"🏦 Биржи: <b>{escape(', '.join(exchange.value for exchange in settings.enabled_exchanges))}</b>",
        f"🧭 Монет в срезе: <b>{len({row.symbol for row in rows})}</b>",
        f"🗂 Секторов с данными: <b>{len(_sector_groups(rows))}</b>",
        "🧩 Сектора: <code>data/sectors.yaml</code> + Прочее",
        "",
        _table(
            ("Метрика", "Факт", "Настройка"),
            (
                (
                    "OI pass",
                    f"{_count_pass(rows, 'oi', settings)}/{len(rows)}",
                    f"{_fmt_pct(settings.thresholds.min_oi_change_pct)} / {settings.oi_period_minutes}m",
                ),
                (
                    "Price pass",
                    f"{_count_pass(rows, 'price', settings)}/{len(rows)}",
                    f"{_fmt_pct(settings.thresholds.min_price_change_pct)} / "
                    f"{settings.price_change_period_minutes}m",
                ),
                (
                    "Vol avg",
                    _avg(_values(row.volatility_pct for row in rows), pct=True),
                    f"{settings.volatility_period_minutes}m",
                ),
            ),
            title="🎛 Фильтры дня",
        ),
        "",
        _coverage_table(rows),
        *_missing_data_notes(rows),
        "",
    ]
    sector_lines = _format_sectors(rows, limit=10)
    if sector_lines:
        lines.extend([sector_lines, ""])
    lines.extend(
        [
            _format_top_table(
                rows,
                title="📈 Топ OI",
                key=lambda row: row.oi_change_pct,
                value=lambda row: _fmt_pct(row.oi_change_pct),
                limit=limit,
            ),
            "",
            _format_top_table(
                rows,
                title="💹 Топ Price change",
                key=lambda row: row.price_change_pct,
                value=lambda row: _fmt_pct(row.price_change_pct),
                limit=limit,
            ),
            "",
            _format_top_table(
                rows,
                title="🌊 Топ Volatility",
                key=lambda row: row.volatility_pct,
                value=lambda row: _fmt_pct(row.volatility_pct),
                limit=limit,
            ),
            "",
            _format_top_table(
                _without_major_symbols(rows),
                title="🟢 Bullish OI",
                key=lambda row: row.bullish_oi_usdt,
                value=lambda row: _fmt_money(row.bullish_oi_usdt),
                limit=limit,
            ),
            "",
            _format_top_table(
                _without_major_symbols(rows),
                title="🔴 Bearish OI",
                key=lambda row: row.bearish_oi_usdt,
                value=lambda row: _fmt_money(row.bearish_oi_usdt),
                limit=limit,
            ),
        ]
    )
    return "\n".join(lines).strip()


def load_sector_map(path: Path | str) -> dict[str, str]:
    sector_path = Path(path)
    if not sector_path.exists():
        return {}
    raw = yaml.safe_load(sector_path.read_text(encoding="utf-8")) or {}
    result: dict[str, str] = {}
    for sector, symbols in raw.items():
        if not isinstance(symbols, list):
            continue
        for symbol in symbols:
            base = _base_symbol(str(symbol).upper())
            result[base] = str(sector)
            if base.startswith("1000"):
                result[base.removeprefix("1000")] = str(sector)
    return result


def _diagnostic_row(
    snapshot: MarketSnapshot,
    candle_list: tuple[Candle, ...],
    buffer: RollingBuffer,
    settings: SignalSettings,
    sector: str | None,
) -> MarketDiagnosticRow:
    oi_old = buffer.get_point_ago(
        snapshot.exchange,
        snapshot.symbol,
        settings.oi_period_minutes,
        snapshot.timestamp,
    )
    price_old = buffer.get_point_ago(
        snapshot.exchange,
        snapshot.symbol,
        settings.price_change_period_minutes,
        snapshot.timestamp,
    )
    volatility_pct = _volatility(snapshot, candle_list, buffer, settings)
    bullish, bearish = _oi_bias(snapshot, buffer, settings, oi_old)
    return MarketDiagnosticRow(
        exchange=snapshot.exchange,
        symbol=snapshot.symbol,
        sector=sector,
        oi_change_pct=calculate_oi_change_pct(
            snapshot.open_interest,
            oi_old.open_interest if oi_old else None,
        ),
        oi_value_change_usdt=(
            snapshot.open_interest_value_usdt - oi_old.open_interest_value_usdt
            if snapshot.open_interest_value_usdt is not None
            and oi_old is not None
            and oi_old.open_interest_value_usdt is not None
            else None
        ),
        price_change_pct=calculate_price_change_pct(
            snapshot.price,
            price_old.price if price_old else None,
        ),
        volatility_pct=volatility_pct,
        volume_24h_usdt=snapshot.volume_24h_usdt,
        bullish_oi_usdt=bullish,
        bearish_oi_usdt=bearish,
    )


def _daily_rows(
    snapshots: list[MarketSnapshot],
    candles: list[Candle],
    settings: SignalSettings,
    sector_map: dict[str, str],
    window_start: datetime,
) -> list[DailyDiagnosticRow]:
    by_key: dict[tuple[ExchangeName, str], list[MarketSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        by_key[(snapshot.exchange, snapshot.symbol)].append(snapshot)
    volatility_by_key: dict[tuple[ExchangeName, str], list[Decimal]] = defaultdict(list)
    for candle in candles:
        value = calculate_volatility_from_candles((candle,))
        if value is not None:
            volatility_by_key[(candle.exchange, candle.symbol)].append(value)

    keys = set(by_key) | set(volatility_by_key)
    rows: list[DailyDiagnosticRow] = []
    for exchange, symbol in sorted(keys, key=lambda item: (item[0].value, item[1])):
        points = sorted(by_key.get((exchange, symbol), ()), key=lambda item: item.timestamp)
        oi_events = 0
        price_events = 0
        best_oi_change_pct: Decimal | None = None
        best_oi_value_change_usdt: Decimal | None = None
        best_price_change_pct: Decimal | None = None
        bullish = ZERO
        bearish = ZERO
        previous: MarketSnapshot | None = None
        for point in points:
            if point.timestamp >= window_start:
                oi_old = _snapshot_ago(points, point.timestamp, settings.oi_period_minutes)
                oi_change_pct = calculate_oi_change_pct(
                    point.open_interest,
                    oi_old.open_interest if oi_old else None,
                )
                oi_value_change = (
                    point.open_interest_value_usdt - oi_old.open_interest_value_usdt
                    if point.open_interest_value_usdt is not None
                    and oi_old is not None
                    and oi_old.open_interest_value_usdt is not None
                    else None
                )
                if (
                    oi_change_pct is not None
                    and oi_change_pct >= settings.thresholds.min_oi_change_pct
                    and oi_value_change is not None
                    and oi_value_change >= settings.thresholds.min_oi_value_change_usdt
                ):
                    oi_events += 1
                    if best_oi_change_pct is None or oi_change_pct > best_oi_change_pct:
                        best_oi_change_pct = oi_change_pct
                        best_oi_value_change_usdt = oi_value_change

                price_old = _snapshot_ago(points, point.timestamp, settings.price_change_period_minutes)
                price_change_pct = calculate_price_change_pct(
                    point.price,
                    price_old.price if price_old else None,
                )
                if (
                    price_change_pct is not None
                    and price_change_pct >= settings.thresholds.min_price_change_pct
                ):
                    price_events += 1
                    if best_price_change_pct is None or price_change_pct > best_price_change_pct:
                        best_price_change_pct = price_change_pct

            if (
                previous is not None
                and point.timestamp >= window_start
                and point.price is not None
                and previous.price is not None
                and point.open_interest_value_usdt is not None
                and previous.open_interest_value_usdt is not None
            ):
                oi_delta = point.open_interest_value_usdt - previous.open_interest_value_usdt
                price_delta = point.price - previous.price
                if oi_delta > ZERO and price_delta > ZERO:
                    bullish += oi_delta
                elif oi_delta > ZERO and price_delta < ZERO:
                    bearish += oi_delta
            previous = point

        rows.append(
            DailyDiagnosticRow(
                exchange=exchange,
                symbol=symbol,
                sector=sector_map.get(_base_symbol(symbol), "Прочее"),
                oi_events=oi_events,
                price_events=price_events,
                best_oi_change_pct=best_oi_change_pct,
                best_oi_value_change_usdt=best_oi_value_change_usdt,
                best_price_change_pct=best_price_change_pct,
                bullish_oi_usdt=bullish,
                bearish_oi_usdt=bearish,
                volatility_values=tuple(volatility_by_key.get((exchange, symbol), ())),
            )
        )
    return rows


def _snapshot_ago(
    points: list[MarketSnapshot],
    now: datetime,
    minutes: int,
) -> MarketSnapshot | None:
    target = now - timedelta(minutes=minutes)
    result: MarketSnapshot | None = None
    price: Decimal | None = None
    open_interest: Decimal | None = None
    open_interest_value_usdt: Decimal | None = None
    for point in points:
        if point.timestamp > target:
            break
        result = point
        if point.price is not None:
            price = point.price
        if point.open_interest is not None:
            open_interest = point.open_interest
        if point.open_interest_value_usdt is not None:
            open_interest_value_usdt = point.open_interest_value_usdt
    if result is None:
        return None
    return result.with_updates(
        price=price,
        open_interest=open_interest,
        open_interest_value_usdt=open_interest_value_usdt,
    )


def _daily_sector_rows(rows: list[DailyDiagnosticRow]) -> list[tuple[str, list[DailyDiagnosticRow]]]:
    groups: dict[str, list[DailyDiagnosticRow]] = defaultdict(list)
    for row in rows:
        groups[row.sector].append(row)
    return sorted(
        groups.items(),
        key=lambda item: (
            _median_decimal([value for row in item[1] for value in row.volatility_values]) or ZERO,
            item[0] != "Прочее",
            sum(row.oi_events + row.price_events for row in item[1]),
        ),
        reverse=True,
    )


def _format_daily_sector_table(sectors: list[tuple[str, list[DailyDiagnosticRow]]]) -> str:
    return _table(
        ("Сектор", "Монет", "OI ev", "Price ev", "Vol med"),
        tuple(
            (
                sector,
                str(len({row.symbol for row in rows})),
                str(sum(row.oi_events for row in rows)),
                str(sum(row.price_events for row in rows)),
                _fmt_pct(_median_decimal([value for row in rows for value in row.volatility_values])),
            )
            for sector, rows in sectors[:10]
        ),
        title="🗂 Сектора за 24h",
    )


def _without_major_symbols(rows):
    return [row for row in rows if _base_symbol(row.symbol) not in {"BTC", "ETH"}]


def _format_daily_top_table(
    rows: list[DailyDiagnosticRow],
    title: str,
    key,
    value,
    extra,
    limit: int,
) -> str:
    ranked = [row for row in rows if key(row) is not None]
    ranked.sort(key=lambda row: key(row), reverse=True)
    if not ranked:
        return "нет данных"
    return _table(
        ("Пара", "Факт", "Инфо", "Сектор"),
        tuple(
            (
                row.symbol,
                value(row),
                extra(row) or "-",
                row.sector,
            )
            for row in ranked[:limit]
        ),
        title=title,
    )


def _volatility(
    snapshot: MarketSnapshot,
    candles: tuple[Candle, ...],
    buffer: RollingBuffer,
    settings: SignalSettings,
) -> Decimal | None:
    cutoff = snapshot.timestamp - timedelta(minutes=settings.volatility_period_minutes)
    period_candles = tuple(candle for candle in candles if candle.open_time >= cutoff)
    if period_candles:
        return calculate_volatility_from_candles(period_candles)
    low, high = buffer.min_max_price(snapshot.exchange, snapshot.symbol, cutoff, snapshot.timestamp)
    if low is None or high is None or snapshot.price in (None, ZERO):
        return None
    return (high - low) / snapshot.price * Decimal("100")


def _oi_bias(
    snapshot: MarketSnapshot,
    buffer: RollingBuffer,
    settings: SignalSettings,
    oi_old: Any,
) -> tuple[Decimal | None, Decimal | None]:
    start = snapshot.timestamp - timedelta(minutes=settings.oi_period_minutes)
    points = list(buffer.points_between(snapshot.exchange, snapshot.symbol, start, snapshot.timestamp))
    if oi_old is not None and oi_old.timestamp < start:
        points.insert(0, oi_old)
    points.append(
        SimpleNamespace(
            timestamp=snapshot.timestamp,
            price=snapshot.price,
            open_interest_value_usdt=snapshot.open_interest_value_usdt,
        )
    )
    points = sorted(
        (
            point
            for point in points
            if point.price is not None and point.open_interest_value_usdt is not None
        ),
        key=lambda item: item.timestamp,
    )
    if len(points) < 2:
        return None, None
    bullish = ZERO
    bearish = ZERO
    previous = points[0]
    for point in points[1:]:
        oi_delta = point.open_interest_value_usdt - previous.open_interest_value_usdt
        price_delta = point.price - previous.price
        if oi_delta > ZERO and price_delta > ZERO:
            bullish += oi_delta
        elif oi_delta > ZERO and price_delta < ZERO:
            bearish += oi_delta
        previous = point
    return bullish, bearish


def _format_sectors(rows: list[MarketDiagnosticRow], limit: int) -> str:
    groups = _sector_groups(rows)
    ranked = sorted(
        groups.items(),
        key=lambda item: (
            item[0] != "Прочее",
            _avg_decimal(_values(row.volatility_pct for row in item[1])) or ZERO,
            len(item[1]),
        ),
        reverse=True,
    )[:limit]
    return _table(
        ("Сектор", "Монет", "Vol", "OI", "Price"),
        tuple(
            (
                sector,
                str(len({row.symbol for row in group})),
                _avg(_values(row.volatility_pct for row in group), pct=True),
                _avg(_values(row.oi_change_pct for row in group), pct=True),
                _avg(_values(row.price_change_pct for row in group), pct=True),
            )
            for sector, group in ranked
        ),
        title="🗂 Сектора",
    )


def _sector_groups(rows: list[MarketDiagnosticRow]) -> dict[str, list[MarketDiagnosticRow]]:
    groups: dict[str, list[MarketDiagnosticRow]] = defaultdict(list)
    for row in rows:
        if row.sector:
            groups[row.sector].append(row)
    return dict(groups)


def _format_top_table(
    rows: list[MarketDiagnosticRow],
    title: str,
    key,
    value,
    limit: int,
) -> str:
    ranked = [row for row in rows if key(row) is not None]
    ranked.sort(key=lambda row: key(row), reverse=True)
    if not ranked:
        return "нет данных"
    return _table(
        ("Пара", "Факт", "Сектор"),
        tuple(
            (
                row.symbol,
                value(row),
                row.sector or "-",
            )
            for row in ranked[:limit]
        ),
        title=title,
    )


def _count_pass(rows: list[MarketDiagnosticRow], kind: str, settings: SignalSettings) -> int:
    if kind == "oi":
        return sum(
            1
            for row in rows
            if row.oi_change_pct is not None
            and row.oi_change_pct >= settings.thresholds.min_oi_change_pct
            and row.oi_value_change_usdt is not None
            and row.oi_value_change_usdt >= settings.thresholds.min_oi_value_change_usdt
        )
    if kind == "price":
        return sum(
            1
            for row in rows
            if row.price_change_pct is not None
            and row.price_change_pct >= settings.thresholds.min_price_change_pct
        )
    return 0


def _values(values) -> list[Decimal]:
    return [value for value in values if value is not None]


def _coverage_table(rows: list[MarketDiagnosticRow]) -> str:
    total = len(rows)
    oi_present, oi_missing = _coverage((row.oi_change_pct for row in rows), total)
    price_present, price_missing = _coverage((row.price_change_pct for row in rows), total)
    volatility_present, volatility_missing = _coverage((row.volatility_pct for row in rows), total)
    bias_present, bias_missing = _coverage(
        (
            (
                row.bullish_oi_usdt
                if row.bullish_oi_usdt is not None or row.bearish_oi_usdt is None
                else row.bearish_oi_usdt
            )
            for row in rows
        ),
        total,
    )
    return _table(
        ("Показатель", "Есть", "Нет"),
        (
            ("OI", oi_present, oi_missing),
            ("Price", price_present, price_missing),
            ("Volatility", volatility_present, volatility_missing),
            ("Bull/Bear", bias_present, bias_missing),
        ),
        title="🧪 Покрытие данных",
    )


def _coverage(values, total: int) -> tuple[str, str]:
    present = sum(1 for value in values if value is not None)
    return f"{present}/{total}", f"{total - present}/{total}"


def _missing_data_notes(rows: list[MarketDiagnosticRow]) -> list[str]:
    total = len(rows)
    volatility_present = sum(1 for row in rows if row.volatility_pct is not None)
    bias_present = sum(
        1
        for row in rows
        if row.bullish_oi_usdt is not None or row.bearish_oi_usdt is not None
    )
    notes: list[str] = []
    if volatility_present < total:
        notes.append(
            "ℹ️ Volatility: нужны свежие свечи или несколько ценовых точек в RAM-buffer "
            "за выбранный период."
        )
    if bias_present < total:
        notes.append(
            "ℹ️ Bullish/Bearish OI: нужны минимум две точки за OI-период, "
            "и в каждой должны быть цена и OI value."
        )
    return notes


def _avg(values: list[Decimal], *, pct: bool = False) -> str:
    value = _avg_decimal(values)
    if value is None:
        return "n/a"
    return _fmt_pct(value) if pct else _fmt_decimal(value)


def _avg_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(len(values))


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(str(median(values)))


def _base_symbol(symbol: str) -> str:
    symbol = symbol.upper()
    if symbol.endswith("USDT"):
        symbol = symbol[:-4]
    return symbol


def _fmt_pct(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    return f"{value.quantize(Decimal('0.01'))}%"


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    return f"{(value / Decimal('1000000')).quantize(Decimal('0.01'))}M $"


def _fmt_decimal(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def _fmt_time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%d.%m %H:%M")


def _table(
    headers: tuple[str, ...],
    rows: tuple[tuple[object, ...], ...],
    title: str | None = None,
) -> str:
    data = [tuple(str(item) for item in headers), *(tuple(str(item) for item in row) for row in rows)]
    widths = [max(len(row[index]) for row in data) for index in range(len(headers))]
    rendered = []
    if title:
        rendered.append(title)
        rendered.append("-" * len(title))
    for row_index, row in enumerate(data):
        rendered.append("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))
        if row_index == 0:
            rendered.append("  ".join("-" * width for width in widths))
    table_width = max(DIAGNOSTIC_TABLE_WIDTH, *(len(line) for line in rendered))
    return "<pre>" + escape("\n".join(line.ljust(table_width) for line in rendered)) + "</pre>"
